// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

const (
	PLASMA_CLEANER_MIN_SIZE = int64(16 * 1024 * 1024)
)

type CompactionManager interface {
}

type compactionManager struct {
	logPrefix string
	config    common.Config
	supvMsgCh MsgChannel
	supvCmdCh MsgChannel
}

type compactionDaemon struct {
	quitch       chan bool
	started      bool
	timer        *time.Timer
	msgch        MsgChannel
	config       common.ConfigHolder
	stats        IndexerStatsHolder
	indexInstMap common.IndexInstMap
	compactions  map[string]*indexCompaction
	history      map[string]*indexCompaction
	clusterAddr  string
	lastCheckDay int32
	mutex        sync.Mutex
}

type indexCompaction struct {
	instId      common.IndexInstId
	partitionId common.PartitionId
	startTime   int64
	endTime     int64
}

//////////////////////////////////////////////////////////////////
// CompactionDaemon
//////////////////////////////////////////////////////////////////

func (cd *compactionDaemon) Start() {
	if !cd.started {
		conf := cd.config.Load()
		dur := time.Second * time.Duration(conf["check_period"].Int())
		cd.timer = time.NewTimer(dur)
		cd.started = true
		go cd.loop()
	}
}

func (cd *compactionDaemon) Stop() {
	if cd.started {
		cd.timer.Stop()
		cd.quitch <- true
		<-cd.quitch
	}
}

func (cd *compactionDaemon) ResetConfig(c common.Config) {
	last_config := cd.config.Load()
	cd.config.Store(c)

	// Auto-compaction settings are unnecessary for plasma and memory optimized
	// indexes. Ignore the auto-compaction settings for these storage modes
	if common.GetStorageMode() != common.FORESTDB {
		return
	}

	abort := c["abort_exceed_interval"].Bool()
	interval := c["interval"].String()

	var start_hr, start_min, end_hr, end_min int
	fmt.Sscanf(interval, "%d:%d,%d:%d", &start_hr, &start_min, &end_hr, &end_min)

	if abort && end_hr == 0 && end_min == 0 {
		// if abort specified, but no end time.
		common.Console(cd.clusterAddr, "Compaction setting misconfigured.  End time is not specified while allowing compaction to abort.")
		logging.Errorf("Compaction setting misconfigured.  End time is not specified while allowing compaction to abort.")
	} else if !abort && !(end_hr == 0 && end_min == 0) {
		// if end_time specified, but no abort.
		common.Console(cd.clusterAddr, "Compaction setting misconfigured.  End time is specified while not allowing compaction to abort.")
		logging.Errorf("Compaction setting misconfigured.  End time is specified while not allowing compaction to abort.")
	}

	// force daemon to re-check start time for the next compaction
	last_interval := last_config["interval"].String()
	var last_start_hr, last_start_min, last_end_hr, last_end_min int
	fmt.Sscanf(last_interval, "%d:%d,%d:%d", &last_start_hr, &last_start_min, &last_end_hr, &last_end_min)
	if last_start_hr != start_hr || last_start_min != start_min {
		atomic.StoreInt32(&cd.lastCheckDay, -1)
	}
}

func (cd *compactionDaemon) loop() {
	hasStartedToday := false

loop:
	for {
		select {
		case _, ok := <-cd.timer.C:

			if stats := cd.stats.Get(); stats != nil && stats.indexerState.Value() != int64(common.INDEXER_BOOTSTRAP) {
				if common.GetStorageMode() == common.FORESTDB {
					if ok {
						hasStartedToday = cd.compactFDB(hasStartedToday)
					}
				} else if common.GetStorageMode() == common.PLASMA {
					if ok {
						cd.compactPlasma()
					}
				}
			}

			conf := cd.config.Load()
			dur := time.Second * time.Duration(conf["check_period"].Int())
			cd.timer.Reset(dur)

		case <-cd.quitch:
			cd.quitch <- true
			break loop
		}
	}
}

//////////////////////////////////////////////////////////////////
// Compact FDB
//////////////////////////////////////////////////////////////////

func (cd *compactionDaemon) needsCompaction(is IndexStorageStats, config common.Config, checkTime time.Time, abortTime time.Time) bool {

	mode := strings.ToLower(config["compaction_mode"].String())
	logging.Infof("CompactionDaemon: Checking fragmentation: %s, mode : %s", is.String(), mode)

	if mode == "full" {
		// if full compaction, then
		// 1) check min_size
		// 2) check min_frag
		if uint64(is.Stats.DiskSize) > config["min_size"].Uint64() {
			if is.GetFragmentation() >= float64(config["min_frag"].Int()) {
				return true
			}
		}
	} else {

		// if circular compaction, then
		// 1) check compaction interval
		// 2) check the week of day
		interval := config["interval"].String()
		isCompactionInterval := true

		var start_hr, start_min, end_hr, end_min int
		n, err := fmt.Sscanf(interval, "%d:%d,%d:%d", &start_hr, &start_min, &end_hr, &end_min)

		if n == 4 && err == nil {

			// validate parameters
			if start_hr < 0 || start_hr > 23 {
				common.Console(cd.clusterAddr, "Compaction setting misconfigured.  Invalid start hour %v.", start_hr)
				logging.Errorf("Compaction setting misconfigured.  Invalid start hour %v.", start_hr)
				return false
			}

			if end_hr < 0 || end_hr > 23 {
				common.Console(cd.clusterAddr, "Compaction setting misconfigured.  Invalid end hour %v.", end_hr)
				logging.Errorf("Compaction setting misconfigured.  Invalid end hour %v.", end_hr)
				return false
			}

			if start_min < 0 || start_min > 59 {
				common.Console(cd.clusterAddr, "Compaction setting misconfigured.  Invalid start min %v.", start_min)
				logging.Errorf("Compaction setting misconfigured.  Invalid start min %v.", start_min)
				return false
			}

			if end_min < 0 || end_min > 59 {
				common.Console(cd.clusterAddr, "Compaction setting misconfigured.  Invalid end min %v.", end_min)
				logging.Errorf("Compaction setting misconfigured.  Invalid end min %v.", end_min)
				return false
			}

			start_min += start_hr * 60
			end_min += end_hr * 60

			// Instead of using current time, use the time when
			// compaction check starts.
			hr, min, _ := checkTime.Clock()
			min += hr * 60

			// if not yet past start time, no compaction.
			if start_min > min {
				isCompactionInterval = false
			}

			// if past start time, check end time
			if isCompactionInterval {

				// If user wants to abort past end date, check if already past end time.
				abort := config["abort_exceed_interval"].Bool()

				// if past 24 hours, then stop this run.
				if abort && time.Now().After(abortTime) {
					return false
				}

				if abort && end_min != 0 {
					now_hr, now_min, _ := time.Now().Clock()
					now_min += now_hr * 60

					// At this point, we know we have past start time.
					// If end time is next day from current time, add minutes
					// for the remaining of today.  To know if end time is next day
					// from current time, current time is larger than start time.
					if start_min > end_min && now_min > start_min {
						end_min += 24 * 60
					}

					if now_min > end_min {
						isCompactionInterval = false
					}

				} else if abort {
					// if abort specified, but no end time.
					logging.Errorf("CompactionDaemon: Compaction setting misconfigured.  " +
						"End time is not specified while allowing compaction to abort.")
				} else if end_min != 0 {
					// if end_time specified, but no abort.
					logging.Errorf("CompactionDaemon: Compaction setting misconfigured.  " +
						"End time is specified while not allowing compaction to abort.")
				}
			}
		}

		if !isCompactionInterval {
			logging.Infof("CompactionDaemon: Compaction attempt skipped since compaction interval is configured for %v", interval)
			return false
		}

		hasDaysOfWeek := false
		days := config["days_of_week"].Strings()
		today := strings.ToLower(checkTime.Weekday().String())
		for _, day := range days {
			if strings.ToLower(strings.TrimSpace(day)) == today {
				return true
			}
			hasDaysOfWeek = true
		}

		if hasDaysOfWeek {
			logging.Infof("CompactionDaemon: Compaction attempt skipped since compaction day is configured for %v", days)
		}
	}

	return false
}

func (cd *compactionDaemon) compactFDB(hasStartedToday bool) bool {

	var stats []IndexStorageStats

	conf := cd.config.Load()

	replych := make(chan []IndexStorageStats)
	statReq := &MsgIndexStorageStats{respch: replych}
	cd.msgch <- statReq
	stats = <-replych

	// each compaction interval cannot go over 24 hours if specified.
	abortTime := time.Now().Add(time.Duration(24) * time.Hour)
	checkTime := time.Now()

	mode := strings.ToLower(conf["compaction_mode"].String())
	if mode == "circular" {
		// if circular compaction, run full compaction at most once a day.
		if atomic.LoadInt32(&cd.lastCheckDay) != int32(checkTime.Weekday()) {
			hasStartedToday = false
			atomic.StoreInt32(&cd.lastCheckDay, int32(checkTime.Weekday()))
		}

		if hasStartedToday {
			return true
		}
	}

	for _, is := range stats {
		conf = cd.config.Load() // refresh to get up-to-date settings
		needUpgrade := is.Stats.NeedUpgrade
		if needUpgrade || cd.needsCompaction(is, conf, checkTime, abortTime) {
			hasStartedToday = true

			errch := make(chan error)
			compactReq := &MsgIndexCompact{
				instId:    is.InstId,
				partnId:   is.PartnId,
				errch:     errch,
				abortTime: abortTime,
			}
			logging.Infof("CompactionDaemon: Compacting index instance:%v", is.InstId)
			if needUpgrade {
				common.Console(cd.clusterAddr, "Compacting index %v.%v for upgrade", is.Bucket, is.Name)
			}
			cd.msgch <- compactReq
			err := <-errch
			if err == nil {
				logging.Infof("CompactionDaemon: Finished compacting index instance:%v", is.InstId)
				if needUpgrade {
					common.Console(cd.clusterAddr, "Finished compacting index %v.%v for upgrade", is.Bucket, is.Name)
				}
			} else {
				logging.Errorf("CompactionDaemon: Index instance:%v Compaction failed with reason - %v", is.InstId, err)
				if needUpgrade {
					common.Console(cd.clusterAddr, "Compaction for index %v.%v failed with reason - %v", is.Bucket, is.Name, err)
				}
			}
		}
	}

	return hasStartedToday
}

//////////////////////////////////////////////////////////////////
// Compact plasma
//////////////////////////////////////////////////////////////////

type compactionHistory []*indexCompaction

func (c compactionHistory) Len() int           { return len(c) }
func (c compactionHistory) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c compactionHistory) Less(i, j int) bool { return c[i].endTime < c[j].endTime }

func (cd *compactionDaemon) compactPlasma() {

	// There is no stats.  Skip compaction
	if cd.stats.Get() == nil {
		logging.Debugf("CompactionDaemon: No index stats. Skip compaction")
		return
	}

	// There is no index.  Skip compaction.
	if len(cd.indexInstMap) == 0 {
		logging.Debugf("CompactionDaemon: No index. Skip compaction")
		return
	}

	config := cd.config.Load()
	if !config["plasma.manual"].Bool() {
		logging.Debugf("CompactionDaemon: Plasma manual compaction is off. Skip compaction")
		return
	}

	cd.mutex.Lock()
	defer cd.mutex.Unlock()

	msgs := cd.addMandatory()
	msgs = append(msgs, cd.addOptional()...)
	for _, msg := range msgs {
		go cd.runCompaction(msg)
	}
}

//
// Add plasma instance (index partition) for mandatory compaction.
// Plasma instance is mandatory for compaction if fragmentation is over min fragmentation threshold.
// Plasma throttle write throughput if fragmentation is over max limit.   Therefore, it
// is important to compact as soon as the index cross the min_frag threshold, so the log cleaner
// has chance to catch up with the mutation rate.
//
// Note that fragmentation ratio does not indicate the time it will take for log cleaning
// to finish.  An large partition with low fragmentation may have more garbage than a small
// partition with high frag ratio.   Since IO bandwdith is fixed, it will take more take
// to compact the large partition.
//
func (cd *compactionDaemon) addMandatory() []*MsgIndexCompact {

	var compactMsgs []*MsgIndexCompact

	config := cd.config.Load()
	threshold := config["min_frag"].Int()
	stats := cd.stats.Get()

	for _, inst := range cd.indexInstMap {
		for _, partn := range inst.Pc.GetAllPartitions() {
			partnStats := stats.GetPartitionStats(inst.InstId, partn.GetPartitionId())

			// if index frag threshold is over limit
			if partnStats != nil &&
				partnStats.fragPercent.Value() > int64(0) &&
				partnStats.fragPercent.Value() > int64(threshold) &&
				partnStats.diskSize.Value() > PLASMA_CLEANER_MIN_SIZE {

				// if index is not running compaction, add it now.
				if cd.addIndexCompactionNoLock(inst.InstId, partn.GetPartitionId(), partnStats) {
					logging.Infof("CompactionDaemon: mandatory compaction: inst %v partition %v fragmentation %v over threshod %v.",
						inst.InstId, partn.GetPartitionId(), partnStats.fragPercent.Value(), threshold)
					compactMsgs = append(compactMsgs, newMsgIndexCompact(inst.InstId, partn.GetPartitionId(), threshold))
				}
			}
		}
	}

	return compactMsgs
}

//
// Add index for optional compaction.   Optional compaction is for plasma instances (partitions) that are below
// min_frag threshold.   If the number of ongoing compactions is below a quota (compaction.plasma.optional.quota),
// compaction manager will add additional discretionary partition for compaction.  The discrentionary partition
// are based on a round-robin basis.  The idea is to allow partitions to be always compacted, with the goal of
// keeping them under mandatory min_frag threshold.  Since this is done in a continous, rotating basis, it
// has the tendency to spread out log cleanning IO bandwdith consumption.
//
// Given the same fragmentation ratio, let assume each partition consumes the same IO bandwith per sec for log cleaning.
// Let denote B as IO bandwidth consumed by log cleaner per sec per partition.    The total bandwith per sec would be B * N,
// where N is the number of partitions for optional compaction.   B * N would remain fairly constant when N is constant.  A
// larger partition would take more time to compact, but this does not affect the total bandwith per sec.
//
// By keeping N as a fraction of total number of partitions, this can also keep total IO consumption
// from growing proportionally with the number of partitions.
//
// Large partition with low fragmentation ratio may take up more disk space than small partition.
// The partition index may seldom gets compacted under mandatory compaction.   Optional compaction will
// allow those large partitions to have a chance to clean its garbage more often.
//
func (cd *compactionDaemon) addOptional() []*MsgIndexCompact {

	var compactMsgs []*MsgIndexCompact

	// Config paramaters:
	// 1) Quota - the number of index under compaction.  If the number is under quota,
	//    then daemon will add discretionary indexes for compaction.
	// 2) optional threshold - The min frag for the index to be considered for optional compaction.
	// 3) decrement - Percentage of fragementation to be reduced for optional compaction.
	//
	config := cd.config.Load()
	threshold := config["min_frag"].Int()
	optionalQuota := config["plasma.optional.quota"].Int()
	optionalThreshold := config["plasma.optional.min_frag"].Int()
	optionalDecr := config["plasma.optional.decrement"].Int()
	stats := cd.stats.Get()

	//
	// Calculate the number of indexes for optional compaction.
	//
	allowance := int(math.Ceil(float64(cd.numInstancesNoLock())*float64(optionalQuota)/100.0)) - cd.numCompactionsNoLock()
	if allowance <= 0 {
		return compactMsgs
	}

	//
	// Find the index eligible for optional compaction
	// 1) fragmentation > 0
	// 2) fragemention between optional threshold and mandatory threshold
	// 3) greater than min disk size (plasma requirements)
	// 4) compaction is not currently running for the index
	//
	sorted := make(compactionHistory, 0, len(cd.history))

	for _, hist := range cd.history {
		partnStats := stats.GetPartitionStats(hist.instId, hist.partitionId)

		if partnStats != nil &&
			partnStats.fragPercent.Value() > int64(0) &&
			partnStats.fragPercent.Value() < int64(threshold) &&
			partnStats.fragPercent.Value() > int64(optionalThreshold) &&
			partnStats.fragPercent.Value()-int64(optionalDecr) > int64(0) &&
			partnStats.diskSize.Value() > PLASMA_CLEANER_MIN_SIZE {

			// if index is not running compaction, add it now.
			if !cd.isIndexCompactingNoLock(hist.instId, hist.partitionId) {
				sorted = append(sorted, hist)
			}
		}
	}

	// Sort the index based on the last time if finish compaction (endTime)
	// Pick the indexes based on the number of optional compaction allowed
	sort.Sort(sorted)
	if len(sorted) > allowance {
		sorted = sorted[:allowance]
	}

	//
	// Add the index for optional compaction
	//
	for _, hist := range sorted {
		partnStats := stats.GetPartitionStats(hist.instId, hist.partitionId)
		if cd.addIndexCompactionNoLock(hist.instId, hist.partitionId, partnStats) {

			// The target fragmentation is lower of
			// 1) current fragment - decrement
			// 2) optional compaction threshold
			target := int(partnStats.fragPercent.Value()) - optionalDecr
			if optionalThreshold < target {
				target = optionalThreshold
			}

			logging.Infof("CompactionDaemon: optional compaction: inst %v partition %v fragmentation %v target %v.",
				hist.instId, hist.partitionId, partnStats.fragPercent.Value(), target)

			compactMsgs = append(compactMsgs, newMsgIndexCompact(hist.instId, hist.partitionId, target))
		}
	}

	return compactMsgs
}

func (cd *compactionDaemon) runCompaction(compactReq *MsgIndexCompact) {

	logging.Infof("CompactionDaemon: run compaction for inst %v partition %v.",
		compactReq.GetInstId(), compactReq.GetPartitionId())

	cd.updateCompactionStartTime(compactReq.GetInstId(), compactReq.GetPartitionId(), time.Now().UnixNano())

	cd.msgch <- compactReq
	err := <-compactReq.GetErrorChannel()

	if err != nil {
		logging.Errorf("CompactionDaemon: Fail to run compaction for inst %v partition %v. Error=%v",
			compactReq.GetInstId(), compactReq.GetPartitionId(), err)
	}

	if cd.removeIndexCompaction(compactReq.GetInstId(), compactReq.GetPartitionId()) {
		logging.Infof("CompactionDaemon: compaction done for inst %v partition %v.",
			compactReq.GetInstId(), compactReq.GetPartitionId())
	}

	cd.updateCompactionEndTime(compactReq.GetInstId(), compactReq.GetPartitionId(), time.Now().UnixNano())
}

func (cd *compactionDaemon) updateIndexInstMap(indexInstMap common.IndexInstMap) {
	cd.mutex.Lock()
	defer cd.mutex.Unlock()

	compactions := make(map[string]*indexCompaction)
	history := make(map[string]*indexCompaction)

	for instId, inst := range indexInstMap {
		for _, partn := range inst.Pc.GetAllPartitions() {
			name := indexCompactionName(instId, partn.GetPartitionId())

			// prune running compaction from non-existent index
			if compaction, ok := cd.compactions[name]; ok {
				compactions[name] = compaction
			}

			// prune compaction history from non-existent index
			if hist, ok := cd.history[name]; ok {
				history[name] = hist
			} else {
				// add compaction history for new index
				history[name] = &indexCompaction{
					instId:      inst.InstId,
					partitionId: partn.GetPartitionId(),
					endTime:     math.MaxInt64,
				}
			}

		}
	}

	cd.indexInstMap = indexInstMap
	cd.compactions = compactions
	cd.history = history
}

func (cd *compactionDaemon) numInstancesNoLock() int {

	count := 0
	instMap := cd.indexInstMap
	for _, inst := range instMap {
		count += len(inst.Pc.GetAllPartitions())
	}
	return count
}

func (cd *compactionDaemon) removeIndexCompaction(instId common.IndexInstId, partitionId common.PartitionId) bool {
	cd.mutex.Lock()
	defer cd.mutex.Unlock()

	instName := indexCompactionName(instId, partitionId)
	if _, ok := cd.compactions[instName]; ok {
		delete(cd.compactions, instName)
		return true
	}
	return false
}

func (cd *compactionDaemon) addIndexCompactionNoLock(instId common.IndexInstId, partitionId common.PartitionId, stats *IndexStats) bool {
	instName := indexCompactionName(instId, partitionId)
	if _, ok := cd.compactions[instName]; !ok {
		cd.compactions[instName] = newIndexCompaction(instId, partitionId, stats)
		return true
	}
	return false
}

func (cd *compactionDaemon) isIndexCompactingNoLock(instId common.IndexInstId, partitionId common.PartitionId) bool {
	instName := indexCompactionName(instId, partitionId)
	_, ok := cd.compactions[instName]
	return ok
}

func (cd *compactionDaemon) numCompactionsNoLock() int {
	return len(cd.compactions)
}

func (cd *compactionDaemon) updateCompactionStartTime(instId common.IndexInstId, partitionId common.PartitionId, startTime int64) {
	cd.mutex.Lock()
	defer cd.mutex.Unlock()

	instName := indexCompactionName(instId, partitionId)
	if hist, ok := cd.history[instName]; ok {
		hist.startTime = startTime
	}
}

func (cd *compactionDaemon) updateCompactionEndTime(instId common.IndexInstId, partitionId common.PartitionId, endTime int64) {
	cd.mutex.Lock()
	defer cd.mutex.Unlock()

	instName := indexCompactionName(instId, partitionId)
	if hist, ok := cd.history[instName]; ok {
		hist.endTime = endTime
	}
}

func newMsgIndexCompact(instId common.IndexInstId, partnId common.PartitionId, minFrag int) *MsgIndexCompact {

	return &MsgIndexCompact{
		instId:  instId,
		partnId: partnId,
		errch:   make(chan error),
		minFrag: minFrag,
	}
}

func newIndexCompaction(instId common.IndexInstId, partnId common.PartitionId, stats *IndexStats) *indexCompaction {

	compact := &indexCompaction{
		instId:      instId,
		partitionId: partnId,
	}

	return compact
}

func indexCompactionName(instId common.IndexInstId, partitionId common.PartitionId) string {
	return fmt.Sprint("%v:%v", instId, partitionId)
}

func computeGarbage(stats *IndexStats) int64 {

	if stats.diskSize.Value() > 0 && stats.dataSize.Value() > 0 && stats.dataSize.Value() < stats.diskSize.Value() {
		return stats.diskSize.Value() - stats.dataSize.Value()
	}

	return 0
}

//////////////////////////////////////////////////////////////////
// CompactionManager
//////////////////////////////////////////////////////////////////

func NewCompactionManager(supvCmdCh MsgChannel, supvMsgCh MsgChannel,
	config common.Config) (CompactionManager, Message) {
	cm := &compactionManager{
		config:    config,
		supvCmdCh: supvCmdCh,
		supvMsgCh: supvMsgCh,
		logPrefix: "CompactionManager",
	}
	go cm.run()
	return cm, &MsgSuccess{}
}

func (cm *compactionManager) run() {
	cd := cm.newCompactionDaemon()
	cd.Start()
loop:
	for {
		select {
		case cmd, ok := <-cm.supvCmdCh:
			if ok {
				if cmd.GetMsgType() == COMPACTION_MGR_SHUTDOWN {
					logging.Infof("%v: Shutting Down", cm.logPrefix)
					cm.supvCmdCh <- &MsgSuccess{}
					break loop
				} else if cmd.GetMsgType() == CONFIG_SETTINGS_UPDATE {
					logging.Infof("%v: Refreshing settings", cm.logPrefix)
					cfgUpdate := cmd.(*MsgConfigUpdate)
					fullConfig := cfgUpdate.GetConfig()
					cfg := fullConfig.SectionConfig("settings.compaction.", true)
					cd.ResetConfig(cfg)
					cm.supvCmdCh <- &MsgSuccess{}
				} else if cmd.GetMsgType() == UPDATE_INDEX_INSTANCE_MAP {
					// Disable compaction manager processing for MOI storage
					if common.GetStorageMode() == common.MOI {
						cm.supvCmdCh <- &MsgSuccess{}
						continue
					}

					stats := cmd.(*MsgUpdateInstMap).GetStatsObject()
					indexInstMap := cmd.(*MsgUpdateInstMap).GetIndexInstMap()
					clone := common.CopyIndexInstMap(indexInstMap)
					cm.supvCmdCh <- &MsgSuccess{}

					cm.handleIndexMap(stats, clone, cd)
				} else {
					cm.supvCmdCh <- &MsgSuccess{}
				}
			} else {
				break loop
			}
		}
	}

	cd.Stop()
}

func (cm *compactionManager) newCompactionDaemon() *compactionDaemon {
	cfg := cm.config.SectionConfig("settings.compaction.", true)
	clusterAddr := cm.config["clusterAddr"].String()
	cd := &compactionDaemon{
		quitch:       make(chan bool),
		started:      false,
		msgch:        cm.supvMsgCh,
		clusterAddr:  clusterAddr,
		lastCheckDay: -1,
		compactions:  make(map[string]*indexCompaction),
		history:      make(map[string]*indexCompaction),
	}
	cd.config.Store(cfg)

	return cd
}

func (cm *compactionManager) handleIndexMap(stats *IndexerStats, indexInstMap common.IndexInstMap, cd *compactionDaemon) {
	if stats != nil && cd != nil {
		cd.stats.Set(stats)
		cd.updateIndexInstMap(indexInstMap)
	}
}
