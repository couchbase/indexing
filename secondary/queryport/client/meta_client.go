package client

import "sync"
import "fmt"
import "errors"
import "strings"
import "math/rand"
import "time"
import "unsafe"
import "sync/atomic"
import "encoding/json"
import "net/http"
import "bytes"
import "io/ioutil"
import "io"
import "math"
import "strconv"
import "sort"

import "github.com/couchbase/indexing/secondary/logging"
import "github.com/couchbase/indexing/secondary/security"
import common "github.com/couchbase/indexing/secondary/common"
import mclient "github.com/couchbase/indexing/secondary/manager/client"

type metadataClient struct {
	cluster  string
	finch    chan bool
	mdClient *mclient.MetadataProvider
	indexers unsafe.Pointer // *indexTopology
	// config
	servicesNotifierRetryTm int
	logtick                 time.Duration
	randomWeight            float64 // value between [0, 1.0)
	equivalenceFactor       float64 // value between [0, 1.0)

	topoChangeLock sync.Mutex
	metaCh         chan bool
	mdNotifyCh     chan bool
	stNotifyCh     chan map[common.IndexInstId]map[common.PartitionId]common.Statistics

	settings *ClientSettings

	refreshLock    sync.Mutex
	refreshCond    *sync.Cond
	refreshCnt     int
	refreshWaitCnt int
}

// sherlock topology management, multi-node & single-partition.
type indexTopology struct {
	version     uint64
	adminports  map[string]common.IndexerId // book-keeping for cluster changes
	topology    map[common.IndexerId][]*mclient.IndexMetadata
	queryports  map[common.IndexerId]string
	replicas    map[common.IndexDefnId][]common.IndexInstId
	equivalents map[common.IndexDefnId][]common.IndexDefnId
	partitions  map[common.IndexDefnId]map[common.PartitionId][]common.IndexInstId
	rw          sync.RWMutex
	loads       map[common.IndexInstId]*loadHeuristics
	// insts could include pending RState inst if there is no corresponding active instance
	insts      map[common.IndexInstId]*mclient.InstanceDefn
	rebalInsts map[common.IndexInstId]*mclient.InstanceDefn
	defns      map[common.IndexDefnId]*mclient.IndexMetadata
	allIndexes []*mclient.IndexMetadata
}

func newMetaBridgeClient(
	cluster string, config common.Config, metaCh chan bool, settings *ClientSettings) (c *metadataClient, err error) {

	b := &metadataClient{
		cluster:    cluster,
		finch:      make(chan bool),
		metaCh:     metaCh,
		mdNotifyCh: make(chan bool, 1),
		stNotifyCh: make(chan map[common.IndexInstId]map[common.PartitionId]common.Statistics, 1),
		settings:   settings,
	}
	b.refreshCond = sync.NewCond(&b.refreshLock)
	b.refreshCnt = 0

	b.servicesNotifierRetryTm = config["servicesNotifierRetryTm"].Int()
	b.logtick = time.Duration(config["logtick"].Int()) * time.Millisecond
	b.randomWeight = config["load.randomWeight"].Float64()
	b.equivalenceFactor = config["load.equivalenceFactor"].Float64()
	// initialize meta-data-provide.
	uuid, err := common.NewUUID()
	if err != nil {
		logging.Errorf("Could not generate UUID in common.NewUUID\n")
		return nil, err
	}
	b.mdClient, err = mclient.NewMetadataProvider(cluster, uuid.Str(), b.mdNotifyCh, b.stNotifyCh, b.settings)
	if err != nil {
		return nil, err
	}

	if err := b.updateIndexerList(false); err != nil {
		logging.Errorf("updateIndexerList(): %v\n", err)
		b.mdClient.Close()
		return nil, err
	}

	go b.watchClusterChanges() // will also update the indexer list
	go b.logstats()
	return b, nil
}

// Sync will update the indexer list.
func (b *metadataClient) Sync() error {
	if err := b.updateIndexerList(true); err != nil {
		logging.Errorf("updateIndexerList(): %v\n", err)
		return err
	}
	return nil
}

// Refresh implement BridgeAccessor{} interface.
func (b *metadataClient) Refresh() ([]*mclient.IndexMetadata, uint64, uint64, error) {

	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	if currmeta.version < b.mdClient.GetMetadataVersion() {
		b.refreshLock.Lock()

		if b.refreshCnt > 0 {
			b.refreshWaitCnt++
			logging.Debugf("Refresh(): wait metadata update.  Refresh Count %v Wait Count %v", b.refreshCnt, b.refreshWaitCnt)
			b.refreshCond.Wait()
			b.refreshWaitCnt--
			b.refreshLock.Unlock()
		} else {
			logging.Debugf("Refresh(): refresh metadata. Refresh Count %v Wait Count %v", b.refreshCnt, b.refreshWaitCnt)
			b.refreshCnt++
			b.refreshLock.Unlock()

			b.safeupdate(nil, false)

			b.refreshLock.Lock()
			b.refreshCnt--
			b.refreshCond.Broadcast()
			b.refreshLock.Unlock()
		}

		currmeta = (*indexTopology)(atomic.LoadPointer(&b.indexers))
	}

	return currmeta.allIndexes, currmeta.version, b.mdClient.GetClusterVersion(), nil
}

// Nodes implement BridgeAccessor{} interface.
func (b *metadataClient) Nodes() ([]*IndexerService, error) {
	// gather Indexer services
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	nodes := make(map[string]*IndexerService)
	for indexerID := range currmeta.topology {
		if indexerID != common.INDEXER_ID_NIL {
			a, q, h, err := b.mdClient.FindServiceForIndexer(indexerID)
			if err == nil {
				nodes[a] = &IndexerService{
					Adminport: a, Httpport: h, Queryport: q, Status: "initial",
				}
			}
		}
	}
	// gather indexer status
	for _, indexer := range b.mdClient.CheckIndexerStatus() {
		if node, ok := nodes[indexer.Adminport]; ok && indexer.Connected {
			node.Status = "online"
		}
	}
	services := make([]*IndexerService, 0, len(nodes))
	for _, node := range nodes {
		services = append(services, node)
	}
	return services, nil
}

// GetIndexDefn implements BridgeAccessor{} interface.
func (b *metadataClient) GetIndexDefn(defnID uint64) *common.IndexDefn {
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	if index, ok := currmeta.defns[common.IndexDefnId(defnID)]; ok {
		return index.Definition
	}

	return nil
}

// GetIndexInst implements BridgeAccessor{} interface.
func (b *metadataClient) GetIndexInst(instId uint64) *mclient.InstanceDefn {
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	if inst, ok := currmeta.insts[common.IndexInstId(instId)]; ok {
		return inst
	}

	return nil
}

// GetIndexReplica implements BridgeAccessor{} interface.
func (b *metadataClient) GetIndexReplica(defnId uint64) []*mclient.InstanceDefn {
	var result []*mclient.InstanceDefn
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	for _, instId := range currmeta.replicas[common.IndexDefnId(defnId)] {
		if inst, ok := currmeta.insts[instId]; ok {
			result = append(result, inst)
		}
	}

	return result
}

// CreateIndex implements BridgeAccessor{} interface.
func (b *metadataClient) CreateIndex(
	indexName, bucket, using, exprType, whereExpr string,
	secExprs []string, desc []bool, isPrimary bool,
	scheme common.PartitionScheme, partitionKeys []string,
	planJSON []byte) (uint64, error) {

	plan := make(map[string]interface{})
	if planJSON != nil && len(planJSON) > 0 {
		err := json.Unmarshal(planJSON, &plan)
		if err != nil {
			return 0, err
		}
	}

	refreshCnt := 0
RETRY:
	defnID, err, needRefresh := b.mdClient.CreateIndexWithPlan(
		indexName, bucket, using, exprType, whereExpr,
		secExprs, desc, isPrimary, scheme, partitionKeys, plan)

	if needRefresh && refreshCnt == 0 {
		fmsg := "GsiClient: Indexer Node List is out-of-date.  Require refresh."
		logging.Debugf(fmsg)
		if err := b.updateIndexerList(false); err != nil {
			logging.Errorf("updateIndexerList(): %v\n", err)
			return uint64(defnID), err
		}
		refreshCnt++
		goto RETRY
	}
	return uint64(defnID), err
}

// BuildIndexes implements BridgeAccessor{} interface.
func (b *metadataClient) BuildIndexes(defnIDs []uint64) error {
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	for _, defnId := range defnIDs {
		if _, ok := currmeta.defns[common.IndexDefnId(defnId)]; !ok {
			return ErrorIndexNotFound
		}
	}

	ids := make([]common.IndexDefnId, len(defnIDs))
	for i, id := range defnIDs {
		ids[i] = common.IndexDefnId(id)
	}
	return b.mdClient.BuildIndexes(ids)
}

// MoveIndex implements BridgeAccessor{} interface.
func (b *metadataClient) MoveIndex(defnID uint64, planJSON map[string]interface{}) error {

	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	if _, ok := currmeta.defns[common.IndexDefnId(defnID)]; !ok {
		return ErrorIndexNotFound
	}

	var httpport string
	for indexerId, _ := range currmeta.topology {
		var err error
		if _, _, httpport, err = b.mdClient.FindServiceForIndexer(indexerId); err == nil {
			break
		}
	}

	if httpport == "" {
		return ErrorNoHost
	}

	timeout := time.Duration(0 * time.Second)

	idList := IndexIdList{DefnIds: []uint64{defnID}}
	ir := IndexRequest{IndexIds: idList, Plan: planJSON}
	body, err := json.Marshal(&ir)
	if err != nil {
		return err
	}

	bodybuf := bytes.NewBuffer(body)

	url := "/moveIndexInternal"
	resp, err := postWithAuth(httpport+url, "application/json", bodybuf, timeout)
	if err != nil {
		errStr := fmt.Sprintf("Error communicating with index node %v. Reason %v", httpport, err)
		return errors.New(errStr)
	}
	defer resp.Body.Close()

	response := new(IndexResponse)
	bytes, _ := ioutil.ReadAll(resp.Body)
	if err := json.Unmarshal(bytes, &response); err != nil {
		return err
	}
	if response.Code == RESP_ERROR {
		return errors.New(response.Error)
	}

	return nil
}

// AlterReplicaCount implements BridgeAccessor{} interface.
func (b *metadataClient) AlterReplicaCount(action string, defnID uint64, planJSON map[string]interface{}) error {
	return b.mdClient.AlterReplicaCount(action, common.IndexDefnId(defnID), planJSON)
}

// DropIndex implements BridgeAccessor{} interface.
func (b *metadataClient) DropIndex(defnID uint64) error {
	err := b.mdClient.DropIndex(common.IndexDefnId(defnID))
	if err == nil { // cleanup index local cache.
		b.safeupdate(nil, false /*force*/)
	}
	return err
}

// GetScanports implements BridgeAccessor{} interface.
func (b *metadataClient) GetScanports() (queryports []string) {
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	queryports = make([]string, 0)
	for _, queryport := range currmeta.queryports {
		queryports = append(queryports, queryport)
	}
	logging.Debugf("Scan ports %v for all indexes", queryports)
	return queryports
}

// GetScanport implements BridgeAccessor{} interface.
func (b *metadataClient) GetScanport(defnID uint64, excludes map[common.IndexDefnId]map[common.PartitionId]map[uint64]bool,
	skips map[common.IndexDefnId]bool) (qp []string,
	targetDefnID uint64, in []uint64, rt []int64, pid [][]common.PartitionId, numPartitions uint32, ok bool) {

	var insts map[common.PartitionId]*mclient.InstanceDefn
	var rollbackTimes map[common.PartitionId]int64

	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	defnID = b.pickEquivalent(defnID, skips)
	if defnID == 0 {
		return nil, 0, nil, nil, nil, 0, false
	}

	var replicas [128]uint64
	n := 0
	for _, replicaID := range currmeta.replicas[common.IndexDefnId(defnID)] {
		replicas[n] = uint64(replicaID)
		n++
	}

	insts, rollbackTimes, ok = b.pickRandom(replicas[:n], defnID, excludes[common.IndexDefnId(defnID)])
	if !ok {
		if len(currmeta.equivalents[common.IndexDefnId(defnID)]) > 1 || len(currmeta.replicas[common.IndexDefnId(defnID)]) > 1 {
			// skip this index definition for retry only if there is equivalent index or replica
			skips[common.IndexDefnId(defnID)] = true
		}
		return nil, 0, nil, nil, nil, 0, false
	}

	targetDefnID = uint64(defnID)
	numPartitions = uint32(len(insts))

	qpm := make(map[common.IndexerId]map[common.IndexInstId][]common.PartitionId)

	for partnId, inst := range insts {
		indexerId, ok := inst.IndexerId[partnId]
		if !ok {
			return nil, 0, nil, nil, nil, 0, false
		}

		if _, ok := qpm[indexerId]; !ok {
			qpm[indexerId] = make(map[common.IndexInstId][]common.PartitionId)
		}

		if _, ok := qpm[indexerId][inst.InstId]; !ok {
			qpm[indexerId][inst.InstId] = make([]common.PartitionId, 0, numPartitions)
		}

		qpm[indexerId][inst.InstId] = append(qpm[indexerId][inst.InstId], partnId)
	}

	for indexerId, instIds := range qpm {
		for instId, partnIds := range instIds {

			// queryport
			q, ok := currmeta.queryports[indexerId]
			if !ok {
				return nil, 0, nil, nil, nil, 0, false
			}
			qp = append(qp, q)

			// rollback time
			t, ok := rollbackTimes[partnIds[0]]
			if !ok {
				return nil, 0, nil, nil, nil, 0, false
			}
			rt = append(rt, t)

			// instance id
			in = append(in, uint64(instId))

			// partitions
			pid = append(pid, partnIds)
		}
	}

	fmsg := "Scan port %s for index defnID %d of equivalent index defnId %d"
	logging.Debugf(fmsg, qp, targetDefnID, defnID)
	return qp, targetDefnID, in, rt, pid, numPartitions, true
}

// Timeit implement BridgeAccessor{} interface.
func (b *metadataClient) Timeit(instID uint64, partitionId common.PartitionId, value float64) {

	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	// currmeta.loads is immutable once constructed
	load, ok := currmeta.loads[common.IndexInstId(instID)]
	if !ok {
		// it should not happen. But if it does, just return.
		return
	}

	load.updateLoad(partitionId, value)
	load.incHit(partitionId)
}

// IsPrimary implement BridgeAccessor{} interface.
func (b *metadataClient) IsPrimary(defnID uint64) bool {
	b.Refresh()
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	if index, ok := currmeta.defns[common.IndexDefnId(defnID)]; ok {
		return index.Definition.IsPrimary
	}
	return false
}

// NumReplica implement BridgeAccessor{} interface.
func (b *metadataClient) NumReplica(defnID uint64) int {
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	return len(currmeta.replicas[common.IndexDefnId(defnID)])
}

// IndexState implement BridgeAccessor{} interface.
func (b *metadataClient) IndexState(defnID uint64) (common.IndexState, error) {
	b.Refresh()
	return b.indexState(defnID)
}

// close this bridge, to be called when a new indexer is added or
// an active indexer leaves the cluster or during system shutdown.
func (b *metadataClient) Close() {
	defer func() { recover() }() // in case async Close is called.
	b.mdClient.Close()
	close(b.finch)
}

//--------------------------------
// local functions to map replicas
//--------------------------------

// compute a map of replicas for each index in 2i.
func (b *metadataClient) computeReplicas(topo map[common.IndexerId][]*mclient.IndexMetadata) (map[common.IndexDefnId][]common.IndexInstId,
	map[common.IndexDefnId]map[common.PartitionId][]common.IndexInstId) {

	replicaMap := make(map[common.IndexDefnId][]common.IndexInstId)
	partitionMap := make(map[common.IndexDefnId]map[common.PartitionId][]common.IndexInstId)

	for _, indexes := range topo { // go through the indexes for each indexer
		for _, index := range indexes {

			if _, ok := replicaMap[index.Definition.DefnId]; ok {
				continue
			}

			replicaMap[index.Definition.DefnId] = make([]common.IndexInstId, 0, len(index.Instances))
			for _, inst := range index.Instances {
				replicaMap[index.Definition.DefnId] = append(replicaMap[index.Definition.DefnId], inst.InstId)
			}

			partitionMap[index.Definition.DefnId] = make(map[common.PartitionId][]common.IndexInstId)
			for _, inst := range index.Instances {
				for partnId, _ := range inst.IndexerId {
					if _, ok := partitionMap[index.Definition.DefnId][partnId]; !ok {
						partitionMap[index.Definition.DefnId][partnId] = make([]common.IndexInstId, 0, len(index.Instances))
					}
					partitionMap[index.Definition.DefnId][partnId] = append(partitionMap[index.Definition.DefnId][partnId], inst.InstId)
				}
			}
		}
	}
	return replicaMap, partitionMap
}

// compute a map of eqivalent indexes for each index in 2i.
func (b *metadataClient) computeEquivalents(topo map[common.IndexerId][]*mclient.IndexMetadata) map[common.IndexDefnId][]common.IndexDefnId {

	equivalentMap := make(map[common.IndexDefnId][]common.IndexDefnId)

	for _, indexes1 := range topo { // go through the indexes for each indexer
		for _, index1 := range indexes1 {

			if _, ok := equivalentMap[index1.Definition.DefnId]; ok { // skip replica
				continue
			}

			// add myself
			seen := make(map[common.IndexDefnId]bool)
			seen[index1.Definition.DefnId] = true
			equivalentMap[index1.Definition.DefnId] = []common.IndexDefnId{index1.Definition.DefnId}

			for _, indexes2 := range topo { // go through the indexes for each indexer

				for _, index2 := range indexes2 {
					if seen[index2.Definition.DefnId] {
						continue
					}
					seen[index2.Definition.DefnId] = true

					if b.equivalentIndex(index1, index2) { // pick equivalents
						equivalentMap[index1.Definition.DefnId] = append(equivalentMap[index1.Definition.DefnId], index2.Definition.DefnId)
					}
				}
			}
		}
	}

	return equivalentMap
}

// compare whether two index are equivalent.
func (b *metadataClient) equivalentIndex(
	index1, index2 *mclient.IndexMetadata) bool {
	d1, d2 := index1.Definition, index2.Definition
	if d1.Bucket != d2.Bucket ||
		d1.IsPrimary != d2.IsPrimary ||
		d1.ExprType != d2.ExprType ||
		d1.PartitionScheme != d2.PartitionScheme ||
		d1.HashScheme != d2.HashScheme ||
		d1.WhereExpr != d2.WhereExpr ||
		d1.RetainDeletedXATTR != d2.RetainDeletedXATTR {

		return false
	}

	if len(d1.SecExprs) != len(d2.SecExprs) {
		return false
	}

	for i, s1 := range d1.SecExprs {
		if s1 != d2.SecExprs[i] {
			return false
		}
	}

	if len(d1.PartitionKeys) != len(d2.PartitionKeys) {
		return false
	}

	for i, s1 := range d1.PartitionKeys {
		if s1 != d2.PartitionKeys[i] {
			return false
		}
	}

	if len(d1.Desc) != len(d2.Desc) {
		return false
	}

	for i, b1 := range d1.Desc {
		if b1 != d2.Desc[i] {
			return false
		}
	}

	return true
}

//--------------------------------------
// local functions for stats
//--------------------------------------

// manage load statistics.
type loadHeuristics struct {
	avgLoad       []uint64
	hit           []uint64
	numPartitions int
	stats         unsafe.Pointer
}

type loadStats struct {
	pending       map[common.PartitionId]int64
	rollbackTime  map[common.PartitionId]int64
	statsTime     map[common.PartitionId]int64
	staleCount    map[common.PartitionId]int64
	numPartitions int
}

func newLoadStats(numPartitions int) *loadStats {

	stats := &loadStats{
		pending:       make(map[common.PartitionId]int64), // initialize to maxInt64 -- no stats to compare
		rollbackTime:  make(map[common.PartitionId]int64), // initialize to 0 -- always allow scan
		statsTime:     make(map[common.PartitionId]int64), // time when stats is collected at indexer
		staleCount:    make(map[common.PartitionId]int64),
		numPartitions: numPartitions,
	}

	for i := 0; i < numPartitions+1; i++ {
		stats.pending[common.PartitionId(i)] = math.MaxInt64
	}

	return stats
}

func newLoadHeuristics(numPartitions int) *loadHeuristics {

	h := &loadHeuristics{
		avgLoad:       make([]uint64, numPartitions+1),
		hit:           make([]uint64, numPartitions+1),
		numPartitions: numPartitions,
		stats:         unsafe.Pointer(newLoadStats(numPartitions)),
	}

	for i := 0; i < numPartitions+1; i++ {
		h.avgLoad[i] = 0
		h.hit[i] = 0
	}

	return h
}

func (b *loadHeuristics) updateLoad(partitionId common.PartitionId, value float64) {

	avgLoadInt := atomic.LoadUint64(&b.avgLoad[int(partitionId)])
	avgLoad := math.Float64frombits(avgLoadInt)

	// compute incremental average.
	avgLoad = (avgLoad + float64(value)) / 2.0

	avgLoadInt = math.Float64bits(avgLoad)
	atomic.StoreUint64(&b.avgLoad[int(partitionId)], avgLoadInt)
}

func (b *loadHeuristics) getLoad(partitionId common.PartitionId) (float64, bool) {

	avgLoadInt := atomic.LoadUint64(&b.avgLoad[int(partitionId)])
	avgLoad := math.Float64frombits(avgLoadInt)

	return avgLoad, avgLoadInt != 0
}

func (b *loadHeuristics) getAvgLoad() float64 {

	avgLoad, ok := b.getLoad(common.PartitionId(0))
	if ok {
		return avgLoad
	}

	count := 0
	avgLoad = 0.0
	for i := 1; i < b.numPartitions+1; i++ {
		n, ok := b.getLoad(common.PartitionId(i))
		if ok {
			avgLoad += n
			count++
		}
	}

	if count == 0 {
		return 0.0
	}

	return avgLoad / float64(count)
}

func (b *loadHeuristics) incHit(partitionId common.PartitionId) {

	atomic.AddUint64(&b.hit[int(partitionId)], 1)
}

func (b *loadHeuristics) getHit(partitionId common.PartitionId) uint64 {

	return atomic.LoadUint64(&b.hit[int(partitionId)])
}

func (b *loadHeuristics) getAvgHit() uint64 {

	avgHit := b.getHit(common.PartitionId(0))
	if avgHit != 0 {
		return avgHit
	}

	avgHit = 0
	for i := 1; i < b.numPartitions+1; i++ {
		avgHit += b.getHit(common.PartitionId(i))
	}

	return avgHit / uint64(b.numPartitions)
}

func (b *loadHeuristics) updateStats(stats *loadStats) {

	atomic.StorePointer(&b.stats, unsafe.Pointer(stats))
}

func (b *loadHeuristics) getStats() *loadStats {

	return (*loadStats)(atomic.LoadPointer(&b.stats))
}

func (b *loadHeuristics) copyStats() *loadStats {

	stats := (*loadStats)(atomic.LoadPointer(&b.stats))
	newStats := newLoadStats(stats.numPartitions)

	for partnId, pending := range stats.pending {
		newStats.pending[partnId] = pending
	}

	for partnId, rollbackTime := range stats.rollbackTime {
		newStats.rollbackTime[partnId] = rollbackTime
	}

	for partnId, statsTime := range stats.statsTime {
		newStats.statsTime[partnId] = statsTime
	}

	for partnId, staleCount := range stats.staleCount {
		newStats.staleCount[partnId] = staleCount
	}

	return newStats
}

func (b *loadHeuristics) cloneRefresh(curInst *mclient.InstanceDefn, newInst *mclient.InstanceDefn) *loadHeuristics {

	clone := newLoadHeuristics(b.numPartitions)
	for partnId, _ := range newInst.IndexerId {
		if newInst.Versions[partnId] == curInst.Versions[partnId] {
			clone.avgLoad[uint64(partnId)] = atomic.LoadUint64(&b.avgLoad[int(partnId)])
			clone.hit[uint64(partnId)] = atomic.LoadUint64(&b.hit[int(partnId)])
		}
	}

	cloneStats := clone.getStats()
	stats := b.getStats()
	for partnId, _ := range newInst.IndexerId {
		if newInst.Versions[partnId] == curInst.Versions[partnId] {
			cloneStats.updatePendingItem(partnId, stats.getPendingItem(partnId))
			cloneStats.updateRollbackTime(partnId, stats.getRollbackTime(partnId))
			cloneStats.updateStatsTime(partnId, stats.statsTime[partnId])
		}
	}

	return clone
}

func (b *loadStats) getPendingItem(partitionId common.PartitionId) int64 {

	return b.pending[partitionId]
}

// getTotalPendingItems returns the total pending items across all partitions.
// Pending items are initialized to math.MaxInt64 to distinguish missing stats
// from actual zeros, so this returns math.MaxInt64 if any partition has that
// value to avoid returning garbage (e.g. a sum of math.MaxInt64s is negative).
func (b *loadStats) getTotalPendingItems() int64 {

	var total int64
	for _, pending := range b.pending {
		if pending == math.MaxInt64 {
			return math.MaxInt64
		}
		total += pending
	}

	return total
}

func (b *loadStats) updatePendingItem(partitionId common.PartitionId, value int64) {

	b.pending[partitionId] = value
}

func (b *loadStats) getRollbackTime(partitionId common.PartitionId) int64 {

	return b.rollbackTime[partitionId]
}

func (b *loadStats) updateRollbackTime(partitionId common.PartitionId, value int64) {

	b.rollbackTime[partitionId] = value
}

func (b *loadStats) updateStatsTime(partitionId common.PartitionId, value int64) {

	if b.statsTime[partitionId] != value {
		b.statsTime[partitionId] = value
		b.staleCount[partitionId] = 0
	} else {
		b.staleCount[partitionId]++
	}
}

func (b *loadStats) isAllStatsCurrent() bool {

	current := true
	for _, stale := range b.staleCount {
		current = current && stale < 10
	}

	return current
}

func (b *loadStats) isStatsCurrent(partitionId common.PartitionId) bool {

	return b.staleCount[partitionId] < 10
}

//-----------------------------------------------
// local functions to pick index for scanning
//-----------------------------------------------

func (b *metadataClient) pickEquivalent(defnID uint64, skips map[common.IndexDefnId]bool) uint64 {

	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	if len(skips) == len(currmeta.equivalents[common.IndexDefnId(defnID)]) {
		return uint64(0)
	}

	for {
		n := rand.Intn(len(currmeta.equivalents[common.IndexDefnId(defnID)]))
		candidate := currmeta.equivalents[common.IndexDefnId(defnID)][n]
		if !skips[candidate] {
			return uint64(candidate)
		}
	}
}

// Given the list of replicas for a given index definition, this function randomly picks the partitons from the available replicas
// for scanning.   This function will filter out any replica partition falls behind from other replicas.  It returns:
// 1) a map of partition Id and index instance
// 2) a map of partition Id and rollback timestamp
//
func (b *metadataClient) pickRandom(replicas []uint64, defnID uint64,
	excludes map[common.PartitionId]map[uint64]bool) (map[common.PartitionId]*mclient.InstanceDefn, map[common.PartitionId]int64, bool) {

	//
	// Determine number of partitions and its range
	//
	partitionRange := func(currmeta *indexTopology, defnID uint64, numPartition int) (uint64, uint64) {
		if defn, ok := currmeta.defns[common.IndexDefnId(defnID)]; ok {
			startPartnId := 0
			if common.IsPartitioned(defn.Definition.PartitionScheme) {
				startPartnId = 1
			}
			return uint64(startPartnId), uint64(startPartnId + numPartition)
		}
		// return 0 if cannot find range
		return 0, 0
	}

	numPartition := func(currmeta *indexTopology, replicas []uint64) uint32 {
		for _, instId := range replicas {
			if inst, ok := currmeta.insts[common.IndexInstId(instId)]; ok {
				return inst.NumPartitions
			}
		}
		// return 0 if cannot find numPartition
		return 0
	}

	numValidReplica := func(currmeta *indexTopology, partnId uint64, replicas []uint64, rollbackTimesList []map[common.PartitionId]int64) int {

		var count int
		for n, replica := range replicas {
			_, ok1 := currmeta.insts[common.IndexInstId(replica)]
			rollbackTime, ok2 := rollbackTimesList[n][common.PartitionId(partnId)]
			ok3 := ok2 && rollbackTime != math.MaxInt64
			if ok1 && ok2 && ok3 {
				count++
			}
		}
		return count
	}

	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	numPartn := numPartition(currmeta, replicas)
	startPartnId, endPartnId := partitionRange(currmeta, defnID, int(numPartn))

	//
	// Shuffle the replica list
	//
	shuffle := func(replicas []uint64) []uint64 {
		num := len(replicas)
		result := make([]uint64, num)

		for _, replica := range replicas {
			found := false
			for !found {
				n := rand.Intn(num)
				if result[n] == 0 {
					result[n] = replica
					found = true
				}
			}
		}
		return result
	}
	replicas = shuffle(replicas)

	//
	// Filter out inst based on pending item stats.
	//
	rollbackTimesList := b.pruneStaleReplica(replicas, excludes)

	// Filter based on timing of scan responses
	b.filterByTiming(currmeta, replicas, rollbackTimesList, startPartnId, endPartnId)

	//
	// Randomly select an inst after filtering
	//
	chosenInst := make(map[common.PartitionId]*mclient.InstanceDefn)
	chosenTimestamp := make(map[common.PartitionId]int64)

	for partnId := startPartnId; partnId < endPartnId; partnId++ {

		var ok bool
		var inst *mclient.InstanceDefn
		var rollbackTime int64

		for n, replica := range replicas {

			var ok1, ok2, ok3 bool
			inst, ok1 = currmeta.insts[common.IndexInstId(replica)]
			rollbackTime, ok2 = rollbackTimesList[n][common.PartitionId(partnId)]
			ok3 = ok2 && rollbackTime != math.MaxInt64
			ok = ok1 && ok2 && ok3

			if ok {
				break
			}
		}

		if ok {
			// find an indexer that holds the active partition
			chosenInst[common.PartitionId(partnId)] = inst
			chosenTimestamp[common.PartitionId(partnId)] = rollbackTime

			// set the rollback time to 0 if there is only one valid replica
			if numValidReplica(currmeta, partnId, replicas, rollbackTimesList) <= 1 {
				chosenTimestamp[common.PartitionId(partnId)] = 0
			}
		} else {
			// cannot find an indexer that holds an active partition
			// try to find an indexer under rebalancing
			for _, instId := range replicas {
				if inst, ok := currmeta.rebalInsts[common.IndexInstId(instId)]; ok {
					if _, ok := inst.IndexerId[common.PartitionId(partnId)]; ok {
						chosenInst[common.PartitionId(partnId)] = inst
						chosenTimestamp[common.PartitionId(partnId)] = 0
					}
				}
			}
		}
	}

	if len(chosenInst) != int(numPartn) {
		logging.Errorf("PickRandom: Fail to find indexer for all index partitions. Num partition %v.  Partition with instances %v ",
			numPartn, len(chosenInst))
		for n, instId := range replicas {
			for partnId := startPartnId; partnId < endPartnId; partnId++ {
				ts, ok := rollbackTimesList[n][common.PartitionId(partnId)]
				logging.Debugf("PickRandom: inst %v partition %v timestamp %v ok %v",
					instId, partnId, ts, ok)
			}
		}
		return nil, nil, false
	}

	return chosenInst, chosenTimestamp, true
}

func (b *metadataClient) filterByTiming(currmeta *indexTopology, replicas []uint64, rollbackTimes []map[common.PartitionId]int64,
	startPartnId uint64, endPartnId uint64) {

	numRollbackTimes := func(partnId uint64) int {
		count := 0
		for _, partnMap := range rollbackTimes {
			if _, ok := partnMap[common.PartitionId(partnId)]; ok {
				count++
			}
		}
		return count
	}

	if rand.Float64() >= b.randomWeight {
		for partnId := startPartnId; partnId < endPartnId; partnId++ {
			// Do not prune if there is only replica with this partition
			if numRollbackTimes(partnId) <= 1 {
				continue
			}

			loadList := make([]float64, len(replicas))
			for i, instId := range replicas {
				if load, ok := currmeta.loads[common.IndexInstId(instId)]; ok {
					if n, ok := load.getLoad(common.PartitionId(partnId)); ok {
						loadList[i] = n
					} else {
						loadList[i] = math.MaxFloat64
					}
				} else {
					loadList[i] = math.MaxFloat64
				}
			}

			// compute replica with least load.
			sort.Float64s(loadList)
			leastLoad := loadList[0]

			//
			// Filter inst based on load
			//
			for i, instId := range replicas {
				if load, ok := currmeta.loads[common.IndexInstId(instId)]; ok {
					if n, ok := load.getLoad(common.PartitionId(partnId)); ok {
						eqivLoad := n * b.equivalenceFactor
						if eqivLoad > leastLoad {
							logging.Verbosef("remove inst %v partition %v from scan due to slow response time (least %v load %v)",
								instId, partnId, leastLoad, eqivLoad)
							delete(rollbackTimes[i], common.PartitionId(partnId))
						}
					}
				}
			}
		}
	}
}

//
// This method prune stale partitions from the given replica.  For each replica, it returns
// the rollback time of up-to-date partition.  Staleness is based on the limit of how far
// the partition is fallen behind the most current partition.
//
// If the index inst does not exist for a replica, it returns an empty map.  Therefore,
// an empty map for a replica could mean index does not exist, or there is no up-to-date partition.
//
// If there is only replica or pruning is disable, it will return rollback time 0 for all
// replica without pruning.
//
// If there is no stats available for a particular partition (across all replicas), then
// no pruning for that partition.
//
func (b *metadataClient) pruneStaleReplica(replicas []uint64, excludes map[common.PartitionId]map[uint64]bool) []map[common.PartitionId]int64 {

	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	resetRollbackTimes := func(rollbackTimeList []map[common.PartitionId]int64) {
		for _, rollbackTimes := range rollbackTimeList {
			for partnId, _ := range rollbackTimes {
				rollbackTimes[partnId] = 0
			}
		}
	}

	isStalePartition := func(minPendings map[common.PartitionId]int64, partnId common.PartitionId) bool {
		if _, ok := minPendings[partnId]; !ok {
			return true
		}
		if uint64(minPendings[partnId]) >= uint64(math.MaxInt64) {
			return true
		}
		return false
	}

	hasStaleStats := func(minPendings map[common.PartitionId]int64) bool {
		for partnId, _ := range minPendings {
			if isStalePartition(minPendings, partnId) {
				return true
			}
		}
		return false
	}

	// If there is only one instance or disable replica pruning, then just return.
	if len(replicas) == 1 || b.settings.DisablePruneReplica() {
		_, rollbackTimes, _ := b.getPendingStats(replicas, currmeta, excludes, false)
		resetRollbackTimes(rollbackTimes)
		return rollbackTimes
	}

	// read the progress stats from each index -- exclude indexer that has not refreshed its stats
	pendings, rollbackTimes, minPendings := b.getPendingStats(replicas, currmeta, excludes, true)
	if hasStaleStats(minPendings) {
		// if there is no current progress stats available, then read stats even if it could be stale
		// This can happen if KV is partitioned away from all indexer nodes, since that indexer cannot
		// refresh progress stats
		pendings, rollbackTimes, minPendings = b.getPendingStats(replicas, currmeta, excludes, false)
	}

	result := make([]map[common.PartitionId]int64, len(replicas))
	for i, instId := range replicas {
		result[i] = make(map[common.PartitionId]int64)

		// find the index inst
		inst, ok := currmeta.insts[common.IndexInstId(instId)]
		if !ok {
			continue
		}

		// prune partition that exceed quota
		// If the instance's partition is excluded, then it will be be in the pendings map.  So it will be skipped, and
		// this method will not return a rollbackTime for this instance partition
		for partnId, pending := range pendings[i] {

			// If there is no progress stats available for any replica/partition, then do not filter.
			// Stats not available when
			// 1) All indexers have not yet been able to send stats to cbq after cluster restart
			// 2) index just finish building but not yet send stats over
			// 3) There is a single instance (no replica) and not available
			// 4) Replica/partiton has just been rebalanced but not yet send stats over
			if isStalePartition(minPendings, partnId) {
				result[i][partnId] = 0
				continue
			}

			percent := b.settings.ScanLagPercent()
			quota := int64(float64(minPendings[partnId]) * (percent + 1.0))

			// compute the quota per partition
			item := b.settings.ScanLagItem() / uint64(inst.NumPartitions)
			if quota < int64(item) {
				quota = int64(item)
			}

			if pending != int64(math.MaxInt64) && pending <= quota {
				result[i][partnId] = rollbackTimes[i][partnId]
			} else {
				logging.Verbosef("remove inst %v partition %v from scan due to stale item count", instId, partnId)
			}
		}
	}

	return result
}

//
// This method returns item pending and rollback time for each partition for every given replica.
// If replica does not exist, it returns a empty map for item pending and rollback time.
// If the partition/replica is excluded, then item pending and rollback time will be missing in the map.
// If stats is not available or stale, it returns MaxInt64 for item pending and rollback time.
// If replica/partition just being rebalanced, it returns MaxInt64 for item pending and 0 for rollback time (see updateTopology).
//
// This method also return the minPending for each partition across all replicas.
// If no replica has valid stat for that partition, minPending is MaxInt64.
// If all replica are excluded for that partition, minPending is also MaxInt64.
//
func (b *metadataClient) getPendingStats(replicas []uint64, currmeta *indexTopology, excludes map[common.PartitionId]map[uint64]bool,
	useCurrent bool) ([]map[common.PartitionId]int64, []map[common.PartitionId]int64,
	map[common.PartitionId]int64) {

	pendings := make([]map[common.PartitionId]int64, len(replicas))
	rollbackTimes := make([]map[common.PartitionId]int64, len(replicas))
	minPending := make(map[common.PartitionId]int64)
	init := make(map[common.PartitionId]bool)

	for i, instId := range replicas {
		rollbackTimes[i] = make(map[common.PartitionId]int64)
		pendings[i] = make(map[common.PartitionId]int64)

		// Get stats from active instance
		inst, ok := currmeta.insts[common.IndexInstId(instId)]
		if !ok {
			continue
		}

		var stats *loadStats
		if load, ok := currmeta.loads[common.IndexInstId(instId)]; ok {
			stats = load.getStats()
		}

		for partnId, _ := range inst.IndexerId {

			if !init[partnId] {
				// minPending can be MaxInt64 if
				// 1) instance partition is excluded
				// 2) there is no stats for the instance partition
				// 3) there is no current stats for the instance partition
				minPending[partnId] = math.MaxInt64
				init[partnId] = true
			}

			if excludes[partnId][instId] {
				continue
			}

			if stats == nil || (useCurrent && !stats.isStatsCurrent(partnId)) {
				pendings[i][partnId] = math.MaxInt64
				rollbackTimes[i][partnId] = math.MaxInt64
				continue
			}

			rollbackTimes[i][partnId] = stats.getRollbackTime(partnId)
			pendings[i][partnId] = stats.getPendingItem(partnId)

			if pendings[i][partnId] < minPending[partnId] {
				minPending[partnId] = pendings[i][partnId]
			}
		}
	}

	return pendings, rollbackTimes, minPending
}

//---------------------------
// local utility functions
//---------------------------

// logstats logs client stats every logtick milliseconds until told to stop.
func (b *metadataClient) logstats() {
	tick := time.NewTicker(b.logtick)
	defer func() {
		tick.Stop()
	}()

loop:
	for {
		<-tick.C

		b.printstats()

		select {
		case _, ok := <-b.finch:
			if !ok {
				break loop
			}
		default:
		}
	}
}

// printstats logs GSI query client stats (to indexer.log or query.log, depending
// on its execution context). Most of its logging is done at the Verbose level.
// Client pending item stats are initialized to math.MaxInt64 to distinguish missing
// stats from actual 0s (used to exclude stale partitions from scans); these will
// appear as 9223372036854775807 counts in the logs.
func (b *metadataClient) printstats() {

	s := make([]string, 0, 16)
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	logging.Infof("connected with %v indexers\n", len(currmeta.topology))
	for id, replicas := range currmeta.replicas {
		logging.Infof("index %v has replicas %v: \n", id, replicas)
	}
	for id, equivalents := range currmeta.equivalents {
		logging.Infof("index %v has equivalents %v: \n", id, equivalents)
	}
	// currmeta.loads is immutable
	for id, _ := range currmeta.insts {
		load := currmeta.loads[id]
		s = append(s, fmt.Sprintf(`"%v": %v`, id, load.getAvgLoad()))
	}
	logging.Infof("client load stats {%v}", strings.Join(s, ","))

	s = make([]string, 0, 16)
	for id, _ := range currmeta.insts {
		load := currmeta.loads[id]
		s = append(s, fmt.Sprintf(`"%v": %v`, id, load.getAvgHit()))
	}
	logging.Infof("client hit stats {%v}", strings.Join(s, ","))

	s = make([]string, 0, 16)
	for id, _ := range currmeta.insts {
		load := currmeta.loads[id]
		s = append(s, fmt.Sprintf(`"%v": %v`, id, load.getStats().getTotalPendingItems()))
	}
	logging.Infof("client pending item stats {%v}", strings.Join(s, ","))

	/*
		s = make([]string, 0, 16)
		for id, _ := range currmeta.insts {
			load := currmeta.loads[id]
			s = append(s, fmt.Sprintf(`"%v": %v`, id, load.getStats().getRollbackTime()))
		}
		logging.Infof("client rollback times {%v}", strings.Join(s, ","))
	*/

	s = make([]string, 0, 16)
	for id, _ := range currmeta.insts {
		load := currmeta.loads[id]
		s = append(s, fmt.Sprintf(`"%v": %v`, id, load.getStats().isAllStatsCurrent()))
	}
	logging.Infof("client stats current {%v}", strings.Join(s, ","))
}

// unprotected access to shared structures.
func (b *metadataClient) indexState(defnID uint64) (common.IndexState, error) {
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	if index, ok := currmeta.defns[common.IndexDefnId(defnID)]; ok {
		if index.Error != "" {
			return common.INDEX_STATE_ERROR, errors.New(index.Error)
		}

		return index.State, nil
	}

	return common.INDEX_STATE_ERROR, ErrorIndexNotFound
}

// unprotected access to shared structures.
func (b *metadataClient) indexInstState(instID uint64) (common.IndexState, error) {
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	if inst, ok := currmeta.insts[common.IndexInstId(instID)]; ok {
		if inst.Error != "" {
			return common.INDEX_STATE_ERROR, errors.New(inst.Error)
		}

		return inst.State, nil
	}

	return common.INDEX_STATE_ERROR, ErrorInstanceNotFound
}

//-----------------------------------------------
// local functions for metadata and stats update
//-----------------------------------------------

// update 2i cluster information,
func (b *metadataClient) updateIndexerList(discardExisting bool) error {

	clusterURL, err := common.ClusterAuthUrl(b.cluster)
	if err != nil {
		return err
	}
	cinfo, err := common.NewClusterInfoCache(clusterURL, "default")
	if err != nil {
		return err
	}
	cinfo.SetUserAgent("updateIndexerList")
	if err := cinfo.Fetch(); err != nil {
		return err
	}

	// UpdateIndexerList is synchronous, except for async callback from WatchMetadata() -- when indexer is
	// not responding fast enough.
	// TopoChangeLock is to protect the updates to index topology made by async callack.   Index topology contains
	// the assignment between admniport and indexerId -- which is udpated by async callback.   Metadata version cannot
	// enforce serializability of such assigment update, since it is not part of metadata tracked by metadataProvider.
	//
	// The adminport-indexerId assignment is used to figure out difference in topology so that gsiClient can call
	// WatchMetadata and UnwatchMetadata properly.   Corruption of such assignment can cause a lot of issue.
	//
	// The lock is to protect race condition in such case
	// 1) make sure that updateIndexerList() is finished before any its callback is invoked.
	//    This is to ensure async callback does not lose its work.
	// 2) make sure that the async callback is called sequentially so their changes on adminport-indexerId are accumulative.
	//    This is to ensure async callback does not lose its work.
	// 3) if there are consecutive topology changes by ns-server, make sure that async callback will not save a stale
	//    adminport-indexerId assignment by overwriting the assignment created by second topology changes.
	b.topoChangeLock.Lock()
	defer b.topoChangeLock.Unlock()

	// populate indexers' adminport and queryport
	adminports, activeNode, failedNode, unhealthyNode, newNode, err := getIndexerAdminports(cinfo)
	if err != nil {
		return err
	}
	b.mdClient.SetClusterStatus(activeNode, failedNode, unhealthyNode, newNode)

	fmsg := "Refreshing indexer list due to cluster changes or auto-refresh."
	logging.Infof(fmsg)
	logging.Infof("Refreshed Indexer List: %v", adminports)

	var curradmns map[string]common.IndexerId
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	if currmeta != nil {
		curradmns = make(map[string]common.IndexerId)
		for adminport, indexerId := range currmeta.adminports {
			curradmns[adminport] = indexerId
		}
	}

	if discardExisting {
		for _, indexerID := range curradmns {
			b.mdClient.UnwatchMetadata(indexerID, activeNode)
		}
		curradmns = make(map[string]common.IndexerId)
	}

	// watch all indexers
	m := make(map[string]common.IndexerId)
	for _, adminport := range adminports { // add new indexer-nodes if any
		if indexerID, ok := curradmns[adminport]; !ok {
			// This adminport is provided by cluster manager.  Meta client will
			// honor cluster manager to treat this adminport as a healthy node.
			// If the indexer is unavail during initialization, WatchMetadata()
			// will return afer timeout. A background watcher will keep
			// retrying, since it can be tranisent partitioning error.
			// If retry eventually successful, this callback will be invoked
			// to update meta_client. The metadata client has to rely on the
			// cluster manager to send a notification if this node is detected
			// to be down, such that the metadata client can stop the
			// background watcher.
			fn := func(ad string, n_id common.IndexerId, o_id common.IndexerId) {
				// following time.Sleep() ensures that bootstrap initialization
				// completes before other async activities into metadataClient{}
				currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
				for currmeta == nil {
					time.Sleep(10 * time.Millisecond)
					currmeta = (*indexTopology)(atomic.LoadPointer(&b.indexers))
				}
				b.updateIndexer(ad, n_id, o_id)
			}

			// WatchMetadata will "unwatch" an old metadata watcher which
			// shares the same indexer Id (but the adminport may be different).
			indexerID = b.mdClient.WatchMetadata(adminport, fn, activeNode)
			m[adminport] = indexerID
		} else {
			err = b.mdClient.UpdateServiceAddrForIndexer(indexerID, adminport)
			m[adminport] = indexerID
			delete(curradmns, adminport)
		}
	}
	// delete indexer-nodes that got removed from cluster.
	for _, indexerID := range curradmns {
		// check if the indexerId exists in var "m".  In case the
		// adminport changes for the same index node, there would
		// be two adminport mapping to the same indexerId, one
		// in b.adminport (old) and the other in "m" (new).  So
		// make sure not to accidently unwatch the indexer.
		found := false
		for _, id := range m {
			if indexerID == id {
				found = true
			}
		}
		if !found {
			b.mdClient.UnwatchMetadata(indexerID, activeNode)
		}
	}
	b.safeupdate(m, true /*force*/)
	return err
}

func (b *metadataClient) updateIndexer(
	adminport string, newIndexerId, oldIndexerId common.IndexerId) {

	func() {
		b.topoChangeLock.Lock()
		defer b.topoChangeLock.Unlock()

		adminports := make(map[string]common.IndexerId)
		currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
		for admnport, indexerId := range currmeta.adminports {
			adminports[admnport] = indexerId
		}

		// UpdateIndexer is a async call.  When this is invoked, currmeta.adminports may be different.
		if _, ok := adminports[adminport]; ok {
			logging.Infof(
				"Acknowledged that new indexer is registered.  Indexer = %v, id = %v",
				adminport, newIndexerId)
			adminports[adminport] = newIndexerId
			b.safeupdate(adminports, true /*force*/)
		} else {
			logging.Infof(
				"New indexer registration is skipped.  Indexer may have been rebalanced out (unwatch).  Indexer = %v, id = %v",
				adminport, newIndexerId)
		}
	}()
}

// replicas:
//		Refresh
//		deleteIndex
// loads:
//		Refresh
//		deleteIndex
// topology:
//		Refresh
//		deleteIndex
//
//		Timeit for b.loads

func (b *metadataClient) updateTopology(
	adminports map[string]common.IndexerId, force bool) *indexTopology {

	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	// Refresh indexer version
	if force {
		b.mdClient.RefreshIndexerVersion()
	}

	mindexes, version := b.mdClient.ListIndex()
	// detect change in indexer cluster or indexes.
	if force == false && currmeta != nil &&
		(!b.hasIndexersChanged(adminports) &&
			!b.hasIndexesChanged(mindexes, version)) {
		return currmeta
	}

	// create a new topology.
	newmeta := &indexTopology{
		version:     version,
		allIndexes:  mindexes,
		adminports:  make(map[string]common.IndexerId),
		topology:    make(map[common.IndexerId][]*mclient.IndexMetadata),
		replicas:    make(map[common.IndexDefnId][]common.IndexInstId),
		equivalents: make(map[common.IndexDefnId][]common.IndexDefnId),
		queryports:  make(map[common.IndexerId]string),
		insts:       make(map[common.IndexInstId]*mclient.InstanceDefn),
		rebalInsts:  make(map[common.IndexInstId]*mclient.InstanceDefn),
		defns:       make(map[common.IndexDefnId]*mclient.IndexMetadata),
	}

	// adminport/queryport
	for adminport, indexerID := range adminports {
		newmeta.adminports[adminport] = indexerID
		newmeta.topology[indexerID] = make([]*mclient.IndexMetadata, 0, 16)

		_, qp, _, err := b.mdClient.FindServiceForIndexer(indexerID)
		if err == nil {
			// This excludes watcher that is not currently connected
			newmeta.queryports[indexerID] = qp
		}
	}

	// insts/defns
	topologyMap := make(map[common.IndexerId]map[common.IndexDefnId]*mclient.IndexMetadata)
	for _, mindex := range mindexes {
		newmeta.defns[mindex.Definition.DefnId] = mindex

		for _, instance := range mindex.Instances {
			for _, indexerId := range instance.IndexerId {
				if _, ok := topologyMap[indexerId]; !ok {
					topologyMap[indexerId] = make(map[common.IndexDefnId]*mclient.IndexMetadata)
				}
				if _, ok := topologyMap[indexerId][instance.DefnId]; !ok {
					topologyMap[indexerId][instance.DefnId] = mindex
				}
			}
			newmeta.insts[instance.InstId] = instance
		}

		for _, instance := range mindex.InstsInRebalance {
			for _, indexerId := range instance.IndexerId {
				if _, ok := topologyMap[indexerId]; !ok {
					topologyMap[indexerId] = make(map[common.IndexDefnId]*mclient.IndexMetadata)
				}
				if _, ok := topologyMap[indexerId][instance.DefnId]; !ok {
					topologyMap[indexerId][instance.DefnId] = mindex
				}
			}
			newmeta.rebalInsts[instance.InstId] = instance
		}
	}

	// topology
	for indexerId, indexes := range topologyMap {
		for _, index := range indexes {
			indexes2, ok := newmeta.topology[indexerId]
			if !ok {
				fmsg := "indexer node %v not available"
				logging.Fatalf(fmsg, indexerId)
				continue
			}
			newmeta.topology[indexerId] = append(indexes2, index)
		}
	}

	// replicas/replicaInRebal
	newmeta.replicas, newmeta.partitions = b.computeReplicas(newmeta.topology)

	// equivalent index
	newmeta.equivalents = b.computeEquivalents(newmeta.topology)

	// loads - after creation, newmeta.loads is immutable, even though the
	// content (loadHeuristics) is mutable
	newmeta.loads = make(map[common.IndexInstId]*loadHeuristics)

	if currmeta != nil {
		// currmeta.loads is immutable
		for instId, curInst := range currmeta.insts {
			if newInst, ok := newmeta.insts[instId]; ok {
				if load, ok := currmeta.loads[instId]; ok {
					// carry over the stats.  It will only copy the stats based on the
					// partitions in newInst.   So if the partition is pruned, the
					// stats will be dropped.  The partition can be pruned when
					// 1) The partition may be dropped when node is rebalanced out
					// 2) The partition may not be available because the indexer is removed due to unwatchMetadata
					newmeta.loads[instId] = load.cloneRefresh(curInst, newInst)
				}
			}
		}
	}

	for instId, inst := range newmeta.insts {
		if _, ok := newmeta.loads[instId]; !ok {
			newmeta.loads[instId] = newLoadHeuristics(int(inst.NumPartitions))
		}
	}

	return newmeta
}

func (b *metadataClient) safeupdate(
	adminports map[string]common.IndexerId, force bool) {

	var currmeta, newmeta *indexTopology

	done := false
	for done == false {
		currmeta = (*indexTopology)(atomic.LoadPointer(&b.indexers))

		// no need to update if cached metadata is already up to date
		if !force && currmeta != nil && b.mdClient.GetMetadataVersion() <= currmeta.version {
			return
		}

		// if adminport is nil, then safeupdate is not triggered by
		// topology change.  Get the adminports from currmeta.
		if currmeta != nil && adminports == nil {
			adminports = currmeta.adminports
		}

		newmeta = b.updateTopology(adminports, force)
		if currmeta == nil {
			// This should happen only during bootstrap
			atomic.StorePointer(&b.indexers, unsafe.Pointer(newmeta))
			logging.Infof("initialized currmeta %v force %v \n", newmeta.version, force)
			return
		} else if force {
			if newmeta.version < currmeta.version {
				// This should not happen.  But if it does force to increment metadata version.
				b.mdClient.IncrementMetadataVersion()
				continue
			}
		} else if newmeta.version <= currmeta.version {
			fmsg := "skip newmeta %v <= %v force %v \n"
			logging.Infof(fmsg, newmeta.version, currmeta.version, force)
			return
		}

		logging.Debugf("updateTopology %v \n", newmeta)
		oldptr := unsafe.Pointer(currmeta)
		newptr := unsafe.Pointer(newmeta)
		done = atomic.CompareAndSwapPointer(&b.indexers, oldptr, newptr)

		// metaCh should never close
		if done && b.metaCh != nil {
			select {
			// update scan clients
			case b.metaCh <- true:
			default:
			}
		}
	}
	fmsg := "switched currmeta from %v -> %v force %v \n"
	logging.Infof(fmsg, currmeta.version, newmeta.version, force)
}

func (b *metadataClient) hasIndexersChanged(
	adminports map[string]common.IndexerId) bool {

	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	for adminport, indexerId := range adminports {
		x, ok := currmeta.adminports[adminport]
		if !ok || x != indexerId { // new indexer node detected.
			return true
		}
	}
	for adminport, indexerId := range currmeta.adminports {
		x, ok := adminports[adminport]
		if !ok || x != indexerId { // indexer node dropped out.
			return true
		}
	}
	return false
}

func (b *metadataClient) hasIndexesChanged(
	mindexes []*mclient.IndexMetadata, version uint64) bool {

	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	if currmeta.version < version {
		fmsg := "metadata provider version changed %v -> %v\n"
		logging.Infof(fmsg, currmeta.version, version)
		return true
	}

	for _, mindex := range mindexes {
		_, ok := currmeta.replicas[mindex.Definition.DefnId]
		if !ok { // new index detected.
			return true
		}
	}

	for _, iindexes := range currmeta.topology { //iindexes -> []*IndexMetadata
	loop:
		for _, index := range iindexes { // index -> *IndexMetadata
			for _, mindex := range mindexes {
				if mindex.Definition.DefnId == index.Definition.DefnId {
					for _, ix := range index.Instances {
						for _, iy := range mindex.Instances {
							if ix.InstId == iy.InstId && ix.State != iy.State {
								return true
							}
						}
					}
					continue loop
				}
			}
			return true
		}
	}
	return false
}

//
// Update statistics index instance
//
func (b *metadataClient) updateStats(stats map[common.IndexInstId]map[common.PartitionId]common.Statistics) {

	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	for instId, statsByPartitions := range stats {

		load, ok := currmeta.loads[instId]
		if !ok {
			continue
		}

		newStats := load.copyStats()

		for partitionId, stats := range statsByPartitions {

			if v := stats.Get("num_docs_pending"); v != nil {
				var pending, total int64
				if v.(float64) == float64(math.MaxInt64) {
					pending = math.MaxInt64
				} else {
					pending = int64(v.(float64))
				}

				if v := stats.Get("num_docs_queued"); v != nil {
					var queued int64
					if v.(float64) == float64(math.MaxInt64) {
						queued = math.MaxInt64
					} else {
						queued = int64(v.(float64))
					}
					if pending == math.MaxInt64 || queued == math.MaxInt64 {
						total = math.MaxInt64
					} else {
						total = pending + queued
					}
					newStats.updatePendingItem(partitionId, total)
				}
			}

			if v := stats.Get("last_rollback_time"); v != nil {
				if rollback, err := strconv.ParseInt(v.(string), 10, 64); err == nil {

					oldRollbackTime := load.getStats().getRollbackTime(partitionId)
					if rollback != oldRollbackTime {
						logging.Infof("Rollback time has changed for index inst %v. New rollback time %v", instId, rollback)
					}

					newStats.updateRollbackTime(partitionId, rollback)
				} else {
					logging.Errorf("Error in converting last_rollback_time %v, type %v", err)
				}
			}

			if v := stats.Get("progress_stat_time"); v != nil {
				if progress, err := strconv.ParseInt(v.(string), 10, 64); err == nil {
					newStats.updateStatsTime(partitionId, progress)
				} else {
					logging.Errorf("Error in converting progress_stat_time %v, type %v", err)
				}
			}
		}

		load.updateStats(newStats)
	}
}

// return adminports for all known indexers.
func getIndexerAdminports(cinfo *common.ClusterInfoCache) ([]string, int, int, int, int, error) {
	iAdminports := make([]string, 0)
	unhealthyNodes := 0
	for _, node := range cinfo.GetNodesByServiceType("indexAdmin") {
		status, err := cinfo.GetNodeStatus(node)
		if err != nil {
			return nil, 0, 0, 0, 0, err
		}
		logging.Verbosef("node %v status: %q", node, status)
		if status == "healthy" || status == "active" || status == "warmup" {
			adminport, err := cinfo.GetServiceAddress(node, "indexAdmin")
			if err != nil {
				return nil, 0, 0, 0, 0, err
			}
			iAdminports = append(iAdminports, adminport)
		} else {
			unhealthyNodes++
			logging.Warnf("node %v status: %q", node, status)
		}
	}

	return iAdminports, // active, healthy indexer node with known ports
		len(cinfo.GetActiveIndexerNodes()), // all active indexer node (known ports + unknown ports + unhealthy node)
		len(cinfo.GetFailedIndexerNodes()), // failover indexer node
		unhealthyNodes, // active, unhealthy indexer node with known ports
		len(cinfo.GetNewIndexerNodes()), // new indexer node
		nil
}

// FIXME/TODO: based on discussion with John-
//
//    if we cannot watch the metadata due to network partition we will
//    have an empty list of index and cannot query, in other words
//    the client will tolerate the partition and rejects scans until it
//    is healed.
//    i) alternatively, figure out a way to propagate error that happens
//       with watchClusterChanges() go-routine.
//
//    and while propating error back to the caller
//    1) we can encourage the caller to Refresh() the client hoping for
//       success, or,
//    2) Close() the client and re-create it.
//
//    side-effects of partitioning,
//    a) query cannot get indexes from the indexer node -- so n1ql has
//       to do bucket scan. It is a perf issue.
//    b) Network disconnected after watcher is up. We have the list of
//       indexes -- but we cannot query on it. N1QL should still degrade
//       to bucket scan.

func (b *metadataClient) watchClusterChanges() {
	selfRestart := func() {
		time.Sleep(time.Duration(b.servicesNotifierRetryTm) * time.Millisecond)
		go b.watchClusterChanges()
	}

	clusterURL, err := common.ClusterAuthUrl(b.cluster)
	if err != nil {
		logging.Errorf("common.ClusterAuthUrl(): %v\n", err)
		selfRestart()
		return
	}

	//This listens to /poolsStreaming/default and /pools/default/nodeServicesStreaming
	// With /poolsStreaming, ns-server is going to send notification when there is
	// topology change or node status change (MB-25865)
	scn, err := common.NewServicesChangeNotifier(clusterURL, "default")
	if err != nil {
		logging.Errorf("common.NewServicesChangeNotifier(): %v\n", err)
		selfRestart()
		return
	}
	defer scn.Close()

	hasUnhealthyNode := false
	ticker := time.NewTicker(time.Duration(5) * time.Minute)
	defer ticker.Stop()

	// For observing node services config
	ch := scn.GetNotifyCh()
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				selfRestart()
				return
			} else if err := b.updateIndexerList(false); err != nil {
				logging.Errorf("updateIndexerList(): %v\n", err)
				selfRestart()
				return
			}
		case _, ok := <-b.mdNotifyCh:
			if ok {
				if err := b.updateIndexerList(false); err != nil {
					logging.Errorf("updateIndexerList(): %v\n", err)
					selfRestart()
					return
				}
			}
		case stats, ok := <-b.stNotifyCh:
			if ok {
				b.updateStats(stats)
			}
		case <-ticker.C:
			// The following code is obsolete with MB-25865.

			// refresh indexer version
			b.mdClient.RefreshIndexerVersion()

			cinfo, err := common.FetchNewClusterInfoCache(b.cluster, common.DEFAULT_POOL, "")
			if err != nil {
				logging.Errorf("updateIndexerList(): %v\n", err)
				selfRestart()
				return
			}

			_, _, _, unhealthyNode, _, err := getIndexerAdminports(cinfo)
			if err != nil {
				logging.Errorf("updateIndexerList(): %v\n", err)
				selfRestart()
				return
			}

			if unhealthyNode != 0 {
				hasUnhealthyNode = true
				b.mdClient.SetClusterStatus(-1, -1, unhealthyNode, -1)

			} else if hasUnhealthyNode {
				// refresh indexer version when there is no more unhealthy node
				hasUnhealthyNode = false
				if err := b.updateIndexerList(false); err != nil {
					logging.Errorf("updateIndexerList(): %v\n", err)
					selfRestart()
					return
				}
			}
		case <-b.finch:
			return
		}
	}
}

func postWithAuth(url string, bodyType string, body io.Reader, timeout time.Duration) (*http.Response, error) {
	params := &security.RequestParams{Timeout: time.Duration(timeout) * time.Second}
	return security.PostWithAuth(url, bodyType, body, params)
}

func getWithAuth(url string) (*http.Response, error) {
	params := &security.RequestParams{Timeout: time.Duration(10) * time.Second}
	return security.GetWithAuth(url, params)
}
