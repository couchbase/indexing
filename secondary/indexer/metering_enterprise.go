//go:build !community

// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/audit"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/regulator"
	"github.com/couchbase/regulator/factory"
	"github.com/couchbase/regulator/metering"
	"github.com/couchbase/regulator/variants"
)

const METERING_FILE_VERSION int64 = 0

// Small row scans
const THROTTLING_SCAN_ITERATIONS_QUANTUM uint64 = 1000

// Large row scans - Say every iteration is 1 RU - 200 Iterations
const THROTTLING_SCAN_BYTES_QUANTUM uint64 = 256 * 200

type Units regulator.Units

func (u *Units) Whole() uint64 {
	return regulator.Units(*u).Whole()
}

type UnitType regulator.UnitType

const (
	IndexWriteBuildVariant  = UnitType(variants.IndexWriteBuildVariant)
	IndexWriteUpdateVariant = UnitType(variants.IndexWriteUpdateVariant)
	IndexWriteInsertVariant = UnitType(variants.IndexWriteUpdateInsertOnlyVariant)

	// Deletes are not normalized with numInserts while tenant management
	IndexWriteDeleteVariant = UnitType(variants.IndexWriteUpdateVariant)
)

type AggregateRecorder struct {
	regulator.AggregateRecorder

	bytesToMeter uint64
	sumOfOps     uint64
	variantRatio map[UnitType]uint64
}

type AggregateRecorderWithCtx struct {
	regulator.AggregateRecorder
	ctx *regulator.Ctx
}

type MeteringThrottlingMgr struct {
	handler   regulator.StatsHttpHandler
	config    common.ConfigHolder
	supvCmdch MsgChannel //supervisor sends commands on this channel

	indexInstMap  IndexInstMapHolder
	indexPartnMap IndexPartnMapHolder

	persister        StatsPersister
	lastPersisterlog uint64
	recoveredWUMap   map[string]uint64
	indexerReady     bool
}

func NewMeteringManager(nodeID string, config common.Config, supvCmdCh MsgChannel) (*MeteringThrottlingMgr, Message) {

	settings := regulator.InitSettings{
		NodeID:  service.NodeID(nodeID),
		Service: regulator.Index,
	}

	handler := factory.InitRegulator(settings)

	mtMgr := &MeteringThrottlingMgr{
		handler:   handler,
		supvCmdch: supvCmdCh,
	}
	mtMgr.config.Store(config)
	mtMgr.indexInstMap.Init()
	mtMgr.indexPartnMap.Init()

	statsDir := path.Join(config["storage_dir"].String(), STATS_DATA_DIR)
	chunkSz := config["statsPersistenceChunkSize"].Int()
	fileName := "metering_data"
	newFileName := "metering_data_new"
	mtMgr.persister = NewFlatFilePersister(statsDir, chunkSz, fileName, newFileName)
	mtMgr.retrieveMeteringData()

	// main loop
	go mtMgr.run()

	return mtMgr, &MsgSuccess{}

}

// RetrieveMeteringData is used to retrieve the persisted last billed write
// units for every bucket on recovery. This data is persisted when control
// plane calls metering endpoint. It is used to account for duplicate units on
// index recovery.
func (m *MeteringThrottlingMgr) retrieveMeteringData() error {
	persistedStats, err := m.persister.ReadPersistedStats()
	if err != nil {
		logging.Warnf("Encountered error while reading persisted stats. Skipping read. Error: %v", err)
		return err
	}

	rawHeader := persistedStats["header"]
	header := rawHeader.(map[string]interface{})
	version := safeGetInt64(header["version"])
	if version != 0 {
		err := fmt.Errorf("Invalid version: %v of metering data found", version)
		logging.Infof("MeteringThrottlingMgr::RetrieveMeteringData Invalid data read. Err: %v", err)
		return err
	}

	meteringData := persistedStats["meteringData"].(map[string]interface{})
	m.recoveredWUMap = make(map[string]uint64)
	for bucket, wu := range meteringData {
		m.recoveredWUMap[bucket] = uint64(safeGetInt64(wu))
	}

	logging.Infof("MeteringThrottlingMgr::RetrieveMeteringData Recovered Write map from file: %v", m.recoveredWUMap)
	return nil
}

//
// Handle _metering endpoint for billing. DP Agent calls _metering at regular
// intervals to get billing data from regulator
//

func (m *MeteringThrottlingMgr) RegisterRestEndpoints() {
	mux := GetHTTPMux()
	mux.HandleFunc(regulator.MeteringEndpoint, m.meteringEndpointHandler)
}

func (m *MeteringThrottlingMgr) meteringEndpointHandler(w http.ResponseWriter,
	r *http.Request) {

	creds, valid, err := common.IsAuthValid(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error() + "\n"))
		return
	} else if !valid {
		audit.Audit(common.AUDIT_UNAUTHORIZED, r,
			"MeteringThrottlingMgr::MeteringEndpointHandler", "")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write(common.HTTP_STATUS_UNAUTHORIZED)
		return
	} else if creds != nil {
		allowed, err := creds.IsAllowed("cluster.settings!read")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		} else if !allowed {
			logging.Verbosef("MeteringThrottlingMgr::MeteringEndpointHandler not enough permissions for getting metering stats")
			w.WriteHeader(http.StatusForbidden)
			w.Write(common.HTTP_STATUS_FORBIDDEN)
			return
		}
	}

	// TODO: Use shared buffer pool
	meteringEPBuf := m.handler.AppendMetrics([]byte{})

	if m.indexerReady {
		m.persistMeteringData()
	}

	w.WriteHeader(200)
	w.Write(meteringEPBuf)
}

// PersistMeteringData persisted the Write Units for every bucket. Its gets the
// data by Getting write units from every slice in partn map and accumulates at
// bucket level. Is used to account for duplicate units on index recovery.
func (m *MeteringThrottlingMgr) persistMeteringData() {
	dataMap := m.getMeteringDataMap()
	if err := m.persister.PersistStats(dataMap); err != nil {
		logging.Warnf("MeteringThrottlingMgr::Persister Error persisting stats: %v", err)
	}

	now := uint64(time.Now().UnixNano())
	sinceLastLog := now - m.lastPersisterlog
	if sinceLastLog > uint64(300*time.Second) || logging.IsEnabled(logging.Verbose) {
		logging.Infof("MeteringThrottlingMgr::PersistMeteringData Persisted Metering data: %v", dataMap)
		m.lastPersisterlog = now
	}
}

func (m *MeteringThrottlingMgr) getMeteringDataMap() map[string]interface{} {
	header := make(map[string]interface{})
	header["version"] = METERING_FILE_VERSION

	dataMap := make(map[string]interface{})
	dataMap["meteringData"] = m.getWriteUnitsFromSlices()
	dataMap["header"] = header

	return dataMap
}

//
// Functions to handle Supervisor commands
//

// main loop that will handle config change and updates to index inst and stream status
func (m *MeteringThrottlingMgr) run() {
	logging.Infof("MeteringThrottlingMgr:: started")
loop:
	for {
		select {
		case cmd, ok := <-m.supvCmdch:
			if ok {
				m.handleSupvervisorCommands(cmd)
			} else {
				//supervisor channel closed. exit
				break loop
			}
		}
	}
	logging.Infof("MeteringThrottlingMgr:: exited...")
}

func (m *MeteringThrottlingMgr) handleSupvervisorCommands(cmd Message) {
	switch cmd.GetMsgType() {
	case CONFIG_SETTINGS_UPDATE:
		m.handleConfigUpdate(cmd)
	case UPDATE_INDEX_INSTANCE_MAP:
		m.handleUpdateIndexInstMap(cmd)
	case UPDATE_INDEX_PARTITION_MAP:
		m.handleUpdateIndexPartnMap(cmd)
	case CLUST_MGR_INDEXER_READY:
		m.handleIndexerReady()
	case METERING_MGR_STOP_WRITE_BILLING:
		m.handleStopWriteBilling(cmd)
	case METERING_MGR_START_WRITE_BILLING:
		m.handleStartWriteBilling(cmd)
	default:
		logging.Errorf("MeteringThrottlingMgr: Received Unknown Command %v", cmd)
		m.supvCmdch <- &MsgError{
			err: Error{code: ERROR_METERING_THROTTLING_UNKNOWN_COMMAND,
				severity: NORMAL,
				category: METERING_THROTTLING_MGR}}
	}
}

// handleStarWriteBilling start WU Billing for slices. This will only impact the
// slices for which WU Billing was stopped
func (m *MeteringThrottlingMgr) handleStartWriteBilling(cmd Message) {
	msg := cmd.(*MsgMeteringUpdate)
	instIdsToStart := msg.GetInstanceIds()

	indexPartnMap := m.indexPartnMap.Get()
	for _, instId := range instIdsToStart {
		if partnInstMap, ok := indexPartnMap[instId]; ok {
			for _, partn := range partnInstMap {
				for _, slice := range partn.Sc.GetAllSlices() {
					slice.SetStopWriteUnitBilling(false)
				}
			}
		}
	}
	msg.respCh <- nil
	m.supvCmdch <- &MsgSuccess{}
}

// handleStopWriteBilling stops the write billing for the instIds give in the
// input message
func (m *MeteringThrottlingMgr) handleStopWriteBilling(cmd Message) {
	msg := cmd.(*MsgMeteringUpdate)
	instIdsToStop := msg.GetInstanceIds()

	indexPartnMap := m.indexPartnMap.Get()
	for _, instId := range instIdsToStop {
		if partnInstMap, ok := indexPartnMap[instId]; ok {
			for _, partn := range partnInstMap {
				for _, slice := range partn.Sc.GetAllSlices() {
					slice.SetStopWriteUnitBilling(true)
				}
			}
		}
	}
	msg.respCh <- nil
	m.supvCmdch <- &MsgSuccess{}
}

func (m *MeteringThrottlingMgr) handleConfigUpdate(cmd Message) {
	cfgUpdate := cmd.(*MsgConfigUpdate)
	m.config.Store(cfgUpdate.GetConfig())
	m.supvCmdch <- &MsgSuccess{}
}

// handleUpdateIndexInstMap updates the indexInstMap
func (m *MeteringThrottlingMgr) handleUpdateIndexInstMap(cmd Message) {

	logging.Tracef("MeteringThrottlingMgr::handleUpdateIndexInstMap %v", cmd)

	req := cmd.(*MsgUpdateInstMap)
	indexInstMap := req.GetIndexInstMap()
	copyIndexInstMap := common.CopyIndexInstMap2(indexInstMap)
	m.indexInstMap.Set(copyIndexInstMap)

	m.supvCmdch <- &MsgSuccess{}
}

// handleUpdateIndexPartnMap updates the indexPartnMap
func (m *MeteringThrottlingMgr) handleUpdateIndexPartnMap(cmd Message) {

	logging.Tracef("MeteringThrottlingMgr::handleUpdateIndexPartnMap %v", cmd)

	req := cmd.(*MsgUpdatePartnMap)
	indexPartnMap := req.GetIndexPartnMap()
	copyIndexPartnMap := CopyIndexPartnMap(indexPartnMap)
	m.indexPartnMap.Set(copyIndexPartnMap)

	m.supvCmdch <- &MsgSuccess{}

}

func (m *MeteringThrottlingMgr) handleIndexerReady() {
	logging.Infof("MeteringThrottlingMgr::handleIndexerReady Received notification after indexer is ready")

	// Once indexer is ready i.e. after it loaded all the snapshots for slices
	// we can get the WUs from slices for all buckets and WUs for bucket that
	// got persisted in metering endpoint handler before recovery.
	if m.recoveredWUMap != nil {
		unitsToRefund, unitsToMeter := m.adjustWriteUnitsOnRecovery()

		// Update regulator with the difference
		for bucketName, diffWUs := range unitsToRefund {
			m.RefundWriteUnitsComputed(bucketName, diffWUs)
			logging.Infof("MeteringThrottlingMgr:handleIndexerReady Refunding Write Units: %v", diffWUs)
		}

		for bucketName, diffWUs := range unitsToMeter {
			m.RecordWriteUnitsComputed(bucketName, diffWUs, true)
			logging.Infof("MeteringThrottlingMgr:handleIndexerReady Recoding Write Units: %v", diffWUs)
		}

		m.persistMeteringData()
		m.recoveredWUMap = nil
	}

	m.indexerReady = true
	m.supvCmdch <- &MsgSuccess{}
}

//
// Functions for Refund on Recovery
//

// AdjustWriteUnitsOnRecovery accounts for duplicate write units during recovery
func (m *MeteringThrottlingMgr) adjustWriteUnitsOnRecovery() (unitsToRefund,
	unitsToMeter map[string]uint64) {
	// Get the sum of all write units from the slice snapshots at bucket level
	snapshotWUMap := m.getWriteUnitsFromSlices()

	logging.Infof("MeteringThrottlingMgr::handleIndexerReady Write units in the current snapshot: %v", snapshotWUMap)

	// Calculate the difference
	unitsToRefund = make(map[string]uint64)
	unitsToMeter = make(map[string]uint64)
	for bucketName, lastBilledWUs := range m.recoveredWUMap {
		snapshotWUs := snapshotWUMap[bucketName]
		if lastBilledWUs > snapshotWUs {
			diffWUs := lastBilledWUs - snapshotWUs
			unitsToRefund[bucketName] = diffWUs
		} else if lastBilledWUs < snapshotWUs {
			diffWUs := snapshotWUs - lastBilledWUs
			unitsToMeter[bucketName] = diffWUs
		}
	}
	return
}

// GetWritesUnitsFromSlices gets write units from all slices in indexPartnMap
// and accumulated write units at bucket level
func (m *MeteringThrottlingMgr) getWriteUnitsFromSlices() map[string]uint64 {
	indexInstMap := m.indexInstMap.Get()
	indexPartnMap := m.indexPartnMap.Get()

	tenantWUsMap := make(map[string]uint64)
	addWUsToMap := func(bucketName string, wus uint64) {
		tenantWUsMap[bucketName] += wus
	}

	for instId, inst := range indexInstMap {
		if partnInstMap, ok := indexPartnMap[instId]; ok {
			for _, partn := range partnInstMap {
				for _, slice := range partn.Sc.GetAllSlices() {
					meteredWriteUnits := slice.GetWriteUnits()
					addWUsToMap(inst.Defn.Bucket, meteredWriteUnits)
				}

			}
		}
	}

	return tenantWUsMap
}

//
// Throttling API
//

// CheckQuotaAndSleep is wrapper for regulator.CheckQuota and sleep based on output
func (m *MeteringThrottlingMgr) CheckQuotaAndSleep(bucketName, user string, isWrite bool,
	timeout time.Duration, ctx *regulator.Ctx) (proceed bool, throttleLatency time.Duration, err error) {

	var readOrWrite regulator.UnitType

	if isWrite {
		readOrWrite = regulator.Write
		if ctx == nil {
			c := getNoUserCtx(bucketName)
			ctx = &c
		}
	} else {
		readOrWrite = regulator.Read
		if ctx == nil {
			c := getUserCtx(bucketName, user)
			ctx = &c
		}
	}

	estimatedUnits, err := regulator.NewUnits(regulator.Index, readOrWrite, uint64(0))
	if err != nil {
		return false, throttleLatency, err
	}

	quotaOpts := regulator.CheckQuotaOpts{
		Timeout:             timeout,
		NoThrottle:          false,
		NoReject:            isWrite,
		EstimatedDuration:   time.Duration(0),
		EstimatedUnitsMulti: []regulator.Units{estimatedUnits},
	}

	for {
		result, throttle, err := regulator.CheckQuota(*ctx, &quotaOpts)
		if err != nil {
			return false, throttleLatency, err
		}
		throttleLatency += throttle

		switch result {
		case regulator.CheckResultThrottleRetry:
			time.Sleep(throttle)
		case regulator.CheckResultThrottleProceed:
			time.Sleep(throttle)
			return true, throttleLatency, nil
		case regulator.CheckResultProceed:
			return true, throttleLatency, nil
		case regulator.CheckResultReject:
			if isWrite {
				return false, throttleLatency, fmt.Errorf("CheckResultReject is not expected")
			}
			return false, throttleLatency, nil
		case regulator.CheckResultError:
			return false, throttleLatency, fmt.Errorf("CheckResultError received from regulator")
		}
	}
}

//
// Metering API
//

func (m *MeteringThrottlingMgr) RecordReadUnits(bucket, user string, bytes uint64, billable bool) (uint64, error) {
	// caller not expected to fail for metering errors
	// hence returning errors for debugging and logging purpose only
	units, err := metering.ByteOperationToUnits(regulator.Index, bytes, regulator.Read)
	if err == nil {
		ctx := getUserCtx(bucket, user)
		if billable {
			return units.Whole(), regulator.RecordUnits(ctx, units)
		} else {
			return units.Whole(), regulator.RecordUnbillableUnits(ctx, units)
		}
	}
	return 0, err
}

func (m *MeteringThrottlingMgr) IndexWriteToWU(bytes uint64, writeVariant UnitType) (Units, error) {
	units, err := metering.ByteOperationToUnits(regulator.Index, bytes, regulator.UnitType(writeVariant))
	if err != nil {
		return 0, err
	}
	return Units(units), err
}

func (m *MeteringThrottlingMgr) RecordWriteUnits(bucket string, units Units, billable bool) error {
	// caller not expected to fail for metering errors
	// hence returning errors for debugging and logging purpose only
	ctx := getNoUserCtx(bucket)
	if billable {
		return regulator.RecordUnits(ctx, regulator.Units(units))
	} else {
		return regulator.RecordUnbillableUnits(ctx, regulator.Units(units))
	}
}

// RecordWriteUnitsComputed records given number of billable write units
func (m *MeteringThrottlingMgr) RecordWriteUnitsComputed(bucket string, writeUnits uint64, billable bool) error {
	// caller not expected to fail for metering errors
	// hence returning errors for debugging and logging purpose only
	units, err := regulator.NewUnits(regulator.Index, regulator.Write, writeUnits)
	if err != nil {
		return err
	}

	ctx := getNoUserCtx(bucket)
	if billable {
		return regulator.RecordUnits(ctx, units)
	} else {
		return regulator.RecordUnbillableUnits(ctx, units)
	}
}

// RefundWriteUnitsComputed will refund given number of billable write units
func (m *MeteringThrottlingMgr) RefundWriteUnitsComputed(bucket string, writeUnits uint64) error {
	// caller not expected to fail for metering errors
	// hence returning errors for debugging and logging purpose only
	units, err := regulator.NewUnits(regulator.Index, regulator.Write, writeUnits)
	if err != nil {
		return err
	}
	ctx := getNoUserCtx(bucket)
	return regulator.RefundUnits(ctx, units)
}

func (m *MeteringThrottlingMgr) WriteMetrics(w http.ResponseWriter) int {
	return m.handler.WriteStats(w)
}

// AddBytesOfVarType records numbers of bytes of given variant type. Given variant
// must be a sub type of variant given when creating the aggregator. If the initial
// aggregator was of write variant you cannot record read variant in it.
func (ag *AggregateRecorder) AddBytesOfVarType(bytes uint64, variant UnitType) {
	ag.AddVariantBytes(bytes, regulator.UnitType(variant))
	logging.Tracef("AggregateRecorder::AddBytesOfVarType adding %v bytes of %s variant",
		bytes, regulator.UnitType(variant))
}

// SetVarRatio sets the variant ratio to divide the bytes recorded by function
// AddBytesInVarRatio to variants in given ratio. Say if you want to record 100
// bytes in update:insert ratio of 2:1 you can set the variant ratio here
// as {[UPDATE]->2, [INSERT]->2}. Should trigger FinishAddsInVarRatio at the end
// to record the last remaining number of bytes. All variants should of the same
// base types as that of the recorder.
func (ag *AggregateRecorder) SetVarRatio(variantRatio map[UnitType]uint64) {
	ag.variantRatio = variantRatio
	ag.sumOfOps = 0
	for _, r := range variantRatio {
		ag.sumOfOps += r
	}
	logging.Tracef("AggregateRecorder::SetVarRatio varRatioMap: %v sumOfOps: %v",
		ag.variantRatio, ag.sumOfOps)
}

// AddBytesInVarRatio records the bytes and divides them in the ratio set using
// SetVarRatio. Say you are recording 92 bytes and ratio set is 2:1 here we record
// 60 bytes to update variant and 30 bytes to insert variant. Remaining 2 bytes
// will be adjusted in FinishAddsInVarRatio.
func (ag *AggregateRecorder) AddBytesInVarRatio(bytesToMeter uint64) {
	if ag.variantRatio == nil || bytesToMeter == 0 {
		return
	}

	ag.bytesToMeter += bytesToMeter
	if ag.bytesToMeter < ag.sumOfOps {
		return
	}

	m := ag.bytesToMeter / ag.sumOfOps
	logging.Tracef("AggregateRecorder::AddBytesInVarRatio distributing %v bytes with given varRatio and a multiple of %v",
		ag.bytesToMeter, m)
	for varType, contrib := range ag.variantRatio {
		numVarBytes := m * contrib
		ag.AddBytesOfVarType(numVarBytes, varType)
	}
	ag.bytesToMeter = ag.bytesToMeter % ag.sumOfOps
	logging.Tracef("AggregateRecorder::AddBytesInVarRatio add %v bytes after recording remaining %v",
		bytesToMeter, ag.bytesToMeter)
}

// FinishAddsInVarRatio records the remainder bytes in the ratio set by SetVarRatio
func (ag *AggregateRecorder) FinishAddsInVarRatio() {
	if ag.variantRatio == nil || ag.bytesToMeter == 0 {
		return
	}

	i := 0
	remaining := ag.bytesToMeter
	for varType, contrib := range ag.variantRatio {
		if i == len(ag.variantRatio)-1 {
			ag.AddBytesOfVarType(remaining, varType)
			break
		}
		numVarBytes := (ag.bytesToMeter * contrib) / ag.sumOfOps
		ag.AddBytesOfVarType(numVarBytes, varType)
		remaining -= numVarBytes
		i++
	}

}

func (agc *AggregateRecorderWithCtx) GetContext() *regulator.Ctx {
	return agc.ctx
}

func (m *MeteringThrottlingMgr) StartWriteAggregateRecorder(bucketName string, billable bool) *AggregateRecorder {
	ctx := getNoUserCtx(bucketName)
	options := &regulator.AggregationOptions{
		Unbilled:         !billable,
		DeferredMetering: true,
	}
	ag := regulator.StartAggregateRecorder(ctx, regulator.Index, regulator.Write, options)
	return &AggregateRecorder{ag, 0, 0, nil}
}

func (m *MeteringThrottlingMgr) StartReadAggregateRecorder(bucketName, user string,
	billable bool) *AggregateRecorderWithCtx {
	ctx := getUserCtx(bucketName, user)
	options := &regulator.AggregationOptions{
		Unbilled: !billable,
	}
	ag := regulator.StartAggregateRecorder(ctx, regulator.Index, regulator.Read, options)
	return &AggregateRecorderWithCtx{ag, &ctx}
}

func getNoUserCtx(bucket string) regulator.Ctx {
	return regulator.NewBucketCtx(bucket)
}

func getUserCtx(bucket, user string) regulator.Ctx {
	return regulator.NewUserCtx(bucket, user)
}
