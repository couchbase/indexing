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
	"net/http"
	"time"

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/regulator"
	"github.com/couchbase/regulator/factory"
	"github.com/couchbase/regulator/metering"
)

type MeteringThrottlingMgr struct {
	handler   regulator.StatsHttpHandler
	config    common.ConfigHolder
	supvCmdch MsgChannel //supervisor sends commands on this channel
}

// redefine regulator result constants and create a mapping function
// so that we dont need to import regulator module elsewhere
var errorMap map[regulator.CheckResult]CheckResult

func RegulatorErrorToIndexerError(err regulator.CheckResult) CheckResult {
	err1, ok := errorMap[err]
	if !ok {
		return CheckResultError
	}
	return err1
}

func init() {
	errorMap = make(map[regulator.CheckResult]CheckResult)
	errorMap[regulator.CheckResultNormal] = CheckResultNormal
	errorMap[regulator.CheckResultThrottle] = CheckResultThrottle
	errorMap[regulator.CheckResultReject] = CheckResultReject
	errorMap[regulator.CheckResultError] = CheckResultError
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

	// main loop
	go mtMgr.run()

	return mtMgr, &MsgSuccess{}

}

func (m *MeteringThrottlingMgr) RegisterRestEndpoints() {
	mux := GetHTTPMux()
	mux.Handle(regulator.MeteringEndpoint, m.handler)
}

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
	default:
		logging.Errorf("MeteringThrottlingMgr: Received Unknown Command %v", cmd)
		m.supvCmdch <- &MsgError{
			err: Error{code: ERROR_METERING_THROTTLING_UNKNOWN_COMMAND,
				severity: NORMAL,
				category: METERING_THROTTLING_MGR}}
	}
}

func (m *MeteringThrottlingMgr) handleConfigUpdate(cmd Message) {
	cfgUpdate := cmd.(*MsgConfigUpdate)
	m.config.Store(cfgUpdate.GetConfig())
	m.supvCmdch <- &MsgSuccess{}
}

// wrappers for checkQuota/throttle/metering etc which will use config and may be stream status
// to determin how to record a certain operation
func (m *MeteringThrottlingMgr) CheckWriteThrottle(bucket string) (
	result CheckResult, throttleTime time.Duration, err error) {
	ctx := getCtx(bucket, "")
	estimatedUnits, err := regulator.NewUnits(regulator.Index, regulator.Write, uint64(0))
	if err == nil {
		quotaOpts := regulator.CheckQuotaOpts{
			MaxThrottle:       time.Duration(0),
			NoThrottle:        false,
			NoReject:          true,
			EstimatedDuration: time.Duration(0),
			EstimatedUnits:    []regulator.Units{estimatedUnits},
		}
		r, d, e := regulator.CheckQuota(ctx, &quotaOpts)
		return RegulatorErrorToIndexerError(r), d, e
	}
	return CheckResultError, time.Duration(0), err
}

func (m *MeteringThrottlingMgr) RecordReadUnits(bucket, user string, bytes uint64, billable bool) (uint64, error) {
	// caller not expected to fail for metering errors
	// hence returning errors for debugging and logging purpose only
	units, err := metering.IndexReadToRU(bytes)
	if err == nil {
		ctx := getCtx(bucket, user)
		if billable {
			return units.Whole(), regulator.RecordUnits(ctx, units)
		} else {
			return units.Whole(), regulator.RecordUnbillableUnits(ctx, units)
		}
	}
	return 0, err
}

func (m *MeteringThrottlingMgr) RecordWriteUnits(bucket string, bytes uint64, update bool, billable bool) error {
	// caller not expected to fail for metering errors
	// hence returning errors for debugging and logging purpose only
	units, err := metering.IndexWriteToWU(bytes, update)
	if err == nil {
		ctx := getCtx(bucket, "")
		if billable {
			return regulator.RecordUnits(ctx, units)
		} else {
			return regulator.RecordUnbillableUnits(ctx, units)
		}
	}
	return err
}

func (m *MeteringThrottlingMgr) RefundWriteUnits(bucket string, bytes uint64) error {
	// caller not expected to fail for metering errors
	// hence returning errors for debugging and logging purpose only
	units, err := metering.IndexWriteToWU(bytes, false)
	if err == nil {
		ctx := getCtx(bucket, "")
		return regulator.RefundUnits(ctx, units)
	}
	return err
}

func (m *MeteringThrottlingMgr) WriteMetrics(w http.ResponseWriter) int {
	return m.handler.WriteMetrics(w)
}

type MeteringTransaction regulator.TransactionalRecorder

func (m *MeteringThrottlingMgr) StartMeteringTxn(bucketName, user string, billable bool) MeteringTransaction {
	ctx := getCtx(bucketName, user)
	options := &regulator.TransactionOptions{
		Unbilled: !billable,
	}
	return regulator.BeginTransactionV2(ctx, options)
}

func getCtx(bucket, user string) regulator.Ctx {
	return regulator.NewUserCtx(bucket, user)
}
