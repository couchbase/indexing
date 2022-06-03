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
	handler   http.Handler
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

	tlsCaFile := config["caFile"].String()
	settings := regulator.InitSettings{
		NodeID:           service.NodeID(nodeID),
		Service:          regulator.Index,
		TlsCAFile:        tlsCaFile,
		BindHttpPort:     0,
		ServiceCheckMask: 0,
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
func (m *MeteringThrottlingMgr) CheckWriteThrottle(bucket, user string, maxThrottle time.Duration) (
	result CheckResult, throttleTime time.Duration, err error) {
	ctx := getCtx(bucket, user)
	estimatedUnits, err := regulator.NewUnits(regulator.Index, regulator.WriteCapacityUnit, uint64(0))
	if err == nil {
		quotaOpts := regulator.CheckQuotaOpts{
			MaxThrottle:       maxThrottle,
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

func (m *MeteringThrottlingMgr) RecordReadUnits(bucket, user string, bytes uint64) error {
	// caller not expected to check for error or fail for metering errors hence no need to return error
	units, _ := metering.IndexReadToRCU(bytes)
	//TBD... log error but take care of log flooding.
	ctx := getCtx(bucket, user)
	_ = regulator.RecordUnits(ctx, units)
	return nil
}

func (m *MeteringThrottlingMgr) RecordWriteUnits(bucket, user string, bytes uint64, update bool) error {
	// caller not expected to check for error or fail for metering errors hence no need to return error
	units, _ := metering.IndexWriteToWCU(bytes, update)
	//TBD... log error but take care of log flooding.
	ctx := getCtx(bucket, user)
	_ = regulator.RecordUnits(ctx, units)
	return nil
}

func getCtx(bucket, user string) regulator.Ctx {
	return regulator.NewUserCtx(bucket, user)
}
