//go:build community

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

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/stubs/regulator"
)

const METERING_FILE_VERSION int64 = 0

const THROTTLING_SCAN_ITERATIONS_QUANTUM uint64 = 1000

const THROTTLING_SCAN_BYTES_QUANTUM uint64 = 256 * 200

type Units regulator.Units

func (u *Units) Whole() uint64 {
	return 0
}

type UnitType regulator.UnitType

const (
	IndexWriteBuildVariant UnitType = iota
	IndexWriteUpdateVariant
	IndexWriteInsertVariant
	IndexWriteDeleteVariant
	IndexWriteArrayVariant
)

type AggregateRecorder regulator.AggregateRecorder

type AggregateRecorderWithCtx struct {
	regulator.AggregateRecorder
	ctx *regulator.Ctx
}

type MeteringThrottlingMgr struct {
}

func NewMeteringManager(nodeID string, config common.Config, supvCmdCh MsgChannel) (*MeteringThrottlingMgr, Message) {
	panic("MeteringManager::NewMeteringManager Not implemented for Community Edition")
	return nil, &MsgSuccess{}
}

func (m *MeteringThrottlingMgr) RegisterRestEndpoints() {
	panic("MeteringManager::RegisterRestEndpoints Not implemented for Community Edition")
}

func (m *MeteringThrottlingMgr) CheckQuotaAndSleep(bucketName, user string, isWrite bool,
	timeout time.Duration, ctx *regulator.Ctx) (proceed bool, throttleLatency time.Duration, err error) {
	panic("MeteringManager::CheckQuotaAndSleep Not implemented for Community Edition")
	return
}

func (m *MeteringThrottlingMgr) RecordReadUnits(bucket, user string, bytes uint64, billable bool) (uint64, error) {
	panic("MeteringManager::RecordReadUnits Not implemented for Community Edition")
	return 0, nil
}

func (m *MeteringThrottlingMgr) IndexWriteToWU(bytes uint64, writeVariant UnitType) (Units, error) {
	panic("MeteringManager::RecordWriteUnits Not implemented for Community Edition")
	return 0, nil
}

func (m *MeteringThrottlingMgr) RecordWriteUnits(bucket string, units Units, billable bool) error {
	panic("MeteringManager::RecordWriteUnits Not implemented for Community Edition")
	return nil
}

func (m *MeteringThrottlingMgr) RecordWriteUnitsComputed(bucket string, writeUnits uint64, billable bool) error {
	panic("MeteringManager::RecordWriteUnits Not implemented for Community Edition")
	return nil
}

func (m *MeteringThrottlingMgr) RefundWriteUnitsComputed(bucket string, writeUnits uint64) error {
	panic("MeteringManager::RecordWriteUnits Not implemented for Community Edition")
	return nil
}

func (m *MeteringThrottlingMgr) WriteMetrics(w http.ResponseWriter) int {
	panic("MeteringManager::WriteMetrics Not implemented for Community Edition")
}

func (agc *AggregateRecorderWithCtx) GetContext() *regulator.Ctx {
	return nil
}

func (m *MeteringThrottlingMgr) StartWriteAggregateRecorder(bucketName string, billable,
	writeVariant UnitType) AggregateRecorder {
	panic("MeteringManager::StartWriteAggregateRecorder Not implemented for Community Edition")
	return AggregateRecorder{}
}

func (m *MeteringThrottlingMgr) StartReadAggregateRecorder(bucketName, user string,
	billable bool) *AggregateRecorderWithCtx {
	panic("MeteringManager::StartReadAggregateRecorder Not implemented for Community Edition")
	return nil
}
