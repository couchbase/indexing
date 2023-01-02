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
)

type MeteringThrottlingMgr struct {
}

func NewMeteringManager(nodeID string, config common.Config, supvCmdCh MsgChannel) (*MeteringThrottlingMgr, Message) {
	panic("MeteringManager::NewMeteringManager Not implemented for Community Edition")
	return nil, &MsgSuccess{}
}

func (m *MeteringThrottlingMgr) RegisterRestEndpoints() {
	panic("MeteringManager::RegisterRestEndpoints Not implemented for Community Edition")
}

func (m *MeteringThrottlingMgr) CheckWriteThrottle(bucket string) (
	result CheckResult, throttleTime time.Duration, err error) {
	panic("MeteringManager::Not implemented for Community Edition")
	return
}

func (m *MeteringThrottlingMgr) RecordWriteUnits(bucket string, bytes uint64, update bool, billable bool) (uint64, error) {
	panic("MeteringManager::RecordWriteUnits Not implemented for Community Edition")
	return 0, nil
}

func (m *MeteringThrottlingMgr) RefundWriteUnits(bucket string, bytes uint64) error {
	panic("MeteringManager::RefundWriteUnits Not implemented for Community Edition")
	return nil
}

func (m *MeteringThrottlingMgr) RecordReadUnits(bucket, user string, bytes uint64, billable bool) (uint64, error) {
	panic("MeteringManager::RecordReadUnits Not implemented for Community Edition")
	return 0, nil
}

func (m *MeteringThrottlingMgr) WriteMetrics(w http.ResponseWriter) int {
	panic("MeteringManager::WriteMetrics Not implemented for Community Edition")
}

type Units struct {
}

func (u *Units) Whole() uint64 {
	return 0
}

type AggregateRecorder struct {
}

func (ar *AggregateRecorder) AddBytes(bytes uint64) error {
	return nil
}

func (ar *AggregateRecorder) State() (metered, pending Units, bytesPending uint64) {
	return
}

func (ar *AggregateRecorder) Commit() (committed Units, err error) {
	return
}

func (ar *AggregateRecorder) Abort() error {
	return nil
}

func (ar *AggregateRecorder) Flush() error {
	return nil
}

func (m *MeteringThrottlingMgr) StartWriteAggregateRecorder(bucketName string, billable, update bool) AggregateRecorder {
	panic("MeteringManager::StartWriteAggregateRecorder Not implemented for Community Edition")
	return AggregateRecorder{}
}

func (m *MeteringThrottlingMgr) StartReadAggregateRecorder(bucketName, user string, billable bool) AggregateRecorder {
	panic("MeteringManager::StartReadAggregateRecorder Not implemented for Community Edition")
	return AggregateRecorder{}
}
