// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

// Package scanreport defines scan reporting structures used by the indexer and query client.
package scanreport

import (
	"github.com/couchbase/indexing/secondary/common"
)

type ScanReportState struct {
	ReqID     string

	// Final report fields
	DefnID          common.IndexDefnId
	SrvrAvgMs       *ServerTimings
	SrvrTotalCounts *ServerCounts
	Partns          map[string][]int
	Retries         int
	HostScanReport   map[string]*HostScanReport
}

type HostScanReport struct {
	SrvrMs     *ServerTimings `json:"srvr_ms,omitempty"`
	SrvrCounts *ServerCounts  `json:"srvr_counts,omitempty"`
	ClientMs   *ClientTimings `json:"client_ms,omitempty"`
}

type ServerTimings struct {
	TotalDur          int64 `json:"total,omitempty"`
	WaitDur           int64 `json:"wait,omitempty"`
	ScanDur           int64 `json:"scan,omitempty"`
	GetSeqnosDur      int64 `json:"getseqnos,omitempty"`
	DiskReadDur       int64 `json:"disk_read,omitempty"`
	DistCompDur       int64 `json:"dist_comp,omitempty"`
	CentroidAssignDur int64 `json:"centroid_assign,omitempty"`
}

type ServerCounts struct {
	RowsReturn  uint64 `json:"rows_return,omitempty"`
	RowsScan    uint64 `json:"rows_scan,omitempty"`
	BytesRead   uint64 `json:"bytes_read,omitempty"`
	CacheHitPer uint64 `json:"cache_hit_per,omitempty"`
}

type ClientTimings struct {
	ScatterDur      uint64 `json:"scatter,omitempty"`
	GatherDur       uint64 `json:"gather,omitempty"`
	GetScanportDur  uint64 `json:"get_scanport,omitempty"`
	ResponseReadDur uint64 `json:"response_read,omitempty"`
}

func (s *ScanReportState) ToMap() map[string]interface{} {
	if s == nil {
		return nil
	}

	m := make(map[string]interface{}, 6)
	m["defn"] = s.DefnID
	if s.SrvrAvgMs != nil {
		m["srvr_avg_ms"] = s.SrvrAvgMs
	}
	if s.SrvrTotalCounts != nil {
		m["srvr_total_counts"] = s.SrvrTotalCounts
	}
	if len(s.Partns) > 0 {
		m["partns"] = s.Partns
	}
	if s.Retries != 0 {
		m["retries"] = s.Retries
	}
	if s.HostScanReport != nil && len(s.HostScanReport) > 0 {
		detailed := make(map[string]interface{}, len(s.HostScanReport))
		for id, detail := range s.HostScanReport {
			detailed[id] = detail
		}
		m["detailed"] = detailed
	}
	return m
}
