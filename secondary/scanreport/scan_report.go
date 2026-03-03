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
	"fmt"

	"github.com/couchbase/indexing/secondary/common"
)

// ScanReportState holds the aggregate state of scan reports.
type ScanReportState struct {
	ReqID string

	HostScanReport  map[string]*HostScanReport
	SrvrAvgNs       *ServerTimings
	SrvrTotalCounts *ServerCounts
	Partns          map[string][]int
	DefnID          common.IndexDefnId
	Retries         int

	// Padding for 8-byte alignment (4 bytes)
	_ [4]byte
}

// HostScanReport contains per-host scan metrics.
type HostScanReport struct {
	SrvrNs     *ServerTimings `json:"srvr_ns,omitempty"`
	SrvrCounts *ServerCounts  `json:"srvr_counts,omitempty"`
	ClientNs   *ClientTimings `json:"client_ns,omitempty"`
}

// ServerTimings contains server-side timing metrics.
type ServerTimings struct {
	TotalDur     int64 `json:"total,omitempty"`
	WaitDur      int64 `json:"wait,omitempty"`
	ScanDur      int64 `json:"scan,omitempty"`
	GetSeqnosDur int64 `json:"getseqnos,omitempty"`
	DiskReadDur  int64 `json:"diskRead,omitempty"`

	AvgDecodeDur   int64 `json:"decode,omitempty"`
	AvgDistCompDur int64 `json:"distComp,omitempty"`
}

// ServerCounts contains server-side count metrics.
type ServerCounts struct {
	RowsReturn  uint64 `json:"rowsReturn,omitempty"`
	RowsScan    uint64 `json:"rowsScan,omitempty"`
	BytesRead   uint64 `json:"bytesRead,omitempty"`
	CacheHitPer uint64 `json:"cacheHitPer,omitempty"`

	RowsFiltered uint64 `json:"rowsFiltered,omitempty"`
	RowsReranked uint64 `json:"rowsReranked,omitempty"`
}

// ClientTimings contains client-side timing metrics.
// Not set yet.
type ClientTimings struct {
	ScatterDur      uint64 `json:"scatter,omitempty"`
	GatherDur       uint64 `json:"gather,omitempty"`
	GetScanportDur  uint64 `json:"getScanport,omitempty"`
	ResponseReadDur uint64 `json:"responseRead,omitempty"`
}

func NewScanReportState(reqID string, defnID common.IndexDefnId) *ScanReportState {
	return &ScanReportState{
		ReqID:  reqID,
		DefnID: defnID,
	}
}

// Format: <instId>[<partnId0>,<partnId1>,...]
func GenPerClientReportId(instId uint64, partnIDs []common.PartitionId) string {
	s := fmt.Sprintf("%d[", instId)
	for i, p := range partnIDs {
		if i > 0 {
			s += ","
		}
		s += fmt.Sprintf("%d", p)
	}
	s += "]"
	return s
}

func BuildPartnsMap(instIds []uint64, partnIdsByInst [][]common.PartitionId) map[string][]int {
	if len(instIds) == 0 || len(partnIdsByInst) == 0 {
		return nil
	}

	partnMap := make(map[string][]int, len(instIds))
	for i := 0; i < len(instIds); i++ {
		instKey := fmt.Sprintf("%d", instIds[i])
		dst := make([]int, 0, len(partnIdsByInst[i]))
		for _, p := range partnIdsByInst[i] {
			dst = append(dst, int(p))
		}

		if _, ok := partnMap[instKey]; ok {
			partnMap[instKey] = append(partnMap[instKey], dst...)
		} else {
			partnMap[instKey] = dst
		}
	}

	return partnMap
}

// Format: "partns": { "<instId>": [<partnId1>, <partnId2>, ...], ... }
func (s *ScanReportState) PopulatePartns(index *common.IndexDefn, instIds []uint64, partnIdsByInst [][]common.PartitionId) {
	// Only populate the partn map if the index is partitioned or has numReplica > 1.
	if index.GetNumReplica() <= 1 && len(index.PartitionKeys) == 0 {
		return
	}

	partns := BuildPartnsMap(instIds, partnIdsByInst)
	s.Partns = partns
}

func (s *ScanReportState) ToMap(includeDetailed bool) map[string]interface{} {
	if s == nil {
		return nil
	}

	m := make(map[string]interface{}, 6)
	m["defn"] = s.DefnID

	// Compute aggregates from per-host reports for concise report
	s.aggregateServerMetrics()

	if s.SrvrAvgNs != nil {
		m["srvr_avg_ns"] = s.SrvrAvgNs
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
	if includeDetailed && s.HostScanReport != nil && len(s.HostScanReport) > 0 {
		detailed := make(map[string]interface{}, len(s.HostScanReport))
		for id, detail := range s.HostScanReport {
			detailed[id] = detail
		}
		m["detailed"] = detailed
	}
	return m
}
