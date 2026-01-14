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

type ScanReportState struct {
	ReqID string

	// Final report fields
	DefnID          common.IndexDefnId
	SrvrAvgMs       *ServerTimings
	SrvrTotalCounts *ServerCounts
	Partns          map[string][]int
	Retries         int
	HostScanReport  map[string]*HostScanReport
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
