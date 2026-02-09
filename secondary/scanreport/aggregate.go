// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package scanreport

import "github.com/couchbase/indexing/secondary/logging"

func (s *ScanReportState) aggregateServerMetrics() {
	if s == nil || len(s.HostScanReport) == 0 {
		return
	}

	if len(s.HostScanReport) == 1 {
		for _, detail := range s.HostScanReport {
			if detail == nil {
				return
			}
			if detail.SrvrNs != nil {
				avgTimings := *detail.SrvrNs
				s.SrvrAvgNs = &avgTimings
			}
			if detail.SrvrCounts != nil {
				totalCounts := *detail.SrvrCounts
				s.SrvrTotalCounts = &totalCounts
			}
			return
		}
	}

	var (
		avgTimings  ServerTimings
		numHosts    int64
		totalCounts ServerCounts
		sumCache    uint64
	)

	for _, detail := range s.HostScanReport {
		if detail == nil {
			continue
		}
		if detail.SrvrNs != nil {
			avgTimings.TotalDur += detail.SrvrNs.TotalDur
			avgTimings.WaitDur += detail.SrvrNs.WaitDur
			avgTimings.ScanDur += detail.SrvrNs.ScanDur
			avgTimings.GetSeqnosDur += detail.SrvrNs.GetSeqnosDur
			avgTimings.DiskReadDur += detail.SrvrNs.DiskReadDur
		}
		if detail.SrvrCounts != nil {
			totalCounts.RowsReturn += detail.SrvrCounts.RowsReturn
			totalCounts.RowsScan += detail.SrvrCounts.RowsScan
			totalCounts.BytesRead += detail.SrvrCounts.BytesRead
			sumCache += detail.SrvrCounts.CacheHitPer
		}
		numHosts++
	}

	if numHosts > 0 {
		s.SrvrAvgNs = &ServerTimings{
			TotalDur:     avgTimings.TotalDur / numHosts,
			WaitDur:      avgTimings.WaitDur / numHosts,
			ScanDur:      avgTimings.ScanDur / numHosts,
			GetSeqnosDur: avgTimings.GetSeqnosDur / numHosts,
			DiskReadDur:  avgTimings.DiskReadDur / numHosts,
		}

		s.SrvrTotalCounts = &ServerCounts{
			RowsReturn:  totalCounts.RowsReturn,
			RowsScan:    totalCounts.RowsScan,
			BytesRead:   totalCounts.BytesRead,
			CacheHitPer: sumCache / uint64(numHosts), // avg percent across indexers
		}
	}
}

// AggregateScanReports aggregates the new report into the existing aggregated report provided by query service.
// If detailed reporting is enabled, the detailed report will be appended to the aggregated report under the
// field "detailed".
// If detailed reporting is not enabled, only aggegated values will be present in the aggregated report.
func AggregateScanReportsFn(aggregatedReport,
	newReport map[string]interface{}) {

	if aggregatedReport == nil || newReport == nil {
		return
	}

	defnID, hasDefn := newReport["defn"]
	if hasDefn {
		existingDefnIDs, exists := aggregatedReport["defn"]
		if !exists {
			aggregatedReport["defn"] = []interface{}{defnID}
		} else {
			ids, ok := existingDefnIDs.([]interface{})
			if !ok {
				logging.Errorf("existingDefnIDs is not []interface{}")
				return
			}

			found := false
			for _, id := range ids {
				if id == defnID {
					found = true
					break
				}
			}

			if !found {
				aggregatedReport["defn"] = append(ids, defnID)
			}
		}
	}

	numScans := uint64(1)
	if existingNumScans, ok := aggregatedReport["num_scans"]; ok {
		existing, ok := existingNumScans.(uint64)
		if !ok {
			logging.Errorf("AggregateScanReportsFn: num_scans is not uint64")
			return
		}
		numScans = existing + 1
	}
	aggregatedReport["num_scans"] = numScans

	if newSrvrAvgNs, ok := newReport["srvr_avg_ns"]; ok {
		aggregateAvgTimings(aggregatedReport, newSrvrAvgNs, numScans)
	}

	if newSrvrTotalCounts, ok := newReport["srvr_total_counts"]; ok {
		aggregateTotalCounts(aggregatedReport, newSrvrTotalCounts, numScans)
	}

	if newPartns, ok := newReport["partns"]; ok {
		aggregatePartitionCounts(aggregatedReport, newPartns)
	}

	if newRetries, ok := newReport["retries"].(int); ok {
		if existingRetries, exists := aggregatedReport["retries"].(int); exists {
			aggregatedReport["retries"] = existingRetries + newRetries
		} else {
			aggregatedReport["retries"] = newRetries
		}
	}

	// "detailed" field is present in the new generated report only in the case of detailed reporting enabled.
	if newReport["detailed"] == nil {
		return
	}

	existingDetailList, exists := aggregatedReport["detailed"]
	if !exists {
		aggregatedReport["detailed"] = []interface{}{newReport}
		return
	}

	detailList, ok := existingDetailList.([]interface{})
	if !ok {
		logging.Errorf("detailed is not []interface{}")
		return
	}
	aggregatedReport["detailed"] = append(detailList, newReport)
}

func aggregateAvgTimings(aggregatedReport map[string]interface{},
	newSrvrAvgNs interface{}, numScans uint64) {

	newTimings, ok := newSrvrAvgNs.(*ServerTimings)
	if !ok {
		logging.Errorf("AggregateScanReportsFn: newSrvrAvgNs is not *ServerTimings")
		return
	}

	existingSrvrAvgNs, exists := aggregatedReport["srvr_avg_ns"]
	if !exists {
		aggregatedReport["srvr_avg_ns"] = copyServerTimings(newTimings)
		return
	}

	existingTimings, ok := existingSrvrAvgNs.(*ServerTimings)
	if !ok {
		logging.Errorf("AggregateScanReportsFn: existingSrvrAvgNs is not *ServerTimings")
		return
	}

	prevCount := int64(numScans) - 1
	if prevCount < 1 {
		prevCount = 1
	}

	existingTimings.TotalDur = (existingTimings.TotalDur*prevCount + newTimings.TotalDur) / int64(numScans)
	existingTimings.WaitDur = (existingTimings.WaitDur*prevCount + newTimings.WaitDur) / int64(numScans)
	existingTimings.ScanDur = (existingTimings.ScanDur*prevCount + newTimings.ScanDur) / int64(numScans)
	existingTimings.GetSeqnosDur = (existingTimings.GetSeqnosDur*prevCount + newTimings.GetSeqnosDur) / int64(numScans)
	existingTimings.DiskReadDur = (existingTimings.DiskReadDur*prevCount + newTimings.DiskReadDur) / int64(numScans)
}

func aggregateTotalCounts(aggregatedReport map[string]interface{},
	newSrvrTotalCounts interface{}, numScans uint64) {

	newCounts, ok := newSrvrTotalCounts.(*ServerCounts)
	if !ok {
		logging.Errorf("AggregateScanReportsFn: newSrvrTotalCounts is not *ServerCounts")
		return
	}

	if !ok {
		return
	}

	existingSrvrTotalCounts, exists := aggregatedReport["srvr_total_counts"]
	if !exists {
		aggregatedReport["srvr_total_counts"] = copyServerCounts(newCounts)
		return
	}

	existingCounts, ok := existingSrvrTotalCounts.(*ServerCounts)
	if !ok {
		logging.Errorf("AggregateScanReportsFn: existingSrvrTotalCounts is not *ServerCounts")
		return
	}

	prevCount := numScans - 1
	if prevCount < 1 {
		prevCount = 1
	}

	existingCounts.RowsReturn += newCounts.RowsReturn
	existingCounts.RowsScan += newCounts.RowsScan
	existingCounts.BytesRead += newCounts.BytesRead
	existingCounts.CacheHitPer = (existingCounts.CacheHitPer*prevCount + newCounts.CacheHitPer) / numScans
}

func aggregatePartitionCounts(aggregatedReport map[string]interface{},
	newPartns interface{}) {

	newPartnsMap, ok := newPartns.(map[string][]int)
	if !ok {
		logging.Errorf("AggregateScanReportsFn: newPartnsMap is not map[string][]int")
		return
	}

	if len(newPartnsMap) == 0 {
		return
	}

	existingPartns, exists := aggregatedReport["partns"]
	if !exists {
		aggregatedReport["partns"] = newPartnsMap
		return
	}

	// If this is the second scan (numScans == 2) i.e. more than one scans, the existingPartns value
	// will be in the form map[string][]int; need to convert it to map[string]map[int]int
	var partnCounts map[string]map[int]int
	switch v := existingPartns.(type) {
	case map[string]map[int]int:
		partnCounts = v
	case map[string][]int:
		partnCounts = make(map[string]map[int]int, len(v))
		for instID, partns := range v {
			partnCounts[instID] = make(map[int]int, len(partns))
			for _, partnID := range partns {
				partnCounts[instID][partnID] = 1
			}
		}
		aggregatedReport["partns"] = partnCounts
	default:
		partnCounts = make(map[string]map[int]int, len(newPartnsMap))
		aggregatedReport["partns"] = partnCounts
	}

	for instID, partns := range newPartnsMap {
		if _, ok := partnCounts[instID]; !ok {
			partnCounts[instID] = make(map[int]int, len(partns))
		}
		for _, partnID := range partns {
			partnCounts[instID][partnID]++
		}
	}
}

func copyServerTimings(st *ServerTimings) *ServerTimings {
	if st == nil {
		return nil
	}
	return &ServerTimings{
		TotalDur:          st.TotalDur,
		WaitDur:           st.WaitDur,
		ScanDur:           st.ScanDur,
		GetSeqnosDur:      st.GetSeqnosDur,
		DiskReadDur:       st.DiskReadDur,
		DistCompDur:       st.DistCompDur,
		CentroidAssignDur: st.CentroidAssignDur,
	}
}

func copyServerCounts(sc *ServerCounts) *ServerCounts {
	if sc == nil {
		return nil
	}
	return &ServerCounts{
		RowsReturn:  sc.RowsReturn,
		RowsScan:    sc.RowsScan,
		BytesRead:   sc.BytesRead,
		CacheHitPer: sc.CacheHitPer,
	}
}
