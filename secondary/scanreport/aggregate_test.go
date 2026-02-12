package scanreport

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAggregateServerMetricsMultipleHosts verifies the aggregateServerMetrics method
// correctly aggregates metrics from multiple indexer hosts. For 2 hosts:
// - Server timings are averaged
// - Row counts, bytes read are summed
// - Cache hit percentage is averaged
func TestAggregateServerMetricsMultipleHosts(t *testing.T) {
	sr := &ScanReportState{
		HostScanReport: map[string]*HostScanReport{
			"host1": {
				SrvrNs:     &ServerTimings{TotalDur: 1000, WaitDur: 100, ScanDur: 800, GetSeqnosDur: 50, DiskReadDur: 60},
				SrvrCounts: &ServerCounts{RowsReturn: 500, RowsScan: 600, BytesRead: 1024, CacheHitPer: 80},
			},
			"host2": {
				SrvrNs:     &ServerTimings{TotalDur: 2000, WaitDur: 200, ScanDur: 1600, GetSeqnosDur: 75, DiskReadDur: 90},
				SrvrCounts: &ServerCounts{RowsReturn: 700, RowsScan: 800, BytesRead: 2048, CacheHitPer: 90},
			},
		},
	}
	sr.aggregateServerMetrics()
	require.EqualValues(t, 1500, sr.SrvrAvgNs.TotalDur)
	require.EqualValues(t, 150, sr.SrvrAvgNs.WaitDur)
	require.EqualValues(t, 1200, sr.SrvrAvgNs.ScanDur)
	require.EqualValues(t, 62, sr.SrvrAvgNs.GetSeqnosDur)
	require.EqualValues(t, 75, sr.SrvrAvgNs.DiskReadDur)
	require.EqualValues(t, 1200, sr.SrvrTotalCounts.RowsReturn)
	require.EqualValues(t, 1400, sr.SrvrTotalCounts.RowsScan)
	require.EqualValues(t, 3072, sr.SrvrTotalCounts.BytesRead)
	require.EqualValues(t, 85, sr.SrvrTotalCounts.CacheHitPer)
}

// TestAggregateServerMetricsHandlesNil verifies that aggregateServerMetrics
// handles a nil ScanReportState pointer without panicking.
func TestAggregateServerMetricsHandlesNil(t *testing.T) {
	var sr *ScanReportState
	sr.aggregateServerMetrics()
}

// TestAggregateServerMetricsEmptyHostReports verifies that aggregateServerMetrics
// handles an empty HostScanReport map by leaving aggregate fields nil.
func TestAggregateServerMetricsEmptyHostReports(t *testing.T) {
	sr := &ScanReportState{HostScanReport: map[string]*HostScanReport{}}
	sr.aggregateServerMetrics()
	require.Nil(t, sr.SrvrAvgNs)
	require.Nil(t, sr.SrvrTotalCounts)
}

// TestAggregateServerMetricsPartialNilHosts verifies that aggregateServerMetrics
// correctly handles partial reports where some fields are nil. Only non-nil
// fields are aggregated.
func TestAggregateServerMetricsPartialNilHosts(t *testing.T) {
	sr := &ScanReportState{
		HostScanReport: map[string]*HostScanReport{
			"host1": {SrvrNs: &ServerTimings{TotalDur: 1000}},
			"host2": {SrvrCounts: &ServerCounts{RowsReturn: 100}},
		},
	}
	sr.aggregateServerMetrics()
	require.EqualValues(t, 500, sr.SrvrAvgNs.TotalDur)
	require.EqualValues(t, 100, sr.SrvrTotalCounts.RowsReturn)
}

// TestAggregateScanReportsFnFirstScan tests the AggregateScanReportsFn function
// with the first scan report (empty aggregated map). It verifies that:
// - The defnID is stored in a slice format
// - num_scans is initialized to 1
// - Server timings and counts are copied
// - Partitions are stored as a list
// - Retries are recorded
func TestAggregateScanReportsFnFirstScan(t *testing.T) {
	aggregated := make(map[string]interface{})
	newReport := map[string]interface{}{
		"defn":              uint64(12345),
		"srvr_avg_ns":       &ServerTimings{TotalDur: 1000},
		"srvr_total_counts": &ServerCounts{RowsReturn: 100},
		"partns":            map[string][]int{"1001": {0, 1}},
		"retries":           1,
	}
	AggregateScanReportsFn(aggregated, newReport)
	require.EqualValues(t, []interface{}{uint64(12345)}, aggregated["defn"])
	require.EqualValues(t, uint64(1), aggregated["num_scans"])
	require.EqualValues(t, 1000, aggregated["srvr_avg_ns"].(*ServerTimings).TotalDur)
	require.EqualValues(t, 100, aggregated["srvr_total_counts"].(*ServerCounts).RowsReturn)
	require.Equal(t, map[string][]int{"1001": {0, 1}}, aggregated["partns"])
	require.Equal(t, 1, aggregated["retries"])
}

// TestAggregateScanReportsFnSecondScan tests the AggregateScanReportsFn function
// with the second scan report. It verifies running aggregation where:
// - num_scans is incremented to 2
// - Server timings are updated with running average
// - Row counts are cumulative sum
// - Partitions are converted to histogram format (map[string]map[int]int)
// - Retries are summed
func TestAggregateScanReportsFnSecondScan(t *testing.T) {
	aggregated := map[string]interface{}{
		"defn":              []interface{}{uint64(12345)},
		"num_scans":         uint64(1),
		"srvr_avg_ns":       &ServerTimings{TotalDur: 1000, WaitDur: 100},
		"srvr_total_counts": &ServerCounts{RowsReturn: 100, RowsScan: 120},
		"partns":            map[string][]int{"1001": {0, 1}},
		"retries":           1,
	}
	newReport := map[string]interface{}{
		"defn":              uint64(12345),
		"srvr_avg_ns":       &ServerTimings{TotalDur: 2000, WaitDur: 200},
		"srvr_total_counts": &ServerCounts{RowsReturn: 200, RowsScan: 220, CacheHitPer: 90},
		"partns":            map[string][]int{"1001": {0, 2}},
		"retries":           0,
	}
	AggregateScanReportsFn(aggregated, newReport)
	require.EqualValues(t, uint64(2), aggregated["num_scans"])
	require.EqualValues(t, 1500, aggregated["srvr_avg_ns"].(*ServerTimings).TotalDur)
	require.EqualValues(t, 150, aggregated["srvr_avg_ns"].(*ServerTimings).WaitDur)
	require.EqualValues(t, 300, aggregated["srvr_total_counts"].(*ServerCounts).RowsReturn)
	require.EqualValues(t, 340, aggregated["srvr_total_counts"].(*ServerCounts).RowsScan)
	partns := aggregated["partns"].(map[string]map[int]int)
	require.Equal(t, 2, partns["1001"][0])
	require.Equal(t, 1, partns["1001"][1])
	require.Equal(t, 1, partns["1001"][2])
}

// TestAggregateScanReportsFnNilInputs verifies that AggregateScanReportsFn
// handles nil inputs gracefully without panicking.
func TestAggregateScanReportsFnNilInputs(t *testing.T) {
	AggregateScanReportsFn(nil, map[string]interface{}{})
	AggregateScanReportsFn(map[string]interface{}{}, nil)
}

// TestAggregateScanReportsFnDetailedMode tests that when detailed mode is enabled,
// the "detailed" field containing per-host reports is accumulated in a slice.
// Each new report with a "detailed" field is appended to the existing slice.
func TestAggregateScanReportsFnDetailedMode(t *testing.T) {
	aggregated := map[string]interface{}{
		"defn":      []interface{}{uint64(12345)},
		"num_scans": uint64(1),
		"detailed":  []interface{}{map[string]interface{}{"scan": 1}},
	}
	newReport := map[string]interface{}{
		"defn":     uint64(12345),
		"detailed": map[string]interface{}{"scan": 2},
	}
	AggregateScanReportsFn(aggregated, newReport)
	detailed := aggregated["detailed"].([]interface{})
	require.Len(t, detailed, 2)
	entry := detailed[1].(map[string]interface{})
	require.EqualValues(t, uint64(12345), entry["defn"])
	entryDetailed := entry["detailed"].(map[string]interface{})
	require.Equal(t, 2, entryDetailed["scan"])
}

// TestAggregatePartitionCountsConversion tests the partition counts aggregation
// when converting from list format to histogram format.
// From: ["1001": {0, 1}] and adding ["1001": {1, 2}]
// To:   ["1001": {0: 1, 1: 2, 2: 1}]
// Shows that partition 1 was accessed in both scans.
func TestAggregatePartitionCountsConversion(t *testing.T) {
	aggregated := map[string]interface{}{"partns": map[string][]int{"1001": {0, 1}}}
	newPartns := map[string][]int{"1001": {1, 2}}
	aggregatePartitionCounts(aggregated, newPartns)
	counts := aggregated["partns"].(map[string]map[int]int)
	require.Equal(t, 1, counts["1001"][0])
	require.Equal(t, 2, counts["1001"][1])
	require.Equal(t, 1, counts["1001"][2])
}

// TestAggregatePartitionCountsFirstScan tests the partition counts aggregation
// on the first scan, where the list format is preserved.
func TestAggregatePartitionCountsFirstScan(t *testing.T) {
	aggregated := map[string]interface{}{}
	newPartns := map[string][]int{"1001": {0, 1, 2}}
	aggregatePartitionCounts(aggregated, newPartns)
	require.Equal(t, newPartns, aggregated["partns"])
}

// TestAggregatePartitionCountsIgnoresEmpty verifies that aggregatePartitionCounts
// handles empty partition maps gracefully by not modifying the aggregated value.
func TestAggregatePartitionCountsIgnoresEmpty(t *testing.T) {
	aggregated := map[string]interface{}{}
	aggregatePartitionCounts(aggregated, map[string][]int{})
	require.NotContains(t, aggregated, "partns")
}

// TestAggregateAvgTimings tests the running average calculation for server timings.
// With existing TotalDur=1000 and new TotalDur=3000 (2nd scan),
// the result should be (1000 + 3000) / 2 = 2000.
func TestAggregateAvgTimings(t *testing.T) {
	aggregated := map[string]interface{}{
		"srvr_avg_ns": &ServerTimings{TotalDur: 1000},
	}
	newTimings := &ServerTimings{TotalDur: 3000}
	aggregateAvgTimings(aggregated, newTimings, 2)
	require.EqualValues(t, 2000, aggregated["srvr_avg_ns"].(*ServerTimings).TotalDur)
}

// TestAggregateTotalCounts tests the cumulative sum calculation for server counts.
// - RowsReturn: 100 + 200 = 300 (cumulative)
// - RowsScan: 120 + 250 = 370 (cumulative)
// - CacheHitPer: (80*1 + 90*1) / 2 = 85 (running average)
func TestAggregateTotalCounts(t *testing.T) {
	aggregated := map[string]interface{}{
		"srvr_total_counts": &ServerCounts{RowsReturn: 100, RowsScan: 120, CacheHitPer: 80},
	}
	newCounts := &ServerCounts{RowsReturn: 200, RowsScan: 250, CacheHitPer: 90}
	aggregateTotalCounts(aggregated, newCounts, 2)
	require.EqualValues(t, 300, aggregated["srvr_total_counts"].(*ServerCounts).RowsReturn)
	require.EqualValues(t, 370, aggregated["srvr_total_counts"].(*ServerCounts).RowsScan)
	require.EqualValues(t, 85, aggregated["srvr_total_counts"].(*ServerCounts).CacheHitPer)
}

// TestCopyServerStructs verifies that the copyServerTimings and copyServerCounts
// functions create deep copies (not shallow copies) of the structs.
// This ensures that modifying the copy doesn't affect the original.
func TestCopyServerStructs(t *testing.T) {
	origTimings := &ServerTimings{TotalDur: 1000}
	copyTimings := copyServerTimings(origTimings)
	require.NotSame(t, origTimings, copyTimings)
	require.Equal(t, origTimings.TotalDur, copyTimings.TotalDur)

	origCounts := &ServerCounts{RowsReturn: 10}
	copyCounts := copyServerCounts(origCounts)
	require.NotSame(t, origCounts, copyCounts)
	require.Equal(t, origCounts.RowsReturn, copyCounts.RowsReturn)
}
