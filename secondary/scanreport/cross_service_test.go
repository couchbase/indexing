package scanreport_test

import (
	"testing"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	protoQuery "github.com/couchbase/indexing/secondary/protobuf/query"
	client "github.com/couchbase/indexing/secondary/queryport/client"
	report "github.com/couchbase/indexing/secondary/scanreport"
	"github.com/couchbase/query/auth"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/tenant"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

// stubContext provides a minimal implementation of datastore.Context for testing.
// It allows controlling scan report wait time and scan capacity.
type stubContext struct {
	wait    time.Duration
	scanCap int64
}

func (c *stubContext) GetScanCap() int64 {
	if c.scanCap > 0 {
		return c.scanCap
	}
	return datastore.GetScanCap()
}

func (c *stubContext) MaxParallelism() int            { return 1 }
func (c *stubContext) Fatal(errors.Error)             {}
func (c *stubContext) Error(errors.Error)             {}
func (c *stubContext) Warning(errors.Error)           {}
func (c *stubContext) GetErrors() []errors.Error      { return nil }
func (c *stubContext) GetReqDeadline() time.Time      { return time.Time{} }
func (c *stubContext) TenantCtx() tenant.Context      { return nil }
func (c *stubContext) SetFirstCreds(string)           {}
func (c *stubContext) FirstCreds() (string, bool)     { return "", true }
func (c *stubContext) RecordFtsRU(tenant.Unit)        {}
func (c *stubContext) RecordGsiRU(tenant.Unit)        {}
func (c *stubContext) RecordKvRU(tenant.Unit)         {}
func (c *stubContext) RecordKvWU(tenant.Unit)         {}
func (c *stubContext) Credentials() *auth.Credentials { return auth.NewCredentials() }
func (c *stubContext) ScanReportWait() time.Duration  { return c.wait }
func (c *stubContext) SkipKey(string) bool            { return false }

// TestStreamEndResponseGetServerScanReport verifies that scan report data
// embedded in a protobuf StreamEndResponse can be correctly extracted via
// GetServerScanReport(). It tests the conversion from protobuf types to
// Go structs.
func TestStreamEndResponseGetServerScanReport(t *testing.T) {
	res := &protoQuery.StreamEndResponse{
		SrvrScanReport: &protoQuery.SrvrScanReport{
			ServerTimings: &protoQuery.ServerTimings{TotalDur: proto.Int64(500)},
			ServerCounts:  &protoQuery.ServerCounts{RowsReturn: proto.Uint64(3)},
		},
	}
	recovered := res.GetServerScanReport()
	require.NotNil(t, recovered)
	require.EqualValues(t, 500, recovered.SrvrNs.TotalDur)
	require.EqualValues(t, 3, recovered.SrvrCounts.RowsReturn)
}

// TestRequestBrokerInitAndAttachScanReport verifies the RequestBroker's
// ability to initialize scan reports and attach indexer reports to them.
// It tests the flow: SetScanReport -> SetPerHostReportIds -> AttachIndexerScanReport.
func TestRequestBrokerInitAndAttachScanReport(t *testing.T) {
	defnID, err := common.NewIndexDefnId()
	require.NoError(t, err)

	broker := client.NewRequestBroker("req", 0, 0)
	broker.SetScanReport("req", defnID, true)
	broker.SetPerHostReportIds([]string{"inst[0]"})

	sr := broker.GetScanReport()
	sr.HostScanReport = map[string]*report.HostScanReport{"inst[0]": {}}

	report := &report.HostScanReport{
		SrvrNs:     &report.ServerTimings{TotalDur: 1234},
		SrvrCounts: &report.ServerCounts{RowsReturn: 55},
	}
	broker.AttachIndexerScanReport(report, 0)

	require.EqualValues(t, 1234, sr.HostScanReport["inst[0]"].SrvrNs.TotalDur)
	require.EqualValues(t, 55, sr.HostScanReport["inst[0]"].SrvrCounts.RowsReturn)
}

// TestRequestBrokerSendFinalReportDetailedToggle tests SendFinalReport in both
// concise and detailed modes. When detailed mode is enabled (ScanReportWait > 0),
// the report should include per-host details wrapped in the "detailed" field.
// In concise mode, only aggregated metrics should be present.
//
// The test runs two cases:
// - concise: detailed=false, wait=0 -> aggregated metrics only
// - detailed: detailed=true, wait=10ms -> aggregated metrics + per-host details
func TestRequestBrokerSendFinalReportDetailedToggle(t *testing.T) {
	for name, detailed := range map[string]bool{"concise": false, "detailed": true} {
		t.Run(name, func(t *testing.T) {
			defnID, err := common.NewIndexDefnId()
			require.NoError(t, err)

			broker := client.NewRequestBroker("req", 0, 0)
			broker.SetScanReport("req", defnID, true)
			broker.SetPerHostReportIds([]string{"inst[0]"})

			sr := broker.GetScanReport()
			sr.HostScanReport = map[string]*report.HostScanReport{
				"inst[0]": {
					SrvrNs:     &report.ServerTimings{TotalDur: 1000, WaitDur: 100},
					SrvrCounts: &report.ServerCounts{RowsReturn: 10, RowsScan: 15},
				},
			}

			ctx := &stubContext{}
			if detailed {
				ctx.wait = 10 * time.Millisecond
			}
			conn := datastore.NewIndexConnection(ctx)
			scanReport := datastore.NewIndexScanReport()
			conn.SetIndexScanReport(scanReport)

			includeDetailed := conn.IsDetailedIndexScanReport()
			require.Equal(t, detailed, includeDetailed, "Detailed mode should match context wait")

			newReport := sr.ToMap(includeDetailed)
			if detailed {
				require.Contains(t, newReport, "detailed", "ToMap should include detailed field when detailed mode enabled")
			} else {
				require.NotContains(t, newReport, "detailed", "ToMap should not include detailed field in concise mode")
			}

			broker.SendFinalReport(conn)

			scanReport.RLock()
			result := scanReport.ScanReport
			avg := result["srvr_avg_ns"].(*report.ServerTimings)
			counts := result["srvr_total_counts"].(*report.ServerCounts)
			detailedData, hasDetailed := result["detailed"]
			scanReport.RUnlock()

			if detailed {
				require.True(t, hasDetailed, "Detailed field should exist when detailed mode is enabled")
				require.IsType(t, []interface{}{}, detailedData, "Detailed field stores aggregated reports in a slice")
				detailedList := detailedData.([]interface{})
				require.Len(t, detailedList, 1, "Should have one aggregated report")
				aggregatedReport := detailedList[0].(map[string]interface{})
				require.Contains(t, aggregatedReport, "defn")
				require.Contains(t, aggregatedReport, "detailed")
				detailedMap := aggregatedReport["detailed"].(map[string]interface{})
				require.Contains(t, detailedMap, "inst[0]", "Detailed map should contain inst[0]")
			} else {
				require.False(t, hasDetailed, "Detailed field should not exist in concise mode")
			}

			require.EqualValues(t, 1000, avg.TotalDur)
			require.EqualValues(t, 100, avg.WaitDur)
			require.EqualValues(t, 10, counts.RowsReturn)
			require.EqualValues(t, 15, counts.RowsScan)
		})
	}
}

// TestRequestBrokerAggregatesMultipleHosts verifies that the RequestBroker
// correctly aggregates metrics from multiple indexer hosts. It tests:
// - Average of server timings across hosts
// - Sum of row counts, bytes read across hosts
// - Average of cache hit percentage across hosts
func TestRequestBrokerAggregatesMultipleHosts(t *testing.T) {
	hostReports := []*report.HostScanReport{
		{SrvrNs: &report.ServerTimings{TotalDur: 1000, WaitDur: 100, ScanDur: 800}, SrvrCounts: &report.ServerCounts{RowsReturn: 10, RowsScan: 12, BytesRead: 1024, CacheHitPer: 80}},
		{SrvrNs: &report.ServerTimings{TotalDur: 2000, WaitDur: 150, ScanDur: 1600}, SrvrCounts: &report.ServerCounts{RowsReturn: 20, RowsScan: 22, BytesRead: 2048, CacheHitPer: 90}},
	}

	defnID, err := common.NewIndexDefnId()
	require.NoError(t, err)
	instID1, err := common.NewIndexInstId()
	require.NoError(t, err)
	instID2, err := common.NewIndexInstId()
	require.NoError(t, err)

	broker := client.NewRequestBroker("req", 0, 0)
	broker.SetScanReport("req", defnID, true)
	hostIDs := []string{
		report.GenPerClientReportId(uint64(instID1), []common.PartitionId{0}),
		report.GenPerClientReportId(uint64(instID2), []common.PartitionId{1}),
	}
	broker.SetPerHostReportIds(hostIDs)

	brokerSR := broker.GetScanReport()
	brokerSR.HostScanReport = make(map[string]*report.HostScanReport, len(hostIDs))
	for _, id := range hostIDs {
		brokerSR.HostScanReport[id] = &report.HostScanReport{}
	}
	for idx, hr := range hostReports {
		broker.AttachIndexerScanReport(hr, idx)
	}

	ctx := &stubContext{wait: time.Millisecond}
	conn := datastore.NewIndexConnection(ctx)
	sr := datastore.NewIndexScanReport()
	conn.SetIndexScanReport(sr)

	broker.SendFinalReport(conn)

	sr.RLock()
	result := sr.ScanReport
	sr.RUnlock()

	avg := result["srvr_avg_ns"].(*report.ServerTimings)
	require.EqualValues(t, 1500, avg.TotalDur)
	require.EqualValues(t, 125, avg.WaitDur)
	tot := result["srvr_total_counts"].(*report.ServerCounts)
	require.EqualValues(t, 30, tot.RowsReturn)
	require.EqualValues(t, 34, tot.RowsScan)
	require.EqualValues(t, 3072, tot.BytesRead)
	require.EqualValues(t, 85, tot.CacheHitPer)
}

// Query side aggregation tests

type datastoreContext struct {
	stubContext
}

func (d *datastoreContext) ScanReportWait() time.Duration { return d.wait }

// TestIndexConnectionScanReportControls verifies that IsDetailedIndexScanReport()
// correctly detects the detailed mode setting based on ScanReportWait().
// - Non-zero wait: detailed mode enabled
// - Zero wait: concise mode
func TestIndexConnectionScanReportControls(t *testing.T) {
	conn := datastore.NewIndexConnection(&datastoreContext{stubContext{wait: time.Millisecond}})
	require.True(t, conn.IsDetailedIndexScanReport())
	conn = datastore.NewIndexConnection(&datastoreContext{})
	require.False(t, conn.IsDetailedIndexScanReport())
}

// TestIndexConnectionAggregateScanReport tests aggregation of multiple scan reports
// at the query service level. It verifies:
// - Two reports with different index definitions are correctly aggregated
// - Server timings are averaged (100ns + 200ns = 150ns average)
// - Row counts are cumulative (10 + 15 = 25)
// - Retries are cumulative (1 + 2 = 3)
// - Partition maps convert to histogram format after multiple aggregations
func TestIndexConnectionAggregateScanReport(t *testing.T) {
	defnID1, err := common.NewIndexDefnId()
	require.NoError(t, err)
	defnID2, err := common.NewIndexDefnId()
	require.NoError(t, err)

	conn := datastore.NewIndexConnection(&datastoreContext{})
	sr := datastore.NewIndexScanReport()
	conn.SetIndexScanReport(sr)
	conn.AggregateScanReport(report.AggregateScanReportsFn, map[string]interface{}{
		"defn":              uint64(defnID1),
		"srvr_avg_ns":       &report.ServerTimings{TotalDur: 100},
		"srvr_total_counts": &report.ServerCounts{RowsReturn: 10},
		"partns":            map[string][]int{"inst[0]": {0, 1}},
		"retries":           1,
	})
	conn.AggregateScanReport(report.AggregateScanReportsFn, map[string]interface{}{
		"defn":              uint64(defnID2),
		"srvr_avg_ns":       &report.ServerTimings{TotalDur: 200},
		"srvr_total_counts": &report.ServerCounts{RowsReturn: 15},
		"partns":            map[string][]int{"inst[1]": {2, 3}},
		"retries":           2,
	})

	rv := sr.GetScanReport()
	require.NotEmpty(t, rv)
	defnIDs := rv["defn"].([]interface{})
	require.Len(t, defnIDs, 2)
	srvrAvgNs := rv["srvr_avg_ns"].(map[string]interface{})
	require.EqualValues(t, 150, srvrAvgNs["total"])
	srvrTotalCounts := rv["srvr_total_counts"].(map[string]interface{})
	require.EqualValues(t, 25, srvrTotalCounts["rows_return"])
	require.EqualValues(t, 3, rv["retries"])
	partns := rv["partns"].(map[string]interface{})
	inst0Partns := partns["inst[0]"].(map[string]interface{})
	require.EqualValues(t, float64(1), inst0Partns["0"])
	require.EqualValues(t, float64(1), inst0Partns["1"])
	inst1Partns := partns["inst[1]"].(map[string]interface{})
	require.EqualValues(t, float64(1), inst1Partns["2"])
	require.EqualValues(t, float64(1), inst1Partns["3"])
}

// TestIndexConnectionNoScanReportOnWaitExhausted verifies behavior when
// the wait time for detailed scan report is exhausted. When the wait time
// elapses before the final report can be sent, the system should:
// - Not panic or error
// - The broker retains its HostScanReport (not automatically cleared)
func TestIndexConnectionNoScanReportOnWaitExhausted(t *testing.T) {
	defnID, err := common.NewIndexDefnId()
	require.NoError(t, err)

	broker := client.NewRequestBroker("req", 0, 0)
	broker.SetScanReport("req", defnID, true)
	broker.SetPerHostReportIds([]string{"inst[0]"})

	sr := broker.GetScanReport()
	sr.HostScanReport = map[string]*report.HostScanReport{
		"inst[0]": {
			SrvrNs:     &report.ServerTimings{TotalDur: 1000, WaitDur: 100},
			SrvrCounts: &report.ServerCounts{RowsReturn: 10, RowsScan: 15},
		},
	}

	waitTime := 1 * time.Microsecond
	ctx := &datastoreContext{stubContext{wait: waitTime}}
	conn := datastore.NewIndexConnection(ctx)
	sender := conn.Sender()

	done := make(chan struct{})
	go func() {
		defer sender.Close()
		defer close(done)

		time.Sleep(2 * time.Microsecond)
		broker.SendFinalReport(conn)
	}()

	conn.SendStop()
	conn.WaitScanReport(ctx.ScanReportWait())

	report := broker.GetScanReport()

	require.NotNil(t, report, "Scan report should not be nil")
	brokerHostReport := report.HostScanReport
	require.NotNil(t, brokerHostReport, "Broker's host scan report remains set")
	require.Contains(t, brokerHostReport, "inst[0]")

	<-done
}
