package scanreport

import (
	"fmt"
	"testing"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/stretchr/testify/require"
)

func createTestIndexInstIds(numInstances int) ([]uint64, error) {
	var instIds []uint64
	for range numInstances {
		id, err := common.NewIndexInstId()
		if err != nil {
			return nil, err
		}
		instIds = append(instIds, uint64(id))
	}
	return instIds, nil
}

// TestNewScanReportStateInitializesFields verifies that NewScanReportState
// creates a new ScanReportState with the correct ReqID and DefnID values,
// and initializes HostScanReport to nil.
func TestNewScanReportStateInitializesFields(t *testing.T) {
	reqID := "req-123"
	defnID, err := common.NewIndexDefnId()
	require.NoError(t, err)

	state := NewScanReportState(reqID, defnID)
	require.Equal(t, reqID, state.ReqID)
	require.Equal(t, defnID, state.DefnID)
	require.Nil(t, state.HostScanReport)
}

// TestGenPerClientReportId verifies that GenPerClientReportId generates
// the correct unique report ID string for each indexer instance/partition combination.
// The format is "instID[partition1,partition2,...]".
func TestGenPerClientReportId(t *testing.T) {
	indexInstIds, err := createTestIndexInstIds(3)
	require.NoError(t, err)

	tests := []struct {
		name     string
		instID   uint64
		partnIDs []common.PartitionId
		expected string
	}{
		{"multiple", indexInstIds[0], []common.PartitionId{0, 1, 2}, fmt.Sprintf("%d[0,1,2]", indexInstIds[0])},
		{"single", indexInstIds[1], []common.PartitionId{7}, fmt.Sprintf("%d[7]", indexInstIds[1])},
		{"empty", indexInstIds[2], nil, fmt.Sprintf("%d[]", indexInstIds[2])},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, GenPerClientReportId(tt.instID, tt.partnIDs))
		})
	}
}

// TestBuildPartnsMap verifies that BuildPartnsMap correctly constructs
// the partition mapping from instance IDs and their partition lists.
// It creates a map from string instance IDs to their partition lists.
func TestBuildPartnsMap(t *testing.T) {
	indexInstIds, err := createTestIndexInstIds(2)
	require.NoError(t, err)

	partns := [][]common.PartitionId{{0, 1}, {0, 1, 2}}
	result := BuildPartnsMap(indexInstIds, partns)
	require.Equal(t, []int{0, 1}, result[fmt.Sprintf("%d", indexInstIds[0])])
	require.Equal(t, []int{0, 1, 2}, result[fmt.Sprintf("%d", indexInstIds[1])])
}

// TestBuildPartnsMapDeduplicatesInstances verifies that when the same instance
// appears multiple times in the input, BuildPartnsMap merges their partitions
// and deduplicates the instance in the output map.
func TestBuildPartnsMapDeduplicatesInstances(t *testing.T) {
	id, error := common.NewIndexInstId()
	if error != nil {
		t.Fatalf("TestBuildPartnsMapDeduplicatesInstances Failed to create test index instance ID: %v", error)
	}
	indexInstIds := []uint64{uint64(id), uint64(id)}
	partns := [][]common.PartitionId{{0, 1}, {2, 3}}
	result := BuildPartnsMap(indexInstIds, partns)
	require.Equal(t, []int{0, 1, 2, 3}, result[fmt.Sprintf("%d", indexInstIds[0])])
}

// TestBuildPartnsMapEmptyInput verifies that BuildPartnsMap handles nil
// inputs gracefully by returning nil.
func TestBuildPartnsMapEmptyInput(t *testing.T) {
	require.Nil(t, BuildPartnsMap(nil, nil))
}

// TestPopulatePartnsPartitionedIndex verifies that PopulatePartns correctly
// populates the Partns field for a partitioned index (NumReplica=1, has PartitionKeys).
// The partitions should be mapped to the instance ID string.
func TestPopulatePartnsPartitionedIndex(t *testing.T) {
	reqID := "req-123"
	defnID, err := common.NewIndexDefnId()
	require.NoError(t, err)

	state := NewScanReportState(reqID, defnID)

	indexInstIds, err := createTestIndexInstIds(1)
	require.NoError(t, err)

	index := &common.IndexDefn{NumReplica: 1, PartitionKeys: []string{"type"}}
	instIDs := indexInstIds
	partns := [][]common.PartitionId{{0, 1}}
	state.PopulatePartns(index, instIDs, partns)
	require.Equal(t, map[string][]int{fmt.Sprintf("%d", indexInstIds[0]): []int{0, 1}}, state.Partns)
}

// TestPopulatePartnsSimpleIndexSkips verifies that PopulatePartns skips
// populating partitions for a simple (non-partitioned, non-replicated) index.
// For NumReplica=1 without PartitionKeys, Partns should remain nil.
func TestPopulatePartnsSimpleIndexSkips(t *testing.T) {
	reqID := "req-123"
	defnID, err := common.NewIndexDefnId()
	require.NoError(t, err)

	state := NewScanReportState(reqID, defnID)
	index := &common.IndexDefn{NumReplica: 1}
	state.PopulatePartns(index, nil, nil)
	require.Nil(t, state.Partns)
}

// TestToMapConcise verifies that ToMap with includeDetailed=false generates
// a concise report map without the per-host "detailed" field.
// The map should contain: defn, srvr_avg_ns, srvr_total_counts, and optionally partns/retries.
func TestToMapConcise(t *testing.T) {
	defnID, err := common.NewIndexDefnId()
	require.NoError(t, err)

	state := &ScanReportState{
		DefnID: defnID,
		HostScanReport: map[string]*HostScanReport{
			"host1": {
				SrvrNs:     &ServerTimings{TotalDur: 1000, WaitDur: 100},
				SrvrCounts: &ServerCounts{RowsReturn: 10, RowsScan: 20, CacheHitPer: 80},
			},
		},
	}
	result := state.ToMap(false)
	require.NotNil(t, result)
	require.EqualValues(t, defnID, result["defn"])
	require.Contains(t, result, "srvr_avg_ns")
	require.Contains(t, result, "srvr_total_counts")
	require.NotContains(t, result, "detailed")
}

// TestToMapDetailed verifies that ToMap with includeDetailed=true generates
// a detailed report map that includes the per-host "detailed" field containing
// each host's individual scan report data.
func TestToMapDetailed(t *testing.T) {
	defnID, err := common.NewIndexDefnId()
	require.NoError(t, err)

	state := &ScanReportState{
		DefnID: defnID,
		HostScanReport: map[string]*HostScanReport{
			"host1": {
				SrvrNs:     &ServerTimings{TotalDur: 1000},
				SrvrCounts: &ServerCounts{RowsReturn: 10},
			},
		},
	}
	result := state.ToMap(true)
	require.Contains(t, result, "detailed")
	detailed := result["detailed"].(map[string]interface{})
	require.Contains(t, detailed, "host1")
}
