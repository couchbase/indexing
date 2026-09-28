package indexer

import (
	"container/list"
	"testing"

	"github.com/couchbase/indexing/secondary/common"
)

// The INIT->MAINT merge gate must also require MAINT_STREAM to have flushed
// past minMergeTs (the TS from which the projector applies the new definition
// to MAINT, see setMergeTs). Below minMergeTs the real instance does not own
// the proxy's partitions: the projector sends UpsertDeletion, which the flusher
// discards for immutable indexes, while Deletion is broadcast to every endpoint
// and gated only by the partition map that mergePartition fills at the fold.
// MAINT's queue below minMergeTs is therefore delete-only for those partitions,
// and merging while it is still queued deletes rows INIT indexed for documents
// that were later re-created. checkFlushTsValidForMerge compared maintTsSeq
// only against initTsSeq, so MAINT << minMergeTs was admitted, and the further
// MAINT lagged the more easily that comparison passed.

const mergeGateNumVb = 8

// mergeGateTs builds a snap-aligned TsVbuuid with the same vbuuids everywhere, so
// that CompareVbuuids() succeeds and only the seqnos differ between timestamps.
func mergeGateTs(bucket string, seqno uint64) *common.TsVbuuid {
	ts := common.NewTsVbuuid(bucket, mergeGateNumVb)
	for i := 0; i < mergeGateNumVb; i++ {
		ts.Seqnos[i] = seqno
		ts.Vbuuids[i] = 1
	}
	return ts
}

// mergeGateTsWithLag is mergeGateTs with one vbucket left behind at lagSeqno,
// so the vector semantics of the gate (one lagging vbucket blocks the merge)
// are exercised and not just the uniform scalar case.
func mergeGateTsWithLag(bucket string, seqno, lagSeqno uint64, lagVb int) *common.TsVbuuid {
	ts := mergeGateTs(bucket, seqno)
	ts.Seqnos[lagVb] = lagSeqno
	return ts
}

// mergeGateTimekeeper builds the minimum timekeeper state checkFlushTsValidForMerge
// touches. Every map written to on the exercised path must be non-nil:
// keyspaceIdFlushCheckDebugLogTime (written when forceLog fires, which it always
// does on a zero last-log time) and streamKeyspaceIdPastMinMergeTs (written when
// INIT passes minMergeTs). streamKeyspaceIdTsListMap must hold a real list.List
// because the code calls tsList.Len() unguarded. config needs clusterAddr
// because ConfigValue.String() is a bare type assertion.
func mergeGateTimekeeper(keyspaceId string, maintFlushTs, maintInProgressTs *common.TsVbuuid) *timekeeper {
	bucket, _, _ := SplitKeyspaceId(keyspaceId)

	ss := &StreamState{
		streamKeyspaceIdStatus: map[common.StreamId]KeyspaceIdStatus{
			common.MAINT_STREAM: {bucket: STREAM_ACTIVE},
			common.INIT_STREAM:  {keyspaceId: STREAM_ACTIVE},
		},
		streamKeyspaceIdLastFlushedTsMap: map[common.StreamId]KeyspaceIdLastFlushedTsMap{
			common.MAINT_STREAM: {bucket: maintFlushTs},
		},
		streamKeyspaceIdFlushInProgressTsMap: map[common.StreamId]KeyspaceIdFlushInProgressTsMap{
			common.MAINT_STREAM: {bucket: maintInProgressTs},
		},
		streamKeyspaceIdAllowMarkFirstSnap: map[common.StreamId]KeyspaceIdAllowMarkFirstSnap{
			common.MAINT_STREAM: {bucket: false},
		},
		streamKeyspaceIdPastMinMergeTs: map[common.StreamId]KeyspaceIdPastMinMergeTs{
			common.INIT_STREAM: {keyspaceId: false},
		},
		// empty collection id keeps the collection-seqno branches (which would
		// make network calls) out of the picture
		streamKeyspaceIdCollectionId: map[common.StreamId]KeyspaceIdCollectionId{
			common.INIT_STREAM: {keyspaceId: ""},
		},
		streamKeyspaceIdTsListMap: map[common.StreamId]KeyspaceIdTsListMap{
			common.INIT_STREAM: {keyspaceId: list.New()},
		},
		streamKeyspaceIdHWTMap: map[common.StreamId]KeyspaceIdHWTMap{
			common.INIT_STREAM: {keyspaceId: mergeGateTs(bucket, 0)},
		},
		keyspaceIdFlushCheckDebugLogTime: map[string]uint64{},
		// written by the merge-phase timer branch; kept non-nil so a future
		// case that enables it (maxTimerInterval > 0) cannot panic on write
		streamKeyspaceIdMergePhaseStartTime: map[common.StreamId]KeyspaceIdMergePhaseStartTime{
			common.MAINT_STREAM: {},
		},
		streamKeyspaceIdMergePhaseTimerInterval: map[common.StreamId]KeyspaceIdMergePhaseTimerInterval{
			common.MAINT_STREAM: {},
		},
	}

	cfg := common.Config{}
	cfg["clusterAddr"] = common.ConfigValue{Value: "127.0.0.1:9000"}
	// 0 disables the merge-phase timer-escalation branch, which would otherwise
	// pull in getInMemSnapInterval() and more config
	cfg["timekeeper.mergePhase.maxTimerInterval"] = common.ConfigValue{Value: uint64(0)}

	return &timekeeper{ss: ss, config: cfg}
}

func TestCheckFlushTsValidForMergeMaintPastMinMergeTs(t *testing.T) {
	const keyspaceId = "default"
	const bucket = "default"

	cases := []struct {
		name            string
		initSeq         uint64
		maintSeq        uint64
		minMerge        uint64
		maintLag        *uint64 // if set, one vbucket of MAINT is left at this seqno
		maintInProgress *uint64 // if set, a MAINT flush up to this seqno is in progress
		want            bool
		why             string
	}{
		{
			name: "MAINT behind minMergeTs - must not merge",
			// Both existing conditions are satisfied: INIT has drained past the
			// handover point and INIT is not behind MAINT. But MAINT still holds
			// the whole pre-handover range, which for the transferred partitions
			// contains deletes and nothing else.
			initSeq: 1000, maintSeq: 100, minMerge: 500, want: false,
			why: "MAINT still holds a delete-only pre-minMergeTs range for the partitions about to be folded in",
		},
		{
			name:    "MAINT past minMergeTs - safe to merge",
			initSeq: 1000, maintSeq: 600, minMerge: 500, want: true,
			why: "every pre-handover mutation has already been dequeued and dropped at the partition-map miss",
		},
		{
			name:    "MAINT exactly at minMergeTs - boundary, safe",
			initSeq: 1000, maintSeq: 500, minMerge: 500, want: true,
			why: "the gate is >=, so equality is past the handover point",
		},
		{
			name:    "INIT behind minMergeTs - existing guard holds",
			initSeq: 400, maintSeq: 800, minMerge: 500, want: false,
			why: "INIT has not drained to the handover point, but MAINT has, so only the INIT guard can reject",
		},
		{
			name:    "INIT behind MAINT - existing guard holds",
			initSeq: 550, maintSeq: 800, minMerge: 500, want: false,
			why: "INIT is behind MAINT",
		},
		{
			name:    "MAINT flush in progress past minMergeTs, last completed flush behind - must not merge",
			initSeq: 1000, maintSeq: 400, minMerge: 500, maintInProgress: uint64Ptr(600), want: false,
			why: "the in-progress flush may still be applying the pre-minMergeTs range; only a completed flush counts",
		},
		{
			name:    "MAINT one vbucket behind minMergeTs - must not merge",
			initSeq: 1000, maintSeq: 600, minMerge: 500, maintLag: uint64Ptr(400), want: false,
			why: "the gate is per vbucket; a single lagging vbucket still holds a delete-only range",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			maintTs := mergeGateTs(bucket, tc.maintSeq)
			if tc.maintLag != nil {
				maintTs = mergeGateTsWithLag(bucket, tc.maintSeq, *tc.maintLag, 3)
			}
			var inProgressTs *common.TsVbuuid
			if tc.maintInProgress != nil {
				inProgressTs = mergeGateTs(bucket, *tc.maintInProgress)
			}
			tk := mergeGateTimekeeper(keyspaceId, maintTs, inProgressTs)

			got, _ := tk.checkFlushTsValidForMerge(
				common.INIT_STREAM,
				keyspaceId,
				mergeGateTs(bucket, tc.initSeq),
				mergeGateTs(bucket, tc.minMerge),
				false, // fetchKVSeq=false: no KV round trips
			)

			if got != tc.want {
				t.Errorf("checkFlushTsValidForMerge(init=%d, maint=%d, minMerge=%d) = %v, want %v\n  %s",
					tc.initSeq, tc.maintSeq, tc.minMerge, got, tc.want, tc.why)
			}
		})
	}
}

func uint64Ptr(v uint64) *uint64 { return &v }
