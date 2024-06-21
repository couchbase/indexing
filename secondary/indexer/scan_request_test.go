package indexer

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/couchbase/indexing/secondary/common"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
	"github.com/couchbase/indexing/secondary/queryport/client"
	"github.com/couchbase/indexing/secondary/vector/codebook"
	"github.com/golang/protobuf/proto"
)

func compareMaps(map1, map2 interface{}) bool {
	// Use reflect.DeepEqual to compare the maps
	return reflect.DeepEqual(map1, map2)
}

func TestScanRequest_getNearestCentroidIDs(t *testing.T) {
	vm := &c.VectorMetadata{
		Dimension:  4,
		Similarity: c.L2,
	}

	cb := &codebook.MockCodebook{
		Trained: true,
		VecMeta: vm,
		Centroids: [][]float32{
			{1.2, 3.4, 5.6, 7.8},
			{2.0, 3.1, 5.0, 7.0},
			{2.2, 3.5, 6.7, 8.9},
			{1.5, 2.3, 4.6, 7.1},
			{2.0, 3.0, 5.1, 7.0},
			{3.1, 4.2, 5.8, 9.1},
			{2.8, 3.6, 5.9, 7.3},
			{2.1, 3.0, 5.0, 7.0},
		},
	}

	sr := &ScanRequest{
		queryVector:  []float32{2.0, 3.0, 5.0, 7.0},
		PartitionIds: []c.PartitionId{1, 2},
		codebookMap: map[c.PartitionId]codebook.Codebook{
			c.PartitionId(1): cb,
			c.PartitionId(2): cb,
		},
	}

	t.Run("nprobe_2", func(t *testing.T) {
		sr.nprobes = 2
		err := sr.getNearestCentroids()
		if err != nil {
			t.Fatal(err)
		}
		t.Log("Nearest CIDs: ", sr.centroidMap)
		if !compareMaps(sr.centroidMap, map[common.PartitionId][]int64{
			common.PartitionId(1): []int64{1, 4}, common.PartitionId(2): []int64{1, 4},
		}) {
			t.Fatal(fmt.Errorf("wrong centoid ids"))
		}
	})

	t.Run("nprobe_3", func(t *testing.T) {
		sr.nprobes = 3
		err := sr.getNearestCentroids()
		if err != nil {
			t.Fatal(err)
		}
		t.Log("Nearest CIDs: ", sr.centroidMap)
		if !compareMaps(sr.centroidMap, map[common.PartitionId][]int64{
			common.PartitionId(1): []int64{1, 4, 7}, common.PartitionId(2): []int64{1, 4, 7},
		}) {
			t.Fatal(fmt.Errorf("wrong centoid ids"))
		}
	})

	t.Run("nprobe_1", func(t *testing.T) {
		sr.nprobes = 1
		err := sr.getNearestCentroids()
		if err != nil {
			t.Fatal(err)
		}
		t.Log("Nearest CIDs: ", sr.centroidMap)
		if !compareMaps(sr.centroidMap, map[common.PartitionId][]int64{
			common.PartitionId(1): []int64{1}, common.PartitionId(2): []int64{1},
		}) {
			t.Fatal(fmt.Errorf("wrong centoid ids"))
		}
	})

	t.Run("not_trained", func(t *testing.T) {
		cb.Trained = false
		sr.nprobes = 1
		err := sr.getNearestCentroids()
		if !strings.Contains(err.Error(), "is not trained") {
			t.Fatal(err)
		}
		t.Log(err)
	})
}

func scansToProtoScans(scans client.Scans) ([]*protobuf.Scan, error) {
	protoScans := make([]*protobuf.Scan, len(scans))
	for i, scan := range scans {
		if scan != nil {
			var filters []*protobuf.CompositeElementFilter
			filters = make([]*protobuf.CompositeElementFilter, len(scan.Filter))
			if scan.Filter != nil {
				for j, f := range scan.Filter {
					var l, h []byte
					var err error
					if f.Low != nil && f.Low != common.MinUnbounded { // Do not encode if unbounded
						l, err = json.Marshal(f.Low)
						if err != nil {
							return nil, err
						}
					}
					if f.High != nil && f.High != common.MaxUnbounded { // Do not encode if unbounded
						h, err = json.Marshal(f.High)
						if err != nil {
							return nil, err
						}
					}
					fl := &protobuf.CompositeElementFilter{
						Low: l, High: h, Inclusion: proto.Uint32(uint32(f.Inclusion)),
					}
					filters[j] = fl
				}
			}
			s := &protobuf.Scan{
				Filters: filters,
			}
			protoScans[i] = s
		}
	}
	return protoScans, nil
}

func TestScanRequest_fillVectorScans(t *testing.T) {
	logging.SetLogLevel(logging.Info)
	dim := 4
	tests := []struct {
		name        string
		secExprs    []string
		vectorPos   int
		centroidMap map[common.PartitionId][]int64
		scans       client.Scans
		wantErr     bool
	}{
		{
			name:      "trailing_vector",
			secExprs:  []string{"name", "age", "color"},
			vectorPos: 2,
			centroidMap: map[common.PartitionId][]int64{
				common.PartitionId(1): []int64{1, 4, 7},
				common.PartitionId(2): []int64{2, 3, 6},
			},
			scans: client.Scans{
				&client.Scan{
					Filter: []*client.CompositeElementFilter{
						&client.CompositeElementFilter{
							Low:       "a",
							High:      "c",
							Inclusion: client.Both,
						},
						&client.CompositeElementFilter{
							Low:       20,
							High:      50,
							Inclusion: client.Both,
						},
						&client.CompositeElementFilter{},
					},
				},
			},
			wantErr: false,
		},
		{
			name:      "leading_vector",
			secExprs:  []string{"color", "name", "age"},
			vectorPos: 0,
			centroidMap: map[common.PartitionId][]int64{
				common.PartitionId(1): []int64{1, 4, 7},
				common.PartitionId(2): []int64{2, 3, 6},
			},
			scans: client.Scans{
				&client.Scan{
					Filter: []*client.CompositeElementFilter{
						&client.CompositeElementFilter{},
						&client.CompositeElementFilter{
							Low:       "a",
							High:      "c",
							Inclusion: client.Both,
						},
						&client.CompositeElementFilter{
							Low:       20,
							High:      50,
							Inclusion: client.Both,
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name:      "single_vector",
			secExprs:  []string{"color"},
			vectorPos: 0,
			centroidMap: map[common.PartitionId][]int64{
				common.PartitionId(1): []int64{1, 4, 7},
				common.PartitionId(2): []int64{2, 3, 6},
			},
			scans: client.Scans{
				&client.Scan{
					Filter: []*client.CompositeElementFilter{
						&client.CompositeElementFilter{},
					},
				},
			},
			wantErr: false,
		},
		{
			name:      "skip_key",
			secExprs:  []string{"name", "age", "color"},
			vectorPos: 2,
			centroidMap: map[common.PartitionId][]int64{
				common.PartitionId(1): []int64{1, 4, 7},
				common.PartitionId(2): []int64{2, 3, 6},
			},
			scans: client.Scans{
				&client.Scan{
					Filter: []*client.CompositeElementFilter{
						&client.CompositeElementFilter{
							Low:       "a",
							High:      "c",
							Inclusion: client.Both,
						},
						&client.CompositeElementFilter{},
						&client.CompositeElementFilter{},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &ScanRequest{
				ScanType:     VectorScanReq,
				DefnID:       1,
				IndexInstId:  1,
				IndexName:    "idx_random",
				Bucket:       "bucket",
				CollectionId: "0",
				keySzCfg: keySizeConfig{
					allowLargeKeys: true,
					maxSecKeyLen:   4608,
				},
				IndexInst: c.IndexInst{
					Defn: c.IndexDefn{
						VectorMeta: &c.VectorMetadata{
							Dimension: dim,
						},
						SecExprs: tt.secExprs,
					},
				},
				isPrimary: false,
				LogPrefix: "testPrefix",
				RequestId: "testRequestID",
			}
			r.centroidMap = tt.centroidMap
			r.vectorPos = tt.vectorPos

			protoScans, err := scansToProtoScans(tt.scans)
			if err != nil {
				t.Fatal(err)
			}
			r.protoScans = protoScans

			t.Logf("ProtoScans: %+v", protoScans)

			if err := r.fillVectorScans(); (err != nil) != tt.wantErr {
				t.Errorf("ScanRequest.fillVectorScans() error = %v, wantErr %v", err, tt.wantErr)
			}

			// TODO: Add code for result verification
			for cid, s := range r.vectorScans {
				for pid, p := range s {
					t.Logf("centroidId: %v PartnID: %v Scan: %+v\n", cid, pid, p)
				}
			}
		})
	}
}

func TestScanRequest_setIndexParams(t *testing.T) {
	logging.SetLogLevel(logging.Info)
	getMs := func(id common.PartitionId) SliceContainer {
		return &MockSliceContainer{
			CB:      nil,
			PartnId: id,
		}
	}

	defnId := c.IndexDefnId(111)
	sco := &scanCoordinator{
		indexPartnMap: IndexPartnMap{
			common.IndexInstId(11): PartitionInstMap{
				common.PartitionId(1): PartitionInst{Sc: getMs(common.PartitionId(1))},
				common.PartitionId(3): PartitionInst{Sc: getMs(common.PartitionId(3))},
				common.PartitionId(5): PartitionInst{Sc: getMs(common.PartitionId(5))},
			},
			common.IndexInstId(22): PartitionInstMap{
				common.PartitionId(2): PartitionInst{Sc: getMs(common.PartitionId(2))},
				common.PartitionId(4): PartitionInst{Sc: getMs(common.PartitionId(4))},
				common.PartitionId(6): PartitionInst{Sc: getMs(common.PartitionId(6))},
			},
		},
		indexInstMap: common.IndexInstMap{
			common.IndexInstId(11): common.IndexInst{
				InstId: common.IndexInstId(11),
				State:  common.INDEX_STATE_ACTIVE,
				RState: common.REBAL_ACTIVE,
				Defn: common.IndexDefn{
					DefnId:          defnId,
					PartitionScheme: common.HASH,
				},
				ReplicaId: 0,
			},
			common.IndexInstId(22): common.IndexInst{
				InstId: common.IndexInstId(22),
				State:  common.INDEX_STATE_ACTIVE,
				RState: common.REBAL_ACTIVE,
				Defn: common.IndexDefn{
					DefnId:          defnId,
					PartitionScheme: common.HASH,
				},
				ReplicaId: 1,
			},
		},
		indexDefnMap: map[common.IndexDefnId][]common.IndexInstId{
			defnId: []common.IndexInstId{11, 22},
		},
	}
	sco.initRollbackInProgress()
	sco.stats.Set(NewIndexerStats())

	tests := []struct {
		name                  string
		PartitionIds          []common.PartitionId
		nprobes               int
		isVectorScan          bool
		parallelCentroidScans int
		instId                common.IndexInstId
		instIdState           common.IndexState
		indexerState          common.IndexerState
		ctxsLen               int
		wantErr               error
	}{
		{
			name:                  "nprobeGtParallelScans",
			PartitionIds:          []common.PartitionId{1, 3},
			nprobes:               6,
			isVectorScan:          true,
			parallelCentroidScans: 3,
			ctxsLen:               6,
			wantErr:               nil,
		},
		{
			name:                  "nprobesLtParallelScans",
			PartitionIds:          []common.PartitionId{1, 3},
			nprobes:               2,
			isVectorScan:          true,
			parallelCentroidScans: 6,
			ctxsLen:               4,
			wantErr:               nil,
		},
		{
			name:                  "nprobesEqParallelScans",
			PartitionIds:          []common.PartitionId{1, 3},
			nprobes:               4,
			isVectorScan:          true,
			parallelCentroidScans: 4,
			ctxsLen:               8,
			wantErr:               nil,
		},
		{
			name:                  "nonVectorScan",
			PartitionIds:          []common.PartitionId{1, 3},
			nprobes:               4,
			isVectorScan:          false,
			parallelCentroidScans: 4,
			ctxsLen:               2,
			wantErr:               nil,
		},
		{
			name:                  "error",
			PartitionIds:          []common.PartitionId{1, 3},
			nprobes:               4,
			isVectorScan:          true,
			parallelCentroidScans: 4,
			ctxsLen:               2,
			instId:                common.IndexInstId(11),
			instIdState:           common.INDEX_STATE_INITIAL,
			wantErr:               ErrNotMyPartition, // As other instance is active
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &ScanRequest{
				DefnID:                uint64(defnId),
				PartitionIds:          tt.PartitionIds,
				User:                  "random",
				SkipReadMetering:      false,
				nprobes:               tt.nprobes,
				isVectorScan:          tt.isVectorScan,
				parallelCentroidScans: tt.parallelCentroidScans,
			}
			r.sco = sco
			if tt.instId != 0 {
				inst := sco.indexInstMap[tt.instId]
				inst.State = tt.instIdState
				sco.indexInstMap[tt.instId] = inst
			}
			r.sco.setIndexerState(tt.indexerState)
			if err := r.setIndexParams(); err != tt.wantErr {
				if strings.Contains(err.Error(), tt.wantErr.Error()) {
					return
				}
				t.Fatalf("ScanRequest.setIndexParams() error = %v, wantErr %v", err, tt.wantErr)
			}

			if len(r.Ctxs) != tt.ctxsLen {
				t.Fatalf("Not getting sufficient readers as needed: %v got %v", tt.ctxsLen, len(r.Ctxs))
			}
		})
	}
}
