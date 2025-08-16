package indexer

import (
	"testing"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/vector"
	"github.com/couchbase/indexing/secondary/vector/codebook"
)

/*
TODO: Fix tests
import (
	"github.com/golang/protobuf/proto"
	"encoding/json"
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
	queryclient "github.com/couchbase/indexing/secondary/queryport/client"
	"reflect"
	"testing"
)


const QUERY_PORT_ADDR = ":7000"

var (
	nkeys   int = 0
	count   int = 0
	nerrors int = 0
	nmesgs  int = 0
	tst     *testing.T
)

func initEnv() {
	// c.LogEnable()
}

func testPK(i int) string {
	return fmt.Sprintf("PrimaryKey-%d", i)
}

func testSK(i int) c.SecondaryKey {
	return c.SecondaryKey{fmt.Sprintf("SecKey-%d", i)}
}

func simpleKeyFeeder(keych chan Key, valch chan Value, errch chan error) {
	_ = valch
	_ = errch

	for i := 0; i < nkeys; i++ {
		keys := append(testSK(i), testPK(i))
		b, _ := json.Marshal(keys)
		k, err := NewKey(b)
		if err != nil {
			panic("crash")
		}
		keych <- k
	}
	close(keych)
}

func simpleErrorFeeder(keych chan Key, valch chan Value, errch chan error) {
	_ = valch
	_ = errch

	for i := 0; i < nkeys; i++ {
		keys := append(testSK(i), testPK(i))
		b, _ := json.Marshal(keys)
		k, err := NewKey(b)
		if err != nil {
			panic("crash")
		}
		keych <- k
	}
	errch <- ErrInternal
}

func verifyInvalidIndex(val queryclient.ResponseReader) bool {
	if val.Error() != nil && val.Error().Error() != c.ErrIndexNotFound.Error() {
		tst.Error("Unexpected response", val.Error(), c.ErrIndexNotFound)
	}

	return true
}

func verifyIndexScanAll(msg queryclient.ResponseReader) bool {
	secEntries, pkEntries, err := msg.GetEntries()
	if len(secEntries) > 0 {
		nmesgs++
		for i := 0; i < len(secEntries); i++ {
			sk := testSK(count)
			pk := testPK(count)
			if !reflect.DeepEqual(sk, secEntries[i]) {
				tst.Error("Invalid sec key received")
			}

			if !reflect.DeepEqual(pk, string(pkEntries[i])) {
				tst.Error("Invalid primary key received")
			}
			count++
		}
	} else if msg.Error() != nil {
		if msg.Error().Error() != ErrInternal.Error() {
			tst.Fatal("Expected error :", err, ", Received :",
				ErrInternal.Error())
		}
		nerrors++
	}
	return true
}

//func TestInvalidIndexScan(t *testing.T) {
	//initEnv()
	//h, err := newScannerTestHarness()
	//if err != nil {
		//t.Fatal(err)
	//}
	//defer h.Shutdown()

	//tst = t
	//defnId := h.createIndex("idx", "default", simpleKeyFeeder)
	//config := c.SystemConfig.SectionConfig("queryport.client.", true)
	//client, err := queryclient.NewGsiClient("localhost:9000", config)
	//if err != nil {
		//t.Errorf("cannot create GsiClient %v", err)
	//}
	//client.ScanAll(uint64(defnId), 40, c.AnyConsistency, nil, verifyInvalidIndex)
	//client.Close()

//}

//func TestIndexScan(t *testing.T) {
	//initEnv()
	 TODO: Add range verification
	//h, err := newScannerTestHarness()
	//if err != nil {
		//t.Fatal(err)
	//}
	//defer h.Shutdown()

	//tst = t
	//count = 0
	//nkeys = 100
	//defnId := h.createIndex("idx", "default", simpleKeyFeeder)
	//config := c.SystemConfig.SectionConfig("queryport.client.", true)
	//client, err := queryclient.NewGsiClient("localhost:9000", config)
	//if err != nil {
		//t.Errorf("cannot create GsiClient %v", err)
	//}
	//low := c.SecondaryKey{"low"}
	//high := c.SecondaryKey{"high"}
	//client.Range(
		//uint64(defnId), low, high, queryclient.Inclusion(Both), false, 0,
		//c.AnyConsistency, nil, verifyIndexScanAll)
	//client.Close()
	//if count != nkeys {
		//t.Error("Scan result entries count mismatch", count, "!=", nkeys)
	//}
//}

//func TestIndexScanAll(t *testing.T) {
	//initEnv()
	//h, err := newScannerTestHarness()
	//if err != nil {
		//t.Fatal(err)
	//}
	//defer h.Shutdown()

	//tst = t
	//count = 0
	//nkeys = 100
	//defnId := h.createIndex("idx", "default", simpleKeyFeeder)
	//config := c.SystemConfig.SectionConfig("queryport.client.", true)
	//client, err := queryclient.NewGsiClient("localhost:9000", config)
	//if err != nil {
		//t.Errorf("cannot create GsiClient %v", err)
	//}
	//client.ScanAll(uint64(defnId), 0, c.AnyConsistency, nil, verifyIndexScanAll)
	//client.Close()
	//if count != nkeys {
		//t.Error("Scan result entries count mismatch", count, "!=", nkeys)
	//}
//}

//func TestIndexScanAllLimit(t *testing.T) {
	//initEnv()
	//h, err := newScannerTestHarness()
	//if err != nil {
		//t.Fatal(err)
	//}
	//defer h.Shutdown()

	//tst = t
	//count = 0
	//nkeys = 10000
	//defnId := h.createIndex("idx", "default", simpleKeyFeeder)
	//config := c.SystemConfig.SectionConfig("queryport.client.", true)
	//client, err := queryclient.NewGsiClient("localhost:9000", config)
	//if err != nil {
		//t.Errorf("cannot create GsiClient %v", err)
	//}
	//client.ScanAll(uint64(defnId), 100, c.AnyConsistency, nil, verifyIndexScanAll)
	//client.Close()
	//if count != 100 {
		//t.Error("Scan result entries count mismatch", count, "!=", 100)
	//}
//}

//func TestScanEmptyIndex(t *testing.T) {
	//initEnv()
	//h, err := newScannerTestHarness()
	//if err != nil {
		//t.Fatal(err)
	//}
	//defer h.Shutdown()

	//tst = t
	//count = 0
	//nkeys = 0
	//defnId := h.createIndex("idx", "default", simpleKeyFeeder)
	//config := c.SystemConfig.SectionConfig("queryport.client.", true)
	//client, err := queryclient.NewGsiClient("localhost:9000", config)
	//if err != nil {
		//t.Errorf("cannot create GsiClient %v", err)
	//}
	//client.ScanAll(uint64(defnId), 0, c.AnyConsistency, nil, verifyIndexScanAll)
	//client.Close()
	//if count != 0 {
		//t.Error("Scan result entries count mismatch", count, "!=", nkeys)
	//}
//}

//func TestIndexScanErrors(t *testing.T) {
	//initEnv()
	//h, err := newScannerTestHarness()
	//if err != nil {
		//t.Fatal(err)
	//}
	//defer h.Shutdown()

	//tst = t
	//count = 0
	//nkeys = 100
	//nerrors = 0
	//defnId := h.createIndex("idx", "default", simpleKeyFeeder)
	//config := c.SystemConfig.SectionConfig("queryport.client.", true)
	//client, err := queryclient.NewGsiClient("localhost:9000", config)
	//if err != nil {
		//t.Errorf("cannot create GsiClient %v", err)
	//}
	//client.ScanAll(uint64(defnId), 0, c.AnyConsistency, nil, verifyIndexScanAll)

	//if count != 100 {
		//t.Error("Scan result entries count mismatch", count, "!=", 100)
	//}

	//if nerrors == 0 {
		//t.Error("Expected a scan error")
	//}

	//count = 0
	//nerrors = 0
	//nkeys = 0

	//client.ScanAll(uint64(defnId), 0, c.AnyConsistency, nil, verifyIndexScanAll)
	//if count != 0 {
		//t.Error("Scan result entries count mismatch", count, "!=", 0)
	//}
	//if nerrors == 0 {
		//t.Error("Expected a scan error")
	//}

	//client.Close()
//}

// TODO: Should be fixed later when client exposes page size settings
//func TestScanPageSize(t *testing.T) {
//initEnv()
//h, err := newScannerTestHarness()
//if err != nil {
//t.Fatal(err)
//}
//defer h.Shutdown()

//// TODO: Verify page size wrt entries page size received from response
//// message.

//tst = t
//count = 0
//nerrors = 0
//nmesgs = 0
//nkeys = 10000

//h.createIndex("idx", "default", simpleKeyFeeder)
//config := c.SystemConfig.SectionConfig("queryport.client.", true)
//client := queryclient.NewClient(QUERY_PORT_ADDR, config)
//client.ScanAll("idx", "default", 4092, 0, c.AnyConsistency, nil, verifyIndexScanAll)
//client.Close()
//if count != nkeys {
//t.Error("Scan result entries count mismatch", count, "!=", nkeys)
//}

//if nmesgs == nkeys {
//t.Error("Index entry pages were not generated")
//}
//}

func TestStatistics(t *testing.T) {
	initEnv()
	h, err := newScannerTestHarness()
	if err != nil {
		t.Fatal(err)
	}
	defer h.Shutdown()
	tst = t
	nkeys = 10000

	testStatisticsResponse := &protobuf.StatisticsResponse{
		Stats: &protobuf.IndexStatistics{
			KeysCount:       proto.Uint64(uint64(nkeys)),
			UniqueKeysCount: proto.Uint64(0),
			KeyMin:          []byte("min"),
			KeyMax:          []byte("max"),
		},
	}

	defnId := h.createIndex("idx", "default", simpleKeyFeeder)
	config := c.SystemConfig.SectionConfig("queryport.client.", true)
	client, err := queryclient.NewGsiClient("localhost:9000", config)
	if err != nil {
		t.Errorf("cannot create GsiClient %v", err)
	}
	low := c.SecondaryKey{"low"}
	high := c.SecondaryKey{"high"}
	out, err := client.RangeStatistics(uint64(defnId), low, high, 0)

	if reflect.DeepEqual(out, testStatisticsResponse.GetStats()) == false {
		t.Errorf("Unexpected stats response %v", out)
	}

	client.Close()
}

func TestStatisticsError(t *testing.T) {
	initEnv()
	h, err := newScannerTestHarness()
	if err != nil {
		t.Fatal(err)
	}
	defer h.Shutdown()
	tst = t
	nkeys = 1000

	defnId := h.createIndex("idx", "default", simpleKeyFeeder)
	config := c.SystemConfig.SectionConfig("queryport.client.", true)
	client, err := queryclient.NewGsiClient("localhost:9000", config)
	if err != nil {
		t.Errorf("cannot create GsiClient %v", err)
	}
	low := c.SecondaryKey{"low"}
	high := c.SecondaryKey{"high"}
	_, err = client.RangeStatistics(uint64(defnId), low, high, 0)

	if err == ErrInternal {
		t.Errorf("Unexpected stats err %v", err)
	}

	client.Close()
}

*/

type MockSliceContainer struct {
	CB      codebook.Codebook
	PartnId common.PartitionId
}

func (m *MockSliceContainer) AddSlice(SliceId, Slice)                      {}
func (m *MockSliceContainer) UpdateSlice(SliceId, Slice)                   {}
func (m *MockSliceContainer) RemoveSlice(SliceId)                          {}
func (m *MockSliceContainer) GetSliceByIndexKey(common.IndexKey) Slice     { return nil }
func (m *MockSliceContainer) GetSliceIdByIndexKey(common.IndexKey) SliceId { return 0 }
func (m *MockSliceContainer) GetAllSlices() []Slice                        { return nil }
func (m *MockSliceContainer) GetSliceById(SliceId) Slice {
	return &plasmaSlice{
		codebook:   m.CB,
		idxPartnId: m.PartnId,
	}
}

func Test_scanCoordinator_fillCodebookMap(t *testing.T) {
	logging.SetLogLevel(logging.Info)
	msNil := &MockSliceContainer{
		CB: nil,
	}

	cb, err := vector.NewCodebookIVFPQ(128, 8, 8, 128, codebook.METRIC_L2, false, false)
	if err != nil || cb == nil {
		t.Errorf("Unable to create index. Err %v", err)
	}
	msVec := &MockSliceContainer{
		CB: cb,
	}

	cb1, err := vector.NewCodebookIVFPQ(128, 8, 8, 128, codebook.METRIC_L2, false, false)
	train_vecs := make([]float32, 0)
	for i := 0; i < 10000*128; i++ {
		train_vecs = append(train_vecs, float32(i))
	}
	err = cb1.Train(train_vecs)
	msVec1 := &MockSliceContainer{
		CB: cb1,
	}

	tests := []struct {
		name          string
		instId        common.IndexInstId
		partnIds      []common.PartitionId
		indexPartnMap IndexPartnMap
		wantErr       bool
	}{
		{
			name:     "trained",
			partnIds: []common.PartitionId{1, 2, 3},
			instId:   common.IndexInstId(1),
			indexPartnMap: IndexPartnMap{
				common.IndexInstId(1): PartitionInstMap{
					common.PartitionId(1): PartitionInst{Sc: msVec1},
					common.PartitionId(2): PartitionInst{Sc: msVec1},
					common.PartitionId(3): PartitionInst{Sc: msVec1},
				},
			},
			wantErr: false,
		},
		{
			name:     "nottrained",
			partnIds: []common.PartitionId{1, 2, 3},
			instId:   common.IndexInstId(1),
			indexPartnMap: IndexPartnMap{
				common.IndexInstId(1): PartitionInstMap{
					common.PartitionId(1): PartitionInst{Sc: msVec},
				},
			},
			wantErr: true,
		},
		{
			name:     "nilcodebook",
			partnIds: []common.PartitionId{1, 2, 3},
			instId:   common.IndexInstId(1),
			indexPartnMap: IndexPartnMap{
				common.IndexInstId(1): PartitionInstMap{
					common.PartitionId(1): PartitionInst{Sc: msNil},
				},
			},
			wantErr: true,
		},
		{
			name:     "notmyindex",
			partnIds: []common.PartitionId{1, 2, 3},
			instId:   common.IndexInstId(2),
			indexPartnMap: IndexPartnMap{
				common.IndexInstId(1): PartitionInstMap{
					common.PartitionId(1): PartitionInst{Sc: msNil},
				},
			},
			wantErr: true,
		},
		{
			name:     "notmypartn",
			partnIds: []common.PartitionId{2, 3},
			instId:   common.IndexInstId(1),
			indexPartnMap: IndexPartnMap{
				common.IndexInstId(1): PartitionInstMap{
					common.PartitionId(1): PartitionInst{Sc: msNil},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &scanCoordinator{
				indexPartnMap: tt.indexPartnMap,
			}
			r := &ScanRequest{
				IndexInst: common.IndexInst{
					InstId: tt.instId,
				},
				PartitionIds: tt.partnIds,
			}

			if err := s.fillCodebookMap(r); (err != nil) != tt.wantErr {
				t.Errorf("scanCoordinator.fillCodebookMap() error = %v, wantErr %v", err, tt.wantErr)
			} else if err != nil {
				logging.Infof("Error Received: %v", err)
			}

			logging.Infof("codebookmap: %+v", r.codebookMap)
		})
	}
}

func Test_scanCoordinator_findIndexInstance(t *testing.T) {
	logging.SetLogLevel(logging.Info)
	getMs := func(id common.PartitionId) SliceContainer {
		return &MockSliceContainer{
			CB:      nil,
			PartnId: id,
		}
	}

	tests := []struct {
		name             string
		defnID           uint64
		partnIds         []common.PartitionId
		user             string
		skipReadMetering bool
		ctxsPerPartition int
		indexPartnMap    IndexPartnMap
		indexInstMap     common.IndexInstMap
		indexDefnMap     map[common.IndexDefnId][]common.IndexInstId
		indexerState     common.IndexerState
		instIdWanted     common.IndexInstId
		wantErr          bool
	}{
		{
			name:             "basic",
			user:             "pushpa",
			ctxsPerPartition: 1,
			defnID:           111,
			partnIds:         []common.PartitionId{1},
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
						DefnId:          111,
						PartitionScheme: common.HASH,
					},
				},
				common.IndexInstId(22): common.IndexInst{
					InstId: common.IndexInstId(22),
					State:  common.INDEX_STATE_ACTIVE,
					RState: common.REBAL_ACTIVE,
					Defn: common.IndexDefn{
						DefnId:          111,
						PartitionScheme: common.HASH,
					},
				},
			},
			indexDefnMap: map[common.IndexDefnId][]common.IndexInstId{
				common.IndexDefnId(111): []common.IndexInstId{11, 22},
			},
			indexerState: common.INDEXER_ACTIVE,
			instIdWanted: 11,
			wantErr:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &scanCoordinator{
				indexInstMap:  tt.indexInstMap,
				indexPartnMap: tt.indexPartnMap,
				indexDefnMap:  tt.indexDefnMap,
			}
			s.setIndexerState(tt.indexerState)
			got, got1, _, err := s.findIndexInstance(tt.defnID, tt.partnIds, tt.user, tt.skipReadMetering, tt.ctxsPerPartition, true)
			if (err != nil) != tt.wantErr {
				t.Errorf("scanCoordinator.findIndexInstance() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			want := tt.indexInstMap[tt.instIdWanted]
			if want.InstId != got.InstId {
				t.Fatalf("scanCoordinator.findIndexInstance() \n\ngot = %v, \n\n want = %v", got, want)
			}

			for _, ctx := range got1 {
				reader := ctx.(*plasmaReaderCtx)
				logging.Infof("Contexts Got: %+v", reader)
			}
		})
	}
}
