package indexer

import (
	"fmt"
	"testing"
	"time"

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

func TestScanAdmissionController(t *testing.T) {
	// Set storage mode to PLASMA to ensure controller runs
	common.SetClusterStorageMode(common.PLASMA)

	// Create mock stats and config for testing
	mockStats := &IndexerStats{}
	mockStats.Init()

	// Set mock memory values that won't trigger throttling
	mockStats.avgResidentPercent.Set(10)                // 10% resident (above 5% threshold)
	mockStats.memoryRss.Set(100 * 1024 * 1024)          // 100MB RSS
	mockStats.memoryQuota.Set(1.5 * 1024 * 1024 * 1024) // 1.5GB quota (above 1GB threshold, RSS < quota)

	mockConfig := common.Config{
		"scan.vector.throttle.minRRThreshold": common.ConfigValue{5, "minimum RR threshold for throttling", 5, false, false},
	}

	// Create mock supervisor message channel
	mockSupvMsgch := make(MsgChannel, 10)

	// Start a goroutine to listen on the supervisor message channel and respond
	go func() {
		for {
			select {
			case msg := <-mockSupvMsgch:
				if msg != nil {
					// Extract the response channel from the message and send true
					if msgReq, ok := msg.(*MsgIndexRRStats); ok && msgReq.respch != nil {
						msgReq.respch <- true
					}
				}
			}
		}
	}()

	// Test basic functionality
	sac := NewScanAdmissionController(mockStats, mockConfig, mockSupvMsgch)

	// Wait for controller to start and process memory stats
	time.Sleep(6 * time.Second)

	// Test immediate admission when throttling is off
	abortch := make(chan bool, 1)
	if !sac.AdmitScan("req1", abortch) {
		t.Error("Expected immediate admission to succeed when throttling is off")
	}

	// Test stats
	throttled, queued := sac.GetStats()
	if throttled != 0 {
		t.Errorf("Expected throttled state 0, got %d", throttled)
	}
	if queued != 0 {
		t.Errorf("Expected 0 queued requests, got %d", queued)
	}

	// Verify that no requests were queued (immediate admission)
	totalQueued := sac.GetTotalQueued()
	if totalQueued != 0 {
		t.Errorf("Expected 0 total queued requests, but got %d", totalQueued)
	}

	// Test shutdown
	sac.Shutdown()
}

func TestScanAdmissionControllerQueuing(t *testing.T) {
	// Set storage mode to PLASMA to ensure controller runs
	common.SetClusterStorageMode(common.PLASMA)

	// Create mock stats and config for testing
	mockStats := &IndexerStats{}
	mockStats.Init()

	// Set mock memory values that will trigger throttling
	mockStats.avgResidentPercent.Set(3)                 // 3% resident (below 5% threshold)
	mockStats.memoryRss.Set(2 * 1024 * 1024 * 1024)     // 2GB RSS (above 1.5GB quota)
	mockStats.memoryQuota.Set(1.5 * 1024 * 1024 * 1024) // 1.5GB quota (above 1GB threshold)

	mockConfig := common.Config{
		"scan.vector.throttle.minRRThreshold": common.ConfigValue{5, "minimum RR threshold for throttling", 5, false, false},
	}

	// Create mock supervisor message channel
	mockSupvMsgch := make(MsgChannel, 10)

	// Start a goroutine to listen on the supervisor message channel and respond
	go func() {
		for {
			select {
			case msg := <-mockSupvMsgch:
				if msg != nil {
					// Extract the response channel from the message and send true
					if msgReq, ok := msg.(*MsgIndexRRStats); ok && msgReq.respch != nil {
						msgReq.respch <- true
					}
				}
			}
		}
	}()

	// Test queuing functionality
	sac := NewScanAdmissionController(mockStats, mockConfig, mockSupvMsgch)

	// Wait for controller to start and process memory stats
	time.Sleep(6 * time.Second)

	// Verify that controller automatically turned on throttling due to memory pressure
	throttled, queued := sac.GetStats()
	if throttled != 1 {
		t.Errorf("Expected controller to automatically turn on throttling due to memory pressure, got throttled state %d", throttled)
	}

	// Test queuing when throttling is on
	done := make(chan bool)
	abortch := make(chan bool, 1)
	go func() {
		admitted := sac.AdmitScan("req1", abortch)
		done <- admitted
	}()

	// Wait a bit for the request to be queued
	time.Sleep(50 * time.Millisecond)

	// Check that request is queued
	throttled, queued = sac.GetStats()
	if throttled != 1 {
		t.Errorf("Expected throttled state 1, got %d", throttled)
	}
	if queued != 1 {
		t.Errorf("Expected 1 queued request, got %d", queued)
	}

	// Verify that the total queued counter is incremented
	totalQueued := sac.GetTotalQueued()
	if totalQueued != 1 {
		t.Errorf("Expected total queued count 1, but got %d", totalQueued)
	}

	// Turn off throttling to process queued requests
	sac.SetThrottleState(false)

	// Check if queued request got admitted
	select {
	case admitted := <-done:
		if !admitted {
			t.Error("Expected queued request to be admitted")
		}
	case <-time.After(10 * time.Second):
		t.Error("Timeout waiting for admission")
	}

	// Verify final state
	throttled, queued = sac.GetStats()
	if throttled != 0 {
		t.Errorf("Expected throttled state 0, got %d", throttled)
	}
	if queued != 0 {
		t.Errorf("Expected 0 queued requests after processing, got %d", queued)
	}

	// Verify that total queued counter remains (it's cumulative, doesn't decrease)
	totalQueued = sac.GetTotalQueued()
	if totalQueued != 1 {
		t.Errorf("Expected total queued count to remain 1 after processing, but got %d", totalQueued)
	}

	sac.Shutdown()
}

func TestScanAdmissionControllerSetThrottleState(t *testing.T) {
	// Set storage mode to PLASMA to ensure controller runs
	common.SetClusterStorageMode(common.PLASMA)

	// Create mock stats and config for testing
	mockStats := &IndexerStats{}
	mockStats.Init()

	// Set mock memory values that won't trigger throttling
	// avgResidentPercent=10% > minRRThreshold=5% (good memory)
	// memoryRSS=100MB < memoryQuota=1.5GB (good memory)
	mockStats.avgResidentPercent.Set(10)                // 10% resident (above 5% threshold)
	mockStats.memoryRss.Set(100 * 1024 * 1024)          // 100MB RSS
	mockStats.memoryQuota.Set(1.5 * 1024 * 1024 * 1024) // 1.5GB quota (above 1GB threshold, RSS < quota)

	mockConfig := common.Config{
		"scan.vector.throttle.minRRThreshold": common.ConfigValue{5, "minimum RR threshold for throttling", 5, false, false},
	}

	// Create mock supervisor message channel
	mockSupvMsgch := make(MsgChannel, 10)

	sac := NewScanAdmissionController(mockStats, mockConfig, mockSupvMsgch)

	// Wait for controller to start and process memory stats
	time.Sleep(6 * time.Second)

	// Test initial state - with good memory conditions, throttling should be off
	throttled, queued := sac.GetStats()
	if throttled != 0 {
		t.Errorf("Expected initial throttled state 0, got %d", throttled)
	}
	if queued != 0 {
		t.Errorf("Expected initial queued requests 0, got %d", queued)
	}

	// Test setting throttling to true
	sac.SetThrottleState(true)
	throttled, queued = sac.GetStats()
	if throttled != 1 {
		t.Errorf("Expected throttled state 1 after SetThrottleState(true), got %d", throttled)
	}

	// Test setting throttling to false
	sac.SetThrottleState(false)
	throttled, queued = sac.GetStats()
	if throttled != 0 {
		t.Errorf("Expected throttled state 0 after SetThrottleState(false), got %d", throttled)
	}

	// Test that setting the same state multiple times works correctly
	sac.SetThrottleState(false) // Should still work
	sac.SetThrottleState(true)  // Should work
	sac.SetThrottleState(true)  // Should work
	throttled, queued = sac.GetStats()
	if throttled != 1 {
		t.Errorf("Expected throttled state 1 after multiple SetThrottleState calls, got %d", throttled)
	}

	sac.Shutdown()
}

func TestScanAdmissionControllerNoStateChange(t *testing.T) {
	// Set storage mode to PLASMA to ensure controller runs
	common.SetClusterStorageMode(common.PLASMA)

	// Create mock stats and config for testing
	mockStats := &IndexerStats{}
	mockStats.Init()

	// Set mock memory values that will trigger throttling
	mockStats.avgResidentPercent.Set(3)                 // 3% resident (below 5% threshold)
	mockStats.memoryRss.Set(2 * 1024 * 1024 * 1024)     // 2GB RSS (above 1.5GB quota)
	mockStats.memoryQuota.Set(1.5 * 1024 * 1024 * 1024) // 1.5GB quota (above 1GB threshold)

	mockConfig := common.Config{
		"scan.vector.throttle.minRRThreshold": common.ConfigValue{5, "minimum RR threshold for throttling", 5, false, false},
	}

	// Create mock supervisor message channel
	mockSupvMsgch := make(MsgChannel, 10)

	// Start a goroutine to listen on the supervisor message channel and respond
	go func() {
		for {
			select {
			case msg := <-mockSupvMsgch:
				if msg != nil {
					// Extract the response channel from the message and send true
					if msgReq, ok := msg.(*MsgIndexRRStats); ok && msgReq.respch != nil {
						msgReq.respch <- true
					}
				}
			}
		}
	}()

	sac := NewScanAdmissionController(mockStats, mockConfig, mockSupvMsgch)

	// Wait for controller to start and process memory stats
	time.Sleep(6 * time.Second)

	// Verify that controller automatically turned on throttling due to memory pressure
	initialThrottled, _ := sac.GetStats()
	if initialThrottled != 1 {
		t.Errorf("Expected initial throttled state 1 (memory pressure), got %d", initialThrottled)
	}

	// Queue a request while throttling is on
	done := make(chan bool)
	abortch := make(chan bool, 1)
	go func() {
		admitted := sac.AdmitScan("req1", abortch)
		done <- admitted
	}()

	// Wait for request to be queued
	time.Sleep(50 * time.Millisecond)

	// Verify request is queued
	throttled, queued := sac.GetStats()
	if throttled != 1 {
		t.Errorf("Expected throttled state 1, got %d", throttled)
	}
	if queued != 1 {
		t.Errorf("Expected 1 queued request, got %d", queued)
	}

	// Test that calling SetThrottleState(false) multiple times processes the queue each time
	// This validates that the method always processes queued requests when turning off throttling
	sac.SetThrottleState(false) // Should process queue and turn off throttling
	sac.SetThrottleState(false) // Should process queue again (even though already off)
	sac.SetThrottleState(false) // Should process queue again

	// Verify final state
	finalThrottled, finalQueued := sac.GetStats()
	if finalThrottled != 0 {
		t.Errorf("Expected final throttled state 0, got %d", finalThrottled)
	}
	if finalQueued != 0 {
		t.Errorf("Expected final queued requests 0, got %d", finalQueued)
	}

	// Check that the queued request got admitted
	select {
	case admitted := <-done:
		if !admitted {
			t.Error("Expected queued request to be admitted")
		}
	case <-time.After(5 * time.Second):
		t.Error("Timeout waiting for admission")
	}

	sac.Shutdown()
}

func TestScanAdmissionControllerWithMemoryStats(t *testing.T) {
	// Set storage mode to PLASMA to ensure controller runs
	common.SetClusterStorageMode(common.PLASMA)

	// Create mock stats and config with realistic memory values
	mockStats := &IndexerStats{}
	mockStats.Init()

	mockConfig := common.Config{
		"scan.vector.throttle.minRRThreshold": common.ConfigValue{5, "minimum RR threshold for throttling", 5, false, false},
	}

	// Set mock memory values
	mockStats.avgResidentPercent.Set(10)                // 10% resident
	mockStats.memoryRss.Set(100 * 1024 * 1024)          // 100MB RSS
	mockStats.memoryQuota.Set(1.5 * 1024 * 1024 * 1024) // 1.5GB quota (above 1GB threshold)

	// Create mock supervisor message channel
	mockSupvMsgch := make(MsgChannel, 10)

	// Start a goroutine to listen on the supervisor message channel and respond
	go func() {
		for {
			select {
			case msg := <-mockSupvMsgch:
				if msg != nil {
					// Extract the response channel from the message and send true
					if msgReq, ok := msg.(*MsgIndexRRStats); ok && msgReq.respch != nil {
						msgReq.respch <- true
					}
				}
			}
		}
	}()

	sac := NewScanAdmissionController(mockStats, mockConfig, mockSupvMsgch)

	// Wait for controller to start and process memory stats
	time.Sleep(6 * time.Second)

	// Test that scan requests are admitted immediately when memory is good
	abortch1 := make(chan bool, 1)
	abortch2 := make(chan bool, 1)
	abortch3 := make(chan bool, 1)

	if !sac.AdmitScan("req1", abortch1) {
		t.Error("Expected immediate admission when memory stats are good")
	}
	if !sac.AdmitScan("req2", abortch2) {
		t.Error("Expected immediate admission when memory stats are good")
	}
	if !sac.AdmitScan("req3", abortch3) {
		t.Error("Expected immediate admission when memory stats are good")
	}

	// Verify that no requests were queued (immediate admission)
	totalQueued := sac.GetTotalQueued()
	if totalQueued != 0 {
		t.Errorf("Expected 0 total queued requests, but got %d", totalQueued)
	}

	// Verify that requests are still not queued after admitting multiple scans
	throttled, queued := sac.GetStats()
	if throttled != 0 {
		t.Errorf("Expected throttled state 0 after admitting scans, got %d", throttled)
	}
	if queued != 0 {
		t.Errorf("Expected 0 queued requests after admitting scans, got %d", queued)
	}

	// Test shutdown
	sac.Shutdown()
}

func TestScanAdmissionControllerConfigDisable(t *testing.T) {
	// Set storage mode to PLASMA to ensure controller runs
	common.SetClusterStorageMode(common.PLASMA)

	// Create mock stats and config for testing
	mockStats := &IndexerStats{}
	mockStats.Init()

	// Test with throttling disabled (minRRThreshold = 0)
	mockConfig := common.Config{
		"scan.vector.throttle.minRRThreshold": common.ConfigValue{0, "throttling disabled", 0, false, false},
	}

	// Create mock supervisor message channel
	mockSupvMsgch := make(MsgChannel, 10)

	// Start a goroutine to listen on the supervisor message channel and respond
	go func() {
		for {
			select {
			case msg := <-mockSupvMsgch:
				if msg != nil {
					// Extract the response channel from the message and send true
					if msgReq, ok := msg.(*MsgIndexRRStats); ok && msgReq.respch != nil {
						msgReq.respch <- true
					}
				}
			}
		}
	}()

	sac := NewScanAdmissionController(mockStats, mockConfig, mockSupvMsgch)

	// Wait for controller to start and process config
	time.Sleep(6 * time.Second)

	// Test that requests are admitted immediately when throttling is disabled
	abortch1 := make(chan bool, 1)
	abortch2 := make(chan bool, 1)

	if !sac.AdmitScan("req1", abortch1) {
		t.Error("Expected immediate admission when throttling is disabled")
	}
	if !sac.AdmitScan("req2", abortch2) {
		t.Error("Expected immediate admission when throttling is disabled")
	}

	// Verify that no requests were queued (immediate admission)
	totalQueued := sac.GetTotalQueued()
	if totalQueued != 0 {
		t.Errorf("Expected 0 total queued requests when throttling is disabled, but got %d", totalQueued)
	}

	// Verify that no requests are queued after admitting scans when throttling is disabled
	throttled, queued := sac.GetStats()
	if throttled != 0 {
		t.Errorf("Expected throttled state 0 when throttling is disabled, got %d", throttled)
	}
	if queued != 0 {
		t.Errorf("Expected 0 queued requests when throttling is disabled, got %d", queued)
	}

	// Test shutdown
	sac.Shutdown()
}

func TestScanAdmissionControllerConfigEnable(t *testing.T) {
	// Set storage mode to PLASMA to ensure controller runs
	common.SetClusterStorageMode(common.PLASMA)

	// Create mock stats and config for testing
	mockStats := &IndexerStats{}
	mockStats.Init()

	// Set mock memory values that won't trigger throttling automatically
	mockStats.avgResidentPercent.Set(10)                // 10% resident (above 5% threshold)
	mockStats.memoryRss.Set(100 * 1024 * 1024)          // 100MB RSS
	mockStats.memoryQuota.Set(1.5 * 1024 * 1024 * 1024) // 1.5GB quota (above 1GB threshold, RSS < quota)

	// Start with throttling disabled
	mockConfig := common.Config{
		"scan.vector.throttle.minRRThreshold": common.ConfigValue{0, "throttling disabled", 0, false, false},
	}

	// Create mock supervisor message channel
	mockSupvMsgch := make(MsgChannel, 10)

	// Start a goroutine to listen on the supervisor message channel and respond
	go func() {
		for {
			select {
			case msg := <-mockSupvMsgch:
				if msg != nil {
					// Extract the response channel from the message and send true
					if msgReq, ok := msg.(*MsgIndexRRStats); ok && msgReq.respch != nil {
						msgReq.respch <- true
					}
				}
			}
		}
	}()

	sac := NewScanAdmissionController(mockStats, mockConfig, mockSupvMsgch)

	// Wait for controller to start and process config
	time.Sleep(6 * time.Second)

	// Verify no requests are queued initially (throttling disabled by config, not memory)
	throttled, queued := sac.GetStats()
	if throttled != 0 {
		t.Errorf("Expected throttled state 0 initially (config disabled), got %d", throttled)
	}
	if queued != 0 {
		t.Errorf("Expected 0 queued requests initially, got %d", queued)
	}

	// Update config to enable throttling
	newConfig := common.Config{
		"scan.vector.throttle.minRRThreshold": common.ConfigValue{5, "throttling enabled", 5, false, false},
	}
	sac.SetConfig(newConfig)

	// Wait for controller to restart
	time.Sleep(200 * time.Millisecond)

	// Test that requests are admitted immediately when throttling is enabled but memory is good
	// This validates that the controller responds to memory conditions, not just config settings
	abortch1 := make(chan bool, 1)
	abortch2 := make(chan bool, 1)

	if !sac.AdmitScan("req1", abortch1) {
		t.Error("Expected immediate admission when throttling enabled but memory is good")
	}
	if !sac.AdmitScan("req2", abortch2) {
		t.Error("Expected immediate admission when throttling enabled but memory is good")
	}

	// Verify that no requests were queued (immediate admission due to good memory)
	totalQueued := sac.GetTotalQueued()
	if totalQueued != 0 {
		t.Errorf("Expected 0 total queued requests, but got %d", totalQueued)
	}

	// Verify current state
	throttled, queued = sac.GetStats()
	if throttled != 0 {
		t.Errorf("Expected throttled state 0 (memory is good), got %d", throttled)
	}
	if queued != 0 {
		t.Errorf("Expected 0 queued requests, got %d", queued)
	}

	// Test shutdown
	sac.Shutdown()
}

func TestScanAdmissionControllerProcessAllRequests(t *testing.T) {
	// Set storage mode to PLASMA to ensure controller runs
	common.SetClusterStorageMode(common.PLASMA)

	// This test validates that:
	// 1. Memory pressure automatically triggers throttling (avgResidentPercent < 5% AND RSS > quota)
	// 2. Multiple requests get queued when throttling is automatically enabled
	// 3. Requests are processed in batches of 50 (maxRequestsPerBatch) when memory improves
	// 4. All queued requests eventually get processed
	// 5. The totalQueued counter accurately tracks request history
	// Create mock stats and config for testing
	mockStats := &IndexerStats{}
	mockStats.Init()

	// Set mock memory values that WILL trigger throttling automatically
	mockStats.avgResidentPercent.Set(3)                 // 3% resident (below 5% threshold)
	mockStats.memoryRss.Set(2 * 1024 * 1024 * 1024)     // 2GB RSS (above 1.5GB quota)
	mockStats.memoryQuota.Set(1.5 * 1024 * 1024 * 1024) // 1.5GB quota (above 1GB threshold)

	mockConfig := common.Config{
		"scan.vector.throttle.minRRThreshold": common.ConfigValue{5, "throttling enabled", 5, false, false},
	}

	// Create mock supervisor message channel
	mockSupvMsgch := make(MsgChannel, 10)

	// Start a goroutine to listen on the supervisor message channel and respond
	go func() {
		for {
			select {
			case msg := <-mockSupvMsgch:
				if msg != nil {
					// Extract the response channel from the message and send true
					if msgReq, ok := msg.(*MsgIndexRRStats); ok && msgReq.respch != nil {
						msgReq.respch <- true
					}
				}
			}
		}
	}()

	sac := NewScanAdmissionController(mockStats, mockConfig, mockSupvMsgch)

	// Wait for controller to start and detect memory pressure
	time.Sleep(6 * time.Second)

	// Verify that throttling is automatically enabled due to memory pressure
	throttled, queued := sac.GetStats()
	if throttled != 1 {
		t.Errorf("Expected throttled state 1 due to memory pressure, got %d", throttled)
	}
	if queued != 0 {
		t.Errorf("Expected 0 queued requests initially, got %d", queued)
	}

	// Queue more than 50 requests to test batch processing
	// We'll queue 60 requests to ensure we test the batch limit of 50
	const numRequests = 60
	doneChannels := make([]chan bool, numRequests)

	for i := 0; i < numRequests; i++ {
		doneChannels[i] = make(chan bool, 1)
		abortch := make(chan bool, 1)
		go func(reqNum int) {
			admitted := sac.AdmitScan(fmt.Sprintf("req%d", reqNum+1), abortch)
			doneChannels[reqNum] <- admitted
		}(i)
	}

	// Wait for requests to be queued and verify using totalQueued counter
	time.Sleep(100 * time.Millisecond)

	// Verify requests are queued using deterministic counter
	totalQueued := sac.GetTotalQueued()
	if totalQueued != int64(numRequests) {
		t.Errorf("Expected total queued count to be %d, got %d", numRequests, totalQueued)
	}

	// Verify current state
	throttled, queued = sac.GetStats()
	if throttled != 1 {
		t.Errorf("Expected throttled state 1, got %d", throttled)
	}
	if queued != numRequests {
		t.Errorf("Expected %d queued requests, got %d", numRequests, queued)
	}

	// Improve memory conditions to automatically turn off throttling
	// This should trigger automatic processing of queued requests
	mockStats.avgResidentPercent.Set(10)                // 10% resident (above 5% threshold)
	mockStats.memoryRss.Set(100 * 1024 * 1024)          // 100MB RSS
	mockStats.memoryQuota.Set(1.5 * 1024 * 1024 * 1024) // 1.5GB quota (above 1GB threshold)

	// Wait for controller to detect improved memory and process queued requests
	// Since we have 15 requests and batch size is 10, it should take 2 batches
	time.Sleep(12 * time.Second)

	// Check that all queued requests got admitted
	for i := 0; i < numRequests; i++ {
		select {
		case admitted := <-doneChannels[i]:
			if !admitted {
				t.Errorf("Expected queued request %d to be admitted", i+1)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("Timeout waiting for admission of request %d", i+1)
		}
	}

	// Verify final state
	throttled, queued = sac.GetStats()
	if throttled != 0 {
		t.Errorf("Expected throttled state 0 after memory improvement, got %d", throttled)
	}
	if queued != 0 {
		t.Errorf("Expected 0 queued requests after processing, got %d", queued)
	}

	// Verify total queued count remains the same (requests were processed, not lost)
	finalTotalQueued := sac.GetTotalQueued()
	if finalTotalQueued != int64(numRequests) {
		t.Errorf("Expected total queued count to remain %d after processing, got %d", numRequests, finalTotalQueued)
	}

	sac.Shutdown()
}

func TestScanAdmissionControllerIndependentMemoryConditions(t *testing.T) {
	// Set storage mode to PLASMA to ensure controller runs
	common.SetClusterStorageMode(common.PLASMA)

	// This test validates the memory condition logic:
	// 1. lowRR condition: requires both poor RR AND RSS above quota (memoryRSS > memoryQuota)
	// 2. highRSS condition: triggers throttling independently when RSS > 1.05 * quota
	// 3. Both conditions can be active simultaneously
	// 4. Recovery from either condition works correctly
	// Create mock stats and config for testing
	mockStats := &IndexerStats{}
	mockStats.Init()

	mockConfig := common.Config{
		"scan.vector.throttle.minRRThreshold": common.ConfigValue{5, "throttling enabled", 5, false, false},
	}

	// Create mock supervisor message channel
	mockSupvMsgch := make(MsgChannel, 10)

	// Start a goroutine to listen on the supervisor message channel and respond
	go func() {
		for {
			select {
			case msg := <-mockSupvMsgch:
				if msg != nil {
					// Extract the response channel from the message and send true
					if msgReq, ok := msg.(*MsgIndexRRStats); ok && msgReq.respch != nil {
						msgReq.respch <- true
					}
				}
			}
		}
	}()

	sac := NewScanAdmissionController(mockStats, mockConfig, mockSupvMsgch)

	// Wait for controller to start
	time.Sleep(1 * time.Second)

	// Test 1: RR condition with RSS above quota (both conditions met for lowRR)
	t.Run("RRWithRSSAboveQuota", func(t *testing.T) {
		// Set memory values: poor RR + RSS above quota to trigger lowRR condition
		mockStats.avgResidentPercent.Set(3)                       // 3% resident (below 5% threshold)
		mockStats.memoryRss.Set((102 * 1024 * 1024 * 1024) / 100) // 1GB RSS (equal to 1GB quota)
		mockStats.memoryQuota.Set(1 * 1024 * 1024 * 1024)         // 1GB quota (above 1GB threshold)

		// Wait for controller to detect the change
		time.Sleep(6 * time.Second)

		// Verify throttling is enabled due to lowRR condition (both RR and RSS conditions met)
		throttled, queued := sac.GetStats()
		if throttled != 1 {
			t.Errorf("Expected throttled state 1 due to lowRR condition (poor RR + RSS above quota), got %d", throttled)
		}

		// Test that requests get queued
		doneCh := make(chan bool, 1)
		abortch := make(chan bool, 1)
		go func() {
			admitted := sac.AdmitScan("req_rr_only", abortch)
			doneCh <- admitted
		}()

		// Wait for request to be queued
		time.Sleep(50 * time.Millisecond)

		// Verify request is queued
		totalQueued := sac.GetTotalQueued()
		if totalQueued != 1 {
			t.Errorf("Expected 1 total queued request, got %d", totalQueued)
		}

		// Fix RR condition to stop throttling
		mockStats.avgResidentPercent.Set(10) // 10% resident (above 5% threshold)

		// Wait for controller to detect improvement
		time.Sleep(6 * time.Second)

		// Verify request gets admitted
		select {
		case admitted := <-doneCh:
			if !admitted {
				t.Error("Expected queued request to be admitted after RR improvement")
			}
		case <-time.After(5 * time.Second):
			t.Error("Timeout waiting for admission after RR improvement")
		}

		// Verify final state
		throttled, queued = sac.GetStats()
		if throttled != 0 {
			t.Errorf("Expected throttled state 0 after RR improvement, got %d", throttled)
		}
		if queued != 0 {
			t.Errorf("Expected 0 queued requests after RR improvement, got %d", queued)
		}
	})

	// Test 2: High RSS condition (triggers highRSS throttling independently)
	t.Run("HighRSSOnly", func(t *testing.T) {
		// Set memory values: good RR, very high RSS to trigger highRSS condition
		mockStats.avgResidentPercent.Set(10)                // 10% resident (above 5% threshold) - good
		mockStats.memoryRss.Set(3 * 1024 * 1024 * 1024)     // 3GB RSS (above 1.05 * 1.5GB = 1.575GB threshold)
		mockStats.memoryQuota.Set(1.5 * 1024 * 1024 * 1024) // 1.5GB quota (above 1GB threshold)

		// Wait for controller to detect the change
		time.Sleep(6 * time.Second)

		// Verify throttling is enabled due to highRSS condition
		throttled, queued := sac.GetStats()
		if throttled != 1 {
			t.Errorf("Expected throttled state 1 due to highRSS condition, got %d", throttled)
		}

		// Test that requests get queued
		doneCh := make(chan bool, 1)
		abortch := make(chan bool, 1)
		go func() {
			admitted := sac.AdmitScan("req_rss_only", abortch)
			doneCh <- admitted
		}()

		// Wait for request to be queued
		time.Sleep(50 * time.Millisecond)

		// Verify request is queued
		totalQueued := sac.GetTotalQueued()
		if totalQueued != 2 { // 1 from previous test + 1 from this test
			t.Errorf("Expected 2 total queued requests, got %d", totalQueued)
		}

		// Fix RSS condition to stop throttling (RSS now below quota)
		mockStats.memoryRss.Set(100 * 1024 * 1024) // 100MB RSS (below 1.5GB quota)

		// Wait for controller to detect improvement
		time.Sleep(6 * time.Second)

		// Verify request gets admitted
		select {
		case admitted := <-doneCh:
			if !admitted {
				t.Error("Expected queued request to be admitted after RSS improvement")
			}
		case <-time.After(5 * time.Second):
			t.Error("Timeout waiting for admission after RSS improvement")
		}

		// Verify final state
		throttled, queued = sac.GetStats()
		if throttled != 0 {
			t.Errorf("Expected throttled state 0 after RSS improvement, got %d", throttled)
		}
		if queued != 0 {
			t.Errorf("Expected 0 queued requests after RSS improvement, got %d", queued)
		}
	})

	// Test 3: Both conditions simultaneously
	t.Run("BothConditions", func(t *testing.T) {
		// Set memory values: both conditions poor
		mockStats.avgResidentPercent.Set(3)                 // 3% resident (below 5% threshold)
		mockStats.memoryRss.Set(3 * 1024 * 1024 * 1024)     // 3GB RSS (above 1.05 * 1.5GB = 1.575GB threshold)
		mockStats.memoryQuota.Set(1.5 * 1024 * 1024 * 1024) // 1.5GB quota (above 1GB threshold)

		// Wait for controller to detect the change
		time.Sleep(6 * time.Second)

		// Verify throttling is enabled due to both conditions
		throttled, queued := sac.GetStats()
		if throttled != 1 {
			t.Errorf("Expected throttled state 1 due to both poor conditions, got %d", throttled)
		}

		// Test that requests get queued
		doneCh := make(chan bool, 1)
		abortch := make(chan bool, 1)
		go func() {
			admitted := sac.AdmitScan("req_both_conditions", abortch)
			doneCh <- admitted
		}()

		// Wait for request to be queued
		time.Sleep(50 * time.Millisecond)

		// Verify request is queued
		totalQueued := sac.GetTotalQueued()
		if totalQueued != 3 { // 2 from previous tests + 1 from this test
			t.Errorf("Expected 3 total queued requests, got %d", totalQueued)
		}

		// Fix only RR condition (RSS still poor)
		mockStats.avgResidentPercent.Set(10) // 10% resident (above 5% threshold)

		// Wait for controller to detect change
		time.Sleep(6 * time.Second)

		// Verify throttling is still enabled (RSS still poor - highRSS condition)
		throttled, queued = sac.GetStats()
		if throttled != 1 {
			t.Errorf("Expected throttled state 1 (RSS still poor - highRSS condition), got %d", throttled)
		}

		// Fix RSS condition to stop throttling completely (RSS now below quota)
		mockStats.memoryRss.Set(100 * 1024 * 1024) // 100MB RSS (below 1.5GB quota)

		// Wait for controller to detect improvement
		time.Sleep(6 * time.Second)

		// Verify request gets admitted
		select {
		case admitted := <-doneCh:
			if !admitted {
				t.Error("Expected queued request to be admitted after both conditions improve")
			}
		case <-time.After(10 * time.Second):
			t.Error("Timeout waiting for admission after both conditions improve")
		}

		// Verify final state
		throttled, queued = sac.GetStats()
		if throttled != 0 {
			t.Errorf("Expected throttled state 0 after both conditions improve, got %d", throttled)
		}
		if queued != 0 {
			t.Errorf("Expected 0 queued requests after both conditions improve, got %d", queued)
		}
	})

	// Test 4: Verify lowRR condition requires RSS above quota (not just above 95%)
	t.Run("LowRRRequiresRSSAboveQuota", func(t *testing.T) {
		// Set memory values: poor RR but RSS below quota (should NOT trigger lowRR)
		mockStats.avgResidentPercent.Set(3)                 // 3% resident (below 5% threshold) - poor RR
		mockStats.memoryRss.Set(1 * 1024 * 1024 * 1024)     // 1GB RSS (below 1.5GB quota)
		mockStats.memoryQuota.Set(1.5 * 1024 * 1024 * 1024) // 1.5GB quota (above 1GB threshold)

		// Wait for controller to detect the change
		time.Sleep(6 * time.Second)

		// Verify throttling is NOT enabled because RSS is below quota (lowRR condition not met)
		throttled, queued := sac.GetStats()
		if throttled != 0 {
			t.Errorf("Expected throttled state 0 (poor RR but RSS below quota), got %d", throttled)
		}

		// Test that requests are admitted immediately (no throttling)
		abortch := make(chan bool, 1)
		if !sac.AdmitScan("req_lowrr_below_quota", abortch) {
			t.Error("Expected immediate admission when RR is poor but RSS is below quota")
		}

		// Verify no requests were queued
		totalQueued := sac.GetTotalQueued()
		if totalQueued != 3 { // Should still be 3 from previous tests
			t.Errorf("Expected 3 total queued requests, got %d", totalQueued)
		}

		// Now increase RSS above quota to trigger lowRR condition
		mockStats.memoryRss.Set(2 * 1024 * 1024 * 1024) // 2GB RSS (above 1.5GB quota)

		// Wait for controller to detect the change
		time.Sleep(6 * time.Second)

		// Verify throttling is now enabled due to lowRR condition (both RR and RSS conditions met)
		throttled, queued = sac.GetStats()
		if throttled != 1 {
			t.Errorf("Expected throttled state 1 after RSS increased above quota, got %d", throttled)
		}

		// Test that new requests get queued
		doneCh := make(chan bool, 1)
		abortch2 := make(chan bool, 1)
		go func() {
			admitted := sac.AdmitScan("req_lowrr_above_quota", abortch2)
			doneCh <- admitted
		}()

		// Wait for request to be queued
		time.Sleep(50 * time.Millisecond)

		// Verify request is queued
		totalQueued = sac.GetTotalQueued()
		if totalQueued != 4 { // 3 from previous tests + 1 from this test
			t.Errorf("Expected 4 total queued requests, got %d", totalQueued)
		}

		// Fix RR condition to stop throttling
		mockStats.avgResidentPercent.Set(10)       // 10% resident (above 5% threshold)
		mockStats.memoryRss.Set(100 * 1024 * 1024) // 100MB RSS (below 1.5GB quota)

		// Wait for controller to detect improvement
		time.Sleep(6 * time.Second)

		// Verify request gets admitted
		select {
		case admitted := <-doneCh:
			if !admitted {
				t.Error("Expected queued request to be admitted after RR improvement")
			}
		case <-time.After(5 * time.Second):
			t.Error("Timeout waiting for admission after RR improvement")
		}

		// Verify final state
		throttled, queued = sac.GetStats()
		if throttled != 0 {
			t.Errorf("Expected throttled state 0 after RR improvement, got %d", throttled)
		}
		if queued != 0 {
			t.Errorf("Expected 0 queued requests after RR improvement, got %d", queued)
		}
	})

	sac.Shutdown()
}
