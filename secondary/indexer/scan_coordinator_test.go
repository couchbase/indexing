// +build ignore

package indexer

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
	if val.Error() != nil && val.Error().Error() != ErrIndexNotFound.Error() {
		tst.Error("Unexpected response", val.Error(), ErrIndexNotFound)
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
