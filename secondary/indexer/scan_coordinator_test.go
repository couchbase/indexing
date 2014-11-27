package indexer

import (
	"encoding/json"
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
	"github.com/couchbase/indexing/secondary/queryport"
	"github.com/couchbaselabs/goprotobuf/proto"
	"reflect"
	"testing"
)

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

func testSK(i int) []string {
	return []string{fmt.Sprintf("SecKey-%d", i)}
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

func verifyInvalidIndex(val interface{}) bool {
	switch val.(type) {
	case *protobuf.ResponseStream:
		msg := val.(*protobuf.ResponseStream)
		if msg.GetErr().GetError() != ErrIndexNotFound.Error() {
			tst.Error("Unexpected response")
		}
	case error:
		tst.Fatal(val)
	}
	return true
}

func verifyIndexScanAll(val interface{}) bool {
	switch val.(type) {
	case *protobuf.ResponseStream:
		msg := val.(*protobuf.ResponseStream)
		entries := msg.GetEntries()
		if len(entries) > 0 {
			nmesgs++
			for _, entry := range entries {
				sk, _ := json.Marshal(testSK(count))
				pk := testPK(count)
				if string(sk) != string(entry.GetEntryKey()) {
					tst.Error("Invalid sec key received")
				}

				if string(pk) != string(entry.GetPrimaryKey()) {
					tst.Error("Invalid primary key received")
				}
				count++
			}
		} else {
			if msg.GetErr() == nil {
				tst.Fatal("Received invalid message")
			}

			err := msg.GetErr().GetError()
			if err != ErrInternal.Error() {
				tst.Fatal("Expected error :", err, ", Received :",
					ErrInternal.Error())
			}
			nerrors++
		}

	case error:
		tst.Fatal(val)
	}
	return true
}

func TestInvalidIndexScan(t *testing.T) {
	initEnv()
	h, err := newScannerTestHarness()
	if err != nil {
		t.Fatal(err)
	}
	defer h.Shutdown()

	tst = t
	h.createIndex("idx", "default", simpleKeyFeeder)
	client := queryport.NewClient(QUERY_PORT_ADDR, c.SystemConfig)
	client.ScanAll("invalid", "default", 1, 40, verifyInvalidIndex)
	client.Close()

}

func TestIndexScan(t *testing.T) {
	initEnv()
	// TODO: Add range verification
	h, err := newScannerTestHarness()
	if err != nil {
		t.Fatal(err)
	}
	defer h.Shutdown()

	tst = t
	count = 0
	nkeys = 100
	h.createIndex("idx", "default", simpleKeyFeeder)
	client := queryport.NewClient(QUERY_PORT_ADDR, c.SystemConfig)
	low, _ := json.Marshal([]string{"low"})
	high, _ := json.Marshal([]string{"high"})
	keys := [][]byte{}
	client.Scan("idx", "default", low, high, keys, uint32(Both), 0, false, 0,
		verifyIndexScanAll)
	client.Close()
	if count != nkeys {
		t.Error("Scan result entries count mismatch", count, "!=", nkeys)
	}
}

func TestIndexScanAll(t *testing.T) {
	initEnv()
	h, err := newScannerTestHarness()
	if err != nil {
		t.Fatal(err)
	}
	defer h.Shutdown()

	tst = t
	count = 0
	nkeys = 100
	h.createIndex("idx", "default", simpleKeyFeeder)
	client := queryport.NewClient(QUERY_PORT_ADDR, c.SystemConfig)
	client.ScanAll("idx", "default", 0, 0, verifyIndexScanAll)
	client.Close()
	if count != nkeys {
		t.Error("Scan result entries count mismatch", count, "!=", nkeys)
	}
}

func TestIndexScanAllLimit(t *testing.T) {
	initEnv()
	h, err := newScannerTestHarness()
	if err != nil {
		t.Fatal(err)
	}
	defer h.Shutdown()

	tst = t
	count = 0
	nkeys = 10000
	h.createIndex("idx", "default", simpleKeyFeeder)
	client := queryport.NewClient(QUERY_PORT_ADDR, c.SystemConfig)
	client.ScanAll("idx", "default", 0, 100, verifyIndexScanAll)
	client.Close()
	if count != 100 {
		t.Error("Scan result entries count mismatch", count, "!=", 100)
	}
}

func TestScanEmptyIndex(t *testing.T) {
	initEnv()
	h, err := newScannerTestHarness()
	if err != nil {
		t.Fatal(err)
	}
	defer h.Shutdown()

	tst = t
	count = 0
	nkeys = 0
	h.createIndex("idx", "default", simpleKeyFeeder)
	client := queryport.NewClient(QUERY_PORT_ADDR, c.SystemConfig)
	client.ScanAll("idx", "default", 0, 0, verifyIndexScanAll)
	client.Close()
	if count != 0 {
		t.Error("Scan result entries count mismatch", count, "!=", nkeys)
	}
}

func TestIndexScanErrors(t *testing.T) {
	initEnv()
	h, err := newScannerTestHarness()
	if err != nil {
		t.Fatal(err)
	}
	defer h.Shutdown()

	tst = t
	count = 0
	nkeys = 100
	nerrors = 0
	h.createIndex("idx", "default", simpleErrorFeeder)
	client := queryport.NewClient(QUERY_PORT_ADDR, c.SystemConfig)
	client.ScanAll("idx", "default", 0, 0, verifyIndexScanAll)

	if count != 100 {
		t.Error("Scan result entries count mismatch", count, "!=", 100)
	}

	if nerrors == 0 {
		t.Error("Expected a scan error")
	}

	count = 0
	nerrors = 0
	nkeys = 0

	client.ScanAll("idx", "default", 0, 0, verifyIndexScanAll)
	if count != 0 {
		t.Error("Scan result entries count mismatch", count, "!=", 0)
	}
	if nerrors == 0 {
		t.Error("Expected a scan error")
	}

	client.Close()
}

func TestScanPageSize(t *testing.T) {
	initEnv()
	h, err := newScannerTestHarness()
	if err != nil {
		t.Fatal(err)
	}
	defer h.Shutdown()

	// TODO: Verify page size wrt entries page size received from response
	// message.

	tst = t
	count = 0
	nerrors = 0
	nmesgs = 0
	nkeys = 10000

	h.createIndex("idx", "default", simpleKeyFeeder)
	client := queryport.NewClient(QUERY_PORT_ADDR, c.SystemConfig)
	client.ScanAll("idx", "default", 4092, 0, verifyIndexScanAll)
	client.Close()
	if count != nkeys {
		t.Error("Scan result entries count mismatch", count, "!=", nkeys)
	}

	if nmesgs == nkeys {
		t.Error("Index entry pages were not generated")
	}
}

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
			Count:      proto.Uint64(uint64(nkeys)),
			UniqueKeys: proto.Uint64(0),
			Min:        []byte("min"),
			Max:        []byte("max"),
		},
	}

	h.createIndex("idx", "default", simpleKeyFeeder)
	client := queryport.NewClient(QUERY_PORT_ADDR, c.SystemConfig)
	low, _ := json.Marshal([]string{"low"})
	high, _ := json.Marshal([]string{"high"})
	keys := [][]byte{}
	out, err := client.Statistics("idx", "default", low,
		high, keys, 0)

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

	h.createIndex("idx", "default", simpleErrorFeeder)
	client := queryport.NewClient(QUERY_PORT_ADDR, c.SystemConfig)
	low, _ := json.Marshal([]string{"low"})
	high, _ := json.Marshal([]string{"high"})
	keys := [][]byte{}
	_, err = client.Statistics("idx", "default", low,
		high, keys, 0)

	if err == ErrInternal {
		t.Errorf("Unexpected stats err %v", err)
	}

	client.Close()
}
