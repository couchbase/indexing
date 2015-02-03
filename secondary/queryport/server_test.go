// +build ignore

package queryport

import "reflect"
import "testing"
import "time"

import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/queryport/client"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
import "github.com/couchbaselabs/goprotobuf/proto"

var testStatisticsResponse = &protobuf.StatisticsResponse{
	Stats: &protobuf.IndexStatistics{
		KeysCount:       proto.Uint64(100),
		UniqueKeysCount: proto.Uint64(100),
		KeyMin:          []byte("aaaaa"),
		KeyMax:          []byte("zzzzz"),
	},
}

var testResponseStream = &protobuf.ResponseStream{
	IndexEntries: []*protobuf.IndexEntry{
		&protobuf.IndexEntry{
			EntryKey: []byte(`["aaaaa"]`), PrimaryKey: []byte("key"),
		},
		&protobuf.IndexEntry{
			EntryKey: []byte(`["aaaaa"]`), PrimaryKey: []byte("key"),
		},
	},
}

func TestStatistics(t *testing.T) {
	c.LogIgnore()
	//c.SetLogLevel(c.LogLevelDebug)

	addr := "localhost:9101"
	serverCallb := func(
		req interface{}, respch chan<- interface{}, quitch <-chan interface{}) {

		switch req.(type) {
		case *protobuf.StatisticsRequest:
			resp := testStatisticsResponse
			select {
			case respch <- resp:
				close(respch)
			case <-quitch:
				t.Fatal("unexpected quit", req)
			}

		default:
			t.Fatal("unknown request", req)
		}
	}

	s := startServer(t, addr, serverCallb)
	time.Sleep(100 * time.Millisecond)

	config := c.SystemConfig.SectionConfig("queryport.client.", true)
	client, err := client.NewGsiClient("localhost:9000", config)
	if err != nil {
		t.Fatal(err)
	}

	l, h := c.SecondaryKey{[]byte("aaaa")}, c.SecondaryKey{[]byte("zzzz")}
	out, err := client.RangeStatistics(0x0 /*defnID*/, l, h, 0)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(out, testStatisticsResponse.GetStats()) == false {
		t.Fatal("failed on client.Statistics()")
	}
	client.Close()
	s.Close()
}

func TestRange(t *testing.T) {
	c.LogIgnore()
	addr := "localhost:9101"
	serverCallb := func(
		req interface{}, respch chan<- interface{}, quitch <-chan interface{}) {

		switch req.(type) {
		case *protobuf.ScanRequest:
		default:
			t.Fatal("unknown request", req)
		}
		sendResponse(t, 10000, respch, quitch)
	}
	s := startServer(t, addr, serverCallb)
	time.Sleep(100 * time.Millisecond)

	config := c.SystemConfig.SectionConfig("queryport.client.", true)
	qc, err := client.NewGsiClient("localhost:9000", config)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	l, h := c.SecondaryKey{[]byte("aaaa")}, c.SecondaryKey{[]byte("zzzz")}
	qc.Range(
		0x0 /*defnID*/, l, h, 100, true, 1000,
		func(val client.ResponseReader) bool {
			if err := val.Error(); err != nil {
				t.Fatal(err)
			} else if skeys, _, err := val.GetEntries(); err != nil {
				t.Fatal(err)

			} else if len(skeys) > 0 {
				count++
				if count == 10000 {
					return false
				}
			}
			return true
		})

	count = 0
	l, h = c.SecondaryKey{[]byte("aaaa")}, c.SecondaryKey{[]byte("zzzz")}
	qc.Range(
		0x0 /*defnID*/, l, h, 100, true, 1000,
		func(val client.ResponseReader) bool {
			count++
			if count == 2 {
				return false
			}
			return true
		})

	qc.Close()
	s.Close()
}

func TestScanAll(t *testing.T) {
	c.LogIgnore()
	addr := "localhost:9101"
	callb := func(
		req interface{}, respch chan<- interface{}, quitch <-chan interface{}) {

		switch req.(type) {
		case *protobuf.ScanAllRequest:
		default:
			t.Fatal("unknown request", req)
		}
		sendResponse(t, 10000, respch, quitch)
	}
	s := startServer(t, addr, callb)
	time.Sleep(100 * time.Millisecond)

	config := c.SystemConfig.SectionConfig("queryport.client.", true)
	qc, err := client.NewGsiClient("localhost:9000", config)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	qc.ScanAll(
		0x0 /*defnID*/, 1000,
		func(val client.ResponseReader) bool {
			if err := val.Error(); err != nil {
				t.Fatal(err)
			} else if skeys, _, err := val.GetEntries(); err != nil {
				t.Fatal(err)
			} else if len(skeys) > 0 {
				count++
				if count == 10000 {
					return false
				}
			}
			return true
		})

	count = 0
	qc.ScanAll(
		0x0 /*defnID*/, 1000,
		func(val client.ResponseReader) bool {
			count++
			if count == 2 {
				return false
			}
			return true
		})

	qc.Close()
	s.Close()
}

func BenchmarkStatistics(b *testing.B) {
	c.LogIgnore()
	addr := "localhost:9101"
	callb := func(
		req interface{}, respch chan<- interface{}, quitch <-chan interface{}) {

		respch <- testStatisticsResponse
		close(respch)
	}
	s := startServer(b, addr, callb)
	time.Sleep(100 * time.Millisecond)

	config := c.SystemConfig.SectionConfig("queryport.client.", true)
	qc, err := client.NewGsiClient("localhost:9000", config)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	l, h := c.SecondaryKey{[]byte("aaaa")}, c.SecondaryKey{[]byte("zzzz")}
	for i := 0; i < b.N; i++ {
		qc.RangeStatistics(0x0 /*defnID*/, l, h, 0)
	}
	b.StopTimer()
	s.Close()
	qc.Close()
	time.Sleep(100 * time.Millisecond)
}

func BenchmarkRange1(b *testing.B) {
	c.LogIgnore()
	addr := "localhost:9101"
	callb := func(
		req interface{}, respch chan<- interface{}, quitch <-chan interface{}) {

		respch <- testResponseStream
		close(respch)
	}
	s := startServer(b, addr, callb)
	time.Sleep(100 * time.Millisecond)

	config := c.SystemConfig.SectionConfig("queryport.client.", true)
	qc, err := client.NewGsiClient("localhost:9000", config)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	l, h := c.SecondaryKey{[]byte("aaaa")}, c.SecondaryKey{[]byte("zzzz")}
	for i := 0; i < b.N; i++ {
		qc.Range(
			0x0 /*defnID*/, l, h, 100, true, 1000,
			func(val client.ResponseReader) bool {
				return true
			})
	}
	b.StopTimer()

	s.Close()
	qc.Close()
	time.Sleep(100 * time.Millisecond)
}

func BenchmarkRange100(b *testing.B) {
	c.LogIgnore()
	addr := "localhost:9101"
	callb := func(
		req interface{}, respch chan<- interface{}, quitch <-chan interface{}) {

		for i := 0; i < 100; i++ {
			respch <- testResponseStream
		}
		close(respch)
	}
	s := startServer(b, addr, callb)
	time.Sleep(100 * time.Millisecond)

	config := c.SystemConfig.SectionConfig("queryport.client.", true)
	qc, err := client.NewGsiClient("localhost:9000", config)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	l, h := c.SecondaryKey{[]byte("aaaa")}, c.SecondaryKey{[]byte("zzzz")}
	for i := 0; i < b.N; i++ {
		qc.Range(
			0x0 /*defnID*/, l, h, 100, true, 1000,
			func(val client.ResponseReader) bool {
				return true
			})
	}
	b.StopTimer()

	s.Close()
	qc.Close()
	time.Sleep(100 * time.Millisecond)
}

func BenchmarkRangeParallel10(b *testing.B) {
	c.LogIgnore()
	addr := "localhost:9101"
	callb := func(
		req interface{}, respch chan<- interface{}, quitch <-chan interface{}) {

		respch <- testResponseStream
		close(respch)
	}
	s := startServer(b, addr, callb)
	time.Sleep(100 * time.Millisecond)

	config := c.SystemConfig.SectionConfig("queryport.client.", true)
	qc, err := client.NewGsiClient("localhost:9000", config)
	if err != nil {
		b.Fatal(err)
	}

	l, h := c.SecondaryKey{[]byte("aaaa")}, c.SecondaryKey{[]byte("zzzz")}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qc.Range(
			0x0 /*defnID*/, l, h, 100, true, 1000,
			func(val client.ResponseReader) bool {
				return false
			})
	}
	b.StopTimer()

	s.Close()
	qc.Close()
	time.Sleep(100 * time.Millisecond)
}

func BenchmarkScanAll(b *testing.B) {
	c.LogIgnore()
	addr := "localhost:9101"
	callb := func(
		req interface{}, respch chan<- interface{}, quitch <-chan interface{}) {

		respch <- testResponseStream
		close(respch)
	}
	s := startServer(b, addr, callb)
	time.Sleep(100 * time.Millisecond)

	config := c.SystemConfig.SectionConfig("queryport.client.", true)
	qc, err := client.NewGsiClient("localhost:9000", config)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qc.ScanAll(
			0x0 /*defnID*/, 1000,
			func(val client.ResponseReader) bool {
				return true
			})
	}
	b.StopTimer()

	s.Close()
	qc.Close()
	time.Sleep(100 * time.Millisecond)
}

func startServer(tb testing.TB, laddr string, callb RequestHandler) *Server {
	config := c.SystemConfig.SectionConfig("queryport.indexer.", true)
	s, err := NewServer(laddr, callb, config)
	if err != nil {
		tb.Fatal(err)
	}
	return s
}

// server callback
func sendResponse(
	tb testing.TB, count int,
	respch chan<- interface{}, quitch <-chan interface{}) {

	i := 0
loop:
	for ; i < count; i++ {
		select {
		case respch <- testResponseStream:
		case <-quitch:
			break loop
		}
	}
	close(respch)
}
