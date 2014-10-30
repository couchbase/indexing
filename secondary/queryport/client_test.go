package queryport

import (
	"reflect"
	"testing"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	"github.com/couchbaselabs/goprotobuf/proto"
)

var testStatisticsResponse = &protobuf.StatisticsResponse{
	Stats: &protobuf.IndexStatistics{
		Count:      proto.Uint64(100),
		UniqueKeys: proto.Uint64(100),
		Min:        []byte("aaaaa"),
		Max:        []byte("zzzzz"),
	},
}

var testResponseStream = &protobuf.ResponseStream{
	Entries: []*protobuf.IndexEntry{
		&protobuf.IndexEntry{
			EntryKey: []byte("aaaaa"), PrimaryKey: []byte("key"),
		},
		&protobuf.IndexEntry{
			EntryKey: []byte("aaaaa"), PrimaryKey: []byte("key"),
		},
	},
}

func TestStatistics(t *testing.T) {
	common.LogIgnore()
	//common.SetLogLevel(common.LogLevelDebug)

	addr := "localhost:8888"
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

	client := NewClient(addr, common.SystemConfig)

	out, err := client.Statistics("idx", "bkt", []byte("aaaa"), []byte("zzzz"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(out, testStatisticsResponse.GetStats()) == false {
		t.Fatal("failed on client.Statistics()")
	}
	client.Close()
	s.Close()
}

func TestScan(t *testing.T) {
	common.LogIgnore()
	addr := "localhost:8888"
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

	client := NewClient(addr, common.SystemConfig)

	count := 0
	client.Scan(
		"idx", "bkt", []byte("aaaa"), []byte("zzzz"), 0, 100, true, 1000,
		func(val interface{}) bool {
			switch v := val.(type) {
			case *protobuf.ResponseStream:
				count++
				if count == 10000 {
					return false
				}

			case error:
				t.Fatal(v)
			}
			return true
		})

	count = 0
	client.Scan(
		"idx", "bkt", []byte("aaaa"), []byte("zzzz"), 0, 100, true, 1000,
		func(val interface{}) bool {
			count++
			if count == 2 {
				return false
			}
			return true
		})

	client.Close()
	s.Close()
}

func TestScanAll(t *testing.T) {
	common.LogIgnore()
	addr := "localhost:8888"
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

	client := NewClient(addr, common.SystemConfig)

	count := 0
	client.ScanAll(
		"idx", "bkt",
		100, 1000,
		func(val interface{}) bool {
			switch v := val.(type) {
			case *protobuf.ResponseStream:
				count++
				if count == 10000 {
					return false
				}

			case error:
				t.Fatal(v)
			}
			return true
		})

	count = 0
	client.ScanAll(
		"idx", "bkt", 100, 1000,
		func(val interface{}) bool {
			count++
			if count == 2 {
				return false
			}
			return true
		})

	client.Close()
	s.Close()
}

func BenchmarkStatistics(b *testing.B) {
	common.LogIgnore()
	addr := "localhost:8888"
	callb := func(
		req interface{}, respch chan<- interface{}, quitch <-chan interface{}) {

		respch <- testStatisticsResponse
		close(respch)
	}
	s := startServer(b, addr, callb)
	time.Sleep(100 * time.Millisecond)

	client := NewClient(addr, common.SystemConfig)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.Statistics("idx", "bkt", []byte("aaaa"), []byte("zzzz"), 0)
	}
	b.StopTimer()
	s.Close()
	client.Close()
	time.Sleep(100 * time.Millisecond)
}

func BenchmarkScan1(b *testing.B) {
	common.LogIgnore()
	addr := "localhost:8888"
	callb := func(
		req interface{}, respch chan<- interface{}, quitch <-chan interface{}) {

		respch <- testResponseStream
		close(respch)
	}
	s := startServer(b, addr, callb)
	time.Sleep(100 * time.Millisecond)

	client := NewClient(addr, common.SystemConfig)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.Scan(
			"idx", "bkt", []byte("aaaa"), []byte("zzzz"), 0, 100, true, 1000,
			func(val interface{}) bool {
				return true
			})
	}
	b.StopTimer()

	s.Close()
	client.Close()
	time.Sleep(100 * time.Millisecond)
}

func BenchmarkScan100(b *testing.B) {
	common.LogIgnore()
	addr := "localhost:8888"
	callb := func(
		req interface{}, respch chan<- interface{}, quitch <-chan interface{}) {

		for i := 0; i < 100; i++ {
			respch <- testResponseStream
		}
		close(respch)
	}
	s := startServer(b, addr, callb)
	time.Sleep(100 * time.Millisecond)

	client := NewClient(addr, common.SystemConfig)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.Scan(
			"idx", "bkt", []byte("aaaa"), []byte("zzzz"), 0, 100, true, 1000,
			func(val interface{}) bool {
				return true
			})
	}
	b.StopTimer()

	s.Close()
	client.Close()
	time.Sleep(100 * time.Millisecond)
}

func BenchmarkScanParallel10(b *testing.B) {
	common.LogIgnore()
	addr := "localhost:8888"
	callb := func(
		req interface{}, respch chan<- interface{}, quitch <-chan interface{}) {

		respch <- testResponseStream
		close(respch)
	}
	s := startServer(b, addr, callb)
	time.Sleep(100 * time.Millisecond)

	client := NewClient(addr, common.SystemConfig)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.Scan(
			"idx", "bkt", []byte("aaaa"), []byte("zzzz"), 0, 100, true, 1000,
			func(val interface{}) bool {
				return false
			})
	}
	b.StopTimer()

	s.Close()
	client.Close()
	time.Sleep(100 * time.Millisecond)
}

func BenchmarkScanAll(b *testing.B) {
	common.LogIgnore()
	addr := "localhost:8888"
	callb := func(
		req interface{}, respch chan<- interface{}, quitch <-chan interface{}) {

		respch <- testResponseStream
		close(respch)
	}
	s := startServer(b, addr, callb)
	time.Sleep(100 * time.Millisecond)

	client := NewClient(addr, common.SystemConfig)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.ScanAll(
			"idx", "bkt", 100, 1000,
			func(val interface{}) bool {
				return true
			})
	}
	b.StopTimer()

	s.Close()
	client.Close()
	time.Sleep(100 * time.Millisecond)
}

func startServer(tb testing.TB, laddr string, callb RequestHandler) *Server {
	s, err := NewServer(laddr, callb, common.SystemConfig)
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
