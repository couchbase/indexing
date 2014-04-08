package indexer

import (
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"io"
	"io/ioutil"
	"log"
	"reflect"
	"testing"
)

var addr = "localhost:8888"
var msgch = make(chan *common.Mutation)
var count = 0

func TestLoopBack(t *testing.T) {
	var client *StreamClient
	var err error

	log.SetOutput(ioutil.Discard)
	doServer(addr, t)

	if client, err = NewStreamClient(addr, 1); err != nil {
		t.Fatal(err)
	}

	m := &common.Mutation{
		Version:  byte(1),
		Command:  byte(1),
		Vbucket:  uint16(512),
		Vbuuid:   uint64(0x1234567812345678),
		Docid:    []byte("cities"),
		Seqno:    uint64(10000000),
		Keys:     [][]byte{[]byte("bangalore"), []byte("delhi"), []byte("jaipur")},
		Oldkeys:  [][]byte{[]byte("varanasi"), []byte("pune"), []byte("mahe")},
		Indexids: []uint32{uint32(1), uint32(2), uint32(3)},
	}

	if err := client.Send(m); err != nil {
		t.Fatal(err)
	}
	mback := <-msgch
	if reflect.DeepEqual(m, mback) == false {
		t.Fatal(fmt.Errorf("unexpected response"))
	}
	client.Stop()
	msgch = nil
}

func BenchmarkClientRequest(b *testing.B) {
	var client *StreamClient
	var err error

	log.SetOutput(ioutil.Discard)

	if client, err = NewStreamClient(addr, 24); err != nil {
		b.Fatal(err)
	}

	m := &common.Mutation{
		Version:  byte(1),
		Command:  byte(1),
		Vbucket:  uint16(512),
		Vbuuid:   uint64(0x1234567812345678),
		Docid:    []byte("cities"),
		Seqno:    uint64(10000000),
		Keys:     [][]byte{[]byte("bangalore"), []byte("delhi"), []byte("jaipur")},
		Oldkeys:  [][]byte{[]byte("varanasi"), []byte("pune"), []byte("mahe")},
		Indexids: []uint32{uint32(1), uint32(2), uint32(3)},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Vbucket = uint16(i % 1024)
		client.Send(m)
	}
	client.Stop()
}

func doServer(addr string, tb testing.TB) *StreamServer {
	var server *StreamServer
	var err error

	mutch := make(chan *common.Mutation, 1)
	errch := make(chan error)

	if server, err = NewStreamServer(addr, mutch, errch); err != nil {
		tb.Fatal(err)
	}

	go func() {
		for {
			select {
			case mutn, ok := <-mutch:
				if ok && msgch != nil {
					msgch <- mutn
				}
				count++
			case err, ok := <-errch:
				if ok {
					if err != io.EOF && err != StreamServerClosed {
						tb.Fatal(err)
					}
				} else {
					tb.Fatal(fmt.Errorf("error channel closed unexpected"))
				}
			}
		}
	}()
	return server
}
