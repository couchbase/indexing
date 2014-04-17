package indexer

import (
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	"io/ioutil"
	"log"
	"reflect"
	"testing"
	"time"
)

var addr = "localhost:8888"
var count = 0
var msgch = make(chan interface{}, 1000)

func TestLoopback(t *testing.T) {
	var client *StreamClient
	var err error

	log.SetOutput(ioutil.Discard)

	vbuckets := []uint16{1, 2, 3, 4}
	vbuuids := []uint64{10, 20, 30, 40}
	vbmap := common.VbConnectionMap{Vbuckets: vbuckets, Vbuuids: vbuuids}

	doServer(addr, t, msgch)

	// start client
	if client, err = NewStreamClient(addr, 2, vbmap); err != nil {
		t.Fatal(err)
	}

	// test timeouts
	if msg, ok := (<-msgch).(RestartVbuckets); ok {
		if reflect.DeepEqual(msg.vbnos, []uint16{1, 3, 2, 4}) == false {
			t.Fatal(fmt.Errorf("mismatch in restart vbuckets"))
		}
	}

	// stop client and restart
	client.Stop()
	if client, err = NewStreamClient(addr, 2, vbmap); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	// test loop back of valid key-versions
	ks_ref := make([]*common.KeyVersions, 0)
	k := common.NewUpsert(1, 10, []byte("cities"), 10000000)
	k.Keys = [][]byte{[]byte("bangalore"), []byte("delhi"), []byte("jaipur")}
	k.Oldkeys = [][]byte{[]byte("varanasi"), []byte("pune"), []byte("mahe")}
	k.Indexids = []uint32{1, 2, 3}
	ks_ref = append(ks_ref, k)

	if err := client.SendKeyVersions(ks_ref); err != nil {
		t.Fatal(err)
	}

	val := <-msgch
	if ks, ok := (val).([]*protobuf.KeyVersions); ok {
		if len(ks) != len(ks_ref) {
			t.Fatal(fmt.Errorf("mismatch in number of key versions"))
		}
		ks_lb := protobuf2KeyVersions(ks)
		if reflect.DeepEqual(ks_lb[0], ks_ref[0]) == false {
			t.Fatal(fmt.Errorf("unexpected response"))
		}
	} else {
		t.Fatal(fmt.Errorf("unexpected type in loopback"))
	}

	// test DropData
	ks_ref = make([]*common.KeyVersions, 0)
	k = common.NewDropData(4, 10, 2)
	k.Keys = [][]byte{[]byte("bangalore"), []byte("delhi"), []byte("jaipur")}
	k.Oldkeys = [][]byte{[]byte("varanasi"), []byte("pune"), []byte("mahe")}
	k.Indexids = []uint32{1, 2, 3}
	ks_ref = append(ks_ref, k)

	if err := client.SendKeyVersions(ks_ref); err != nil {
		t.Fatal(err)
	}

	if msg, ok := (<-msgch).(DropVbucketData); ok {
		if reflect.DeepEqual(msg.vbnos, []uint16{4}) == false {
			t.Fatal(fmt.Errorf("mismatch in drop vbuckets"))
		}
	} else {
		t.Fatal(fmt.Errorf("unexpected type in loopback"))
	}
	client.Stop()
}

func BenchmarkLoopback(b *testing.B) {
	var client *StreamClient
	var err error

	vbuckets := []uint16{1, 2, 3, 4}
	vbuuids := []uint64{10, 20, 30, 40}
	vbmap := common.VbConnectionMap{Vbuckets: vbuckets, Vbuuids: vbuuids}
	if client, err = NewStreamClient(addr, 1, vbmap); err != nil {
		b.Fatal(err)
	}

	ks_ref := make([]*common.KeyVersions, 0)
	k := common.NewUpsert(1, 10, []byte("cities"), 10000000)
	k.Keys = [][]byte{[]byte("bangalore"), []byte("delhi"), []byte("jaipur")}
	k.Oldkeys = [][]byte{[]byte("varanasi"), []byte("pune"), []byte("mahe")}
	k.Indexids = []uint32{1, 2, 3}
	ks_ref = append(ks_ref, k)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.SendKeyVersions(ks_ref)
		<-msgch
	}
	client.Stop()
}

func doServer(addr string, tb testing.TB, msgch chan interface{}) *MutationStream {
	var mStream *MutationStream
	var err error

	mutch := make(chan interface{}, 10000)
	sbch := make(chan interface{}, 100)
	tm := 2 * time.Second

	if mStream, err = NewMutationStream(addr, mutch, sbch, tm); err != nil {
		tb.Fatal(err)
	}

	go func() {
		var mutn, err interface{}
		var ok bool
		for {
			select {
			case mutn, ok = <-mutch:
				msgch <- mutn
			case err, ok = <-sbch:
				msgch <- err
			}
			if ok == false {
				return
			}
		}
	}()
	return mStream
}
