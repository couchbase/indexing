package indexer

import (
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	"io/ioutil"
	"log"
	"testing"
)

// TODO:
// - test live StreamBegin and StreamEnd.

var addrST = "localhost:8888"

func TestStreamTimeout(t *testing.T) {
	maxconns, maxvbuckets, mutChanSize := 2, 4, 100
	log.SetOutput(ioutil.Discard)

	// start server
	msgch := make(chan interface{}, mutChanSize)
	errch := make(chan interface{}, 1000)
	daemon := doServer(addrST, t, msgch, errch, mutChanSize)
	flags := StreamTransportFlag(0).SetProtobuf()

	// start client
	client, _ := NewStreamClient(addrST, maxconns, flags)
	maxBuckets := 2
	// test timeouts
	vbmaps := makeVbmaps(maxvbuckets, maxBuckets) // vbmaps
	for i := 0; i < maxBuckets; i++ {
		if err := client.SendVbmap(vbmaps[i]); err != nil {
			t.Fatal(err)
		}
	}
	go func() {
		vbno, vbuuid := vbmaps[0].Vbuckets[0], vbmaps[0].Vbuuids[0]
		seqno, docid, maxCount := uint64(10), []byte(nil), 1
		for {
			// send sync messages
			vb := c.NewVbKeyVersions("default0", vbno, vbuuid, 1)
			kv := c.NewKeyVersions(seqno, docid, maxCount)
			kv.AddSync()
			vb.AddKeyVersions(kv)
			client.SendKeyVersions([]*c.VbKeyVersions{vb})
			seqno++
		}
	}()

	wait := true
	for wait {
		verify(msgch, errch, func(mutn, err interface{}) {
			if err == nil {
				return
			}
			ref := ((maxvbuckets / maxconns) * maxBuckets)
			if rs, ok := (err).([]*RestartVbuckets); ok { // check
				if len(rs) != 2 {
					t.Fatal("mismatch in restart vbuckets")
				}
				refBuckets := map[string]bool{"default0": true, "default1": true}
				for _, r := range rs {
					delete(refBuckets, r.Bucket)
					if len(r.Vbuckets) != ref {
						t.Fatal("mismatch in restart vbuckets")
					}
				}
				if len(refBuckets) > 0 {
					t.Fatal("mismatch in restart vbuckets")
				}
				wait = false
			} else {
				t.Fatal(fmt.Errorf("expected restart vbuckets"))
			}
		})
	}

	client.Close()
	daemon.Close()
}

func TestStreamLoopback(t *testing.T) {
	var client *StreamClient
	var err error

	maxconns, maxvbuckets, mutChanSize := 8, 32, 100
	log.SetOutput(ioutil.Discard)

	// start server
	msgch := make(chan interface{}, mutChanSize)
	errch := make(chan interface{}, 1000)
	daemon := doServer(addrST, t, msgch, errch, mutChanSize)
	flags := StreamTransportFlag(0).SetProtobuf()
	maxBuckets := 2
	vbmaps := makeVbmaps(maxvbuckets, maxBuckets)

	// start client and test loopback
	if client, err = NewStreamClient(addrST, maxconns, flags); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxBuckets; i++ {
		if err := client.SendVbmap(vbmaps[i]); err != nil {
			t.Fatal(err)
		}
	}
	count := 200
	seqno := 1
	for i := 1; i <= count; i += 2 {
		nVbs, nMuts, nIndexes := maxvbuckets, 5, 5
		vbsRef :=
			constructVbKeyVersions("default0", seqno, nVbs, nMuts, nIndexes)
		vbsRef = append(
			vbsRef,
			constructVbKeyVersions("default1", seqno, nVbs, nMuts, nIndexes)...)
		err := client.SendKeyVersions(vbsRef)
		if err != nil {
			t.Fatal(err)
		}
		seqno += nMuts
		// gather
		pvbs := make([]*protobuf.VbKeyVersions, 0)
		for len(pvbs) < nVbs*2 {
			verify(msgch, errch, func(mutn, err interface{}) {
				if err != nil {
					t.Fatal(err)
				}
				if pvbsSub, ok := mutn.([]*protobuf.VbKeyVersions); !ok {
					err := fmt.Errorf("unexpected type in loopback %T", pvbsSub)
					t.Fatal(err)
				} else {
					pvbs = append(pvbs, pvbsSub...)
				}
			})
		}
		vbs := protobuf2VbKeyVersions(pvbs)
		for _, vbRef := range vbsRef {
			vbRef.Kvs = vbRef.Kvs[:cap(vbRef.Kvs)] // fix KeyVersions
			for _, vb := range vbs {
				if vb.Uuid == vbRef.Uuid {
					if vb.Bucket != vbRef.Bucket {
						t.Fatal(fmt.Errorf("unexpected response"))
					} else if vb.Vbucket != vbRef.Vbucket {
						t.Fatal(fmt.Errorf("unexpected response"))
					} else if vb.Vbuuid != vbRef.Vbuuid {
						t.Fatal(fmt.Errorf("unexpected response"))
					} else if len(vb.Kvs) != nMuts {
						t.Fatal(fmt.Errorf("unexpected response"))
					}
				}
			}
		}
	}

	client.Close()
	daemon.Close()
}

func BenchmarkLoopback(b *testing.B) {
	var client *StreamClient

	maxconns, maxvbuckets, mutChanSize := 8, 32, 100
	log.SetOutput(ioutil.Discard)

	// start server
	msgch := make(chan interface{}, mutChanSize)
	errch := make(chan interface{}, 1000)
	daemon := doServer(addrST, b, msgch, errch, mutChanSize)
	flags := StreamTransportFlag(0).SetProtobuf()
	maxBuckets := 2
	vbmaps := makeVbmaps(maxvbuckets, maxBuckets)

	// start client and test loopback
	client, _ = NewStreamClient(addrST, maxconns, flags)
	for i := 0; i < maxBuckets; i++ {
		client.SendVbmap(vbmaps[i])
	}

	go func() {
		nVbs, nMuts, nIndexes := maxvbuckets, 5, 5
		seqno := 1
		for {
			vbsRef :=
				constructVbKeyVersions("default0", seqno, nVbs, nMuts, nIndexes)
			vbs :=
				constructVbKeyVersions("default1", seqno, nVbs, nMuts, nIndexes)
			vbsRef = append(vbsRef, vbs...)
			client.SendKeyVersions(vbsRef)
			seqno += nMuts
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		verify(msgch, errch, func(mutn, err interface{}) {
			if err != nil {
				fmt.Printf("%T\n", err)
				b.Fatal(err)
			}
			if pvbsSub, ok := mutn.([]*protobuf.VbKeyVersions); !ok {
				err := fmt.Errorf("unexpected type in loopback %T", pvbsSub)
				b.Fatal(err)
			}
		})
	}
	client.Close()
	daemon.Close()
}
