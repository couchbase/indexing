package dataport

import (
	"testing"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
)

// TODO:
// - test live StreamBegin and StreamEnd.

func TestTimeout(t *testing.T) {
	c.LogIgnore()

	addr := "localhost:8888"
	maxBuckets, maxconns, maxvbuckets, mutChanSize := 2, 2, 4, 100

	// start server
	msgch := make(chan []*protobuf.VbKeyVersions, mutChanSize)
	errch := make(chan interface{}, mutChanSize)
	daemon, err := NewServer(addr, msgch, errch)
	if err != nil {
		t.Fatal(err)
	}

	// start client
	flags := TransportFlag(0).SetProtobuf()
	client, _ := NewClient(addr, maxconns, flags)

	vbmaps := makeVbmaps(maxvbuckets, maxBuckets) // vbmaps

	// send StreamBegin
	vbs := make([]*c.VbKeyVersions, 0, maxvbuckets)
	for _, vbmap := range vbmaps {
		for i := 0; i < len(vbmap.Vbuckets); i++ { // for N vbuckets
			vbno, vbuuid := vbmap.Vbuckets[i], vbmap.Vbuuids[i]
			vb := c.NewVbKeyVersions(vbmap.Bucket, vbno, vbuuid, 1)
			kv := c.NewKeyVersions(uint64(0), []byte("Bourne"), 1)
			kv.AddStreamBegin()
			vb.AddKeyVersions(kv)
			vbs = append(vbs, vb)
		}
	}
	client.SendKeyVersions(vbs)

	go func() { // this routine will keep one connection alive
		for i := 0; ; i++ {
			idx := i % len(vbmaps[0].Vbuckets)
			vbno, vbuuid := vbmaps[0].Vbuckets[idx], vbmaps[0].Vbuuids[idx]
			// send sync messages
			vb := c.NewVbKeyVersions("default0", vbno, vbuuid, 1)
			kv := c.NewKeyVersions(10, nil, 1)
			kv.AddSync()
			vb.AddKeyVersions(kv)
			client.SendKeyVersions([]*c.VbKeyVersions{vb})
			<-time.After(c.DataportReadDeadline * time.Millisecond)
		}
	}()

	wait := true
	for wait {
		verify(msgch, errch, func(mutn, err interface{}) {
			if err == nil {
				return
			}
			ref := maxvbuckets / maxconns
			if rs, ok := (err).(RepairVbuckets); ok { // check
				t.Logf("%T %v \n", rs, rs)
				if len(rs) != 2 {
					t.Fatal("mismatch in repair vbuckets")
				}
				refBuckets := map[string]bool{"default0": true, "default1": true}
				for bucket, vbnos := range rs {
					delete(refBuckets, bucket)
					if len(vbnos) != ref {
						t.Fatalf("mismatch in repair vbuckets %v %v", vbnos, ref)
					}
				}
				if len(refBuckets) > 0 {
					t.Fatalf("mismatch in repair vbuckets %v", refBuckets)
				}
				wait = false
			} else {
				t.Fatalf("expected repair vbuckets %T", err)
			}
		})
	}

	<-time.After(100 * time.Millisecond)
	client.Close()

	<-time.After(100 * time.Millisecond)
	daemon.Close()
}

func TestLoopback(t *testing.T) {
	c.LogIgnore()

	addr := "localhost:8888"
	maxBuckets, maxconns, maxvbuckets, mutChanSize := 2, 8, 32, 100

	// start server
	msgch := make(chan []*protobuf.VbKeyVersions, mutChanSize)
	errch := make(chan interface{}, 1000)
	daemon, err := NewServer(addr, msgch, errch)
	if err != nil {
		t.Fatal(err)
	}

	// start client and test loopback
	flags := TransportFlag(0).SetProtobuf()
	client, err := NewClient(addr, maxconns, flags)
	if err != nil {
		t.Fatal(err)
	}

	vbmaps := makeVbmaps(maxvbuckets, maxBuckets) // vbmaps

	// send StreamBegin
	vbs := make([]*c.VbKeyVersions, 0, maxvbuckets)
	for _, vbmap := range vbmaps {
		for i := 0; i < len(vbmap.Vbuckets); i++ { // for N vbuckets
			vbno, vbuuid := vbmap.Vbuckets[i], vbmap.Vbuuids[i]
			vb := c.NewVbKeyVersions(vbmap.Bucket, vbno, vbuuid, 1)
			kv := c.NewKeyVersions(uint64(0), []byte("Bourne"), 1)
			kv.AddStreamBegin()
			vb.AddKeyVersions(kv)
			vbs = append(vbs, vb)
		}
	}
	client.SendKeyVersions(vbs)

	count, seqno := 200, 1
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
				t.Logf("%T %v\n", err, err)
				if err != nil {
					t.Fatal(err)
				}
				if pvbsSub, ok := mutn.([]*protobuf.VbKeyVersions); !ok {
					t.Fatalf("unexpected type in loopback %T", pvbsSub)
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
						t.Fatal("unexpected response")
					} else if vb.Vbucket != vbRef.Vbucket {
						t.Fatal("unexpected response")
					} else if vb.Vbuuid != vbRef.Vbuuid {
						t.Fatal("unexpected response")
					} else if len(vb.Kvs) != nMuts {
						t.Fatal("unexpected response")
					}
				}
			}
		}
	}

	client.Close()
	daemon.Close()
}

func BenchmarkLoopback(b *testing.B) {
	c.LogIgnore()

	addr := "localhost:8888"
	maxBuckets, maxconns, maxvbuckets, mutChanSize := 2, 8, 32, 100

	// start server
	msgch := make(chan []*protobuf.VbKeyVersions, mutChanSize)
	errch := make(chan interface{}, 1000)
	daemon, err := NewServer(addr, msgch, errch)
	if err != nil {
		b.Fatal(err)
	}

	// start client and test loopback
	flags := TransportFlag(0).SetProtobuf()
	client, _ := NewClient(addr, maxconns, flags)

	vbmaps := makeVbmaps(maxvbuckets, maxBuckets)

	// send StreamBegin
	vbs := make([]*c.VbKeyVersions, 0, maxvbuckets)
	for _, vbmap := range vbmaps {
		for i := 0; i < len(vbmap.Vbuckets); i++ { // for N vbuckets
			vbno, vbuuid := vbmap.Vbuckets[i], vbmap.Vbuuids[i]
			vb := c.NewVbKeyVersions(vbmap.Bucket, vbno, vbuuid, 1)
			kv := c.NewKeyVersions(uint64(0), []byte("Bourne"), 1)
			kv.AddStreamBegin()
			vb.AddKeyVersions(kv)
			vbs = append(vbs, vb)
		}
	}
	client.SendKeyVersions(vbs)

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
				b.Fatal(err)
			}
			if pvbsSub, ok := mutn.([]*protobuf.VbKeyVersions); !ok {
				b.Fatalf("unexpected type in loopback %T", pvbsSub)
			}
		})
	}

	client.Close()
	daemon.Close()
}
