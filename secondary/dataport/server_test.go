package dataport

import "testing"
import "time"

import c "github.com/couchbase/indexing/secondary/common"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/data"
import "github.com/couchbase/indexing/secondary/transport"

func TestTimeout(t *testing.T) {
	c.LogIgnore()
	//c.SetLogLevel(c.LogLevelDebug)

	addr := "localhost:8888"
	maxBuckets, maxvbuckets, mutChanSize := 2, 4, 100

	// start server
	appch := make(chan interface{}, mutChanSize)
	prefix := "projector.dataport.indexer."
	config := c.SystemConfig.SectionConfig(prefix, true /*trim*/)
	daemon, err := NewServer(addr, maxvbuckets, config, appch)
	if err != nil {
		t.Fatal(err)
	}

	// start client
	flags := transport.TransportFlag(0).SetProtobuf()
	prefix = "projector.dataport.client."
	config = c.SystemConfig.SectionConfig(prefix, true /*trim*/)
	client, _ := NewClient(addr, flags, maxvbuckets, config)

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
	client.SendKeyVersions(vbs, true)

	go func() { // this routine will keep one connection alive
		for i := 0; ; i++ {
			idx := i % len(vbmaps[0].Vbuckets)
			vbno, vbuuid := vbmaps[0].Vbuckets[idx], vbmaps[0].Vbuuids[idx]
			// send sync messages
			vb := c.NewVbKeyVersions("default0", vbno, vbuuid, 1)
			kv := c.NewKeyVersions(10, nil, 1)
			kv.AddSync()
			vb.AddKeyVersions(kv)
			client.SendKeyVersions([]*c.VbKeyVersions{vb}, true)
			<-time.After(6000 * time.Millisecond)
		}
	}()

	wait := true
	for wait {
		select {
		case msg := <-appch:
			switch ce := msg.(type) {
			case []*protobuf.VbKeyVersions:
			case ConnectionError:
				ref := maxvbuckets
				t.Logf("%T %v \n", ce, ref)
				if len(ce) != 2 {
					t.Fatal("mismatch in ConnectionError")
				}
				refBuckets := map[string]bool{"default0": true, "default1": true}
				for bucket, vbnos := range ce {
					delete(refBuckets, bucket)
					if len(vbnos) != ref {
						t.Fatalf("mismatch in ConnectionError %v %v", vbnos, ref)
					}
				}
				if len(refBuckets) > 0 {
					t.Fatalf("mismatch in ConnectionError %v", refBuckets)
				}
				wait = false

			default:
				t.Fatalf("expected connection error %T", msg)
			}
		}
	}

	<-time.After(100 * time.Millisecond)
	client.Close()

	<-time.After(100 * time.Millisecond)
	daemon.Close()
}

func TestLoopback(t *testing.T) {
	c.LogIgnore()
	//c.SetLogLevel(c.LogLevelDebug)

	addr := "localhost:8888"
	maxBuckets, maxvbuckets, mutChanSize := 2, 32, 100

	// start server
	appch := make(chan interface{}, mutChanSize)
	prefix := "projector.dataport.indexer."
	config := c.SystemConfig.SectionConfig(prefix, true /*trim*/)
	daemon, err := NewServer(addr, maxvbuckets, config, appch)
	if err != nil {
		t.Fatal(err)
	}

	// start client and test loopback
	flags := transport.TransportFlag(0).SetProtobuf()
	prefix = "projector.dataport.client."
	config = c.SystemConfig.SectionConfig(prefix, true /*trim*/)
	client, err := NewClient(addr, flags, maxvbuckets, config)
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
	client.SendKeyVersions(vbs, true)

	count, seqno := 200, 1
	for i := 1; i <= count; i += 2 {
		nVbs, nMuts, nIndexes := maxvbuckets, 5, 5
		vbsRef :=
			constructVbKeyVersions("default0", seqno, nVbs, nMuts, nIndexes)
		vbsRef = append(
			vbsRef,
			constructVbKeyVersions("default1", seqno, nVbs, nMuts, nIndexes)...)
		err := client.SendKeyVersions(vbsRef, true)
		if err != nil {
			t.Fatal(err)
		}
		seqno += nMuts

		// gather
		pvbs := make([]*protobuf.VbKeyVersions, 0)
		for len(pvbs) < nVbs*2 {
			select {
			case msg := <-appch:
				t.Logf("%T %v\n", msg, msg)
				if pvbsSub, ok := msg.([]*protobuf.VbKeyVersions); !ok {
					t.Fatalf("unexpected type in loopback %T", msg)
				} else {
					pvbs = append(pvbs, pvbsSub...)
				}
			}
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
	maxBuckets, maxvbuckets, mutChanSize := 2, 32, 100

	// start server
	appch := make(chan interface{}, mutChanSize)
	prefix := "projector.dataport.indexer."
	config := c.SystemConfig.SectionConfig(prefix, true /*trim*/)
	daemon, err := NewServer(addr, maxvbuckets, config, appch)
	if err != nil {
		b.Fatal(err)
	}

	// start client and test loopback
	flags := transport.TransportFlag(0).SetProtobuf()
	prefix = "projector.dataport.client."
	config = c.SystemConfig.SectionConfig(prefix, true /*trim*/)
	client, _ := NewClient(addr, flags, maxvbuckets, config)

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
	client.SendKeyVersions(vbs, true)

	go func() {
		nVbs, nMuts, nIndexes := maxvbuckets, 5, 5
		seqno := 1
		for {
			vbsRef :=
				constructVbKeyVersions("default0", seqno, nVbs, nMuts, nIndexes)
			vbs :=
				constructVbKeyVersions("default1", seqno, nVbs, nMuts, nIndexes)
			vbsRef = append(vbsRef, vbs...)
			client.SendKeyVersions(vbsRef, true)
			seqno += nMuts
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		select {
		case msg := <-appch:
			if pvbsSub, ok := msg.([]*protobuf.VbKeyVersions); !ok {
				b.Fatalf("unexpected type in loopback %T", pvbsSub)
			}
		}
	}

	client.Close()
	daemon.Close()
}
