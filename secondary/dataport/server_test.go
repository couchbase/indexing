package dataport

import (
	"fmt"
	"testing"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"

	protobuf "github.com/couchbase/indexing/secondary/protobuf/data"
)

func TestTimeout(t *testing.T) {
	logging.SetLogLevel(logging.Silent)

	raddr := "localhost:8888"
	maxBuckets, maxvbuckets, mutChanSize := 2, 4, 100

	// start server
	appch := make(chan interface{}, mutChanSize)
	prefix := "indexer.dataport."
	dconfig := c.SystemConfig.SectionConfig(prefix, true /*trim*/)
	daemon, err := NewServer(raddr, dconfig, appch, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// start endpoint
	config := c.SystemConfig.SectionConfig("projector.dataport.", true /*trim*/)
	endp, err := NewRouterEndpoint("clust", "topic", raddr, maxvbuckets, config)
	if err != nil {
		t.Fatal(err)
	}

	vbmaps := makeVbmaps(maxvbuckets, maxBuckets) // vbmaps

	// send StreamBegin
	for _, vbmap := range vbmaps {
		for i := 0; i < len(vbmap.Vbuckets); i++ { // for N vbuckets
			vbno, vbuuid := vbmap.Vbuckets[i], vbmap.Vbuuids[i]
			kv := c.NewKeyVersions(uint64(0), []byte("Bourne"), 1)
			kv.AddStreamBegin()
			dkv := &c.DataportKeyVersions{
				Bucket: vbmap.Bucket, Vbno: vbno, Vbuuid: vbuuid, Kv: kv,
			}
			if err := endp.Send(dkv); err != nil {
				t.Fatal(err)
			}
		}
	}

	go func() { // this routine will keep one connection alive
		for i := 0; ; i++ {
			vbmap := vbmaps[0] // keep sending sync for first vbucket alone
			idx := i % len(vbmap.Vbuckets)
			vbno, vbuuid := vbmap.Vbuckets[idx], vbmap.Vbuuids[idx]
			// send sync messages
			kv := c.NewKeyVersions(10, nil, 1)
			kv.AddSync()
			dkv := &c.DataportKeyVersions{
				Bucket: vbmap.Bucket, Vbno: vbno, Vbuuid: vbuuid, Kv: kv,
			}
			if endp.Send(dkv); err != nil {
				t.Fatal(err)
			}
			<-time.After(
				time.Duration(dconfig["tcpReadDeadline"].Int()) * time.Millisecond)
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
				t.Logf("%T %v \n", ce, ce)
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
	endp.Close()

	<-time.After(100 * time.Millisecond)
	daemon.Close()
}

func TestLoopback(t *testing.T) {
	logging.SetLogLevel(logging.Silent)

	raddr := "localhost:8888"
	maxBuckets, maxvbuckets, mutChanSize := 2, 32, 100

	// start server
	appch := make(chan interface{}, mutChanSize)
	prefix := "indexer.dataport."
	config := c.SystemConfig.SectionConfig(prefix, true /*trim*/)
	daemon, err := NewServer(raddr, config, appch, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// start endpoint
	config = c.SystemConfig.SectionConfig("projector.dataport.", true /*trim*/)
	endp, err := NewRouterEndpoint("clust", "topic", raddr, maxvbuckets, config)
	if err != nil {
		t.Fatal(err)
	}

	vbmaps := makeVbmaps(maxvbuckets, maxBuckets) // vbmaps

	// send StreamBegin
	for _, vbmap := range vbmaps {
		for i := 0; i < len(vbmap.Vbuckets); i++ { // for N vbuckets
			vbno, vbuuid := vbmap.Vbuckets[i], vbmap.Vbuuids[i]
			kv := c.NewKeyVersions(uint64(0), []byte("Bourne"), 1)
			kv.AddStreamBegin()
			dkv := &c.DataportKeyVersions{
				Bucket: vbmap.Bucket, Vbno: vbno, Vbuuid: vbuuid, Kv: kv,
			}
			if err := endp.Send(dkv); err != nil {
				t.Fatal(err)
			}
		}
	}

	count, seqno := 200, 1
	for i := 1; i <= count; i += 2 {
		nVbs, nMuts, nIndexes := maxvbuckets, 5, 5
		dkvs := dataKeyVersions("default0", seqno, nVbs, nMuts, nIndexes)
		dkvs = append(dkvs, dataKeyVersions("default1", seqno, nVbs, nMuts, nIndexes)...)
		for _, dkv := range dkvs {
			if err := endp.Send(dkv); err != nil {
				t.Fatal(err)
			}
		}
		seqno += nMuts

		// gather
		pvbs := make([]*protobuf.VbKeyVersions, 0)
	loop:
		for {
			select {
			case msg := <-appch:
				//t.Logf("%T %v\n", msg, msg)
				if pvbsSub, ok := msg.([]*protobuf.VbKeyVersions); !ok {
					t.Fatalf("unexpected type in loopback %T", msg)
				} else {
					pvbs = append(pvbs, pvbsSub...)
				}
			case <-time.After(10 * time.Millisecond):
				break loop
			}
		}
		commands := make(map[byte]int)
		for _, vb := range protobuf2VbKeyVersions(pvbs) {
			for _, kv := range vb.Kvs {
				for _, cmd := range kv.Commands {
					if _, ok := commands[byte(cmd)]; !ok {
						commands[byte(cmd)] = 0
					}
					commands[byte(cmd)]++
				}
			}
		}
		if StreamBegins, ok := commands[c.StreamBegin]; ok && StreamBegins != 64 {
			t.Fatalf("unexpected response %v", StreamBegins)
		}
		if commands[c.Upsert] != 1600 {
			t.Fatalf("unexpected response %v", commands[c.Upsert])
		}
	}

	endp.Close()
	daemon.Close()
}

func BenchmarkLoopback(b *testing.B) {
	logging.SetLogLevel(logging.Silent)

	raddr := "localhost:8888"
	maxBuckets, maxvbuckets, mutChanSize := 2, 32, 100

	// start server
	appch := make(chan interface{}, mutChanSize)
	prefix := "indexer.dataport."
	config := c.SystemConfig.SectionConfig(prefix, true /*trim*/)
	daemon, err := NewServer(raddr, config, appch, nil, nil)
	if err != nil {
		b.Fatal(err)
	}

	// start endpoint
	config = c.SystemConfig.SectionConfig("projector.dataport.", true /*trim*/)
	endp, err := NewRouterEndpoint("clust", "topic", raddr, maxvbuckets, config)
	if err != nil {
		b.Fatal(err)
	}

	vbmaps := makeVbmaps(maxvbuckets, maxBuckets)

	// send StreamBegin
	for _, vbmap := range vbmaps {
		for i := 0; i < len(vbmap.Vbuckets); i++ { // for N vbuckets
			vbno, vbuuid := vbmap.Vbuckets[i], vbmap.Vbuuids[i]
			kv := c.NewKeyVersions(uint64(0), []byte("Bourne"), 1)
			kv.AddStreamBegin()
			dkv := &c.DataportKeyVersions{
				Bucket: vbmap.Bucket, Vbno: vbno, Vbuuid: vbuuid, Kv: kv,
			}
			if err := endp.Send(dkv); err != nil {
				b.Fatal(err)
			}
		}
	}

	go func() {
		nVbs, nMuts, nIndexes, seqno := maxvbuckets, 5, 5, 1
		for {
			dkvs := dataKeyVersions("default0", seqno, nVbs, nMuts, nIndexes)
			dkvs = append(dkvs, dataKeyVersions("default1", seqno, nVbs, nMuts, nIndexes)...)
			for _, dkv := range dkvs {
				endp.Send(dkv)
			}
			seqno += nMuts
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		select {
		case msg := <-appch:
			if _, ok := msg.([]*protobuf.VbKeyVersions); !ok {
				b.Fatalf("unexpected type in loopback %T", msg)
			}
		}
	}

	endp.Close()
	daemon.Close()
}

func makeVbmaps(maxvbuckets int, maxBuckets int) []*c.VbConnectionMap {
	vbmaps := make([]*c.VbConnectionMap, 0, maxBuckets)
	for i := 0; i < maxBuckets; i++ {
		vbmap := &c.VbConnectionMap{
			Bucket:   fmt.Sprintf("default%v", i),
			Vbuckets: make([]uint16, 0, maxvbuckets),
			Vbuuids:  make([]uint64, 0, maxvbuckets),
		}
		for i := 0; i < maxvbuckets; i++ {
			vbmap.Vbuckets = append(vbmap.Vbuckets, uint16(i))
			vbmap.Vbuuids = append(vbmap.Vbuuids, uint64(i*10))
		}
		vbmaps = append(vbmaps, vbmap)
	}
	return vbmaps
}

func dataKeyVersions(bucket string, seqno, nVbs, nMuts, nIndexes int) []*c.DataportKeyVersions {
	dkvs := make([]*c.DataportKeyVersions, 0)
	for i := 0; i < nVbs; i++ { // for N vbuckets
		vbno, vbuuid := uint16(i), uint64(i*10)
		for j := 0; j < nMuts; j++ {
			kv := c.NewKeyVersions(uint64(seqno+j), []byte("Bourne"), nIndexes)
			for k := 0; k < nIndexes; k++ {
				key := fmt.Sprintf("bangalore%v", k)
				oldkey := fmt.Sprintf("varanasi%v", k)
				kv.AddUpsert(uint64(k), []byte(key), []byte(oldkey))
			}
			dkv := &c.DataportKeyVersions{
				Bucket: bucket, Vbno: vbno, Vbuuid: vbuuid, Kv: kv,
			}
			dkvs = append(dkvs, dkv)
		}
	}
	return dkvs
}
