// +build ignore

package dataport

import "fmt"
import "testing"

import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/transport"

var addr = "localhost:8888"

func TestClient(t *testing.T) {
	maxBuckets, maxvbuckets, mutChanSize := 2, 8, 1000
	c.LogIgnore()

	// start server
	appch := make(chan interface{}, mutChanSize)
	prefix := "indexer.dataport."
	config := c.SystemConfig.SectionConfig(prefix, true /*trim*/)
	daemon, err := NewServer(addr, maxvbuckets, config, appch)
	if err != nil {
		t.Fatal(err)
	}

	// start client and test number of connection.
	flags := transport.TransportFlag(0).SetProtobuf()
	prefix = "projector.dataport.client."
	config = c.SystemConfig.SectionConfig(prefix, true /*trim*/)
	maxconns := config["parConnections"].Int()
	client, err := NewClient(
		"cluster", "backfill", addr, flags, maxvbuckets, config)
	if err != nil {
		t.Fatal(err)
	} else if len(client.conns) != maxconns {
		t.Fatal("failed dataport client connections")
	} else if len(client.conns) != len(client.connChans) {
		t.Fatal("failed dataport client connection channels")
	} else if len(client.conns) != len(client.conn2Vbs) {
		t.Fatal("failed dataport client connection channels")
	} else {
		vbmaps := makeVbmaps(maxvbuckets, maxBuckets) // vbmaps
		for i := 0; i < maxBuckets; i++ {
			if err := client.SendVbmap(vbmaps[i]); err != nil {
				t.Fatal(err)
			}
		}
		validateClientInstance(client, maxvbuckets, maxconns, maxBuckets, t)
	}
	client.Close()
	daemon.Close()
}

func TestStreamBegin(t *testing.T) {
	maxBuckets, maxvbuckets, mutChanSize := 2, 8, 1000
	c.LogIgnore()

	// start server
	appch := make(chan interface{}, mutChanSize)
	prefix := "indexer.dataport."
	config := c.SystemConfig.SectionConfig(prefix, true /*trim*/)
	daemon, err := NewServer(addr, maxvbuckets, config, appch)
	if err != nil {
		t.Fatal(err)
	}

	// start client
	flags := transport.TransportFlag(0).SetProtobuf()
	prefix = "projector.dataport.client."
	config = c.SystemConfig.SectionConfig(prefix, true /*trim*/)
	client, _ := NewClient(
		"cluster", "backfill", addr, flags, maxvbuckets, config)
	vbmaps := makeVbmaps(maxvbuckets, maxBuckets) // vbmaps
	for i := 0; i < maxBuckets; i++ {
		if err := client.SendVbmap(vbmaps[i]); err != nil {
			t.Fatal(err)
		}
	}

	// test a live StreamBegin
	bucket, vbno, vbuuid := "default0", uint16(maxvbuckets), uint64(1111)
	uuid := c.StreamID(bucket, vbno)
	vals, err := client.Getcontext()
	if err != nil {
		t.Fatal(err)
	}
	vbChans := vals[0].(map[string]chan interface{})
	if _, ok := vbChans[uuid]; ok {
		t.Fatal("duplicate id")
	}
	vb := c.NewVbKeyVersions(bucket, vbno, vbuuid, 1)
	seqno, docid, maxCount := uint64(10), []byte("document-name"), 10
	kv := c.NewKeyVersions(seqno, docid, maxCount)
	kv.AddStreamBegin()
	vb.AddKeyVersions(kv)
	err = client.SendKeyVersions([]*c.VbKeyVersions{vb}, true)
	client.Getcontext() // syncup
	if err != nil {
		t.Fatal(err)
	} else if _, ok := vbChans[uuid]; !ok {
		fmt.Printf("%v %v\n", len(vbChans), uuid)
		t.Fatal("failed StreamBegin")
	}
	client.Close()
	daemon.Close()
}

func TestStreamEnd(t *testing.T) {
	maxBuckets, maxvbuckets, mutChanSize := 2, 8, 100
	c.LogIgnore()

	// start server
	appch := make(chan interface{}, mutChanSize)
	prefix := "indexer.dataport."
	config := c.SystemConfig.SectionConfig(prefix, true /*trim*/)
	daemon, err := NewServer(addr, maxvbuckets, config, appch)
	if err != nil {
		t.Fatal(err)
	}

	// start client
	flags := transport.TransportFlag(0).SetProtobuf()
	prefix = "projector.dataport.client."
	config = c.SystemConfig.SectionConfig(prefix, true /*trim*/)
	client, _ := NewClient(
		"cluster", "backfill", addr, flags, maxvbuckets, config)
	vbmaps := makeVbmaps(maxvbuckets, maxBuckets) // vbmaps
	for i := 0; i < maxBuckets; i++ {
		if err := client.SendVbmap(vbmaps[i]); err != nil {
			t.Fatal(err)
		}
	}
	// test a live StreamEnd
	bucket, vbno := "default0", vbmaps[0].Vbuckets[0]
	vbuuid := uint64(vbmaps[0].Vbuuids[0])
	uuid := c.StreamID(bucket, vbno)
	vals, _ := client.Getcontext()
	vbChans := vals[0].(map[string]chan interface{})
	if _, ok := vbChans[uuid]; !ok {
		t.Fatal("expected uuid")
	}
	vb := c.NewVbKeyVersions(bucket, vbno, vbuuid, 1)
	seqno, docid, maxCount := uint64(10), []byte("document-name"), 10
	kv := c.NewKeyVersions(seqno, docid, maxCount)
	kv.AddStreamEnd()
	vb.AddKeyVersions(kv)
	err = client.SendKeyVersions([]*c.VbKeyVersions{vb}, true)
	client.Getcontext() // syncup
	if err != nil {
		t.Fatal(err)
	} else if _, ok := vbChans[uuid]; ok {
		t.Fatal("failed StreamEnd")
	}
	client.Close()
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

func validateClientInstance(
	client *Client, maxvbuckets, maxconns, maxBuckets int, t *testing.T) {

	ref := ((maxvbuckets / maxconns) * maxBuckets)
	vals, _ := client.Getcontext()
	vbChans := vals[0].(map[string]chan interface{})
	// validate vbucket channels
	refChans := make(map[chan interface{}][]string)
	for uuid, ch := range vbChans {
		if ids, ok := refChans[ch]; !ok {
			refChans[ch] = make([]string, 0)
		} else {
			refChans[ch] = append(ids, uuid)
		}
	}
	// validate connection to vbuckets.
	conn2Vbs := vals[1].(map[int][]string)
	for _, ids := range conn2Vbs {
		if len(ids) != ref {
			t.Fatal("failed dataport client, vbucket mapping")
		}
	}
}
