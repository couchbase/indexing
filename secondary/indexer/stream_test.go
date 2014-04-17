package indexer

import (
	"bytes"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	"testing"
)

const stream_test_maxKeyvers = 1

type testConnection struct {
	rptr int
	wptr int
	buf  []byte
}

func newTestConnection() *testConnection {
	return &testConnection{buf: make([]byte, 100000)}
}

func (tc *testConnection) Write(b []byte) (n int, err error) {
	newptr := tc.wptr + len(b)
	copy(tc.buf[tc.wptr:newptr], b)
	tc.wptr = newptr
	return len(b), nil
}

func (tc *testConnection) Read(b []byte) (n int, err error) {
	newptr := tc.rptr + len(b)
	copy(b, tc.buf[tc.rptr:newptr])
	tc.rptr = newptr
	return len(b), nil
}

func (tc *testConnection) reset() {
	tc.wptr, tc.rptr = 0, 0
}

func TestPktKeyVersions(t *testing.T) {
	var payload interface{}
	var err error

	k := common.NewUpsert(512, 0x1234567812345678, []byte("cities"), 10000000)
	k.Keys = [][]byte{[]byte("bangalore"), []byte("delhi"), []byte("jaipur")}
	k.Oldkeys = [][]byte{[]byte("varanasi"), []byte("pune"), []byte("mahe")}
	k.Indexids = []uint32{1, 2, 3}

	ks := make([]*common.KeyVersions, 0, stream_test_maxKeyvers)
	for i := 0; i < stream_test_maxKeyvers; i++ {
		n := *k
		ks = append(ks, &n)
	}

	tc := newTestConnection()
	tc.reset()
	flags := streamTransportFlag(0).setProtobuf()
	pkt := NewStreamTransportPacket(GetMaxStreamDataLen(), flags)
	if err = pkt.Send(tc, ks); err != nil {
		t.Fatal(err)
	}
	if payload, err = pkt.Receive(tc); err != nil {
		t.Fatal(err)
	}

	n_ks, ok := payload.([]*protobuf.KeyVersions)
	if ok == false {
		t.Fatal("fail send/receive")
	}
	if len(n_ks) != stream_test_maxKeyvers {
		t.Fatal("expected exact number of KeyVersions encoded")
	}

	Command := byte(n_ks[0].GetCommand())
	Vbucket := uint16(n_ks[0].GetVbucket())
	Vbuuid := n_ks[0].GetVbuuid()
	Docid := n_ks[0].GetDocid()
	Seqno := n_ks[0].GetSeqno()
	Keys := n_ks[0].GetKeys()
	Oldkeys := n_ks[0].GetOldkeys()
	Indexids := n_ks[0].GetIndexids()

	if ks[0].Vbucket != Vbucket || ks[0].Vbuuid != Vbuuid ||
		bytes.Compare(Docid, ks[0].Docid) != 0 || Seqno != ks[0].Seqno ||
		Command != ks[0].Command {
		t.Fatal("Mistmatch between encode and decode")
	}
	for i, _ := range Keys {
		if bytes.Compare(Keys[i], ks[0].Keys[i]) != 0 {
			t.Fatal("Mismatch in keys")
		}
		if bytes.Compare(Oldkeys[i], ks[0].Oldkeys[i]) != 0 {
			t.Fatal("Mismatch in old-keys")
		}
		if Indexids[i] != ks[0].Indexids[i] {
			t.Fatal("Mismatch in indexids")
		}
	}
}

func TestPktVbmap(t *testing.T) {
	vbuckets := []uint16{1, 2, 3, 4}
	vbuuids := []uint64{10, 20, 30, 40}

	vbmap := common.VbConnectionMap{Vbuckets: vbuckets, Vbuuids: vbuuids}
	tc := newTestConnection()
	tc.reset()
	flags := streamTransportFlag(0).setProtobuf()
	pkt := NewStreamTransportPacket(GetMaxStreamDataLen(), flags)
	pkt.Send(tc, vbmap)
	payload, _ := pkt.Receive(tc)

	vbmap_p, ok := payload.(*protobuf.VbConnectionMap)
	if ok == false {
		t.Fatal("expected reference VbConnectionMap object")
	}

	n_vbuckets := vbmap_p.GetVbuckets()
	n_vbuuids := vbmap_p.GetVbuuids()
	if len(vbuckets) != len(n_vbuckets) {
		t.Fatal("unexpected number of vbuckets")
	}
	for i, _ := range vbuckets {
		if vbuckets[i] != uint16(n_vbuckets[i]) {
			t.Fatal("unexpected vbucket number")
		} else if vbuuids[i] != n_vbuuids[i] {
			t.Fatal("unexpected vbuuid number")
		}
	}
}

func BenchmarkSendKeyVersions(b *testing.B) {
	k := common.NewUpsert(512, 0x1234567812345678, []byte("cities"), 10000000)
	k.Keys = [][]byte{[]byte("bangalore"), []byte("delhi"), []byte("jaipur")}
	k.Oldkeys = [][]byte{[]byte("varanasi"), []byte("pune"), []byte("mahe")}
	k.Indexids = []uint32{1, 2, 3}

	ks := make([]*common.KeyVersions, 0, stream_test_maxKeyvers)
	for i := 0; i < stream_test_maxKeyvers; i++ {
		n := *k
		ks = append(ks, &n)
	}

	tc := newTestConnection()
	tc.reset()
	flags := streamTransportFlag(0).setProtobuf()
	pkt := NewStreamTransportPacket(GetMaxStreamDataLen(), flags)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tc.reset()
		pkt.Send(tc, ks)
	}
}

func BenchmarkReceiveKeyVersions(b *testing.B) {
	k := common.NewUpsert(512, 0x1234567812345678, []byte("cities"), 10000000)
	k.Keys = [][]byte{[]byte("bangalore"), []byte("delhi"), []byte("jaipur")}
	k.Oldkeys = [][]byte{[]byte("varanasi"), []byte("pune"), []byte("mahe")}
	k.Indexids = []uint32{1, 2, 3}

	ks := make([]*common.KeyVersions, 0, stream_test_maxKeyvers)
	for i := 0; i < stream_test_maxKeyvers; i++ {
		n := *k
		ks = append(ks, &n)
	}

	tc := newTestConnection()
	tc.reset()
	flags := streamTransportFlag(0).setProtobuf()
	pkt := NewStreamTransportPacket(GetMaxStreamDataLen(), flags)
	pkt.Send(tc, ks)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tc.reset()
		pkt.Receive(tc)
	}
}

func BenchmarkSendVbmap(b *testing.B) {
	vbuckets := []uint16{1, 2, 3, 4}
	vbuuids := []uint64{10, 20, 30, 40}
	vbmap := common.VbConnectionMap{Vbuckets: vbuckets, Vbuuids: vbuuids}

	tc := newTestConnection()
	tc.reset()
	flags := streamTransportFlag(0).setProtobuf()
	pkt := NewStreamTransportPacket(GetMaxStreamDataLen(), flags)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tc.reset()
		pkt.Send(tc, vbmap)
	}
}

func BenchmarkReceiveVbmap(b *testing.B) {
	vbuckets := []uint16{1, 2, 3, 4}
	vbuuids := []uint64{10, 20, 30, 40}
	vbmap := common.VbConnectionMap{Vbuckets: vbuckets, Vbuuids: vbuuids}

	tc := newTestConnection()
	tc.reset()
	flags := streamTransportFlag(0).setProtobuf()
	pkt := NewStreamTransportPacket(GetMaxStreamDataLen(), flags)
	pkt.Send(tc, vbmap)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tc.reset()
		pkt.Receive(tc)
	}
}
