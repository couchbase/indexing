package mutation

import (
	"bytes"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	"testing"
)

const conf_maxKeyvers = 1

func TestNewUpsert(t *testing.T) {
	m := common.NewMutation(conf_maxKeyvers)
	k := NewUpsert(512, 0x1234567812345678, []byte("cities"), 10000000)
	k.Keys = [][]byte{[]byte("bangalore"), []byte("delhi"), []byte("jaipur")}
	k.Oldkeys = [][]byte{[]byte("varanasi"), []byte("pune"), []byte("mahe")}
	k.Indexids = []uint32{1, 2, 3}

	ks := make([]*common.KeyVersions, 0, conf_maxKeyvers)
	for i := 0; i < conf_maxKeyvers; i++ {
		n := *k
		ks = append(ks, &n)
	}
	testKeyVersions(t, m, ks)

	// Test VbConnectionMap
	vbuckets := []uint16{1, 2, 3, 4}
	vbuuids := []uint64{10, 20, 30, 40}
	testVbConnectionMap(t, m, vbuckets, vbuuids)
}

func TestNewUpsertDeletion(t *testing.T) {
	m := common.NewMutation(conf_maxKeyvers)
	k := NewUpsertDeletion(512, 0x1234567812345678, []byte("cities"), 10000000)
	k.Indexids = []uint32{1, 2, 3}

	ks := make([]*common.KeyVersions, 0, conf_maxKeyvers)
	for i := 0; i < conf_maxKeyvers; i++ {
		n := *k
		ks = append(ks, &n)
	}
	testKeyVersions(t, m, ks)
}

func TestNewSync(t *testing.T) {
	m := common.NewMutation(conf_maxKeyvers)
	k := NewSync(512, 0x1234567812345678, 10000000)
	k.Indexids = []uint32{1, 2, 3}

	ks := make([]*common.KeyVersions, 0, conf_maxKeyvers)
	for i := 0; i < conf_maxKeyvers; i++ {
		n := *k
		ks = append(ks, &n)
	}
	testKeyVersions(t, m, ks)
}

func TestNewStreamBegin(t *testing.T) {
	m := common.NewMutation(conf_maxKeyvers)
	k := NewStreamBegin(512, 0x1234567812345678, 1000000)
	k.Indexids = []uint32{1, 2, 3}

	ks := make([]*common.KeyVersions, 0, conf_maxKeyvers)
	for i := 0; i < conf_maxKeyvers; i++ {
		n := *k
		ks = append(ks, &n)
	}
	testKeyVersions(t, m, ks)
}

func TestNewStreamEnd(t *testing.T) {
	m := common.NewMutation(conf_maxKeyvers)
	k := NewStreamEnd(512, 0x1234567812345678, 1000000)
	k.Indexids = []uint32{1, 2, 3}

	ks := make([]*common.KeyVersions, 0, conf_maxKeyvers)
	for i := 0; i < conf_maxKeyvers; i++ {
		n := *k
		ks = append(ks, &n)
	}
	testKeyVersions(t, m, ks)
}

func BenchmarkNewUpsertEncode(b *testing.B) {
	benchmarkMutationEncode(b, func() *common.KeyVersions {
		k := NewUpsert(512, 0x1234567812345678, []byte("cities"), 10000000)
		k.Keys = [][]byte{[]byte("bangalore"), []byte("delhi"), []byte("jaipur")}
		k.Oldkeys = [][]byte{[]byte("varanasi"), []byte("pune"), []byte("mahe")}
		k.Indexids = []uint32{1, 2, 3}
		return k
	})
}
func BenchmarkNewUpsertDecode(b *testing.B) {
	benchmarkMutationDecode(b, func() *common.KeyVersions {
		k := NewUpsert(512, 0x1234567812345678, []byte("cities"), 10000000)
		k.Keys = [][]byte{[]byte("bangalore"), []byte("delhi"), []byte("jaipur")}
		k.Oldkeys = [][]byte{[]byte("varanasi"), []byte("pune"), []byte("mahe")}
		k.Indexids = []uint32{1, 2, 3}
		return k
	})
}

func BenchmarkNewDeletionEncode(b *testing.B) {
	benchmarkMutationEncode(b, func() *common.KeyVersions {
		k := NewDeletion(512, 0x1234567812345678, []byte("cities"), 10000000)
		k.Indexids = []uint32{1, 2, 3}
		return k
	})
}

func BenchmarkNewDeletionDecode(b *testing.B) {
	benchmarkMutationDecode(b, func() *common.KeyVersions {
		k := NewDeletion(512, 0x1234567812345678, []byte("cities"), 10000000)
		k.Indexids = []uint32{1, 2, 3}
		return k
	})
}

func BenchmarkNewUpsertDeletionEncode(b *testing.B) {
	benchmarkMutationEncode(b, func() *common.KeyVersions {
		k := NewUpsertDeletion(512, 0x1234567812345678, []byte("cities"), 10000000)
		k.Indexids = []uint32{1, 2, 3}
		return k
	})
}

func BenchmarkNewUpsertDeletionDecode(b *testing.B) {
	benchmarkMutationDecode(b, func() *common.KeyVersions {
		k := NewUpsertDeletion(512, 0x1234567812345678, []byte("cities"), 10000000)
		k.Indexids = []uint32{1, 2, 3}
		return k
	})
}

func BenchmarkNewSyncEncode(b *testing.B) {
	benchmarkMutationEncode(b, func() *common.KeyVersions {
		k := NewSync(512, 0x1234567812345678, 10000000)
		k.Indexids = []uint32{1, 2, 3}
		return k
	})
}

func BenchmarkNewSyncDecode(b *testing.B) {
	benchmarkMutationDecode(b, func() *common.KeyVersions {
		k := NewSync(512, 0x1234567812345678, 10000000)
		k.Indexids = []uint32{1, 2, 3}
		return k
	})
}

func BenchmarkNewStreamBeginEncode(b *testing.B) {
	benchmarkMutationEncode(b, func() *common.KeyVersions {
		k := NewStreamBegin(512, 0x1234567812345678, 1000000)
		k.Indexids = []uint32{1, 2, 3}
		return k
	})
}

func BenchmarkNewStreamBeginDecode(b *testing.B) {
	benchmarkMutationDecode(b, func() *common.KeyVersions {
		k := NewStreamBegin(512, 0x1234567812345678, 1000000)
		k.Indexids = []uint32{1, 2, 3}
		return k
	})
}

func BenchmarkNewStreamEndEncode(b *testing.B) {
	benchmarkMutationEncode(b, func() *common.KeyVersions {
		k := NewStreamEnd(512, 0x1234567812345678, 1000000)
		k.Indexids = []uint32{1, 2, 3}
		return k
	})
}

func BenchmarkNewStreamEndDecode(b *testing.B) {
	benchmarkMutationDecode(b, func() *common.KeyVersions {
		k := NewStreamEnd(512, 0x1234567812345678, 1000000)
		k.Indexids = []uint32{1, 2, 3}
		return k
	})
}

func BenchmarkVbmapEncode(b *testing.B) {
	m := common.NewMutation(conf_maxKeyvers)
	m.NewPayload(common.PAYLOAD_VBMAP)

	vbuckets := make([]uint16, 1024)
	vbuuids := make([]uint64, 1024)
	for i := 0; i < 1024; i++ {
		vbuckets[i] = uint16(i)
		vbuuids[i] = uint64(1000000 + i)
	}
	m.SetVbuckets(vbuckets, vbuuids)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Encode()
	}
}

func BenchmarkVbmapDecode(b *testing.B) {
	m := common.NewMutation(conf_maxKeyvers)
	m.NewPayload(common.PAYLOAD_VBMAP)

	vbuckets := make([]uint16, 1024)
	vbuuids := make([]uint64, 1024)
	for i := 0; i < 1024; i++ {
		vbuckets[i] = uint16(i)
		vbuuids[i] = uint64(1000000 + i)
	}
	m.SetVbuckets(vbuckets, vbuuids)
	data, _ := m.Encode()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Decode(data)
	}
}

func benchmarkMutationEncode(b *testing.B, fn func() *common.KeyVersions) {
	m := common.NewMutation(conf_maxKeyvers)
	m.NewPayload(common.PAYLOAD_KEYVERSIONS)
	k := fn()
	m.AddKeyVersions(k)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Encode()
	}
}

func benchmarkMutationDecode(b *testing.B, fn func() *common.KeyVersions) {
	m := common.NewMutation(conf_maxKeyvers)
	m.NewPayload(common.PAYLOAD_KEYVERSIONS)
	k := fn()
	m.AddKeyVersions(k)
	data, _ := m.Encode()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Decode(data)
	}
}

func testKeyVersions(t *testing.T, m *common.Mutation, ks []*common.KeyVersions) {
	var data []byte
	var err error

	m.NewPayload(common.PAYLOAD_KEYVERSIONS)

	if m.SetVbuckets(nil, nil) == nil {
		t.Fatal("expected an error")
	}

	for _, k := range ks {
		n := *k
		if err = m.AddKeyVersions(&n); err != nil {
			t.Fatal(err)
		}
	}
	if m.AddKeyVersions(ks[0]) == nil {
		t.Fatal("expected an error")
	}

	if data, err = m.Encode(); err != nil {
		t.Fatal(err)
	}

	var obj interface{}

	newm := &common.Mutation{}
	if obj, err = newm.Decode(data); err != nil {
		t.Fatal(err)
	}
	n_ks, ok := obj.([]*protobuf.KeyVersions)
	if ok == false {
		t.Fatal("expected slice of reference to KeyVersions object")
	}
	if len(n_ks) != conf_maxKeyvers {
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

func testVbConnectionMap(t *testing.T, m *common.Mutation, vbuckets []uint16, vbuuids []uint64) {
	var data []byte
	var err error

	m.NewPayload(common.PAYLOAD_VBMAP)

	if m.AddKeyVersions(nil) == nil {
		t.Fatal("expected an error")
	}

	if err = m.SetVbuckets(vbuckets, vbuuids); err != nil {
		t.Fatal(err)
	}

	if data, err = m.Encode(); err != nil {
		t.Fatal(err)
	}

	var obj interface{}

	newm := &common.Mutation{}
	if obj, err = newm.Decode(data); err != nil {
		t.Fatal(err)
	}
	vb, ok := obj.(*protobuf.VbConnectionMap)
	if ok == false {
		t.Fatal("expected reference VbConnectionMap object")
	}

	n_vbuckets := vb.GetVbuckets()
	n_vbuuids := vb.GetVbuuids()
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
