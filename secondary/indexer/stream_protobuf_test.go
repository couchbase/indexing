package indexer

import (
	"bytes"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	"testing"
)

const conf_maxKeyvers = 1

func TestNewUpsert(t *testing.T) {
	k := common.NewUpsert(512, 0x1234567812345678, []byte("cities"), 10000000)
	k.Keys = [][]byte{[]byte("bangalore"), []byte("delhi"), []byte("jaipur")}
	k.Oldkeys = [][]byte{[]byte("varanasi"), []byte("pune"), []byte("mahe")}
	k.Indexids = []uint32{1, 2, 3}

	ks := make([]*common.KeyVersions, 0, conf_maxKeyvers)
	for i := 0; i < conf_maxKeyvers; i++ {
		n := *k
		ks = append(ks, &n)
	}
	testKeyVersions(t, ks)

	// Test VbConnectionMap
	vbuckets := []uint16{1, 2, 3, 4}
	vbuuids := []uint64{10, 20, 30, 40}
	testVbConnectionMap(t, vbuckets, vbuuids)
}

func TestNewUpsertDeletion(t *testing.T) {
	k := common.NewUpsertDeletion(512, 0x1234567812345678, []byte("cities"), 10000000)
	k.Indexids = []uint32{1, 2, 3}

	ks := make([]*common.KeyVersions, 0, conf_maxKeyvers)
	for i := 0; i < conf_maxKeyvers; i++ {
		n := *k
		ks = append(ks, &n)
	}
	testKeyVersions(t, ks)
}

func TestNewSync(t *testing.T) {
	k := common.NewSync(512, 0x1234567812345678, 10000000)
	k.Indexids = []uint32{1, 2, 3}

	ks := make([]*common.KeyVersions, 0, conf_maxKeyvers)
	for i := 0; i < conf_maxKeyvers; i++ {
		n := *k
		ks = append(ks, &n)
	}
	testKeyVersions(t, ks)
}

func TestNewStreamBegin(t *testing.T) {
	k := common.NewStreamBegin(512, 0x1234567812345678, 1000000)
	k.Indexids = []uint32{1, 2, 3}

	ks := make([]*common.KeyVersions, 0, conf_maxKeyvers)
	for i := 0; i < conf_maxKeyvers; i++ {
		n := *k
		ks = append(ks, &n)
	}
	testKeyVersions(t, ks)
}

func TestNewStreamEnd(t *testing.T) {
	k := common.NewStreamEnd(512, 0x1234567812345678, 1000000)
	k.Indexids = []uint32{1, 2, 3}

	ks := make([]*common.KeyVersions, 0, conf_maxKeyvers)
	for i := 0; i < conf_maxKeyvers; i++ {
		n := *k
		ks = append(ks, &n)
	}
	testKeyVersions(t, ks)
}

func testKeyVersions(t *testing.T, ks []*common.KeyVersions) {
	var data []byte
	var err error
	var payload interface{}

	if data, err = protobufEncode(ks); err != nil {
		t.Fatal(err)
	}
	if payload, err = protobufDecode(data); err != nil {
		t.Fatal(err)
	}

	n_ks, ok := payload.([]*protobuf.KeyVersions)
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

func testVbConnectionMap(t *testing.T, vbuckets []uint16, vbuuids []uint64) {
	var data []byte
	var err error
	var payload interface{}

	vbmap := common.VbConnectionMap{Vbuckets: vbuckets, Vbuuids: vbuuids}
	if data, err = protobufEncode(vbmap); err != nil {
		t.Fatal(err)
	}
	if payload, err = protobufDecode(data); err != nil {
		t.Fatal(err)
	}
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

func BenchmarkNewUpsertEncode(b *testing.B) {
	benchmarkMutationEncode(b, func() *common.KeyVersions {
		k := common.NewUpsert(512, 0x1234567812345678, []byte("cities"), 10000000)
		k.Keys = [][]byte{[]byte("bangalore"), []byte("delhi"), []byte("jaipur")}
		k.Oldkeys = [][]byte{[]byte("varanasi"), []byte("pune"), []byte("mahe")}
		k.Indexids = []uint32{1, 2, 3}
		return k
	})
}

func BenchmarkNewUpsertDecode(b *testing.B) {
	benchmarkMutationDecode(b, func() *common.KeyVersions {
		k := common.NewUpsert(512, 0x1234567812345678, []byte("cities"), 10000000)
		k.Keys = [][]byte{[]byte("bangalore"), []byte("delhi"), []byte("jaipur")}
		k.Oldkeys = [][]byte{[]byte("varanasi"), []byte("pune"), []byte("mahe")}
		k.Indexids = []uint32{1, 2, 3}
		return k
	})
}

func BenchmarkNewDeletionEncode(b *testing.B) {
	benchmarkMutationEncode(b, func() *common.KeyVersions {
		k := common.NewDeletion(512, 0x1234567812345678, []byte("cities"), 10000000)
		k.Indexids = []uint32{1, 2, 3}
		return k
	})
}

func BenchmarkNewDeletionDecode(b *testing.B) {
	benchmarkMutationDecode(b, func() *common.KeyVersions {
		k := common.NewDeletion(512, 0x1234567812345678, []byte("cities"), 10000000)
		k.Indexids = []uint32{1, 2, 3}
		return k
	})
}

func BenchmarkNewUpsertDeletionEncode(b *testing.B) {
	benchmarkMutationEncode(b, func() *common.KeyVersions {
		k := common.NewUpsertDeletion(512, 0x1234567812345678, []byte("cities"), 10000000)
		k.Indexids = []uint32{1, 2, 3}
		return k
	})
}

func BenchmarkNewUpsertDeletionDecode(b *testing.B) {
	benchmarkMutationDecode(b, func() *common.KeyVersions {
		k := common.NewUpsertDeletion(512, 0x1234567812345678, []byte("cities"), 10000000)
		k.Indexids = []uint32{1, 2, 3}
		return k
	})
}

func BenchmarkNewSyncEncode(b *testing.B) {
	benchmarkMutationEncode(b, func() *common.KeyVersions {
		k := common.NewSync(512, 0x1234567812345678, 10000000)
		k.Indexids = []uint32{1, 2, 3}
		return k
	})
}

func BenchmarkNewSyncDecode(b *testing.B) {
	benchmarkMutationDecode(b, func() *common.KeyVersions {
		k := common.NewSync(512, 0x1234567812345678, 10000000)
		k.Indexids = []uint32{1, 2, 3}
		return k
	})
}

func BenchmarkNewStreamBeginEncode(b *testing.B) {
	benchmarkMutationEncode(b, func() *common.KeyVersions {
		k := common.NewStreamBegin(512, 0x1234567812345678, 1000000)
		k.Indexids = []uint32{1, 2, 3}
		return k
	})
}

func BenchmarkNewStreamBeginDecode(b *testing.B) {
	benchmarkMutationDecode(b, func() *common.KeyVersions {
		k := common.NewStreamBegin(512, 0x1234567812345678, 1000000)
		k.Indexids = []uint32{1, 2, 3}
		return k
	})
}

func BenchmarkNewStreamEndEncode(b *testing.B) {
	benchmarkMutationEncode(b, func() *common.KeyVersions {
		k := common.NewStreamEnd(512, 0x1234567812345678, 1000000)
		k.Indexids = []uint32{1, 2, 3}
		return k
	})
}

func BenchmarkNewStreamEndDecode(b *testing.B) {
	benchmarkMutationDecode(b, func() *common.KeyVersions {
		k := common.NewStreamEnd(512, 0x1234567812345678, 1000000)
		k.Indexids = []uint32{1, 2, 3}
		return k
	})
}

func BenchmarkVbmapEncode1024(b *testing.B) {
	vbuckets := make([]uint16, 1024)
	vbuuids := make([]uint64, 1024)
	for i := 0; i < 1024; i++ {
		vbuckets[i] = uint16(i)
		vbuuids[i] = uint64(1000000 + i)
	}
	vbmap := common.VbConnectionMap{Vbuckets: vbuckets, Vbuuids: vbuuids}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		protobufEncode(vbmap)
	}
}

func BenchmarkVbmapDecode1024(b *testing.B) {
	vbuckets := make([]uint16, 1024)
	vbuuids := make([]uint64, 1024)
	for i := 0; i < 1024; i++ {
		vbuckets[i] = uint16(i)
		vbuuids[i] = uint64(1000000 + i)
	}
	vbmap := common.VbConnectionMap{Vbuckets: vbuckets, Vbuuids: vbuuids}
	data, _ := protobufEncode(vbmap)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		protobufDecode(data)
	}
}

func benchmarkMutationEncode(b *testing.B, fn func() *common.KeyVersions) {
	k := fn()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		protobufEncode([]*common.KeyVersions{k})
	}
}

func benchmarkMutationDecode(b *testing.B, fn func() *common.KeyVersions) {
	k := fn()
	data, _ := protobufEncode([]*common.KeyVersions{k})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		protobufDecode(data)
	}
}
