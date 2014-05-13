package indexer

import (
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	"testing"
)

const confMaxKeyvers = 10

func TestVbConnectionMap(t *testing.T) {
	vbuckets := []uint16{1, 2, 3, 4}
	vbuuids := []uint64{10, 20, 30, 40}

	vbmap := &common.VbConnectionMap{
		Bucket:   "default",
		Vbuckets: vbuckets,
		Vbuuids:  vbuuids,
	}
	data, err := protobufEncode(vbmap)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := protobufDecode(data)
	if err != nil {
		t.Fatal(err)
	}
	vbmap1, ok := payload.(*protobuf.VbConnectionMap)
	if ok == false {
		t.Fatal("expected reference VbConnectionMap object")
	}
	if vbmap.Equal(protobuf2Vbmap(vbmap1)) == false {
		t.Fatal("failed VbConnectionMap")
	}
}

func TestAddUpsert(t *testing.T) {
	kv := kvUpserts()
	vbno, vbuuid, nMuts := uint16(10), uint64(1000), 10
	vb := common.NewVbKeyVersions("default", vbno, vbuuid, nMuts)
	addKeyVersions(vb, []*common.KeyVersions{kv}, 1, nMuts)
	testKeyVersions(t, vb)
}

func TestAddUpsertDeletion(t *testing.T) {
	kv := kvUpsertDeletions()
	vbno, vbuuid, nMuts := uint16(10), uint64(1000), 10
	vb := common.NewVbKeyVersions("default", vbno, vbuuid, nMuts)
	addKeyVersions(vb, []*common.KeyVersions{kv}, 1, nMuts)
	testKeyVersions(t, vb)
}

func TestAddDeletion(t *testing.T) {
	kv := kvDeletions()
	vbno, vbuuid, nMuts := uint16(10), uint64(1000), 10
	vb := common.NewVbKeyVersions("default", vbno, vbuuid, nMuts)
	addKeyVersions(vb, []*common.KeyVersions{kv}, 1, nMuts)
	testKeyVersions(t, vb)
}

func TestAddSync(t *testing.T) {
	seqno, docid, maxCount := uint64(10), []byte(nil), 1
	kv := common.NewKeyVersions(seqno, docid, maxCount)
	kv.AddSync()
	// add it to VbKeyVersions
	vbno, vbuuid, nMuts := uint16(10), uint64(1000), 10
	vb := common.NewVbKeyVersions("default", vbno, vbuuid, nMuts)
	vb.AddKeyVersions(kv)
	testKeyVersions(t, vb)
}

func TestAddDropData(t *testing.T) {
	seqno, docid, maxCount := uint64(10), []byte(nil), 1
	kv := common.NewKeyVersions(seqno, docid, maxCount)
	kv.AddDropData()
	// add it to VbKeyVersions
	vbno, vbuuid, nMuts := uint16(10), uint64(1000), 10
	vb := common.NewVbKeyVersions("default", vbno, vbuuid, nMuts)
	vb.AddKeyVersions(kv)
	testKeyVersions(t, vb)
}

func TestNewStreamBegin(t *testing.T) {
	seqno, docid, maxCount := uint64(10), []byte("document-name"), 10
	kv := common.NewKeyVersions(seqno, docid, maxCount)
	kv.AddStreamBegin()
	// add it to VbKeyVersions
	vbno, vbuuid, nMuts := uint16(10), uint64(1000), 10
	vb := common.NewVbKeyVersions("default", vbno, vbuuid, nMuts)
	vb.AddKeyVersions(kv)
	testKeyVersions(t, vb)
}

func TestNewStreamEnd(t *testing.T) {
	seqno, docid, maxCount := uint64(10), []byte("document-name"), 10
	kv := common.NewKeyVersions(seqno, docid, maxCount)
	kv.AddStreamEnd()
	// add it to VbKeyVersions
	vbno, vbuuid, nMuts := uint16(10), uint64(1000), 10
	vb := common.NewVbKeyVersions("default", vbno, vbuuid, nMuts)
	vb.AddKeyVersions(kv)
	testKeyVersions(t, vb)
}

func testKeyVersions(t *testing.T, vb *common.VbKeyVersions) {
	var data []byte
	var err error
	var payload interface{}

	vbsRef := []*common.VbKeyVersions{vb}
	if data, err = protobufEncode(vbsRef); err != nil {
		t.Fatal(err)
	}
	if payload, err = protobufDecode(data); err != nil {
		t.Fatal(err)
	}

	val, ok := payload.([]*protobuf.VbKeyVersions)
	if ok == false {
		t.Fatal("expected slice of reference to KeyVersions object")
	}
	vbs := protobuf2VbKeyVersions(val)
	for i, vb := range vbsRef {
		if vb.Equal(vbs[i]) == false {
			t.Fatal("failed protobuf encoding")
		}
	}
}

func BenchmarkAddUpsertEncode(b *testing.B) {
	benchmarkMutationEncode(b, func() *common.VbKeyVersions {
		kv := kvUpserts()
		vbno, vbuuid, nMuts := uint16(10), uint64(1000), 10
		vb := common.NewVbKeyVersions("default", vbno, vbuuid, nMuts)
		addKeyVersions(vb, []*common.KeyVersions{kv}, 1, nMuts)
		return vb
	})
}

func BenchmarkAddUpsertDecode(b *testing.B) {
	benchmarkMutationDecode(b, func() *common.VbKeyVersions {
		kv := kvUpserts()
		vbno, vbuuid, nMuts := uint16(10), uint64(1000), 10
		vb := common.NewVbKeyVersions("default", vbno, vbuuid, nMuts)
		addKeyVersions(vb, []*common.KeyVersions{kv}, 1, nMuts)
		return vb
	})
}

func BenchmarkAddDeletionEncode(b *testing.B) {
	benchmarkMutationEncode(b, func() *common.VbKeyVersions {
		kv := kvDeletions()
		vbno, vbuuid, nMuts := uint16(10), uint64(1000), 10
		vb := common.NewVbKeyVersions("default", vbno, vbuuid, nMuts)
		addKeyVersions(vb, []*common.KeyVersions{kv}, 1, nMuts)
		return vb
	})
}

func BenchmarkAddDeletionDecode(b *testing.B) {
	benchmarkMutationDecode(b, func() *common.VbKeyVersions {
		kv := kvDeletions()
		vbno, vbuuid, nMuts := uint16(10), uint64(1000), 10
		vb := common.NewVbKeyVersions("default", vbno, vbuuid, nMuts)
		addKeyVersions(vb, []*common.KeyVersions{kv}, 1, nMuts)
		return vb
	})
}

func BenchmarkAddUpsertDeletionEncode(b *testing.B) {
	benchmarkMutationEncode(b, func() *common.VbKeyVersions {
		kv := kvUpsertDeletions()
		vbno, vbuuid, nMuts := uint16(10), uint64(1000), 10
		vb := common.NewVbKeyVersions("default", vbno, vbuuid, nMuts)
		addKeyVersions(vb, []*common.KeyVersions{kv}, 1, nMuts)
		return vb
	})
}

func BenchmarkAddUpsertDeletionDecode(b *testing.B) {
	benchmarkMutationDecode(b, func() *common.VbKeyVersions {
		kv := kvUpsertDeletions()
		vbno, vbuuid, nMuts := uint16(10), uint64(1000), 10
		vb := common.NewVbKeyVersions("default", vbno, vbuuid, nMuts)
		addKeyVersions(vb, []*common.KeyVersions{kv}, 1, nMuts)
		return vb
	})
}

func BenchmarkAddSyncEncode(b *testing.B) {
	benchmarkMutationEncode(b, func() *common.VbKeyVersions {
		seqno, docid, maxCount := uint64(10), []byte(nil), 1
		kv := common.NewKeyVersions(seqno, docid, maxCount)
		kv.AddSync()
		vbno, vbuuid, nMuts := uint16(10), uint64(1000), 10
		vb := common.NewVbKeyVersions("default", vbno, vbuuid, nMuts)
		vb.AddKeyVersions(kv)
		return vb
	})
}

func BenchmarkAddSyncDecode(b *testing.B) {
	benchmarkMutationDecode(b, func() *common.VbKeyVersions {
		seqno, docid, maxCount := uint64(10), []byte(nil), 1
		kv := common.NewKeyVersions(seqno, docid, maxCount)
		kv.AddSync()
		vbno, vbuuid, nMuts := uint16(10), uint64(1000), 10
		vb := common.NewVbKeyVersions("default", vbno, vbuuid, nMuts)
		vb.AddKeyVersions(kv)
		return vb
	})
}

func BenchmarkAddStreamBeginEncode(b *testing.B) {
	benchmarkMutationEncode(b, func() *common.VbKeyVersions {
		seqno, docid, maxCount := uint64(10), []byte("document-name"), 10
		kv := common.NewKeyVersions(seqno, docid, maxCount)
		kv.AddStreamBegin()
		// add it to VbKeyVersions
		vbno, vbuuid, nMuts := uint16(10), uint64(1000), 10
		vb := common.NewVbKeyVersions("default", vbno, vbuuid, nMuts)
		vb.AddKeyVersions(kv)
		return vb
	})
}

func BenchmarkAddStreamBeginDecode(b *testing.B) {
	benchmarkMutationDecode(b, func() *common.VbKeyVersions {
		seqno, docid, maxCount := uint64(10), []byte("document-name"), 10
		kv := common.NewKeyVersions(seqno, docid, maxCount)
		kv.AddStreamBegin()
		// add it to VbKeyVersions
		vbno, vbuuid, nMuts := uint16(10), uint64(1000), 10
		vb := common.NewVbKeyVersions("default", vbno, vbuuid, nMuts)
		vb.AddKeyVersions(kv)
		return vb
	})
}

func BenchmarkAddStreamEndEncode(b *testing.B) {
	benchmarkMutationEncode(b, func() *common.VbKeyVersions {
		seqno, docid, maxCount := uint64(10), []byte("document-name"), 10
		kv := common.NewKeyVersions(seqno, docid, maxCount)
		kv.AddStreamEnd()
		// add it to VbKeyVersions
		vbno, vbuuid, nMuts := uint16(10), uint64(1000), 10
		vb := common.NewVbKeyVersions("default", vbno, vbuuid, nMuts)
		vb.AddKeyVersions(kv)
		return vb
	})
}

func BenchmarkAddStreamEndDecode(b *testing.B) {
	benchmarkMutationDecode(b, func() *common.VbKeyVersions {
		seqno, docid, maxCount := uint64(10), []byte("document-name"), 10
		kv := common.NewKeyVersions(seqno, docid, maxCount)
		kv.AddStreamEnd()
		// add it to VbKeyVersions
		vbno, vbuuid, nMuts := uint16(10), uint64(1000), 10
		vb := common.NewVbKeyVersions("default", vbno, vbuuid, nMuts)
		vb.AddKeyVersions(kv)
		return vb
	})
}

func BenchmarkVbmapEncode1024(b *testing.B) {
	vbuckets := make([]uint16, 1024)
	vbuuids := make([]uint64, 1024)
	for i := 0; i < 1024; i++ {
		vbuckets[i] = uint16(i)
		vbuuids[i] = uint64(1000000 + i)
	}
	vbmap := &common.VbConnectionMap{
		Bucket:   "default",
		Vbuckets: vbuckets,
		Vbuuids:  vbuuids,
	}

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
	vbmap := &common.VbConnectionMap{
		Bucket:   "default",
		Vbuckets: vbuckets,
		Vbuuids:  vbuuids,
	}
	data, _ := protobufEncode(vbmap)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		protobufDecode(data)
	}
}

func benchmarkMutationEncode(b *testing.B, fn func() *common.VbKeyVersions) {
	vb := fn()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		protobufEncode([]*common.VbKeyVersions{vb})
	}
}

func benchmarkMutationDecode(b *testing.B, fn func() *common.VbKeyVersions) {
	vb := fn()
	data, _ := protobufEncode([]*common.VbKeyVersions{vb})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		protobufDecode(data)
	}
}

func kvUpserts() *common.KeyVersions {
	seqno, docid, maxCount := uint64(10), []byte("document-name"), 10
	kv := common.NewKeyVersions(seqno, docid, maxCount)
	kv.AddUpsert(1, []byte("bangalore"), []byte("varanasi"))
	kv.AddUpsert(2, []byte("delhi"), []byte("pune"))
	kv.AddUpsert(3, []byte("jaipur"), []byte("mahe"))
	return kv
}

func kvUpsertDeletions() *common.KeyVersions {
	seqno, docid, maxCount := uint64(10), []byte("document-name"), 10
	kv := common.NewKeyVersions(seqno, docid, maxCount)
	kv.AddUpsertDeletion(1, []byte("varanasi"))
	kv.AddUpsertDeletion(2, []byte("pune"))
	kv.AddUpsertDeletion(3, []byte("mahe"))
	return kv
}

func kvDeletions() *common.KeyVersions {
	seqno, docid, maxCount := uint64(10), []byte("document-name"), 10
	kv := common.NewKeyVersions(seqno, docid, maxCount)
	kv.AddDeletion(1, []byte("varanasi"))
	kv.AddDeletion(2, []byte("pune"))
	kv.AddDeletion(3, []byte("mahe"))
	return kv
}

func addKeyVersions(vb *common.VbKeyVersions, kvs []*common.KeyVersions, seqno uint64, nMuts int) uint64 {
	ln := len(kvs)
	for i := 0; i < nMuts; i++ {
		newkv := *kvs[i%ln]
		newkv.Seqno = seqno
		vb.AddKeyVersions(&newkv)
		seqno++
	}
	return seqno
}
