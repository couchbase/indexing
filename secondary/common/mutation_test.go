// TODO: test case for, VbConnectionMap.Ids() VbConnectionMap.GetVbuuid()
//  VbKeyVersions.Free(), VbKeyVersions.FreeKeyVersions(), KeyVersions.Free().

package common

import (
	"testing"
)

func TestPayloadVbmap(t *testing.T) {
	p := NewStreamPayload(PayloadVbmap, 3)

	// test mixing payload
	vb := NewVbKeyVersions("default", 1 /*vbno*/, 10 /*vbuuid*/, 1000, 16)
	if p.AddVbKeyVersions(vb) == nil {
		t.Fatal("expected an error")
	}
	// test adding vbmap payload
	vbuckets := []uint16{1, 2, 3, 4}
	vbuuids := []uint64{10, 20, 30, 40}
	if err := p.SetVbmap("default", vbuckets, vbuuids); err != nil {
		t.Fatal(err)
	}
}

func TestKVEqual(t *testing.T) {
	seqno, docid, maxCount := uint64(10), []byte("document-name"), 10
	kv1 := NewKeyVersions(seqno, docid, maxCount, 0)
	kv2 := NewKeyVersions(seqno, docid, maxCount, 0)
	for i := 0; i < maxCount; i++ {
		uuid := uint64(i * 10000)
		kv1.AddUpsert(uuid, []byte("newkey"), []byte("oldkey"), nil, nil)
		kv2.AddUpsert(uuid, []byte("newkey"), []byte("oldkey"), nil, nil)
	}
	if kv1.Equal(kv2) == false {
		t.Fatal("failed KeyVersions equality")
	}
	kv2.AddSync()
	if kv1.Equal(kv2) {
		t.Fatal("failed KeyVersions equality")
	}
}

func TestPayloadKeyVersions(t *testing.T) {
	nVb := 3
	p := NewStreamPayload(PayloadKeyVersions, nVb)

	keys := [][]byte{[]byte("bangalore"), []byte("delhi"), []byte("jaipur")}
	oldkeys := [][]byte{[]byte("varanasi"), []byte("pune"), []byte("mahe")}
	uuids := []uint64{1, 2, 3}
	nIndexes := 3
	for i := 0; i < nVb; i++ { // for N vbuckets
		vbno, vbuuid := uint16(i), uint64(i*10)
		vb := NewVbKeyVersions("default", vbno, vbuuid, 1000, 16)
		for j := 0; j < 10; j++ { // for 10 mutations
			kv := NewKeyVersions(512 /*seqno*/, []byte("Bourne"), nIndexes, 0)
			kv.AddUpsert(uuids[0], keys[0], oldkeys[0], nil, nil)
			kv.AddUpsert(uuids[1], keys[1], oldkeys[1], nil, nil)
			kv.AddUpsert(uuids[2], keys[2], oldkeys[2], nil, nil)
			vb.AddKeyVersions(kv)
		}
		p.AddVbKeyVersions(vb)
	}
	// test mixing payloads
	if p.SetVbmap("default", nil, nil) == nil {
		t.Fatal("expected an error")
	}
}

func BenchmarkKVEqual(b *testing.B) {
	seqno, docid, maxCount := uint64(10), []byte("document-name"), 10
	kv1 := NewKeyVersions(seqno, docid, maxCount, 0)
	kv2 := NewKeyVersions(seqno, docid, maxCount, 0)
	for i := 0; i < maxCount; i++ {
		uuid := uint64(i * 10000)
		kv1.AddUpsert(uuid, []byte("newkey"), []byte("oldkey"), nil, nil)
		kv2.AddUpsert(uuid, []byte("newkey"), []byte("oldkey"), nil, nil)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv1.Equal(kv2)
	}
}
