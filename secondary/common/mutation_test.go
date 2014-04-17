package common

import (
	"testing"
)

const conf_maxKeyvers = 10

func TestMutationKeyVersions(t *testing.T) {
	m := NewMutation(conf_maxKeyvers)
	// prepare
	k := NewUpsert(512, 0x1234567812345678, []byte("cities"), 10000000)
	k.Keys = [][]byte{[]byte("bangalore"), []byte("delhi"), []byte("jaipur")}
	k.Oldkeys = [][]byte{[]byte("varanasi"), []byte("pune"), []byte("mahe")}
	k.Indexids = []uint32{1, 2, 3}

	ks := make([]*KeyVersions, 0, conf_maxKeyvers)
	for i := 0; i < conf_maxKeyvers; i++ {
		n := *k
		ks = append(ks, &n)
	}

	m.NewPayload(PAYLOAD_KEYVERSIONS)

	// test mixing payloads
	if m.SetVbuckets(nil, nil) == nil {
		t.Fatal("expected an error")
	}
	// test adding key-versions
	for _, k := range ks {
		if err := m.AddKeyVersions(k); err != nil {
			t.Fatal(err)
		}
	}
	// test over adding key-version
	if m.AddKeyVersions(ks[0]) == nil {
		t.Fatal("expected an error")
	}
	// test getting back key-versions
	for i, nks := range m.GetKeyVersions() {
		if nks != ks[i] {
			t.Fatal("mismatch while getting back keyversions")
		}
	}
}

func TestMutationVbmap(t *testing.T) {
	m := NewMutation(conf_maxKeyvers)
	m.NewPayload(PAYLOAD_VBMAP)

	// test mixing payload
	if m.AddKeyVersions(nil) == nil {
		t.Fatal("expected an error")
	}
	// test adding vbmap payload
	vbuckets := []uint16{1, 2, 3, 4}
	vbuuids := []uint64{10, 20, 30, 40}
	if err := m.SetVbuckets(vbuckets, vbuuids); err != nil {
		t.Fatal(err)
	}
	// test getting back vbmap
	vbmap := m.GetVbmap()
	for i, vbno := range vbmap.Vbuckets {
		if vbno != vbuckets[i] || vbmap.Vbuuids[i] != vbuuids[i] {
			t.Fatal("mismatch in vbmap")
		}
	}
}
