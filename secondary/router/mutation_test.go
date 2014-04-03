package mutation

import (
	"bytes"
	"github.com/couchbase/indexing/secondary/common"
	"testing"
)

func TestNewUpsert(t *testing.T) {
	var data []byte
	var err error
	m := NewUpsert(512, 0x1234567812345678, []byte("cities"), 10000000)
	m.Keys = [][]byte{[]byte("bangalore"), []byte("delhi"), []byte("jaipur")}
	m.Oldkeys = [][]byte{[]byte("varanasi"), []byte("pune"), []byte("mahe")}
	m.Indexids = []uint32{1, 2, 3}
	if data, err = m.Encode(); err != nil {
		t.Fatal(err)
	}

	newm := &common.Mutation{}
	if err = newm.Decode(data); err != nil {
		t.Fatal(err)
	}

	if m.Vbucket != newm.Vbucket || newm.Vbuuid != m.Vbuuid ||
		bytes.Compare(newm.Docid, m.Docid) != 0 || newm.Seqno != m.Seqno ||
		newm.Command != m.Command {
		t.Fatal("Mistmatch between encode and decode")
	}
	for i, _ := range newm.Keys {
		if bytes.Compare(newm.Keys[i], m.Keys[i]) != 0 {
			t.Fatal("Mismatch in keys")
		}
		if bytes.Compare(newm.Oldkeys[i], m.Oldkeys[i]) != 0 {
			t.Fatal("Mismatch in old-keys")
		}
		if newm.Indexids[i] != m.Indexids[i] {
			t.Fatal("Mismatch in indexids")
		}
	}
}

func TestNewDeletion(t *testing.T) {
	var data []byte
	var err error
	m := NewDeletion(512, 0x1234567812345678, []byte("cities"), 10000000)
	m.Indexids = []uint32{1, 2, 3}
	if data, err = m.Encode(); err != nil {
		t.Fatal(err)
	}

	newm := &common.Mutation{}
	if err = newm.Decode(data); err != nil {
		t.Fatal(err)
	}

	if m.Vbucket != newm.Vbucket || newm.Vbuuid != m.Vbuuid ||
		bytes.Compare(newm.Docid, m.Docid) != 0 || newm.Seqno != m.Seqno ||
		newm.Command != m.Command {
		t.Fatal("Mistmatch between encode and decode")
	}
}

func TestNewUpsertDeletion(t *testing.T) {
	var data []byte
	var err error
	m := NewUpsertDeletion(512, 0x1234567812345678, []byte("cities"), 10000000)
	m.Indexids = []uint32{1, 2, 3}
	if data, err = m.Encode(); err != nil {
		t.Fatal(err)
	}

	newm := &common.Mutation{}
	if err = newm.Decode(data); err != nil {
		t.Fatal(err)
	}

	if m.Vbucket != newm.Vbucket || newm.Vbuuid != m.Vbuuid ||
		bytes.Compare(newm.Docid, m.Docid) != 0 || newm.Seqno != m.Seqno ||
		newm.Command != m.Command {
		t.Fatal("Mistmatch between encode and decode")
	}
}

func TestNewSync(t *testing.T) {
	var data []byte
	var err error
	m := NewSync(512, 0x1234567812345678, 10000000)
	m.Indexids = []uint32{1, 2, 3}
	if data, err = m.Encode(); err != nil {
		t.Fatal(err)
	}

	newm := &common.Mutation{}
	if err = newm.Decode(data); err != nil {
		t.Fatal(err)
	}

	if m.Vbucket != newm.Vbucket || newm.Vbuuid != m.Vbuuid ||
		newm.Seqno != m.Seqno || newm.Command != m.Command {
		t.Fatal("Mistmatch between encode and decode")
	}
}

func TestNewStreamBegin(t *testing.T) {
	var data []byte
	var err error
	m := NewStreamBegin(512, 0x1234567812345678)
	m.Indexids = []uint32{1, 2, 3}
	if data, err = m.Encode(); err != nil {
		t.Fatal(err)
	}

	newm := &common.Mutation{}
	if err = newm.Decode(data); err != nil {
		t.Fatal(err)
	}

	if m.Vbucket != newm.Vbucket || newm.Vbuuid != m.Vbuuid ||
		newm.Command != m.Command {
		t.Fatal("Mistmatch between encode and decode")
	}
}

func TestNewStreamEnd(t *testing.T) {
	var data []byte
	var err error
	m := NewStreamEnd(512, 0x1234567812345678)
	m.Indexids = []uint32{1, 2, 3}
	if data, err = m.Encode(); err != nil {
		t.Fatal(err)
	}

	newm := &common.Mutation{}
	if err = newm.Decode(data); err != nil {
		t.Fatal(err)
	}

	if m.Vbucket != newm.Vbucket || newm.Vbuuid != m.Vbuuid ||
		newm.Command != m.Command {
		t.Fatal("Mistmatch between encode and decode")
	}
}

func BenchmarkNewUpsertEncode(b *testing.B) {
	benchmarkMutationEncode(b, func() *common.Mutation {
		m := NewUpsert(512, 0x1234567812345678, []byte("cities"), 10000000)
		m.Keys = [][]byte{[]byte("bangalore"), []byte("delhi"), []byte("jaipur")}
		m.Oldkeys = [][]byte{[]byte("varanasi"), []byte("pune"), []byte("mahe")}
		m.Indexids = []uint32{1, 2, 3}
		return m
	})
}
func BenchmarkNewUpsertDecode(b *testing.B) {
	benchmarkMutationDecode(b, func() (*common.Mutation, []byte) {
		m := NewUpsert(512, 0x1234567812345678, []byte("cities"), 10000000)
		m.Keys = [][]byte{[]byte("bangalore"), []byte("delhi"), []byte("jaipur")}
		m.Oldkeys = [][]byte{[]byte("varanasi"), []byte("pune"), []byte("mahe")}
		m.Indexids = []uint32{1, 2, 3}
		data, _ := m.Encode()
		return m, data
	})
}

func BenchmarkNewDeletionEncode(b *testing.B) {
	benchmarkMutationEncode(b, func() *common.Mutation {
		m := NewDeletion(512, 0x1234567812345678, []byte("cities"), 10000000)
		m.Indexids = []uint32{1, 2, 3}
		return m
	})
}

func BenchmarkNewDeletionDecode(b *testing.B) {
	benchmarkMutationDecode(b, func() (*common.Mutation, []byte) {
		m := NewDeletion(512, 0x1234567812345678, []byte("cities"), 10000000)
		m.Indexids = []uint32{1, 2, 3}
		data, _ := m.Encode()
		return m, data
	})
}

func BenchmarkNewUpsertDeletionEncode(b *testing.B) {
	benchmarkMutationEncode(b, func() *common.Mutation {
		m := NewUpsertDeletion(512, 0x1234567812345678, []byte("cities"), 10000000)
		m.Indexids = []uint32{1, 2, 3}
		return m
	})
}

func BenchmarkNewUpsertDeletionDecode(b *testing.B) {
	benchmarkMutationDecode(b, func() (*common.Mutation, []byte) {
		m := NewUpsertDeletion(512, 0x1234567812345678, []byte("cities"), 10000000)
		m.Indexids = []uint32{1, 2, 3}
		data, _ := m.Encode()
		return m, data
	})
}

func BenchmarkNewSyncEncode(b *testing.B) {
	benchmarkMutationEncode(b, func() *common.Mutation {
		m := NewSync(512, 0x1234567812345678, 10000000)
		m.Indexids = []uint32{1, 2, 3}
		return m
	})
}

func BenchmarkNewSyncDecode(b *testing.B) {
	benchmarkMutationDecode(b, func() (*common.Mutation, []byte) {
		m := NewSync(512, 0x1234567812345678, 10000000)
		m.Indexids = []uint32{1, 2, 3}
		data, _ := m.Encode()
		return m, data
	})
}

func BenchmarkNewStreamBeginEncode(b *testing.B) {
	benchmarkMutationEncode(b, func() *common.Mutation {
		m := NewStreamBegin(512, 0x1234567812345678)
		m.Indexids = []uint32{1, 2, 3}
		return m
	})
}

func BenchmarkNewStreamBeginDecode(b *testing.B) {
	benchmarkMutationDecode(b, func() (*common.Mutation, []byte) {
		m := NewStreamBegin(512, 0x1234567812345678)
		m.Indexids = []uint32{1, 2, 3}
		data, _ := m.Encode()
		return m, data
	})
}

func BenchmarkNewStreamEndEncode(b *testing.B) {
	benchmarkMutationEncode(b, func() *common.Mutation {
		m := NewStreamEnd(512, 0x1234567812345678)
		m.Indexids = []uint32{1, 2, 3}
		return m
	})
}

func BenchmarkNewStreamEndDecode(b *testing.B) {
	benchmarkMutationDecode(b, func() (*common.Mutation, []byte) {
		m := NewStreamEnd(512, 0x1234567812345678)
		m.Indexids = []uint32{1, 2, 3}
		data, _ := m.Encode()
		return m, data
	})
}

func benchmarkMutationEncode(b *testing.B, fn func() *common.Mutation) {
	m := fn()
	for i := 0; i < b.N; i++ {
		m.Encode()
	}
}

func benchmarkMutationDecode(b *testing.B, fn func() (*common.Mutation, []byte)) {
	m, data := fn()
	for i := 0; i < b.N; i++ {
		m.Decode(data)
	}
}
