package indexer

import (
	forestdb "github.com/couchbase/indexing/secondary/fdb"
	"os"
	"testing"
)

func TestForestDBIterator(t *testing.T) {
	defer os.RemoveAll("test")

	dbfile, err := forestdb.Open("test", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dbfile.Close()

	kvstore, err := dbfile.OpenKVStoreDefault(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer kvstore.Close()

	// store a bunch of values to test the iterator

	kvstore.SetKV([]byte("a"), []byte("vala"))
	kvstore.SetKV([]byte("b"), []byte("valb"))
	kvstore.SetKV([]byte("c"), []byte("valc"))
	kvstore.SetKV([]byte("d"), []byte("vald"))
	kvstore.SetKV([]byte("e"), []byte("vale"))
	kvstore.SetKV([]byte("f"), []byte("valf"))
	kvstore.SetKV([]byte("g"), []byte("valg"))
	kvstore.SetKV([]byte("h"), []byte("valh"))
	kvstore.SetKV([]byte("i"), []byte("vali"))
	kvstore.SetKV([]byte("j"), []byte("valj"))

	dbfile.Commit(forestdb.COMMIT_MANUAL_WAL_FLUSH)

	info, err := kvstore.Info()
	if err != nil {
		t.Fatal(err)
	}
	lastSeqNum := info.LastSeqNum()

	iter, err := newForestDBIterator(kvstore, lastSeqNum)
	if err != nil {
		t.Fatal(err)
	}

	defer iter.Close()

	iter.SeekFirst()

	count := 0
	var firstKey, lastKey []byte

	for ; iter.Valid(); iter.Next() {
		if firstKey == nil {
			firstKey = iter.Key()
		}
		count++
		lastKey = iter.Key()
	}

	if count != 10 {
		t.Errorf("exptected to iterate 10, saw %d", count)
	}
	if string(firstKey) != "a" {
		t.Errorf("expected fist key to be a, got %s", firstKey)
	}
	if string(lastKey) != "j" {
		t.Errorf("expected last key to be j, got %s", lastKey)
	}

}

func TestForestDBIteratorSeek(t *testing.T) {
	defer os.RemoveAll("test")

	dbfile, err := forestdb.Open("test", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dbfile.Close()

	kvstore, err := dbfile.OpenKVStoreDefault(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer kvstore.Close()

	// store a bunch of values to test the iterator

	kvstore.SetKV([]byte("a"), []byte("vala"))
	kvstore.SetKV([]byte("b"), []byte("valb"))
	kvstore.SetKV([]byte("c"), []byte("valc"))
	kvstore.SetKV([]byte("d"), []byte("vald"))
	kvstore.SetKV([]byte("e"), []byte("vale"))
	kvstore.SetKV([]byte("f"), []byte("valf"))
	kvstore.SetKV([]byte("g"), []byte("valg"))
	kvstore.SetKV([]byte("h"), []byte("valh"))
	kvstore.SetKV([]byte("i"), []byte("vali"))
	kvstore.SetKV([]byte("j"), []byte("valj"))

	dbfile.Commit(forestdb.COMMIT_MANUAL_WAL_FLUSH)

	info, err := kvstore.Info()
	if err != nil {
		t.Fatal(err)
	}
	lastSeqNum := info.LastSeqNum()

	iter, err := newForestDBIterator(kvstore, lastSeqNum)
	if err != nil {
		t.Fatal(err)
	}

	defer iter.Close()

	iter.Seek([]byte("d"))

	count := 0
	var firstKey, lastKey []byte

	for ; iter.Valid(); iter.Next() {
		if firstKey == nil {
			firstKey = iter.Key()
		}
		count++
		lastKey = iter.Key()
	}

	if count != 7 {
		t.Errorf("exptected to iterate 7, saw %d", count)
	}
	if string(firstKey) != "d" {
		t.Errorf("expected fist key to be d, got %s", firstKey)
	}
	if string(lastKey) != "j" {
		t.Errorf("expected last key to be j, got %s", lastKey)
	}

}
