package custom_comparator

import (
	"os"
	"testing"
	"unsafe"

	"github.com/couchbase/goforestdb"
)

func TestForestCustomComparator(t *testing.T) {
	defer os.RemoveAll("test")

	dbfile, err := forestdb.Open("test", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dbfile.Close()

	kvconfig := forestdb.DefaultKVStoreConfig()
	kvconfig.SetCustomCompare(unsafe.Pointer(CompareBytesReversedPointer))

	kvstore, err := dbfile.OpenKVStore("tkv", kvconfig)
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

	iter, err := kvstore.IteratorInit([]byte("g"), []byte("c"), forestdb.ITR_NONE)
	if err != nil {
		t.Fatal(err)
	}

	doc, err := iter.Next()
	count := 0
	var firstKey, lastKey []byte
	for err == nil {
		count++
		if firstKey == nil {
			firstKey = doc.Key()
		}
		lastKey = doc.Key()
		doc, err = iter.Next()
	}
	if count != 5 {
		t.Errorf("exptected to iterate 5, saw %d", count)
	}
	if string(firstKey) != "g" {
		t.Errorf("expected fist key to be c, got %s", firstKey)
	}
	if string(lastKey) != "c" {
		t.Errorf("expected last key to be g, got %s", lastKey)
	}
	if err != forestdb.RESULT_ITERATOR_FAIL {
		t.Errorf("expected %#v, got %#v", forestdb.RESULT_ITERATOR_FAIL, err)
	}
}
