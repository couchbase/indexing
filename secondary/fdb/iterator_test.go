//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.
package forestdb

import (
	"os"
	"testing"
)

func TestForestDBIterator(t *testing.T) {
	defer os.RemoveAll("test")

	dbfile, err := Open("test", nil)
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

	iter, err := kvstore.IteratorInit([]byte("c"), []byte("g"), ITR_NONE)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	doc, err := iter.Get()
	count := 0
	var firstKey, lastKey []byte
	for err == nil {
		count++
		if firstKey == nil {
			firstKey = doc.Key()
		}
		lastKey = doc.Key()
		err = iter.Next()
		if err == nil {
			doc, err = iter.Get()
		}
	}
	if count != 5 {
		t.Errorf("exptected to iterate 5, saw %d", count)
	}
	if string(firstKey) != "c" {
		t.Errorf("expected fist key to be c, got %s", firstKey)
	}
	if string(lastKey) != "g" {
		t.Errorf("expected lats key to be g, got %s", lastKey)
	}
	if err != FDB_RESULT_ITERATOR_FAIL {
		t.Errorf("expected %#v, got %#v", FDB_RESULT_ITERATOR_FAIL, err)
	}

}

func TestForestDBIteratorSeq(t *testing.T) {
	defer os.RemoveAll("test")

	dbfile, err := Open("test", nil)
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

	iter, err := kvstore.IteratorSequenceInit(3, 7, ITR_NONE)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	doc, err := iter.Get()
	count := 0
	var firstKey, lastKey []byte
	for err == nil {
		count++
		if firstKey == nil {
			firstKey = doc.Key()
		}
		lastKey = doc.Key()
		err = iter.Next()
		if err == nil {
			doc, err = iter.Get()
		}
	}
	if count != 5 {
		t.Errorf("exptected to iterate 5, saw %d", count)
	}
	if string(firstKey) != "c" {
		t.Errorf("expected fist key to be c, got %s", firstKey)
	}
	if string(lastKey) != "g" {
		t.Errorf("expected lats key to be g, got %s", lastKey)
	}
	if err != FDB_RESULT_ITERATOR_FAIL {
		t.Errorf("expected %#v, got %#v", FDB_RESULT_ITERATOR_FAIL, err)
	}

}

func TestForestDBIteratorSeek(t *testing.T) {
	defer os.RemoveAll("test")

	dbfile, err := Open("test", nil)
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
	kvstore.SetKV([]byte("i"), []byte("vali"))
	kvstore.SetKV([]byte("j"), []byte("valj"))

	iter, err := kvstore.IteratorInit([]byte("c"), []byte("j"), ITR_NONE)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	doc, err := iter.Get()
	if err != nil {
		t.Fatal(err)
	}
	key := doc.Key()
	if string(key) != "c" {
		t.Fatalf("expected first key 'c', got %s", string(key))
	}

	// now seek to e (exists) should skip over d
	err = iter.Seek([]byte("e"), FDB_ITR_SEEK_HIGHER)
	if err != nil {
		t.Fatal(err)
	}
	doc, err = iter.Get()
	if err != nil {
		t.Fatal(err)
	}
	key = doc.Key()
	if string(key) != "e" {
		t.Fatalf("expected first key 'e', got %s", string(key))
	}

	// now seek to h (does not exist) should be on i
	err = iter.Seek([]byte("h"), FDB_ITR_SEEK_HIGHER)
	if err != nil {
		t.Fatal(err)
	}
	doc, err = iter.Get()
	if err != nil {
		t.Fatal(err)
	}
	key = doc.Key()
	if string(key) != "i" {
		t.Fatalf("expected first key 'i', got %s", string(key))
	}
}

func TestForestDBIteratorPrev(t *testing.T) {
	defer os.RemoveAll("test")

	dbfile, err := Open("test", nil)
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

	iter, err := kvstore.IteratorInit([]byte("a"), []byte("j"), ITR_NONE)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	err = iter.Seek([]byte("e"), FDB_ITR_SEEK_HIGHER)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := iter.Get()
	if err != nil {
		t.Fatal(err)
	}

	err = iter.Prev()
	if err != nil {
		t.Fatal(err)
	}
	doc, err = iter.Get()
	if err != nil {
		t.Fatal(err)
	}
	key := doc.Key()
	if string(key) != "d" {
		t.Fatalf("expected first key 'd', got %s", string(key))
	}

}

func TestForestDBIteratorOnSnapshot(t *testing.T) {
	defer os.RemoveAll("test")

	dbfile, err := Open("test", nil)
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

	dbfile.Commit(COMMIT_NORMAL)

	kvinfo, err := kvstore.Info()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := kvstore.SnapshotOpen(kvinfo.LastSeqNum())
	if err != nil {
		t.Fatal(err)
	}

	iter, err := snapshot.IteratorInit([]byte("c"), []byte("g"), ITR_NONE)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	doc, err := iter.Get()
	count := 0
	var firstKey, lastKey []byte
	for err == nil {
		count++
		if firstKey == nil {
			firstKey = doc.Key()
		}
		lastKey = doc.Key()
		err = iter.Next()
		if err == nil {
			doc, err = iter.Get()
		}
	}
	if count != 5 {
		t.Errorf("exptected to iterate 5, saw %d", count)
	}
	if string(firstKey) != "c" {
		t.Errorf("expected fist key to be c, got %s", firstKey)
	}
	if string(lastKey) != "g" {
		t.Errorf("expected lats key to be g, got %s", lastKey)
	}
	if err != FDB_RESULT_ITERATOR_FAIL {
		t.Errorf("expected %#v, got %#v", FDB_RESULT_ITERATOR_FAIL, err)
	}

}

func TestForestDBIteratorPreAlloc(t *testing.T) {
	defer os.RemoveAll("test")

	dbfile, err := Open("test", nil)
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

	iter, err := kvstore.IteratorInit([]byte("c"), []byte("g"), ITR_NONE)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	keybuf := make([]byte, 10)
	valbuf := make([]byte, 10)
	doc, err := NewDoc(keybuf, nil, valbuf)
	if err != nil {
		t.Fatal(err)
	}
	defer doc.Close()

	err = iter.GetPreAlloc(doc)
	count := 0
	var firstKey, lastKey []byte
	for err == nil {
		count++
		if firstKey == nil {
			firstKey = doc.Key()
		}
		lastKey = doc.Key()
		err = iter.Next()
		if err == nil {
			err = iter.GetPreAlloc(doc)
		}
	}
	if count != 5 {
		t.Errorf("exptected to iterate 5, saw %d", count)
	}
	if string(firstKey) != "c" {
		t.Errorf("expected fist key to be c, got %s", firstKey)
	}
	if string(lastKey) != "g" {
		t.Errorf("expected lats key to be g, got %s", lastKey)
	}
	if err != FDB_RESULT_ITERATOR_FAIL {
		t.Errorf("expected %#v, got %#v", FDB_RESULT_ITERATOR_FAIL, err)
	}

}
