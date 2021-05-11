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

func TestForestDBKVCrud(t *testing.T) {
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

	// get a non-existant key
	var val []byte
	val, err = kvstore.GetKV([]byte("doesnotexist"))
	if val != nil {
		t.Error("expected nil value")
	}
	if err != FDB_RESULT_KEY_NOT_FOUND {
		t.Errorf("expected %v, got %v", FDB_RESULT_KEY_NOT_FOUND, err)
	}

	// put a new key
	err = kvstore.SetKV([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Error(err)
	}

	// lookup that key
	val, err = kvstore.GetKV([]byte("key1"))
	if err != nil {
		t.Error(err)
	}
	if string(val) != "value1" {
		t.Errorf("expected value1, got %s", val)
	}

	// update it
	err = kvstore.SetKV([]byte("key1"), []byte("value1-updated"))
	if err != nil {
		t.Error(err)
	}

	// look it up again
	val, err = kvstore.GetKV([]byte("key1"))
	if err != nil {
		t.Error(err)
	}
	if string(val) != "value1-updated" {
		t.Errorf("expected value1-updated, got %s", val)
	}

	// delete it
	err = kvstore.DeleteKV([]byte("key1"))
	if err != nil {
		t.Error(err)
	}

	// look it up again
	val, err = kvstore.GetKV([]byte("key1"))
	if err != FDB_RESULT_KEY_NOT_FOUND {
		t.Error(err)
	}
	if val != nil {
		t.Error("expected nil value, got %#v", val)
	}

	// delete it again
	err = kvstore.DeleteKV([]byte("key1"))
	if err != nil {
		t.Error(err)
	}

	// dete non-existant key
	err = kvstore.DeleteKV([]byte("doesnotext"))
	if err != nil {
		t.Error(err)
	}
}
