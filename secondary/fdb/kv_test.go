//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
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
	if err != RESULT_KEY_NOT_FOUND {
		t.Errorf("expected %v, got %v", RESULT_KEY_NOT_FOUND, err)
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
	if err != RESULT_KEY_NOT_FOUND {
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
