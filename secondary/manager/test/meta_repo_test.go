// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package test

import (
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/manager"
	"testing"
	"time"
)

// For this test, use index definition from 100 - 110

func TestMetadataRepoForIndexDefn(t *testing.T) {

	common.LogEnable()
	common.SetLogLevel(common.LogLevelDebug)

	common.Infof("Start TestMetadataRepo *********************************************************")

	var addr = "localhost:9885"
	var leader = "localhost:9884"

	repo, err := manager.NewMetadataRepo(addr, leader, "./config.json", nil)
	if err != nil {
		t.Fatal(err)
	}

	// clean up
	repo.DropIndexByName("Default", "metadata_repo_test")
	repo.DropIndexByName("Default", "metadata_repo_test_2")
	repo.DropIndexByName("Default", "metadata_repo_test_3")
	repo.DropIndexByName("Default", "metadata_repo_test_4")

	time.Sleep(time.Duration(1000) * time.Millisecond)

	// Add a new index definition : 100
	idxDefn := &common.IndexDefn{
		DefnId:          common.IndexDefnId(100),
		Name:            "metadata_repo_test",
		Using:           common.ForestDB,
		Bucket:          "Default",
		IsPrimary:       false,
		SecExprs:        []string{"Testing"},
		ExprType:        common.N1QL,
		PartitionScheme: common.HASH,
		PartitionKey:    "Testing"}

	if err := repo.CreateIndex(idxDefn); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Duration(1000) * time.Millisecond)

	// Get the index definition	by name
	idxDefn, err = repo.GetIndexDefnByName("Default", "metadata_repo_test")
	if err != nil {
		t.Fatal(err)
	}

	if idxDefn == nil {
		t.Fatal("Cannot find index definition")
	}

	// Delete the index definition by name
	if err := repo.DropIndexByName("Default", "metadata_repo_test"); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Duration(1000) * time.Millisecond)

	// Get the index definition	by name
	idxDefn, err = repo.GetIndexDefnByName("Default", "metadata_repo_test")

	if idxDefn != nil {
		t.Fatal("Find deleted index definition")
	}

	// Add a new index definition : 101
	idxDefn = &common.IndexDefn{
		DefnId:          common.IndexDefnId(101),
		Name:            "metadata_repo_test_2",
		Using:           common.ForestDB,
		Bucket:          "Default",
		IsPrimary:       false,
		SecExprs:        []string{"Testing"},
		ExprType:        common.N1QL,
		PartitionScheme: common.HASH,
		PartitionKey:    "Testing"}

	if err := repo.CreateIndex(idxDefn); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Duration(1000) * time.Millisecond)

	// Get the index definition by Id
	idxDefn, err = repo.GetIndexDefnById(common.IndexDefnId(101))
	if err != nil {
		t.Fatal(err)
	}

	if idxDefn == nil {
		t.Fatal("Cannot find index definition")
	}

	// Delete the index definition by Id
	if err := repo.DropIndexById(common.IndexDefnId(101)); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Duration(1000) * time.Millisecond)

	// Get the index definition by Id
	idxDefn, err = repo.GetIndexDefnById(common.IndexDefnId(101))

	if idxDefn != nil {
		t.Fatal("Find deleted index definition")
	}

	// Add a new index definition : 102
	idxDefn = &common.IndexDefn{
		DefnId:          common.IndexDefnId(102),
		Name:            "metadata_repo_test_3",
		Using:           common.ForestDB,
		Bucket:          "Default",
		IsPrimary:       false,
		SecExprs:        []string{"Testing"},
		ExprType:        common.N1QL,
		PartitionScheme: common.HASH,
		PartitionKey:    "Testing"}

	if err = repo.CreateIndex(idxDefn); err != nil {
		t.Fatal(err)
	}

	// Add a new index definition : 103
	idxDefn = &common.IndexDefn{
		DefnId:          common.IndexDefnId(103),
		Name:            "metadata_repo_test_4",
		Using:           common.ForestDB,
		Bucket:          "Default",
		IsPrimary:       false,
		SecExprs:        []string{"Testing"},
		ExprType:        common.N1QL,
		PartitionScheme: common.HASH,
		PartitionKey:    "Testing"}

	if err = repo.CreateIndex(idxDefn); err != nil {
		t.Fatal(err)
	}

	/*

	   // Test the iterator
	   iter, err := repo.NewIterator()
	   if err != nil {
	       t.Fatal("Fail to get the iterator")
	   }

	   found := false
	   for !found {
	       key, _, err := iter.Next()
	       if err != nil {
	           common.Infof("error during iteration %s", err.Error())
	           break
	       }

	       common.Infof("key during iteration %s", key)
	       if key == "metadata_repo_test_3" {
	           found = true
	       }
	   }

	   if !found {
	       t.Fatal("Cannot find index defn 'metadata_repo_test_3' in iterator")
	   }

	*/
	time.Sleep(time.Duration(1000) * time.Millisecond)

	common.Infof("Stop TestMetadataRepo. Tearing down *********************************************************")

	// clean up
	repo.DropIndexByName("Default", "metadata_repo_test")
	repo.DropIndexByName("Default", "metadata_repo_test_2")
	repo.DropIndexByName("Default", "metadata_repo_test_3")
	repo.DropIndexByName("Default", "metadata_repo_test_4")

	time.Sleep(time.Duration(1000) * time.Millisecond)
}
