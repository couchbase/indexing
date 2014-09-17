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

func TestMetadataRepoForIndexDefn(t *testing.T) {

	var addr = "localhost:9885"
	var leader = "localhost:9884"

	repo, err := manager.NewMetadataRepo(addr, leader)
	if err != nil {
		t.Fatal(err)
	}

	// clean up
	repo.DropIndexByName("metadata_repo_test")
	repo.DropIndexByName("metadata_repo_test_2")

	time.Sleep(time.Duration(1000) * time.Millisecond)

	// Add a new index definition : 100
	idxDefn := &common.IndexDefn{
		DefnId:          common.IndexDefnId(100),
		Name:            "metadata_repo_test",
		Using:           common.ForestDB,
		Bucket:          "Default",
		IsPrimary:       false,
		OnExprList:      []string{"Testing"},
		ExprType:        common.N1QL,
		PartitionScheme: common.HASH,
		PartitionKey:    "Testing"}

	if err := repo.CreateIndex(idxDefn); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Duration(1000) * time.Millisecond)

	// Get the index definition	by name
	idxDefn, err = repo.GetIndexDefnByName("metadata_repo_test")
	if err != nil {
		t.Fatal(err)
	}

	if idxDefn == nil {
		t.Fatal("Cannot find index definition")
	}

	// Delete the index definition by name
	if err := repo.DropIndexByName("metadata_repo_test"); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Duration(1000) * time.Millisecond)

	// Get the index definition	by name
	idxDefn, err = repo.GetIndexDefnByName("metadata_repo_test")

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
		OnExprList:      []string{"Testing"},
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

	// clean up
	repo.DropIndexByName("metadata_repo_test")
	repo.DropIndexByName("metadata_repo_test_2")

}
