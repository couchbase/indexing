// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package client

import (
	"fmt"
	gometa "github.com/couchbase/gometa/server"
	"github.com/couchbase/indexing/secondary/common"
	"testing"
	"time"
)

// For this test, use Index Defn Id from 100 - 110
func TestMetadataProvider(t *testing.T) {

	common.LogEnable()
	common.SetLogLevel(common.LogLevelDebug)

	common.Infof("Start EmbeddedServer *********************************************************")

	var msgAddr = "localhost:9884"
	server, err := gometa.RunEmbeddedServer(msgAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Terminate()

	common.Infof("Cleanup Test *********************************************************")

	cleanupTest(server, t)

	common.Infof("Setup Initial Data *********************************************************")

	setupInitialData(server, t)

	common.Infof("Start Provider *********************************************************")

	var providerId = "TestMetadataProvider"
	provider, err := NewMetadataProvider(providerId)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	provider.WatchMetadata(msgAddr)

	// the gometa server is running in the same process.  So sleep to make sure that the server
	// has a chance to finish off initialization.
	time.Sleep(time.Duration(1000) * time.Millisecond)

	common.Infof("Verify Initial Data *********************************************************")

	if !lookup(provider, common.IndexDefnId(100)) {
		t.Fatal("Cannot find Index Defn 100 from MetadataProvider")
	}
	common.Infof("found Index Defn 100")

	if !lookup(provider, common.IndexDefnId(101)) {
		t.Fatal("Cannot find Index Defn 101 from MetadataProvider")
	}
	common.Infof("found Index Defn 101")

	common.Infof("Change Data *********************************************************")

	newDefnId, err := provider.CreateIndex("metadata_provider_test_102", "Default", common.ForestDB,
		common.N1QL, "Testing", "Testing", msgAddr, []string{"Testing"}, false)
	if err != nil {
		t.Fatal("Cannot create Index Defn 102 through MetadataProvider")
	}

	if err := provider.DropIndex(common.IndexDefnId(101), msgAddr); err != nil {
		t.Fatal("Cannot drop Index Defn 101 through MetadataProvider")
	}

	common.Infof("Verify Changed Data *********************************************************")

	if !lookup(provider, common.IndexDefnId(100)) {
		t.Fatal("Cannot find Index Defn 100 from MetadataProvider")
	}
	common.Infof("found Index Defn 100")

	if lookup(provider, common.IndexDefnId(101)) {
		t.Fatal("Found Deleted Index Defn 101 from MetadataProvider")
	}
	common.Infof("cannot found deleted Index Defn 101")

	if !lookup(provider, newDefnId) {
		t.Fatal(fmt.Sprintf("Cannot Found Index Defn %d from MetadataProvider", newDefnId))
	}
	common.Infof("Found Index Defn %d", newDefnId)

	common.Infof("Cleanup Test *********************************************************")

	cleanupTest(server, t)
}

func lookup(provider *MetadataProvider, id common.IndexDefnId) bool {

	metas := provider.ListIndex()

	found := false
	for _, meta := range metas {
		if meta.Definition.DefnId == id {
			found = true
			break
		}
	}

	return found
}

func setupInitialData(server *gometa.EmbeddedServer, t *testing.T) {

	// Add a new index definition : 100
	idxDefn := &common.IndexDefn{
		DefnId:          common.IndexDefnId(100),
		Name:            "metadata_provider_test_100",
		Using:           common.ForestDB,
		Bucket:          "Default",
		IsPrimary:       false,
		SecExprs:        []string{"Testing"},
		ExprType:        common.N1QL,
		PartitionScheme: common.HASH,
		PartitionKey:    "Testing"}

	value, err := marshallIndexDefn(idxDefn)
	if err != nil {
		t.Fatal(err)
	}

	server.SetValue("IndexDefinitionId/100", value)

	// Add a new index definition : 101
	idxDefn = &common.IndexDefn{
		DefnId:          common.IndexDefnId(101),
		Name:            "metadata_provider_test_101",
		Using:           common.ForestDB,
		Bucket:          "Default",
		IsPrimary:       false,
		SecExprs:        []string{"Testing"},
		ExprType:        common.N1QL,
		PartitionScheme: common.HASH,
		PartitionKey:    "Testing"}

	value, err = marshallIndexDefn(idxDefn)
	if err != nil {
		t.Fatal(err)
	}

	server.SetValue("IndexDefinitionId/101", value)
}

// clean up
func cleanupTest(server *gometa.EmbeddedServer, t *testing.T) {

	_, err := server.GetValue("IndexDefinitionId/100")
	if err != nil {
		common.Infof("cleanupTest() :  cannot find index defn 100.  No cleanup ...")
	} else {
		common.Infof("cleanupTest.cleanupTest() :  found index defn 100.  Cleaning up ...")

		server.DeleteValue("IndexDefinitionId/100")

		_, err := server.GetValue("IndexDefinitionId/100")
		if err == nil {
			common.Infof("cleanupTest() :  cannot cleanup index defn 100.  ...")
		}
	}

	_, err = server.GetValue("IndexDefinitionId/101")
	if err != nil {
		common.Infof("cleanupTest() :  cannot find index defn 101.  No cleanup ...")
	} else {
		common.Infof("cleanupTest.cleanupTest() :  found index defn 101.  Cleaning up ...")

		server.DeleteValue("IndexDefinitionId/101")

		_, err := server.GetValue("IndexDefinitionId/101")
		if err == nil {
			common.Infof("cleanupTest() :  cannot cleanup index defn 101.  ...")
		}
	}

	_, err = server.GetValue("IndexDefinitionId/102")
	if err != nil {
		common.Infof("cleanupTest() :  cannot find index defn 102.  No cleanup ...")
	} else {
		common.Infof("cleanupTest.cleanupTest() :  found index defn 102.  Cleaning up ...")

		server.DeleteValue("IndexDefinitionId/102")

		_, err := server.GetValue("IndexDefinitionId/102")
		if err == nil {
			common.Infof("cleanupTest() :  cannot cleanup index defn 102.  ...")
		}
	}
}
