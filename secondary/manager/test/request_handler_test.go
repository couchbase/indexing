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
	"github.com/couchbaselabs/goprotobuf/proto"
	util "github.com/couchbase/indexing/secondary/manager/test/util"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
	"testing"
	"time"
	"bytes"
	"net/http"
	"encoding/json"
)

// For this test, use index definition id from 500 - 510

func TestRequestHandler(t *testing.T) {

	common.LogEnable()
	common.SetLogLevel(common.LogLevelDebug)

	common.Infof("Start TestRequestHandler *********************************************************")

	/*
	var requestAddr = "localhost:9885"
	var leaderAddr = "localhost:9884"
	*/
	var config = "./config.json"

	common.Infof("********** Setup index manager") 
	factory := new(util.TestDefaultClientFactory)
	env := new(util.TestDefaultClientEnv)
	admin := manager.NewProjectorAdmin(factory, env, nil)
	//mgr, err := manager.NewIndexManagerInternal(requestAddr, leaderAddr, config, admin)
	mgr, err := manager.NewIndexManagerInternal("localhost:9886", "localhost:" + manager.COORD_MAINT_STREAM_PORT, admin)
	if err != nil {
		t.Fatal(err)
	}
	mgr.StartCoordinator(config)
	defer mgr.Close()
	time.Sleep(time.Duration(1000) * time.Millisecond)

	common.Infof("********** Cleanup Old Test") 
	cleanupRequestHandlerTest(mgr, t) 
	time.Sleep(time.Duration(1000) * time.Millisecond)
	
	common.Infof("********** Start running request handler test") 
	createIndexRequest(t)
	getTopologyRequest(t)
	dropIndexRequest(t)
	
	common.Infof("********** Cleanup Test") 
	cleanupRequestHandlerTest(mgr, t) 
	mgr.CleanupTopology()
	mgr.CleanupStabilityTimestamp()
	time.Sleep(time.Duration(1000) * time.Millisecond)

	common.Infof("Done TestRequestHandler. Tearing down *********************************************************")
	mgr.Close()
	time.Sleep(time.Duration(1000) * time.Millisecond)
}

// clean up
func cleanupRequestHandlerTest(mgr *manager.IndexManager, t *testing.T) {

	_, err := mgr.GetIndexDefnById(common.IndexDefnId(500))
	if err != nil {
		common.Infof("RequestHandlerTest.cleanupRequestHandlerTest() :  cannot find index defn request_handler_test.  No cleanup ...")
	} else {
		common.Infof("RequestHandlerTest.cleanupRequestHandlerTest() :  found index defn request_handler_test.  Cleaning up ...")

		err = mgr.HandleDeleteIndexDDL(common.IndexDefnId(500))
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)

		// double check if we have really cleaned up
		_, err := mgr.GetIndexDefnById(common.IndexDefnId(500))
		if err == nil {
			t.Fatal("RequestHandlerTest.cleanupRequestHandlerTest(): Cannot clean up index defn request_handler_test")
		}
	}
}

func createIndexRequest(t *testing.T) {

	common.Infof("********** Start createIndexRequest") 
	
    // Construct request body.
    info := manager.IndexInfo{
		DefnID:    		 "500",
		Name:            "request_handler_test",
		Bucket:          "Default",
		Using:           common.ForestDB,
		IsPrimary:       false,
		SecExprs:        []string{"Testing"},
		Exprtype:        common.N1QL,
		PartnExpr:		 "Testing",
		WhereExpr:	     "Testing",
    }
    
    req := manager.IndexRequest{Version : uint64(1), Type: manager.CREATE, Index: info}
    body, err := json.Marshal(req)
    if err != nil {
    	t.Fatal(err)
    }
    
    bodybuf := bytes.NewBuffer(body)
    resp, err := http.Post("http://localhost" + manager.INDEX_DDL_HTTP_ADDR + "/createIndex", "application/json", bodybuf)
    if err != nil {
       	t.Fatal(err)
    }
        
    validateIndexResponse(resp, t)
    
	common.Infof("********** Done createIndexRequest") 
}

func dropIndexRequest(t *testing.T) {

	common.Infof("********** Start dropIndexRequest") 
	
    // Construct request body.
    info := manager.IndexInfo{
		DefnID:    		 "500",
		Name:            "request_handler_test",
		Bucket:          "Default",
    }
    
    req := manager.IndexRequest{Version : uint64(1), Type: manager.DROP, Index: info}
    body, err := json.Marshal(req)
    if err != nil {
    	t.Fatal(err)
    }
    
    bodybuf := bytes.NewBuffer(body)
    resp, err := http.Post("http://localhost" + manager.INDEX_DDL_HTTP_ADDR + "/dropIndex", "application/json", bodybuf)
    if err != nil {
       	t.Fatal(err)
    }
    
    validateIndexResponse(resp, t)
        
	common.Infof("********** Done dropIndexRequest") 
}

func getTopologyRequest(t *testing.T) {

	common.Infof("********** Start getTopologyRequest") 
	
    req := manager.TopologyRequest{Version : uint64(1), Type: manager.GET, Bucket: "Default"}
    body, err := json.Marshal(req)
    if err != nil {
    	t.Fatal(err)
    }
    
    bodybuf := bytes.NewBuffer(body)
    resp, err := http.Post("http://localhost" + manager.INDEX_DDL_HTTP_ADDR + "/getTopology", "application/json", bodybuf)
    if err != nil {
       	t.Fatal(err)
    }
   
   	found := false 
    insts := validateTopologyResponse(resp, t)
    for _, inst := range insts.GetInstances() {
    	if inst.GetIndexInstance().GetDefinition().GetDefnID() == uint64(500) {
    		found = true
    	}
    }
    
    if !found {
       	t.Fatal("Cannot find index definition 500 in topology")
    }
        
	common.Infof("********** Done getTopologyRequest") 
}

func validateIndexResponse(r *http.Response, t *testing.T) {

    defer r.Body.Close()
        
	resp := manager.IndexResponse{}
	buf := make([]byte, r.ContentLength)

	// Body will be non-null but can return EOF if being empty
	if n, err := r.Body.Read(buf); err != nil && int64(n) != r.ContentLength {
     	t.Fatal(err)
	}

	if err := json.Unmarshal(buf, &resp); err != nil {
     	t.Fatal(err)
	}

    if resp.Status != manager.RESP_SUCCESS  {
     	t.Fatal("Fail to get SUCCESS response")
    }
}

func validateTopologyResponse(r *http.Response, t *testing.T) *protobuf.Instances {

    defer r.Body.Close()
        
	resp := manager.TopologyResponse{}
	buf := make([]byte, r.ContentLength)

	// Body will be non-null but can return EOF if being empty
	if n, err := r.Body.Read(buf); err != nil && int64(n) != r.ContentLength {
     	t.Fatal(err)
	}

	if err := json.Unmarshal(buf, &resp); err != nil {
     	t.Fatal(err)
	}

    if resp.Status != manager.RESP_SUCCESS  {
     	t.Fatal("Fail to get SUCCESS response")
    }
    
   	insts := new(protobuf.Instances)
	if err := proto.Unmarshal(resp.Instances, insts); err != nil {
     	t.Fatal("Fail to unmarshall instances")
	} 
	
    if len(insts.GetInstances()) == 0 {
     	t.Fatal("No instances returned")
    }
    
	return insts
}

