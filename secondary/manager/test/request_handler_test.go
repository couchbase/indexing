// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package test

import (
	"bytes"
	"encoding/json"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager"
	util "github.com/couchbase/indexing/secondary/manager/test/util"
	"net/http"
	"os"
	"testing"
	"time"
)

// For this test, use index definition id from 500 - 510

func TestRequestHandler(t *testing.T) {

	logging.SetLogLevel(logging.Trace)

	cfg := common.SystemConfig.SectionConfig("indexer", true /*trim*/)
	cfg.Set("storage_dir", common.ConfigValue{"./data/", "metadata file path", "./"})
	os.MkdirAll("./data/", os.ModePerm)

	logging.Infof("Start TestRequestHandler *********************************************************")

	var config = "./config.json"

	logging.Infof("********** Setup index manager")
	var msgAddr = "localhost:9884"
	var httpAddr = "localhost:9102"
	addrPrv := util.NewFakeAddressProvider(msgAddr, httpAddr)
	mgr, err := manager.NewIndexManagerInternal(addrPrv, nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	mgr.StartCoordinator(config)
	defer mgr.Close()
	time.Sleep(time.Duration(1000) * time.Millisecond)

	logging.Infof("********** Start HTTP Server")
	go func() {
		if err := http.ListenAndServe(":9102", nil); err != nil {
			t.Fatal("Fail to start HTTP server on :9102")
		}
	}()

	logging.Infof("********** Cleanup Old Test")
	cleanupRequestHandlerTest(mgr, t)
	time.Sleep(time.Duration(1000) * time.Millisecond)

	logging.Infof("********** Start running request handler test")
	createIndexRequest(t)
	dropIndexRequest(t)

	logging.Infof("********** Cleanup Test")
	cleanupRequestHandlerTest(mgr, t)
	mgr.CleanupTopology()
	mgr.CleanupStabilityTimestamp()
	time.Sleep(time.Duration(1000) * time.Millisecond)

	logging.Infof("Done TestRequestHandler. Tearing down *********************************************************")
	mgr.Close()
	time.Sleep(time.Duration(1000) * time.Millisecond)
}

// clean up
func cleanupRequestHandlerTest(mgr *manager.IndexManager, t *testing.T) {

	_, err := mgr.GetIndexDefnById(common.IndexDefnId(500))
	if err != nil {
		logging.Infof("RequestHandlerTest.cleanupRequestHandlerTest() :  cannot find index defn request_handler_test.  No cleanup ...")
	} else {
		logging.Infof("RequestHandlerTest.cleanupRequestHandlerTest() :  found index defn request_handler_test.  Cleaning up ...")

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

	logging.Infof("********** Start createIndexRequest")

	/*
		DefnId          IndexDefnId     `json:"defnId,omitempty"`
		Name            string          `json:"name,omitempty"`
		Using           IndexType       `json:"using,omitempty"`
		Bucket          string          `json:"bucket,omitempty"`
		IsPrimary       bool            `json:"isPrimary,omitempty"`
		SecExprs        []string        `json:"secExprs,omitempty"`
		ExprType        ExprType        `json:"exprType,omitempty"`
		PartitionScheme PartitionScheme `json:"partitionScheme,omitempty"`
		PartitionKey    string          `json:"partitionKey,omitempty"`
		WhereExpr       string          `json:"where,omitempty"`
		Deferred        bool            `json:"deferred,omitempty"`
		Nodes           []string        `json:"nodes,omitempty"`
	*/

	// Construct request body.
	info := common.IndexDefn{
		DefnId:          common.IndexDefnId(500),
		Name:            "request_handler_test",
		Using:           common.ForestDB,
		Bucket:          "Default",
		IsPrimary:       false,
		SecExprs:        []string{"Testing"},
		ExprType:        common.N1QL,
		WhereExpr:       "Testing",
		PartitionKey:    "Testing",
		PartitionScheme: common.SINGLE,
		Deferred:        false,
		Nodes:           []string{"localhost"},
	}

	req := manager.IndexRequest{Version: uint64(1), Type: manager.CREATE, Index: info}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	bodybuf := bytes.NewBuffer(body)
	resp, err := http.Post("http://localhost:9102/createIndex", "application/json", bodybuf)
	if err != nil {
		t.Fatal(err)
	}

	validateIndexResponse(resp, t)

	logging.Infof("********** Done createIndexRequest")
}

func dropIndexRequest(t *testing.T) {

	logging.Infof("********** Start dropIndexRequest")

	// Construct request body.
	info := common.IndexDefn{
		DefnId: common.IndexDefnId(500),
		Name:   "request_handler_test",
		Bucket: "Default",
	}

	req := manager.IndexRequest{Version: uint64(1), Type: manager.DROP, Index: info}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	bodybuf := bytes.NewBuffer(body)
	resp, err := http.Post("http://localhost:9102/dropIndex", "application/json", bodybuf)
	if err != nil {
		t.Fatal(err)
	}

	validateIndexResponse(resp, t)

	logging.Infof("********** Done dropIndexRequest")
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

	if resp.Status != manager.RESP_SUCCESS {
		t.Fatal("Fail to get SUCCESS response")
	}
}
