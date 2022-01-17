// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	couchbase "github.com/couchbase/indexing/secondary/dcp"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"
)

//
// Iternal version:
// Services like indexer can keep track of cluster version using ns_server
// cluster APIs. But those APIs can be used to get only the major version
// and the minor version of the release. The patch version is not a part
// of existing ns_server API.
//
// Indexer service can introduce minor fuctionalities in the patch releases
// given the severity and priority. These functionalities sometimes require
// cluster wide version check before they can be safely enabled.
//
// Internal version framework provides the ability to perform cluster version
// check for the patch releases as well.
//
// Notes:
// - The internal version is a string reprsenting the ns_server's version
//   string. For example, string "7.0.1" means, major verion 7, minor
//   version 0 and patch version 1.
// - Internal version for indexing service is hardcoded to the same value
//   as that of ns_server value.
//   -  This needs to change if ns_server naming convention changes in
//      the future.
// - Internal version needs to be updated only when new features or
//   functionalities (which require cluster wide version check) are being
//   added in a patch release.
//

type InternalVersion string

const localVersion = "7.0.2"

const MIN_VER_STD_GSI_EPHEMERAL = "7.0.2"

type InternalVersionJson struct {
	Version string `json:"version,omitempty"`
}

func (one InternalVersion) LessThan(other InternalVersion) bool {
	// TODO: Do we need format validation?

	return one < other
}

func (iv InternalVersion) String() string {
	return string(iv)
}

func GetInternalVersion() InternalVersion {

	return InternalVersion(localVersion)
}

func GetMarshalledInternalVersion() ([]byte, error) {
	iv := &InternalVersionJson{
		Version: localVersion,
	}

	return json.Marshal(&iv)
}

func UnmarshalInternalVersion(data []byte) (InternalVersion, error) {
	iv := &InternalVersionJson{}

	err := json.Unmarshal(data, iv)
	if err != nil {
		return InternalVersion(""), err
	}

	return InternalVersion(iv.Version), nil
}

func GetInternalClusterVersion(ninfo NodesInfoProvider) (InternalVersion, error) {

	ninfo.RLock()
	defer ninfo.RUnlock()

	//
	// Contact only the active indexer nodes. This ensures DDL availability.
	//
	indexers := ninfo.GetActiveIndexerNodes()
	versions := make([]InternalVersion, len(indexers))
	errors := make(map[string]error)

	var lck sync.Mutex
	var wg sync.WaitGroup

	getInternalNodeVersion := func(i int, node couchbase.Node) {
		defer wg.Done()

		nid, found := ninfo.GetNodeIdByUUID(node.NodeUUID)
		if !found {
			lck.Lock()
			defer lck.Unlock()

			err := fmt.Errorf("GetInternalClusterVersion nid for %v not found", node.NodeUUID)
			logging.Errorf("%v", err)
			errors[node.NodeUUID] = err
			return
		}

		addr, err := ninfo.GetServiceAddress(nid, INDEX_HTTP_SERVICE, true)
		if err != nil {
			lck.Lock()
			defer lck.Unlock()

			err := fmt.Errorf("GetInternalClusterVersion error in GetServiceAddress for %v", node.NodeUUID)
			logging.Errorf("%v", err)
			errors[node.NodeUUID] = err
			return
		}

		url := addr + "/getInternalVersion"
		params := &security.RequestParams{Timeout: 120 * time.Second}
		response, err := security.GetWithAuth(url, params)
		if err != nil {
			lck.Lock()
			defer lck.Unlock()

			err1 := fmt.Errorf("GetInternalClusterVersion error (%v) in GetWithAuth for %v", err, node.NodeUUID)
			logging.Errorf("%v", err1)
			errors[node.NodeUUID] = err1
			return
		}

		defer response.Body.Close()

		if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNotFound {
			lck.Lock()
			defer lck.Unlock()

			err := fmt.Errorf("GetInternalClusterVersion error %v for node %v", response.StatusCode, node.NodeUUID)
			logging.Errorf("%v", err)
			errors[node.NodeUUID] = err
			return
		}

		if response.StatusCode == http.StatusNotFound {
			lck.Lock()
			defer lck.Unlock()

			versions[i] = InternalVersion("")
			logging.Infof("GetInternalClusterVersion status code for node %v is StatusNotFound", node.NodeUUID)
			return
		}

		buf := new(bytes.Buffer)
		if _, err := buf.ReadFrom(response.Body); err != nil {
			lck.Lock()
			defer lck.Unlock()

			err1 := fmt.Errorf("GetInternalClusterVersion error %v in reading response from node %v", err, node.NodeUUID)
			logging.Errorf("%v", err1)
			errors[node.NodeUUID] = err1
			return
		}

		var ver InternalVersion
		if ver, err = UnmarshalInternalVersion(buf.Bytes()); err != nil {
			lck.Lock()
			defer lck.Unlock()

			err1 := fmt.Errorf("GetInternalClusterVersion error %v in unmarshalling response from node %v", err, node.NodeUUID)
			logging.Errorf("%v", err1)
			errors[node.NodeUUID] = err1
			return
		}

		logging.Debugf("GetInternalClusterVersion got version %v for node %v", ver, node.NodeUUID)

		lck.Lock()
		defer lck.Unlock()

		versions[i] = ver
	}

	for i, node := range indexers {
		wg.Add(1)

		go getInternalNodeVersion(i, node)
	}

	wg.Wait()

	if len(errors) != 0 {
		for _, err := range errors {
			return InternalVersion(""), err
		}
	}

	min := InternalVersion(localVersion)
	for _, ver := range versions {
		if ver.LessThan(min) {
			min = ver
		}
	}

	return min, nil
}
