// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package manager

import (
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"time"
)

///////////////////////////////////////////////////////
// Type Definition
///////////////////////////////////////////////////////

type IndexManager struct {
	repo        *MetadataRepo
	coordinator *Coordinator
}

///////////////////////////////////////////////////////
// public function
///////////////////////////////////////////////////////

//
// Create a new IndexManager
//
func NewIndexManager(repoHost string,
	leader string,
	config string) (mgr *IndexManager, err error) {

	mgr = new(IndexManager)
	mgr.repo, err = NewMetadataRepo(repoHost, leader)
	if err != nil {
		return nil, err
	}

	mgr.coordinator = NewCoordinator(mgr.repo)
	go mgr.coordinator.Run(config)

	return mgr, nil
}

//
// Clean up the IndexManager
//
func (m *IndexManager) Close() {
	m.repo.Close()
	m.coordinator.Terminate()
}

//
// Get an index definiton by name
//
func (m *IndexManager) GetIndexDefnByName(name string) (*common.IndexDefn, error) {
	return m.repo.GetIndexDefnByName(name)
}

//
// Get an index definiton by id
//
func (m *IndexManager) GetIndexDefnById(id common.IndexDefnId) (*common.IndexDefn, error) {
	return m.repo.GetIndexDefnById(id)
}

//
// Handle Create Index DDL.  This function will block until
// 1) The index defn is persisted durably in the dictionary
// 2) The index defn is applied locally to each "active" indexer
//    node.  An active node is a running node that is in the same
//    network partition as the leader.   A leader is always in
//    the majority partition.
//
// This function will return an error if the outcome of the
// request is not known (e.g. the node is partitioned
// from the network).  It may still mean that the request
// is able to go through (processed by some other nodes).
//
// A Index DDL can be processed by any node. If this node is a leader,
// then the DDL request will be processed by the leader.  If it is a
// follower, it will forward the request to the leader.
//
// This function will not be processed until the index manager
// is either a leader or follower. Therefore, if (1) the node is
// in the minority partition after network partition or (2) the leader
// dies, this node will unblock any in-flight request initiated
// by this node (by returning error).  The node will run leader
// election again. Until this node has became a leader or follower,
// it will not be able to handle another request.
//
// If this node is partitioned from its leader, it can still recieve
// updates from the dictionary if this node still connects to it.
//
func (m *IndexManager) HandleCreateIndexDDL(defn *common.IndexDefn) error {

	content, err := marshallIndexDefn(defn)
	if err != nil {
		return err
	}

	// TODO: Make request id a string
	id := uint64(time.Now().UnixNano())
	if !m.coordinator.NewRequest(id, uint32(OPCODE_ADD_IDX_DEFN), defn.Name, content) {
		// TODO: double check if it exists in the dictionary
		return fmt.Errorf("Fail to complete processing create index statement for index '%s'", defn.Name)
	}

	return nil
}

func (m *IndexManager) HandleDeleteIndexDDL(name string) error {

	// TODO: Make request id a string
	id := uint64(time.Now().UnixNano())
	if !m.coordinator.NewRequest(id, uint32(OPCODE_DEL_IDX_DEFN), name, nil) {
		// TODO: double check if it exists in the dictionary
		return fmt.Errorf("Fail to complete processing delete index statement for index '%s'", name)
	}

	return nil
}
