// @copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package indexer

import (
	"github.com/couchbase/cbauth/service"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/planner"
)

////////////////////////////////////////////////////////////////////////////////////////////////////
// ServerlessManager class
////////////////////////////////////////////////////////////////////////////////////////////////////

// ServerlessManager provides the implementation of the ns_server RPC interface
// ServerlessManager(defined in cbauth/service/interface.go).
type ServerlessManager struct {
	clusterAddr string
}

// NewServerlessManager is the constructor for the ServerlessManager class.
func NewServerlessManager(clusterAddr string) *ServerlessManager {
	m := &ServerlessManager{
		clusterAddr: clusterAddr,
	}
	return m
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//
//  service.ServerlessManager interface implementation
//  Interface defined in cbauth/service/interface.go
//
////////////////////////////////////////////////////////////////////////////////////////////////////

//GetDefragmentedUtilization computes the per-node utilization of each indexer node after running
//the rebalance algorithm and returns the stats in the format specified by service.DefragmentedUtilizationInfo.
//Following format is used for returned stats:
//{
//         "127.0.0.1:9001" : {
//             "memory_used_actual": 600000000,
//             "units_used_actual": 6000,
//             "num_tenants" :2
//         },
//         "127.0.0.1:9003" : {
//             "memory_used_actual": 300000000,
//             "units_used_actual": 3000,
//             "num_tenants" : 1
//         },
//}
func (m *ServerlessManager) GetDefragmentedUtilization() (*service.DefragmentedUtilizationInfo, error) {

	if c.IsServerlessDeployment() {
		defragStats, err := planner.GetDefragmentedUtilization(m.clusterAddr)
		retVal := service.DefragmentedUtilizationInfo(defragStats)
		return &retVal, err
	} else {
		return nil, nil
	}

}
