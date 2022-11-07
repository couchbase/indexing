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
)

////////////////////////////////////////////////////////////////////////////////////////////////////
// ServerlessManager class
////////////////////////////////////////////////////////////////////////////////////////////////////

// ServerlessManager provides the implementation of the ns_server RPC interface
// ServerlessManager(defined in cbauth/service/interface.go).
type ServerlessManager struct {
	httpAddr string
}

// NewServerlessManager is the constructor for the ServerlessManager class.
// httpAddr gives the host:port of the local node for Index Service HTTP calls.
func NewServerlessManager(httpAddr string) *ServerlessManager {
	m := &ServerlessManager{
		httpAddr: httpAddr,
	}
	return m
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//
//  service.ServerlessManager interface implementation
//  Interface defined in cbauth/service/interface.go
//
////////////////////////////////////////////////////////////////////////////////////////////////////

func (m *ServerlessManager) GetDefragmentedUtilization() (*service.DefragmentedUtilizationInfo, error) {
	return nil, nil

}
