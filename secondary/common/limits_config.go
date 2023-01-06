// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package common

import (
	"github.com/couchbase/cbauth"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"
)

type LimitsCache struct{}

func NewLimitsCache() (*LimitsCache, error) {
	lcCache := &LimitsCache{}
	return lcCache, nil
}

//
// TODO:
// It is recommended to use ConfigRefreshCallback before calling
// GetLimitsConfig(). But each process can register only one
// ConfigRefreshCallback. So, directly calling GetLimitsConfig(),
// without a callback also does the trick.
// As per current implementation, GetLimitsConfig() is a local
// call and returns last cached value received from ns_server.
//

//
// This should return true only when limits are enforced
// and cluster version >= 7.1
//
func (lcCache *LimitsCache) EnforceLimits() (bool, error) {
	if security.IsToolsConfigUsed() {
		return false, nil
	}
	lc, err := cbauth.GetLimitsConfig()

	if err != nil {
		logging.Errorf("Error in EnforceLimits. error %v", err)
		return false, err
	}

	return lc.EnforceLimits, nil
}
