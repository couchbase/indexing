// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package common

import (
	"fmt"

	"github.com/couchbase/cbauth"
)

type sessionPermissionsCache struct {
	permissions map[string]bool
	creds       cbauth.Creds
}

func NewSessionPermissionsCache(cr cbauth.Creds) *sessionPermissionsCache {
	p := &sessionPermissionsCache{}
	p.permissions = make(map[string]bool)
	p.creds = cr
	return p
}

func (p *sessionPermissionsCache) checkPermissions(permissions []string) (bool, error) {

	allow := false
	err := error(nil)

	for _, permission := range permissions {
		allow, err = p.creds.IsAllowed(permission)
		if !allow || err != nil {
			break
		}
	}

	return allow, err
}

func (p *sessionPermissionsCache) CheckAndAddBucketLevelPermission(bucket, op string) bool {
	if bucketLevelPermission, ok := p.permissions[bucket]; ok {
		return bucketLevelPermission
	} else {
		permission := fmt.Sprintf("cluster.bucket[%s].n1ql.index!%s", bucket, op)
		p.permissions[bucket], _ = p.checkPermissions([]string{permission})
		return p.permissions[bucket]
	}
}

func (p *sessionPermissionsCache) CheckAndAddScopeLevelPermission(bucket, scope, op string) bool {
	scopeLevel := fmt.Sprintf("%s:%s", bucket, scope)
	if scopeLevelPermission, ok := p.permissions[scopeLevel]; ok {
		return scopeLevelPermission
	} else {
		permission := fmt.Sprintf("cluster.scope[%s].n1ql.index!%s", scopeLevel, op)
		p.permissions[scopeLevel], _ = p.checkPermissions([]string{permission})
		return p.permissions[scopeLevel]
	}
}

func (p *sessionPermissionsCache) CheckAndAddCollectionLevelPermission(bucket, scope, collection, op string) bool {
	collectionLevel := fmt.Sprintf("%s:%s:%s", bucket, scope, collection)
	if collectionLevelPermission, ok := p.permissions[collectionLevel]; ok {
		return collectionLevelPermission
	} else {
		permission := fmt.Sprintf("cluster.collection[%s].n1ql.index!%s", collectionLevel, op)
		p.permissions[collectionLevel], _ = p.checkPermissions([]string{permission})
		return p.permissions[collectionLevel]
	}
}

// isAllowed returns true iff a caller's (creds, op) allow access to IndexDefn (bucket, scope, collection).
func (p *sessionPermissionsCache) IsAllowed(bucket, scope, collection, op string) bool {

	if p.CheckAndAddBucketLevelPermission(bucket, op) {
		return true
	} else if p.CheckAndAddScopeLevelPermission(bucket, scope, op) {
		return true
	} else if p.CheckAndAddCollectionLevelPermission(bucket, scope, collection, op) {
		return true
	}
	return false
}
