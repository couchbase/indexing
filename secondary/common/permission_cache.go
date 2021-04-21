// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

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
