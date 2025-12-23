// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"testing"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/query/datastore"
)

// TestIkAttributesMatchSecExprAttr verifies that the IkAttributes constants in
// query/datastore match the SecExprAttr constants in indexing/secondary/common.
//
// These constants MUST remain in sync because we do direct type casting between
// them (e.g., c.SecExprAttr(key.Attributes) in queryport/n1ql/secondary_index.go).
//
// If this test fails, update the constants in one of the files to match:
//   - query/datastore/index.go (IkAttributes)
//   - indexing/secondary/common/index.go (SecExprAttr)
func TestIkAttributesMatchSecExprAttr(t *testing.T) {
	// IMPORTANT: When adding new constants to either IkAttributes or SecExprAttr,
	// you MUST add them to BOTH files and add a new entry to the tests slice below.
	//
	// Files to keep in sync:
	//   - query/datastore/index.go (IkAttributes)
	//   - indexing/secondary/common/index.go (SecExprAttr)

	tests := []struct {
		name        string
		ikAttr      datastore.IkAttributes
		secExprAttr c.SecExprAttr
	}{
		// Iota-based constants (bit flags)
		{"DESC", datastore.IK_DESC, c.SEC_EXPR_ATTR_DESC},
		{"MISSING", datastore.IK_MISSING, c.SEC_EXPR_ATTR_MISSING},
		{"DENSE_VECTOR", datastore.IK_DENSE_VECTOR, c.SEC_EXPR_ATTR_DENSE_VECTOR},
		{"SPARSE_VECTOR", datastore.IK_SPARSE_VECTOR, c.SEC_EXPR_ATTR_SPARSE_VECTOR},
		{"MULTI_VECTOR", datastore.IK_MULTI_VECTOR, c.SEC_EXPR_ATTR_MULTI_VECTOR},
		// Derived/alias constants
		{"VECTOR", datastore.IK_VECTOR, c.SEC_EXPR_ATTR_VECTOR},
		{"VECTORS", datastore.IK_VECTORS, c.SEC_EXPR_ATTR_VECTORS},
		{"NONE", datastore.IK_NONE, c.SEC_EXPR_ATTR_NONE},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if int(tt.ikAttr) != int(tt.secExprAttr) {
				t.Errorf("IK_%s (%d) != SEC_EXPR_ATTR_%s (%d) - constants must match for direct type casting",
					tt.name, tt.ikAttr, tt.name, tt.secExprAttr)
			}
		})
	}

}
