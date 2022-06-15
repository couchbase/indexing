// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package manager

import (
	"github.com/couchbase/indexing/secondary/common"
	mc "github.com/couchbase/indexing/secondary/manager/common"
)

////////////////////////////////////////////////////////////////////////////////////////////////////
// Type Definitions
////////////////////////////////////////////////////////////////////////////////////////////////////

// LocalIndexMetadata is the metadata returned by getIndexStatus
// for all indexes on a single indexer node.
type LocalIndexMetadata struct {
	IndexerId        string             `json:"indexerId,omitempty"`
	NodeUUID         string             `json:"nodeUUID,omitempty"`
	StorageMode      string             `json:"storageMode,omitempty"`
	LocalSettings    map[string]string  `json:"localSettings,omitempty"`
	IndexTopologies  []IndexTopology    `json:"topologies,omitempty"`
	IndexDefinitions []common.IndexDefn `json:"definitions,omitempty"`

	// Pseudofields that should not be checksummed for ETags. These are stored in
	// requestHandlerCache but NOT in MetadataRepo.
	Timestamp        int64  `json:"timestamp,omitempty"`        // UnixNano meta repo retrieval time; not stored therein
	ETag             uint64 `json:"eTag,omitempty"`             // checksum (HTTP entity tag); 0 is HTTP_VAL_ETAG_INVALID
	ETagExpiry       int64  `json:"eTagExpiry,omitempty"`       // ETag expiration UnixNano time
	AllIndexesActive bool   `json:"allIndexesActive,omitempty"` // all indexes *included in this object* are active as described by the info contained in this object
}

// ClusterIndexMetadata represents the index metadata for the entire cluster.
type ClusterIndexMetadata struct {
	Metadata    []LocalIndexMetadata                           `json:"metadata,omitempty"`
	SchedTokens map[common.IndexDefnId]*mc.ScheduleCreateToken `json:"schedTokens,omitempty"`
}
