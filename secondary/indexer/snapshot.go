// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"github.com/couchbase/indexing/secondary/common"
)

//Snapshot interface
type Snapshot interface {
	IndexReader

	Open() error
	Close() error
	IsOpen() bool

	Id() SliceId
	IndexInstId() common.IndexInstId
	IndexDefnId() common.IndexDefnId

	Timestamp() *common.TsVbuuid

	Info() SnapshotInfo
}

type SnapshotInfo interface {
	Timestamp() *common.TsVbuuid
	IsCommitted() bool
	IsOSOSnap() bool
	Stats() map[string]interface{}
}
