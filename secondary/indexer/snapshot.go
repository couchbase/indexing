// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

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
	Stats() map[string]interface{}
}
