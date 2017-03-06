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

type SliceId uint64

type SliceStatus int16

const (
	//Slice is warming up(open db files etc), not ready for operations
	SLICE_STATUS_PREPARING SliceStatus = iota
	//Ready for operations
	SLICE_STATUS_ACTIVE
	//Marked for deletion
	SLICE_STATUS_TERMINATE
)

//Slice represents the unit of physical storage for index
type Slice interface {
	Id() SliceId
	Path() string
	Status() SliceStatus
	IndexInstId() common.IndexInstId
	IndexDefnId() common.IndexDefnId
	IsActive() bool
	IsDirty() bool

	SetActive(bool)
	SetStatus(SliceStatus)

	UpdateConfig(common.Config)

	IndexWriter
	GetReaderContext() IndexReaderContext
}
