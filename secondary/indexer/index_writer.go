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
	"time"
)

type StorageStatistics struct {
	DataSize          int64
	MemUsed           int64
	DiskSize          int64
	ExtraSnapDataSize int64

	GetBytes    int64
	InsertBytes int64
	DeleteBytes int64

	NeedUpgrade bool

	InternalData []string
}

type IndexWriter interface {

	//Persist a key/value pair
	Insert(key []byte, docid []byte, meta *MutationMeta) error

	//Delete a key/value pair by docId
	Delete(docid []byte, meta *MutationMeta) error

	// Create commited commited snapshot or inmemory snapshot
	NewSnapshot(*common.TsVbuuid, bool) (SnapshotInfo, error)

	// Get the list of commited snapshots
	GetSnapshots() ([]SnapshotInfo, error)

	// Create open snapshot handle
	OpenSnapshot(SnapshotInfo) (Snapshot, error)

	//Rollback to given snapshot
	Rollback(s SnapshotInfo) error

	//Rollback to initial state
	RollbackToZero() error

	// Statistics used for compaction trigger
	Statistics() (StorageStatistics, error)

	// Perform file compaction
	Compact(abortTime time.Time, minFrag int) error

	// Dealloc resources
	Close()

	// Reference counting operators
	IncrRef()
	DecrRef()

	//Destroy/Wipe the index completely
	Destroy()
}
