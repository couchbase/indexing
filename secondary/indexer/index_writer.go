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
	"time"

	"github.com/couchbase/indexing/secondary/common"
)

// Fragementation is calculated based on DataSizeOnDisk and LogSpace stat
// a. DataSize is uncompressed lss_data_size i.e. it is lss_data_size * compression_ratio
// b. DataSizeOnDisk is lss_data_size
// b. LogSpace is the size of the fraction of the log that will be
//    considered during next iteration of cleaning
// c. DiskSize is lss_used_space + size of Checkpoint info. lss_used_space
//    is the size of the log on disk (It can include garbaged data marked
//    by log cleaner)
//
// In case of forestDB, DataSizeOnDisk is same as DataSize, LogSpace is same as DiskSize
type StorageStatistics struct {
	DataSize          int64
	DataSizeOnDisk    int64
	LogSpace          int64
	DiskSize          int64
	MemUsed           int64
	ExtraSnapDataSize int64

	GetBytes    int64
	InsertBytes int64
	DeleteBytes int64

	NeedUpgrade bool

	InternalData    []string
	InternalDataMap map[string]interface{}
}

type IndexWriter interface {

	//Persist a key/value pair
	Insert(key []byte, docid []byte, meta *MutationMeta) error

	//Delete a key/value pair by docId
	Delete(docid []byte, meta *MutationMeta) error

	// Create commited commited snapshot or inmemory snapshot
	NewSnapshot(*common.TsVbuuid, bool) (SnapshotInfo, error)

	// Notify flush is done
	FlushDone()

	// Get the list of commited snapshots
	GetSnapshots() ([]SnapshotInfo, error)

	// Create open snapshot handle
	OpenSnapshot(SnapshotInfo) (Snapshot, error)

	//Rollback to given snapshot
	Rollback(s SnapshotInfo) error

	//Rollback to initial state
	RollbackToZero() error

	//Return TS for last rollback operation
	LastRollbackTs() *common.TsVbuuid

	SetLastRollbackTs(ts *common.TsVbuuid)

	// Statistics used for compaction trigger
	Statistics(consumerFilter uint64) (StorageStatistics, error)

	// Prepare stats for efficient
	PrepareStats()

	// Perform file compaction
	Compact(abortTime time.Time, minFrag int) error

	// Dealloc resources
	Close()

	// Reference counting operators
	IncrRef()
	DecrRef()
	CheckAndIncrRef() bool

	//Destroy/Wipe the index completely
	Destroy()
}
