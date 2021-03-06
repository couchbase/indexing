// +build !community

package indexer

// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

import (
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/plasma"
)

var errStorageCorrupted = fmt.Errorf("Storage corrupted and unrecoverable")

func NewPlasmaSlice(storage_dir string, log_dir string, path string, sliceId SliceId, idxDefn common.IndexDefn,
	idxInstId common.IndexInstId, partitionId common.PartitionId,
	isPrimary bool, numPartitions int,
	sysconf common.Config, idxStats *IndexStats, indexerStats *IndexerStats, isNew bool) (*plasmaSlice, error) {
	return newPlasmaSlice(storage_dir, log_dir, path, sliceId,
		idxDefn, idxInstId, partitionId, isPrimary, numPartitions,
		sysconf, idxStats, indexerStats, isNew)
}

func DestroyPlasmaSlice(storageDir string, path string) error {
	return destroyPlasmaSlice(storageDir, path)
}

func ListPlasmaSlices() ([]string, error) {
	return listPlasmaSlices()
}

func BackupCorruptedPlasmaSlice(storageDir string, prefix string, rename func(string) (string, error), clean func(string)) error {
	return backupCorruptedPlasmaSlice(storageDir, prefix, rename, clean)
}

func RecoveryDone() {
	plasma.RecoveryDone()
}
