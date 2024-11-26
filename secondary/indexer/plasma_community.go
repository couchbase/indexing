//go:build community
// +build community

package indexer

// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

import (
	"fmt"

	"github.com/couchbase/indexing/secondary/common"
)

var errStorageCorrupted = fmt.Errorf("Storage corrupted and unrecoverable")
var errStoragePathNotFound = fmt.Errorf("Storage path not found for recovery")
var errRecoveryCancelled = fmt.Errorf("storage recovery is cancelled")
var errCodebookPathNotFound = fmt.Errorf("Codebook path not found for recovery")
var errCodebookCorrupted = fmt.Errorf("Codebook corrupted and unrecoverable")

func NewPlasmaSlice(storage_dir string, log_dir string, path string, sliceId SliceId, idxDefn common.IndexDefn,
	idxInstId common.IndexInstId, partitionId common.PartitionId,
	isPrimary bool, numPartitions int, sysconf common.Config, idxStats *IndexStats,
	memQuota int64, isNew bool, isInitialBuild bool, meteringMgr *MeteringThrottlingMgr,
	numVBuckets, replicaId int, shardIds []common.ShardId,
	cancelCh chan bool, codebookPath string) (Slice, error) {
	panic("Plasma is only supported in Enterprise Edition")
}

func deleteFreeWriters(instId common.IndexInstId) {
	// do nothing
}

func DestroyPlasmaSlice(storageDir string, path string) error {
	// do nothing
	return nil
}

func ListPlasmaSlices() ([]string, error) {
	// do nothing
	return nil, nil
}

func BackupCorruptedPlasmaSlice(string, string, func(string) (string, error), func(string)) error {
	// do nothing
	return nil
}

func RecoveryDone() {
}

func GetShardCompactVersion() int {
	return 0
}

func GetEmptyShardInfo() ([]common.ShardId, error) {
	return nil, nil
}

func DestroyShard(shardId common.ShardId) error {
	return nil
}

func GetRPCRootDir() string {
	return ""
}
