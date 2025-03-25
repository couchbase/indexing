//go:build !community
// +build !community

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
	"github.com/couchbase/plasma"
)

var (
	errStorageCorrupted     = fmt.Errorf("Storage corrupted and unrecoverable")
	errStoragePathNotFound  = fmt.Errorf("Storage path not found for recovery")
	errRecoveryCancelled    = fmt.Errorf("storage recovery is cancelled")
	errCodebookPathNotFound = fmt.Errorf("Codebook path not found for recovery")
	errCodebookCorrupted    = fmt.Errorf("Codebook corrupted and unrecoverable")
)

func NewPlasmaSlice(storage_dir string, log_dir string, path string, sliceId SliceId, idxDefn common.IndexDefn,
	idxInstId common.IndexInstId, partitionId common.PartitionId,
	isPrimary bool, numPartitions int, sysconf common.Config, idxStats *IndexStats,
	memQuota int64, isNew bool, isInitialBuild bool, meteringMgr *MeteringThrottlingMgr,
	numVBuckets, replicaId int, shardIds []common.ShardId,
	cancelCh chan bool, codebookPath string) (*plasmaSlice, error) {

	return newPlasmaSlice(storage_dir, log_dir, path, sliceId,
		idxDefn, idxInstId, partitionId, isPrimary, numPartitions,
		sysconf, idxStats, memQuota, isNew, isInitialBuild, meteringMgr,
		numVBuckets, replicaId, shardIds, cancelCh, codebookPath)
}

// DestroySlice_Plasma - Destroy a plasma slice which cannot be initialised (usually due to
// corruption)
func DestroySlice_Plasma(storageDir string, path string) error {
	return destroySlice_Plasma(storageDir, path)
}

func ListPlasmaSlices() ([]string, error) {
	return listPlasmaSlices()
}

func BackupCorruptedSlice_Plasma(storageDir string, prefix string, rename func(string) (string, error), clean func(string)) error {
	return backupCorruptedSlice_Plasma(storageDir, prefix, rename, clean)
}

func RecoveryDone_Plasma() {
	plasma.RecoveryDone()
}

// GetShardCompatVersion_Plasma - Get the compatibility version of plasma shards changing them across
// versions will prevent older versions of indexers to use DCP rebalance over file based rebalance
func GetShardCompatVersion_Plasma() int {
	if common.IsServerlessDeployment() {
		return plasma.ShardCompatVersionServerless
	}
	return plasma.ShardCompatVersion
}

// GetEmptyShardInfo_Plasma - Get the list of empty plasma shards on recovery
func GetEmptyShardInfo_Plasma() ([]common.ShardId, error) {
	plasmaShards, err := plasma.GetCurrentEmptyShardsInfo()
	var gsiShards []common.ShardId
	for _, shard := range plasmaShards {
		gsiShards = append(gsiShards, common.ShardId(shard))
	}
	return gsiShards, err
}

// DestroyShard_Plasma - Destroy empty plasma shards
func DestroyShard_Plasma(shardId common.ShardId) error {
	return plasma.DestroyShardID(plasma.ShardId(shardId))
}

func GetRPCRootDir() string {
	return plasma.RPCRootDir
}
