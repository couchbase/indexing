// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

//go:build community
// +build community

package indexer

import "github.com/couchbase/indexing/secondary/common"

func NewBhiveSlice(storage_dir string, log_dir string, path string, sliceId SliceId,
	idxDefn common.IndexDefn, idxInstId common.IndexInstId, partitionId common.PartitionId,
	numPartitions int, sysconf common.Config, idxStats *IndexStats, memQuota int64,
	isNew bool, isInitialBuild bool, numVBuckets int, replicaId int, shardIds []common.ShardId,
	cancelCh chan bool, codebookPath string, graphBuildDone bool) (Slice, error) {
	panic("B-Hive storage engine not supported in community edition")
}

// BackupCorruptedSlice_Bhive - placeholder for community edition
func BackupCorruptedSlice_Bhive(
	storageDir, prefix string,
	rename func(string) (string, error),
	clean func(string),
) error {
	return nil
}

// DestroySlice_Bhive - placeholder for community edition
func DestroySlice_Bhive(storageDir string, path string) error {
	return nil
}

// GetEmptyShardInfo_Bhive - placeholder for community edition
func GetEmptyShardInfo_Bhive() ([]common.ShardId, error) {
	return nil, nil
}

// DestroyShard_Bhive - placeholder for community edition
func DestroyShard_Bhive(shardId common.ShardId) error {
	return nil
}

// RecoveryDone_Bhive - placeholder for community edition
func RecoveryDone_Bhive() {}

// GetShardCompatVersion_Bhive - placeholder for community edition
func GetShardCompatVersion_Bhive() int {
	return -1
}
