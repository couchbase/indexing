// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"fmt"

	"github.com/couchbase/indexing/secondary/common"
)

type errCode int16

const (
	ERROR_PANIC errCode = iota

	//Slab Manager
	ERROR_SLAB_INIT
	ERROR_SLAB_BAD_ALLOC_REQUEST
	ERROR_SLAB_INTERNAL_ALLOC_ERROR
	ERROR_SLAB_MEM_LIMIT_EXCEED
	ERROR_SLAB_INTERNAL_ERROR

	//Stream Reader
	ERROR_STREAM_INIT
	ERROR_STREAM_READER_UNKNOWN_COMMAND
	ERROR_STREAM_READER_UNKNOWN_ERROR
	ERROR_STREAM_READER_PANIC
	ERROR_STREAM_READER_STREAM_SHUTDOWN

	//Mutation Manager
	ERROR_MUT_MGR_INTERNAL_ERROR
	ERROR_MUT_MGR_STREAM_ALREADY_OPEN
	ERROR_MUT_MGR_STREAM_ALREADY_CLOSED
	ERROR_MUT_MGR_UNKNOWN_COMMAND
	ERROR_MUT_MGR_UNCLEAN_SHUTDOWN
	ERROR_MUT_MGR_PANIC

	//Mutation Queue
	ERROR_MUTATION_QUEUE_INIT

	//Timekeeper
	ERROR_TK_UNKNOWN_STREAM

	//KVSender
	ERROR_KVSENDER_UNKNOWN_INDEX
	ERROR_KVSENDER_STREAM_ALREADY_OPEN
	ERROR_KVSENDER_STREAM_REQUEST_ERROR
	ERROR_KV_SENDER_UNKNOWN_STREAM
	ERROR_KV_SENDER_UNKNOWN_BUCKET
	ERROR_KVSENDER_STREAM_ALREADY_CLOSED

	//ScanCoordinator
	ERROR_SCAN_COORD_UNKNOWN_COMMAND
	ERROR_SCAN_COORD_INTERNAL_ERROR

	//INDEXER
	ERROR_INDEX_ALREADY_EXISTS
	ERROR_INDEXER_INTERNAL_ERROR
	ERROR_INDEX_BUILD_IN_PROGRESS
	ERROR_INDEX_DROP_IN_PROGRESS
	ERROR_INDEXER_UNKNOWN_INDEX
	ERROR_INDEXER_UNKNOWN_BUCKET
	ERROR_INDEXER_IN_RECOVERY
	ERROR_INDEXER_NOT_ACTIVE
	ERROR_INDEXER_REBALANCE_IN_PROGRESS
	ERROR_MAX_PARALLEL_COLLECTION_BUILDS
	ERROR_SHARD_REBALANCE_NOT_IN_PROGRESS
	ERROR_INDEXER_PAUSE_RESUME_IN_PROGRESS

	//STORAGE_MGR
	ERROR_STORAGE_MGR_ROLLBACK_FAIL
	ERROR_STORAGE_MGR_MERGE_SNAPSHOT_FAIL

	//CLUSTER_MGR_AGENT
	ERROR_CLUSTER_MGR_AGENT_INIT
	ERROR_CLUSTER_MGR_CREATE_FAIL
	ERROR_CLUSTER_MGR_DROP_FAIL

	ERROR_INDEX_MANAGER_PANIC
	ERROR_INDEX_MANAGER_CHANNEL_CLOSE

	ERROR_SCAN_COORD_QUERYPORT_FAIL
	ERROR_BUCKET_EPHEMERAL
	ERROR_BUCKET_EPHEMERAL_STD

	//metering throttling mgr
	ERROR_METERING_THROTTLING_UNKNOWN_COMMAND
)

type errSeverity int16

const (
	FATAL errSeverity = iota
	NORMAL
)

type errCategory int16

const (
	MESSAGING errCategory = iota
	STORAGE
	MUTATION_QUEUE
	TOPOLOGY
	STREAM_READER
	SLAB_MANAGER
	MUTATION_MANAGER
	TIMEKEEPER
	SCAN_COORD
	INDEXER
	STORAGE_MGR
	CLUSTER_MGR
	METERING_THROTTLING_MGR
)

type Error struct {
	code     errCode
	severity errSeverity
	category errCategory
	cause    error
	msg      string
}

func (e Error) String() string {
	return fmt.Sprintf("%v", e.cause)
}

func (e Error) convertError() common.IndexerErrCode {

	switch e.code {
	case ERROR_INDEX_ALREADY_EXISTS:
		return common.IndexAlreadyExist
	case ERROR_INDEXER_INTERNAL_ERROR:
		return common.TransientError
	case ERROR_INDEX_BUILD_IN_PROGRESS:
		return common.IndexBuildInProgress
	case ERROR_INDEX_DROP_IN_PROGRESS:
		return common.DropIndexInProgress
	case ERROR_INDEXER_UNKNOWN_INDEX:
		return common.IndexNotExist
	case ERROR_INDEXER_UNKNOWN_BUCKET:
		return common.InvalidBucket
	case ERROR_INDEXER_IN_RECOVERY:
		return common.IndexerInRecovery
	case ERROR_INDEXER_NOT_ACTIVE:
		return common.IndexerNotActive
	case ERROR_INDEXER_REBALANCE_IN_PROGRESS:
		return common.RebalanceInProgress
	case ERROR_INDEXER_PAUSE_RESUME_IN_PROGRESS:
		return common.PauseResumeInProgress
	case ERROR_BUCKET_EPHEMERAL:
		return common.BucketEphemeral
	case ERROR_MAX_PARALLEL_COLLECTION_BUILDS:
		return common.MaxParallelCollectionBuilds
	case ERROR_BUCKET_EPHEMERAL_STD:
		return common.BucketEphemeralStd
	}

	return common.TransientError
}
