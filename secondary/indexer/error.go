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
	case ERROR_BUCKET_EPHEMERAL:
		return common.BucketEphemeral
	case ERROR_MAX_PARALLEL_COLLECTION_BUILDS:
		return common.MaxParallelCollectionBuilds
	}

	return common.TransientError
}
