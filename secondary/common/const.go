// constants used by secondary indexing system.

package common

import (
	"errors"
)

// error codes

// ErrorEmptyN1QLExpression
var ErrorEmptyN1QLExpression = errors.New("secondary.emptyN1QLExpression")

// ErrorUnexpectedPayload
var ErrorUnexpectedPayload = errors.New("secondary.unexpectedPayload")

// ErrorClosed
var ErrorClosed = errors.New("genServer.closed")

// ErrorChannelFull
var ErrorChannelFull = errors.New("secondary.channelFull")

// ErrorNotMyVbucket
var ErrorNotMyVbucket = errors.New("secondary.notMyVbucket")

// ErrorInvalidRequest
var ErrorInvalidRequest = errors.New("secondary.invalidRequest")

// ErrorNotFound
var ErrorNotFound = errors.New("secondary.notFound")

// ProtobufDataPathMajorNum major version number for mutation data path.
var ProtobufDataPathMajorNum byte // = 0

// ProtobufDataPathMinorNum minor version number for mutation data path.
var ProtobufDataPathMinorNum byte = 1

// ErrScanTimedOut from indexer
var ErrScanTimedOut = errors.New("Index scan timed out")

// Index not found
var ErrIndexNotFound = errors.New("Index not found")

// Index not ready
var ErrIndexNotReady = errors.New("Index not ready for serving queries")

// ErrClientCancel when query client cancels an ongoing scan request.
var ErrClientCancel = errors.New("Client requested cancel")

var ErrIndexerInBootstrap = errors.New("Indexer In Warmup State. Please retry the request later.")

//
// List of errors leading to failure of index creation
//
var ErrAnotherIndexCreation = errors.New("Create index or Alter replica cannot proceed due to another concurrent create index request.")
var ErrRebalanceRunning = errors.New("Create index or Alter replica cannot proceed due to rebalance in progress.")
var ErrNetworkPartition = errors.New("Create index or Alter replica cannot proceed due to network partition, node failover or indexer failure.")
var ErrDuplicateIndex = errors.New("Index already exist. Create index cannot proceed due to presence of duplicate index name.")
var ErrIndexAlreadyExists = errors.New("Index already exist.")
var ErrBucketNotFound = errors.New("Bucket Not Found")
var ErrScopeNotFound = errors.New("Scope Not Found")
var ErrCollectionNotFound = errors.New("Collection Not Found")
var ErrDuplicateCreateToken = errors.New("Index already exist. Duplicate index is already scheduled for creation.")
var ErrIndexerNotAvailable = errors.New("Fails to create index.  There is no available index service that can process this request at this time.")
var ErrNotEnoughIndexers = errors.New("Fails to create index.  There are not enough indexer nodes to create index with replica")
var ErrIndexerConnection = errors.New("Unable to connect to all indexer nodes")
var ErrBucketUUIDChanged = errors.New("Bucket UUID has changed. Bucket may have been dropped and recreated.")
var ErrScopeIdChanged = errors.New("ScopeId has changed. Scope may have been dropped and recreated.")
var ErrCollectionIdChanged = errors.New("CollectionId has changed. Collection may have been dropped and recreated.")

var NonRetryableErrorsInCreate = []error{
	ErrDuplicateIndex,
	ErrIndexAlreadyExists,
	ErrBucketNotFound,
	ErrScopeNotFound,
	ErrCollectionNotFound,
	ErrDuplicateCreateToken,
	ErrBucketUUIDChanged,
	ErrScopeIdChanged,
	ErrCollectionIdChanged,
}

var RetryableErrorsInCreate = []error{
	ErrAnotherIndexCreation,
	ErrRebalanceRunning,
	ErrNetworkPartition,
	ErrIndexerNotAvailable,
	ErrNotEnoughIndexers,
	ErrIndexerConnection,
}

var KeyspaceDeletedErrorsInCreate = []error{
	ErrBucketNotFound,
	ErrScopeNotFound,
	ErrCollectionNotFound,
	ErrBucketUUIDChanged,
	ErrScopeIdChanged,
	ErrCollectionIdChanged,
}

const INDEXER_45_VERSION = 1
const INDEXER_50_VERSION = 2
const INDEXER_55_VERSION = 3
const INDEXER_65_VERSION = 4
const INDEXER_70_VERSION = 5
const INDEXER_CUR_VERSION = INDEXER_70_VERSION

const DEFAULT_POOL = "default"
const DEFAULT_SCOPE = "_default"
const DEFAULT_COLLECTION = "_default"
const DEFAULT_SCOPE_ID = "0"
const DEFAULT_COLLECTION_ID = "0"

const NON_PARTITION_ID = PartitionId(0)

var NULL = []byte("null")
