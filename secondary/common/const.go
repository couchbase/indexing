// constants used by secondary indexing system.

package common

import (
	"errors"
)

const ERR_TRAINING string = "ErrTraining: "
const ERR_BUILD_AFTER_TRAINING string = "ErrInBuildAfterTraining: "

// error codes

// ErrorEmptyN1QLExpression
var ErrorEmptyN1QLExpression = errors.New("secondary.emptyN1QLExpression")

// ErrorUnexpectedPayload
var ErrorUnexpectedPayload = errors.New("secondary.unexpectedPayload")

// ErrorClosed
var ErrorClosed = errors.New("genServer.closed")

// ErrorAborted
var ErrorAborted = errors.New("genServer.aborted")

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

var ErrIndexNotFoundRebal = errors.New("Index not found (rebalance)") // magic string;
// do not use other than in lifecycle.go buildIndexesLifecycleMgr

// Index not ready
var ErrIndexNotReady = errors.New("Index not ready for serving queries")

// ErrClientCancel when query client cancels an ongoing scan request.
var ErrClientCancel = errors.New("Client requested cancel")

var ErrIndexerInBootstrap = errors.New("Indexer In Warmup State. Please retry the request later.")

var ErrMarshalFailed = errors.New("json.Marshal failed")
var ErrUnmarshalFailed = errors.New("json.Unmarshal failed")

var ErrAuthMissing = errors.New("Unauthenticated access. Missing authentication information.")

var ErrNumVbRange = errors.New("NumVbs out of valid range")

// List of errors leading to failure of index creation
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
var ErrIndexScopeLimitReached = errors.New("Limit for number of indexes that can be created per scope has been reached.")
var ErrBucketAlreadyExist = errors.New("Bucket Already Present")
var ErrUnInitializedClusterInfo = errors.New("uninitialized clusterInfo")
var ErrPlannerMaxResourceUsageLimit = errors.New("Max Node Resource Usage Limit Reached.")
var ErrPlannerConstraintViolation = errors.New("Planner Constraint Violation.")
var ErrIndexBucketLimitReached = errors.New("Limit for number of indexes that can be created per bucket has been reached.")
var ErrServerBusy = errors.New("Server is busy.")
var ErrDiskLimitReached = errors.New("Bucket's disk size limit has been reached.")
var ErrIndexInAsyncRecovery = errors.New("Index is in async recovery. Index drop will be attempted after recovery is complete")
var ErrIndexDeletedDuringRebal = errors.New("Fail to create index as index is already deleted. Skipping index creation during rebalance")
var ErrRebalanceOrCleanupPending = errors.New("Rebalance in progress or cleanup pending from previous rebalance.")
var ErrTransientError = errors.New("Encountered transient error")
var ErrPartitionsLimitReached = errors.New("Number of partitions for index can not be greater than 'indexer.settings.maxNumPartitions'. Please reduce num_partition or increase maxNumPartitions.")
var ErrInsufficientItemsForTraining = errors.New(ERR_TRAINING + "Number of items in the keyspace are less than the number of centroids required for training")
var ErrUnsupportedQuantisationScheme = errors.New(ERR_TRAINING + "Quantisation scheme is currently not supported")

var ErrSliceClosed = errors.New("Encountered slice operation after its closed")

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
	ErrIndexScopeLimitReached,
	ErrIndexBucketLimitReached,
	ErrDiskLimitReached,
	ErrPartitionsLimitReached,
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
const INDEXER_71_VERSION = 6
const INDEXER_72_VERSION = 7
const INDEXER_75_VERSION = 8
const INDEXER_76_VERSION = 9
const INDEXER_77_VERSION = 10

// Since there is a possibility of 7.7 version at the time of this patch,
// linear numbering scheme will not work for 8.0 version i.e. we cannot
// move indexer version from 9 to 10 as 7.7 should come before 8.0.
// Hence, using a large number like 800 can solve the problem so that
// 8.0 can be 800, 8.1 can be 801, 8.2 can be 802. This scheme
// assumes that the minor versions do not exceed 8.99 (or) 899.
// When 7.7 releases, we can assign indexer version as 707.
//
// Note: 801 would mean 8.1 version and not 8.0.1. ns_server does not
// capture sub-versions of minor version.
const INDEXER_80_VERSION = 800
const INDEXER_CUR_VERSION = INDEXER_77_VERSION

const INDEXER_PRIORITY = ServerPriority("7.7.0")

// ##### IMPORTANT ##### When updating the above, also update util.go func GetVersion.

const DEFAULT_POOL = "default"
const DEFAULT_SCOPE = "_default"
const DEFAULT_COLLECTION = "_default"
const DEFAULT_SCOPE_ID = "0"
const DEFAULT_COLLECTION_ID = "0"
const QUERY_COLLECTION = "_query"

const SYSTEM_SCOPE = "_system"

const NON_PARTITION_ID = PartitionId(0)

var NULL = []byte("null")

const MIN_VBUCKETS_ALLOWED = 1
const MAX_VBUCKETS_ALLOWED = 1024

// HTTP header fields
const HTTP_KEY_CONTENT_TYPE = "Content-Type"  // usually application/json
const HTTP_KEY_ETAG_REQUEST = "If-None-Match" // http.Request checksum field
const HTTP_KEY_ETAG_RESPONSE = "ETag"         // http.Response checksum field

// Magic values of HTTP header fields and related constants
const HTTP_VAL_APPLICATION_JSON = "application/json" // for HTTP_KEY_CONTENT_TYPE
const HTTP_VAL_ETAG_BASE = 16                        // base (hex) of string-form ETag values in HTTP headers
const HTTP_VAL_ETAG_INVALID = 0                      // ETag value of 0 is treated as invalid or missing

// Byte slices for HTTP error response bodies. We use these instead of e.g.
// http.StatusText(http.StatusUnauthorized) as those don't show the error number,
// and having them here also makes it easier for developers to know the contents.
var HTTP_STATUS_UNAUTHORIZED []byte
var HTTP_STATUS_FORBIDDEN []byte

func init() {
	HTTP_STATUS_UNAUTHORIZED = []byte("401 Unauthorized\n")
	HTTP_STATUS_FORBIDDEN = []byte("403 Forbidden\n")
}

// Audit event IDs
const AUDIT_UNAUTHORIZED = uint32(49152) // HTTP_STATUS_UNAUTHORIZED
const AUDIT_FORBIDDEN = uint32(49153)    // HTTP_STATUS_FORBIDDEN

// Ingress lockdown error
var ErrNoIngress = errors.New("AccessNoIngress")

type TaskType int

const (
	RebalanceTask TaskType = iota
	PauseResumeTask
)

const STAT_LOG_TS_FORMAT = "2006-01-02T15:04:05.000-07:00"

func IsVectorTrainingError(errStr string) bool {
	return len(errStr) > len(ERR_TRAINING) && errStr[0:len(ERR_TRAINING)] == ERR_TRAINING
}

func IsBuildErrAfterTraining(errStr string) bool {
	return len(errStr) > len(ERR_BUILD_AFTER_TRAINING) && errStr[0:len(ERR_BUILD_AFTER_TRAINING)] == ERR_BUILD_AFTER_TRAINING
}
