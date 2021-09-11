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
const INDEXER_71_VERSION = 6
const INDEXER_CUR_VERSION = INDEXER_71_VERSION

// ##### IMPORTANT ##### When updating the above, also update util.go func GetVersion.

const DEFAULT_POOL = "default"
const DEFAULT_SCOPE = "_default"
const DEFAULT_COLLECTION = "_default"
const DEFAULT_SCOPE_ID = "0"
const DEFAULT_COLLECTION_ID = "0"

const NON_PARTITION_ID = PartitionId(0)

var NULL = []byte("null")

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
