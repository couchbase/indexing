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

const INDEXER_45_VERSION = 1
const INDEXER_50_VERSION = 2
const INDEXER_55_VERSION = 3
const INDEXER_65_VERSION = 4
const INDEXER_70_VERSION = 5
const INDEXER_CUR_VERSION = INDEXER_65_VERSION

const DEFAULT_POOL = "default"
const DEFAULT_SCOPE = "_default"
const DEFAULT_COLLECTION = "_default"

const NON_PARTITION_ID = PartitionId(0)

var NULL = []byte("null")
