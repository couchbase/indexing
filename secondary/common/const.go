// constants used by secondary indexing system.

package common

import (
	"fmt"
)

// error codes

// ErrorEmptyN1QLExpression, returned by N1QL Evaluator for secondary keys.
var ErrorEmptyN1QLExpression = fmt.Errorf("errorEmptyN1QLExpression")

// ErrorUnexpectedPayload, returned by mutation constructor.
var ErrorUnexpectedPayload = fmt.Errorf("errorUnexpectedPayload")

// ErrorClosed, returned by gen-server APIs.
var ErrorClosed = fmt.Errorf("errorClosed")

// ErrorNotMyVbucket, returned if vbucket is not found in VbConnectionMap.
var ErrorNotMyVbucket = fmt.Errorf("errorNotMyVbucket")

// ErrorInvalidRequest
var ErrorInvalidRequest = fmt.Errorf("errorInvalidRequest")

// TODO: ideally we would like to have these constants configurable.
const (
	// Debug boolean to enable debug mode
	Debug = true

	// MaxVbuckets is maximum number of vbuckets for any bucket in kv.
	MaxVbuckets = 1024

	// GenserverChannelSize is typical channel size for channels that carry
	// gen-server request.
	GenserverChannelSize = 64

	// MutationChannelSize is typical channel size for channels that carry
	// kv-mutations.
	MutationChannelSize = 10000

	// KeyVersionsChannelSize is typical channel size for channels that carry
	// projected key-versions.
	KeyVersionsChannelSize = 10000

	// VbucketSyncTimeout timeout, in milliseconds, is for sending Sync
	// messages for inactive vbuckets.
	VbucketSyncTimeout = 5

	// EndpointBufferTimeout timeout, in milliseconds, is for endpoints to send
	// buffered key-versions to downstream.
	EndpointBufferTimeout = 1

	// TransmitBufferTimeout timeout, in milliseconds, is for endpoints to send
	// buffered key-versions to downstream.
	TransmitBufferTimeout = 1

	// MaxStreamDataLen is maximum payload length, in bytes, for transporting
	// data from router to indexer.
	MaxStreamDataLen = 100 * 1024

	// StreamReadDeadline timeout, in milliseconds, is timeout while reading
	// from socket.
	StreamReadDeadline = 2000

	// ConnsPerEndpoint number of parallel connections per endpoint.
	ConnsPerEndpoint = 1

	// VbucketHarakiriTimeout timeout, in milliseconds, is timeout after which
	// vbucket-routine will commit harakiri.
	VbucketHarakiriTimeout = 10 * 1000

	// EndpointHarakiriTimeout timeout, in milliseconds, is timeout after which
	// endpoint-routine will commit harakiri.
	EndpointHarakiriTimeout = 10 * 1000

	// AdminportReadTimeout timeout, in milliseconds, is read timeout for
	// golib's http server.
	AdminportReadTimeout = 0

	// AdminportWriteTimeout timeout, in milliseconds, is write timeout for
	// golib's http server.
	AdminportWriteTimeout = 0

	// AdminportURLPrefix is prefix string for HTTP url.
	AdminportURLPrefix = "/"

	// MaxIndexesPerBucket is maximum number of index supported per bucket.
	MaxIndexesPerBucket = 64
)
