// constants used by secondary indexing system.

package common

import (
	"errors"
	"time"
)

// error codes

// ErrorEmptyN1QLExpression
var ErrorEmptyN1QLExpression = errors.New("secondary.emptyN1QLExpression")

// ErrorUnexpectedPayload
var ErrorUnexpectedPayload = errors.New("secondary.unexpectedPayload")

// ErrorClosed
var ErrorClosed = errors.New("secondary.closed")

// ErrorNotMyVbucket
var ErrorNotMyVbucket = errors.New("secondary.notMyVbucket")

// ErrorInvalidRequest
var ErrorInvalidRequest = errors.New("secondary.invalidRequest")

// TODO: ideally we would like to have these constants configurable.
const (
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
	VbucketSyncTimeout = 500

	// EndpointBufferTimeout timeout, in milliseconds, is for endpoints to send
	// buffered key-versions to downstream.
	EndpointBufferTimeout = 1

	// TransmitBufferTimeout timeout, in milliseconds, is for endpoints to send
	// buffered key-versions to downstream.
	TransmitBufferTimeout = 1

	// MaxDataportPayload is maximum payload length, in bytes, for transporting
	// data from router to downstream client.
	MaxDataportPayload = 1000 * 1024

	// DataportReadDeadline timeout, in milliseconds, is timeout while reading
	// from socket.
	DataportReadDeadline = 4000

	// MaxQueryportPayload is maximum payload length, in bytes, for transporting
	// data from router to downstream client.
	MaxQueryportPayload = 1000 * 1024

	// QueryportReadDeadline timeout, in milliseconds, is timeout while reading
	// from socket.
	QueryportReadDeadline = 4000

	// QueryportWriteDeadline timeout, in milliseconds, is timeout while writing
	// from socket.
	QueryportWriteDeadline = 4000

	// QueryportConnPoolTimeout is default timeout for retrieving a connection
	// from the pool.
	QueryportConnPoolTimeout = time.Hour * 24 * 30

	// QueryportConnPoolAvailWaitTime is the amount of time to wait for an
	// existing connection from the pool before considering the creation of a
	// new one.
	QueryportConnPoolAvailWaitTime = time.Millisecond

	// ConnsPerEndpoint number of parallel connections per endpoint.
	ConnsPerEndpoint = 1

	// VbucketHarakiriTimeout timeout, in milliseconds, is timeout after which
	// vbucket-routine will commit harakiri.
	VbucketHarakiriTimeout = 10 * 1000

	// EndpointHarakiriTimeout timeout, in milliseconds, is timeout after which
	// endpoint-routine will commit harakiri.
	EndpointHarakiriTimeout = 10 * 1000

	// AdminportURLPrefix path prefix for adminport listening on http.
	AdminportURLPrefix = "/adminport/"

	// AdminportReadTimeout timeout, in milliseconds, is read timeout for
	// golib's http server.
	AdminportReadTimeout = 0

	// AdminportWriteTimeout timeout, in milliseconds, is write timeout for
	// golib's http server.
	AdminportWriteTimeout = 0

	// MaxIndexesPerBucket is maximum number of index supported per bucket.
	MaxIndexesPerBucket = 64

	// QueryportPageSize is number of index-entries that shall be returned as
	// single payload.
	QueryportPageSize = 100
)

// ProtobufDataPathMajorNum major version number for mutation data path.
var ProtobufDataPathMajorNum byte = 0

// ProtobufDataPathMinorNum minor version number for mutation data path.
var ProtobufDataPathMinorNum byte = 1

const UrlSep = "/"
