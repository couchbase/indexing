// Package adminport provides admin-port client/server library that can be
// used by system components to talk with each other. It is based on
// request/response protocol, by default used http for transport and protobuf,
// JSON for payload. Admin port can typically be used for collecting
// statistics, administering and managing cluster.
package common

import (
	"errors"

	c "github.com/couchbase/indexing/secondary/common"
)

// errors codes

// ErrorRegisteringRequest
var ErrorRegisteringRequest = errors.New("adminport.registeringRequest")

// ErrorMessageUnknown
var ErrorMessageUnknown = errors.New("adminport.unknownMessage")

// ErrorPathNotFound
var ErrorPathNotFound = errors.New("adminport.pathNotFound")

// ErrorRequest
var ErrorRequest = errors.New("adminport.request")

// ErrorServerStarted
var ErrorServerStarted = errors.New("adminport.serverStarted")

// ErrorDecodeRequest
var ErrorDecodeRequest = errors.New("adminport.decodeRequest")

// ErrorEncodeResponse
var ErrorEncodeResponse = errors.New("adminport.encodeResponse")

// ErrorDecodeResponse
var ErrorDecodeResponse = errors.New("adminport.decodeResponse")

// ErrorInternal
var ErrorInternal = errors.New("adminport.internal")

// MessageMarshaller APIs message format.
type MessageMarshaller interface {
	// Name of the message
	Name() string

	// Content type to be used by the transport layer.
	ContentType() string

	// Encode function shall marshal message to byte array.
	Encode() (data []byte, err error)

	// Decode function shall unmarshal byte array back to message.
	Decode(data []byte) (err error)
}

// Request API for server application to handle incoming request.
type Request interface {
	// Get message from request packet.
	GetMessage() MessageMarshaller

	// Send a response message back to the client.
	Send(MessageMarshaller) error

	// Send error back to the client.
	SendError(error) error
}

// Server API for adminport
type Server interface {
	// Register a request message that shall be supported by adminport-server
	Register(msg MessageMarshaller) error

	// RegisterHandler a request message that shall be supported by
	// adminport-server
	RegisterHTTPHandler(pattern string, handler interface{}) error

	// Unregister a previously registered request message
	Unregister(msg MessageMarshaller) error

	// Start server routine and wait for incoming request, Register() and
	// Unregister() APIs cannot be called after starting the server.
	Start() error

	// GetStatistics returns server statistics.
	GetStatistics() c.Statistics

	// Sets closeReqCh state to REQCH_CLOSE which enables server to close its request channel
	CloseReqch()

	// Stop server routine.
	Stop()
}

// Client API for a remote adminport
type Client interface {
	// Request shall post a `request` message to server, wait for response and
	// decode response into `response` argument. `response` argument must be a
	// pointer to an object implementing `MessageMarshaller` interface.
	Request(request, response MessageMarshaller) (err error)
}
