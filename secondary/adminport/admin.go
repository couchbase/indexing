// Other than mutation path and query path, most of the components in secondary
// index talk to each other via admin port. Admin port can also be used for
// collecting statistics, administering and managing cluster.
//
// An admin port is started as a server daemon and listens for request messages,
// where every request is serviced by sending back a response to the client.

package adminport

import (
	"fmt"
)

// errors codes

// ErrorRegisteringRequest shall be returned by adminport daemon
var ErrorRegisteringRequest = fmt.Errorf("errorRegisteringRequest")

// ErrorMessageUnknown shall returned by adminport daemon
var ErrorMessageUnknown = fmt.Errorf("errorMessageUnknown")

// MessageMarshaller API abstracts the underlying messaging format. For instance,
// in case of protobuf defined structures, respective structure definition
// should implement following method receivers.
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

	// Unregister a previously registered request message
	Unregister(msg MessageMarshaller) error

	// Start server routine and wait for incoming request, Register() and
	// Unregister() APIs cannot be called after starting the server.
	Start() error

	// Stop server routine. TODO: server routine shall quite only after
	// outstanding requests are serviced.
	Stop()
}

// Client API for a remote adminport
type Client interface {
	// Request shall post a `request` message to server, wait for response and
	// decode response into `response` argument. `response` argument must be a
	// pointer to an object implementing `MessageMarshaller` interface.
	Request(request, response MessageMarshaller) (err error)

	// RequestStat shall get ComponentStat for specified component-name and
	// return back in response.
	RequestStat(name string, response MessageMarshaller) (err error)

	// RequestStats is plural form of RequestStat to retrieve statistics for
	// all the components.
	RequestStats(response MessageMarshaller) (err error)
}
