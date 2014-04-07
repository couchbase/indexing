// Other than mutation path and query path, most of the components in secondary
// index talk to each other via admin port. Admin port can also be used for
// administering and managing the cluster.
//
// An admin port is started as a server daemon and listens for request messages,
// where every request is serviced by sending back a response to the client.
// Request and Response messages are defined using protobuf and http is used as
// the carrier.

package adminport

// MessageMarshaller API abstracts the underlying messaging format. For instance,
// in case of protobuf defined structures, respective structure definition
// should implement following methiod receivers.
type MessageMarshaller interface {
	// Name of the message
	Name() string

	// Encode function marshal message to byte array.
	Encode() (data []byte, err error)

	// Decode function unmarshal byte array to message.
	Decode(data []byte) (err error)
}

// Request API for server application to handle incoming request.
type Request interface {
	// Get message from request packet.
	GetMessage() MessageMarshaller

	// Send a response message back to the client.
	Send(MessageMarshaller) error
}

// Server API for adminport
type Server interface {
	// Register a request message that will be valid for admin server
	Register(msg MessageMarshaller) error

	// Unregister a previously registered request message
	Unregister(msg MessageMarshaller) error

	// Start server routine and wait for incoming request, Register() and
	// Unregister() APIs cannot be called after starting the server.
	Start() error

	// Stop server routine, server routine will quite only after outstanding
	// requests are serviced.
	Stop()
}

// Client API for a remote adminport
type Client interface {
	// Request will post a `request` message to server, wait for response and
	// decode response into `response` argument. `response` argument must be
	// pointer to an object implementing `MessageMarshaller` interface.
	Request(request, response MessageMarshaller) (err error)
}
