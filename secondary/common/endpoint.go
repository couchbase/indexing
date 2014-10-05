package common

// RouterEndpointFactory will create a new endpoint instance for
// {topic, remote-address}
type RouterEndpointFactory func(
	topic, raddr string,
	settings map[string]interface{}) (RouterEndpoint, error)

// RouterEndpoint abstracts downstream for feed.
type RouterEndpoint interface {
	// Ping will check whether endpoint is active.
	Ping() bool

	// Send will post data to endpoint client.
	Send(data interface{}) error

	// GetStatistics to gather statistics information from endpoint.
	GetStatistics() map[string]interface{}

	// Close will shutdown this endpoint and release its resources.
	Close() error
}
