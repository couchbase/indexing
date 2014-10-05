// endpoint concurrency model:
//
//                  NewRouterEndpoint()
//                            |
//                            |
//                         (spawn)
//                            |
//                            |  (flushTimeout)
//        Ping() -----*----> run ----------> dataport-client ---> TCP
//                    |       ^
//        Send() -----*       | endpoint routine buffers messages,
//                    |       | batches them based on timeout and
//       Close() -----*       | message-count and periodically flushes
//                            | them out via dataport-client.
//                            |
//                            V
//                          buffers

package dataport

import "fmt"
import "time"
import "runtime/debug"

import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/transport"

// RouterEndpoint structure, per topic, to gather key-versions / mutations
// from one or more vbuckets and push them downstream to a
// specific node.
type RouterEndpoint struct {
	topic       string
	timestamp   int64   // immutable
	raddr       string  // immutable
	client      *Client // immutable
	bufferBlock bool
	// gen-server
	kvch  chan []interface{} // carries *c.KeyVersions
	reqch chan []interface{} // carries control commands
	finch chan bool
	// misc.
	logPrefix string
}

// NewRouterEndpoint instantiate a new RouterEndpoint
// routine and return its reference.
func NewRouterEndpoint(
	topic, raddr string,
	settings map[string]interface{}) (*RouterEndpoint, error) {

	// settings
	n, ok := settings["numConns"].(int)
	if !ok {
		n = 1
	}
	bufferBlock, ok := settings["noblock"].(bool)
	if !ok {
		bufferBlock = true
	}

	flags := transport.TransportFlag(0).SetProtobuf()
	client, err := NewClient(raddr, n, flags)
	if err != nil {
		return nil, err
	}
	endpoint := &RouterEndpoint{
		topic:       topic,
		raddr:       raddr,
		client:      client,
		bufferBlock: bufferBlock,
		kvch:        make(chan []interface{}, c.KeyVersionsChannelSize),
		reqch:       make(chan []interface{}, c.GenserverChannelSize),
		finch:       make(chan bool),
		timestamp:   time.Now().UnixNano(),
	}
	endpoint.logPrefix = fmt.Sprintf(
		"[%v->endpc(%v) %v]", topic, endpoint.timestamp, endpoint.raddr)

	go endpoint.run(endpoint.kvch, endpoint.reqch)
	c.Infof("%v started (with %v conns) ...\n", endpoint.logPrefix, n)
	return endpoint, nil
}

// commands
const (
	endpCmdPing byte = iota + 1
	endpCmdGetStatistics
	endpCmdClose
)

// Ping whether endpoint is active, synchronous call.
func (endpoint *RouterEndpoint) Ping() bool {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{endpCmdPing, respch}
	resp, err := c.FailsafeOp(endpoint.reqch, respch, cmd, endpoint.finch)
	if err != nil {
		return false
	}
	return resp[0].(bool)
}

// Send KeyVersions to other end, asynchronous call.
func (endpoint *RouterEndpoint) Send(data interface{}) error {
	cmd := []interface{}{data}
	return c.FailsafeOpAsync(endpoint.kvch, cmd, endpoint.finch)
}

// GetStatistics for this endpoint, synchronous call.
func (endpoint *RouterEndpoint) GetStatistics() map[string]interface{} {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{endpCmdGetStatistics, respch}
	resp, _ := c.FailsafeOp(endpoint.reqch, respch, cmd, endpoint.finch)
	return resp[0].(map[string]interface{})
}

// Close this endpoint.
func (endpoint *RouterEndpoint) Close() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{endpCmdClose, respch}
	resp, err := c.FailsafeOp(endpoint.reqch, respch, cmd, endpoint.finch)
	return c.OpError(err, resp, 0)
}

// run
func (endpoint *RouterEndpoint) run(
	kvch chan []interface{}, reqch chan []interface{}) {

	defer func() { // panic safe
		if r := recover(); r != nil {
			c.Errorf("%v run() crashed: %v\n", endpoint.logPrefix, r)
			c.StackTrace(string(debug.Stack()))
		}
		// close this endpoint
		endpoint.client.Close()
		close(endpoint.finch)
		c.Infof("%v ... stopped\n", endpoint.logPrefix)
	}()

	raddr, client := endpoint.raddr, endpoint.client

	flushTimeout := time.Tick(c.EndpointBufferTimeout * time.Millisecond)
	harakiri := time.After(c.EndpointHarakiriTimeout * time.Millisecond)
	buffers := newEndpointBuffers(raddr)

	stats, _ := c.NewStatistics(nil)
	mutationCount := float64(0)
	vbmapCount := float64(0)
	flushCount := float64(0)

	flushBuffers := func() error {
		l := len(buffers.vbs)
		if l == 0 {
			c.Tracef("%v empty keyversions\n", endpoint.logPrefix)
			return nil
		}
		c.Tracef("%v sent %v vbuckets to %q\n", endpoint.logPrefix, l, raddr)
		err := buffers.flushBuffers(client, endpoint.bufferBlock)
		if err != nil {
			c.Errorf("%v flushBuffers() %v", endpoint.logPrefix, err)
		}
		return err
	}

loop:
	for {
		select {
		case msg := <-kvch:
			data, ok := msg[0].(*c.DataportKeyVersions)
			if !ok {
				panic(fmt.Errorf("invalid data type %T\n", msg[0]))
			}

			kv := data.Kv
			buffers.addKeyVersions(data.Bucket, data.Vbno, data.Vbuuid, kv)
			c.Tracef("%v added %v keyversions <%v:%v:%v> to %q\n",
				endpoint.logPrefix, kv.Length(), data.Vbno, kv.Seqno,
				kv.Commands, buffers.raddr)
			mutationCount++
			// reload harakiri
			harakiri = time.After(c.EndpointHarakiriTimeout * time.Millisecond)

		case msg := <-reqch:
			switch msg[0].(byte) {
			case endpCmdPing:
				respch := msg[1].(chan []interface{})
				respch <- []interface{}{true}

			case endpCmdGetStatistics:
				respch := msg[1].(chan []interface{})
				stats.Set("mutations", mutationCount)
				stats.Set("vbmaps", vbmapCount)
				stats.Set("flushes", flushCount)
				respch <- []interface{}{map[string]interface{}(stats)}

			case endpCmdClose:
				respch := msg[1].(chan []interface{})
				flushBuffers()
				respch <- []interface{}{nil}
				break loop
			}

		case <-flushTimeout:
			if err := flushBuffers(); err != nil {
				break loop
			}
			flushCount++

		case <-harakiri:
			c.Infof("%v committed harakiri\n", endpoint.logPrefix)
			flushBuffers()
			break loop
		}
	}
}
