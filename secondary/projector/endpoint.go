// concurrency model:
//
//                       NewEndpoint()
//                            |
//                            |
//                         (spawn)
//                            |
//                            |  (flushTimeout)
//   SendVbmap() -----*----> run ----------> stream-client ---> TCP
//                    |       ^
//        Ping() -----*       |
//                    |       |
//        Close() ----*       |
//                    |       |
//        Send() -----*       |
//                            V
//                          buffers
//
// TODO:
// - endpoints can be differentiated between subscriber-endpoint or
//   coordinator-endpoint
//   * this is to optimize on payload for coordinator
//   * and to handle failure cases, especially when coordinator fails.

package projector

import (
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/indexer"
	"log"
	"time"
)

// Endpoint structure to gather key-versions / mutations from one or more
// vbuckets and push them downstream to a specific node.
type Endpoint struct {
	raddr  string                // immutable
	client *indexer.StreamClient // immutable
	coord  bool                  // whether this endpoint is coordinator
	// gen-server
	kvch      chan []interface{} // carries *c.KeyVersions
	reqch     chan []interface{} // carries control commands
	finch     chan bool
	logPrefix string
}

// NewEndpoint instanstiat a new Endpoint routine and return its reference.
func NewEndpoint(feed *Feed, raddr string, n int, coord bool) (*Endpoint, error) {
	flags := indexer.StreamTransportFlag(0).SetProtobuf() // TODO: configurable
	client, err := indexer.NewStreamClient(raddr, n, flags)
	if err != nil {
		return nil, err
	}
	endpoint := &Endpoint{
		raddr:  raddr,
		client: client,
		coord:  coord,
		kvch:   make(chan []interface{}, c.KeyVersionsChannelSize),
		reqch:  make(chan []interface{}, c.GenserverChannelSize),
		finch:  make(chan bool),
	}
	endpoint.logPrefix = endpoint.getLogPrefix(feed)

	go endpoint.run(endpoint.kvch, endpoint.reqch)
	log.Printf("%v, ... started (%v)\n", endpoint.logPrefix, n)
	return endpoint, nil
}

func (endpoint *Endpoint) getLogPrefix(feed *Feed) string {
	return fmt.Sprintf("endpoint %v:%v", feed.topic, endpoint.raddr)
}

func (endpoint *Endpoint) isCoord() bool {
	return endpoint.coord
}

// commands
const (
	endpCmdPing byte = iota + 1
	endpCmdClose
	endpCmdSendVbmap
)

// Ping whether endpoint is active, synchronous call.
func (endpoint *Endpoint) Ping() bool {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{endpCmdPing, respch}
	resp, err := c.FailsafeOp(endpoint.reqch, respch, cmd, endpoint.finch)
	if err != nil {
		return false
	}
	return resp[0].(bool)
}

// SendVbmap to other end, synchronous call
func (endpoint *Endpoint) SendVbmap(vbmap *c.VbConnectionMap) error {
	if vbmap == nil {
		return ErrorArgument
	}
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{endpCmdSendVbmap, vbmap, respch}
	resp, err := c.FailsafeOp(endpoint.reqch, respch, cmd, endpoint.finch)
	return c.OpError(err, resp, 0)
}

// Send KeyVersions to other end, asynchronous call.
func (endpoint *Endpoint) Send(bucket string, vbno uint16, vbuuid uint64, kv *c.KeyVersions) error {
	if kv == nil {
		return ErrorArgument
	}
	var respch chan []interface{}
	cmd := []interface{}{bucket, vbno, vbuuid, kv}
	_, err := c.FailsafeOp(endpoint.kvch, respch, cmd, endpoint.finch)
	return err
}

// Close this endpoint.
func (endpoint *Endpoint) Close() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{endpCmdClose, respch}
	resp, err := c.FailsafeOp(endpoint.reqch, respch, cmd, endpoint.finch)
	return c.OpError(err, resp, 0)
}

// run
func (endpoint *Endpoint) run(kvch chan []interface{}, reqch chan []interface{}) {
	defer func() { // panic safe
		if r := recover(); r != nil {
			log.Printf("Endpoint:run() crashed `%v`\n", endpoint.raddr)
			endpoint.doClose()
		}
	}()

	raddr := endpoint.raddr
	client := endpoint.client

	flushTimeout := time.After(c.EndpointBufferTimeout * time.Millisecond)
	buffers := newEndpointBuffers(endpoint, raddr)

loop:
	for {
		harakiri := time.After(c.EndpointHarakiriTimeout * time.Millisecond)
		select {
		case msg := <-kvch:
			bucket := msg[0].(string)
			vbno := msg[1].(uint16)
			vbuuid := msg[2].(uint64)
			kv := msg[3].(*c.KeyVersions)
			buffers.addKeyVersions(bucket, vbno, vbuuid, kv)

		case msg := <-reqch:
			switch msg[0].(byte) {
			case endpCmdPing:
				respch := msg[1].(chan []interface{})
				respch <- []interface{}{true}

			case endpCmdSendVbmap:
				vbmap := msg[1].(*c.VbConnectionMap)
				respch := msg[2].(chan []interface{})
				respch <- []interface{}{client.SendVbmap(vbmap)}

			case endpCmdClose:
				respch := msg[1].(chan []interface{})
				buffers.flushBuffers(client)
				endpoint.doClose()
				respch <- []interface{}{nil}
				break loop
			}

		case <-flushTimeout:
			flushTimeout = time.After(c.EndpointBufferTimeout * time.Millisecond)
			if err := buffers.flushBuffers(client); err != nil {
				endpoint.doClose()
				break loop
			}
			buffers = newEndpointBuffers(endpoint, raddr)

		case <-harakiri:
			buffers.flushBuffers(client)
			endpoint.doClose()
			log.Printf("%v, committed harakiri\n", endpoint.logPrefix)
			break loop
		}
	}
}

func (endpoint *Endpoint) doClose() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("%v, doClose() paniced: %v\n", endpoint.logPrefix, r)
		}
	}()

	endpoint.client.Close()
	close(endpoint.finch)
	log.Printf("%v, ... closed\n", endpoint.logPrefix)
}
