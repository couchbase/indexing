// endpoint concurrency model:
//
//                  NewRouterEndpoint()
//                            |
//                            |
//                         (spawn)
//                            |
//                            |  (flushTimeout || > bufferSize)
//        Ping() -----*----> run -------------------------------> TCP
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
import "net"
import "time"

import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/transport"
import "github.com/couchbase/indexing/secondary/logging"

// RouterEndpoint structure, per topic, to gather key-versions / mutations
// from one or more vbuckets and push them downstream to a
// specific node.
type RouterEndpoint struct {
	topic     string
	timestamp int64  // immutable
	raddr     string // immutable
	// config params
	logPrefix string
	keyChSize int // channel size for key-versions
	// live update is possible
	block      bool          // should endpoint block when remote is slow
	bufferSize int           // size of buffer to wait till flush
	bufferTm   time.Duration // timeout to flush endpoint-buffer
	harakiriTm time.Duration // timeout after which endpoint commits harakiri
	// gen-server
	ch    chan []interface{} // carries control commands
	finch chan bool
	// downstream
	pkt  *transport.TransportPacket
	conn net.Conn
}

// NewRouterEndpoint instantiate a new RouterEndpoint
// routine and return its reference.
func NewRouterEndpoint(
	cluster, topic, raddr string, maxvbs int,
	config c.Config) (*RouterEndpoint, error) {

	conn, err := net.Dial("tcp", raddr)
	if err != nil {
		return nil, err
	}

	endpoint := &RouterEndpoint{
		topic:      topic,
		raddr:      raddr,
		finch:      make(chan bool),
		timestamp:  time.Now().UnixNano(),
		keyChSize:  config["keyChanSize"].Int(),
		block:      config["remoteBlock"].Bool(),
		bufferSize: config["bufferSize"].Int(),
		bufferTm:   time.Duration(config["bufferTimeout"].Int()),
		harakiriTm: time.Duration(config["harakiriTimeout"].Int()),
	}
	endpoint.ch = make(chan []interface{}, endpoint.keyChSize)
	endpoint.conn = conn
	// TODO: add configuration params for transport flags.
	flags := transport.TransportFlag(0).SetProtobuf()
	maxPayload := config["maxPayload"].Int()
	endpoint.pkt = transport.NewTransportPacket(maxPayload, flags)
	endpoint.pkt.SetEncoder(transport.EncodingProtobuf, protobufEncode)
	endpoint.pkt.SetDecoder(transport.EncodingProtobuf, protobufDecode)

	endpoint.logPrefix = fmt.Sprintf(
		"ENDP[<-(%v,%4x)<-%v #%v]",
		endpoint.raddr, uint16(endpoint.timestamp), cluster, topic)

	go endpoint.run(endpoint.ch)
	logging.Infof("%v started ...\n", endpoint.logPrefix)
	return endpoint, nil
}

// commands
const (
	endpCmdPing byte = iota + 1
	endpCmdSend
	endpCmdResetConfig
	endpCmdGetStatistics
	endpCmdClose
)

// Ping whether endpoint is active, synchronous call.
func (endpoint *RouterEndpoint) Ping() bool {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{endpCmdPing, respch}
	resp, err := c.FailsafeOp(endpoint.ch, respch, cmd, endpoint.finch)
	if err != nil {
		return false
	}
	return resp[0].(bool)
}

// ResetConfig synchronous call.
func (endpoint *RouterEndpoint) ResetConfig(config c.Config) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{endpCmdResetConfig, config, respch}
	_, err := c.FailsafeOp(endpoint.ch, respch, cmd, endpoint.finch)
	return err
}

// Send KeyVersions to other end, asynchronous call.
// Asynchronous call. Return ErrorChannelFull that can be used by caller.
func (endpoint *RouterEndpoint) Send(data interface{}) error {
	cmd := []interface{}{endpCmdSend, data}
	if endpoint.block {
		return c.FailsafeOpAsync(endpoint.ch, cmd, endpoint.finch)
	}
	return c.FailsafeOpNoblock(endpoint.ch, cmd, endpoint.finch)
}

// GetStatistics for this endpoint, synchronous call.
func (endpoint *RouterEndpoint) GetStatistics() map[string]interface{} {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{endpCmdGetStatistics, respch}
	resp, _ := c.FailsafeOp(endpoint.ch, respch, cmd, endpoint.finch)
	return resp[0].(map[string]interface{})
}

// Close this endpoint.
func (endpoint *RouterEndpoint) Close() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{endpCmdClose, respch}
	resp, err := c.FailsafeOp(endpoint.ch, respch, cmd, endpoint.finch)
	return c.OpError(err, resp, 0)
}

// run
func (endpoint *RouterEndpoint) run(ch chan []interface{}) {
	defer func() { // panic safe
		if r := recover(); r != nil {
			logging.Errorf("%v run() crashed: %v\n", endpoint.logPrefix, r)
			logging.Errorf("%s", logging.StackTrace())
		}
		// close the connection
		endpoint.conn.Close()
		// close this endpoint
		close(endpoint.finch)
		logging.Infof("%v ... stopped\n", endpoint.logPrefix)
	}()

	raddr := endpoint.raddr

	flushTimeout := time.Tick(endpoint.bufferTm * time.Millisecond)
	harakiri := time.After(endpoint.harakiriTm * time.Millisecond)
	buffers := newEndpointBuffers(raddr)

	messageCount := int64(0)
	flushCount := int64(0)
	mutationCount := int64(0)

	flushBuffers := func() (err error) {
		logging.Tracef("%v sent %v mutations to %q\n",
			endpoint.logPrefix, mutationCount, raddr)
		if mutationCount > 0 {
			flushCount++
			err = buffers.flushBuffers(endpoint.conn, endpoint.pkt)
			if err != nil {
				logging.Errorf("%v flushBuffers() %v\n", endpoint.logPrefix, err)
			}
		}
		mutationCount = 0
		return
	}

loop:
	for {
		select {
		case msg := <-ch:
			switch msg[0].(byte) {
			case endpCmdPing:
				respch := msg[1].(chan []interface{})
				respch <- []interface{}{true}

			case endpCmdSend:
				data, ok := msg[1].(*c.DataportKeyVersions)
				if !ok {
					panic(fmt.Errorf("invalid data type %T\n", msg[1]))
				}

				kv := data.Kv
				buffers.addKeyVersions(data.Bucket, data.Vbno, data.Vbuuid, kv)
				logging.Tracef("%v added %v keyversions <%v:%v:%v> to %q\n",
					endpoint.logPrefix, kv.Length(), data.Vbno, kv.Seqno,
					kv.Commands, buffers.raddr)
				messageCount++ // count cummulative mutations
				// reload harakiri
				mutationCount++ // count queued up mutations.
				if mutationCount > int64(endpoint.bufferSize) {
					if err := flushBuffers(); err != nil {
						break loop
					}
				}
				harakiri = time.After(endpoint.harakiriTm * time.Millisecond)

			case endpCmdResetConfig:
				prefix := endpoint.logPrefix
				config := msg[1].(c.Config)
				endpoint.block = config["remoteBlock"].Bool()
				endpoint.bufferSize = config["bufferSize"].Int()
				endpoint.bufferTm =
					time.Duration(config["bufferTimeout"].Int())
				endpoint.harakiriTm =
					time.Duration(config["harakiriTimeout"].Int())
				flushTimeout = time.Tick(endpoint.bufferTm * time.Millisecond)
				if harakiri != nil { // load harakiri only when it is active
					harakiri = time.After(endpoint.harakiriTm * time.Millisecond)
					infomsg := "%v reloaded harakiriTm: %v\n"
					logging.Infof(infomsg, prefix, endpoint.harakiriTm)
				}
				respch := msg[2].(chan []interface{})
				respch <- []interface{}{nil}

			case endpCmdGetStatistics:
				respch := msg[1].(chan []interface{})
				stats := endpoint.newStats()
				stats.Set("messageCount", float64(messageCount))
				stats.Set("flushCount", float64(flushCount))
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
			// FIXME: Ideally we don't have to reload the harakir here,
			// because _this_ execution path happens only when there is
			// little activity in the data-path. On the other hand,
			// downstream can block for reasons independant of datapath,
			// hence the precaution.
			harakiri = time.After(endpoint.harakiriTm * time.Millisecond)

		case <-harakiri:
			logging.Infof("%v committed harakiri\n", endpoint.logPrefix)
			flushBuffers()
			break loop
		}
	}
}

func (endpoint *RouterEndpoint) newStats() c.Statistics {
	m := map[string]interface{}{
		"messageCount": float64(0),
		"flushCount":   float64(0),
	}
	stats, _ := c.NewStatistics(m)
	return stats
}
