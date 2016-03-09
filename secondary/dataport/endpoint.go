// endpoint concurrency model:
//
//                  NewRouterEndpoint()
//                            |
//                            |
//                         (spawn)
//                            |
//                            |  (flushTick || > bufferSize)
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
import "strconv"
import "strings"

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
	statTick   time.Duration // timeout for logging statistics
	// gen-server
	ch    chan []interface{} // carries control commands
	finch chan bool
	// downstream
	pkt  *transport.TransportPacket
	conn net.Conn
	// statistics
	mutCount    int64
	upsertCount int64
	deleteCount int64
	upsdelCount int64
	syncCount   int64
	beginCount  int64
	endCount    int64
	snapCount   int64
	flushCount  int64
	prjLatency  *Average
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
		statTick:   time.Duration(config["statTick"].Int()),
		bufferTm:   time.Duration(config["bufferTimeout"].Int()),
		harakiriTm: time.Duration(config["harakiriTimeout"].Int()),
		prjLatency: &Average{},
	}
	endpoint.ch = make(chan []interface{}, endpoint.keyChSize)
	endpoint.conn = conn
	// TODO: add configuration params for transport flags.
	flags := transport.TransportFlag(0).SetProtobuf()
	maxPayload := config["maxPayload"].Int()
	endpoint.pkt = transport.NewTransportPacket(maxPayload, flags)
	endpoint.pkt.SetEncoder(transport.EncodingProtobuf, protobufEncode)
	endpoint.pkt.SetDecoder(transport.EncodingProtobuf, protobufDecode)

	endpoint.statTick *= time.Millisecond
	endpoint.bufferTm *= time.Millisecond
	endpoint.harakiriTm *= time.Millisecond

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

// WaitForExit will block until endpoint exits.
func (endpoint *RouterEndpoint) WaitForExit() error {
	return c.FailsafeOpAsync(nil, []interface{}{}, endpoint.finch)
}

// run
func (endpoint *RouterEndpoint) run(ch chan []interface{}) {
	flushTick := time.NewTicker(endpoint.bufferTm)
	harakiri := time.NewTimer(endpoint.harakiriTm)

	defer func() { // panic safe
		if r := recover(); r != nil {
			logging.Errorf("%v run() crashed: %v\n", endpoint.logPrefix, r)
			logging.Errorf("%s", logging.StackTrace())
		}
		if flushTick != nil {
			flushTick.Stop()
		}
		if harakiri != nil {
			harakiri.Stop()
		}
		// close the connection
		endpoint.conn.Close()
		// close this endpoint
		close(endpoint.finch)
		logging.Infof("%v ... stopped\n", endpoint.logPrefix)
	}()

	statSince := time.Now()
	var stitems [14]string
	logstats := func() {
		prjLatency := endpoint.prjLatency
		stitems[0] = `"topic":"` + endpoint.topic + `"`
		stitems[1] = `"raddr":"` + endpoint.raddr + `"`
		stitems[2] = `"mutCount":` + strconv.Itoa(int(endpoint.mutCount))
		stitems[3] = `"upsertCount":` + strconv.Itoa(int(endpoint.upsertCount))
		stitems[4] = `"deleteCount":` + strconv.Itoa(int(endpoint.deleteCount))
		stitems[5] = `"upsdelCount":` + strconv.Itoa(int(endpoint.upsertCount))
		stitems[6] = `"syncCount":` + strconv.Itoa(int(endpoint.syncCount))
		stitems[7] = `"beginCount":` + strconv.Itoa(int(endpoint.beginCount))
		stitems[8] = `"endCount":` + strconv.Itoa(int(endpoint.endCount))
		stitems[9] = `"snapCount":` + strconv.Itoa(int(endpoint.snapCount))
		stitems[10] = `"flushCount":` + strconv.Itoa(int(endpoint.flushCount))
		stitems[11] = `"latency.min":` + strconv.Itoa(int(prjLatency.Min()))
		stitems[12] = `"latency.max":` + strconv.Itoa(int(prjLatency.Max()))
		stitems[13] = `"latency.avg":` + strconv.Itoa(int(prjLatency.Mean()))
		statjson := strings.Join(stitems[:], ",")
		fmsg := "%v stats {%v}\n"
		logging.Infof(fmsg, endpoint.logPrefix, statjson)
	}

	raddr := endpoint.raddr
	lastActiveTime := time.Now()
	buffers := newEndpointBuffers(raddr)

	messageCount := 0
	flushBuffers := func() (err error) {
		fmsg := "%v sent %v mutations to %q\n"
		logging.Tracef(fmsg, endpoint.logPrefix, messageCount, raddr)
		if messageCount > 0 {
			err = buffers.flushBuffers(endpoint, endpoint.conn, endpoint.pkt)
			if err != nil {
				logging.Errorf("%v flushBuffers() %v\n", endpoint.logPrefix, err)
			}
			endpoint.flushCount++
		}
		messageCount = 0
		if time.Since(statSince) > endpoint.statTick {
			logstats()
			statSince = time.Now()
		}
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
				buffers.addKeyVersions(
					data.Bucket, data.Vbno, data.Vbuuid, kv, endpoint)
				logging.Tracef("%v added %v keyversions <%v:%v:%v> to %q\n",
					endpoint.logPrefix, kv.Length(), data.Vbno, kv.Seqno,
					kv.Commands, buffers.raddr)

				messageCount++ // count queued up mutations.
				if messageCount > endpoint.bufferSize {
					if err := flushBuffers(); err != nil {
						break loop
					}
				}

				lastActiveTime = time.Now()

			case endpCmdResetConfig:
				prefix := endpoint.logPrefix
				config := msg[1].(c.Config)
				if cv, ok := config["remoteBlock"]; ok {
					endpoint.block = cv.Bool()
				}
				if cv, ok := config["bufferSize"]; ok {
					endpoint.bufferSize = cv.Int()
				}
				if cv, ok := config["statTick"]; ok {
					endpoint.statTick = time.Duration(cv.Int())
					endpoint.statTick *= time.Millisecond
				}
				if cv, ok := config["bufferTimeout"]; ok {
					endpoint.bufferTm = time.Duration(cv.Int())
					endpoint.bufferTm *= time.Millisecond
					flushTick.Stop()
					flushTick = time.NewTicker(endpoint.bufferTm)
				}
				if cv, ok := config["harakiriTimeout"]; ok {
					endpoint.harakiriTm = time.Duration(cv.Int())
					endpoint.harakiriTm *= time.Millisecond
					if harakiri != nil { // load harakiri only when it is active
						harakiri.Reset(endpoint.harakiriTm)
						fmsg := "%v reloaded harakiriTm: %v\n"
						logging.Infof(fmsg, prefix, endpoint.harakiriTm)
					}
				}
				respch := msg[2].(chan []interface{})
				respch <- []interface{}{nil}

			case endpCmdGetStatistics: // TODO: this is defunct now.
				respch := msg[1].(chan []interface{})
				stats := endpoint.newStats()
				respch <- []interface{}{map[string]interface{}(stats)}

			case endpCmdClose:
				respch := msg[1].(chan []interface{})
				flushBuffers()
				respch <- []interface{}{nil}
				break loop
			}

		case <-flushTick.C:
			if err := flushBuffers(); err != nil {
				break loop
			}
			// FIXME: Ideally we don't have to reload the harakir here,
			// because _this_ execution path happens only when there is
			// little activity in the data-path. On the other hand,
			// downstream can block for reasons independant of datapath,
			// hence the precaution.
			lastActiveTime = time.Now()

		case <-harakiri.C:
			if time.Since(lastActiveTime) > endpoint.harakiriTm {
				logging.Infof("%v committed harakiri\n", endpoint.logPrefix)
				flushBuffers()
				break loop
			}
			harakiri.Reset(endpoint.harakiriTm)
		}
	}
	logstats()
}

func (endpoint *RouterEndpoint) newStats() c.Statistics {
	m := map[string]interface{}{}
	stats, _ := c.NewStatistics(m)
	return stats
}
