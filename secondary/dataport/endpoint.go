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

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"
	"github.com/couchbase/indexing/secondary/stats"
	"github.com/couchbase/indexing/secondary/transport"

	dcpTransport "github.com/couchbase/indexing/secondary/dcp/transport"
)

// RouterEndpoint structure, per topic, to gather key-versions / mutations
// from one or more vbuckets and push them downstream to a
// specific node.
type RouterEndpoint struct {
	topic     string
	timestamp int64  // immutable
	raddr     string // immutable
	cluster   string
	// config params
	logPrefix string
	keyChSize int // channel size for key-versions
	// live update is possible
	block      bool          // should endpoint block when remote is slow
	bufferSize int           // size of buffer to wait till flush
	bufferTm   time.Duration // timeout to flush endpoint-buffer
	harakiriTm time.Duration // timeout after which endpoint commits harakiri
	syncTm     time.Duration // timeout after which endpoint sends sync message

	// gen-server
	ch    chan []interface{} // carries control commands
	finch chan bool
	done  uint32
	// downstream
	pkt  *transport.TransportPacket
	conn net.Conn
	// statistics
	stats *EndpointStats

	// Book-keeping for verifying sequence order.
	// TODO: This introduces a map lookup in mutation path. Need to anlayse perf implication.
	seqOrders map[string]dcpTransport.SeqOrderState

	// Mapping between vbuckets and keyspace
	keyspaceIdVBMap map[string]map[uint16]bool
}

type EndpointStats struct {
	closed      stats.BoolVal
	mutCount    stats.Uint64Val
	upsertCount stats.Uint64Val
	deleteCount stats.Uint64Val
	upsdelCount stats.Uint64Val
	syncCount   stats.Uint64Val
	beginCount  stats.Uint64Val
	endCount    stats.Uint64Val
	snapCount   stats.Uint64Val
	flushCount  stats.Uint64Val
	prjLatency  stats.Average
	endpCh      chan []interface{}

	// Collections specific
	collectionCreate  stats.Uint64Val
	collectionDrop    stats.Uint64Val
	collectionFlush   stats.Uint64Val
	scopeCreate       stats.Uint64Val
	scopeDrop         stats.Uint64Val
	collectionChanged stats.Uint64Val
	updateSeqno       stats.Uint64Val
	seqnoAdvanced     stats.Uint64Val
	osoSnapshotStart  stats.Uint64Val
	osoSnapshotEnd    stats.Uint64Val

	cmdStats map[byte]*stats.Uint64Val
}

func (stats *EndpointStats) Init() {
	stats.closed.Init()
	stats.mutCount.Init()
	stats.upsertCount.Init()
	stats.deleteCount.Init()
	stats.upsdelCount.Init()
	stats.syncCount.Init()
	stats.beginCount.Init()
	stats.endCount.Init()
	stats.snapCount.Init()
	stats.flushCount.Init()
	stats.prjLatency.Init()

	stats.collectionCreate.Init()
	stats.collectionDrop.Init()
	stats.collectionFlush.Init()
	stats.scopeCreate.Init()
	stats.scopeDrop.Init()
	stats.collectionChanged.Init()
	stats.updateSeqno.Init()
	stats.seqnoAdvanced.Init()
	stats.osoSnapshotStart.Init()
	stats.osoSnapshotEnd.Init()
}

func (stats *EndpointStats) IsClosed() bool {
	return stats.closed.Value()
}

func (stats *EndpointStats) String() string {
	var stitems [24]string
	stitems[0] = `"mutCount":` + strconv.FormatUint(stats.mutCount.Value(), 10)
	stitems[1] = `"upsertCount":` + strconv.FormatUint(stats.upsertCount.Value(), 10)
	stitems[2] = `"deleteCount":` + strconv.FormatUint(stats.deleteCount.Value(), 10)
	stitems[3] = `"upsdelCount":` + strconv.FormatUint(stats.upsdelCount.Value(), 10)
	stitems[4] = `"syncCount":` + strconv.FormatUint(stats.syncCount.Value(), 10)
	stitems[5] = `"beginCount":` + strconv.FormatUint(stats.beginCount.Value(), 10)
	stitems[6] = `"endCount":` + strconv.FormatUint(stats.endCount.Value(), 10)
	stitems[7] = `"snapCount":` + strconv.FormatUint(stats.snapCount.Value(), 10)
	stitems[8] = `"flushCount":` + strconv.FormatUint(stats.flushCount.Value(), 10)

	stitems[9] = `"collectionCreate":` + strconv.FormatUint(stats.collectionCreate.Value(), 10)
	stitems[10] = `"collectionDrop":` + strconv.FormatUint(stats.collectionDrop.Value(), 10)
	stitems[11] = `"collectionFlush":` + strconv.FormatUint(stats.collectionFlush.Value(), 10)
	stitems[12] = `"scopeCreate":` + strconv.FormatUint(stats.scopeCreate.Value(), 10)
	stitems[13] = `"scopeDrop":` + strconv.FormatUint(stats.scopeDrop.Value(), 10)
	stitems[14] = `"collectionChanged":` + strconv.FormatUint(stats.collectionChanged.Value(), 10)
	stitems[15] = `"updateSeqno":` + strconv.FormatUint(stats.updateSeqno.Value(), 10)
	stitems[16] = `"seqnoAdvanced":` + strconv.FormatUint(stats.seqnoAdvanced.Value(), 10)
	stitems[17] = `"osoSnapshotStart":` + strconv.FormatUint(stats.osoSnapshotStart.Value(), 10)
	stitems[18] = `"osoSnapshotEnd":` + strconv.FormatUint(stats.osoSnapshotEnd.Value(), 10)

	stitems[19] = `"latency.min":` + strconv.FormatInt(stats.prjLatency.Min(), 10)
	stitems[20] = `"latency.max":` + strconv.FormatInt(stats.prjLatency.Max(), 10)
	stitems[21] = `"latency.avg":` + strconv.FormatInt(stats.prjLatency.Mean(), 10)
	stitems[22] = `"latency.movingAvg":` + strconv.FormatInt(stats.prjLatency.MovingAvg(), 10)
	stitems[23] = `"endpChLen":` + strconv.FormatUint((uint64)(len(stats.endpCh)), 10)
	statjson := strings.Join(stitems[:], ",")
	return fmt.Sprintf("{%v}", statjson)
}

// NewRouterEndpoint instantiate a new RouterEndpoint
// routine and return its reference.
func NewRouterEndpoint(
	cluster, topic, raddr string, maxvbs int,
	config c.Config) (*RouterEndpoint, error) {

	conn, err := security.MakeConn(raddr)
	if err != nil {
		return nil, err
	}

	endpoint := &RouterEndpoint{
		topic:           topic,
		raddr:           raddr,
		cluster:         cluster,
		finch:           make(chan bool),
		timestamp:       time.Now().UnixNano(),
		keyChSize:       config["keyChanSize"].Int(),
		block:           config["remoteBlock"].Bool(),
		bufferSize:      config["bufferSize"].Int(),
		bufferTm:        time.Duration(config["bufferTimeout"].Int()),
		harakiriTm:      time.Duration(config["harakiriTimeout"].Int()),
		syncTm:          time.Duration(config["syncTimeout"].Int()),
		stats:           &EndpointStats{},
		seqOrders:       make(map[string]dcpTransport.SeqOrderState),
		keyspaceIdVBMap: make(map[string]map[uint16]bool),
	}
	endpoint.ch = make(chan []interface{}, endpoint.keyChSize)
	endpoint.conn = conn

	endpoint.stats.Init()
	endpoint.stats.endpCh = endpoint.ch
	// TODO: add configuration params for transport flags.
	flags := transport.TransportFlag(0).SetProtobuf()
	maxPayload := config["maxPayload"].Int()
	endpoint.pkt = transport.NewTransportPacket(maxPayload, flags)
	endpoint.pkt.SetEncoder(transport.EncodingProtobuf, protobufEncode)
	endpoint.pkt.SetDecoder(transport.EncodingProtobuf, protobufDecode)

	endpoint.bufferTm *= time.Millisecond
	endpoint.harakiriTm *= time.Millisecond
	endpoint.syncTm *= time.Millisecond

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

	return atomic.LoadUint32(&endpoint.done) == 0
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

// Send KeyVersions to other end, asynchronous call.
// Asynchronous call. Return ErrorChannelFull that can be used by caller.
// Returns ErrorAbort if abortCh is closed on callers side
func (endpoint *RouterEndpoint) Send2(data interface{}, abortCh chan bool) error {
	cmd := []interface{}{endpCmdSend, data}
	if endpoint.block {
		return c.FailsafeOpAsync2(endpoint.ch, cmd, endpoint.finch, abortCh)
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

// Get the map of endpoint name to pointer for the stats object
func (endpoint *RouterEndpoint) GetStats() map[string]interface{} {
	if atomic.LoadUint32(&endpoint.done) == 0 && endpoint.stats != nil {
		endpStat := make(map[string]interface{}, 0)
		key := fmt.Sprintf(
			"ENDP[<-(%v,%4x)<-%v #%v]",
			endpoint.raddr, uint16(endpoint.timestamp), endpoint.cluster, endpoint.topic)
		endpStat[key] = endpoint.stats
		return endpStat
	}
	return nil
}

func (endpoint *RouterEndpoint) logStats() {
	key := fmt.Sprintf(
		"<-(%v,%4x)<-%v #%v",
		endpoint.raddr, uint16(endpoint.timestamp), endpoint.cluster, endpoint.topic)
	stats := endpoint.stats.String()
	logging.Infof("ENDP[%v] stats: %v", key, stats)
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
	harakiri := time.NewTimer(endpoint.harakiriTm)
	flushTick := time.NewTimer(endpoint.bufferTm)
	flushTickActive := true
	syncTick := time.NewTicker(endpoint.syncTm)

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
		atomic.StoreUint32(&endpoint.done, 1)
		close(endpoint.finch)
		//Update closed in stats object and log the stats before exiting
		endpoint.stats.closed.Set(true)
		endpoint.logStats()
		logging.Infof("%v ... stopped\n", endpoint.logPrefix)
	}()

	raddr := endpoint.raddr
	lastActiveTime := time.Now()
	buffers := newEndpointBuffers(raddr)

	messageCount := 0
	flushBuffers := func() (err error) {

		logging.LazyTrace(func() string {
			fmsg := "%v sent %v mutations to %q\n"
			return fmt.Sprintf(fmsg, endpoint.logPrefix, messageCount, raddr)
		})

		if messageCount > 0 {
			err = buffers.flushBuffers(endpoint, endpoint.conn, endpoint.pkt)
			if err != nil {
				logging.Errorf("%v flushBuffers() %v\n", endpoint.logPrefix, err)
			}
			endpoint.stats.flushCount.Add(1)
		}
		messageCount = 0
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
					data.KeyspaceId, data.Vbno, data.Vbuuid,
					data.Opaque2, data.OSO, kv, endpoint)
				logging.LazyTrace(func() string {
					return fmt.Sprintf("%v added %v keyversions <%v:%v:%v> to %q\n",
						endpoint.logPrefix, kv.Length(), data.Vbno, kv.Seqno,
						kv.Commands, buffers.raddr)
				})

				messageCount++ // count queued up mutations.
				if messageCount > endpoint.bufferSize {
					if err := flushBuffers(); err != nil {
						break loop
					}
				} else {
					// Received a message but could not flush as the number of messages
					// in buffer are less then minimum number of messages required for
					// flush. If flushTick is not active, reset flushTick so that these
					// messages will be flushed in the next "endpoint.bufferTm" duration
					if !flushTickActive {
						flushTick.Reset(endpoint.bufferTm)
						flushTickActive = true
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
				if cv, ok := config["bufferTimeout"]; ok {
					endpoint.bufferTm = time.Duration(cv.Int())
					endpoint.bufferTm *= time.Millisecond
					flushTick.Stop()
					flushTick.Reset(endpoint.bufferTm)
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
				if cv, ok := config["syncTimeout"]; ok {
					endpoint.syncTm = time.Duration(cv.Int()) * time.Millisecond
					syncTick.Stop()
					syncTick.Reset(endpoint.syncTm)
					fmsg := "%v reloaded syncTm: %v\n"
					logging.Infof(fmsg, prefix, endpoint.syncTm)
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
			// FlushTick has fired. FlushTick will be re-activated
			// when there are any new messages
			flushTickActive = false

			// FIXME: Ideally we don't have to reload the harakir here,
			// because _this_ execution path happens only when there is
			// little activity in the data-path. On the other hand,
			// downstream can block for reasons independant of datapath,
			// hence the precaution.
			lastActiveTime = time.Now()

		case <-syncTick.C:
			// Sync message to indexer requires keyspaceId and vbno.
			// Without these, indexer would filter the mutation and log the
			// message. This can lead to unnecessary log flooding.
			//
			// In mixed mode cluster, this problem can not be solved from indexer
			// side. To avoid that, projector keeps a track of keyspaceId and vbno's
			// that it is currently processing. It uses the first vbucket no.
			// belonging to the first keyspace it encounteres in the map to
			// send the sync message to indexer
			for keyspaceId, vbmap := range endpoint.keyspaceIdVBMap {
				for vb, _ := range vbmap {
					kv := c.NewKeyVersions(0 /*seqno*/, nil, 1, 0 /*ctime*/)
					kv.AddSync()
					data := &c.DataportKeyVersions{keyspaceId, vb, 0, kv, 0, false}

					buffers.addKeyVersions(
						data.KeyspaceId, data.Vbno, data.Vbuuid,
						data.Opaque2, data.OSO, kv, endpoint)
					messageCount++
					if err := flushBuffers(); err != nil {
						logging.Errorf("%v Error observed during flush, err: %v", err)
						break loop
					}

					// break here as only one message is enough to keep TCP connection alive
					break
				}
				break
			}
		case <-harakiri.C:
			if time.Since(lastActiveTime) > endpoint.harakiriTm {
				logging.Infof("%v committed harakiri\n", endpoint.logPrefix)
				flushBuffers()
				break loop
			}
			harakiri.Reset(endpoint.harakiriTm)
		}
	}
}

func (endpoint *RouterEndpoint) newStats() c.Statistics {
	m := map[string]interface{}{}
	stats, _ := c.NewStatistics(m)
	return stats
}
