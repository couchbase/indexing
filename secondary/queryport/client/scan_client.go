// Package queryport provides a simple library to spawn a queryport and access
// queryport via passive client API.
//
// ---> Request                 ---> Request
//      <--- Response                <--- Response
//      <--- Response                <--- Response
//      ...                     ---> EndStreamRequest
//      <--- StreamEndResponse       <--- Response (residue)
//                                   <--- StreamEndResponse

package client

import "errors"
import "fmt"
import "io"
import "net"
import "time"
import "encoding/json"

import "github.com/couchbase/indexing/secondary/logging"
import "github.com/couchbase/indexing/secondary/common"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
import "github.com/couchbase/indexing/secondary/transport"
import "github.com/golang/protobuf/proto"

// GsiScanClient for scan operations.
type GsiScanClient struct {
	queryport string
	pool      *connectionPool
	// config params
	maxPayload         int // TODO: what if it exceeds ?
	readDeadline       time.Duration
	writeDeadline      time.Duration
	poolSize           int
	poolOverflow       int
	cpTimeout          time.Duration
	cpAvailWaitTimeout time.Duration
	logPrefix          string
}

func NewGsiScanClient(queryport string, config common.Config) *GsiScanClient {
	t := time.Duration(config["connPoolAvailWaitTimeout"].Int())
	c := &GsiScanClient{
		queryport:          queryport,
		maxPayload:         config["maxPayload"].Int(),
		readDeadline:       time.Duration(config["readDeadline"].Int()),
		writeDeadline:      time.Duration(config["writeDeadline"].Int()),
		poolSize:           config["settings.poolSize"].Int(),
		poolOverflow:       config["settings.poolOverflow"].Int(),
		cpTimeout:          time.Duration(config["connPoolTimeout"].Int()),
		cpAvailWaitTimeout: t,
		logPrefix:          fmt.Sprintf("[GsiScanClient:%q]", queryport),
	}
	c.pool = newConnectionPool(
		queryport, c.poolSize, c.poolOverflow, c.maxPayload, c.cpTimeout,
		c.cpAvailWaitTimeout)
	logging.Infof("%v started ...\n", c.logPrefix)
	return c
}

// LookupStatistics for a single secondary-key.
func (c *GsiScanClient) LookupStatistics(
	defnID uint64, value common.SecondaryKey) (common.IndexStatistics, error) {

	// serialize lookup value.
	val, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	req := &protobuf.StatisticsRequest{
		DefnID: proto.Uint64(defnID),
		Span:   &protobuf.Span{Equals: [][]byte{val}},
	}
	resp, err := c.doRequestResponse(req)
	if err != nil {
		return nil, err
	}
	statResp := resp.(*protobuf.StatisticsResponse)
	if statResp.GetErr() != nil {
		err = errors.New(statResp.GetErr().GetError())
		return nil, err
	}
	return statResp.GetStats(), nil
}

// RangeStatistics for index range.
func (c *GsiScanClient) RangeStatistics(
	defnID uint64, low, high common.SecondaryKey,
	inclusion Inclusion) (common.IndexStatistics, error) {

	// serialize low and high values.
	l, err := json.Marshal(low)
	if err != nil {
		return nil, err
	}
	h, err := json.Marshal(high)
	if err != nil {
		return nil, err
	}

	req := &protobuf.StatisticsRequest{
		DefnID: proto.Uint64(defnID),
		Span: &protobuf.Span{
			Range: &protobuf.Range{
				Low: l, High: h, Inclusion: proto.Uint32(uint32(inclusion)),
			},
		},
	}
	resp, err := c.doRequestResponse(req)
	if err != nil {
		return nil, err
	}
	statResp := resp.(*protobuf.StatisticsResponse)
	if statResp.GetErr() != nil {
		err = errors.New(statResp.GetErr().GetError())
		return nil, err
	}
	return statResp.GetStats(), nil
}

// Lookup scan index between low and high.
func (c *GsiScanClient) Lookup(
	defnID uint64, values []common.SecondaryKey,
	distinct bool, limit int64,
	cons common.Consistency, vector *TsConsistency,
	callb ResponseHandler) (error, bool) {

	// serialize lookup value.
	equals := make([][]byte, 0, len(values))
	for _, value := range values {
		val, err := json.Marshal(value)
		if err != nil {
			return err, false
		}
		equals = append(equals, val)
	}

	connectn, err := c.pool.Get()
	if err != nil {
		return err, false
	}
	healthy := true
	defer func() { c.pool.Return(connectn, healthy) }()

	conn, pkt := connectn.conn, connectn.pkt

	req := &protobuf.ScanRequest{
		DefnID:   proto.Uint64(defnID),
		Span:     &protobuf.Span{Equals: equals},
		Distinct: proto.Bool(distinct),
		Limit:    proto.Int64(limit),
		Cons:     proto.Uint32(uint32(cons)),
	}
	if vector != nil {
		req.Vector = protobuf.NewTsConsistency(
			vector.Vbnos, vector.Seqnos, vector.Vbuuids, vector.Crc64)
	}

	// ---> protobuf.ScanRequest
	if err := c.sendRequest(conn, pkt, req); err != nil {
		fmsg := "%v Lookup() request transport failed `%v`\n"
		logging.Errorf(fmsg, c.logPrefix, err)
		healthy = false
		return err, false
	}

	cont, partial := true, false
	for cont {
		// <--- protobuf.ResponseStream
		cont, healthy, err = c.streamResponse(conn, pkt, callb)
		if err != nil { // if err, cont should have been set to false
			fmsg := "%v Lookup() response failed `%v`\n"
			logging.Errorf(fmsg, c.logPrefix, err)
		} else { // partially succeeded
			partial = true
		}
	}
	return err, partial
}

// Range scan index between low and high.
func (c *GsiScanClient) Range(
	defnID uint64, low, high common.SecondaryKey, inclusion Inclusion,
	distinct bool, limit int64, cons common.Consistency, vector *TsConsistency,
	callb ResponseHandler) (error, bool) {

	// serialize low and high values.
	l, err := json.Marshal(low)
	if err != nil {
		return err, false
	}
	h, err := json.Marshal(high)
	if err != nil {
		return err, false
	}

	connectn, err := c.pool.Get()
	if err != nil {
		return err, false
	}
	healthy := true
	defer func() { c.pool.Return(connectn, healthy) }()

	conn, pkt := connectn.conn, connectn.pkt

	req := &protobuf.ScanRequest{
		DefnID: proto.Uint64(defnID),
		Span: &protobuf.Span{
			Range: &protobuf.Range{
				Low: l, High: h, Inclusion: proto.Uint32(uint32(inclusion)),
			},
		},
		Distinct: proto.Bool(distinct),
		Limit:    proto.Int64(limit),
		Cons:     proto.Uint32(uint32(cons)),
	}
	if vector != nil {
		req.Vector = protobuf.NewTsConsistency(
			vector.Vbnos, vector.Seqnos, vector.Vbuuids, vector.Crc64)
	}
	// ---> protobuf.ScanRequest
	if err := c.sendRequest(conn, pkt, req); err != nil {
		fmsg := "%v Range() request transport failed `%v`\n"
		logging.Errorf(fmsg, c.logPrefix, err)
		healthy = false
		return err, false
	}

	cont, partial := true, false
	for cont {
		// <--- protobuf.ResponseStream
		cont, healthy, err = c.streamResponse(conn, pkt, callb)
		if err != nil { // if err, cont should have been set to false
			fmsg := "%v Range() response failed `%v`\n"
			logging.Errorf(fmsg, c.logPrefix, err)
		} else { // partial succeeded
			partial = true
		}
	}
	return err, partial
}

// Range scan index between low and high.
func (c *GsiScanClient) RangePrimary(
	defnID uint64, low, high []byte, inclusion Inclusion,
	distinct bool, limit int64, cons common.Consistency, vector *TsConsistency,
	callb ResponseHandler) (error, bool) {

	connectn, err := c.pool.Get()
	if err != nil {
		return err, false
	}
	healthy := true
	defer func() { c.pool.Return(connectn, healthy) }()

	conn, pkt := connectn.conn, connectn.pkt

	req := &protobuf.ScanRequest{
		DefnID: proto.Uint64(defnID),
		Span: &protobuf.Span{
			Range: &protobuf.Range{
				Low: low, High: high,
				Inclusion: proto.Uint32(uint32(inclusion)),
			},
		},
		Distinct: proto.Bool(distinct),
		Limit:    proto.Int64(limit),
		Cons:     proto.Uint32(uint32(cons)),
	}
	if vector != nil {
		req.Vector = protobuf.NewTsConsistency(
			vector.Vbnos, vector.Seqnos, vector.Vbuuids, vector.Crc64)
	}
	// ---> protobuf.ScanRequest
	if err := c.sendRequest(conn, pkt, req); err != nil {
		fmsg := "%v RangePrimary() request transport failed `%v`\n"
		logging.Errorf(fmsg, c.logPrefix, err)
		healthy = false
		return err, false
	}

	cont, partial := true, false
	for cont {
		// <--- protobuf.ResponseStream
		cont, healthy, err = c.streamResponse(conn, pkt, callb)
		if err != nil { // if err, cont should have been set to false
			fmsg := "%v RangePrimary() response failed `%v`\n"
			logging.Errorf(fmsg, c.logPrefix, err)
		} else {
			partial = true
		}
	}
	return err, partial
}

// ScanAll for full table scan.
func (c *GsiScanClient) ScanAll(
	defnID uint64, limit int64,
	cons common.Consistency, vector *TsConsistency,
	callb ResponseHandler) (error, bool) {

	connectn, err := c.pool.Get()
	if err != nil {
		return err, false
	}
	healthy := true
	defer func() { c.pool.Return(connectn, healthy) }()

	conn, pkt := connectn.conn, connectn.pkt

	req := &protobuf.ScanAllRequest{
		DefnID: proto.Uint64(defnID),
		Limit:  proto.Int64(limit),
		Cons:   proto.Uint32(uint32(cons)),
	}
	if vector != nil {
		req.Vector = protobuf.NewTsConsistency(
			vector.Vbnos, vector.Seqnos, vector.Vbuuids, vector.Crc64)
	}
	if err := c.sendRequest(conn, pkt, req); err != nil {
		fmsg := "%v ScanAll() request transport failed `%v`\n"
		logging.Errorf(fmsg, c.logPrefix, err)
		healthy = false
		return err, false
	}

	cont, partial := true, false
	for cont {
		// <--- protobuf.ResponseStream
		cont, healthy, err = c.streamResponse(conn, pkt, callb)
		if err != nil { // if err, cont should have been set to false
			fmsg := "%v ScanAll() response failed `%v`\n"
			logging.Errorf(fmsg, c.logPrefix, err)
		} else {
			partial = true
		}
	}
	return err, partial
}

// CountLookup to count number entries for given set of keys.
func (c *GsiScanClient) CountLookup(
	defnID uint64, values []common.SecondaryKey,
	cons common.Consistency, vector *TsConsistency) (int64, error) {

	// serialize match value.
	equals := make([][]byte, 0, len(values))
	for _, value := range values {
		val, err := json.Marshal(value)
		if err != nil {
			return 0, err
		}
		equals = append(equals, val)
	}

	req := &protobuf.CountRequest{
		DefnID: proto.Uint64(defnID),
		Span:   &protobuf.Span{Equals: equals},
		Cons:   proto.Uint32(uint32(cons)),
	}
	if vector != nil {
		req.Vector = protobuf.NewTsConsistency(
			vector.Vbnos, vector.Seqnos, vector.Vbuuids, vector.Crc64)
	}
	resp, err := c.doRequestResponse(req)
	if err != nil {
		return 0, err
	}
	countResp := resp.(*protobuf.CountResponse)
	if countResp.GetErr() != nil {
		err = errors.New(countResp.GetErr().GetError())
		return 0, err
	}
	return countResp.GetCount(), nil
}

// CountRange to count number entries in the given range.
func (c *GsiScanClient) CountRange(
	defnID uint64, low, high common.SecondaryKey, inclusion Inclusion,
	cons common.Consistency, vector *TsConsistency) (int64, error) {

	// serialize low and high values.
	l, err := json.Marshal(low)
	if err != nil {
		return 0, err
	}
	h, err := json.Marshal(high)
	if err != nil {
		return 0, err
	}

	req := &protobuf.CountRequest{
		DefnID: proto.Uint64(defnID),
		Span: &protobuf.Span{
			Range: &protobuf.Range{
				Low: l, High: h, Inclusion: proto.Uint32(uint32(inclusion)),
			},
		},
		Cons: proto.Uint32(uint32(cons)),
	}
	if vector != nil {
		req.Vector = protobuf.NewTsConsistency(
			vector.Vbnos, vector.Seqnos, vector.Vbuuids, vector.Crc64)
	}

	resp, err := c.doRequestResponse(req)
	if err != nil {
		return 0, err
	}
	countResp := resp.(*protobuf.CountResponse)
	if countResp.GetErr() != nil {
		err = errors.New(countResp.GetErr().GetError())
		return 0, err
	}
	return countResp.GetCount(), nil
}

func (c *GsiScanClient) Close() error {
	return c.pool.Close()
}

func (c *GsiScanClient) doRequestResponse(req interface{}) (interface{}, error) {
	connectn, err := c.pool.Get()
	if err != nil {
		return nil, err
	}
	healthy := true
	defer func() { c.pool.Return(connectn, healthy) }()

	conn, pkt := connectn.conn, connectn.pkt

	// ---> protobuf.*Request
	if err := c.sendRequest(conn, pkt, req); err != nil {
		fmsg := "%v %T request transport failed `%v`\n"
		logging.Errorf(fmsg, c.logPrefix, req, err)
		healthy = false
		return nil, err
	}

	timeoutMs := c.readDeadline * time.Millisecond
	conn.SetReadDeadline(time.Now().Add(timeoutMs))
	// <--- protobuf.*Response
	resp, err := pkt.Receive(conn)
	if err != nil {
		fmsg := "%v %T response transport failed `%v`\n"
		logging.Errorf(fmsg, c.logPrefix, req, err)
		healthy = false
		return nil, err
	}

	conn.SetReadDeadline(time.Now().Add(timeoutMs))
	// <--- protobuf.StreamEndResponse (skipped) TODO: knock this off.
	if endResp, err := pkt.Receive(conn); err != nil {
		fmsg := "%v %T response transport failed `%v`\n"
		logging.Errorf(fmsg, c.logPrefix, req, err)
		healthy = false
		return nil, err
	} else if endResp != nil {
		healthy = false
		return nil, ErrorProtocol
	}
	return resp, nil
}

func (c *GsiScanClient) sendRequest(
	conn net.Conn, pkt *transport.TransportPacket, req interface{}) (err error) {

	timeoutMs := c.writeDeadline * time.Millisecond
	conn.SetWriteDeadline(time.Now().Add(timeoutMs))
	return pkt.Send(conn, req)
}

func (c *GsiScanClient) streamResponse(
	conn net.Conn,
	pkt *transport.TransportPacket,
	callb ResponseHandler) (cont bool, healthy bool, err error) {

	var resp interface{}
	var finish bool

	laddr := conn.LocalAddr()
	timeoutMs := c.readDeadline * time.Millisecond
	conn.SetReadDeadline(time.Now().Add(timeoutMs))
	if resp, err = pkt.Receive(conn); err != nil {
		//resp := &protobuf.ResponseStream{
		//    Err: &protobuf.Error{Error: proto.String(err.Error())},
		//}
		//callb(resp) // callback with error
		cont, healthy = false, false
		if err == io.EOF {
			err = fmt.Errorf("server closed connection (EOF)")
		} else {
			fmsg := "%v connection %q response transport failed `%v`\n"
			logging.Errorf(fmsg, c.logPrefix, laddr, err)
		}

	} else if resp == nil {
		finish = true
		fmsg := "%v connection %q received StreamEndResponse"
		logging.Tracef(fmsg, c.logPrefix, laddr)
		callb(&protobuf.StreamEndResponse{}) // callback most likely return true
		cont, healthy = false, true

	} else {
		streamResp := resp.(*protobuf.ResponseStream)
		if err = streamResp.Error(); err == nil {
			cont = callb(streamResp)
		}
		healthy = true
	}

	var closeErr error
	if cont == false && healthy == true && finish == false {
		if closeErr, healthy = c.closeStream(conn, pkt); err == nil {
			err = closeErr
		}
	}
	return
}

func (c *GsiScanClient) closeStream(
	conn net.Conn, pkt *transport.TransportPacket) (err error, healthy bool) {

	var resp interface{}
	laddr := conn.LocalAddr()
	healthy = true
	// request server to end the stream.
	err = c.sendRequest(conn, pkt, &protobuf.EndStreamRequest{})
	if err != nil {
		fmsg := "%v closeStream() request transport failed `%v`\n"
		logging.Errorf(fmsg, c.logPrefix, err)
		healthy = false
		return
	}
	fmsg := "%v connection %q transmitted protobuf.EndStreamRequest"
	logging.Tracef(fmsg, c.logPrefix, laddr)

	timeoutMs := c.readDeadline * time.Millisecond
	// flush the connection until stream has ended.
	for true {
		conn.SetReadDeadline(time.Now().Add(timeoutMs))
		resp, err = pkt.Receive(conn)
		if err != nil {
			healthy = false
			if err == io.EOF {
				logging.Errorf("%v connection %q closed \n", c.logPrefix, laddr)
				return
			}
			fmsg := "%v connection %q response transport failed `%v`\n"
			logging.Errorf(fmsg, c.logPrefix, laddr, err)
			return

		} else if resp == nil { // End of stream marker
			return
		}
	}
	return
}
