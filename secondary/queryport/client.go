// Package provides a simple library to spawn a queryport and access queryport
// via passive client API.
package queryport

import (
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"code.google.com/p/goprotobuf/proto"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	"github.com/couchbase/indexing/secondary/transport"
)

var ErrorProtocol = errors.New("queryport.protocol")

// ResponseHandler shall interpret response packets from server
// and handle them. If handler is not interested in receiving any
// more response it shall return false, else it shall continue
// until *protobufEncode.StreamEndResponse message is received.
type ResponseHandler func(resp interface{}) bool

// Client structure.
type Client struct {
	raddr     string
	pool      *connectionPool
	logPrefix string
}

// NewClient instance with `raddr` pointing to queryport server.
func NewClient(raddr string, poolSize, poolOverflow int) (c *Client) {
	c = &Client{
		raddr:     raddr,
		pool:      newConnectionPool(raddr, poolSize, poolOverflow),
		logPrefix: fmt.Sprintf("[QueryPortClient:%q]", raddr),
	}
	common.Infof("%v started ...\n", c.logPrefix)
	return c
}

// Close the client and all open connections with server.
func (c *Client) Close() {
	c.pool.Close()
	common.Infof("%v ... stopped\n", c.logPrefix)
}

// Statistics for index range.
func (c *Client) Statistics(low, high []byte, inclusion uint32) (*protobuf.IndexStatistics, error) {
	connectn, err := c.pool.Get()
	if err != nil {
		return nil, err
	}
	healthy := true
	defer c.pool.Return(connectn, healthy)

	conn, pkt := connectn.conn, connectn.pkt

	r := &protobuf.Range{
		Low:       low,
		High:      high,
		Inclusion: proto.Uint32(inclusion),
	}
	req := &protobuf.StatisticsRequest{Span: &protobuf.Span{Range: r}}
	if err := c.sendRequest(conn, pkt, req); err != nil {
		msg := "%v Statistics() request transport failed `%v`\n"
		common.Errorf(msg, c.logPrefix, err)
		healthy = false
		return nil, err
	}

	timeoutMs := common.QueryportReadDeadline * time.Millisecond
	conn.SetReadDeadline(time.Now().Add(timeoutMs))
	resp, err := pkt.Receive(conn)
	if err != nil {
		msg := "%v Statistics() response transport failed `%v`\n"
		common.Errorf(msg, c.logPrefix, err)
		healthy = false
		return nil, err
	}

	// skip StreamEndResponse
	conn.SetReadDeadline(time.Now().Add(timeoutMs))
	endResp, err := pkt.Receive(conn)
	if _, ok := endResp.(*protobuf.StreamEndResponse); !ok {
		return nil, ErrorProtocol
	}
	return (resp.(*protobuf.StatisticsResponse)).GetStats(), nil
}

// Scan index for a range.
func (c *Client) Scan(
	low, high []byte, inclusion uint32, pageSize int64,
	distinct bool, limit int64, callb ResponseHandler) error {

	connectn, err := c.pool.Get()
	if err != nil {
		return err
	}
	healthy := true
	defer c.pool.Return(connectn, healthy)

	conn, pkt := connectn.conn, connectn.pkt

	r := &protobuf.Range{
		Low:       []byte(low),
		High:      []byte(high),
		Inclusion: proto.Uint32(inclusion),
	}
	req := &protobuf.ScanRequest{
		Span:     &protobuf.Span{Range: r},
		Distinct: proto.Bool(distinct),
		PageSize: proto.Int64(pageSize),
		Limit:    proto.Int64(limit),
	}
	if err := c.sendRequest(conn, pkt, req); err != nil {
		msg := "%v Scan() request transport failed `%v`\n"
		common.Errorf(msg, c.logPrefix, err)
		healthy = false
		return err
	}

	cont := true
	for cont {
		cont, healthy, err = c.streamResponse(conn, pkt, callb)
		if err != nil {
			msg := "%v Scan() response failed `%v`\n"
			common.Errorf(msg, c.logPrefix, err)
		}
	}
	return nil
}

func (c *Client) ScanAll(pageSize int64, limit int64, callb func(interface{}) bool) error {
	connectn, err := c.pool.Get()
	if err != nil {
		return err
	}
	healthy := true
	defer c.pool.Return(connectn, healthy)

	conn, pkt := connectn.conn, connectn.pkt

	req := &protobuf.ScanAllRequest{
		PageSize: proto.Int64(pageSize),
		Limit:    proto.Int64(limit),
	}
	if err := c.sendRequest(conn, pkt, req); err != nil {
		common.Errorf(
			"%v ScanAll() request transport failed `%v`\n",
			c.logPrefix, err)
		healthy = false
		return err
	}

	cont := true
	for cont {
		cont, healthy, err = c.streamResponse(conn, pkt, callb)
		if err != nil {
			msg := "%v ScanAll() response failed `%v`\n"
			common.Errorf(msg, c.logPrefix, err)
		}
	}
	return nil
}

func (c *Client) sendRequest(
	conn net.Conn, pkt *transport.TransportPacket, req interface{}) (err error) {

	timeoutMs := common.QueryportWriteDeadline * time.Millisecond
	conn.SetWriteDeadline(time.Now().Add(timeoutMs))
	return pkt.Send(conn, req)
}

func (c *Client) streamResponse(
	conn net.Conn,
	pkt *transport.TransportPacket,
	callb ResponseHandler) (cont bool, healthy bool, err error) {

	var resp interface{}
	var finish bool

	laddr := conn.LocalAddr()
	timeoutMs := common.QueryportReadDeadline * time.Millisecond
	conn.SetReadDeadline(time.Now().Add(timeoutMs))
	resp, err = pkt.Receive(conn)
	if err != nil {
		callb(err)
		cont, healthy = false, false
		if err != io.EOF {
			msg := "%v connection %q response transport failed `%v`\n"
			common.Errorf(msg, c.logPrefix, laddr, err)
		}

	} else if _, finish = resp.(*protobuf.StreamEndResponse); finish {
		msg := "%v connection %q received StreamEndResponse"
		common.Debugf(msg, c.logPrefix, laddr)
		callb(resp) // callback most likely return true
		cont, healthy = false, true

	} else {
		cont = callb(resp)
		healthy = true
	}

	if cont == false && healthy == true && finish == false {
		err = c.closeStream(conn, pkt)
	}
	return
}

func (c *Client) closeStream(
	conn net.Conn, pkt *transport.TransportPacket) (err error) {

	var resp interface{}
	laddr := conn.LocalAddr()
	// request server to end the stream.
	err = c.sendRequest(conn, pkt, &protobuf.EndStreamRequest{})
	if err != nil {
		msg := "%v closeStream() request transport failed `%v`\n"
		common.Errorf(msg, c.logPrefix, err)
		return
	}
	msg := "%v connection %q transmitted protobuf.EndStreamRequest"
	common.Debugf(msg, c.logPrefix, laddr)

	timeoutMs := common.QueryportReadDeadline * time.Millisecond
	// flush the connection until stream has ended.
	for true {
		conn.SetReadDeadline(time.Now().Add(timeoutMs))
		resp, err = pkt.Receive(conn)
		if err == io.EOF {
			common.Errorf("%v connection %q closed \n", c.logPrefix, laddr)
			return

		} else if err != nil {
			msg := "%v connection %q response transport failed `%v`\n"
			common.Errorf(msg, c.logPrefix, laddr, err)
			return

		} else if _, ok := resp.(*protobuf.StreamEndResponse); ok {
			return
		}
	}
	return
}
