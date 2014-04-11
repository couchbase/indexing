// client API to push mutation messages to the other end.

package indexer

import (
	"encoding/binary"
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"log"
	"net"
)

// StreamClient is active client for streaming mutations to downstream
// components.
type StreamClient struct {
	raddr string
	conns []net.Conn
	buf   []byte
}

// NewStreamClient returns a pool of connection. Multiple connections, based
// on parameter `n`,  can be used to speed up mutation transport across network.
// A vbucket is always bounded to a connection and ensure that mutations within
// a vbucket are serialized.
func NewStreamClient(raddr string, n int) (c *StreamClient, err error) {
	var conn net.Conn

	c = &StreamClient{
		raddr: raddr,
		conns: make([]net.Conn, 0, n),
		buf:   make([]byte, GetMaxStreamDataLen()+4+2),
	}
	for i := 0; i < n; i++ {
		if conn, err = net.Dial("tcp", raddr); err != nil {
			break
		}
		c.conns = append(c.conns, conn)
	}

	if err != nil {
		c.closeConnections()
		return nil, err
	}
	return c, nil
}

// Send a mutation to indexer endpoint using one of the connection, based on
// vbcuket, opened for the endpoint. If Send() returns back an error it is
// adviced to stop the client, its connection pool, and restart the client for
// the endpoint.
//
// Returns the connection error, if there is one.
func (c *StreamClient) Send(mutation *common.Mutation) (err error) {
	var data []byte

	ks := m.GetKeyVersions()
	connid := ks[0].Vbucket % uint16(len(c.conns))
	for _, k := range ks {
		if connid != (k.Vbucket % uint16(len(c.conns))) {
			return fmt.Errorf("vbuckets should be bounded to same connection")
		}
	}

	if data, err = mutation.Encode(); err == nil {
		flags := 0
		binary.BigEndian.PutUint32(c.buf[:4], uint32(len(data)))
		binary.BigEndian.PutUint16(c.buf[4:6], uint16(flags))
		copy(c.buf[6:], data)
		l := 4 + 2 + len(data)
		_, err = c.conns[connid].Write(c.buf[:l])
	}
	return
}

// Stop the client and all of its active connection.
func (c *StreamClient) Stop() {
	if c.conns != nil {
		log.Printf("Stoping client to %v\n", c.raddr)
		c.closeConnections()
	}
}

func (c *StreamClient) closeConnections() {
	recoverClose = func(conn net.Conn) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("error closing connection: %v\n", err)
			}
		}()
		conn.Close()
	}

	if c.conns != nil {
		for _, conn := range c.conns {
			recoverClose(conn)
		}
		c.conns = nil
	}
}

func GetMaxStreamDataLen() int {
	return 10 * 1024 // TODO: move this to config file.
}
