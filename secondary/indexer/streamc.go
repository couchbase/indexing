// Client API to push mutation messages to the other end. This is an all or
// nothing client for each downstream host.
//
// Error messages returned by the API:
// - StreamClientClosed, means a fault has accured and all connections to
//   downstream host is closed
// - error, mostly means contract between application and library is broken
//   due to a bug.
//
// In any case, client will close all connections open with that remote host,
// and it is up to the application to reconnect with remote host.

package indexer

import (
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"log"
	"net"
	"sync"
)

var (
	StreamClientClosed = fmt.Errorf("client already closed")
)

// StreamClient is active client, for each remote host, and there can be
// multiple connections opened with remote host for the same endpoint.
type StreamClient struct {
	mu        sync.Mutex
	raddr     string
	conns     map[int]net.Conn
	vbChans   map[uint16]chan []*common.KeyVersions
	connChans map[int]chan []*common.KeyVersions
}

// NewStreamClient returns a pool of connection. Multiple connections, based
// on parameter `n`,  can be used to speed up mutation transport across network.
// A vbucket is always bounded to a connection and ensure that mutations within
// a vbucket are serialized.
func NewStreamClient(raddr string, n int, vbmap common.VbConnectionMap) (c *StreamClient, err error) {
	var conn net.Conn
	c = &StreamClient{
		raddr:     raddr,
		conns:     make(map[int]net.Conn),
		vbChans:   make(map[uint16]chan []*common.KeyVersions),
		connChans: make(map[int]chan []*common.KeyVersions),
	}
	connVbs := make(map[int]*common.VbConnectionMap)
	pkts := make(map[int]*StreamTransportPacket)
	// open connections with remote
	for i := 0; i < n; i++ {
		if conn, err = net.Dial("tcp", raddr); err != nil {
			break
		}
		c.connChans[i] = make(chan []*common.KeyVersions, 10000)
		c.conns[i] = conn
		flags := streamTransportFlag(0).setProtobuf()
		pkts[i] = NewStreamTransportPacket(GetMaxStreamDataLen(), flags)
		connVbs[i] = &common.VbConnectionMap{
			Vbuckets: make([]uint16, 0),
			Vbuuids:  make([]uint64, 0),
		}
	}

	// bind vbuckets with connections
	if err == nil {
		for i, vbno := range vbmap.Vbuckets {
			j := i % n
			connVbs[j].Vbuckets = append(connVbs[j].Vbuckets, vbno)
			connVbs[j].Vbuuids = append(connVbs[j].Vbuuids, vbmap.Vbuuids[i])
			c.vbChans[vbno] = c.connChans[j]
		}
		// send vbmap for each connection to downstream host
		for i, conn := range c.conns {
			if err = pkts[i].Send(conn, *connVbs[i]); err != nil {
				break
			}
			go c.streamKeyVersions(conn, pkts[i], c.connChans[i])
		}
	}

	// if err close connections
	if err != nil {
		c.Stop()
		return nil, err
	}
	return c, nil
}

// Send KeyVersions to downstream endpoint using one of the connection. Note
// that a mutation message can carry more than on KeyVersions payload, in
// which case, all of them should belong to the same vbucket.
//
// Returns the connection error, if there is one.
// If Send() returns back an error it is adviced to stop the client,
// its connection pool, and wait for a reconnect request.
func (c *StreamClient) SendKeyVersions(keys []*common.KeyVersions) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	if len(keys) < 1 {
		err = fmt.Errorf("no keyversions found")
	}
	// sanity check: whether KeyVersions belong to same vbucket.
	// TODO: can be removed in production.
	vbucket := keys[0].Vbucket
	for _, kv := range keys {
		if vbucket != kv.Vbucket {
			err = fmt.Errorf("key-versions belong to different vbuckets")
		}
	}

	// send it to streamer routine
	if err == nil {
		var ok bool

		c.mu.Lock()
		if c.conns != nil {
			_, ok = c.vbChans[vbucket]
		} else {
			err = fmt.Errorf("client already closed")
		}
		c.mu.Unlock()

		if ok {
			c.vbChans[vbucket] <- keys
		} else {
			err = fmt.Errorf("unable to resolve vbucket %v to connection", vbucket)
		}
	}
	return
}

// Stop the client and all its active connection with downstream host.
func (c *StreamClient) Stop() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conns != nil {
		log.Printf("stoping client to %q\n", c.raddr)
		c.closeConnections()
	}
	return
}

// close all connections with downstream host.
func (c *StreamClient) closeConnections() {
	recoverClose := func(conn net.Conn) {
		laddr := conn.LocalAddr().String()
		defer func() {
			if r := recover(); r != nil {
				log.Printf("error closing connection %q: `%v`\n", laddr, r)
			}
		}()
		conn.Close()
		log.Printf("connection %q closed\n", laddr)
	}
	// close streaming channels
	for _, ch := range c.connChans {
		close(ch)
	}
	// close connections
	if c.conns != nil {
		for _, conn := range c.conns {
			recoverClose(conn)
		}
	}
	c.conns = nil
	c.vbChans = nil
	c.connChans = nil
}

func (c *StreamClient) streamKeyVersions(
	conn net.Conn,
	pkt *StreamTransportPacket,
	kvch chan []*common.KeyVersions) {

	laddr := conn.LocalAddr().String()
loop:
	for {
		keyversions, ok := <-kvch
		if ok {
			if err := pkt.Send(conn, keyversions); err != nil {
				log.Printf("connection conn %q failed with %v\n", laddr, err)
				c.Stop()
				break loop
			}
		} else {
			log.Printf("input channel closed for %q", laddr)
			break loop
		}
	}
}
