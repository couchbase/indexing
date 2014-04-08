// client API for stream server

package indexer

import (
    "encoding/binary"
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
// A vbucket is always bounded to a connection.
func NewStreamClient(raddr string, n int) (c *StreamClient, err error) {
    var conn net.Conn

    c = &StreamClient{
        raddr: raddr,
        conns: make([]net.Conn, 0, n),
        buf:   make([]byte, MAX_STREAM_DATA_LEN+4+2),
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

// Send a mutation to stream server using one of the connection, based on
// vbucket, in connection-pool. If Send() returns back an error it is adviced
// to stop the client, its connection pool, and restart the client for the
// server.
func (c *StreamClient) Send(mutation *common.Mutation) (err error) {
    var data []byte
    i := mutation.Vbucket % uint16(len(c.conns))
    if data, err = mutation.Encode(); err == nil {
        flags := 0
        binary.BigEndian.PutUint32(c.buf[:4], uint32(len(data)))
        binary.BigEndian.PutUint16(c.buf[4:6], uint16(flags))
        copy(c.buf[6:], data)
        l := 4 + 2 + len(data)
        _, err = c.conns[i].Write(c.buf[:l])
        log.Println("send", len(c.buf))
    }
    return
}

// Stop the client and all of its active connection.
func (c *StreamClient) Stop() {
    if c.conns != nil {
        log.Printf("Stoping client to %v", c.raddr)
        c.closeConnections()
    }
}

func (c *StreamClient) closeConnections() {
    for _, conn := range c.conns {
        conn.Close()
    }
    c.conns = nil
}
