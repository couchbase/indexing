package queryport

import (
	"errors"
	"fmt"
	"net"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/transport"
)

var ErrorClosedPool = errors.New("queryport.closedPool")
var ErrorNoPool = errors.New("queryport.errorNoPool")
var ErrorPoolTimeout = errors.New("queryport.connPoolTimeout")

type connectionPool struct {
	host        string
	mkConn      func(host string) (*connection, error)
	connections chan *connection
	createsem   chan bool
	logPrefix   string
}

type connection struct {
	conn net.Conn
	pkt  *transport.TransportPacket
}

func newConnectionPool(host string, poolSize, poolOverflow int) *connectionPool {
	cp := &connectionPool{
		host:        host,
		connections: make(chan *connection, poolSize),
		createsem:   make(chan bool, poolSize+poolOverflow),
		logPrefix:   fmt.Sprintf("[Queryport-connpool:%v]", host),
	}
	cp.mkConn = cp.defaultMkConn
	c.Infof("%v started ...\n", cp.logPrefix)
	return cp
}

// ConnPoolTimeout is notified whenever connections are acquired from a pool.
var ConnPoolCallback func(host string, source string, start time.Time, err error)

func (cp *connectionPool) defaultMkConn(host string) (*connection, error) {
	c.Infof("%v open new connection ...\n", cp.logPrefix)
	conn, err := net.Dial("tcp", host)
	if err != nil {
		return nil, err
	}
	flags := transport.TransportFlag(0).SetProtobuf()
	pkt := transport.NewTransportPacket(c.MaxQueryportPayload, flags)
	pkt.SetEncoder(transport.EncodingProtobuf, protobufEncode)
	pkt.SetDecoder(transport.EncodingProtobuf, protobufDecode)
	return &connection{conn, pkt}, nil
}

func (cp *connectionPool) Close() (err error) {
	defer func() {
		if r := recover(); r != nil {
			c.Errorf("%v closing connection pool %v\n", cp.logPrefix, r)
		}
	}()
	close(cp.connections)
	for connectn := range cp.connections {
		connectn.conn.Close()
	}
	c.Infof("%v ... stopped\n", cp.logPrefix)
	return
}

func (cp *connectionPool) GetWithTimeout(d time.Duration) (connectn *connection, err error) {
	if cp == nil {
		return nil, ErrorNoPool
	}

	path, ok := "", false

	if ConnPoolCallback != nil {
		defer func(path *string, start time.Time) {
			ConnPoolCallback(cp.host, *path, start, err)
		}(&path, time.Now())
	}

	path = "short-circuit"

	// short-circuit available connetions.
	select {
	case connectn, ok = <-cp.connections:
		if !ok {
			return nil, ErrorClosedPool
		}
		return connectn, nil
	default:
	}

	t := time.NewTimer(c.QueryportConnPoolAvailWaitTime)
	defer t.Stop()

	// Try to grab an available connection within 1ms
	select {
	case connectn, ok = <-cp.connections:
		path = "avail1"
		if !ok {
			return nil, ErrorClosedPool
		}
		return connectn, nil

	case <-t.C:
		// No connection came around in time, let's see
		// whether we can get one or build a new one first.
		t.Reset(d) // Reuse the timer for the full timeout.
		select {
		case connectn, ok = <-cp.connections:
			path = "avail2"
			if !ok {
				return nil, ErrorClosedPool
			}
			return connectn, nil

		case cp.createsem <- true:
			path = "create"
			// Build a connection if we can't get a real one.
			// This can potentially be an overflow connection, or
			// a pooled connection.
			connectn, err := cp.mkConn(cp.host)
			if err != nil {
				// On error, release our create hold
				<-cp.createsem
			}
			return connectn, err

		case <-t.C:
			return nil, ErrorPoolTimeout
		}
	}
}

func (cp *connectionPool) Get() (*connection, error) {
	return cp.GetWithTimeout(c.QueryportConnPoolTimeout)
}

func (cp *connectionPool) Return(connectn *connection, healthy bool) {
	if connectn.conn == nil {
		return
	}

	laddr := connectn.conn.LocalAddr()
	if cp == nil {
		c.Infof("%v pool closed\n", cp.logPrefix, laddr)
		connectn.conn.Close()
	}

	if healthy {
		defer func() {
			if recover() != nil {
				// This happens when the pool has already been
				// closed and we're trying to return a
				// connection to it anyway.  Just close the
				// connection.
				connectn.conn.Close()
			}
		}()

		select {
		case cp.connections <- connectn:
			c.Debugf("%v connection %q reclaimed to pool\n", cp.logPrefix, laddr)
		default:
			c.Debugf("%v closing overflow connection %q\n", cp.logPrefix, laddr)
			<-cp.createsem
			connectn.conn.Close()
		}

	} else {
		c.Infof("%v closing unhealthy connection %q\n", cp.logPrefix, laddr)
		<-cp.createsem
		connectn.conn.Close()
	}
}
