package client

import "errors"
import "fmt"
import "net"
import "runtime/debug"
import "time"

import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/transport"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/query"

// ErrorClosedPool
var ErrorClosedPool = errors.New("queryport.closedPool")

// ErrorNoPool
var ErrorNoPool = errors.New("queryport.errorNoPool")

// ErrorPoolTimeout
var ErrorPoolTimeout = errors.New("queryport.connPoolTimeout")

type connectionPool struct {
	host        string
	mkConn      func(host string) (*connection, error)
	connections chan *connection
	createsem   chan bool
	// config params
	maxPayload   int
	timeout      time.Duration
	availTimeout time.Duration
	logPrefix    string
}

type connection struct {
	conn net.Conn
	pkt  *transport.TransportPacket
}

func newConnectionPool(
	host string,
	poolSize, poolOverflow, maxPayload int,
	timeout, availTimeout time.Duration) *connectionPool {

	cp := &connectionPool{
		host:         host,
		connections:  make(chan *connection, poolSize),
		createsem:    make(chan bool, poolSize+poolOverflow),
		maxPayload:   maxPayload,
		timeout:      timeout,
		availTimeout: availTimeout,
		logPrefix:    fmt.Sprintf("[Queryport-connpool:%v]", host),
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
	pkt := transport.NewTransportPacket(cp.maxPayload, flags)
	pkt.SetEncoder(transport.EncodingProtobuf, protobuf.ProtobufEncode)
	pkt.SetDecoder(transport.EncodingProtobuf, protobuf.ProtobufDecode)
	return &connection{conn, pkt}, nil
}

func (cp *connectionPool) Close() (err error) {
	defer func() {
		if r := recover(); r != nil {
			c.Errorf("%v Close() crashed: %v\n", cp.logPrefix, r)
			c.StackTrace(string(debug.Stack()))
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
		c.Debugf("%v new connection from pool\n", cp.logPrefix)
		return connectn, nil
	default:
	}

	t := time.NewTimer(cp.availTimeout * time.Millisecond)
	defer t.Stop()

	// Try to grab an available connection within 1ms
	select {
	case connectn, ok = <-cp.connections:
		path = "avail1"
		if !ok {
			return nil, ErrorClosedPool
		}
		c.Debugf("%v new connection (avail1) from pool\n", cp.logPrefix)
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
			c.Debugf("%v new connection (avail2) from pool\n", cp.logPrefix)
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
			c.Debugf("%v new connection (create) from pool\n", cp.logPrefix)
			return connectn, err

		case <-t.C:
			return nil, ErrorPoolTimeout
		}
	}
}

func (cp *connectionPool) Get() (*connection, error) {
	return cp.GetWithTimeout(cp.timeout * time.Millisecond)
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
