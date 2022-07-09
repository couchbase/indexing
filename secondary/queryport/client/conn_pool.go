package client

import (
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"
	"github.com/couchbase/indexing/secondary/transport"
	"github.com/golang/protobuf/proto"

	protobuf "github.com/couchbase/indexing/secondary/protobuf/query"

	gometrics "github.com/rcrowley/go-metrics"
)

const (
	CONN_RELEASE_INTERVAL      = 5  // Seconds. Don't change as long as go-metrics/ewma is being used.
	NUM_CONN_RELEASE_INTERVALS = 60 // Don't change as long as go-metrics/ewma is being used.
	CONN_COUNT_LOG_INTERVAL    = 60 // Seconds.
)

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
	maxPayload       int
	timeout          time.Duration
	availTimeout     time.Duration
	logPrefix        string
	curActConns      int32
	minPoolSizeWM    int32
	freeConns        int32
	relConnBatchSize int32
	stopCh           chan bool
	ewma             gometrics.EWMA
	kaInterval       time.Duration
	authHost         string
	cluster          string
	needsAuth        *uint32
}

type connection struct {
	conn          net.Conn
	pkt           *transport.TransportPacket
	authenticated bool
}

type authInfo struct {
	user string
	pass string
}

func newConnectionPool(
	host string,
	poolSize, poolOverflow, maxPayload int,
	timeout, availTimeout time.Duration,
	minPoolSizeWM int32, relConnBatchSize int32, kaInterval int,
	cluster string, needsAuth *uint32) *connectionPool {

	cp := &connectionPool{
		host:             host,
		connections:      make(chan *connection, poolSize),
		createsem:        make(chan bool, poolSize+poolOverflow),
		maxPayload:       maxPayload,
		timeout:          timeout,
		availTimeout:     availTimeout,
		logPrefix:        fmt.Sprintf("[Queryport-connpool:%v]", host),
		minPoolSizeWM:    minPoolSizeWM,
		relConnBatchSize: relConnBatchSize,
		stopCh:           make(chan bool, 1),
		kaInterval:       time.Duration(kaInterval) * time.Second,
		cluster:          cluster,
		needsAuth:        needsAuth,
	}

	// Ignore the error in initHostportForAuth, if any.
	// It will be retried again in doAuth.
	cp.initHostportForAuth()

	cp.mkConn = cp.defaultMkConn
	cp.ewma = gometrics.NewEWMA5()
	logging.Infof("%v started poolsize %v overflow %v low WM %v relConn batch size %v ...\n",
		cp.logPrefix, poolSize, poolOverflow, minPoolSizeWM, relConnBatchSize)
	go cp.releaseConnsRoutine()
	return cp
}

// ConnPoolTimeout is notified whenever connections are acquired from a pool.
var ConnPoolCallback func(host string, source string, start time.Time, err error)

func (cp *connectionPool) defaultMkConn(host string) (*connection, error) {
	logging.Infof("%v open new connection ...\n", cp.logPrefix)
	conn, err := security.MakeConn(host)
	if err != nil {
		return nil, err
	}

	flags := transport.TransportFlag(0).SetProtobuf()
	pkt := transport.NewTransportPacket(cp.maxPayload, flags)
	pkt.SetEncoder(transport.EncodingProtobuf, protobuf.ProtobufEncode)
	pkt.SetDecoder(transport.EncodingProtobuf, protobuf.ProtobufDecode)

	if cp.kaInterval > time.Duration(0) {
		tcpconn, ok := conn.(*net.TCPConn)
		if ok {
			tcpconn.SetKeepAlive(true)
			tcpconn.SetKeepAlivePeriod(cp.kaInterval)
		}
	}

	cn := &connection{
		conn:          conn,
		pkt:           pkt,
		authenticated: false,
	}

	// Do Auth
	err = cp.doAuth(cn)
	if err != nil {
		return nil, err
	}

	return cn, nil
}

func (cp *connectionPool) doAuth(conn *connection) error {

	// Check if auth is supported / configured before doing auth
	clustVer := common.GetClusterVersion()
	needsAuth := atomic.LoadUint32(cp.needsAuth)
	intVer := common.GetInternalVersion()
	if clustVer < common.INDEXER_71_VERSION && needsAuth == 0 && intVer.LessThan(common.MIN_VER_SRV_AUTH) {
		logging.Verbosef("%v doAuth Auth is not needed for connection (%v,%v) clustVer %v, intVer %v, needsAuth %v ",
			cp.logPrefix, conn.conn.LocalAddr(), conn.conn.RemoteAddr(), clustVer, intVer, needsAuth)
		return nil
	}

	user, pass, err := cp.getAuthInfo()
	if err != nil {
		logging.Errorf("%v doAuth error %v in getAuthInfo for connection (%v,%v)",
			cp.logPrefix, err, conn.conn.LocalAddr(), conn.conn.RemoteAddr())
		return err
	}

	// Send Auth packet.
	authReq := &protobuf.AuthRequest{
		User:          &user,
		Pass:          &pass,
		ClientVersion: proto.Uint32(uint32(common.INDEXER_CUR_VERSION)),
	}

	err = conn.pkt.Send(conn.conn, authReq)
	if err != nil {
		logging.Errorf("%v doAuth pkt.Send returns error %v for connection (%v,%v)",
			cp.logPrefix, err, conn.conn.LocalAddr(), conn.conn.RemoteAddr())
		return err
	}

	// Receive Auth response.
	var resp interface{}
	resp, err = conn.pkt.Receive(conn.conn)
	if err != nil {
		logging.Errorf("%v doAuth pkt.Receive returns error %v for connection (%v,%v)",
			cp.logPrefix, err, conn.conn.LocalAddr(), conn.conn.RemoteAddr())
		return err
	}

	authResp, ok := resp.(*protobuf.AuthResponse)
	if !ok {
		logging.Errorf("%v doAuth invalid auth response from %v for connection (%v,%v)",
			cp.logPrefix, cp.host, conn.conn.LocalAddr(), conn.conn.RemoteAddr())
		return errors.New("Invalid auth response")
	}

	if authResp.GetCode() != transport.AUTH_SUCCESS {
		logging.Errorf("%v doAuth invalid auth credentials for connection (%v,%v)",
			cp.logPrefix, conn.conn.LocalAddr(), conn.conn.RemoteAddr())
		return transport.ErrorAuthFailure
	}

	conn.authenticated = true
	logging.Verbosef("%v doAuth auth successful for connection (%v,%v)",
		cp.logPrefix, conn.conn.LocalAddr(), conn.conn.RemoteAddr())

	return nil
}

func (cp *connectionPool) getAuthInfo() (string, string, error) {

	if cp.authHost == "" {
		err := cp.initHostportForAuth()
		if err != nil {
			logging.Errorf("%v doAtuh error in initHostportForAuth: %v", cp.logPrefix, err)
			return "", "", err
		}
	}

	user, pass, err := cbauth.GetHTTPServiceAuth(cp.authHost)
	if err != nil {
		logging.Errorf("%v doAuth cbauth.GetHTTPServiceAuth returns error %v", cp.logPrefix, err)
		return "", "", err
	}

	return user, pass, nil
}

func (cp *connectionPool) initHostportForAuth() error {

	clusterUrl, err := common.ClusterAuthUrl(cp.cluster)
	if err != nil {
		return err
	}

	cinfo, err := common.NewClusterInfoCache(clusterUrl, common.DEFAULT_POOL)
	if err != nil {
		return err
	}

	cinfo.Lock()
	defer cinfo.Unlock()

	cinfo.SetUserAgent(cp.logPrefix)

	err = cinfo.FetchNodesAndSvsInfo()
	if err != nil {
		return err
	}

	var authHost string
	authHost, err = cinfo.TranslatePort(cp.host, common.INDEX_SCAN_SERVICE, common.INDEX_HTTP_SERVICE)
	if err != nil {
		cp.authHost = ""
		return err
	}

	cp.authHost = authHost
	return nil
}

func (cp *connectionPool) Close() (err error) {
	defer func() {
		if r := recover(); r != nil {
			logging.Verbosef("%v Close() crashed: %v\n", cp.logPrefix, r)
			logging.Verbosef("%s", logging.StackTrace())
		}
	}()
	cp.stopCh <- true
	close(cp.connections)
	for connectn := range cp.connections {
		connectn.conn.Close()
	}
	logging.Infof("%v ... stopped\n", cp.logPrefix)
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
		logging.Debugf("%v new connection from pool\n", cp.logPrefix)
		atomic.AddInt32(&cp.freeConns, -1)
		atomic.AddInt32(&cp.curActConns, 1)
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
		logging.Debugf("%v new connection (avail1) from pool\n", cp.logPrefix)
		atomic.AddInt32(&cp.freeConns, -1)
		atomic.AddInt32(&cp.curActConns, 1)
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
			logging.Debugf("%v new connection (avail2) from pool\n", cp.logPrefix)
			atomic.AddInt32(&cp.freeConns, -1)
			atomic.AddInt32(&cp.curActConns, 1)
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
			} else {
				atomic.AddInt32(&cp.curActConns, 1)
			}
			logging.Debugf("%v new connection (create) from pool\n", cp.logPrefix)
			return connectn, err

		case <-t.C:
			return nil, ErrorPoolTimeout
		}
	}
}

func (cp *connectionPool) Renew(conn *connection) (*connection, error) {

	newConn, err := cp.mkConn(cp.host)
	if err == nil {
		logging.Infof("%v closing unhealthy connection %q\n", cp.logPrefix, conn.conn.LocalAddr())
		conn.conn.Close()
		conn = newConn
	}

	return conn, err
}

func (cp *connectionPool) Get() (*connection, error) {
	return cp.GetWithTimeout(cp.timeout * time.Millisecond)
}

func (cp *connectionPool) Return(connectn *connection, healthy bool) {
	defer atomic.AddInt32(&cp.curActConns, -1)
	if connectn == nil || connectn.conn == nil {
		return
	}

	laddr := connectn.conn.LocalAddr()
	if cp == nil {
		logging.Infof("%v pool closed\n", cp.logPrefix, laddr)
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
			logging.Debugf("%v connection %q reclaimed to pool\n", cp.logPrefix, laddr)
			atomic.AddInt32(&cp.freeConns, 1)
		default:
			logging.Debugf("%v closing overflow connection %q poolSize=%v\n", cp.logPrefix, laddr, len(cp.connections))
			<-cp.createsem
			connectn.conn.Close()
		}

	} else {
		logging.Infof("%v closing unhealthy connection %q authenticated %v\n", cp.logPrefix, laddr, connectn.authenticated)
		<-cp.createsem
		connectn.conn.Close()
	}
}

func max(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

func (cp *connectionPool) numConnsToRetain() (int32, bool) {
	avg := cp.ewma.Rate()
	act := atomic.LoadInt32(&cp.curActConns)
	num := max(act, int32(avg))
	num = max(cp.minPoolSizeWM, num)
	fc := atomic.LoadInt32(&cp.freeConns)
	totalConns := act + fc
	if totalConns-cp.relConnBatchSize >= num {
		// Don't release more than relConnBatchSize number of connections
		// in 1 iteration
		logging.Debugf("%v releasinng connections ...", cp.logPrefix)
		return totalConns - cp.relConnBatchSize, true
	}
	return totalConns, false
}

func (cp *connectionPool) releaseConns(numRetConns int32) {
	for {
		fc := atomic.LoadInt32(&cp.freeConns)
		act := atomic.LoadInt32(&cp.curActConns)
		totalConns := act + fc
		if totalConns > numRetConns && fc > 0 {
			select {
			case conn, ok := <-cp.connections:
				if !ok {
					return
				}
				atomic.AddInt32(&cp.freeConns, -1)
				conn.conn.Close()
			default:
				break
			}
		} else {
			break
		}
	}
}

func (cp *connectionPool) releaseConnsRoutine() {
	i := 0
	j := 0
	for {
		time.Sleep(time.Second)
		select {
		case <-cp.stopCh:
			logging.Infof("%v Stopping releaseConnsRoutine", cp.logPrefix)
			return

		default:
			// ewma.Update happens every second
			act := atomic.LoadInt32(&cp.curActConns)
			cp.ewma.Update(int64(act))

			// ewma.Tick() and ewma.Rate() is called every 5 seconds.
			if i == CONN_RELEASE_INTERVAL-1 {
				cp.ewma.Tick()
				numRetConns, needToFreeConns := cp.numConnsToRetain()
				if needToFreeConns {
					cp.releaseConns(numRetConns)
				}
			}

			// Log active and free connection count history every minute.
			fc := atomic.LoadInt32(&cp.freeConns)
			if j == CONN_COUNT_LOG_INTERVAL-1 {
				logging.Infof("%v active conns %v, free conns %v", cp.logPrefix, act, fc)
			}

			i = (i + 1) % CONN_RELEASE_INTERVAL
			j = (j + 1) % CONN_COUNT_LOG_INTERVAL
		}
	}
}
