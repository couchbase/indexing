package couchbase

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"encoding/binary"

	"github.com/couchbase/indexing/secondary/dcp/transport"
	memcached "github.com/couchbase/indexing/secondary/dcp/transport/client"
)

var errClosedPool = errors.New("the pool is closed")
var errNoPool = errors.New("no pool")

// GenericMcdAuthHandler is a kind of AuthHandler that performs
// special auth exchange (like non-standard auth, possibly followed by
// select-bucket).
type GenericMcdAuthHandler interface {
	AuthHandler
	AuthenticateMemcachedConn(string, *memcached.Client) error
}

// Default timeout for retrieving a connection from the pool.
var ConnPoolTimeout = time.Hour * 24 * 30

// ConnPoolAvailWaitTime is the amount of time to wait for an existing
// connection from the pool before considering the creation of a new
// one.
var ConnPoolAvailWaitTime = time.Millisecond

type connectionPool struct {
	host        string
	mkConn      func(host string, ah AuthHandler) (*memcached.Client, error)
	auth        AuthHandler
	connections chan *memcached.Client
	createsem   chan bool
}

func newConnectionPool(host string, ah AuthHandler, poolSize, poolOverflow int) *connectionPool {
	return &connectionPool{
		host:        host,
		connections: make(chan *memcached.Client, poolSize),
		createsem:   make(chan bool, poolSize+poolOverflow),
		mkConn:      defaultMkConn,
		auth:        ah,
	}
}

// ConnPoolTimeout is notified whenever connections are acquired from a pool.
var ConnPoolCallback func(host string, source string, start time.Time, err error)

func defaultMkConn(
	host string, ah AuthHandler) (conn *memcached.Client, err error) {

	conn, err = memcached.Connect("tcp", host)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			conn.Close()
			conn = nil
			return
		}
	}()

	conn.SetMcdConnectionDeadline()
	defer conn.ResetMcdConnectionDeadline()

	if gah, ok := ah.(GenericMcdAuthHandler); ok {
		err = gah.AuthenticateMemcachedConn(host, conn)
		return
	}
	name, pass := ah.GetCredentials()
	if name != "default" {
		_, err = conn.Auth(name, pass)
	}
	return
}

func (cp *connectionPool) Close() (err error) {
	defer func() { err, _ = recover().(error) }()
	close(cp.connections)
	for c := range cp.connections {
		c.Close()
	}
	return
}

func (cp *connectionPool) GetWithTimeout(d time.Duration) (rv *memcached.Client, err error) {
	if cp == nil {
		return nil, errNoPool
	}

	path := ""

	if ConnPoolCallback != nil {
		defer func(path *string, start time.Time) {
			ConnPoolCallback(cp.host, *path, start, err)
		}(&path, time.Now())
	}

	path = "short-circuit"

	// short-circuit available connetions.
	select {
	case rv, isopen := <-cp.connections:
		if !isopen {
			return nil, errClosedPool
		}
		return rv, nil
	default:
	}

	t := time.NewTimer(ConnPoolAvailWaitTime)
	defer t.Stop()

	// Try to grab an available connection within 1ms
	select {
	case rv, isopen := <-cp.connections:
		path = "avail1"
		if !isopen {
			return nil, errClosedPool
		}
		return rv, nil
	case <-t.C:
		// No connection came around in time, let's see
		// whether we can get one or build a new one first.
		t.Reset(d) // Reuse the timer for the full timeout.
		select {
		case rv, isopen := <-cp.connections:
			path = "avail2"
			if !isopen {
				return nil, errClosedPool
			}
			return rv, nil
		case cp.createsem <- true:
			path = "create"
			// Build a connection if we can't get a real one.
			// This can potentially be an overflow connection, or
			// a pooled connection.
			rv, err := cp.mkConn(cp.host, cp.auth)
			if err != nil {
				// On error, release our create hold
				<-cp.createsem
			}
			return rv, err
		case <-t.C:
			return nil, ErrTimeout
		}
	}
}

func (cp *connectionPool) Get() (*memcached.Client, error) {
	return cp.GetWithTimeout(ConnPoolTimeout)
}

func (cp *connectionPool) Return(c *memcached.Client) {
	if c == nil {
		return
	}

	if cp == nil {
		c.Close()
	}

	if c.IsHealthy() {
		defer func() {
			if recover() != nil {
				// This happens when the pool has already been
				// closed and we're trying to return a
				// connection to it anyway.  Just close the
				// connection.
				c.Close()
			}
		}()

		select {
		case cp.connections <- c:
		default:
			// Overflow connection.
			<-cp.createsem
			c.Close()
		}
	} else {
		<-cp.createsem
		c.Close()
	}
}

func (cp *connectionPool) StartTapFeed(args *memcached.TapArguments) (*memcached.TapFeed, error) {
	if cp == nil {
		return nil, errNoPool
	}
	mc, err := cp.Get()
	if err != nil {
		return nil, err
	}

	// A connection can't be used after TAP; Dont' count it against the
	// connection pool capacity
	<-cp.createsem

	return mc.StartTapFeed(*args)
}

const DEFAULT_WINDOW_SIZE = uint32(20 * 1024 * 1024) // 20 Mb

func (cp *connectionPool) StartDcpFeed(
	name DcpFeedName, sequence, flags uint32,
	outch chan *memcached.DcpEvent,
	opaque uint16,
	supvch chan []interface{},
	config map[string]interface{},
	streamNo int) (*memcached.DcpFeed, error) {

	if cp == nil {
		return nil, errNoPool
	}

	mc, err := cp.Get() // Don't call Return() on this
	if err != nil {
		return nil, err
	}
	// A connection can't be used after it has been allocated to DCP;
	// Dont' count it against the connection pool capacity
	<-cp.createsem

	defaultConnBufferSize := DEFAULT_WINDOW_SIZE
	if val, ok := config["useMutationQueue"]; ok && val.(bool) {
		if val, ok := config["mutation_queue.connection_buffer_size"]; ok {
			defaultConnBufferSize = uint32(val.(int))
		}
	} else if val, ok := config["connection_buffer_size"]; ok {
		defaultConnBufferSize = uint32(val.(int))
	}

	dcpf, err := memcached.NewDcpFeed(
		mc, string(name), outch,
		opaque, supvch, config, streamNo,
	)
	if err == nil {
		err = dcpf.DcpOpen(
			string(name), sequence, flags, defaultConnBufferSize, opaque,
		)
		if err == nil {
			return dcpf, err
		}
	}
	mc.Close()
	return nil, err
}

func (cp *connectionPool) GetDcpConn(name DcpFeedName) (*memcached.Client, error) {
	mc, err := cp.Get() // Don't call Return() on this
	if err != nil {
		return nil, err
	}

	rq := &transport.MCRequest{
		Opcode: transport.DCP_OPEN,
		Key:    []byte(string(name)),
		Opaque: 0,
	}
	rq.Extras = make([]byte, 8)
	binary.BigEndian.PutUint32(rq.Extras[:4], 0)
	binary.BigEndian.PutUint32(rq.Extras[4:], 1) // we are consumer

	mc.SetMcdConnectionDeadline()
	defer mc.ResetMcdConnectionDeadline()

	if err := mc.Transmit(rq); err != nil {
		return nil, err
	}

	_, err = mc.Receive()
	if err != nil {
		return nil, err
	}

	return mc, nil
}

func GetSeqs(mc *memcached.Client, seqnos []uint64, buf []byte) error {
	res := &transport.MCResponse{}
	rq := &transport.MCRequest{
		Opcode: transport.DCP_GET_SEQNO,
		Opaque: 0,
	}

	rq.Extras = make([]byte, 4)
	binary.BigEndian.PutUint32(rq.Extras, 1) // Only active vbuckets

	mc.SetMcdConnectionDeadline()
	defer mc.ResetMcdConnectionDeadline()

	if err := mc.Transmit(rq); err != nil {
		return err
	}

	if err := mc.ReceiveInBuf(res, buf); err != nil {
		return err
	}

	if res.Status != transport.SUCCESS {
		return fmt.Errorf("failed %d", res.Status)
	}

	if len(res.Body)%10 != 0 {
		fmsg := "invalid body length %v, in get-seqnos\n"
		err := fmt.Errorf(fmsg, len(res.Body))
		return err
	}
	for i := 0; i < len(seqnos); i++ {
		seqnos[i] = 0
	}

	for i := 0; i < len(res.Body); i += 10 {
		vbno := int(binary.BigEndian.Uint16(res.Body[i : i+2]))
		seqno := binary.BigEndian.Uint64(res.Body[i+2 : i+10])
		seqnos[vbno] = seqno
	}

	return nil
}

// Get seqnos of vbuckets in active/replica/pending state
func GetSeqsAllVbStates(mc *memcached.Client, seqnos []uint64, buf []byte) error {
	res := &transport.MCResponse{}
	rq := &transport.MCRequest{
		Opcode: transport.DCP_GET_SEQNO,
		Opaque: 0,
	}

	mc.SetMcdConnectionDeadline()
	defer mc.ResetMcdConnectionDeadline()

	if err := mc.Transmit(rq); err != nil {
		return err
	}

	if err := mc.ReceiveInBuf(res, buf); err != nil {
		return err
	}

	if res.Status != transport.SUCCESS {
		return fmt.Errorf("failed %d", res.Status)
	}

	if len(res.Body)%10 != 0 {
		fmsg := "invalid body length %v, in get-seqnos\n"
		err := fmt.Errorf(fmsg, len(res.Body))
		return err
	}
	for i := 0; i < len(seqnos); i++ {
		seqnos[i] = 0
	}

	for i := 0; i < len(res.Body); i += 10 {
		vbno := int(binary.BigEndian.Uint16(res.Body[i : i+2]))
		seqno := binary.BigEndian.Uint64(res.Body[i+2 : i+10])
		seqnos[vbno] = seqno
	}

	return nil
}

func GetCollectionSeqs(mc *memcached.Client, seqnos []uint64, buf []byte,
	collectionId string) error {

	//TODO (Collections): possibly better method of converting hex string to bytes directly
	cid, err := strconv.ParseUint(collectionId, 16, 32)
	if err != nil {
		return err
	}
	extras := make([]byte, 8)
	binary.BigEndian.PutUint32(extras[0:4], 1)          // Only active vbuckets
	binary.BigEndian.PutUint32(extras[4:], uint32(cid)) // encode collectionId

	return GetSeqsWithExtras(mc, seqnos, buf, extras)
}

func GetCollectionSeqsAllVbStates(mc *memcached.Client, seqnos []uint64, buf []byte,
	collectionId string) error {

	cid, err := strconv.ParseUint(collectionId, 16, 32)
	if err != nil {
		return err
	}

	extras := make([]byte, 8)
	binary.BigEndian.PutUint32(extras[0:4], 0)          // All vb states
	binary.BigEndian.PutUint32(extras[4:], uint32(cid)) // encode collectionId

	return GetSeqsWithExtras(mc, seqnos, buf, extras)
}

func GetSeqsWithExtras(mc *memcached.Client, seqnos []uint64, buf []byte,
	extras []byte) error {

	res := &transport.MCResponse{}
	rq := &transport.MCRequest{
		Opcode: transport.DCP_GET_SEQNO,
		Opaque: 0,
	}

	rq.Extras = extras

	mc.SetMcdConnectionDeadline()
	defer mc.ResetMcdConnectionDeadline()

	if err := mc.Transmit(rq); err != nil {
		return err
	}

	if err := mc.ReceiveInBuf(res, buf); err != nil {
		return err
	}

	if res.Status != transport.SUCCESS {
		return fmt.Errorf("failed %d", res.Status)
	}

	if len(res.Body)%10 != 0 {
		fmsg := "invalid body length %v, in get-seqnos\n"
		err := fmt.Errorf(fmsg, len(res.Body))
		return err
	}
	for i := 0; i < len(seqnos); i++ {
		seqnos[i] = 0
	}

	for i := 0; i < len(res.Body); i += 10 {
		vbno := int(binary.BigEndian.Uint16(res.Body[i : i+2]))
		seqno := binary.BigEndian.Uint64(res.Body[i+2 : i+10])
		seqnos[vbno] = seqno
	}

	return nil
}
