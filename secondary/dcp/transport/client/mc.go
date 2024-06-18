// Package memcached provides a memcached binary protocol client.
package memcached

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/dcp/transport"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"
)

const bufsize = 1024

// The Client itself.
type Client struct {
	conn    io.ReadWriteCloser
	healthy bool

	// Book-keeping to enable collections on connections later
	// when the cluster is fully upgraded
	collectionsEnabled uint32 // 0 => collections disabled, 1 => collections enabled

	hdrBuf []byte
}

var dialFun = net.Dial

var ErrorEnableJSON = errors.New("dcp.EnableJSON")
var ErrorJSONNotEnabled = errors.New("dcp.ErrorJSONNotEnabled")

const opaqueRandomScan = 0xBEAF0002

// Timeout for memcached communication where indexer/projector
// is actively waiting. In Seconds.
var DcpMemcachedTimeout uint32 = 120

// 30 minutes
const DcpMutationReadTimeout uint32 = 1800

func GetDcpMemcachedTimeout() uint32 {
	return atomic.LoadUint32(&DcpMemcachedTimeout)
}

func SetDcpMemcachedTimeout(val uint32) {
	atomic.StoreUint32(&DcpMemcachedTimeout, val)
}

// Connect to a memcached server.
func Connect(prot, dest string) (rv *Client, err error) {
	conn, err := security.MakeConn(dest)
	if err != nil {
		return nil, err
	}
	return Wrap(conn)
}

// Wrap an existing transport.
func Wrap(rwc io.ReadWriteCloser) (rv *Client, err error) {
	return &Client{
		conn:    rwc,
		healthy: true,
		hdrBuf:  make([]byte, transport.HDR_LEN),
	}, nil
}

// Close the connection when you're done.
func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) SetCollectionsEnabled() {
	atomic.StoreUint32(&c.collectionsEnabled, 1)
}

func (c *Client) IsCollectionsEnabled() bool {
	return (atomic.LoadUint32(&c.collectionsEnabled) == 1)
}

func (c *Client) SetDeadline(t time.Time) error {
	return c.conn.(net.Conn).SetDeadline(t)
}

func (c *Client) SetReadDeadline(t time.Time) error {
	return c.conn.(net.Conn).SetReadDeadline(t)
}

func (c *Client) SetWriteDeadline(t time.Time) error {
	return c.conn.(net.Conn).SetWriteDeadline(t)
}

// Set Memcached Connection Deadline.
// Ignore the error in SetDeadline, if any.
// There are no side effects in SetDeadline error codepaths.
func (c *Client) SetMcdConnectionDeadline() {
	timeout := time.Duration(GetDcpMemcachedTimeout()) * time.Second
	err := c.SetDeadline(time.Now().Add(timeout))
	if err != nil {
		logging.Debugf("Error in SetMcdConnectionDeadline: %v", err)
	}
}

// Reset Memcached Connection Deadline.
// Log the error in SetDeadline, if any. If SetMcdConnectionDeadline
// was successful, and ResetMcdConnectionDeadline fails, the connection may
// get closed during some other IO operation. So, report the error if any.
// No need to explicitly fail the product workflow.
func (c *Client) ResetMcdConnectionDeadline() {
	err := c.SetDeadline(time.Time{})
	if err != nil {
		logging.Errorf("Error in ResetMcdConnectionDeadline: %v", err)
	}
}

// Set Memcached Connection WriteDeadline.
func (c *Client) SetMcdConnectionWriteDeadline() {
	timeout := time.Duration(GetDcpMemcachedTimeout()) * time.Second
	err := c.SetWriteDeadline(time.Now().Add(timeout))
	if err != nil {
		logging.Debugf("Error in SetMcdConnectionWriteDeadline: %v", err)
	}
}

// Reset Memcached Connection WriteDeadline.
func (c *Client) ResetMcdConnectionWriteDeadline() {
	err := c.SetWriteDeadline(time.Time{})
	if err != nil {
		logging.Errorf("Error in ResetMcdConnectionWriteDeadline: %v", err)
	}
}

// Set Memcached Connection ReadDeadline.
func (c *Client) SetMcdMutationReadDeadline() {
	timeout := time.Duration(DcpMutationReadTimeout) * time.Second
	err := c.SetReadDeadline(time.Now().Add(timeout))
	if err != nil {
		logging.Debugf("Error in SetMcdConnectionReadDeadline: %v", err)
	}
}

// Reset Memcached Connection ReadDeadline.
func (c *Client) ResetMcdMutationReadDeadline() {
	err := c.SetReadDeadline(time.Time{})
	if err != nil {
		logging.Errorf("Error in ResetMcdConnectionReadDeadline: %v", err)
	}
}

// IsHealthy returns true unless the client is belived to have
// difficulty communicating to its server.
//
// This is useful for connection pools where we want to
// non-destructively determine that a connection may be reused.
func (c Client) IsHealthy() bool {
	return c.healthy
}

// Send a custom request and get the response.
func (c *Client) Send(req *transport.MCRequest) (rv *transport.MCResponse, err error) {
	c.SetMcdConnectionDeadline()
	defer c.ResetMcdConnectionDeadline()

	_, err = transmitRequest(c.conn, req)
	if err != nil {
		c.healthy = false
		return
	}
	resp, _, err := getResponse(c.conn, c.hdrBuf)
	c.healthy = !transport.IsFatal(err)
	return resp, err
}

// Transmit send a request, but does not wait for a response.
func (c *Client) Transmit(req *transport.MCRequest) error {
	_, err := transmitRequest(c.conn, req)
	if err != nil {
		c.healthy = false
	}
	return err
}

// TransmitResponse send a request, but does not wait for a response.
func (c *Client) TransmitResponse(res *transport.MCResponse) error {
	_, err := transmitResponse(c.conn, res)
	if err != nil {
		c.healthy = false
	}
	return err
}

// Receive a response
func (c *Client) Receive() (*transport.MCResponse, error) {
	resp, _, err := getResponse(c.conn, c.hdrBuf)
	if err != nil {
		c.healthy = false
	}
	return resp, err
}

func (c *Client) ReceiveInBuf(res *transport.MCResponse, buf []byte) error {
	if c.conn == nil {
		return errNoConn
	}
	_, err := res.Receive2(c.conn, buf[:transport.HDR_LEN], buf[transport.HDR_LEN:])
	if err == nil && res.Status != transport.SUCCESS {
		err = res
	}
	if err != nil {
		c.healthy = false
	}
	return err
}

// Get the value for a key.
func (c *Client) Get(vb uint16, key string) (*transport.MCResponse, error) {
	return c.Send(&transport.MCRequest{
		Opcode:  transport.GET,
		VBucket: vb,
		Key:     []byte(key),
	})
}

func IsUnknownScopeOrCollection(e error) bool {
	return transport.IsUnknownScopeOrCollection(e)
}

// Del deletes a key.
func (c *Client) Del(vb uint16, key string) (*transport.MCResponse, error) {
	return c.Send(&transport.MCRequest{
		Opcode:  transport.DELETE,
		VBucket: vb,
		Key:     []byte(key)})
}

// AuthList lists SASL auth mechanisms.
func (c *Client) AuthList() (*transport.MCResponse, error) {
	return c.Send(&transport.MCRequest{
		Opcode: transport.SASL_LIST_MECHS})
}

// Auth performs SASL PLAIN authentication against the server.
func (c *Client) Auth(user, pass string) (*transport.MCResponse, error) {
	res, err := c.AuthList()

	if err != nil {
		return res, err
	}

	authMech := string(res.Body)
	if strings.Index(authMech, "PLAIN") != -1 {
		return c.Send(&transport.MCRequest{
			Opcode: transport.SASL_AUTH,
			Key:    []byte("PLAIN"),
			Body:   []byte(fmt.Sprintf("\x00%s\x00%s", user, pass))})
	}
	return res, fmt.Errorf("auth mechanism PLAIN not supported")
}

// SelectBucket for this connection.
func (c *Client) SelectBucket(bucket string) (*transport.MCResponse, error) {

	return c.Send(&transport.MCRequest{
		Opcode: transport.SELECT_BUCKET,
		Key:    []byte(fmt.Sprintf("%s", bucket))})
}

func (c *Client) store(opcode transport.CommandCode, vb uint16,
	key string, flags int, exp int, body []byte) (*transport.MCResponse, error) {

	req := &transport.MCRequest{
		Opcode:  opcode,
		VBucket: vb,
		Key:     []byte(key),
		Cas:     0,
		Opaque:  0,
		Extras:  []byte{0, 0, 0, 0, 0, 0, 0, 0},
		Body:    body}

	binary.BigEndian.PutUint64(req.Extras, uint64(flags)<<32|uint64(exp))
	return c.Send(req)
}

func (c *Client) storeCas(opcode transport.CommandCode, vb uint16,
	key string, flags int, exp int, cas uint64, body []byte) (*transport.MCResponse, error) {

	req := &transport.MCRequest{
		Opcode:  opcode,
		VBucket: vb,
		Key:     []byte(key),
		Cas:     cas,
		Opaque:  0,
		Extras:  []byte{0, 0, 0, 0, 0, 0, 0, 0},
		Body:    body}

	binary.BigEndian.PutUint64(req.Extras, uint64(flags)<<32|uint64(exp))
	return c.Send(req)
}

// Incr increments the value at the given key.
func (c *Client) Incr(vb uint16, key string,
	amt, def uint64, exp int) (uint64, error) {

	req := &transport.MCRequest{
		Opcode:  transport.INCREMENT,
		VBucket: vb,
		Key:     []byte(key),
		Extras:  make([]byte, 8+8+4),
	}
	binary.BigEndian.PutUint64(req.Extras[:8], amt)
	binary.BigEndian.PutUint64(req.Extras[8:16], def)
	binary.BigEndian.PutUint32(req.Extras[16:20], uint32(exp))

	resp, err := c.Send(req)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(resp.Body), nil
}

// Add a value for a key (store if not exists).
func (c *Client) Add(vb uint16, key string, flags int, exp int,
	body []byte) (*transport.MCResponse, error) {
	return c.store(transport.ADD, vb, key, flags, exp, body)
}

// Set the value for a key.
func (c *Client) Set(vb uint16, key string, flags int, exp int,
	body []byte) (*transport.MCResponse, error) {
	return c.store(transport.SET, vb, key, flags, exp, body)
}

// SetCas set the value for a key with cas
func (c *Client) SetCas(vb uint16, key string, flags int, exp int, cas uint64,
	body []byte) (*transport.MCResponse, error) {
	return c.storeCas(transport.SET, vb, key, flags, exp, cas, body)
}

// Append data to the value of a key.
func (c *Client) Append(vb uint16, key string, data []byte) (*transport.MCResponse, error) {
	req := &transport.MCRequest{
		Opcode:  transport.APPEND,
		VBucket: vb,
		Key:     []byte(key),
		Cas:     0,
		Opaque:  0,
		Body:    data}

	return c.Send(req)
}

// GetBulk gets keys in bulk
func (c *Client) GetBulk(vb uint16, keys []string) (map[string]*transport.MCResponse, error) {
	rv := map[string]*transport.MCResponse{}
	going := true

	defer func() {
		going = false
	}()

	errch := make(chan error, 2)

	go func() {
		defer func() { errch <- nil }()
		for going {
			res, err := c.Receive()
			if err != nil {
				errch <- err
				return
			}
			switch res.Opcode {
			case transport.GET:
				going = false
			case transport.GETQ:
			default:
				arg1 := logging.TagUD(res)
				logging.Fatalf("Unexpected opcode in GETQ response: %+v", arg1)
			}
			rv[keys[res.Opaque]] = res
		}
	}()

	for i, k := range keys {
		op := transport.GETQ
		if i == len(keys)-1 {
			op = transport.GET
		}
		err := c.Transmit(&transport.MCRequest{
			Opcode:  op,
			VBucket: vb,
			Key:     []byte(k),
			Opaque:  uint32(i),
		})
		if err != nil {
			return rv, err
		}
	}

	return rv, <-errch
}

// ObservedStatus is the type reported by the Observe method
type ObservedStatus uint8

// Observation status values.
const (
	ObservedNotPersisted     = ObservedStatus(0x00) // found, not persisted
	ObservedPersisted        = ObservedStatus(0x01) // found, persisted
	ObservedNotFound         = ObservedStatus(0x80) // not found (or a persisted delete)
	ObservedLogicallyDeleted = ObservedStatus(0x81) // pending deletion (not persisted yet)
)

// ObserveResult represents the data obtained by an Observe call
type ObserveResult struct {
	Status          ObservedStatus // Whether the value has been persisted/deleted
	Cas             uint64         // Current value's CAS
	PersistenceTime time.Duration  // Node's average time to persist a value
	ReplicationTime time.Duration  // Node's average time to replicate a value
}

// Observe gets the persistence/replication/CAS state of a key
func (c *Client) Observe(vb uint16, key string) (result ObserveResult, err error) {
	// http://www.couchbase.com/wiki/display/couchbase/Observe
	body := make([]byte, 4+len(key))
	binary.BigEndian.PutUint16(body[0:2], vb)
	binary.BigEndian.PutUint16(body[2:4], uint16(len(key)))
	copy(body[4:4+len(key)], key)

	res, err := c.Send(&transport.MCRequest{
		Opcode:  transport.OBSERVE,
		VBucket: vb,
		Body:    body,
	})
	if err != nil {
		return
	}

	// Parse the response data from the body:
	if len(res.Body) < 2+2+1 {
		err = io.ErrUnexpectedEOF
		return
	}
	outVb := binary.BigEndian.Uint16(res.Body[0:2])
	keyLen := binary.BigEndian.Uint16(res.Body[2:4])
	if len(res.Body) < 2+2+int(keyLen)+1+8 {
		err = io.ErrUnexpectedEOF
		return
	}
	outKey := string(res.Body[4 : 4+keyLen])
	if outVb != vb || outKey != key {
		err = fmt.Errorf("observe returned wrong vbucket/key: %d/%q", outVb, outKey)
		return
	}
	result.Status = ObservedStatus(res.Body[4+keyLen])
	result.Cas = binary.BigEndian.Uint64(res.Body[5+keyLen:])
	// The response reuses the Cas field to store time statistics:
	result.PersistenceTime = time.Duration(res.Cas>>32) * time.Millisecond
	result.ReplicationTime = time.Duration(res.Cas&math.MaxUint32) * time.Millisecond
	return
}

// CheckPersistence checks whether a stored value has been persisted to disk yet.
func (result ObserveResult) CheckPersistence(cas uint64, deletion bool) (persisted bool, overwritten bool) {
	switch {
	case result.Status == ObservedNotFound && deletion:
		persisted = true
	case result.Cas != cas:
		overwritten = true
	case result.Status == ObservedPersisted:
		persisted = true
	}
	return
}

// CasOp is the type of operation to perform on this CAS loop.
type CasOp uint8

const (
	// CASStore instructs the server to store the new value normally
	CASStore = CasOp(iota)
	// CASQuit instructs the client to stop attempting to CAS, leaving value untouched
	CASQuit
	// CASDelete instructs the server to delete the current value
	CASDelete
)

// User specified termination is returned as an error.
func (c CasOp) Error() string {
	switch c {
	case CASStore:
		return "CAS store"
	case CASQuit:
		return "CAS quit"
	case CASDelete:
		return "CAS delete"
	}
	panic("Unhandled value")
}

//////// CAS TRANSFORM

// CASState tracks the state of CAS over several operations.
//
// This is used directly by CASNext and indirectly by CAS
type CASState struct {
	initialized bool   // false on the first call to CASNext, then true
	Value       []byte // Current value of key; update in place to new value
	Cas         uint64 // Current CAS value of key
	Exists      bool   // Does a value exist for the key? (If not, Value will be nil)
	Err         error  // Error, if any, after CASNext returns false
	resp        *transport.MCResponse
}

// CASNext is a non-callback, loop-based version of CAS method.
//
//  Usage is like this:
//
// var state memcached.CASState
// for client.CASNext(vb, key, exp, &state) {
//     state.Value = some_mutation(state.Value)
// }
// if state.Err != nil { ... }
func (c *Client) CASNext(vb uint16, k string, exp int, state *CASState) bool {
	if state.initialized {
		if !state.Exists {
			// Adding a new key:
			if state.Value == nil {
				state.Cas = 0
				return false // no-op (delete of non-existent value)
			}
			state.resp, state.Err = c.Add(vb, k, 0, exp, state.Value)
		} else {
			// Updating / deleting a key:
			req := &transport.MCRequest{
				Opcode:  transport.DELETE,
				VBucket: vb,
				Key:     []byte(k),
				Cas:     state.Cas}
			if state.Value != nil {
				req.Opcode = transport.SET
				req.Opaque = 0
				req.Extras = []byte{0, 0, 0, 0, 0, 0, 0, 0}
				req.Body = state.Value

				flags := 0
				exp := 0 // ??? Should we use initialexp here instead?
				binary.BigEndian.PutUint64(req.Extras, uint64(flags)<<32|uint64(exp))
			}
			state.resp, state.Err = c.Send(req)
		}

		// If the response status is KEY_EEXISTS or NOT_STORED there's a conflict and we'll need to
		// get the new value (below). Otherwise, we're done (either success or failure) so return:
		if !(state.resp != nil && (state.resp.Status == transport.KEY_EEXISTS ||
			state.resp.Status == transport.NOT_STORED)) {
			state.Cas = state.resp.Cas
			return false // either success or fatal error
		}
	}

	// Initial call, or after a conflict: GET the current value and CAS and return them:
	state.initialized = true
	if state.resp, state.Err = c.Get(vb, k); state.Err == nil {
		state.Exists = true
		state.Value = state.resp.Body
		state.Cas = state.resp.Cas
	} else if state.resp != nil && state.resp.Status == transport.KEY_ENOENT {
		state.Err = nil
		state.Exists = false
		state.Value = nil
		state.Cas = 0
	} else {
		return false // fatal error
	}
	return true // keep going...
}

// CasFunc is type type of function to perform a CAS transform.
//
// Input is the current value, or nil if no value exists.
// The function should return the new value (if any) to set, and the store/quit/delete operation.
type CasFunc func(current []byte) ([]byte, CasOp)

// CAS performs a CAS transform with the given function.
//
// If the value does not exist, a nil current value will be sent to f.
func (c *Client) CAS(vb uint16, k string, f CasFunc,
	initexp int) (*transport.MCResponse, error) {
	var state CASState
	for c.CASNext(vb, k, initexp, &state) {
		newValue, operation := f(state.Value)
		if operation == CASQuit || (operation == CASDelete && state.Value == nil) {
			return nil, operation
		}
		state.Value = newValue
	}
	return state.resp, state.Err
}

// StatValue is one of the stats returned from the Stats method.
type StatValue struct {
	// The stat key
	Key string
	// The stat value
	Val string
}

// Stats requests server-side stats.
//
// Use "" as the stat key for toplevel stats.
func (c *Client) Stats(key string) ([]StatValue, error) {
	rv := make([]StatValue, 0, 128)

	req := &transport.MCRequest{
		Opcode: transport.STAT,
		Key:    []byte(key),
		Opaque: 918494,
	}

	_, err := transmitRequest(c.conn, req)
	if err != nil {
		return rv, err
	}

	for {
		res, _, err := getResponse(c.conn, c.hdrBuf)
		if err != nil {
			return rv, err
		}
		k := string(res.Key)
		if k == "" {
			break
		}
		rv = append(rv, StatValue{
			Key: k,
			Val: string(res.Body),
		})
	}

	return rv, nil
}

// StatsMap requests server-side stats similarly to Stats, but returns
// them as a map.
//
// Use "" as the stat key for toplevel stats.
func (c *Client) StatsMap(key string) (map[string]string, error) {
	rv := make(map[string]string)
	st, err := c.Stats(key)
	if err != nil {
		return rv, err
	}
	for _, sv := range st {
		rv[sv.Key] = sv.Val
	}
	return rv, nil
}

// Hijack exposes the underlying connection from this client.
//
// It also marks the connection as unhealthy since the client will
// have lost control over the connection and can't otherwise verify
// things are in good shape for connection pools.
func (c *Client) Hijack() io.ReadWriteCloser {
	c.healthy = false
	return c.conn
}

func (c *Client) GetLocalAddr() string {
	if c != nil && c.conn != nil {
		return c.conn.(net.Conn).LocalAddr().String()
	}
	return ""
}

func (c *Client) GetRemoteAddr() string {
	if c != nil && c.conn != nil {
		return c.conn.(net.Conn).RemoteAddr().String()
	}
	return ""
}

func (c *Client) EnableCollections(clientName string) error {
	if !c.IsCollectionsEnabled() {
		if resp, err := c.sendHeloCollections(clientName); err != nil {
			return err
		} else {
			opcode := resp.Opcode
			body := resp.Body
			if opcode != transport.HELO {
				logging.Errorf("Memcached HELO for %v (feature_collections) opcode = %v. Expecting opcode = 0x1f", clientName, opcode)
				return ErrorEnableCollections
			} else if (len(body) != 2) || (body[0] != 0x00 && body[1] != transport.FEATURE_COLLECTIONS) {
				logging.Errorf("Memcached HELO for %v (feature_collections) body = %v. Expecting body = 0x0012", clientName, body)
				return ErrorCollectionsNotEnabled
			}
		}
		c.SetCollectionsEnabled()
	}
	return nil
}

func (c *Client) sendHeloCollections(name string) (resp *transport.MCResponse, err error) {
	req := &transport.MCRequest{
		Opcode: transport.HELO,
		Key:    ([]byte)(name),
		Body:   []byte{0x00, transport.FEATURE_COLLECTIONS},
	}

	return c.Send(req)
}

func (c *Client) EnableJSON(clientName string) error {
	if resp, err := c.sendEnableJSON(clientName); err != nil {
		return err
	} else {
		opcode := resp.Opcode
		body := resp.Body
		if opcode != transport.HELO {
			logging.Errorf("Memcached HELO for %v (feature_json) opcode = %v. Expecting opcode = 0x1f", clientName, opcode)
			return ErrorEnableJSON
		} else if (len(body) != 2) || (body[0] != 0x00 && body[1] != transport.FEATURE_JSON) {
			logging.Errorf("Memcached HELO for %v (feature_json) body = %v. Expecting body = 0x0b", clientName, body)
			return ErrorJSONNotEnabled
		}
	}
	return nil
}

func (c *Client) sendEnableJSON(name string) (resp *transport.MCResponse, err error) {
	req := &transport.MCRequest{
		Opcode: transport.HELO,
		Key:    ([]byte)(name),
		Body:   []byte{0x00, transport.FEATURE_JSON},
	}

	return c.Send(req)
}

func (c *Client) CreateRandomScan(vb uint16, collId string, sampleSize int64) (
	*transport.MCResponse, error) {

	req := &transport.MCRequest{
		Opcode:   transport.CREATE_RANGE_SCAN,
		VBucket:  vb,
		Datatype: dcpJSON,
		Opaque:   opaqueRandomScan,
	}

	s := make(map[string]interface{})
	seed := uint32(rand.Int())

	s["seed"] = seed
	s["samples"] = sampleSize
	m := make(map[string]interface{})
	m["collection"] = collId
	m["sampling"] = s
	m["key_only"] = false
	req.Body, _ = json.Marshal(m)

	return c.Send(req)
}

func (c *Client) ContinueRangeScan(vb uint16, uuid []byte, items uint32, timeout uint32, maxSize uint32) error {

	req := &transport.MCRequest{
		Opcode:  transport.CONTINUE_RANGE_SCAN,
		VBucket: vb,
		Extras:  make([]byte, 28),
		Opaque:  opaqueRandomScan,
	}
	copy(req.Extras, uuid)
	binary.BigEndian.PutUint32(req.Extras[16:], items)
	binary.BigEndian.PutUint32(req.Extras[20:], timeout)
	binary.BigEndian.PutUint32(req.Extras[24:], maxSize)

	c.SetMcdConnectionDeadline()
	defer c.ResetMcdConnectionDeadline()

	return c.Transmit(req)
}

func (c *Client) CancelRangeScan(vb uint16, uuid []byte) (
	*transport.MCResponse, error) {

	req := &transport.MCRequest{
		Opcode:  transport.CANCEL_RANGE_SCAN,
		VBucket: vb,
		Extras:  make([]byte, 16),
		Opaque:  opaqueRandomScan,
	}
	copy(req.Extras, uuid)
	return c.Send(req)
}
