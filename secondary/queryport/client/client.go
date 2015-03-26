package client

import "errors"
import "time"

import "github.com/couchbase/indexing/secondary/logging"
import "github.com/couchbase/indexing/secondary/common"
import "code.google.com/p/goprotobuf/proto"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
import mclient "github.com/couchbase/indexing/secondary/manager/client"

// TODO:
// - Timeit() uses the wall-clock time instead of process-time to compute
//   load. This is very crude.

// ErrorProtocol
var ErrorProtocol = errors.New("queryport.client.protocol")

// ErrorNoHost
var ErrorNoHost = errors.New("queryport.client.noHost")

// ErrorEmptyDeployment
var ErrorEmptyDeployment = errors.New("queryport.client.emptyDeployment")

// ErrorManyDeployment
var ErrorManyDeployment = errors.New("queryport.client.manyDeployment")

// ErrorInvalidDeploymentNode
var ErrorInvalidDeploymentNode = errors.New("queryport.client.invdDeployPlan")

// ErrorIndexNotFound
var ErrorIndexNotFound = errors.New("queryport.indexNotFound")

// ErrorInstanceNotFound
var ErrorInstanceNotFound = errors.New("queryport.instanceNotFound")

// ErrorIndexNotReady
var ErrorIndexNotReady = errors.New("queryport.indexNotReady")

// ErrorClientUninitialized
var ErrorClientUninitialized = errors.New("queryport.clientUninitialized")

// ResponseHandler shall interpret response packets from server
// and handle them. If handler is not interested in receiving any
// more response it shall return false, else it shall continue
// until *protobufEncode.StreamEndResponse message is received.
type ResponseHandler func(resp ResponseReader) bool

// ResponseReader to obtain the actual data returned from server,
// handlers, should first call Error() and then call GetEntries().
type ResponseReader interface {
	// GetEntries returns a list of secondary-key and corresponding
	// primary-key if returned value is nil, then there are no more
	// entries for this query.
	GetEntries() ([]common.SecondaryKey, [][]byte, error)

	// Error returns the error value, if nil there is no error.
	Error() error
}

// Remoteaddr string in the shape of "<host:port>"
type Remoteaddr string

// Inclusion specifier for range queries.
type Inclusion uint32

const (
	// Neither does not include low-key and high-key
	Neither Inclusion = iota
	// Low includes low-key but does not include high-key
	Low
	// High includes high-key but does not include low-key
	High
	// Both includes both low-key and high-key
	Both
)

// BridgeAccessor for Create,Drop,List,Refresh operations.
type BridgeAccessor interface {
	// Synchronously update current server metadata to the client
	// A Refresh call followed by a Sync() ensures that client is
	// up to date wrt the server.
	Sync() error

	// Refresh shall refresh to latest set of index managed by GSI
	// cluster, cache it locally and return the list of index.
	Refresh() ([]*mclient.IndexMetadata, error)

	// Nodes shall return a map of adminport and queryport for indexer
	// nodes.
	Nodes() (map[string]string, error)

	// CreateIndex and return defnID of created index.
	// name
	//      index name
	// bucket
	//      bucket name in which index is defined.
	// using
	//      token should always be GSI.
	// exprType
	//      token specifies how in interpret partnExpr, whereExpr, secExprs
	// partnExpr
	//      marshalled expression of type `exprType` that emits partition
	//      value from a kv-document.
	// whereExpr
	//      marshalled predicate-expression of type `exprType` that emits
	//      a boolean from a kv-document.
	// secExprs
	//      marshalled list of expression of type `exprType` that emits
	//      an array of secondary-key values from a kv-document.
	// isPrimary
	//      specify whether the index is created on docid.
	// with
	//      JSON marshalled description about index deployment (and more...).
	CreateIndex(
		name, bucket, using, exprType, partnExpr, whereExpr string,
		secExprs []string, isPrimary bool,
		with []byte) (defnID uint64, err error)

	// BuildIndexes to build a deferred set of indexes. This call implies
	// that indexes specified are already created.
	BuildIndexes(defnIDs []uint64) error

	// DropIndex to drop index specified by `defnID`.
	// - if index is in deferred build state, it shall be removed
	//   from deferred list.
	DropIndex(defnID uint64) error

	// GetScanports shall return list of queryports for all indexer in
	// the cluster.
	GetScanports() (queryports []string)

	// GetScanport shall fetch queryport address for indexer, under least
	// load, hosting index `defnID` or an equivalent of `defnID`
	GetScanport(defnID uint64) (queryport string, targetDefnID uint64, ok bool)

	// GetIndex will return the index-definition structure for defnID.
	GetIndexDefn(defnID uint64) *common.IndexDefn

	// IndexState returns the current state of index `defnID` and error.
	IndexState(defnID uint64) (common.IndexState, error)

	// Timeit will add `value` to incrementalAvg for index-load.
	Timeit(defnID uint64, value float64)

	// Close this accessor.
	Close()
}

// GsiAccessor for index operation on GSI cluster.
type GsiAccessor interface {
	BridgeAccessor

	// LookupStatistics for a single secondary-key.
	LookupStatistics(
		defnID uint64, v common.SecondaryKey) (common.IndexStatistics, error)

	// RangeStatistics for index range.
	RangeStatistics(
		defnID uint64, low, high common.SecondaryKey,
		inclusion Inclusion) (common.IndexStatistics, error)

	// Lookup scan index between low and high.
	Lookup(
		defnID uint64, values []common.SecondaryKey,
		distinct bool, limit int64,
		cons common.Consistency, vector *TsConsistency,
		callb ResponseHandler) error

	// Range scan index between low and high.
	Range(
		defnID uint64, low, high common.SecondaryKey,
		inclusion Inclusion, distinct bool, limit int64,
		cons common.Consistency, vector *TsConsistency,
		callb ResponseHandler) error

	// ScanAll for full table scan.
	ScanAll(
		defnID uint64, limit int64,
		cons common.Consistency, vector *TsConsistency,
		callb ResponseHandler) error

	// CountLookup of all entries in index.
	CountLookup(
		defnID uint64,
		cons common.Consistency, vector *TsConsistency) (int64, error)

	// CountRange of all entries in index.
	CountRange(
		defnID uint64,
		cons common.Consistency, vector *TsConsistency) (int64, error)
}

var useMetadataProvider = true

// GsiClient for accessing GSI cluster. The client shall
// use `adminport` for meta-data operation and `queryport`
// for index-scan related operations.
type GsiClient struct {
	bridge       BridgeAccessor // manages adminport
	cluster      string
	maxvb        int
	config       common.Config
	queryClients map[string]*gsiScanClient
}

// NewGsiClient returns client to access GSI cluster.
func NewGsiClient(
	cluster string, config common.Config) (c *GsiClient, err error) {

	if useMetadataProvider {
		c, err = makeWithMetaProvider(cluster, config)
	} else {
		c, err = makeWithCbq(cluster, config)
	}
	if err != nil {
		return nil, err
	}
	c.maxvb = -1
	c.Refresh()
	return c, nil
}

// IndexState implements BridgeAccessor{} interface.
func (c *GsiClient) IndexState(defnID uint64) (common.IndexState, error) {
	if c.bridge == nil {
		return common.INDEX_STATE_ERROR, ErrorClientUninitialized
	}
	return c.bridge.IndexState(defnID)
}

// Sync implements BridgeAccessor{} interface.
func (c *GsiClient) Sync() error {
	if c.bridge == nil {
		return ErrorClientUninitialized
	}
	return c.bridge.Sync()
}

// Refresh implements BridgeAccessor{} interface.
func (c *GsiClient) Refresh() ([]*mclient.IndexMetadata, error) {
	if c.bridge == nil {
		return nil, ErrorClientUninitialized
	}
	return c.bridge.Refresh()
}

// Nodes implements BridgeAccessor{} interface.
func (c *GsiClient) Nodes() (map[string]string, error) {
	if c.bridge == nil {
		return nil, ErrorClientUninitialized
	}
	return c.bridge.Nodes()
}

// BucketTs will return the current vbucket-timestamp.
func (c *GsiClient) BucketTs(bucketn string) (*TsConsistency, error) {
	b, err := common.ConnectBucket(c.cluster, "default" /*pooln*/, bucketn)
	if err != nil {
		return nil, err
	}
	defer b.Close()
	if c.maxvb == -1 {
		if c.maxvb, err = common.MaxVbuckets(b); err != nil {
			return nil, err
		}
	}
	seqnos, vbuuids := common.BucketTs(b, c.maxvb)
	vbnos := make([]uint16, c.maxvb)
	for i := range vbnos {
		vbnos[i] = uint16(i)
	}
	return NewTsConsistency(vbnos, seqnos, vbuuids), nil
}

// CreateIndex implements BridgeAccessor{} interface.
func (c *GsiClient) CreateIndex(
	name, bucket, using, exprType, partnExpr, whereExpr string,
	secExprs []string, isPrimary bool,
	with []byte) (defnID uint64, err error) {

	if c.bridge == nil {
		return defnID, ErrorClientUninitialized
	}
	defnID, err = c.bridge.CreateIndex(
		name, bucket, using, exprType, partnExpr, whereExpr,
		secExprs, isPrimary, with)
	return defnID, err
}

// BuildIndexes implements BridgeAccessor{} interface.
func (c *GsiClient) BuildIndexes(defnIDs []uint64) error {
	if c.bridge == nil {
		return ErrorClientUninitialized
	}
	return c.bridge.BuildIndexes(defnIDs)
}

// DropIndex implements BridgeAccessor{} interface.
func (c *GsiClient) DropIndex(defnID uint64) error {
	if c.bridge == nil {
		return ErrorClientUninitialized
	}
	return c.bridge.DropIndex(defnID)
}

// LookupStatistics for a single secondary-key.
func (c *GsiClient) LookupStatistics(
	defnID uint64, value common.SecondaryKey) (common.IndexStatistics, error) {

	if c.bridge == nil {
		return nil, ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err := c.bridge.IndexState(defnID); err != nil {
		return nil, err
	}

	var stats common.IndexStatistics
	var err error
	err = c.doScan(defnID, func(qc *gsiScanClient, targetDefnID uint64) error {
		stats, err = qc.LookupStatistics(targetDefnID, value)
		return err
	})
	return stats, err
}

// RangeStatistics for index range.
func (c *GsiClient) RangeStatistics(
	defnID uint64, low, high common.SecondaryKey,
	inclusion Inclusion) (common.IndexStatistics, error) {

	if c.bridge == nil {
		return nil, ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err := c.bridge.IndexState(defnID); err != nil {
		return nil, err
	}
	var stats common.IndexStatistics
	var err error
	err = c.doScan(defnID, func(qc *gsiScanClient, targetDefnID uint64) error {
		stats, err = qc.RangeStatistics(targetDefnID, low, high, inclusion)
		return err
	})
	return stats, err
}

// Lookup scan index between low and high.
func (c *GsiClient) Lookup(
	defnID uint64, values []common.SecondaryKey,
	distinct bool, limit int64,
	cons common.Consistency, vector *TsConsistency,
	callb ResponseHandler) error {

	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err := c.bridge.IndexState(defnID); err != nil {
		protoResp := &protobuf.ResponseStream{
			Err: &protobuf.Error{Error: proto.String(err.Error())},
		}
		callb(protoResp)
		return nil
	}
	return c.doScan(defnID, func(qc *gsiScanClient, targetDefnID uint64) (err error) {
		index := c.bridge.GetIndexDefn(targetDefnID)
		if cons == common.SessionConsistency && vector == nil {
			if vector, err = c.BucketTs(index.Bucket); err != nil {
				return err
			}
		}
		return qc.Lookup(targetDefnID, values, distinct, limit, cons, vector, callb)
	})
}

// Range scan index between low and high.
func (c *GsiClient) Range(
	defnID uint64, low, high common.SecondaryKey,
	inclusion Inclusion, distinct bool, limit int64,
	cons common.Consistency, vector *TsConsistency,
	callb ResponseHandler) error {

	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err := c.bridge.IndexState(defnID); err != nil {
		protoResp := &protobuf.ResponseStream{
			Err: &protobuf.Error{Error: proto.String(err.Error())},
		}
		callb(protoResp)
		return nil
	}
	return c.doScan(defnID, func(qc *gsiScanClient, targetDefnID uint64) (err error) {
		index := c.bridge.GetIndexDefn(targetDefnID)
		if cons == common.SessionConsistency && vector == nil {
			if vector, err = c.BucketTs(index.Bucket); err != nil {
				return err
			}
		}
		return qc.Range(
			targetDefnID, low, high, inclusion, distinct, limit, cons, vector, callb)
	})
}

// ScanAll for full table scan.
func (c *GsiClient) ScanAll(
	defnID uint64, limit int64,
	cons common.Consistency, vector *TsConsistency,
	callb ResponseHandler) error {

	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err := c.bridge.IndexState(defnID); err != nil {
		protoResp := &protobuf.ResponseStream{
			Err: &protobuf.Error{Error: proto.String(err.Error())},
		}
		callb(protoResp)
		return nil
	}
	return c.doScan(defnID, func(qc *gsiScanClient, targetDefnID uint64) (err error) {
		index := c.bridge.GetIndexDefn(targetDefnID)
		if cons == common.SessionConsistency && vector == nil {
			if vector, err = c.BucketTs(index.Bucket); err != nil {
				return err
			}
		}
		return qc.ScanAll(targetDefnID, limit, cons, vector, callb)
	})
}

// CountLookup to count number entries for given set of keys.
func (c *GsiClient) CountLookup(
	defnID uint64, values []common.SecondaryKey,
	cons common.Consistency, vector *TsConsistency) (count int64, err error) {

	if c.bridge == nil {
		return count, ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err := c.bridge.IndexState(defnID); err != nil {
		return 0, err
	}

	err = c.doScan(defnID, func(qc *gsiScanClient, targetDefnID uint64) error {
		index := c.bridge.GetIndexDefn(targetDefnID)
		if cons == common.SessionConsistency && vector == nil {
			if vector, err = c.BucketTs(index.Bucket); err != nil {
				return err
			}
		}
		count, err = qc.CountLookup(targetDefnID, values, cons, vector)
		return err
	})
	return count, err
}

// CountRange to count number entries in the given range.
func (c *GsiClient) CountRange(
	defnID uint64,
	low, high common.SecondaryKey,
	inclusion Inclusion,
	cons common.Consistency, vector *TsConsistency) (count int64, err error) {

	if c.bridge == nil {
		return count, ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err := c.bridge.IndexState(defnID); err != nil {
		return 0, err
	}
	err = c.doScan(defnID, func(qc *gsiScanClient, targetDefnID uint64) error {
		index := c.bridge.GetIndexDefn(targetDefnID)
		if cons == common.SessionConsistency && vector == nil {
			if vector, err = c.BucketTs(index.Bucket); err != nil {
				return err
			}
		}
		count, err = qc.CountRange(targetDefnID, low, high, inclusion, cons, vector)
		return err
	})
	return count, err
}

// Close the client and all open connections with server.
func (c *GsiClient) Close() {
	if c.bridge == nil {
		return
	}
	c.bridge.Close()
	for _, queryClient := range c.queryClients {
		queryClient.Close()
	}
}

func (c *GsiClient) updateScanClients() {
	cache := make(map[string]bool)
	// add new indexer-nodes
	for _, queryport := range c.bridge.GetScanports() {
		cache[queryport] = true
		if _, ok := c.queryClients[queryport]; !ok {
			c.queryClients[queryport] = newGsiScanClient(queryport, c.config)
		}
	}
	// forget removed indexer-nodes.
	for queryport, queryClient := range c.queryClients {
		if _, ok := cache[queryport]; !ok {
			queryClient.Close()
			delete(c.queryClients, queryport)
		}
	}
}

func (c *GsiClient) doScan(
	defnID uint64, callb func(*gsiScanClient, uint64) error) error {

	var qc *gsiScanClient
	var err error
	var ok1, ok2 bool
	var queryport string
	var targetDefnID uint64

	wait := c.config["retryIntervalScanport"].Int()
	retry := c.config["retryScanPort"].Int()
	for i := 0; i < retry; i++ {
		if queryport, targetDefnID, ok1 = c.bridge.GetScanport(defnID); ok1 {
			if qc, ok2 = c.queryClients[queryport]; ok2 {
				begin := time.Now().UnixNano()
				if err = callb(qc, targetDefnID); err == nil {
					c.bridge.Timeit(targetDefnID, float64(time.Now().UnixNano()-begin))
					return nil
				}
			}
		}
		logging.Infof(
			"Retrying scan for index %v (%v %v) ...\n", targetDefnID, ok1, ok2)
		c.updateScanClients()
		time.Sleep(time.Duration(wait) * time.Millisecond)
	}
	if err != nil {
		return err
	}
	return ErrorNoHost
}

// create GSI client using cbqBridge and ScanCoordinator
func makeWithCbq(cluster string, config common.Config) (*GsiClient, error) {

	var err error
	c := &GsiClient{
		cluster:      cluster,
		config:       config,
		queryClients: make(map[string]*gsiScanClient),
	}
	if c.bridge, err = newCbqClient(cluster); err != nil {
		return nil, err
	}
	for _, queryport := range c.bridge.GetScanports() {
		queryClient := newGsiScanClient(queryport, config)
		c.queryClients[queryport] = queryClient
	}
	return c, nil
}

func makeWithMetaProvider(
	cluster string, config common.Config) (c *GsiClient, err error) {

	c = &GsiClient{
		cluster:      cluster,
		config:       config,
		queryClients: make(map[string]*gsiScanClient),
	}
	c.bridge, err = newMetaBridgeClient(cluster, config)
	if err != nil {
		return nil, err
	}
	c.updateScanClients()
	return c, nil
}

//--------------------------
// Consistency and Stability
//--------------------------

// TsConsistency specifies a subset of vbuckets to be used as
// timestamp vector to specify consistency criteria.
//
// Timestamp-vector will be ignored for AnyConsistency, computed
// locally by scan-coordinator or accepted as scan-arguments for
// SessionConsistency.
type TsConsistency struct {
	Vbnos   []uint16
	Seqnos  []uint64
	Vbuuids []uint64
}

// NewTsConsistency returns a new consistency vector object.
func NewTsConsistency(
	vbnos []uint16, seqnos []uint64, vbuuids []uint64) *TsConsistency {

	return &TsConsistency{Vbnos: vbnos, Seqnos: seqnos, Vbuuids: vbuuids}
}

// Override vbucket's {seqno, vbuuid} in the timestamp-vector,
// if vbucket is not present in the vector, append them to vector.
func (ts *TsConsistency) Override(
	vbno uint16, seqno, vbuuid uint64) *TsConsistency {

	for i, vb := range ts.Vbnos {
		if vbno == vb {
			ts.Seqnos[i], ts.Vbuuids[i] = seqno, vbuuid
			return ts
		}
	}
	ts.Vbnos = append(ts.Vbnos, vbno)
	ts.Seqnos = append(ts.Seqnos, seqno)
	ts.Vbuuids = append(ts.Vbuuids, vbuuid)
	return ts
}
