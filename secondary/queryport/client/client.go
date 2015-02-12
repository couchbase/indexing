package client

import "errors"
import "time"

import "github.com/couchbase/indexing/secondary/logging"
import "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbaselabs/goprotobuf/proto"
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
var ErrorInvalidDeploymentNode = errors.New("queryport.client.invalidDeploymentPlan")

// ErrorIndexNotFound
var ErrorIndexNotFound = errors.New("queryport.indexNotFound")

// ErrorInstanceNotFound
var ErrorInstanceNotFound = errors.New("queryport.instanceNotFound")

// ErrorIndexNotReady
var ErrorIndexNotReady = errors.New("queryport.indexNotReady")

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
		with []byte) (common.IndexDefnId, error)

	// BuildIndexes to build a deferred set of indexes. This call implies
	// that indexes specified are already created.
	BuildIndexes(defnIDs []common.IndexDefnId) error

	// DropIndex to drop index specified by `defnID`.
	// - if index is in deferred build state, it shall be removed
	//   from deferred list.
	DropIndex(defnID common.IndexDefnId) error

	// GetScanports shall return list of queryports for all indexer in
	// the cluster.
	GetScanports() (queryports []string)

	// GetScanport shall fetch queryport address for indexer, under least
	// load, hosting index `defnID` or an equivalent of `defnID`
	GetScanport(defnID common.IndexDefnId) (queryport string, ok bool)

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
		distinct bool, limit int64, callb ResponseHandler) error

	// Range scan index between low and high.
	Range(
		defnID uint64, low, high common.SecondaryKey,
		inclusion Inclusion, distinct bool, limit int64,
		callb ResponseHandler) error

	// ScanAll for full table scan.
	ScanAll(defnID uint64, limit int64, callb ResponseHandler) error

	// CountLookup of all entries in index.
	CountLookup(defnID uint64) (int64, error)

	// CountRange of all entries in index.
	CountRange(defnID uint64) (int64, error)
}

var useMetadataProvider = true

// GsiClient for accessing GSI cluster. The client shall
// use `adminport` for meta-data operation and `queryport`
// for index-scan related operations.
type GsiClient struct {
	bridge       BridgeAccessor // manages adminport
	config       common.Config
	queryClients map[string]*gsiScanClient
}

// NewGsiClient returns client to access GSI cluster.
func NewGsiClient(
	cluster string,
	config common.Config) (c *GsiClient, err error) {

	if useMetadataProvider {
		c, err = makeWithMetaProvider(cluster, config)
	} else {
		c, err = makeWithCbq(cluster, config)
	}
	if err != nil {
		return nil, err
	}
	c.Refresh()
	return c, nil
}

// IndexState implements BridgeAccessor{} interface.
func (c *GsiClient) IndexState(defnID uint64) (common.IndexState, error) {
	return c.bridge.IndexState(defnID)
}

// Refresh implements BridgeAccessor{} interface.
func (c *GsiClient) Refresh() ([]*mclient.IndexMetadata, error) {
	return c.bridge.Refresh()
}

// Nodes implements BridgeAccessor{} interface.
func (c *GsiClient) Nodes() (map[string]string, error) {
	return c.bridge.Nodes()
}

// CreateIndex implements BridgeAccessor{} interface.
func (c *GsiClient) CreateIndex(
	name, bucket, using, exprType, partnExpr, whereExpr string,
	secExprs []string, isPrimary bool,
	with []byte) (uint64, error) {

	defnID, err := c.bridge.CreateIndex(
		name, bucket, using, exprType, partnExpr, whereExpr,
		secExprs, isPrimary, with)
	return uint64(defnID), err
}

// BuildIndexes implements BridgeAccessor{} interface.
func (c *GsiClient) BuildIndexes(defnIDs []uint64) error {
	ids := make([]common.IndexDefnId, len(defnIDs))
	for i, id := range defnIDs {
		ids[i] = common.IndexDefnId(id)
	}
	return c.bridge.BuildIndexes(ids)
}

// DropIndex implements BridgeAccessor{} interface.
func (c *GsiClient) DropIndex(defnID uint64) error {
	return c.bridge.DropIndex(common.IndexDefnId(defnID))
}

// LookupStatistics for a single secondary-key.
func (c *GsiClient) LookupStatistics(
	defnID uint64, value common.SecondaryKey) (common.IndexStatistics, error) {

	// check whether the index is present and available.
	if _, err := c.bridge.IndexState(defnID); err != nil {
		return nil, err
	}

	var stats common.IndexStatistics
	var err error
	err = c.doScan(defnID, func(qc *gsiScanClient) error {
		stats, err = qc.LookupStatistics(defnID, value)
		return err
	})
	return stats, err
}

// RangeStatistics for index range.
func (c *GsiClient) RangeStatistics(
	defnID uint64, low, high common.SecondaryKey,
	inclusion Inclusion) (common.IndexStatistics, error) {

	// check whether the index is present and available.
	if _, err := c.bridge.IndexState(defnID); err != nil {
		return nil, err
	}
	var stats common.IndexStatistics
	var err error
	err = c.doScan(defnID, func(qc *gsiScanClient) error {
		stats, err = qc.RangeStatistics(defnID, low, high, inclusion)
		return err
	})
	return stats, err
}

// Lookup scan index between low and high.
func (c *GsiClient) Lookup(
	defnID uint64, values []common.SecondaryKey,
	distinct bool, limit int64, callb ResponseHandler) error {

	// check whether the index is present and available.
	if _, err := c.bridge.IndexState(defnID); err != nil {
		protoResp := &protobuf.ResponseStream{
			Err: &protobuf.Error{Error: proto.String(err.Error())},
		}
		callb(protoResp)
		return nil
	}
	return c.doScan(defnID, func(qc *gsiScanClient) error {
		return qc.Lookup(defnID, values, distinct, limit, callb)
	})
}

// Range scan index between low and high.
func (c *GsiClient) Range(
	defnID uint64, low, high common.SecondaryKey,
	inclusion Inclusion, distinct bool, limit int64,
	callb ResponseHandler) error {

	// check whether the index is present and available.
	if _, err := c.bridge.IndexState(defnID); err != nil {
		protoResp := &protobuf.ResponseStream{
			Err: &protobuf.Error{Error: proto.String(err.Error())},
		}
		callb(protoResp)
		return nil
	}
	return c.doScan(defnID, func(qc *gsiScanClient) error {
		return qc.Range(defnID, low, high, inclusion, distinct, limit, callb)
	})
}

// ScanAll for full table scan.
func (c *GsiClient) ScanAll(
	defnID uint64, limit int64, callb ResponseHandler) error {

	// check whether the index is present and available.
	if _, err := c.bridge.IndexState(defnID); err != nil {
		protoResp := &protobuf.ResponseStream{
			Err: &protobuf.Error{Error: proto.String(err.Error())},
		}
		callb(protoResp)
		return nil
	}
	return c.doScan(defnID, func(qc *gsiScanClient) error {
		return qc.ScanAll(defnID, limit, callb)
	})
}

// CountLookup to count number entries for given set of keys.
func (c *GsiClient) CountLookup(
	defnID uint64, values []common.SecondaryKey) (count int64, err error) {

	// check whether the index is present and available.
	if _, err := c.bridge.IndexState(defnID); err != nil {
		return 0, err
	}
	err = c.doScan(defnID, func(qc *gsiScanClient) error {
		count, err = qc.CountLookup(defnID, values)
		return err
	})
	return count, err
}

// CountRange to count number entries in the given range.
func (c *GsiClient) CountRange(
	defnID uint64,
	low, high common.SecondaryKey, inclusion Inclusion) (count int64, err error) {

	// check whether the index is present and available.
	if _, err := c.bridge.IndexState(defnID); err != nil {
		return 0, err
	}
	err = c.doScan(defnID, func(qc *gsiScanClient) error {
		count, err = qc.CountRange(defnID, low, high, inclusion)
		return err
	})
	return count, err
}

// Close the client and all open connections with server.
func (c *GsiClient) Close() {
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
	defnID uint64, callb func(*gsiScanClient) error) error {

	var qc *gsiScanClient
	var err error
	var ok1, ok2 bool
	var queryport string

	wait := c.config["retryIntervalScanport"].Int()
	retry := c.config["retryScanPort"].Int()
	for i := 0; i < retry; i++ {
		if queryport, ok1 = c.bridge.GetScanport(common.IndexDefnId(defnID)); ok1 {
			if qc, ok2 = c.queryClients[queryport]; ok2 {
				begin := time.Now().UnixNano()
				if err = callb(qc); err == nil {
					c.bridge.Timeit(defnID, float64(time.Now().UnixNano()-begin))
					return nil
				}
			}
		}
		logging.Infof("Retrying scan for index %v (%v %v) ...\n", defnID, ok1, ok2)
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
	cluster string,
	config common.Config) (c *GsiClient, err error) {

	c = &GsiClient{
		config:       config,
		queryClients: make(map[string]*gsiScanClient),
	}
	c.bridge, err = newMetaBridgeClient(cluster)
	if err != nil {
		return nil, err
	}
	c.updateScanClients()
	return c, nil
}
