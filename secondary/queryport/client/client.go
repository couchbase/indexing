package client

import "errors"

import "github.com/couchbase/indexing/secondary/common"
import mclient "github.com/couchbase/indexing/secondary/manager/client"

// ErrorProtocol
var ErrorProtocol = errors.New("queryport.client.protocol")

// ErrorNoHost
var ErrorNoHost = errors.New("queryport.client.noHost")

// ErrorIndexNotFound
var ErrorIndexNotFound = errors.New("queryport.indexNotFound")

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
	// Refresh will refresh to latest set of index managed by GSI
	// cluster and return the list of index.
	Refresh() ([]*mclient.IndexMetadata, error)

	// CreateIndex and return defnID of created index.
	CreateIndex(
		name, bucket, using, exprType, partnExpr, whereExpr string,
		secExprs []string, isPrimary bool) (common.IndexDefnId, error)

	// DropIndex from GSI cluster.
	DropIndex(defnID common.IndexDefnId) error

	// GetQueryports will return list of queryports for all indexer in
	// the cluster.
	GetQueryports() (queryports []string)

	// GetQueryport will fetch queryport address for indexer hosting
	// index `defnID`
	GetQueryport(defnID common.IndexDefnId) (queryport string, ok bool)

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

// TODO: integration with MetadataProvider
var useMetadataProvider = false

// GsiClient for accessing GSI cluster. The client will
// use `adminport` for meta-data operation and `queryport`
// for index-scan related operations.
type GsiClient struct {
	bridge       BridgeAccessor // manages adminport
	queryClients map[string]*gsiScanClient
}

// NewGsiClient returns client to access GSI cluster.
func NewGsiClient(
	cluster, serviceAddr string,
	config common.Config) (c *GsiClient, err error) {

	if useMetadataProvider {
		c, err = makeWithMetaProvider(cluster, serviceAddr, config)
	} else {
		c, err = makeWithCbq(config)
	}
	if err != nil {
		return nil, err
	}
	c.Refresh()
	return c, nil
}

// Refresh implements BridgeAccessor{} interface.
func (c *GsiClient) Refresh() ([]*mclient.IndexMetadata, error) {
	return c.bridge.Refresh()
}

// CreateIndex implements BridgeAccessor{} interface.
func (c *GsiClient) CreateIndex(
	name, bucket, using, exprType, partnExpr, whereExpr string,
	secExprs []string, isPrimary bool) (common.IndexDefnId, error) {

	return c.bridge.CreateIndex(
		name, bucket, using, exprType, partnExpr, whereExpr,
		secExprs, isPrimary)
}

// DropIndex implements BridgeAccessor{} interface.
func (c *GsiClient) DropIndex(defnID common.IndexDefnId) error {
	return c.bridge.DropIndex(defnID)
}

// LookupStatistics for a single secondary-key.
func (c *GsiClient) LookupStatistics(
	defnID uint64, value common.SecondaryKey) (common.IndexStatistics, error) {

	queryport, ok := c.bridge.GetQueryport(common.IndexDefnId(defnID))
	if !ok {
		return nil, ErrorNoHost
	}
	qc := c.queryClients[queryport]
	return qc.LookupStatistics(defnID, value)
}

// RangeStatistics for index range.
func (c *GsiClient) RangeStatistics(
	defnID uint64, low, high common.SecondaryKey,
	inclusion Inclusion) (common.IndexStatistics, error) {

	queryport, ok := c.bridge.GetQueryport(common.IndexDefnId(defnID))
	if !ok {
		return nil, ErrorNoHost
	}
	qc := c.queryClients[queryport]
	return qc.RangeStatistics(defnID, low, high, inclusion)
}

// Lookup scan index between low and high.
func (c *GsiClient) Lookup(
	defnID uint64, values []common.SecondaryKey,
	distinct bool, limit int64, callb ResponseHandler) error {

	queryport, ok := c.bridge.GetQueryport(common.IndexDefnId(defnID))
	if !ok {
		return ErrorNoHost
	}
	qc := c.queryClients[queryport]
	return qc.Lookup(defnID, values, distinct, limit, callb)
}

// Range scan index between low and high.
func (c *GsiClient) Range(
	defnID uint64, low, high common.SecondaryKey,
	inclusion Inclusion, distinct bool, limit int64,
	callb ResponseHandler) error {

	queryport, ok := c.bridge.GetQueryport(common.IndexDefnId(defnID))
	if !ok {
		return ErrorNoHost
	}
	qc := c.queryClients[queryport]
	return qc.Range(defnID, low, high, inclusion, distinct, limit, callb)
}

// ScanAll for full table scan.
func (c *GsiClient) ScanAll(
	defnID uint64, limit int64, callb ResponseHandler) error {

	queryport, ok := c.bridge.GetQueryport(common.IndexDefnId(defnID))
	if !ok {
		return ErrorNoHost
	}
	qc := c.queryClients[queryport]
	return qc.ScanAll(defnID, limit, callb)
}

// CountLookup to count number entries for given set of keys.
func (c *GsiClient) CountLookup(
	defnID uint64, values []common.SecondaryKey) (int64, error) {

	queryport, ok := c.bridge.GetQueryport(common.IndexDefnId(defnID))
	if !ok {
		return 0, ErrorNoHost
	}
	qc := c.queryClients[queryport]
	return qc.CountLookup(defnID, values)
}

// CountRange to count number entries in the given range.
func (c *GsiClient) CountRange(
	defnID uint64,
	low, high common.SecondaryKey, inclusion Inclusion) (int64, error) {

	queryport, ok := c.bridge.GetQueryport(common.IndexDefnId(defnID))
	if !ok {
		return 0, ErrorNoHost
	}
	qc := c.queryClients[queryport]
	return qc.CountRange(defnID, low, high, inclusion)
}

// Close the client and all open connections with server.
func (c *GsiClient) Close() {
	c.bridge.Close()
	for _, queryClient := range c.queryClients {
		queryClient.Close()
	}
}

// create GSI client using cbqBridge and ScanCoordinator
func makeWithCbq(config common.Config) (*GsiClient, error) {
	c := &GsiClient{
		queryClients: make(map[string]*gsiScanClient),
	}
	c.bridge = newCbqClient()
	for _, queryport := range c.bridge.GetQueryports() {
		queryClient := newGsiScanClient(queryport, config)
		c.queryClients[queryport] = queryClient
	}
	return c, nil
}

func makeWithMetaProvider(
	cluster, serviceAddr string,
	config common.Config) (c *GsiClient, err error) {

	c = &GsiClient{
		queryClients: make(map[string]*gsiScanClient),
	}
	c.bridge, err = newMetaBridgeClient(cluster, serviceAddr)
	if err != nil {
		return nil, err
	}
	for _, queryport := range c.bridge.GetQueryports() {
		queryClient := newGsiScanClient(queryport, config)
		c.queryClients[queryport] = queryClient
	}
	return c, nil
}
