package client

import "time"
import "unsafe"
import "io"
import "sync/atomic"
import "fmt"

import "github.com/couchbase/indexing/secondary/logging"
import "github.com/couchbase/indexing/secondary/platform"
import "github.com/couchbase/indexing/secondary/common"
import "github.com/golang/protobuf/proto"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
import mclient "github.com/couchbase/indexing/secondary/manager/client"

// TODO:
// - Timeit() uses the wall-clock time instead of process-time to compute
//   load. This is very crude.

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

type Scans []*Scan

type Scan struct {
	Seek   common.SecondaryKey
	Filter []*CompositeElementFilter
}

type CompositeElementFilter struct {
	Low       interface{}
	High      interface{}
	Inclusion Inclusion
}

type IndexProjection struct {
	EntryKeys  []int64
	PrimaryKey bool
}

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
	Nodes() ([]*IndexerService, error)

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

	// GetScanport shall fetch queryport address for indexer,
	// if `retry` is ZERO, pick the indexer under least
	// load, else do a round-robin, based on the retry count,
	// if more than one indexer is found hosing the index or an
	// equivalent index.
	GetScanport(
		defnID uint64,
		retry int,
		excludes map[uint64]bool) (queryport string, targetDefnID uint64, ok bool)

	// GetIndex will return the index-definition structure for defnID.
	GetIndexDefn(defnID uint64) *common.IndexDefn

	// IndexState returns the current state of index `defnID` and error.
	IndexState(defnID uint64) (common.IndexState, error)

	// IsPrimary returns whether index is on primary key.
	IsPrimary(defnID uint64) bool

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
		defnID uint64, requestId string, v common.SecondaryKey) (common.IndexStatistics, error)

	// RangeStatistics for index range.
	RangeStatistics(
		defnID uint64, requestId string, low, high common.SecondaryKey,
		inclusion Inclusion) (common.IndexStatistics, error)

	// Lookup scan index between low and high.
	Lookup(
		defnID uint64, requestId string, values []common.SecondaryKey,
		distinct bool, limit int64,
		cons common.Consistency, vector *TsConsistency,
		callb ResponseHandler) error

	// Range scan index between low and high.
	Range(
		defnID uint64, requestId string, low, high common.SecondaryKey,
		inclusion Inclusion, distinct bool, limit int64,
		cons common.Consistency, vector *TsConsistency,
		callb ResponseHandler) error

	// ScanAll for full table scan.
	ScanAll(
		defnID uint64, requestId string, limit int64,
		cons common.Consistency, vector *TsConsistency,
		callb ResponseHandler) error

	// Multiple scans with composite index filters
	MultiScan(
		defnID uint64, requestId string, scans Scans,
		reverse, distinct bool, projection *IndexProjection, offset, limit int64,
		cons common.Consistency, vector *TsConsistency,
		callb ResponseHandler) error

	// CountLookup of all entries in index.
	CountLookup(
		defnID uint64, requestId string, values []common.SecondaryKey,
		cons common.Consistency, vector *TsConsistency) (int64, error)

	// CountRange of all entries in index.
	CountRange(
		defnID uint64, requestId string,
		low, high common.SecondaryKey, inclusion Inclusion,
		cons common.Consistency, vector *TsConsistency) (int64, error)
}

var useMetadataProvider = true

// IndexerService returns the status of the indexer node
// as observed by the GsiClient.
type IndexerService struct {
	Adminport string
	Queryport string
	Status    string // one of "initial", "online", "recovery"
}

// GsiClient for accessing GSI cluster. The client shall
// use `adminport` for meta-data operation and `queryport`
// for index-scan related operations.
type GsiClient struct {
	bridge       BridgeAccessor // manages adminport
	cluster      string
	maxvb        int
	config       common.Config
	queryClients unsafe.Pointer // map[string(queryport)]*GsiScanClient
	bucketHash   unsafe.Pointer // map[string]uint64 // bucket -> crc64
	metaCh       chan bool      // listen to metadata changes
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

func (c *GsiClient) Bridge() BridgeAccessor {
	return c.bridge
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
func (c *GsiClient) Nodes() ([]*IndexerService, error) {
	if c.bridge == nil {
		return nil, ErrorClientUninitialized
	}
	return c.bridge.Nodes()
}

// BucketSeqnos will return the current vbucket-timestamp using GET_SEQNOS
// command.
func (c *GsiClient) BucketSeqnos(
	bucketn string, hash64 uint64) (*TsConsistency, error) {

	seqnos, err := common.BucketSeqnos(c.cluster, "default" /*pool*/, bucketn)
	if err != nil {
		return nil, err
	}
	vbnos := make([]uint16, len(seqnos))
	for i := range seqnos {
		vbnos[i] = uint16(i)
	}
	vector := NewTsConsistency(vbnos, seqnos, nil)
	vector.Crc64 = hash64
	return vector, nil
}

// BucketTs will return the current vbucket-timestamp using STATS
// command.
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
	seqnos, vbuuids, err := common.BucketTs(b, c.maxvb)
	if err != nil {
		return nil, err
	}
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

	err = common.IsValidIndexName(name)
	if err != nil {
		return 0, err
	}

	if c.bridge == nil {
		return defnID, ErrorClientUninitialized
	}
	begin := time.Now()
	defnID, err = c.bridge.CreateIndex(
		name, bucket, using, exprType, partnExpr, whereExpr,
		secExprs, isPrimary, with)
	fmsg := "CreateIndex %v %v/%v using:%v exprType:%v partnExpr:%v " +
		"whereExpr:%v secExprs:%v isPrimary:%v with:%v - " +
		"elapsed(%v) err(%v)"
	logging.Infof(
		fmsg, defnID, bucket, name, using, exprType, partnExpr, whereExpr,
		secExprs, isPrimary, string(with), time.Since(begin), err)
	return defnID, err
}

// BuildIndexes implements BridgeAccessor{} interface.
func (c *GsiClient) BuildIndexes(defnIDs []uint64) error {
	if c.bridge == nil {
		return ErrorClientUninitialized
	}
	begin := time.Now()
	err := c.bridge.BuildIndexes(defnIDs)
	fmsg := "BuildIndexes %v - elapsed(%v), err(%v)"
	logging.Infof(fmsg, defnIDs, time.Since(begin), err)
	return err
}

// DropIndex implements BridgeAccessor{} interface.
func (c *GsiClient) DropIndex(defnID uint64) error {
	if c.bridge == nil {
		return ErrorClientUninitialized
	}
	begin := time.Now()
	err := c.bridge.DropIndex(defnID)
	fmsg := "DropIndex %v - elapsed(%v), err(%v)"
	logging.Infof(fmsg, defnID, time.Since(begin), err)
	return err
}

// LookupStatistics for a single secondary-key.
func (c *GsiClient) LookupStatistics(
	defnID uint64, requestId string, value common.SecondaryKey) (common.IndexStatistics, error) {

	return nil, ErrorNotImplemented
}

// RangeStatistics for index range.
func (c *GsiClient) RangeStatistics(
	defnID uint64, requestId string, low, high common.SecondaryKey,
	inclusion Inclusion) (common.IndexStatistics, error) {

	return nil, ErrorNotImplemented
}

// Lookup scan index between low and high.
func (c *GsiClient) Lookup(
	defnID uint64, requestId string, values []common.SecondaryKey,
	distinct bool, limit int64,
	cons common.Consistency, vector *TsConsistency,
	callb ResponseHandler) (err error) {

	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err = c.bridge.IndexState(defnID); err != nil {
		protoResp := &protobuf.ResponseStream{
			Err: &protobuf.Error{Error: proto.String(err.Error())},
		}
		callb(protoResp)
		return
	}

	begin := time.Now()

	err = c.doScan(
		defnID, requestId,
		func(qc *GsiScanClient, index *common.IndexDefn) (error, bool) {
			var err error

			vector, err = c.getConsistency(qc, cons, vector, index.Bucket)
			if err != nil {
				return err, false
			}
			return qc.Lookup(
				uint64(index.DefnId), requestId, values, distinct, limit, cons,
				vector, callb)
		})

	if err != nil { // callback with error
		resp := &protobuf.ResponseStream{
			Err: &protobuf.Error{Error: proto.String(err.Error())},
		}
		callb(resp)
	}

	fmsg := "Lookup {%v,%v} - elapsed(%v) err(%v)"
	logging.Verbosef(fmsg, defnID, requestId, time.Since(begin), err)
	return
}

// Range scan index between low and high.
func (c *GsiClient) Range(
	defnID uint64, requestId string, low, high common.SecondaryKey,
	inclusion Inclusion, distinct bool, limit int64,
	cons common.Consistency, vector *TsConsistency,
	callb ResponseHandler) (err error) {

	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err = c.bridge.IndexState(defnID); err != nil {
		protoResp := &protobuf.ResponseStream{
			Err: &protobuf.Error{Error: proto.String(err.Error())},
		}
		callb(protoResp)
		return
	}

	begin := time.Now()

	err = c.doScan(
		defnID, requestId,
		func(qc *GsiScanClient, index *common.IndexDefn) (error, bool) {
			var err error

			vector, err = c.getConsistency(qc, cons, vector, index.Bucket)
			if err != nil {
				return err, false
			}
			if c.bridge.IsPrimary(uint64(index.DefnId)) {
				var l, h []byte
				var what string
				// primary keys are plain sequence of binary.
				if low != nil && len(low) > 0 {
					if l, what = curePrimaryKey(low[0]); what == "after" {
						return nil, true
					}
				}
				if high != nil && len(high) > 0 {
					if h, what = curePrimaryKey(high[0]); what == "before" {
						return nil, true
					}
				}
				return qc.RangePrimary(
					uint64(index.DefnId), requestId, l, h, inclusion, distinct,
					limit, cons, vector, callb)
			}
			// dealing with secondary index.
			return qc.Range(
				uint64(index.DefnId), requestId, low, high, inclusion, distinct,
				limit, cons, vector, callb)
		})

	if err != nil { // callback with error
		resp := &protobuf.ResponseStream{
			Err: &protobuf.Error{Error: proto.String(err.Error())},
		}
		callb(resp)
	}

	fmsg := "Range {%v,%v} - elapsed(%v) err(%v)"
	logging.Verbosef(fmsg, defnID, requestId, time.Since(begin), err)
	return
}

// ScanAll for full table scan.
func (c *GsiClient) ScanAll(
	defnID uint64, requestId string, limit int64,
	cons common.Consistency, vector *TsConsistency,
	callb ResponseHandler) (err error) {

	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err = c.bridge.IndexState(defnID); err != nil {
		protoResp := &protobuf.ResponseStream{
			Err: &protobuf.Error{Error: proto.String(err.Error())},
		}
		callb(protoResp)
		return
	}

	begin := time.Now()

	err = c.doScan(
		defnID, requestId,
		func(qc *GsiScanClient, index *common.IndexDefn) (error, bool) {
			var err error

			vector, err = c.getConsistency(qc, cons, vector, index.Bucket)
			if err != nil {
				return err, false
			}
			return qc.ScanAll(uint64(index.DefnId), requestId, limit, cons, vector, callb)
		})

	if err != nil { // callback with error
		resp := &protobuf.ResponseStream{
			Err: &protobuf.Error{Error: proto.String(err.Error())},
		}
		callb(resp)
	}

	fmsg := "ScanAll {%v,%v} - elapsed(%v) err(%v)"
	logging.Verbosef(fmsg, defnID, requestId, time.Since(begin), err)
	return
}

func (c *GsiClient) MultiScan(
	defnID uint64, requestId string, scans Scans, reverse,
	distinct bool, projection *IndexProjection, offset, limit int64,
	cons common.Consistency, vector *TsConsistency,
	callb ResponseHandler) (err error) {

	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err = c.bridge.IndexState(defnID); err != nil {
		protoResp := &protobuf.ResponseStream{
			Err: &protobuf.Error{Error: proto.String(err.Error())},
		}
		callb(protoResp)
		return
	}

	begin := time.Now()

	err = c.doScan(
		defnID, requestId,
		func(qc *GsiScanClient, index *common.IndexDefn) (error, bool) {
			var err error

			vector, err = c.getConsistency(qc, cons, vector, index.Bucket)
			if err != nil {
				return err, false
			}

			// TODO: Handle RangePrimary
			if c.bridge.IsPrimary(uint64(index.DefnId)) {
				return qc.MultiScanPrimary(
					uint64(index.DefnId), requestId, scans, reverse, distinct,
					projection, offset, limit, cons, vector, callb)
			}

			return qc.MultiScan(
				uint64(index.DefnId), requestId, scans, reverse, distinct,
				projection, offset, limit, cons, vector, callb)
		})

	if err != nil { // callback with error
		resp := &protobuf.ResponseStream{
			Err: &protobuf.Error{Error: proto.String(err.Error())},
		}
		callb(resp)
	}

	fmsg := "Scans {%v,%v} - elapsed(%v) err(%v)"
	logging.Verbosef(fmsg, defnID, requestId, time.Since(begin), err)
	return
}

// CountLookup to count number entries for given set of keys.
func (c *GsiClient) CountLookup(
	defnID uint64, requestId string, values []common.SecondaryKey,
	cons common.Consistency, vector *TsConsistency) (count int64, err error) {

	if c.bridge == nil {
		return count, ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err := c.bridge.IndexState(defnID); err != nil {
		return 0, err
	}

	begin := time.Now()

	err = c.doScan(
		defnID, requestId,
		func(qc *GsiScanClient, index *common.IndexDefn) (error, bool) {
			var err error

			vector, err = c.getConsistency(qc, cons, vector, index.Bucket)
			if err != nil {
				return err, false
			}

			if c.bridge.IsPrimary(uint64(index.DefnId)) {
				equals := make([][]byte, 0, len(values))
				// primary keys are plain sequence of binary.
				for _, value := range values {
					e, _ := curePrimaryKey(value[0])
					equals = append(equals, e)
				}

				count, err = qc.CountLookupPrimary(
					uint64(index.DefnId), requestId, equals, cons, vector)
				return err, false
			}

			count, err = qc.CountLookup(uint64(index.DefnId), requestId, values, cons, vector)
			return err, false
		})

	fmsg := "CountLookup {%v,%v} - elapsed(%v) err(%v)"
	logging.Verbosef(fmsg, defnID, requestId, time.Since(begin), err)
	return count, err
}

// CountRange to count number entries in the given range.
func (c *GsiClient) CountRange(
	defnID uint64, requestId string,
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

	begin := time.Now()

	err = c.doScan(
		defnID, requestId,
		func(qc *GsiScanClient, index *common.IndexDefn) (error, bool) {
			var err error

			vector, err = c.getConsistency(qc, cons, vector, index.Bucket)
			if err != nil {
				return err, false
			}
			if c.bridge.IsPrimary(uint64(index.DefnId)) {
				var l, h []byte
				var what string
				// primary keys are plain sequence of binary.
				if low != nil && len(low) > 0 {
					if l, what = curePrimaryKey(low[0]); what == "after" {
						return nil, true
					}
				}
				if high != nil && len(high) > 0 {
					if h, what = curePrimaryKey(high[0]); what == "before" {
						return nil, true
					}
				}
				count, err = qc.CountRangePrimary(
					uint64(index.DefnId), requestId, l, h, inclusion, cons, vector)
				return err, false
			}

			count, err = qc.CountRange(
				uint64(index.DefnId), requestId, low, high, inclusion, cons, vector)
			return err, false
		})

	fmsg := "CountRange {%v,%v} - elapsed(%v) err(%v)"
	logging.Verbosef(fmsg, defnID, requestId, time.Since(begin), err)
	return count, err
}

// DescribeError return error description as human readable string.
func (c *GsiClient) DescribeError(err error) string {
	if desc, ok := errorDescriptions[err.Error()]; ok {
		return desc
	}
	return err.Error()
}

// Close the client and all open connections with server.
func (c *GsiClient) Close() {
	if c.bridge == nil {
		return
	}
	c.bridge.Close()
	qcs := *((*map[string]*GsiScanClient)(atomic.LoadPointer(&c.queryClients)))
	for _, qc := range qcs {
		qc.Close()
	}
}

func (c *GsiClient) updateScanClients() {
	newclients, staleclients := map[string]bool{}, map[string]bool{}
	cache := map[string]bool{}
	qcs := *((*map[string]*GsiScanClient)(atomic.LoadPointer(&c.queryClients)))
	// add new indexer-nodes
	for _, queryport := range c.bridge.GetScanports() {
		cache[queryport] = true
		if _, ok := qcs[queryport]; !ok {
			newclients[queryport] = true
		}
	}
	// forget stale indexer-nodes.
	for queryport, qc := range qcs {
		if _, ok := cache[queryport]; !ok {
			qc.Close()
			staleclients[queryport] = true
		}
	}
	if len(newclients) > 0 || len(staleclients) > 0 {
		clients := make(map[string]*GsiScanClient)
		for queryport, qc := range qcs {
			if _, ok := staleclients[queryport]; ok {
				continue
			}
			qc.RefreshServerVersion()
			clients[queryport] = qc
		}
		for queryport := range newclients {
			if qc, err := NewGsiScanClient(queryport, c.config); err == nil {
				clients[queryport] = qc
			} else {
				logging.Errorf("Unable to initialize gsi scanclient (%v)", err)
			}
		}

		atomic.StorePointer(&c.queryClients, unsafe.Pointer(&clients))
	}
}

func (c *GsiClient) doScan(
	defnID uint64, requestId string,
	callb func(*GsiScanClient, *common.IndexDefn) (error, bool)) (err error) {

	var qc *GsiScanClient
	var ok1, ok2, partial bool
	var queryport string
	var targetDefnID uint64
	var scan_err error
	var excludes map[uint64]bool

	wait := c.config["retryIntervalScanport"].Int()
	retry := c.config["retryScanPort"].Int()
	evictRetry := c.config["settings.poolSize"].Int()
	for i := 0; true; {
		qcs :=
			*((*map[string]*GsiScanClient)(atomic.LoadPointer(&c.queryClients)))
		if queryport, targetDefnID, ok1 = c.bridge.GetScanport(defnID, i, excludes); ok1 {
			index := c.bridge.GetIndexDefn(targetDefnID)
			if qc, ok2 = qcs[queryport]; ok2 {
				begin := time.Now()
				scan_err, partial = callb(qc, index)
				if c.isTimeit(scan_err) {
					c.bridge.Timeit(targetDefnID, float64(time.Since(begin)))
					return scan_err
				}
				if scan_err != nil && scan_err != io.EOF && partial {
					// partially succeeded scans, we don't reset-hash and we
					// don't retry
					return scan_err
				} else if scan_err == io.EOF && evictRetry > 0 {
					logging.Warnf("evict retry (%v)...\n", evictRetry)
					evictRetry--
					continue
				} else { // TODO: make this error message precise
					// reset the hash so that we do a full STATS for next
					// query.
					c.setBucketHash(index.Bucket, 0)
				}
			}
			err = fmt.Errorf("%v from %v", scan_err, queryport)
		}

		// If there is an error coming from indexer that cannot serve the scan request
		// (including io error), then exclude this defnID and retry with another replica.
		// If we exhaust all the replica, then GetScanport() will return ok1=false.
		if ok1 && ok2 && evictRetry > 0 {
			if excludes == nil {
				excludes = make(map[uint64]bool)
			}
			excludes[targetDefnID] = true

			logging.Warnf(
				"Scan failed with error for index %v.  Trying scan again with replica, reqId:%v : %v ...\n",
				targetDefnID, requestId, scan_err)
			continue
		}

		// If we cannot find a valid scansport, then retry up to retryScanport by refreshing
		// the clients.
		if i = i + 1; i < retry {
			excludes = nil
			logging.Warnf(
				"Trying scan again for index %v (%v %v), reqId:%v : %v ...\n",
				targetDefnID, ok1, ok2, requestId, scan_err)
			c.updateScanClients()
			time.Sleep(time.Duration(wait) * time.Millisecond)
			continue
		}
		break
	}
	if err != nil {
		return err
	}
	return ErrorNoHost
}

func (c *GsiClient) isTimeit(err error) bool {
	if err == nil {
		return true
	} else if err.Error() == common.ErrClientCancel.Error() {
		return true
	}
	return false
}

func (c *GsiClient) getConsistency(
	qc *GsiScanClient, cons common.Consistency,
	vector *TsConsistency, bucket string) (*TsConsistency, error) {

	if cons == common.QueryConsistency {
		if vector == nil {
			return nil, ErrorExpectedTimestamp
		}
		return vector, nil
	} else if cons == common.SessionConsistency {
		var err error
		// Server version is old (cb 4.0.x)
		if qc.NeedSessionConsVector() {
			if hash64, ok := c.getBucketHash(bucket); ok && hash64 != 0 {
				begin := time.Now()
				fmsg := "Time taken by GET_SEQNOS call, %v CRC: %v\n"
				defer func() { logging.Debugf(fmsg, time.Since(begin), hash64) }()
				if vector, err = c.BucketSeqnos(bucket, hash64); err != nil {
					return nil, err
				}

			} else {
				begin := time.Now()
				fmsg := "Time taken by STATS call, %v\n"
				defer func() { logging.Debugf(fmsg, time.Since(begin)) }()
				if vector, err = c.BucketTs(bucket); err != nil {
					return nil, err
				}
				vector.Crc64 = common.HashVbuuid(vector.Vbuuids)
				vector.Vbuuids = nil
				c.setBucketHash(bucket, vector.Crc64)
				logging.Debugf("STATS CRC: %v\n", vector.Crc64)
			}
		} else {
			vector = nil
		}
	} else if cons == common.AnyConsistency {
		vector = nil
	} else {
		return nil, ErrorInvalidConsistency
	}
	return vector, nil
}

func (c *GsiClient) setBucketHash(bucketn string, crc64 uint64) {
	for {
		ptr := platform.LoadPointer(&c.bucketHash)
		oldm := (*map[string]uint64)(ptr)
		newm := map[string]uint64{}
		for k, v := range *oldm {
			newm[k] = v
		}
		newm[bucketn] = crc64
		if platform.CompareAndSwapPointer(&c.bucketHash, ptr, unsafe.Pointer(&newm)) {
			return
		}
	}
}

func (c *GsiClient) getBucketHash(bucketn string) (uint64, bool) {
	bucketHash := (*map[string]uint64)(platform.LoadPointer(&c.bucketHash))
	crc64, ok := (*bucketHash)[bucketn]
	return crc64, ok
}

// create GSI client using cbqBridge and ScanCoordinator
func makeWithCbq(cluster string, config common.Config) (*GsiClient, error) {
	var err error
	c := &GsiClient{
		cluster: cluster,
		config:  config,
	}
	platform.StorePointer(&c.bucketHash, (unsafe.Pointer)(new(map[string]uint64)))
	if c.bridge, err = newCbqClient(cluster); err != nil {
		return nil, err
	}
	clients := make(map[string]*GsiScanClient)
	for _, queryport := range c.bridge.GetScanports() {
		if qc, err := NewGsiScanClient(queryport, config); err == nil {
			clients[queryport] = qc
		}
	}
	atomic.StorePointer(&c.queryClients, unsafe.Pointer(&clients))
	return c, nil
}

func makeWithMetaProvider(
	cluster string, config common.Config) (c *GsiClient, err error) {

	c = &GsiClient{
		cluster:      cluster,
		config:       config,
		queryClients: unsafe.Pointer(new(map[string]*GsiScanClient)),
		metaCh:       make(chan bool, 1),
	}
	platform.StorePointer(&c.bucketHash, (unsafe.Pointer)(new(map[string]uint64)))
	c.bridge, err = newMetaBridgeClient(cluster, config, c.metaCh)
	if err != nil {
		return nil, err
	}
	c.updateScanClients()
	go c.listenMetaChange()
	return c, nil
}

func (c *GsiClient) listenMetaChange() {
	for {
		select {
		case <-c.metaCh:
			c.updateScanClients()
		}
	}
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
	Crc64   uint64
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

func curePrimaryKey(key interface{}) ([]byte, string) {
	if key == nil {
		return nil, "ok"
	}
	switch v := key.(type) {
	case []byte:
		return v, "ok"
	case string:
		return []byte(v), "ok"
	case []interface{}:
		return nil, "after"
	case map[string]interface{}:
		return nil, "after"
	}
	return nil, "before"
}
