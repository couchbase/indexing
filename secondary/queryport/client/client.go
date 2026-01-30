// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	commonjson "github.com/couchbase/indexing/secondary/common/json"
	"github.com/couchbase/indexing/secondary/logging/systemevent"
	report "github.com/couchbase/indexing/secondary/scanreport"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/goextended/syncx"
	"github.com/couchbase/indexing/secondary/logging"
	mclient "github.com/couchbase/indexing/secondary/manager/client"
	"github.com/couchbase/indexing/secondary/security"
	"github.com/couchbase/query/value"
)

// TODO:
// - Timeit() uses the wall-clock time instead of process-time to compute
//   load. This is very crude.

// Identify an instance of resoponse handler
type ResponseHandlerId int

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
	GetEntries(dataEncFmt common.DataEncodingFormat) (*common.ScanResultEntries, [][]byte, error)

	// Error returns the error value, if nil there is no error.
	Error() error

	GetReadUnits() uint64

	GetServerScanReport() *report.HostScanReport
}

// ResponseSender is responsible for forwarding result to the client
// after streams from multiple servers/ResponseHandler have been merged.
// mskey - marshalled sec key (as Value)
// uskey - unmarshalled sec key (as byte)
type ResponseSender func(pkey []byte, mskey []value.Value, uskey common.ScanResultKey, tmpbuf *[]byte) (bool, *[]byte)

// ResponseHandlerFactory returns an instance of ResponseHandler
type ResponseHandlerFactory func(id ResponseHandlerId, instId uint64, partitions []common.PartitionId) ResponseHandler

// ScanRequestHandler initiates a request to a single server connection
type ScanRequestHandler func(*GsiScanClient, *common.IndexDefn, int64, []common.PartitionId, ResponseHandler) (error, bool)

// CountRequestHandler initiates a request to a single server connection
type CountRequestHandler func(*GsiScanClient, *common.IndexDefn, int64, []common.PartitionId) (int64, error, bool)

// ResponseTimer updates timing of responses
type ResponseTimer func(instID uint64, partitionId common.PartitionId, value float64)

// ResponseWaiter for backfill done
type BackfillWaiter func()

// scanClientMaker fetches a scan client
type scanClientMaker func(scanport string) *GsiScanClient

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

// Groupby/Aggregate
type GroupKey struct {
	EntryKeyId int32  // Id that can be used in IndexProjection
	KeyPos     int32  // >=0 means use expr at index key position otherwise use Expr
	Expr       string // group expression
}

type Aggregate struct {
	AggrFunc   common.AggrFuncType // Aggregate operation
	EntryKeyId int32               // Id that can be used in IndexProjection
	KeyPos     int32               // >=0 means use expr at index key position otherwise use Expr
	Expr       string              // Aggregate expression
	Distinct   bool                // Aggregate only on Distinct values with in the group
}

type GroupAggr struct {
	Name               string       // name of the index aggregate
	Group              []*GroupKey  // group keys, nil means no group by
	Aggrs              []*Aggregate // aggregates with in the group, nil means no aggregates
	DependsOnIndexKeys []int32      // GROUP and Aggregates Depends on List of index keys positions
	IndexKeyNames      []string     // Index key names used in expressions
	AllowPartialAggr   bool         // Partial aggregates are allowed
	OnePerPrimaryKey   bool         // Leading Key is ALL & equality span consider one per docid
}

type IndexKeyOrder struct {
	KeyPos []int
	Desc   []bool
}

type IndexVector struct {
	QueryVector       []float32            // dense query vector
	QuerySparseVector *common.SparseVector // sparse vector
	IndexKeyPos       int                  // vector key pos in index
	Probes            int                  // nprobes
	TopNScan          int                  // TopNScan for Bhive Index, Override default only when  > 0
	Rerank            bool                 // Enable reranking by using actual vector
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
	Refresh() ([]*mclient.IndexMetadata, uint64, uint64, bool, error)

	// Nodes shall return a map of adminport and queryport for indexer
	// nodes.
	Nodes() ([]*IndexerService, error)

	// GetNode returns the indexerservice for the corresponding node
	GetNode(common.IndexerId) (*IndexerService, error)

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
	// desc
	//		For each expression in `secExprs`, specifies whether DESC
	//		attribute has been specified or not
	// hasVectorAttr
	//		For each expression in `secExprs`, specifies whether VECTOR
	//		attribute has been specified or not
	// isPrimary
	//      specify whether the index is created on docid.
	// with
	//      JSON marshalled description about index deployment (and more...).
	// include
	//		List of secondary expressions used as include columns
	// isBhive
	//		Specifies whether an index is BHIVE vector index
	CreateIndex(
		name, bucket, scope, collection, using, exprType, whereExpr string,
		secExprs []string, desc []bool, hasVectorAttr []bool,
		indexMissingLeadingKey, isPrimary bool,
		scheme common.PartitionScheme, partitionKeys []string,
		with []byte, include []string, isBhive bool, secExprsAttrs []common.SecExprAttr) (defnID uint64, err error)

	// BuildIndexes to build a deferred set of indexes. This call implies
	// that indexes specified are already created.
	BuildIndexes(defnIDs []uint64) error

	// MoveIndex to move a set of indexes to different node.
	MoveIndex(defnID uint64, with map[string]interface{}) error

	// AlterReplicaCount to change replica count of index
	AlterReplicaCount(action string, defnID uint64, with map[string]interface{}) error

	// DropIndex to drop index specified by `defnID`.
	// - if index is in deferred build state, it shall be removed
	//   from deferred list.
	DropIndex(defnID uint64, bucketName string) error

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
		excludes map[common.IndexDefnId]map[common.PartitionId]map[uint64]bool,
		skips map[common.IndexDefnId]bool) (queryport []string, targetDefnID uint64, targetInstID []uint64,
		rollbackTime []int64, partition [][]common.PartitionId, numPartitions uint32, ok bool)

	// GetIndexDefn will return the index-definition structure for defnID.
	GetIndexDefn(defnID uint64) *common.IndexDefn

	// GetIndexInst will return the index-instance structure for instId.
	GetIndexInst(instId uint64) *mclient.InstanceDefn

	// GetIndexReplica will return the index-instance structure for defnId.
	GetIndexReplica(defnId uint64) []*mclient.InstanceDefn

	// IndexState returns the current state of index `defnID` and error.
	IndexState(defnID uint64) (common.IndexState, error)

	// IsPrimary returns whether index is on primary key.
	IsPrimary(defnID uint64) bool

	//Return the number of replica and equivalent indexes
	NumReplica(defnID uint64) int

	// Timeit will add `value` to incrementalAvg for index-load.
	Timeit(instID uint64, partitionId common.PartitionId, value float64)

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
		cons common.Consistency, tsvector *TsConsistency,
		callb ResponseHandler, scanParams map[string]interface{}) error

	// Lookup scan index between low and high.
	LookupInternal(
		defnID uint64, requestId string, values []common.SecondaryKey,
		distinct bool, limit int64,
		cons common.Consistency, tsvector *TsConsistency,
		broker *RequestBroker, scanParams map[string]interface{}) error

	// Range scan index between low and high.
	Range(
		defnID uint64, requestId string, low, high common.SecondaryKey,
		inclusion Inclusion, distinct bool, limit int64,
		cons common.Consistency, tsvector *TsConsistency,
		callb ResponseHandler, scanParams map[string]interface{}) error

	// Range scan index between low and high.
	RangeInternal(
		defnID uint64, requestId string, low, high common.SecondaryKey,
		inclusion Inclusion, distinct bool, limit int64,
		cons common.Consistency, tsvector *TsConsistency,
		broker *RequestBroker, scanParams map[string]interface{}) error

	// ScanAll for full table scan.
	ScanAll(
		defnID uint64, requestId string, limit int64,
		cons common.Consistency, tsvector *TsConsistency,
		callb ResponseHandler, scanParams map[string]interface{}) error

	// ScanAll for full table scan.
	ScanAllInternal(
		defnID uint64, requestId string, limit int64,
		cons common.Consistency, tsvector *TsConsistency,
		broker *RequestBroker, scanParams map[string]interface{}) error

	// Multiple scans with composite index filters
	MultiScan(
		defnID uint64, requestId string, scans Scans,
		reverse, distinct bool, projection *IndexProjection, offset, limit int64,
		cons common.Consistency, tsvector *TsConsistency,
		callb ResponseHandler, scanParams map[string]interface{}) error

	// Multiple scans with composite index filters
	MultiScanInternal(
		defnID uint64, requestId string, scans Scans,
		reverse, distinct bool, projection *IndexProjection, offset, limit int64,
		cons common.Consistency, tsvector *TsConsistency,
		broker *RequestBroker, scanParams map[string]interface{}) error

	// CountLookup of all entries in index.
	CountLookup(
		defnID uint64, requestId string, values []common.SecondaryKey,
		cons common.Consistency, tsvector *TsConsistency) (int64, error)

	// CountLookup of all entries in index.
	CountLookupInternal(
		defnID uint64, requestId string, values []common.SecondaryKey,
		cons common.Consistency, tsvector *TsConsistency,
		broker *RequestBroker) (int64, error)

	// CountRange of all entries in index.
	CountRange(
		defnID uint64, requestId string,
		low, high common.SecondaryKey, inclusion Inclusion,
		cons common.Consistency, tsvector *TsConsistency) (int64, error)

	// CountRange of all entries in index.
	CountRangeInternal(
		defnID uint64, requestId string,
		low, high common.SecondaryKey, inclusion Inclusion,
		cons common.Consistency, tsvector *TsConsistency,
		broker *RequestBroker) (int64, error)

	// Count using MultiScan
	MultiScanCount(
		defnID uint64, requestId string,
		scans Scans, distinct bool,
		cons common.Consistency, tsvector *TsConsistency, scanParams map[string]interface{}) (int64, uint64, error)

	// Count using MultiScan
	MultiScanCountInternal(
		defnID uint64, requestId string,
		scans Scans, distinct bool,
		cons common.Consistency, tsvector *TsConsistency,
		broker *RequestBroker, scanParams map[string]interface{}) (int64, uint64, error)

	// Scan API3 with grouping and aggregates support
	Scan3(
		defnID uint64, requestId string, scans Scans,
		reverse, distinct bool, projection *IndexProjection, offset, limit int64,
		groupAggr *GroupAggr,
		cons common.Consistency, tsvector *TsConsistency,
		callb ResponseHandler, scanParams map[string]interface{}) error

	// Scan API3 with grouping and aggregates support
	Scan3Internal(
		defnID uint64, requestId string, scans Scans,
		reverse, distinct bool, projection *IndexProjection, offset, limit int64,
		groupAggr *GroupAggr,
		cons common.Consistency, tsvector *TsConsistency,
		broker *RequestBroker, scanParams map[string]interface{}) error

	// StorageStatistics API4 for getting per partition storage stats.
	// Return value is a slice of maps, each map is storage stats per partition
	StorageStatistics(defnID uint64, requestId string) ([]map[string]interface{}, error)
}

var useMetadataProvider = true
var pInitOnceRetyOnErr syncx.OnceOnSuccess

// IndexerService returns the status of the indexer node
// as observed by the GsiClient.
type IndexerService struct {
	Adminport string
	Queryport string
	Httpport  string
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
	settings     *ClientSettings
	killch       chan bool
	numScans     int64
	scanResponse int64
	dataEncFmt   uint32
	qcLock       sync.Mutex
	needsAuth    *uint32
}

// NewGsiClient returns client to access GSI cluster.
func NewGsiClient(
	cluster string, config common.Config) (c *GsiClient, err error) {

	// needsRefresh is set to true by cbindex tool
	needsRefresh := false
	if val, ok := config["needsRefresh"]; ok {
		needsRefresh = val.Bool()
	}

	return NewGsiClientWithSettings(cluster, config, needsRefresh, true)
}

func NewGsiClientWithSettings(
	cluster string, config common.Config, needRefresh bool, encryptLocalHost bool) (c *GsiClient, err error) {

	if useMetadataProvider {
		c, err = makeWithMetaProvider(cluster, config, needRefresh, encryptLocalHost)
	} else {
		c, err = makeWithCbq(cluster, config, encryptLocalHost)
	}
	if err != nil {
		return nil, err
	}
	c.maxvb = -1

	var clusterVer uint64
	var refreshErr error
	_, _, clusterVer, _, refreshErr = c.Refresh()
	if refreshErr == nil {
		c.UpdateDataEncodingFormat(clusterVer)
	} else {
		// Use old data format if c.Refresh() returns error
		c.SetDataEncodingFormat(common.DATA_ENC_JSON)
	}

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

func (c *GsiClient) UpdateDataEncodingFormat(clusterVer uint64) {
	if clusterVer >= common.INDEXER_65_VERSION {
		msg := "GsiClient::UpdateUsecjson: using collatejson as data format "
		msg += "between indexer and GsiClient"
		logging.Debugf(msg)
		c.SetDataEncodingFormat(common.DATA_ENC_COLLATEJSON)
	} else {
		c.SetDataEncodingFormat(common.DATA_ENC_JSON)
	}
}

// Refresh implements BridgeAccessor{} interface.
func (c *GsiClient) Refresh() ([]*mclient.IndexMetadata, uint64, uint64, bool, error) {
	if c.bridge == nil {
		return nil, 0, 0, false, ErrorClientUninitialized
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

	return c.CreateIndex3(name, bucket, using, exprType,
		whereExpr, secExprs, nil, isPrimary, common.SINGLE, nil, with)
}

func (c *GsiClient) CreateIndex3(
	name, bucket, using, exprType, whereExpr string,
	secExprs []string, desc []bool, isPrimary bool,
	scheme common.PartitionScheme, partitionKeys []string,
	with []byte) (defnID uint64, err error) {

	return c.CreateIndex4(name, bucket, common.DEFAULT_SCOPE,
		common.DEFAULT_COLLECTION, using, exprType, whereExpr,
		secExprs, desc, false, isPrimary, scheme, partitionKeys, with)
}

// scope and collection parameters are ignored as of now.
func (c *GsiClient) CreateIndex4(
	name, bucket, scope, collection, using, exprType, whereExpr string,
	secExprs []string, desc []bool, indexMissingLeadingKey, isPrimary bool,
	scheme common.PartitionScheme, partitionKeys []string,
	with []byte) (defnID uint64, err error) {

	return c.CreateIndex6(name, bucket, scope, collection,
		using, exprType, whereExpr, secExprs, desc, nil, false,
		isPrimary, scheme, partitionKeys, with, nil, false, nil)

}

// scope and collection parameters are ignored as of now.
func (c *GsiClient) CreateIndex6(
	name, bucket, scope, collection, using, exprType, whereExpr string,
	secExprs []string, desc []bool, hasVectorAttr []bool,
	indexMissingLeadingKey, isPrimary bool,
	scheme common.PartitionScheme, partitionKeys []string,
	with []byte, include []string, isBhive bool, secExprsAttrs []common.SecExprAttr) (defnID uint64, err error) {

	err = common.IsValidIndexName(name)
	if err != nil {
		return 0, err
	}

	if c.bridge == nil {
		return defnID, ErrorClientUninitialized
	}

	logging.Infof("CreateIndex %v %v %v %v ...", bucket, scope, collection, name)
	begin := time.Now()
	defnID, err = c.bridge.CreateIndex(
		name, bucket, scope, collection, using, exprType, whereExpr,
		secExprs, desc, hasVectorAttr, indexMissingLeadingKey, isPrimary, scheme,
		partitionKeys, with, include, isBhive, secExprsAttrs)
	fmsg := "CreateIndex %v %v %v %v/%v using:%v exprType:%v " +
		"whereExpr:%v secExprs:%v desc:%v hasVectorAttr:%v indexMissingLeadingKey:%v isPrimary:%v scheme:%v " +
		" partitionKeys:%v with:%v include:%v isBhive:%v - elapsed(%v) err(%v)"

	origSecExprs, _, _, _, _ := common.GetUnexplodedExprs(secExprs, desc, hasVectorAttr, secExprsAttrs)
	logging.Infof(
		fmsg, defnID, bucket, scope, collection, name, using, exprType, logging.TagUD(whereExpr),
		logging.TagUD(origSecExprs), desc, hasVectorAttr, indexMissingLeadingKey, isPrimary, scheme,
		logging.TagUD(partitionKeys), string(with), logging.TagStrUD(include), isBhive, time.Since(begin), err)
	return defnID, err
}

// BuildIndexes implements BridgeAccessor{} interface.
func (c *GsiClient) BuildIndexes(defnIDs []uint64) error {
	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	logging.Infof("BuildIndexes %v ...", defnIDs)
	begin := time.Now()
	err := c.bridge.BuildIndexes(defnIDs)
	fmsg := "BuildIndexes %v - elapsed(%v), err(%v)"
	logging.Infof(fmsg, defnIDs, time.Since(begin), err)
	return err
}

// MoveIndex implements BridgeAccessor{} interface.
func (c *GsiClient) MoveIndex(defnID uint64, with map[string]interface{}) error {
	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	logging.Infof("MoveIndex %v ...", defnID)
	begin := time.Now()
	err := c.bridge.MoveIndex(defnID, with)
	fmsg := "MoveIndex %v - elapsed(%v), err(%v)"
	logging.Infof(fmsg, defnID, time.Since(begin), err)
	return err
}

// AlterReplicaCount implements BridgeAccessor{} interface.
func (c *GsiClient) AlterReplicaCount(action string, defnID uint64, with map[string]interface{}) error {
	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	logging.Infof("AlterReplicaCount %v %v ...", defnID, action)
	begin := time.Now()
	err := c.bridge.AlterReplicaCount(action, defnID, with)
	fmsg := "AlterReplicaCount %v - elapsed(%v), err(%v)"
	logging.Infof(fmsg, defnID, time.Since(begin), err)
	return err
}

// DropIndex implements BridgeAccessor{} interface.
func (c *GsiClient) DropIndex(defnID uint64, bucketName string) error {
	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	logging.Infof("DropIndex %v ...", defnID)
	begin := time.Now()
	err := c.bridge.DropIndex(defnID, bucketName)
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
	cons common.Consistency, tsvector *TsConsistency,
	callb ResponseHandler, scanParams map[string]interface{}) (err error) {

	dataEncFmt := c.GetDataEncodingFormat()
	broker := makeDefaultRequestBroker(callb, dataEncFmt)
	return c.LookupInternal(defnID, requestId, values, distinct, limit, cons, tsvector, broker, scanParams, time.Time{}, 0)
}

// Lookup scan index between low and high.
func (c *GsiClient) LookupInternal(
	defnID uint64, requestId string, values []common.SecondaryKey,
	distinct bool, limit int64,
	cons common.Consistency, tsvector *TsConsistency,
	broker *RequestBroker, scanParams map[string]interface{},
	reqDeadline time.Time, reqDeadlineSlack time.Duration) (err error) {

	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err = c.bridge.IndexState(defnID); err != nil {
		return err
	}

	begin := time.Now()

	handler := func(qc *GsiScanClient, index *common.IndexDefn, rollbackTime int64, partitions []common.PartitionId,
		callb ResponseHandler) (error, bool) {
		var err error

		dataEncFmt := broker.GetDataEncodingFormat()

		tsvector, err = c.getConsistency(qc, cons, tsvector, index.Bucket)
		if err != nil {
			return err, false
		}
		return qc.Lookup(
			uint64(index.DefnId), requestId, values, distinct, broker.GetLimit(), cons,
			tsvector, callb, rollbackTime, partitions, dataEncFmt, broker.DoRetry(),
			scanParams, reqDeadline, reqDeadlineSlack)
	}

	broker.SetScanRequestHandler(handler)
	broker.SetLimit(limit)

	_, err = c.doScan(defnID, requestId, broker)
	if err != nil { // callback with error
		return err
	}

	fmsg := "Lookup {%v,%v} - elapsed(%v) err(%v)"
	logging.Verbosef(fmsg, defnID, requestId, time.Since(begin), err)
	return
}

// Range scan index between low and high.
func (c *GsiClient) Range(
	defnID uint64, requestId string, low, high common.SecondaryKey,
	inclusion Inclusion, distinct bool, limit int64,
	cons common.Consistency, tsvector *TsConsistency,
	callb ResponseHandler, scanParams map[string]interface{}) (err error) {

	dataEncFmt := c.GetDataEncodingFormat()
	broker := makeDefaultRequestBroker(callb, dataEncFmt)
	return c.RangeInternal(defnID, requestId, low, high, inclusion, distinct, limit, cons, tsvector, broker, scanParams, time.Time{}, 0)
}

// Range scan index between low and high.
func (c *GsiClient) RangeInternal(
	defnID uint64, requestId string, low, high common.SecondaryKey,
	inclusion Inclusion, distinct bool, limit int64,
	cons common.Consistency, tsvector *TsConsistency,
	broker *RequestBroker, scanParams map[string]interface{},
	reqDeadline time.Time, reqDeadlineSlack time.Duration) (err error) {

	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err = c.bridge.IndexState(defnID); err != nil {
		return err
	}

	begin := time.Now()

	handler := func(qc *GsiScanClient, index *common.IndexDefn, rollbackTime int64, partitions []common.PartitionId,
		handler ResponseHandler) (error, bool) {
		var err error

		dataEncFmt := broker.GetDataEncodingFormat()

		tsvector, err = c.getConsistency(qc, cons, tsvector, index.Bucket)
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
				broker.GetLimit(), cons, tsvector, handler, rollbackTime,
				partitions, dataEncFmt, broker.DoRetry(), scanParams, reqDeadline,
				reqDeadlineSlack)
		}
		// dealing with secondary index.
		return qc.Range(
			uint64(index.DefnId), requestId, low, high, inclusion, distinct,
			broker.GetLimit(), cons, tsvector, handler, rollbackTime, partitions,
			dataEncFmt, broker.DoRetry(), scanParams, reqDeadline, reqDeadlineSlack)
	}

	broker.SetScanRequestHandler(handler)
	broker.SetLimit(limit)

	_, err = c.doScan(defnID, requestId, broker)
	if err != nil { // callback with error
		return err
	}

	fmsg := "Range {%v,%v} - elapsed(%v) err(%v)"
	logging.Verbosef(fmsg, defnID, requestId, time.Since(begin), err)
	return
}

// ScanAll for full table scan.
func (c *GsiClient) ScanAll(
	defnID uint64, requestId string, limit int64,
	cons common.Consistency, tsvector *TsConsistency,
	callb ResponseHandler, scanParams map[string]interface{}) (err error) {

	dataEncFmt := c.GetDataEncodingFormat()
	broker := makeDefaultRequestBroker(callb, dataEncFmt)
	return c.ScanAllInternal(defnID, requestId, limit, cons, tsvector, broker, scanParams, time.Time{}, 0)
}

// ScanAll for full table scan.
func (c *GsiClient) ScanAllInternal(
	defnID uint64, requestId string, limit int64,
	cons common.Consistency, tsvector *TsConsistency,
	broker *RequestBroker, scanParams map[string]interface{},
	reqDeadline time.Time, reqDeadlineSlack time.Duration) (err error) {

	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err = c.bridge.IndexState(defnID); err != nil {
		return err
	}

	var generateScanReport bool
	if broker.scanReport != nil {
		generateScanReport = true
	}

	begin := time.Now()

	handler := func(qc *GsiScanClient, index *common.IndexDefn, rollbackTime int64, partitions []common.PartitionId,
		handler ResponseHandler) (error, bool) {
		var err error

		dataEncFmt := broker.GetDataEncodingFormat()

		tsvector, err = c.getConsistency(qc, cons, tsvector, index.Bucket)
		if err != nil {
			return err, false
		}
		return qc.ScanAll(uint64(index.DefnId), requestId, broker.GetLimit(),
			cons, tsvector, handler, rollbackTime, partitions, dataEncFmt, broker.DoRetry(),
			scanParams, reqDeadline, reqDeadlineSlack, generateScanReport)
	}

	broker.SetScanRequestHandler(handler)
	broker.SetLimit(limit)

	_, err = c.doScan(defnID, requestId, broker)
	if err != nil { // callback with error
		return err
	}

	fmsg := "ScanAll {%v,%v} - elapsed(%v) err(%v)"
	logging.Verbosef(fmsg, defnID, requestId, time.Since(begin), err)
	return
}

func (c *GsiClient) MultiScan(
	defnID uint64, requestId string, scans Scans, reverse,
	distinct bool, projection *IndexProjection, offset, limit int64,
	cons common.Consistency, tsvector *TsConsistency,
	callb ResponseHandler, scanParams map[string]interface{}) (err error) {

	dataEncFmt := c.GetDataEncodingFormat()
	broker := makeDefaultRequestBroker(callb, dataEncFmt)
	return c.MultiScanInternal(defnID, requestId, scans, reverse, distinct, projection, offset, limit, cons, tsvector, broker, scanParams, time.Time{}, 0)
}

func (c *GsiClient) MultiScanInternal(
	defnID uint64, requestId string, scans Scans, reverse,
	distinct bool, projection *IndexProjection, offset, limit int64,
	cons common.Consistency, tsvector *TsConsistency,
	broker *RequestBroker, scanParams map[string]interface{},
	reqDeadline time.Time, reqDeadlineSlack time.Duration) (err error) {

	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err = c.bridge.IndexState(defnID); err != nil {
		return err
	}

	begin := time.Now()

	handler := func(qc *GsiScanClient, index *common.IndexDefn, rollbackTime int64, partitions []common.PartitionId,
		handler ResponseHandler) (error, bool) {
		var err error

		dataEncFmt := broker.GetDataEncodingFormat()

		tsvector, err = c.getConsistency(qc, cons, tsvector, index.Bucket)
		if err != nil {
			return err, false
		}

		if c.bridge.IsPrimary(uint64(index.DefnId)) {
			return qc.MultiScanPrimary(
				uint64(index.DefnId), requestId, scans, reverse, distinct,
				projection, broker.GetOffset(), broker.GetLimit(), cons,
				tsvector, handler, rollbackTime, partitions, dataEncFmt, broker.DoRetry(),
				scanParams, reqDeadline, reqDeadlineSlack)
		}

		return qc.MultiScan(
			uint64(index.DefnId), requestId, scans, reverse, distinct,
			projection, broker.GetOffset(), broker.GetLimit(), cons, tsvector,
			handler, rollbackTime, partitions, dataEncFmt, broker.DoRetry(), scanParams,
			reqDeadline, reqDeadlineSlack)
	}

	broker.SetScanRequestHandler(handler)
	broker.SetLimit(limit)
	broker.SetOffset(offset)
	broker.SetScans(scans)
	broker.SetProjection(projection)
	broker.SetDistinct(distinct)

	_, err = c.doScan(defnID, requestId, broker)
	if err != nil { // callback with error
		return err
	}

	fmsg := "Scans {%v,%v} - elapsed(%v) err(%v)"
	logging.Verbosef(fmsg, defnID, requestId, time.Since(begin), err)
	return
}

func (c *GsiClient) CountLookup(
	defnID uint64, requestId string, values []common.SecondaryKey,
	cons common.Consistency, tsvector *TsConsistency) (count int64, err error) {

	dataEncFmt := c.GetDataEncodingFormat()
	broker := makeDefaultRequestBroker(nil, dataEncFmt)
	return c.CountLookupInternal(defnID, requestId, values, cons, tsvector, broker, time.Time{}, 0)
}

// CountLookup to count number entries for given set of keys.
func (c *GsiClient) CountLookupInternal(
	defnID uint64, requestId string, values []common.SecondaryKey,
	cons common.Consistency, tsvector *TsConsistency,
	broker *RequestBroker, reqDeadline time.Time,
	reqDeadlineSlack time.Duration) (count int64, err error) {

	if c.bridge == nil {
		return count, ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err := c.bridge.IndexState(defnID); err != nil {
		return 0, err
	}

	begin := time.Now()

	handler := func(qc *GsiScanClient, index *common.IndexDefn, rollbackTime int64, partitions []common.PartitionId) (int64, error, bool) {
		var err error
		var count int64

		tsvector, err = c.getConsistency(qc, cons, tsvector, index.Bucket)
		if err != nil {
			return 0, err, false
		}

		if c.bridge.IsPrimary(uint64(index.DefnId)) {
			equals := make([][]byte, 0, len(values))
			// primary keys are plain sequence of binary.
			for _, value := range values {
				e, _ := curePrimaryKey(value[0])
				equals = append(equals, e)
			}

			count, err = qc.CountLookupPrimary(
				uint64(index.DefnId), requestId, equals, cons, tsvector, rollbackTime, partitions, broker.DoRetry(),
				reqDeadline, reqDeadlineSlack)
			return count, err, false
		}

		count, err = qc.CountLookup(uint64(index.DefnId), requestId, values, cons, tsvector, rollbackTime, partitions, broker.DoRetry(),
			reqDeadline, reqDeadlineSlack)
		return count, err, false
	}

	broker.SetCountRequestHandler(handler)

	count, err = c.doScan(defnID, requestId, broker)

	fmsg := "CountLookup {%v,%v} - elapsed(%v) err(%v)"
	logging.Verbosef(fmsg, defnID, requestId, time.Since(begin), err)
	return count, err
}

func (c *GsiClient) CountRange(
	defnID uint64, requestId string,
	low, high common.SecondaryKey,
	inclusion Inclusion,
	cons common.Consistency, tsvector *TsConsistency) (count int64, err error) {

	dataEncFmt := c.GetDataEncodingFormat()
	broker := makeDefaultRequestBroker(nil, dataEncFmt)
	return c.CountRangeInternal(defnID, requestId, low, high, inclusion, cons, tsvector, broker, time.Time{}, 0)
}

// CountRange to count number entries in the given range.
func (c *GsiClient) CountRangeInternal(
	defnID uint64, requestId string,
	low, high common.SecondaryKey,
	inclusion Inclusion,
	cons common.Consistency, tsvector *TsConsistency,
	broker *RequestBroker, reqDeadline time.Time,
	reqDeadlineSlack time.Duration) (count int64, err error) {

	if c.bridge == nil {
		return count, ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err := c.bridge.IndexState(defnID); err != nil {
		return 0, err
	}

	begin := time.Now()

	handler := func(qc *GsiScanClient, index *common.IndexDefn, rollbackTime int64, partitions []common.PartitionId) (int64, error, bool) {
		var err error
		var count int64

		tsvector, err = c.getConsistency(qc, cons, tsvector, index.Bucket)
		if err != nil {
			return 0, err, false
		}
		if c.bridge.IsPrimary(uint64(index.DefnId)) {
			var l, h []byte
			var what string
			// primary keys are plain sequence of binary.
			if low != nil && len(low) > 0 {
				if l, what = curePrimaryKey(low[0]); what == "after" {
					return 0, nil, true
				}
			}
			if high != nil && len(high) > 0 {
				if h, what = curePrimaryKey(high[0]); what == "before" {
					return 0, nil, true
				}
			}
			count, err = qc.CountRangePrimary(
				uint64(index.DefnId), requestId, l, h, inclusion, cons, tsvector, rollbackTime, partitions, broker.DoRetry(),
				reqDeadline, reqDeadlineSlack)
			return count, err, false
		}

		count, err = qc.CountRange(
			uint64(index.DefnId), requestId, low, high, inclusion, cons, tsvector, rollbackTime, partitions, broker.DoRetry(),
			reqDeadline, reqDeadlineSlack)
		return count, err, false
	}

	broker.SetCountRequestHandler(handler)

	count, err = c.doScan(defnID, requestId, broker)

	fmsg := "CountRange {%v,%v} - elapsed(%v) err(%v)"
	logging.Verbosef(fmsg, defnID, requestId, time.Since(begin), err)
	return count, err
}

func (c *GsiClient) MultiScanCount(defnID uint64, requestId string, scans Scans,
	distinct bool, cons common.Consistency, tsvector *TsConsistency, scanParams map[string]interface{},
	reqDeadline time.Time, reqDeadlineSlack time.Duration) (
	count int64, readUnits uint64, err error) {

	dataEncFmt := c.GetDataEncodingFormat()
	broker := makeDefaultRequestBroker(nil, dataEncFmt)
	return c.MultiScanCountInternal(defnID, requestId, scans, distinct, cons, tsvector, broker, scanParams, reqDeadline, reqDeadlineSlack)
}

func (c *GsiClient) MultiScanCountInternal(
	defnID uint64, requestId string,
	scans Scans, distinct bool,
	cons common.Consistency, tsvector *TsConsistency,
	broker *RequestBroker, scanParams map[string]interface{},
	reqDeadline time.Time, reqDeadlineSlack time.Duration) (
	count int64, readUnits uint64, err error) {

	if c.bridge == nil {
		return count, 0, ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err := c.bridge.IndexState(defnID); err != nil {
		return 0, 0, err
	}

	begin := time.Now()

	handler := func(qc *GsiScanClient, index *common.IndexDefn, rollbackTime int64, partitions []common.PartitionId) (int64, error, bool) {
		var err error
		var ru uint64

		tsvector, err = c.getConsistency(qc, cons, tsvector, index.Bucket)
		if err != nil {
			return 0, err, false
		}
		if c.bridge.IsPrimary(uint64(index.DefnId)) {
			count, ru, err = qc.MultiScanCountPrimary(
				uint64(index.DefnId), requestId, scans, distinct, cons, tsvector, rollbackTime, partitions, broker.DoRetry(), scanParams,
				reqDeadline, reqDeadlineSlack)
			atomic.AddUint64(&readUnits, ru)
			return count, err, false
		}

		count, ru, err = qc.MultiScanCount(uint64(index.DefnId), requestId,
			scans, distinct, cons, tsvector, rollbackTime, partitions, broker.DoRetry(),
			scanParams, reqDeadline, reqDeadlineSlack)
		atomic.AddUint64(&readUnits, ru)
		return count, err, false
	}

	broker.SetCountRequestHandler(handler)

	count, err = c.doScan(defnID, requestId, broker)

	fmsg := "MultiScanCount {%v,%v} - elapsed(%v) err(%v)"
	logging.Verbosef(fmsg, defnID, requestId, time.Since(begin), err)
	return count, readUnits, err
}

func (c *GsiClient) Scan3(
	defnID uint64, requestId string, scans Scans, reverse,
	distinct bool, projection *IndexProjection, offset, limit int64,
	groupAggr *GroupAggr, indexOrder *IndexKeyOrder,
	cons common.Consistency, tsvector *TsConsistency,
	callb ResponseHandler, scanParams map[string]interface{}) (err error) {

	dataEncFmt := c.GetDataEncodingFormat()
	broker := makeDefaultRequestBroker(callb, dataEncFmt)
	return c.ScanInternal("scan3", defnID, requestId, scans, reverse, distinct,
		projection, offset, limit, groupAggr, indexOrder,
		nil, "", nil, nil, cons, tsvector, broker, scanParams,
		nil, time.Time{}, 0)
}

func (c *GsiClient) Scan6(
	defnID uint64, requestId string, scans Scans, reverse,
	distinct bool, projection *IndexProjection, offset, limit int64,
	groupAggr *GroupAggr, indexOrder *IndexKeyOrder,
	indexKeyNames []string, inlineFilter string,
	includeColumnsScans Scans, partnSets [][]interface{},
	cons common.Consistency, tsvector *TsConsistency,
	callb ResponseHandler, scanParams map[string]interface{},
	indexVector *IndexVector) (err error) {

	dataEncFmt := c.GetDataEncodingFormat()
	broker := makeDefaultRequestBroker(callb, dataEncFmt)
	return c.ScanInternal("scan6", defnID, requestId, scans, reverse, distinct,
		projection, offset, limit, groupAggr, indexOrder,
		indexKeyNames, inlineFilter, includeColumnsScans, partnSets, cons, tsvector,
		broker, scanParams, indexVector, time.Time{}, 0)
}

func (c *GsiClient) ScanInternal(logPrefix string,
	defnID uint64, requestId string, scans Scans, reverse,
	distinct bool, projection *IndexProjection, offset, limit int64,
	groupAggr *GroupAggr, indexOrder *IndexKeyOrder,
	indexKeyNames []string, inlineFilter string,
	includeColumnScans Scans, partnSets [][]interface{},
	cons common.Consistency, tsvector *TsConsistency,
	broker *RequestBroker, scanParams map[string]interface{},
	indexVector *IndexVector, reqDeadline time.Time, reqDeadlineSlack time.Duration) (err error) {

	if c.bridge == nil {
		return ErrorClientUninitialized
	}

	// check whether the index is present and available.
	if _, err = c.bridge.IndexState(defnID); err != nil {
		return err
	}

	begin := time.Now()

	var generateScanReport bool
	if broker.scanReport != nil {
		generateScanReport = true
	}

	handler := func(qc *GsiScanClient, index *common.IndexDefn, rollbackTime int64, partitions []common.PartitionId,
		handler ResponseHandler) (error, bool) {
		var err error

		dataEncFmt := broker.GetDataEncodingFormat()

		tsvector, err = c.getConsistency(qc, cons, tsvector, index.Bucket)
		if err != nil {
			return err, false
		}

		if c.bridge.IsPrimary(uint64(index.DefnId)) {
			return qc.ScanPrimary(
				uint64(index.DefnId), requestId, scans, reverse, distinct,
				projection, broker.GetOffset(), broker.GetLimit(), groupAggr,
				broker.GetSorted(), cons, tsvector, handler, rollbackTime,
				partitions, dataEncFmt, broker.DoRetry(), scanParams, reqDeadline,
				reqDeadlineSlack, generateScanReport)
		}

		return qc.Scan(
			uint64(index.DefnId), requestId, scans, reverse, distinct,
			projection, broker.GetOffset(), broker.GetLimit(), groupAggr,
			broker.GetSorted(), cons, tsvector, handler, rollbackTime,
			partitions, dataEncFmt, broker.DoRetry(), scanParams, indexVector,
			reqDeadline, reqDeadlineSlack, indexOrder, indexKeyNames, inlineFilter,
			includeColumnScans, generateScanReport)
	}

	broker.SetScanRequestHandler(handler)
	broker.SetLimit(limit)
	broker.SetOffset(offset)
	broker.SetScans(scans)
	broker.SetGroupAggr(groupAggr)
	broker.SetProjection(projection)
	broker.SetSorted(indexOrder != nil)
	broker.SetDistinct(distinct)
	broker.SetIndexOrder(indexOrder)
	broker.SetPartnSets(partnSets)

	_, err = c.doScan(defnID, requestId, broker)
	if err != nil { // callback with error
		return err
	}

	fmsg := "%v {%v,%v} - elapsed(%v) err(%v)"
	logging.Verbosef(fmsg, logPrefix, defnID, requestId, time.Since(begin), err)
	return
}

// -------------------------------------
// StorageStatistics implementation
// -------------------------------------
type StorageStats struct {
	Index         string
	Id            uint64
	PartitionId   common.PartitionId
	LastResetTime uint64
	Stats         map[string]interface{}
}

const STAT_PARTITION_ID = "PARTITION_ID"
const STAT_NUM_PAGES = "NUM_PAGES"
const STAT_NUM_ITEMS = "NUM_ITEMS"
const STAT_RESIDENT_RATIO = "RESIDENT_RATIO"
const STAT_NUM_INSERT = "NUM_INSERT"
const STAT_NUM_DELETE = "NUM_DELETE"
const STAT_AVG_ITEM_SIZE = "AVG_ITEM_SIZE"
const STAT_AVG_PAGE_SIZE = "AVG_PAGE_SIZE"
const STAT_LAST_RESET_TIME = "LAST_RESET_TIME"
const STAT_BHIVE_GRAPH_RES_RATIO = "GRAPH_RES_RATIO"
const STAT_BHIVE_GRAPH_HIT_RATIO = "GRAPH_HIT_RATIO"
const STAT_BHIVE_NUM_VEC_OPS = "NUM_VEC_OPS"
const STAT_BHIVE_GRAPH_DISK_SIZE = "GRAPH_DISK_SIZE"
const STAT_BHIVE_FULL_VEC_SIZE = "FULL_VEC_SIZE"
const STAT_BHIVE_NUM_ITEMS = "BHIVE_NUM_ITEMS"
const STAT_BHIVE_AVG_ITEM_SIZE = "BHIVE_AVG_ITEM_SIZE"

// A set of partitions for given index definition is chosen using metaclient's
// GetScanport. It returns a set of target replica InstanceIds with corresponding
// PartitionIds per InstanceID. It is possible that some partitions are from one
// replica and other are from a different replica, this is the same logic that applies
// when partitions/replica are chosen do a scan (See doScan).
// There is no replica retry based on excludes for storage stats. Consumer of this
// API should retry in case of error.
//
// Steps to retrieve StorageStatistics:
//  1. Get a set of queryports, corresponding targetInstanceIds and partitions per InstanceId
//  2. Get adminports from queryports and construct statsUrls for participating indexer nodes
//  3. For stats of each node, get targetInstanceId of corresponding node and pick partition stats
//     for that targetInstanceId.
//  4. Filter relevant storage stats as needed by CBO
func (c *GsiClient) StorageStatistics(defnID uint64, requestId string) ([]map[string]interface{}, error) {

	var excludes map[common.IndexDefnId]map[common.PartitionId]map[uint64]bool
	skips := make(map[common.IndexDefnId]bool)

	storageMode := c.Settings().StorageMode()

	if storageMode == "forestdb" {
		// StorageStatistics not supported for forestdb
		return nil, nil
	}

	if queryports, _, targetInstIds, _, partitions, _, ok := c.bridge.GetScanport(defnID, excludes, skips); ok {

		// urls is list of Stats REST endpoints for all indexer nodes
		// hosting the requested index
		statUrls := []string{}
		nodes, err := c.Nodes()
		if err != nil {
			return nil, err
		}

		for _, qp := range queryports {
			for _, n := range nodes {
				if qp == n.Queryport {
					url := "http://" + n.Httpport + "/stats/storage?consumerFilter=n1qlStorageStats"
					statUrls = append(statUrls, url)
				}
			}
		}

		stats, err := getStatsFromIndexerNodes(statUrls, targetInstIds, partitions, storageMode)
		if err != nil {
			return nil, err
		}
		return stats, nil
	}

	return nil, errors.New("Unable to retrieve storage statistics from any replica index.")
}

// DefnStorageStatistics is the new API for storage statistics. We get stats for all replicas
// of a definition and return them to the query client per instance
func (c *GsiClient) DefnStorageStatistics(defnID uint64, requestID string) (
	map[common.IndexInstId][]map[string]interface{}, error) {

	storageMode := c.Settings().StorageMode()

	if storageMode == "forestdb" {
		// StorageStatistics not supported for forestdb
		return nil, nil
	}

	var insts = c.bridge.GetIndexReplica(defnID)

	// urls is list of Stats REST endpoints for all indexer nodes
	// hosting the requested index
	statUrls := []string{}

	var targetInstIDs = make([]uint64, 0, len(insts))
	var nodesForInst = make(map[common.IndexerId]string)
	for _, inst := range insts {
		if inst != nil {
			targetInstIDs = append(targetInstIDs, uint64(inst.InstId))
			for _, nodeID := range inst.IndexerId {
				if _, exists := nodesForInst[nodeID]; !exists {
					node, err := c.bridge.GetNode(nodeID)
					if err == nil {
						nodesForInst[nodeID] = node.Httpport
					} else {
						logging.Warnf("DefnStorageStatistics: got err %v for node %v info",
							err, nodeID)
					}
				}
			}
		}
	}

	for _, nodeHTTPPort := range nodesForInst {
		url := "http://" + nodeHTTPPort + "/stats/storage?consumerFilter=n1qlStorageStats"
		statUrls = append(statUrls, url)
	}

	var allNodeStats, err = restGetStatsFromIndexerNodes(statUrls, targetInstIDs)
	if err != nil {
		logging.Warnf("DefnStorageStatistics failed to read storage stats from indexers with error - %v, reqID %v",
			err, requestID)
		return nil, err
	}

	var res = make(map[common.IndexInstId][]map[string]interface{})
	for _, nodeStats := range allNodeStats {
		for _, nodeStat := range nodeStats {
			var id = common.IndexInstId(nodeStat.Id)

			if _, exists := res[id]; !exists {
				res[id] = make([]map[string]interface{}, 0)
			}

			var partnStats = getStatsForPartition(nodeStat, storageMode)
			res[id] = append(res[id], partnStats)
		}
	}

	logging.Debugf("DefnStorageStatistics returning stats for index %v, reqID %v - \n%v",
		defnID, requestID, res)
	return res, nil
}

func restGetStatsFromIndexerNodes(statURLs []string, targetInstIDs []uint64) (
	[][]StorageStats, error) {

	statsSpec := &common.StatsIndexSpec{}
	for _, targetInst := range targetInstIDs {
		statsSpec.Instances = append(statsSpec.Instances, common.IndexInstId(targetInst))
	}

	instBytes, err := json.Marshal(statsSpec)
	if err != nil {
		errStr := fmt.Sprintf("Error marshalling instIDs : %v, err: %v",
			targetInstIDs, err)
		return nil, errors.New(errStr)
	}

	var res = make([][]StorageStats, 0, len(targetInstIDs))

	for _, statURL := range statURLs {

		bodyBuf := bytes.NewBuffer(instBytes)
		resp, err := postWithAuth(statURL, "application/json", bodyBuf, time.Duration(10)*time.Second)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		bytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			errStr := fmt.Sprintf("Error reading stats from %v : %v", statURL, err)
			return nil, errors.New(errStr)
		}

		var nodeStats []StorageStats
		err = commonjson.Unmarshal(bytes, &nodeStats)
		if err != nil {
			errStr := fmt.Sprintf("Error unmarshalling stats from %v : %v", statURL, err)
			return nil, errors.New(errStr)
		}

		res = append(res, nodeStats)
	}

	return res, nil
}

func getStatsFromIndexerNodes(statUrls []string, targetInstIds []uint64,
	partitions [][]common.PartitionId, storageMode string) ([]map[string]interface{}, error) {

	var allNodeStats, err = restGetStatsFromIndexerNodes(statUrls, targetInstIds)
	if err != nil {
		return nil, err
	}
	storageStats := make([]map[string]interface{}, 0)
	for i, nodeStats := range allNodeStats {
		for _, nodeStat := range nodeStats {
			if targetInstIds[i] == nodeStat.Id && contains(partitions[i], nodeStat.PartitionId) {
				partnStats := getStatsForPartition(nodeStat, storageMode)
				storageStats = append(storageStats, partnStats)
			}
		}
	}
	return storageStats, nil
}

func contains(partitionIds []common.PartitionId, partitionId common.PartitionId) bool {
	for _, id := range partitionIds {
		if partitionId == id {
			return true
		}
	}
	return false
}

func getStatsForPartition(instStats StorageStats, storageMode string) map[string]interface{} {

	if storageMode == "plasma" {
		storageStats := make(map[string]interface{})
		storageStats[STAT_PARTITION_ID] = instStats.PartitionId

		stats := instStats.Stats
		if _, ok := stats["MainStore"]; !ok {
			return nil
		}
		mainStoreStats := stats["MainStore"].(map[string]interface{})

		// Bhive specific stats
		if _, exists := mainStoreStats["graph_resident_ratio"]; exists {
			storageStats[STAT_BHIVE_NUM_ITEMS] = mainStoreStats["items_count"]
			storageStats[STAT_BHIVE_AVG_ITEM_SIZE] = mainStoreStats["avg_item_size"]
			storageStats[STAT_BHIVE_GRAPH_RES_RATIO] = mainStoreStats["graph_resident_ratio"]
			storageStats[STAT_BHIVE_GRAPH_HIT_RATIO] = mainStoreStats["graph_hit_ratio"]
			storageStats[STAT_BHIVE_NUM_VEC_OPS] = mainStoreStats["num_vec_ops_per_cell"]
			storageStats[STAT_BHIVE_GRAPH_DISK_SIZE] = mainStoreStats["norm_graph_disk_size"]
			storageStats[STAT_BHIVE_FULL_VEC_SIZE] = mainStoreStats["norm_full_vector_size"]
		} else {
			// Plasma stats
			storageStats[STAT_LAST_RESET_TIME] = instStats.LastResetTime
			storageStats[STAT_NUM_PAGES] = mainStoreStats["num_pages"]
			storageStats[STAT_NUM_ITEMS] = mainStoreStats["items_count"]
			storageStats[STAT_RESIDENT_RATIO] = mainStoreStats["resident_ratio"]
			storageStats[STAT_NUM_INSERT] = mainStoreStats["inserts"]
			storageStats[STAT_NUM_DELETE] = mainStoreStats["deletes"]
			storageStats[STAT_AVG_ITEM_SIZE] = mainStoreStats["avg_item_size"]
			storageStats[STAT_AVG_PAGE_SIZE] = mainStoreStats["avg_page_size"]
		}
		return storageStats
	}

	if storageMode == "memory_optimized" {
		storageStats := make(map[string]interface{})
		storageStats[STAT_PARTITION_ID] = instStats.PartitionId
		stats := instStats.Stats
		items_count := stats["items_count"].(int64)
		data_size := stats["data_size"].(int64)
		avg_item_size := int64(0)
		if items_count > 0 {
			avg_item_size = data_size / items_count
		}
		storageStats[STAT_NUM_ITEMS] = items_count
		storageStats[STAT_AVG_ITEM_SIZE] = avg_item_size
		return storageStats
	}

	return nil
}

//-------------------------------------
// StorageStatistics implementation end
//-------------------------------------

// DescribeError return error description as human readable string.
func (c *GsiClient) DescribeError(err error) string {
	if desc, ok := errorDescriptions[err.Error()]; ok {
		return desc
	}
	return err.Error()
}

// DescribeError return error description as human readable string.
func (c *GsiClient) Settings() *ClientSettings {
	return c.settings
}

// Close the client and all open connections with server.
func (c *GsiClient) Close() {
	if c == nil {
		return
	}
	if c.settings != nil {
		c.settings.Close()
	}
	if c.bridge == nil {
		return
	}
	c.bridge.Close()
	qcs := *((*map[string]*GsiScanClient)(atomic.LoadPointer(&c.queryClients)))
	for _, qc := range qcs {
		qc.Close()
	}
	close(c.killch)
}

// This function updates the scan clients based on the list of available
// scan ports. The list of scan ports is maintained by looking at the
// indexer nodes from the cluster topology (currmeta).
// Note that this function is not responsible for updating currmeta itself.
func (c *GsiClient) updateScanClients() {

	newclients, staleclients, closedclients := map[string]bool{}, map[string]bool{}, map[string]bool{}

	needsRefresh := func() bool {
		qcs := *((*map[string]*GsiScanClient)(atomic.LoadPointer(&c.queryClients)))
		scanPorts := c.bridge.GetScanports()
		if len(qcs) != len(scanPorts) {
			return true
		}

		cache := map[string]bool{}
		for _, queryport := range scanPorts {
			cache[queryport] = true
			if _, ok := qcs[queryport]; !ok {
				return true
			}
		}

		for queryport, qc := range qcs {
			if _, ok := cache[queryport]; !ok {
				return true
			}

			if qc.IsClosed() {
				return true
			}
		}

		return false
	}

	if !needsRefresh() {
		return
	}

	// With respect to the performance, taking a lock here should be fine as
	// 1. This happens only in case of replica retry or topology change.
	// 2. needsRefresh avoids unnecessary locks.
	// 3. Double check locking approach helps in avoiding multiple threads
	//    performing the same steps for keeping queryClients list updated.
	// Also note that, the steps for keeping queryClients list updated have
	// side effect. For example, it is not a good idea that multiple threads
	// trying to close the scan client concurrently.

	c.qcLock.Lock()
	defer c.qcLock.Unlock()

	cache := map[string]bool{}
	qcsPtr := atomic.LoadPointer(&c.queryClients)
	qcs := *((*map[string]*GsiScanClient)(qcsPtr))
	// add new indexer-nodes
	for _, queryport := range c.bridge.GetScanports() {
		cache[queryport] = true
		if qc, ok := qcs[queryport]; !ok {
			newclients[queryport] = true
		} else {
			if qc.IsClosed() {
				closedclients[queryport] = true
			}
		}
	}
	// forget stale indexer-nodes.
	for queryport, qc := range qcs {
		if _, ok := cache[queryport]; !ok {
			qc.Close()
			staleclients[queryport] = true
		}
	}
	if len(newclients) > 0 || len(staleclients) > 0 || len(closedclients) > 0 {
		clients := make(map[string]*GsiScanClient)
		for queryport, qc := range qcs {
			if _, ok := staleclients[queryport]; ok {
				continue
			}

			if qc.IsClosed() {
				logging.Infof("Found a closed scanclient for %v. Initializing a new scan client.", queryport)
				if qc, err := NewGsiScanClient(queryport, c.cluster, c.config, c.needsAuth); err == nil {
					clients[queryport] = qc
				} else {
					logging.Errorf("Unable to initialize gsi scanclient (%v)", err)
				}
				continue
			}

			// If the client is not stale and not closed, try to refresh server version.
			qc.RefreshServerVersion()
			clients[queryport] = qc
		}

		for queryport := range newclients {
			if qc, err := NewGsiScanClient(queryport, c.cluster, c.config, c.needsAuth); err == nil {
				clients[queryport] = qc
			} else {
				logging.Errorf("Unable to initialize gsi scanclient (%v)", err)
			}
		}

		atomic.StorePointer(&c.queryClients, unsafe.Pointer(&clients))
	}
}

func (c *GsiClient) getScanClients(queryports []string) ([]*GsiScanClient, bool) {

	qcs := *((*map[string]*GsiScanClient)(atomic.LoadPointer(&c.queryClients)))

	qc := make([]*GsiScanClient, len(queryports))
	var ok bool

	for i, queryport := range queryports {
		if _, ok = qcs[queryport]; ok {
			qc[i] = qcs[queryport]
		} else {
			break
		}
	}

	return qc, ok
}

func (c *GsiClient) updateExcludes(defnID uint64, excludes map[common.IndexDefnId]map[common.PartitionId]map[uint64]bool,
	errMap map[common.PartitionId]map[uint64]error) map[common.IndexDefnId]map[common.PartitionId]map[uint64]bool {

	defnId := common.IndexDefnId(defnID)

	if excludes == nil {
		excludes = make(map[common.IndexDefnId]map[common.PartitionId]map[uint64]bool)
	}

	if _, ok := excludes[defnId]; !ok {
		excludes[defnId] = make(map[common.PartitionId]map[uint64]bool)
	}

	for partnId, instErrMap := range errMap {
		for instId, err := range instErrMap {
			if !isgone(err) {
				if _, ok := excludes[defnId][partnId]; !ok {
					excludes[defnId][partnId] = make(map[uint64]bool)
				}
				excludes[defnId][partnId][instId] = true
			} else {
				// if it is network error, then
				// exclude all partitions on all replicas
				// residing on the failed node.

				// doScan() may scan the partition from insts or rebalInsts.
				// So the failure can be coming from insts or rebalInsts.
				// But the function GetIndexInst() will only look for the
				// inst under insts.
				// 1) Non-partitioned index.  InstId unqiuely identify if
				//    it is coming from insts or rebalInsts.  If it is
				//    coming from rebalInsts, we do not add it to exclude map.
				// 2) Partitioned index.  Same InstId could be used in both
				//    insts or rebalInsts.  If inst contains the partition,
				//    then it will add to the exclude map, even if
				//    the error may indeed coming from rebalInsts.  This is
				//    fine since inst must be skipped in pickRandom
				//    in the first place, otherwise, rebalInsts will not be used.
				//    So adding it to the exclude list will not affect skipRandom.
				//
				if inst := c.bridge.GetIndexInst(instId); inst != nil {
					failIndexerId := inst.IndexerId[partnId]

					for _, replica := range c.bridge.GetIndexReplica(defnID) {
						for p, indexerId := range replica.IndexerId {
							if indexerId == failIndexerId {
								if _, ok := excludes[defnId][p]; !ok {
									excludes[defnId][p] = make(map[uint64]bool)
								}
								excludes[defnId][p][uint64(replica.InstId)] = true
							}
						}
					}
				}
			}
		}
	}

	return excludes
}

func (c *GsiClient) makeScanClient(scanport string) *GsiScanClient {

	if qc, ok := c.getScanClients([]string{scanport}); ok {
		return qc[0]
	}

	return nil
}

func (c *GsiClient) doScan(defnID uint64, requestId string, broker *RequestBroker) (int64, error) {

	atomic.AddInt64(&c.numScans, 1)
	defer atomic.AddInt64(&c.numScans, -1)

	var excludes map[common.IndexDefnId]map[common.PartitionId]map[uint64]bool
	var err error

	broker.SetResponseTimer(c.bridge.Timeit)
	skips := make(map[common.IndexDefnId]bool)
	partnSets := broker.GetPartnSets()

	wait := c.config["retryIntervalScanport"].Int()
	retry := c.config["retryScanPort"].Int()

	retryCount := -1
	defer func() {
		if broker.scanReport != nil {
			broker.scanReport.Retries = retryCount
		}
	}()

	for i := 0; true; {
		retryCount++
		foundScanport := false

		queryports, targetDefnID, targetInstIds, rollbackTimes, partitions, numPartitions, ok := c.bridge.GetScanport(defnID, excludes, skips)

		if broker.scanReport != nil && defnID != uint64(broker.scanReport.DefnID) {
			broker.scanReport.DefnID = common.IndexDefnId(defnID)
		}

		var index *common.IndexDefn
		if ok {
			index = c.bridge.GetIndexDefn(targetDefnID)
			if index == nil {
				err = fmt.Errorf("Index definition not found")
			}
		}

		if ok && index != nil {

			start := time.Now()
			count, scan_errs, partial, refresh := broker.scatter(c.makeScanClient, index, queryports, targetInstIds,
				rollbackTimes, partitions, numPartitions, partnSets, c.settings)

			if !refresh {
				foundScanport = true

				if c.isTimeit(scan_errs) {
					c.updateScanResponse(time.Now().Sub(start).Nanoseconds())
					return count, getScanError(scan_errs)
				}

				excludes = c.updateExcludes(defnID, excludes, scan_errs)
				if len(scan_errs) != 0 && partial {
					// partially succeeded scans, we don't reset-hash and we don't retry
					return 0, getScanError(scan_errs)

				} else { // TODO: make this error message precise
					// reset the hash so that we do a full STATS for next query.
					c.setBucketHash(index.Bucket, 0)
				}
				err = fmt.Errorf("%v from %v", getScanError(scan_errs), queryports)

				if len(queryports) == len(partitions) && len(queryports) == len(targetInstIds) {
					for i, _ := range queryports {
						logging.Warnf("scan failed: requestId %v queryport %v inst %v partition %v", requestId, queryports[i], targetInstIds[i], partitions[i])
					}
				}
			}
		}

		// If there is an error coming from indexer that cannot serve the scan request
		// (including io error), then exclude this defnID and retry with another replica.
		// If we exhaust all the replica, then GetScanport() will return ok=false.
		if foundScanport {
			logging.Warnf(
				"Scan failed with error for index %v.  Trying scan again with replica, reqId:%v : %v ...\n",
				defnID, requestId, err)
			continue
		}

		// If we cannot find a valid scansport, then retry up to retryScanport by refreshing
		// the clients.
		if i = i + 1; i < retry {
			excludes = nil
			skips = make(map[common.IndexDefnId]bool)
			broker.SetRetry(true)
			logging.Warnf(
				"Fail to find indexers to satisfy query request.  Trying scan again for index %v, reqId:%v : %v ...\n",
				defnID, requestId, err)
			c.updateScanClients()
			time.Sleep(time.Duration(wait) * time.Millisecond)
			continue
		}

		logging.Warnf("Fail to find indexers to satisfy query request.  Terminate scan for index %v,  reqId:%v : %v\n",
			defnID, requestId, err)
		break
	}
	if err != nil {
		return 0, err
	}
	return 0, ErrorNoHost
}

func (c *GsiClient) isTimeit(errMap map[common.PartitionId]map[uint64]error) bool {
	if len(errMap) == 0 {
		return true
	}

	for _, instErrMap := range errMap {
		for _, err := range instErrMap {
			if err.Error() != common.ErrClientCancel.Error() {
				return false
			}
		}
	}

	return true
}

// TODO (Collections): Not used in 4.5 onwards. Changes to below method
// will be taken care of as part of scan consistency task for collections
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
		ptr := atomic.LoadPointer(&c.bucketHash)
		oldm := (*map[string]uint64)(ptr)
		newm := map[string]uint64{}
		for k, v := range *oldm {
			newm[k] = v
		}
		newm[bucketn] = crc64
		if atomic.CompareAndSwapPointer(&c.bucketHash, ptr, unsafe.Pointer(&newm)) {
			return
		}
	}
}

func (c *GsiClient) getBucketHash(bucketn string) (uint64, bool) {
	bucketHash := (*map[string]uint64)(atomic.LoadPointer(&c.bucketHash))
	crc64, ok := (*bucketHash)[bucketn]
	return crc64, ok
}

// create GSI client using cbqBridge and ScanCoordinator
func makeWithCbq(cluster string, config common.Config, encryptLocalHost bool) (*GsiClient, error) {
	var err error

	var needsAuth uint32

	c := &GsiClient{
		cluster:   cluster,
		config:    config,
		needsAuth: &needsAuth,
	}

	if err := c.initSecurityContext(encryptLocalHost); err != nil {
		return nil, err
	}

	watchClusterVer(cluster)

	atomic.StorePointer(&c.bucketHash, (unsafe.Pointer)(new(map[string]uint64)))
	if c.bridge, err = newCbqClient(cluster); err != nil {
		return nil, err
	}
	clients := make(map[string]*GsiScanClient)
	for _, queryport := range c.bridge.GetScanports() {
		if qc, err := NewGsiScanClient(queryport, c.cluster, config, c.needsAuth); err == nil {
			clients[queryport] = qc
		}
	}
	atomic.StorePointer(&c.queryClients, unsafe.Pointer(&clients))
	return c, nil
}

func makeWithMetaProvider(
	cluster string, config common.Config, needRefresh bool, encryptLocalHost bool) (c *GsiClient, err error) {

	// Note: Query client settings changed before the first query is run
	// will not generate an event as we will not have a client by then.
	if err := systemevent.InitSystemEventLogger(cluster); err != nil {
		return nil, err
	}

	var needsAuth uint32

	c = &GsiClient{
		cluster:      cluster,
		config:       config,
		queryClients: unsafe.Pointer(new(map[string]*GsiScanClient)),
		metaCh:       make(chan bool, 1),
		settings:     NewClientSettings(needRefresh, config),
		killch:       make(chan bool, 1),
		needsAuth:    &needsAuth,
	}

	if err := c.initSecurityContext(encryptLocalHost); err != nil {
		return nil, err
	}

	watchClusterVer(cluster)

	atomic.StorePointer(&c.bucketHash, (unsafe.Pointer)(new(map[string]uint64)))
	c.bridge, err = newMetaBridgeClient(cluster, config, c.metaCh, c.settings)
	if err != nil {
		return nil, err
	}

	c.updateScanClients()
	go c.listenMetaChange(c.killch)
	go c.logstats(c.killch)
	return c, nil
}

func (c *GsiClient) listenMetaChange(killch chan bool) {
	for {
		select {
		case <-c.metaCh:
			c.updateScanClients()
		case <-killch:
			return
		}
	}
}

func (c *GsiClient) logstats(killch chan bool) {

	logtick := time.Duration(c.config["logtick"].Int()) * time.Millisecond
	tick := time.NewTicker(logtick)

	defer func() {
		tick.Stop()
	}()

	for {
		select {
		case <-tick.C:
			logging.Infof("num concurrent scans {%v}", atomic.LoadInt64(&c.numScans))
			logging.Infof("average scan response {%v ms}", atomic.LoadInt64(&c.scanResponse)/int64(time.Millisecond))
		case <-killch:
			return
		}
	}
}

func (c *GsiClient) updateScanResponse(value int64) {

	current := atomic.LoadInt64(&c.scanResponse)
	atomic.StoreInt64(&c.scanResponse, (current+value)/2)
}

func (c *GsiClient) SetDataEncodingFormat(val common.DataEncodingFormat) {
	atomic.StoreUint32(&c.dataEncFmt, uint32(val))
}

func (c *GsiClient) GetDataEncodingFormat() common.DataEncodingFormat {
	if !c.Settings().AllowCJsonScanFormat() {
		return common.DATA_ENC_JSON
	}

	return common.DataEncodingFormat(atomic.LoadUint32(&c.dataEncFmt))
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
		return nil, "before"
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

func isAnyGone(scan_err map[common.PartitionId]map[uint64]error) bool {

	if len(scan_err) == 0 {
		return false
	}

	for _, instErrs := range scan_err {
		for _, err := range instErrs {
			if isgone(err) {
				return true
			}
		}
	}

	return false
}

func isgone(scan_err error) bool {
	if scan_err == nil {
		return false
	}

	// if indexer crash in the middle of scan, it can return EOF
	// if a scan is sent to a already crashed indexer, it will return connection refused
	if scan_err == io.EOF {
		return true
	} else if err, ok := scan_err.(net.Error); ok && err.Timeout() {
		return true
	} else if strings.Contains(scan_err.Error(), syscall.ECONNRESET.Error()) || // connection reset
		strings.Contains(scan_err.Error(), syscall.EPIPE.Error()) { // broken pipe
		return true
	}
	return false
}

func getScanError(errMap map[common.PartitionId]map[uint64]error) error {

	if len(errMap) == 0 {
		return nil
	}

	errs := make(map[string]bool)

	for _, instErrMap := range errMap {
		for _, scan_err := range instErrMap {
			if !errs[scan_err.Error()] {
				errs[scan_err.Error()] = true
			}
		}
	}

	var allErrs string
	for errStr, _ := range errs {
		allErrs = fmt.Sprintf("%v %v", allErrs, errStr)
	}

	return fmt.Errorf("%v", allErrs)
}

func (c *GsiClient) initSecurityContext(encryptLocalHost bool) (err error) {
	pInitOnceRetyOnErr.Do(func() error {
		logger := func(err error) { common.Console(c.cluster, err.Error()) }
		if err = security.InitSecurityContextForClient(logger, c.cluster, "", "", "", encryptLocalHost); err != nil {
			return err
		}

		if err = common.RefreshSecurityContextOnTopology(c.cluster); err != nil {
			return err
		}

		go common.MonitorServiceForPortChanges(c.cluster)
		return nil
	})

	return
}

// refreshSecurityContextOnTopology - DEPRECATED. use common.RefreshSecurityContextOnTopology
func refreshSecurityContextOnTopology(clusterAddr string) error {
	return common.RefreshSecurityContextOnTopology(clusterAddr)
}

var watchingClusterVer uint32

func watchClusterVer(cluster string) {
	if atomic.CompareAndSwapUint32(&watchingClusterVer, 0, 1) {
		go common.WatchClusterVersionChanges(cluster, int64(common.INDEXER_76_VERSION))
		go common.MonitorInternalVersion(int64(common.INDEXER_76_VERSION), common.MIN_VER_SRV_AUTH, cluster)
	}
}
