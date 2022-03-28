// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package n1ql

import (
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	l "github.com/couchbase/indexing/secondary/logging"

	"github.com/couchbase/indexing/secondary/collatejson"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/security"

	qclient "github.com/couchbase/indexing/secondary/queryport/client"

	mclient "github.com/couchbase/indexing/secondary/manager/client"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/parser"
	"github.com/couchbase/query/timestamp"
	"github.com/couchbase/query/value"

	qlog "github.com/couchbase/query/logging"

	json "github.com/couchbase/indexing/secondary/common/json"
)

const DONEREQUEST = 1
const BACKFILLPREFIX = "scan-results"
const BACKFILLTICK = 5

// ErrorIndexEmpty is index not initialized.
var ErrorIndexEmpty = errors.NewError(
	fmt.Errorf("gsi.indexEmpty"), "Fatal null reference to index")

var ErrorInternal = errors.NewError(fmt.Errorf("Internal Error"), "Internal Error")

// ErrorIndexNotAvailable means client indexes list needs to be
// refreshed.
var ErrorIndexNotAvailable = fmt.Errorf("index not available")

var n1ql2GsiInclusion = map[datastore.Inclusion]qclient.Inclusion{
	datastore.NEITHER: qclient.Neither,
	datastore.LOW:     qclient.Low,
	datastore.HIGH:    qclient.High,
	datastore.BOTH:    qclient.Both,
}

var ErrorScheduledCreate = fmt.Errorf("This index is scheduled for background creation." +
	" The operation is not allowed on this index at this time. Please wait until the index is created.")

var ErrorScheduledCreateFailed = fmt.Errorf("This index was scheduled for background creation" +
	" but the background creation has failed. Please drop and recreate this index.")

// TODO: Right now, using index state nil for index scheduled for background
// creation. This maps to datastore OFFLINE state. But here the OFFLINE state
// is overloaded. Can not use PENDING state as well as it will break workflow
// for "build all unbuilt indexes".
//
// There is a need for new datastore state to represent index scheduled for
// background creation.
var gsi2N1QLState = map[c.IndexState]datastore.IndexState{
	c.INDEX_STATE_CREATED:   datastore.PENDING, // might also be DEFERRED
	c.INDEX_STATE_READY:     datastore.PENDING, // might also be DEFERRED
	c.INDEX_STATE_INITIAL:   datastore.BUILDING,
	c.INDEX_STATE_CATCHUP:   datastore.BUILDING,
	c.INDEX_STATE_ACTIVE:    datastore.ONLINE,
	c.INDEX_STATE_DELETED:   datastore.OFFLINE,
	c.INDEX_STATE_ERROR:     datastore.OFFLINE,
	c.INDEX_STATE_SCHEDULED: datastore.SCHEDULED,
}

var n1ql2GsiConsistency = map[datastore.ScanConsistency]c.Consistency{
	datastore.UNBOUNDED: c.AnyConsistency,
	datastore.SCAN_PLUS: c.SessionConsistency,
	datastore.AT_PLUS:   c.QueryConsistency,
}

//--------------------
// datastore.Indexer{}
//--------------------

// contains all index loaded via gsi cluster.
type gsiKeyspace struct {
	// for 8-byte alignment fot atomic access.
	scandur        int64
	blockeddur     int64
	throttledur    int64
	primedur       int64
	totalscans     int64
	backfillSize   int64
	totalbackfills int64

	rw         sync.RWMutex
	clusterURL string
	namespace  string // pool
	bucket     string
	scope      string
	keyspace   string // collection name when keyspace maps to collection

	gsiClient      *qclient.GsiClient
	config         c.Config
	indexes        map[uint64]datastore.Index // defnID -> index
	primaryIndexes map[uint64]datastore.PrimaryIndex
	logPrefix      string
	version        uint64
	clusterVersion uint64

	prevTotalScans int64 // used in monitoring
	closed         uint32
}

// NewGSIIndexer manage new set of indexes under namespace->keyspace,
// also called as, pool->bucket.
// will return an error when,
// - GSI cluster is not available.
// - network partitions / errors.
func NewGSIIndexer(clusterURL, namespace, keyspace string,
	securityconf *datastore.ConnectionSecurityConfig) (datastore.Indexer, errors.Error) {

	//TODO (Collections): This method can be blocked after integration
	// with query service to disallow creation of bucket level keyspace

	// passing keyspace as bucket name from old to new API
	return NewGSIIndexer2(clusterURL, namespace, keyspace, c.DEFAULT_SCOPE, c.DEFAULT_COLLECTION, securityconf)
}

// During cluster version < 7.0 (mixed mode), scope and keyspace
// are expected to be only _default as non-default collections cannot be
// created during mixed mode.
func NewGSIIndexer2(clusterURL, namespace, bucket, scope, keyspace string,
	securityconf *datastore.ConnectionSecurityConfig) (datastore.Indexer, errors.Error) {

	l.SetLogLevel(l.Info)

	gsi := &gsiKeyspace{
		clusterURL:     clusterURL,
		namespace:      namespace,
		bucket:         bucket,
		scope:          scope,
		keyspace:       keyspace,
		indexes:        make(map[uint64]datastore.Index), // defnID -> index
		primaryIndexes: make(map[uint64]datastore.PrimaryIndex),
	}

	tm := time.Now().UnixNano()
	gsi.logPrefix = fmt.Sprintf("GSIC[%s/%s-%s-%s-%v]", namespace, bucket, scope, keyspace, tm)

	// get the singleton-client
	conf, err := c.GetSettingsConfig(c.SystemConfig)
	if err != nil {
		return nil, errors.NewError(err, "GSI config instantiation failed")
	}
	qconf := conf.SectionConfig("queryport.client.", true /*trim*/)
	client, err := getSingletonClient(clusterURL, conf, securityconf)
	if err != nil {
		l.Errorf("%v GSI instantiation failed: %v", gsi.logPrefix, err)
		return nil, errors.NewError(err, "GSI client instantiation failed")
	}
	gsi.gsiClient, gsi.config = client, qconf
	// refresh indexes for this service->namespace->keyspace
	if err := gsi.Refresh(); err != nil {
		l.Errorf("%v Refresh() failed: %v", gsi.logPrefix, err)
		return nil, err
	}
	l.Infof("%v started ...", gsi.logPrefix)

	logtick := time.Duration(qconf["logtick"].Int()) * time.Millisecond

	MonitorIndexer(gsi, BACKFILLTICK*time.Second, logtick)

	return gsi, nil
}

//
// change security setting
//
func (gsi *gsiKeyspace) SetConnectionSecurityConfig(conf *datastore.ConnectionSecurityConfig) {

	security.Refresh(conf.TLSConfig, conf.ClusterEncryptionConfig, conf.CertFile, conf.KeyFile, conf.CAFile)
}

// A global API that is independent of keyspace
func SetConnectionSecurityConfig(conf *datastore.ConnectionSecurityConfig) {
	security.Refresh(conf.TLSConfig, conf.ClusterEncryptionConfig, conf.CertFile, conf.KeyFile, conf.CAFile)
}

// KeyspaceId implements datastore.Indexer{} interface.
// Id of the keyspace to which this indexer belongs
func (gsi *gsiKeyspace) KeyspaceId() string {
	return gsi.keyspace
}

func (gsi *gsiKeyspace) BucketId() string {
	return gsi.bucket
}

func (gsi *gsiKeyspace) ScopeId() string {
	return gsi.scope
}

// Name implements datastore.Indexer{} interface.
// Unique within a Keyspace.
func (gsi *gsiKeyspace) Name() datastore.IndexType {
	return datastore.GSI
}

// IndexIds implements datastore.Indexer{} interface. Ids of the
// latest set of indexes from GSI cluster, defined on this keyspace.
func (gsi *gsiKeyspace) IndexIds() ([]string, errors.Error) {
	if err := gsi.Refresh(); err != nil {
		return nil, err
	}

	gsi.rw.RLock()
	defer gsi.rw.RUnlock()
	ids := make([]string, 0, len(gsi.indexes))
	for _, index := range gsi.indexes {
		ids = append(ids, index.Id())
	}
	for _, index := range gsi.primaryIndexes {
		ids = append(ids, index.Id())
	}
	l.Debugf("%v IndexIds %v", gsi.logPrefix, ids)
	return ids, nil
}

// IndexNames implements datastore.Indexer{} interface. Names of the
// latest set of indexes from GSI cluster, defined on this keyspace.
func (gsi *gsiKeyspace) IndexNames() ([]string, errors.Error) {
	if err := gsi.Refresh(); err != nil {
		return nil, err
	}

	gsi.rw.RLock()
	defer gsi.rw.RUnlock()
	names := make([]string, 0, len(gsi.indexes))
	for _, index := range gsi.indexes {
		names = append(names, index.Name())
	}
	for _, index := range gsi.primaryIndexes {
		names = append(names, index.Name())
	}
	l.Debugf("%v IndexNames %v", gsi.logPrefix, names)
	return names, nil
}

// IndexById implements datastore.Indexer{} interface. Find an index on this
// keyspace using the index's id.
func (gsi *gsiKeyspace) IndexById(id string) (datastore.Index, errors.Error) {
	gsi.rw.RLock()
	defer gsi.rw.RUnlock()
	defnID := string2defnID(id)
	index, ok := gsi.indexes[defnID]
	if !ok {
		index, ok = gsi.primaryIndexes[defnID]
		if !ok {
			errmsg := fmt.Sprintf("GSI index id %v not found.", id)
			err := errors.NewError(nil, errmsg)
			return nil, errors.NewCbIndexNotFoundError(err)
		}
	}
	arg1 := l.TagUD(index)
	l.Debugf("%v IndexById %v = %v", gsi.logPrefix, id, arg1)
	return index, nil
}

// IndexByName implements datastore.Indexer{} interface. Find an index on
// this keyspace using the index's name.
func (gsi *gsiKeyspace) IndexByName(name string) (datastore.Index, errors.Error) {
	gsi.rw.RLock()
	defer gsi.rw.RUnlock()

	for _, index := range gsi.indexes {
		if index.Name() == name {
			return index, nil
		}
	}
	for _, index := range gsi.primaryIndexes {
		if index.Name() == name {
			return index, nil
		}
	}
	err := errors.NewError(nil, fmt.Sprintf("GSI index %v not found.", name))
	return nil, errors.NewCbIndexNotFoundError(err)
}

// Indexes implements datastore.Indexer{} interface. Return the latest
// set of all indexes from GSI cluster, defined on this keyspace.
func (gsi *gsiKeyspace) Indexes() ([]datastore.Index, errors.Error) {
	if err := gsi.Refresh(); err != nil {
		return nil, err
	}

	gsi.rw.RLock()
	defer gsi.rw.RUnlock()
	indexes := make([]datastore.Index, 0, len(gsi.indexes))
	for _, index := range gsi.indexes {
		indexes = append(indexes, index)
	}
	for _, index := range gsi.primaryIndexes {
		indexes = append(indexes, index)
	}

	arg1 := l.TagUD(indexes)
	l.Debugf("%v gsiKeySpace.Indexes(): %v", gsi.logPrefix, arg1)
	return indexes, nil
}

// PrimaryIndexes implements datastore.Indexer{} interface. Returns the
// server-recommended primary indexes.
func (gsi *gsiKeyspace) PrimaryIndexes() ([]datastore.PrimaryIndex, errors.Error) {
	if err := gsi.Refresh(); err != nil {
		return nil, err
	}

	gsi.rw.RLock()
	defer gsi.rw.RUnlock()
	indexes := make([]datastore.PrimaryIndex, 0, len(gsi.primaryIndexes))
	for _, index := range gsi.primaryIndexes {
		indexes = append(indexes, index)
	}
	arg1 := l.TagUD(indexes)
	l.Debugf("%v gsiKeySpace.PrimaryIndexes(): %v", gsi.logPrefix, arg1)
	return indexes, nil
}

// CreatePrimaryIndex implements datastore.Indexer{} interface. Create or
// return a primary index on this keyspace
func (gsi *gsiKeyspace) CreatePrimaryIndex(
	requestId, name string, with value.Value) (datastore.PrimaryIndex, errors.Error) {

	return gsi.CreatePrimaryIndex3(requestId, name, nil, with)
}

// CreateIndex3 implements datastore.Indexer3{} interface. Create a secondary
// index on this keyspace
func (gsi *gsiKeyspace) CreatePrimaryIndex3(
	requestId, name string, indexPartition *datastore.IndexPartition,
	with value.Value) (pi datastore.PrimaryIndex, retErr errors.Error) {

	defer func() {
		if r := recover(); r != nil {
			pi = nil
			retErr = ErrorInternal
			l.Errorf("CreatePrimaryIndex3::Recovered from panic. Stacktrace %v", string(debug.Stack()))
		}
	}()

	var withJSON []byte
	var err error
	if with != nil {
		if withJSON, err = with.MarshalJSON(); err != nil {
			return nil, errors.NewError(err, "GSI error marshalling WITH clause")
		}
	}

	// partition
	partitionScheme := c.PartitionScheme(c.SINGLE)
	var partitionKeys []string
	if indexPartition != nil {
		partitionScheme = partitionKey(indexPartition.Strategy)
		for _, expr := range indexPartition.Exprs {
			partitionKeys = append(partitionKeys, expression.NewStringer().Visit(expr))
		}
	}

	defnID, err := gsi.gsiClient.CreateIndex4(
		name,
		gsi.bucket,   /*bucket-name*/
		gsi.scope,    /*scope-name*/
		gsi.keyspace, /*collection-name*/
		"GSI",        /*using*/
		"N1QL",       /*exprType*/
		"",           /*whereStr*/
		nil,          /*secStrs*/
		nil,          /* desc */
		false,        /* indexMissingLeadingKey */
		true,         /*isPrimary*/
		partitionScheme,
		partitionKeys,
		withJSON)
	if err != nil {
		return nil, errors.NewError(err, "GSI CreatePrimaryIndex()")
	}
	// refresh to get back the newly created index.
	if err := gsi.Refresh(); err != nil {
		return nil, err
	}
	index, errr := gsi.IndexById(defnID2String(defnID))
	if errr != nil {
		return nil, errr
	}
	return index.(datastore.PrimaryIndex), nil
}

// CreateIndex implements datastore.Indexer{} interface. Create a secondary
// index on this keyspace
func (gsi *gsiKeyspace) CreateIndex(
	requestId, name string, seekKey, rangeKey expression.Expressions,
	where expression.Expression, with value.Value) (
	si datastore.Index, retErr errors.Error) {

	defer func() {
		if r := recover(); r != nil {
			si = nil
			retErr = ErrorInternal
			l.Errorf("CreateIndex::Recovered from panic. Stacktrace %v", string(debug.Stack()))
		}
	}()

	var partnStr string
	if seekKey != nil && len(seekKey) > 0 {
		partnStr = expression.NewStringer().Visit(seekKey[0])
	}

	var whereStr string
	if where != nil {
		whereStr = expression.NewStringer().Visit(where)
	}

	secStrs := make([]string, len(rangeKey))
	for i, key := range rangeKey {
		s := expression.NewStringer().Visit(key)
		secStrs[i] = s
	}

	var withJSON []byte
	var err error
	if with != nil {
		if withJSON, err = with.MarshalJSON(); err != nil {
			return nil, errors.NewError(err, "GSI error marshalling WITH clause")
		}
	}
	defnID, err := gsi.gsiClient.CreateIndex(
		name,
		gsi.bucket, /*bucket-name*/
		"GSI",      /*using*/
		"N1QL",     /*exprType*/
		partnStr, whereStr, secStrs,
		false, /*isPrimary*/
		withJSON)
	if err != nil {
		return nil, errors.NewError(err, "GSI CreateIndex()")
	}
	// refresh to get back the newly created index.
	if err := gsi.Refresh(); err != nil {
		return nil, err
	}
	return gsi.IndexById(defnID2String(defnID))
}

// CreateIndex2 implements datastore.Indexer2{} interface. Create a secondary
// index on this keyspace
func (gsi *gsiKeyspace) CreateIndex2(
	requestId, name string, seekKey expression.Expressions, rangeKey datastore.IndexKeys,
	where expression.Expression, with value.Value) (
	datastore.Index, errors.Error) {

	return gsi.CreateIndex3(requestId, name, rangeKey, nil, where, with)
}

// CreateIndex3 implements datastore.Indexer3{} interface. Create a secondary
// index on this keyspace
func (gsi *gsiKeyspace) CreateIndex3(
	requestId, name string, rangeKey datastore.IndexKeys, indexPartition *datastore.IndexPartition,
	where expression.Expression, with value.Value) (si datastore.Index, retErr errors.Error) {

	defer func() {
		if r := recover(); r != nil {
			si = nil
			retErr = ErrorInternal
			l.Errorf("CreateIndex3::Recovered from panic. Stacktrace %v", string(debug.Stack()))
		}
	}()

	// where
	var whereStr string
	if where != nil {
		whereStr = expression.NewStringer().Visit(where)
	}

	// index keys
	secStrs := make([]string, 0)
	desc := make([]bool, 0)
	indexMissingLeadingKey := false
	// For flattened array index, explode the secExprs string. E.g.,
	// for the index:
	// create index idx on default(org,
	//        DISTINCT ARRAY FLATTEN_KEYS(v.name, v.age DESC, v.year) for v in dob end, email)
	// The secExprs array will contain
	// [`org`, `DISTINCT ARRAY FLATTEN_KEYS(v.name, v.age DESC, v.year) for v in dob end`,
	//         `DISTINCT ARRAY FLATTEN_KEYS(v.name, v.age DESC, v.year) for v in dob end`,
	//         `DISTINCT ARRAY FLATTEN_KEYS(v.name, v.age DESC, v.year) for v in dob end`,
	//         `email`]
	//
	// The number of times the "FLATTEN_KEYS" expression is repated inside the secExprs
	// is proportional to the number of fields in the "FLATTEN_KEYS" expression.
	//
	// The `secExprs` slice is exploded for two reasons:
	//   a. To match the DESC attributes for flattened array expressions. In the
	//      above example, the desc slice is constructed as [false, false, true, false, false].
	//      The length of desc slice and len(secExprs) are made equal
	//   b. For scans which depend on key positions (like group, groupAggrs),
	//      match the key position in scans with the key positions in secExprs.
	//      E.g., keyPos 4 in scans will map to keyPos 4 in secExprs i.e. `email`
	for keyPos, key := range rangeKey {
		s := expression.NewStringer().Visit(key.Expr)
		isArray, _, isFlatten := key.Expr.IsArrayIndexKey()

		if isArray && isFlatten {
			if all, ok := key.Expr.(*expression.All); ok && all.Flatten() {
				fk := all.FlattenKeys()
				for pos, _ := range fk.Operands() {
					secStrs = append(secStrs, s)
					desc = append(desc, fk.HasDesc(pos))
					if keyPos == 0 && pos == 0 {
						indexMissingLeadingKey = fk.HasMissing(pos)
					}
				}
			}
		} else {
			secStrs = append(secStrs, s)
			desc = append(desc, key.HasAttribute(datastore.IK_DESC))
			if keyPos == 0 {
				indexMissingLeadingKey = key.HasAttribute(datastore.IK_MISSING)
			}
		}
	}

	// with
	var withJSON []byte
	var err error
	if with != nil {
		if withJSON, err = with.MarshalJSON(); err != nil {
			return nil, errors.NewError(err, "GSI error marshalling WITH clause")
		}
	}

	// partition
	partitionScheme := c.PartitionScheme(c.SINGLE)
	var partitionKeys []string
	if indexPartition != nil {
		partitionScheme = partitionKey(indexPartition.Strategy)
		for _, expr := range indexPartition.Exprs {
			partitionKeys = append(partitionKeys, expression.NewStringer().Visit(expr))
		}
	}

	defnID, err := gsi.gsiClient.CreateIndex4(
		name,
		gsi.bucket,   /*bucket-name*/
		gsi.scope,    /*scope-name*/
		gsi.keyspace, /*collection-name*/
		"GSI",        /*using*/
		"N1QL",       /*exprType*/
		whereStr,
		secStrs,
		desc,
		indexMissingLeadingKey,
		false, /*isPrimary*/
		partitionScheme,
		partitionKeys,
		withJSON)
	if err != nil {
		return nil, errors.NewError(err, "GSI CreateIndex()")
	}
	// refresh to get back the newly created index.
	if err := gsi.Refresh(); err != nil {
		return nil, err
	}
	return gsi.IndexById(defnID2String(defnID))
}

func partitionKey(partitionType datastore.PartitionType) c.PartitionScheme {
	if partitionType == datastore.HASH_PARTITION {
		return c.PartitionScheme(c.KEY)
	}

	return c.PartitionScheme(c.SINGLE)
}

// BuildIndexes implements datastore.Indexer{} interface.
func (gsi *gsiKeyspace) BuildIndexes(requestId string, names ...string) (retErr errors.Error) {

	defer func() {
		if r := recover(); r != nil {
			retErr = ErrorInternal
			l.Errorf("BuildIndexes::Recovered from panic. Stacktrace %v", string(debug.Stack()))
		}
	}()

	defnIDs := make([]uint64, len(names))
	for i, name := range names {
		index, err := gsi.IndexByName(name)
		if err != nil {
			return errors.NewError(err, "BuildIndexes")
		}

		// Please note that the gsiKeyspace uses datastore.Index to cache
		// the index objects of type secondaryIndex*. But datastore.Index
		// interface doesn't provide CheckScheduled API. So, typecasting is
		// required.
		// Whenever a new type (liike secondaryIndex5) is introduced, this
		// typecasting code needs to be fixed.
		if idx, ok := index.(*secondaryIndex4); ok {
			if err := idx.CheckScheduled(); err != nil {
				return errors.NewError(fmt.Errorf("%v: %v", err.Error(), name), "BuildIndexes")
			}
		}

		defnIDs[i] = string2defnID(index.Id())
	}
	err := gsi.gsiClient.BuildIndexes(defnIDs)
	if err != nil {
		return errors.NewError(err, "BuildIndexes")
	}
	return nil
}

// Refresh list of indexes and scanner clients.
func (gsi *gsiKeyspace) Refresh() errors.Error {
	l.Tracef("%v gsiKeyspace.Refresh()", gsi.logPrefix)
	indexes, version, clusterVersion, forceRefresh, err := gsi.gsiClient.Refresh()
	if err != nil {
		return errors.NewError(err, "GSI Refresh()")
	}

	cachedVersion := atomic.LoadUint64(&gsi.version)
	cachedClusterVersion := atomic.LoadUint64(&gsi.clusterVersion)

	// has metadata version changed?
	if cachedVersion < version || cachedClusterVersion < clusterVersion || forceRefresh {
		gsi.gsiClient.UpdateDataEncodingFormat(clusterVersion)

		si_s := make([]*secondaryIndex, 0, len(indexes))
		for _, index := range indexes {

			if index.Definition.Bucket != gsi.bucket || index.Definition.Scope != gsi.scope ||
				index.Definition.Collection != gsi.keyspace {
				continue
			}
			si, err := newSecondaryIndexFromMetaData(gsi, clusterVersion, index)
			if err != nil {
				return err
			}
			si_s = append(si_s, si)
		}
		if err := gsi.setIndexes(si_s, version, clusterVersion); err != nil {
			return err
		}
	}

	return nil
}

func (gsi *gsiKeyspace) MetadataVersion() uint64 {

	return atomic.LoadUint64(&gsi.version)
}

func (gsi *gsiKeyspace) SetLogLevel(level qlog.Level) {
	switch level {
	case qlog.NONE:
		l.SetLogLevel(l.Silent)
	case qlog.SEVERE:
		l.SetLogLevel(l.Fatal)
	case qlog.ERROR:
		l.SetLogLevel(l.Error)
	case qlog.WARN:
		l.SetLogLevel(l.Warn)
	case qlog.INFO:
		l.SetLogLevel(l.Info)
	case qlog.REQUEST:
		l.SetLogLevel(l.Timing)
	case qlog.TRACE:
		l.SetLogLevel(l.Debug) //reversed
	case qlog.DEBUG:
		l.SetLogLevel(l.Trace)
	default:
		l.Warnf("Unknown query log level '%v'", level)
	}
}

// Synchronise gsi client with the servers and refresh the indexes list.
func (gsi *gsiKeyspace) SyncRefresh() errors.Error {
	err := gsi.gsiClient.Sync()
	if err != nil {
		return errors.NewError(err, "GSI SyncRefresh()")
	}
	return gsi.Refresh()
}

func (gsi *gsiKeyspace) GetGsiClientConfig() map[string]interface{} {
	conf := make(map[string]interface{})
	key := "indexer.settings.storage_mode"
	conf[key] = gsi.gsiClient.Settings().StorageMode()
	return conf
}

//------------------------------------------
// private functions for datastore.Indexer{}
//------------------------------------------

func (gsi *gsiKeyspace) setIndexes(si []*secondaryIndex,
	version, clusterVersion uint64) errors.Error {

	gsi.rw.Lock()
	defer gsi.rw.Unlock()
	atomic.StoreUint64(&gsi.version, version)
	atomic.StoreUint64(&gsi.clusterVersion, clusterVersion)
	gsi.indexes = make(map[uint64]datastore.Index)               // defnID -> index
	gsi.primaryIndexes = make(map[uint64]datastore.PrimaryIndex) // defnID -> index
	for _, si := range si {
		if si.isPrimary {
			gsi.primaryIndexes[si.defnID] = gsi.getPrimaryIndexFromVersion(si, clusterVersion)
		} else {
			gsi.indexes[si.defnID] = gsi.getIndexFromVersion(si, clusterVersion)
		}
	}
	return nil
}

// for getIndex() use IndexById()

func (gsi *gsiKeyspace) delIndex(id string) {
	gsi.rw.Lock()
	defer gsi.rw.Unlock()
	defnID := string2defnID(id)
	delete(gsi.indexes, defnID)
	delete(gsi.primaryIndexes, defnID)
}

func (gsi *gsiKeyspace) getIndexFromVersion(index *secondaryIndex,
	clusterVersion uint64) datastore.Index {

	if clusterVersion >= c.INDEXER_65_VERSION {
		si2 := &secondaryIndex2{secondaryIndex: *index}
		si3 := &secondaryIndex3{secondaryIndex2: *si2}
		si4 := datastore.Index(&secondaryIndex4{secondaryIndex3: *si3})
		return si4
	} else if clusterVersion >= c.INDEXER_55_VERSION {
		si2 := &secondaryIndex2{secondaryIndex: *index}
		si3 := datastore.Index(&secondaryIndex3{secondaryIndex2: *si2})
		return si3
	} else if clusterVersion >= c.INDEXER_50_VERSION {
		si2 := datastore.Index(&secondaryIndex2{secondaryIndex: *index})
		return si2
	}

	return datastore.Index(index)
}

func (gsi *gsiKeyspace) getPrimaryIndexFromVersion(index *secondaryIndex,
	clusterVersion uint64) datastore.PrimaryIndex {

	if clusterVersion >= c.INDEXER_65_VERSION {
		si2 := &secondaryIndex2{secondaryIndex: *index}
		si3 := &secondaryIndex3{secondaryIndex2: *si2}
		si4 := datastore.PrimaryIndex(&secondaryIndex4{secondaryIndex3: *si3})
		return si4
	} else if clusterVersion >= c.INDEXER_55_VERSION {
		si2 := &secondaryIndex2{secondaryIndex: *index}
		si3 := datastore.PrimaryIndex(&secondaryIndex3{secondaryIndex2: *si2})
		return si3
	} else if clusterVersion >= c.INDEXER_50_VERSION {
		si2 := datastore.PrimaryIndex(&secondaryIndex2{secondaryIndex: *index})
		return si2
	}

	return datastore.PrimaryIndex(index)
}

// Closing gsiKeyspace only means stopping of the stats logger and
// backfill monitor. This will enable garbage collection of the
// gsiKeyspace object.
//
// New scan request on closed gsiKeyspace can lead to scans reading
// stale value of backfillSize, which can lead to:
// (1) exceeding backfill limit (can lead to disk getting full)
//     OR
// (2) false scan failures (due to backfillLimit) which require backfill.
//
// So, to be safe, caller should NEVER call Close() when
// (1) There are any ongoing queries using this gsiKeyspace object.
//     OR
// (2) New incoming queries are expected to use this gsiKeyspace object.
func (gsi *gsiKeyspace) Close() {
	l.Infof("gsiKeyspace::Close Closing %v", getKeyForIndexer(gsi))
	if atomic.CompareAndSwapUint32(&gsi.closed, 0, 1) {
		UnmonitorIndexer(gsi)
	}
}

func CloseGsiKeyspace(gsi datastore.Indexer) {
	gsi.(*gsiKeyspace).Close()
}

//------------------
// datastore.Index{}
//------------------

// secondaryIndex to hold meta data information, network-address for
// a single secondary-index.
type secondaryIndex struct {
	gsi       *gsiKeyspace // back-reference to container.
	bucketn   string
	name      string // name of the index
	defnID    uint64
	isPrimary bool
	using     c.IndexType
	partnExpr expression.Expressions
	secExprs  expression.Expressions
	desc      []bool
	missing   []bool
	whereExpr expression.Expression
	state     datastore.IndexState
	err       string
	deferred  bool

	scheduled bool
	schedFail bool
}

// for metadata-provider.
func newSecondaryIndexFromMetaData(
	gsi *gsiKeyspace,
	version uint64,
	imd *mclient.IndexMetadata) (si *secondaryIndex, err errors.Error) {

	if !imd.Scheduled && !imd.ScheduleFailed {
		if len(imd.Instances) < 1 && len(imd.InstsInRebalance) < 1 {
			return nil, errors.NewError(nil, "no instance are created by GSI")
		}
	}

	indexDefn := imd.Definition
	defnID := uint64(indexDefn.DefnId)
	si = &secondaryIndex{
		gsi:       gsi,
		bucketn:   indexDefn.Bucket,
		name:      indexDefn.Name,
		defnID:    defnID,
		isPrimary: indexDefn.IsPrimary,
		using:     indexDefn.Using,
		desc:      indexDefn.Desc,
		missing:   nil, //TODO: Add here after adding missing to indexDefn
		state:     gsi2N1QLState[imd.State],
		err:       imd.Error,
		deferred:  indexDefn.Deferred,
		scheduled: imd.Scheduled,
		schedFail: imd.ScheduleFailed,
	}

	if indexDefn.SecExprs != nil {
		origSecExprs, origDesc, _ := common.GetUnexplodedExprs(indexDefn.SecExprs, indexDefn.Desc)
		exprs := make(expression.Expressions, 0, len(origSecExprs))
		for _, secExpr := range origSecExprs {
			expr, _ := parser.Parse(secExpr)
			exprs = append(exprs, expr)
		}
		si.secExprs = exprs
		si.desc = origDesc
	}

	if len(indexDefn.PartitionKeys) != 0 {
		exprs := make(expression.Expressions, 0, len(indexDefn.PartitionKeys))
		for _, partnExpr := range indexDefn.PartitionKeys {
			expr, _ := parser.Parse(partnExpr)
			exprs = append(exprs, expr)
		}
		si.partnExpr = exprs
	}

	if indexDefn.WhereExpr != "" {
		expr, _ := parser.Parse(indexDefn.WhereExpr)
		si.whereExpr = expr
	}

	if indexDefn.Deferred &&
		(imd.State == c.INDEX_STATE_CREATED ||
			imd.State == c.INDEX_STATE_READY) {
		si.state = datastore.DEFERRED
	}

	return si, nil
}

// KeyspaceId implement Index{} interface.
func (si *secondaryIndex) KeyspaceId() string {
	return si.gsi.KeyspaceId()
}

// BucketId implement Index{} interface.
func (si *secondaryIndex) BucketId() string {
	return si.gsi.bucket
}

// ScopeId implement Index{} interface.
func (si *secondaryIndex) ScopeId() string {
	return si.gsi.scope
}

// Indexer implement Index{} interface.
func (si *secondaryIndex) Indexer() datastore.Indexer {
	return si.gsi
}

// Id implement Index{} interface.
func (si *secondaryIndex) Id() string {
	return defnID2String(si.defnID)
}

// Name implement Index{} interface.
func (si *secondaryIndex) Name() string {
	return si.name
}

// Type implement Index{} interface.
func (si *secondaryIndex) Type() datastore.IndexType {
	return datastore.GSI
}

// SeekKey implement Index{} interface.
func (si *secondaryIndex) SeekKey() expression.Expressions {
	if si != nil && si.partnExpr != nil {
		return si.partnExpr
	}
	return nil
}

// RangeKey implement Index{} interface.
func (si *secondaryIndex) RangeKey() expression.Expressions {
	if si != nil && si.secExprs != nil {
		return si.secExprs
	}
	return nil
}

// Condition implement Index{} interface.
func (si *secondaryIndex) Condition() expression.Expression {
	if si != nil && si.whereExpr != nil {
		return si.whereExpr
	}
	return nil
}

// IsPrimary implements Index{} interface.
func (si *secondaryIndex) IsPrimary() bool {
	return si.isPrimary
}

// State implement Index{} interface.
func (si *secondaryIndex) State() (datastore.IndexState, string, errors.Error) {
	return si.state, si.err, nil
}

// Statistics implement Index{} interface.
func (si *secondaryIndex) Statistics(
	requestId string, span *datastore.Span) (datastore.Statistics, errors.Error) {

	if si == nil {
		return nil, ErrorIndexEmpty
	}

	client := si.gsi.gsiClient

	if err := si.CheckScheduled(); err != nil {
		return nil, n1qlError(client, err)
	}

	defnID := si.defnID
	if span.Seek != nil {
		seek := values2SKey(span.Seek)
		pstats, err := client.LookupStatistics(defnID, requestId, seek)
		if err != nil {
			return nil, n1qlError(client, err)
		}
		return newStatistics(pstats), nil
	}
	low := values2SKey(span.Range.Low)
	high := values2SKey(span.Range.High)
	incl := n1ql2GsiInclusion[span.Range.Inclusion]
	pstats, err := client.RangeStatistics(defnID, requestId, low, high, incl)
	if err != nil {
		return nil, n1qlError(client, err)
	}
	return newStatistics(pstats), nil
}

// Count implement Index{} interface.
func (si *secondaryIndex) Count(span *datastore.Span,
	cons datastore.ScanConsistency,
	vector timestamp.Vector) (int64, errors.Error) {

	if si == nil {
		return 0, ErrorIndexEmpty
	}

	client := si.gsi.gsiClient

	if err := si.CheckScheduled(); err != nil {
		return 0, n1qlError(client, err)
	}

	if span.Seek != nil {
		seek := values2SKey(span.Seek)
		count, e := client.CountLookup(si.defnID, "", []c.SecondaryKey{seek},
			n1ql2GsiConsistency[cons], vector2ts(vector))
		if e != nil {
			return 0, n1qlError(client, e)
		}
		return count, nil

	}
	low, high := values2SKey(span.Range.Low), values2SKey(span.Range.High)
	incl := n1ql2GsiInclusion[span.Range.Inclusion]
	count, e := client.CountRange(si.defnID, "", low, high, incl,
		n1ql2GsiConsistency[cons], vector2ts(vector))
	if e != nil {
		return 0, n1qlError(client, e)
	}
	return count, nil
}

// Drop implement Index{} interface.
func (si *secondaryIndex) Drop(requestId string) errors.Error {
	if si == nil {
		return ErrorIndexEmpty
	}

	if err := si.gsi.gsiClient.DropIndex(si.defnID); err != nil {
		return errors.NewError(err, "GSI Drop()")
	}
	si.gsi.delIndex(si.Id())
	return nil
}

func (si *secondaryIndex) cleanupBackfillFile(requestId string, broker *qclient.RequestBroker) {
	if broker != nil {
		for _, tmpfile := range broker.GetBackfills() {
			tmpfile.Close()
			name := tmpfile.Name()
			fmsg := "%v request(%v) removing temp file %v ...\n"
			l.Infof(fmsg, si.gsi.logPrefix, requestId, name)
			if err := os.Remove(name); err != nil {
				fmsg := "%v remove temp file %v unexpected failure: %v\n"
				l.Errorf(fmsg, si.gsi.logPrefix, name, err)
			}
			atomic.AddInt64(&si.gsi.totalbackfills, 1)
		}
	}
}

// Scan implement Index{} interface.
func (si *secondaryIndex) Scan(
	requestId string, span *datastore.Span, distinct bool, limit int64,
	cons datastore.ScanConsistency, vector timestamp.Vector,
	conn *datastore.IndexConnection) {

	sender := conn.Sender()
	var backfillSync int64

	var waitGroup sync.WaitGroup
	var broker *qclient.RequestBroker

	defer sender.Close()
	defer func() { // cleanup tmpfile
		waitGroup.Wait()
		si.cleanupBackfillFile(requestId, broker)
	}()
	defer func() {
		atomic.StoreInt64(&backfillSync, DONEREQUEST)
	}()

	starttm := time.Now()

	client, cnf := si.gsi.gsiClient, si.gsi.config

	if err := si.CheckScheduled(); err != nil {
		conn.Error(n1qlError(client, err))
		return
	}

	if span.Seek != nil {
		seek := values2SKey(span.Seek)
		broker = makeRequestBroker(requestId, si, client, conn, cnf, &waitGroup, &backfillSync, sender.Capacity())
		err := client.LookupInternal(
			si.defnID, requestId, []c.SecondaryKey{seek}, distinct, limit,
			n1ql2GsiConsistency[cons], vector2ts(vector), broker)
		if err != nil {
			conn.Error(n1qlError(client, err))
		}
	} else {
		low, high := values2SKey(span.Range.Low), values2SKey(span.Range.High)
		incl := n1ql2GsiInclusion[span.Range.Inclusion]
		broker = makeRequestBroker(requestId, si, client, conn, cnf, &waitGroup, &backfillSync, sender.Capacity())
		err := client.RangeInternal(
			si.defnID, requestId, low, high, incl, distinct, limit,
			n1ql2GsiConsistency[cons], vector2ts(vector), broker)
		if err != nil {
			conn.Error(n1qlError(client, err))
		}
	}
	atomic.AddInt64(&si.gsi.totalscans, 1)
	atomic.AddInt64(&si.gsi.scandur, int64(time.Since(starttm)))
}

// Scan implement PrimaryIndex{} interface.
func (si *secondaryIndex) ScanEntries(
	requestId string, limit int64, cons datastore.ScanConsistency,
	vector timestamp.Vector, conn *datastore.IndexConnection) {

	sender := conn.Sender()
	var backfillSync int64

	var waitGroup sync.WaitGroup
	var broker *qclient.RequestBroker

	defer sender.Close()
	defer func() {
		waitGroup.Wait()
		si.cleanupBackfillFile(requestId, broker)
	}()
	defer func() {
		atomic.StoreInt64(&backfillSync, DONEREQUEST)
	}()

	starttm := time.Now()

	client, cnf := si.gsi.gsiClient, si.gsi.config

	if err := si.CheckScheduled(); err != nil {
		conn.Error(n1qlError(client, err))
		return
	}

	broker = makeRequestBroker(requestId, si, client, conn, cnf, &waitGroup, &backfillSync, sender.Capacity())
	err := client.ScanAllInternal(
		si.defnID, requestId, limit,
		n1ql2GsiConsistency[cons], vector2ts(vector), broker)
	if err != nil {
		conn.Error(n1qlError(client, err))
	}

	atomic.AddInt64(&si.gsi.totalscans, 1)
	atomic.AddInt64(&si.gsi.scandur, int64(time.Since(starttm)))
}

func (si *secondaryIndex) CheckScheduled() error {
	if si.schedFail {
		return ErrorScheduledCreateFailed
	}

	if si.scheduled {
		return ErrorScheduledCreate
	}

	return nil
}

//--------------------
// datastore.Index2{}
//--------------------

type secondaryIndex2 struct {
	secondaryIndex
}

// Scan2 implement Index2 interface.
func (si *secondaryIndex2) Scan2(
	requestId string, spans datastore.Spans2, reverse, distinct, ordered bool,
	projection *datastore.IndexProjection, offset, limit int64,
	cons datastore.ScanConsistency, vector timestamp.Vector,
	conn *datastore.IndexConnection) {

	sender := conn.Sender()
	var backfillSync int64
	var waitGroup sync.WaitGroup
	var broker *qclient.RequestBroker

	defer sender.Close()
	defer func() {
		if broker != nil {
			l.Debugf("scan2: scan request %v closing sender.  Receive Count %v Sent Count %v",
				requestId, broker.ReceiveCount(), broker.SendCount())
		}
	}()
	defer func() { // cleanup tmpfile
		waitGroup.Wait()
		si.cleanupBackfillFile(requestId, broker)
	}()
	defer func() {
		atomic.StoreInt64(&backfillSync, DONEREQUEST)
	}()

	starttm := time.Now()

	client, cnf := si.gsi.gsiClient, si.gsi.config

	if err := si.CheckScheduled(); err != nil {
		conn.Error(n1qlError(client, err))
		return
	}

	gsiscans := n1qlspanstogsi(spans)
	gsiprojection := n1qlprojectiontogsi(projection)
	broker = makeRequestBroker(requestId, &si.secondaryIndex, client, conn, cnf, &waitGroup, &backfillSync, sender.Capacity())
	err := client.MultiScanInternal(
		si.defnID, requestId, gsiscans, reverse, distinct,
		gsiprojection, offset, limit,
		n1ql2GsiConsistency[cons], vector2ts(vector),
		broker)
	if err != nil {
		conn.Error(n1qlError(client, err))
	}

	atomic.AddInt64(&si.gsi.totalscans, 1)
	atomic.AddInt64(&si.gsi.scandur, int64(time.Since(starttm)))

	l.Debugf("scan2: scan request %v done.  Receive Count %v Sent Count %v NumIndexers %v err %v",
		requestId, broker.ReceiveCount(), broker.SendCount(), broker.NumIndexers(), err)
}

// RangeKey2 implements Index2{} interface.
func (si *secondaryIndex2) RangeKey2() datastore.IndexKeys {

	if si != nil && si.secExprs != nil {
		idxkeys := make(datastore.IndexKeys, 0, len(si.secExprs))
		for i, exprS := range si.secExprs {
			idxkey := &datastore.IndexKey{
				Expr: exprS,
			}

			attr := datastore.IK_NONE
			if si.desc != nil && si.desc[i] {
				attr |= datastore.IK_DESC
			}
			if si.missing != nil && si.missing[i] {
				attr |= datastore.IK_MISSING
			}
			idxkey.SetAttribute(attr, true)

			idxkeys = append(idxkeys, idxkey)
		}
		return idxkeys
	}
	return nil
}

//--------------------
// datastore.CountIndex2{}
//--------------------

// Count2 implements CountIndex2{} interface.
func (si *secondaryIndex2) Count2(requestId string, spans datastore.Spans2,
	cons datastore.ScanConsistency, vector timestamp.Vector) (int64, errors.Error) {

	if si == nil {
		return 0, ErrorIndexEmpty
	}
	client := si.gsi.gsiClient

	if err := si.CheckScheduled(); err != nil {
		return 0, n1qlError(client, err)
	}

	gsiscans := n1qlspanstogsi(spans)

	count, e := client.MultiScanCount(si.defnID, requestId, gsiscans, false,
		n1ql2GsiConsistency[cons], vector2ts(vector))
	if e != nil {
		return 0, n1qlError(client, e)
	}
	return count, nil
}

// CanCountDistinct implements CountIndex2{} interface.
func (si *secondaryIndex2) CanCountDistinct() bool {
	return true
}

// CountDistinct implements CountIndex2{} interface.
func (si *secondaryIndex2) CountDistinct(requestId string, spans datastore.Spans2,
	cons datastore.ScanConsistency, vector timestamp.Vector) (int64, errors.Error) {

	if si == nil {
		return 0, ErrorIndexEmpty
	}
	client := si.gsi.gsiClient

	if err := si.CheckScheduled(); err != nil {
		return 0, n1qlError(client, err)
	}

	gsiscans := n1qlspanstogsi(spans)

	count, e := client.MultiScanCount(si.defnID, requestId, gsiscans, true,
		n1ql2GsiConsistency[cons], vector2ts(vector))
	if e != nil {
		return 0, n1qlError(client, e)
	}
	return count, nil
}

//-------------------------------------
// datastore API3 implementation
//-------------------------------------

type secondaryIndex3 struct {
	secondaryIndex2
}

// CreateAggregate implement Index3 interface.
func (si *secondaryIndex3) CreateAggregate(requestId string, groupAggs *datastore.IndexGroupAggregates,
	with value.Value) errors.Error {
	return errors.NewError(fmt.Errorf("Create Aggregate not supported"), "")
}

func (si *secondaryIndex3) DropAggregate(requestId, name string) errors.Error {
	return errors.NewError(fmt.Errorf("Drops Aggregate not supported"), "")
}

func (si *secondaryIndex3) Aggregates() ([]datastore.IndexGroupAggregates, errors.Error) {
	return nil, errors.NewError(fmt.Errorf("Precomputed Aggregates not supported"), "")
}

func (si *secondaryIndex3) PartitionKeys() (*datastore.IndexPartition, errors.Error) {

	if len(si.partnExpr) == 0 {
		return nil, nil
	}

	keys := make([]expression.Expression, len(si.partnExpr))
	for i, key := range si.partnExpr {
		keys[i] = key
	}

	keyPartition := &datastore.IndexPartition{
		Strategy: datastore.HASH_PARTITION,
		Exprs:    expression.Expressions(keys),
	}

	return keyPartition, nil
}

// Scan3 implement Index3 interface.
func (si *secondaryIndex3) Scan3(
	requestId string, spans datastore.Spans2, reverse, distinctAfterProjection bool,
	projection *datastore.IndexProjection, offset, limit int64,
	groupAggs *datastore.IndexGroupAggregates, indexOrders datastore.IndexKeyOrders,
	cons datastore.ScanConsistency, vector timestamp.Vector,
	conn *datastore.IndexConnection) {

	sender := conn.Sender()
	var backfillSync int64
	var waitGroup sync.WaitGroup
	var broker *qclient.RequestBroker

	defer sender.Close()
	defer func() {
		if broker != nil {
			l.Debugf("scan3: scan request %v closing sender.  Receive Count %v Sent Count %v",
				requestId, broker.ReceiveCount(), broker.SendCount())
		}
	}()
	defer func() { // cleanup tmpfile
		waitGroup.Wait()
		si.cleanupBackfillFile(requestId, broker)
	}()
	defer func() {
		atomic.StoreInt64(&backfillSync, DONEREQUEST)
	}()

	starttm := time.Now()

	client, cnf := si.gsi.gsiClient, si.gsi.config

	if err := si.CheckScheduled(); err != nil {
		conn.Error(n1qlError(client, err))
		return
	}

	gsiscans := n1qlspanstogsi(spans)
	gsiprojection := n1qlprojectiontogsi(projection)
	gsigroupaggr := n1qlgroupaggrtogsi(groupAggs)
	indexorder := n1qlindexordertogsi(indexOrders)
	broker = makeRequestBroker(requestId, &si.secondaryIndex, client, conn, cnf, &waitGroup, &backfillSync, sender.Capacity())
	err := client.Scan3Internal(
		si.defnID, requestId, gsiscans, reverse, distinctAfterProjection,
		gsiprojection, offset, limit, gsigroupaggr, indexorder,
		n1ql2GsiConsistency[cons], vector2ts(vector),
		broker)
	if err != nil {
		conn.Error(n1qlError(client, err))
	}

	atomic.AddInt64(&si.gsi.totalscans, 1)
	atomic.AddInt64(&si.gsi.scandur, int64(time.Since(starttm)))

	l.Debugf("scan3: scan request %v done.  Receive Count %v Sent Count %v NumIndexers %v err %v",
		requestId, broker.ReceiveCount(), broker.SendCount(), broker.NumIndexers(), err)
}

// Alter implements datastore.Index3 interface.
func (si *secondaryIndex3) Alter(requestId string, with value.Value) (
	datastore.Index, errors.Error) {

	if with == nil {
		return datastore.Index(si), nil
	}

	if err := si.CheckScheduled(); err != nil {
		client := si.gsi.gsiClient
		return nil, n1qlError(client, err)
	}

	var ErrorMarshalWith = "GSI AlterIndex() Error marshalling WITH clause"
	var ErrorUmmarshalWith = "GSI AlterIndex() Error unmarshalling WITH clause"
	var ErrorActionMissing = "GSI AlterIndex() action key missing in WITH clause"
	var ErrorUnsupportedAction = "GSI AlterIndex() Unsupported action value"

	var withMap map[string]interface{}
	var withJSON []byte
	var err error
	if withJSON, err = with.MarshalJSON(); err != nil {
		return nil, errors.NewError(err, ErrorMarshalWith)
	}
	if err = json.Unmarshal(withJSON, &withMap); err != nil {
		return nil, errors.NewError(err, ErrorUmmarshalWith)
	}

	action, ok := withMap["action"]
	if !ok {
		return nil, errors.NewError(fmt.Errorf(ErrorActionMissing), "")
	}

	action, ok = action.(string)
	if !ok {
		return nil, errors.NewError(fmt.Errorf(ErrorUnsupportedAction), "")
	}
	switch action {
	case "move":
		client := si.gsi.gsiClient
		e := client.MoveIndex(si.defnID, withMap)
		if e != nil {
			return nil, errors.NewError(e, "GSI AlterIndex()")
		}
		return datastore.Index(si), nil
	case "replica_count", "drop_replica":
		client := si.gsi.gsiClient
		e := client.AlterReplicaCount(action.(string), si.defnID, withMap)
		if e != nil {
			return nil, errors.NewError(e, "GSI AlterIndex()")
		}
		return datastore.Index(si), nil
	default:
		return nil, errors.NewError(fmt.Errorf(ErrorUnsupportedAction), "")
	}

	return datastore.Index(si), nil
}

// ScanEntries3 implements datastore.PrimaryIndex3 interface.
func (si *secondaryIndex3) ScanEntries3(
	requestId string, projection *datastore.IndexProjection, offset, limit int64,
	groupAggs *datastore.IndexGroupAggregates, indexOrders datastore.IndexKeyOrders,
	cons datastore.ScanConsistency, vector timestamp.Vector,
	conn *datastore.IndexConnection) {

	if groupAggs != nil {
		conn.Error(errors.NewError(nil, fmt.Sprintf("IndexGroupAggregates not supported in ScanEntries3")))
	}

	spans := datastore.Spans2{&datastore.Span2{}} // Span for full table scan
	si.Scan3(requestId, spans, false, false, nil, offset, limit, nil, indexOrders,
		cons, vector, conn)
}

//-------------------------------------
// datastore API3 implementation end
//-------------------------------------

//-------------------------------------
// datastore API4 implementation
//-------------------------------------

type secondaryIndex4 struct {
	secondaryIndex3
}

func (si *secondaryIndex4) StorageMode() (datastore.IndexStorageMode, errors.Error) {
	storage := c.IndexTypeToStorageMode(si.using)
	switch storage {
	case c.MOI:
		return datastore.INDEX_MODE_MOI, nil
	case c.PLASMA:
		return datastore.INDEX_MODE_PLASMA, nil
	case c.FORESTDB:
		return datastore.INDEX_MODE_FDB, nil
	}
	return "", errors.NewError(nil, "Index4 StorageMode(): Unknown storage mode")
}

func (si *secondaryIndex4) LeadKeyHistogram(requestId string) (*datastore.Histogram, errors.Error) {
	return nil, errors.NewNotImplemented("Index4 LeadKeyHistogram")
}

func (si *secondaryIndex4) StorageStatistics(requestid string) ([]map[datastore.IndexStatType]value.Value,
	errors.Error) {

	if si == nil {
		return nil, ErrorIndexEmpty
	}
	client := si.gsi.gsiClient

	if err := si.CheckScheduled(); err != nil {
		return nil, n1qlError(client, err)
	}

	stats, e := client.StorageStatistics(si.defnID, requestid)
	if e != nil {
		return nil, n1qlError(client, e)
	}

	return gsistatston1ql(stats), nil
}

//-------------------------------------
// datastore API4 implementation end
//-------------------------------------

//-------------------------------------
// private functions for secondaryIndex
//-------------------------------------

func makeRequestBroker(
	requestId string,
	si *secondaryIndex,
	client *qclient.GsiClient,
	conn *datastore.IndexConnection,
	config c.Config,
	waitGroup *sync.WaitGroup,
	backfillSync *int64,
	size int) *qclient.RequestBroker {

	broker := qclient.NewRequestBroker(requestId, int64(size), conn.MaxParallelism())
	dataEncFmt := client.GetDataEncodingFormat()

	broker.SetDataEncodingFormat(dataEncFmt)

	factory := func(id qclient.ResponseHandlerId, instId uint64, partitions []c.PartitionId) qclient.ResponseHandler {
		return makeResponsehandler(id, requestId, si, client, conn, broker, config, waitGroup, backfillSync, instId, partitions)
	}

	sender := func(pkey []byte, value []value.Value, skey c.ScanResultKey, tmpbuf *[]byte) (bool, *[]byte) {
		return sendEntry(broker, si, pkey, value, skey, conn, tmpbuf)
	}

	backfillWaiter := func() {
		if waitGroup != nil {
			waitGroup.Wait()
		}
	}

	broker.SetResponseHandlerFactory(factory)
	broker.SetResponseSender(sender)
	broker.SetBackfillWaiter(backfillWaiter)

	return broker
}

func makeResponsehandler(
	id qclient.ResponseHandlerId,
	requestId string,
	si *secondaryIndex,
	client *qclient.GsiClient,
	conn *datastore.IndexConnection,
	broker *qclient.RequestBroker,
	config c.Config,
	waitGroup *sync.WaitGroup,
	backfillSync *int64,
	instId uint64,
	partitions []c.PartitionId) qclient.ResponseHandler {

	sender := conn.Sender()

	var enc *gob.Encoder
	var dec *gob.Decoder
	var readfd *os.File
	var backfillFin, backfillEntries int64

	var tmpfile *os.File

	backfillLimit := si.gsi.getTmpSpaceLimit()
	primed, starttm, ticktm := false, time.Now(), time.Now()
	lprefix := si.gsi.logPrefix

	//
	// This function returns false if wants to
	// stop execution.   This could be due to
	// 1) internal error.  In this case, this
	//    function will post the error directly to cbq-engine.
	//    The error will not be posted to GsiScanClient,
	//    so GsiClient would not return error or retry.
	//    For internal error, this function should call
	//    broker.Error() so that scatter/gather could stop.
	//
	// 2) cbq-engine asks to stop.  In this case,
	//    SendEntries() will return false.  SendEntries()
	//    is responsible for stopping scatter/gather.
	//
	// 3) scan is done (e.g. limit has reached).
	//    In this case, SendEntries() will return false.
	//    Gathering routine is responsible for stopping
	//    scatter/gather.
	//
	backfill := func() {
		name := tmpfile.Name()
		defer func() {
			if readfd != nil {
				readfd.Close()
			}
			waitGroup.Done()
			atomic.AddInt64(&backfillFin, 1)
			l.Debugf(
				"%v %q finished reading from temp file for %v ...\n",
				lprefix, requestId, name)

			if r := recover(); r != nil {
				l.Errorf("%v %q Error %v during temp file read", lprefix, requestId, r)
			}
		}()
		l.Debugf(
			"%v %q started temp file read for %v ...\n", lprefix, requestId, name)
		for {
			var skeys c.ScanResultEntries

			if broker.IsClose() {
				return
			}

			if pending := atomic.LoadInt64(&backfillEntries); pending > 0 {
				atomic.AddInt64(&backfillEntries, -1)
			} else if done := atomic.LoadInt64(backfillSync); done == DONEREQUEST {
				return
			} else {
				time.Sleep(1 * time.Millisecond)
				continue
			}
			cummsize := atomic.LoadInt64(&si.gsi.backfillSize) / (1024 * 1024)
			if cummsize > backfillLimit {
				fmsg := "%v temp file size exceeded limit %v, %v"
				err := fmt.Errorf(fmsg, requestId, backfillLimit, cummsize)
				conn.Error(n1qlError(client, err))
				broker.Error(err, instId, partitions)
				return
			}

			if err := dec.Decode(&skeys); err != nil {
				fmsg := "%v %q decoding from temp file %v: %v\n"
				l.Errorf(fmsg, lprefix, requestId, name, err)
				conn.Error(n1qlError(client, err))
				broker.Error(err, instId, partitions)
				return
			}
			pkeys := make([][]byte, 0)
			if err := dec.Decode(&pkeys); err != nil {
				fmsg := "%v %q decoding from temp file %v: %v\n"
				l.Errorf(fmsg, lprefix, requestId, name, err)
				conn.Error(n1qlError(client, err))
				broker.Error(err, instId, partitions)
				return
			}
			l.Tracef("%v temp file read %v entries\n", lprefix, skeys.GetLength())
			if primed == false {
				atomic.AddInt64(&si.gsi.primedur, int64(time.Since(starttm)))
				primed = true
			}

			ln := int(broker.Len(id))
			if ln < 0 {
				ln = sender.Length()
			}

			if ln > 0 && skeys.GetLength() > 0 {
				atomic.AddInt64(&si.gsi.throttledur, int64(time.Since(ticktm)))
			}
			cont, err := broker.SendEntries(id, pkeys, &skeys)
			if err != nil {
				fmsg := "%v %q error %v in SendEntries\n"
				l.Errorf(fmsg, lprefix, requestId, err)
				conn.Error(n1qlError(client, err))
				broker.Error(err, instId, partitions)
				return
			}
			if !cont {
				return
			}
			ticktm = time.Now()
		}
	}

	//
	// This function returns false if wants to
	// stop execution.   This could be due to
	// 1) internal error.  In this case, this
	//    function will post the error directly to cbq-engine.
	//    The error will not be posted to GsiScanClient,
	//    so GsiClient would not return error or retry.
	//    For internal error, this function should call
	//    broker.Error() so that scatter/gather could stop.
	//
	// 2) cbq-engine asks to stop.  In this case,
	//    SendEntries() will return false.  SendEntries()
	//    is responsible for stopping scatter/gather.
	//
	// 3) scan is done (e.g. limit has reached).
	//    In this case, SendEntries() will return false.
	//    Gathering routine is responsible for stopping
	//    scatter/gather.
	//
	return func(data qclient.ResponseReader) bool {
		err := data.Error()
		if err != nil {
			conn.Error(n1qlError(client, err))
			broker.Error(err, instId, partitions)
			return false
		}

		dataEncFmt := broker.GetDataEncodingFormat()
		skeys, pkeys, err := data.GetEntries(dataEncFmt)
		if err != nil {
			conn.Error(n1qlError(client, err))
			broker.Error(err, instId, partitions)
			return false
		}
		if len(pkeys) != 0 || skeys.GetLength() != 0 {
			if len(pkeys) != 0 {
				broker.IncrementReceiveCount(len(pkeys))
			} else {
				broker.IncrementReceiveCount(skeys.GetLength())
			}
		}

		ln := int(broker.Len(id))
		if ln < 0 {
			ln = sender.Length()
		}

		cp := int(broker.Cap(id))
		if cp < 0 {
			cp = sender.Capacity()
		}

		if backfillLimit > 0 && tmpfile == nil && ((cp - ln) < skeys.GetLength()) {
			prefix := BACKFILLPREFIX + strconv.Itoa(os.Getpid())
			tmpfile, err = ioutil.TempFile(si.gsi.getTmpSpaceDir(), prefix)
			name := ""
			if tmpfile != nil {
				name = tmpfile.Name()
			}
			if err != nil {
				fmsg := "%v %q creating temp file %v : %v\n"
				l.Errorf(fmsg, lprefix, requestId, name, err)
				tmpfile = nil
				conn.Error(n1qlError(client, err))
				broker.Error(err, instId, partitions)
				return false

			} else {
				fmsg := "%v %v new temp file ... %v\n"
				l.Infof(fmsg, lprefix, requestId, name)
				broker.AddBackfill(tmpfile)
				// encoder
				enc = gob.NewEncoder(tmpfile)
				readfd, err = os.OpenFile(name, os.O_RDONLY, 0666)
				if err != nil {
					fmsg := "%v %v reading temp file %v: %v\n"
					l.Errorf(fmsg, lprefix, requestId, name, err)
					conn.Error(n1qlError(client, err))
					broker.Error(err, instId, partitions)
					return false
				}
				// decoder
				dec = gob.NewDecoder(readfd)
				waitGroup.Add(1)
				go backfill()
			}
		}

		if tmpfile != nil {
			// whether temp-file is exhausted the limit.
			cummsize := atomic.LoadInt64(&si.gsi.backfillSize) / (1024 * 1024)
			if cummsize > backfillLimit {
				fmsg := "%q temp file size exceeded limit %v, %v"
				err := fmt.Errorf(fmsg, requestId, backfillLimit, cummsize)
				conn.Error(n1qlError(client, err))
				broker.Error(err, instId, partitions)
				return false
			}

			l.Tracef("%v temp file write %v entries\n", lprefix, skeys.GetLength())
			if atomic.LoadInt64(&backfillFin) > 0 {
				return false
			}
			if err := enc.Encode(skeys); err != nil {
				conn.Error(n1qlError(client, err))
				broker.Error(err, instId, partitions)
				return false
			}
			if err := enc.Encode(pkeys); err != nil {
				conn.Error(n1qlError(client, err))
				broker.Error(err, instId, partitions)
				return false
			}
			atomic.AddInt64(&backfillEntries, 1)

		} else {
			l.Tracef("%v response cap:%v len:%v entries:%v\n", lprefix, cp, ln, skeys.GetLength())
			if primed == false {
				atomic.AddInt64(&si.gsi.primedur, int64(time.Since(starttm)))
				primed = true
			}
			if int(ln) > 0 && skeys.GetLength() > 0 {
				atomic.AddInt64(&si.gsi.throttledur, int64(time.Since(ticktm)))
			}
			cont, err := broker.SendEntries(id, pkeys, skeys)
			if err != nil {
				fmsg := "%v %q error %v in SendEntries\n"
				l.Errorf(fmsg, lprefix, requestId, err)
				conn.Error(n1qlError(client, err))
				broker.Error(err, instId, partitions)
				return false
			}
			if !cont {
				return false
			}
			ticktm = time.Now()
		}
		return true
	}
}

func isStaleMetaError(err error) bool {
	switch err.Error() {
	case qclient.ErrIndexNotFound.Error():
		fallthrough
	case qclient.ErrIndexNotReady.Error():
		return true
	}

	return false
}

func n1qlError(client *qclient.GsiClient, err error) errors.Error {
	switch strings.TrimSpace(err.Error()) {
	case c.ErrScanTimedOut.Error():
		return errors.NewCbIndexScanTimeoutError(err)
	case qclient.ErrorIndexNotFound.Error():
		return errors.NewCbIndexNotFoundError(err)
	case qclient.ErrIndexNotFound.Error():
		return errors.NewCbIndexNotFoundError(err)
	}

	return errors.NewError(err, client.DescribeError(err))
}

//-----------------------
// datastore.Statistics{}
//-----------------------

type statistics struct {
	count      int64
	uniqueKeys int64
	min        value.Values
	max        value.Values
}

// return an
// adaptor from gsi index statistics structure to datastore.Statistics{}
func newStatistics(pstats c.IndexStatistics) datastore.Statistics {
	stats := &statistics{}
	stats.count, _ = pstats.Count()
	stats.uniqueKeys, _ = pstats.DistinctCount()
	min, _ := pstats.MinKey()
	stats.min = skey2Values(min)
	max, _ := pstats.MaxKey()
	stats.max = skey2Values(max)
	return stats
}

// Count implement Statistics{} interface.
func (stats *statistics) Count() (int64, errors.Error) {
	return stats.count, nil
}

// DistinctCount implement Statistics{} interface.
func (stats *statistics) DistinctCount() (int64, errors.Error) {
	return stats.uniqueKeys, nil
}

// Min implement Statistics{} interface.
func (stats *statistics) Min() (value.Values, errors.Error) {
	return stats.min, nil
}

// Max implement Statistics{} interface.
func (stats *statistics) Max() (value.Values, errors.Error) {
	return stats.max, nil
}

// Bins implement Statistics{} interface.
func (stats *statistics) Bins() ([]datastore.Statistics, errors.Error) {
	return nil, nil
}

//------------------
// private functions
//------------------

// shape of key passed to scan-coordinator (indexer node) is,
//      [key1, key2, ... keyN]
// where N expressions supplied in CREATE INDEX
// to evaluate secondary-key.
func values2SKey(vals value.Values) c.SecondaryKey {
	skey := make(c.SecondaryKey, 0, len(vals))
	for _, val := range []value.Value(vals) {
		skey = append(skey, val.ActualForIndex())
	}
	return skey
}

// shape of return key from scan-coordinator is,
//      [key1, key2, ... keyN]
// where N keys where evaluated using N expressions supplied in
// CREATE INDEX.
func skey2Values(skey c.SecondaryKey) []value.Value {
	vals := make([]value.Value, len(skey))
	for i := 0; i < len(skey); i++ {
		if s, ok := skey[i].(string); ok && collatejson.MissingLiteral.Equal(s) {
			vals[i] = value.NewMissingValue()
		} else {
			vals[i] = value.NewValue(skey[i])
		}
	}
	return vals
}

// get cluster info and refresh ns-server data.
func getClusterInfo(
	cluster string, pooln string) (*c.ClusterInfoCache, errors.Error) {

	clusterURL, err := c.ClusterAuthUrl(cluster)
	if err != nil {
		return nil, errors.NewError(err, fmt.Sprintf("ClusterAuthUrl() failed"))
	}
	cinfo, err := c.NewClusterInfoCache(clusterURL, pooln)
	if err != nil {
		return nil, errors.NewError(err, fmt.Sprintf("ClusterInfo() failed"))
	}
	cinfo.SetUserAgent("n1ql::getClusterInfo")
	if err := cinfo.Fetch(); err != nil {
		msg := fmt.Sprintf("Fetch ClusterInfo() failed")
		return nil, errors.NewError(err, msg)
	}
	return cinfo, nil
}

func defnID2String(id uint64) string {
	return strconv.FormatUint(id, 16)
}

func string2defnID(id string) uint64 {
	defnID, _ := strconv.ParseUint(id, 16, 64)
	return defnID
}

func vector2ts(vector timestamp.Vector) *qclient.TsConsistency {
	if vector == nil || len(vector.Entries()) == 0 {
		return nil
	}
	vbnos := make([]uint16, 0, 1024)
	seqnos := make([]uint64, 0, 1024)
	vbuuids := make([]uint64, 0, 1024)
	for _, entry := range vector.Entries() {
		vbnos = append(vbnos, uint16(entry.Position()))
		seqnos = append(seqnos, uint64(entry.Value()))
		vbuuids = append(vbuuids, uint64(guard2Vbuuid(entry.Guard())))
	}
	return qclient.NewTsConsistency(vbnos, seqnos, vbuuids)
}

func guard2Vbuuid(guard string) uint64 {
	vbuuid, _ := strconv.ParseUint(guard, 10, 64)
	return vbuuid
}

//-----------------
// singleton client
//-----------------

var muclient sync.Mutex
var singletonClient *qclient.GsiClient

func getSingletonClient(clusterURL string, conf c.Config,
	securityconf *datastore.ConnectionSecurityConfig) (*qclient.GsiClient, error) {

	muclient.Lock()
	defer muclient.Unlock()
	if singletonClient == nil {

		// Cleanup backfill files, if any.
		// Here, it is safe to cleanup backfill files unconditionally as
		// singleton GsiClient is used for requests and absence of this
		// singleton GsiClient means this is the first request.
		cleanupAllTmpFiles()

		if strings.Contains(strings.ToLower(clusterURL), "http") ||
			strings.Contains(strings.ToLower(clusterURL), "https") {
			parsedUrl, err := url.Parse(clusterURL)
			if err != nil {
				return nil, err
			}
			clusterURL = parsedUrl.Host
		}

		l.Infof("creating GsiClient for %v", clusterURL)

		if securityconf != nil {
			security.Refresh(securityconf.TLSConfig, securityconf.ClusterEncryptionConfig,
				securityconf.CertFile, securityconf.KeyFile, securityconf.CAFile)
		}

		qconf := conf.SectionConfig("queryport.client.", true /*trim*/)
		encryptLocalHost := conf["security.encryption.encryptLocalhost"].Bool()
		client, err := qclient.NewGsiClientWithSettings(clusterURL, qconf, true, encryptLocalHost)
		if err != nil {
			return nil, fmt.Errorf("in NewGsiClient(): %v", err)
		}
		singletonClient = client
	}
	return singletonClient, nil
}

func init() {
	// register gob objects for complex composite keys.
	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})
}

func sendEntry(broker *qclient.RequestBroker, si *secondaryIndex, pkey []byte,
	value []value.Value, skey c.ScanResultKey,
	conn *datastore.IndexConnection, tmpbuf *[]byte) (bool, *[]byte) {

	var start time.Time
	blockedtm, blocked := int64(0), false

	sender := conn.Sender()

	var err error
	var retBuf *[]byte
	if value == nil {
		value, err, retBuf = skey.Get(tmpbuf)
		if err != nil {
			msg := fmt.Sprintf("Error %v in sendEntry", err)
			conn.Error(errors.NewError(err, msg))
			return false, nil
		}
	}

	// Primary-key is mandatory.
	e := &datastore.IndexEntry{
		PrimaryKey: string(pkey),
		EntryKey:   value}
	cp, ln := sender.Capacity(), sender.Length()
	if ln == cp {
		start, blocked = time.Now(), true
	}

	cont := sender.SendEntry(e)
	if !cont {
		return false, nil
	}

	if blocked {
		blockedtm += int64(time.Since(start))
		blocked = false
	}

	atomic.AddInt64(&si.gsi.blockeddur, blockedtm)
	broker.IncrementSendCount()
	return true, retBuf
}

func n1qlspanstogsi(spans datastore.Spans2) qclient.Scans {
	sc := make(qclient.Scans, len(spans))
	for i, s := range spans {
		sc[i] = &qclient.Scan{}
		if len(s.Seek) > 0 {
			sc[i].Seek = values2SKey(s.Seek)
		} else {
			sc[i].Filter = n1qlrangestogsi(s.Ranges)
		}
	}
	return sc
}

func n1qlrangestogsi(ranges []*datastore.Range2) []*qclient.CompositeElementFilter {
	fl := make([]*qclient.CompositeElementFilter, 0, len(ranges))
	for _, r := range ranges {
		var l, h interface{}
		if r.Low == nil {
			l = c.MinUnbounded
		} else {
			l = r.Low.ActualForIndex()
		}

		if r.High == nil {
			h = c.MaxUnbounded
		} else {
			h = r.High.ActualForIndex()
		}

		incl := n1ql2GsiInclusion[r.Inclusion]
		filter := &qclient.CompositeElementFilter{Low: l, High: h, Inclusion: incl}
		fl = append(fl, filter)
	}
	return fl
}

func n1qlprojectiontogsi(projection *datastore.IndexProjection) *qclient.IndexProjection {
	if projection == nil {
		return nil
	}

	entrykeys := make([]int64, 0, len(projection.EntryKeys))
	for _, key := range projection.EntryKeys {
		entrykeys = append(entrykeys, int64(key))
	}
	proj := &qclient.IndexProjection{
		EntryKeys:  entrykeys,
		PrimaryKey: projection.PrimaryKey,
	}
	return proj
}

func n1qlgroupaggrtogsi(groupAggs *datastore.IndexGroupAggregates) *qclient.GroupAggr {
	if groupAggs == nil {
		return nil
	}

	//Group
	var groups []*qclient.GroupKey
	if groupAggs.Group != nil {
		groups = make([]*qclient.GroupKey, len(groupAggs.Group))
		for i, grp := range groupAggs.Group {
			g := &qclient.GroupKey{
				EntryKeyId: int32(grp.EntryKeyId),
				KeyPos:     int32(grp.KeyPos),
			}
			if grp.Expr != nil {
				g.Expr = expression.NewStringer().Visit(grp.Expr)
			}
			groups[i] = g
		}
	}

	//Aggrs
	var aggregates []*qclient.Aggregate
	if groupAggs.Aggregates != nil {
		aggregates = make([]*qclient.Aggregate, len(groupAggs.Aggregates))
		for i, aggr := range groupAggs.Aggregates {
			a := &qclient.Aggregate{
				AggrFunc:   n1qlaggrtypetogsi(aggr.Operation),
				EntryKeyId: int32(aggr.EntryKeyId),
				KeyPos:     int32(aggr.KeyPos),
				Distinct:   aggr.Distinct,
			}
			if aggr.Expr != nil {
				a.Expr = expression.NewStringer().Visit(aggr.Expr)
			}
			aggregates[i] = a
		}
	}

	var dependsOnIndexKeys []int32
	if groupAggs.DependsOnIndexKeys != nil {
		dependsOnIndexKeys = make([]int32, len(groupAggs.DependsOnIndexKeys))
		for i, ikey := range groupAggs.DependsOnIndexKeys {
			dependsOnIndexKeys[i] = int32(ikey)
		}
	}

	ga := &qclient.GroupAggr{
		Name:               groupAggs.Name,
		Group:              groups,
		Aggrs:              aggregates,
		DependsOnIndexKeys: dependsOnIndexKeys,
		IndexKeyNames:      groupAggs.IndexKeyNames,
		AllowPartialAggr:   groupAggs.AllowPartialAggr,
		OnePerPrimaryKey:   groupAggs.OneForPrimaryKey,
	}

	return ga
}

func n1qlindexordertogsi(indexOrders datastore.IndexKeyOrders) *qclient.IndexKeyOrder {

	if len(indexOrders) == 0 {
		return nil
	}

	order := &qclient.IndexKeyOrder{
		KeyPos: make([]int, len(indexOrders)),
		Desc:   make([]bool, len(indexOrders)),
	}

	for i, o := range indexOrders {
		order.KeyPos[i] = o.KeyPos
		order.Desc[i] = o.Desc
	}

	return order
}

func n1qlaggrtypetogsi(aggrType datastore.AggregateType) c.AggrFuncType {
	switch aggrType {
	case datastore.AGG_MIN:
		return c.AGG_MIN
	case datastore.AGG_MAX:
		return c.AGG_MAX
	case datastore.AGG_SUM:
		return c.AGG_SUM
	case datastore.AGG_COUNT:
		return c.AGG_COUNT
	case datastore.AGG_COUNTN:
		return c.AGG_COUNTN
	default:
		return c.AGG_INVALID
	}
}

func gsistatston1ql(stats []map[string]interface{}) []map[datastore.IndexStatType]value.Value {
	storageStats := make([]map[datastore.IndexStatType]value.Value, 0)
	for _, partitionStats := range stats {
		n1qlPartnStats := make(map[datastore.IndexStatType]value.Value)
		for key, val := range partitionStats {
			n1qlKey, ok := gsistatnameton1ql(key)
			if ok {
				n1qlPartnStats[n1qlKey] = value.NewValue(val)
			}
		}
		storageStats = append(storageStats, n1qlPartnStats)
	}
	return storageStats
}

func gsistatnameton1ql(name string) (datastore.IndexStatType, bool) {
	switch name {
	case qclient.STAT_PARTITION_ID:
		return datastore.IX_STAT_PARTITION_ID, true
	case qclient.STAT_NUM_PAGES:
		return datastore.IX_STAT_NUM_PAGES, true
	case qclient.STAT_NUM_ITEMS:
		return datastore.IX_STAT_NUM_ITEMS, true
	case qclient.STAT_RESIDENT_RATIO:
		return datastore.IX_STAT_RES_RATIO, true
	case qclient.STAT_NUM_INSERT:
		return datastore.IX_STAT_NUM_INSERT, true
	case qclient.STAT_NUM_DELETE:
		return datastore.IX_STAT_NUM_DELETE, true
	case qclient.STAT_AVG_ITEM_SIZE:
		return datastore.IX_STAT_AVG_ITEM_SIZE, true
	case qclient.STAT_AVG_PAGE_SIZE:
		return datastore.IX_STAT_AVG_PAGE_SIZE, true
	}
	return datastore.IndexStatType(""), false
}

//-------------------------------------
// IndexConfig Implementation
//-------------------------------------

const gConfigKeyTmpSpaceDir = "query_tmpspace_dir"
const gConfigKeyTmpSpaceLimit = "query_tmpspace_limit"

var gIndexConfig indexConfig

type indexConfig struct {
	config atomic.Value
}

func GetIndexConfig() (datastore.IndexConfig, errors.Error) {
	return &gIndexConfig, nil
}

func (c *indexConfig) SetConfig(conf map[string]interface{}) errors.Error {
	err := c.validateConfig(conf)
	if err != nil {
		return err
	}

	c.processConfig(conf)

	//make local copy so caller caller doesn't accidently modify
	localconf := make(map[string]interface{})
	for k, v := range conf {
		localconf[k] = v
	}

	l.Infof("GSIC - Setting config %v", conf)
	c.config.Store(localconf)
	return nil
}

//SetParam should not be called concurrently with SetConfig
func (c *indexConfig) SetParam(name string, val interface{}) errors.Error {

	conf := c.config.Load().(map[string]interface{})

	if conf != nil {
		tempconf := make(map[string]interface{})
		tempconf[name] = val
		err := c.validateConfig(tempconf)
		if err != nil {
			return err
		}
		c.processConfig(tempconf)
		l.Infof("GSIC - Setting param %v %v", name, val)
		conf[name] = val
	} else {
		conf = make(map[string]interface{})
		conf[name] = val
		return c.SetConfig(conf)
	}
	return nil
}

func (c *indexConfig) validateConfig(conf map[string]interface{}) errors.Error {

	if conf == nil {
		return nil
	}

	if v, ok := conf[gConfigKeyTmpSpaceDir]; ok {
		if _, ok1 := v.(string); !ok1 {
			err := fmt.Errorf("GSI Invalid Config Key %v Value %v", gConfigKeyTmpSpaceDir, v)
			l.Errorf(err.Error())
			return errors.NewError(err, err.Error())
		}
	}

	if v, ok := conf[gConfigKeyTmpSpaceLimit]; ok {
		if _, ok1 := v.(int64); !ok1 {
			err := fmt.Errorf("GSI Invalid Config Key %v Value %v", gConfigKeyTmpSpaceLimit, v)
			l.Errorf(err.Error())
			return errors.NewError(err, err.Error())
		}
	}

	return nil
}

func (c *indexConfig) processConfig(conf map[string]interface{}) {

	var olddir interface{}
	var newdir interface{}

	if conf != nil {
		newdir, _ = conf[gConfigKeyTmpSpaceDir]
	}

	prevconf := gIndexConfig.getConfig()

	if prevconf != nil {
		olddir, _ = prevconf[gConfigKeyTmpSpaceDir]
	}

	if olddir == nil {
		olddir = getDefaultTmpDir()
	}

	//cleanup any stale files
	if olddir != newdir {
		cleanupTmpFiles(olddir.(string))
		if newdir != nil {
			cleanupTmpFiles(newdir.(string))
		}
	}

	return

}

//best effort cleanup as tmpdir may change during restart
func cleanupTmpFiles(olddir string) {

	files, err := ioutil.ReadDir(olddir)
	if err != nil {
		return
	}

	conf, _ := c.GetSettingsConfig(c.SystemConfig)
	scantm := conf["indexer.settings.scan_timeout"].Int() // in ms.

	for _, file := range files {
		fname := path.Join(olddir, file.Name())
		mtime := file.ModTime()
		since := (time.Since(mtime).Seconds() * 1000) * 2 // twice the lng scan
		if (strings.Contains(fname, "scan-backfill") || strings.Contains(fname, BACKFILLPREFIX)) && int(since) > scantm {
			fmsg := "GSI client: removing old file %v last-modified @ %v"
			l.Infof(fmsg, fname, mtime)
			os.Remove(fname)
		}
	}

}

func getTmpSpaceDir() string {
	conf := gIndexConfig.getConfig()
	if conf == nil {
		return getDefaultTmpDir()
	}

	if v, ok := conf[gConfigKeyTmpSpaceDir]; ok {
		return v.(string)
	} else {
		return getDefaultTmpDir()
	}
}

// cleanupAllTmpFiles unconditionally deletes all the backfill files.
// So, it needs to be executed when there are no ongoing scan requests.
//
// cleanupAllTmpFiles uses the "known" temp space directory. So, it can
// default to "default temp space directory" i.e. os.TempDir(). So the
// cleanup of backfill files may be skipped if cleanup is triggered before
// the correct tmp space directory is set in gIndexConfig.
//
// cleanupAllTmpFiles performs the cleanup in two phases:
// Phase 1: Get the list of the files synchronously.
// Phase 2: deletes the backfill files from the above list asynchronously.
func cleanupAllTmpFiles() {
	dirpath := getTmpSpaceDir()

	files, err := ioutil.ReadDir(dirpath)
	if err != nil {
		l.Warnf("GSI client: Skipping cleaning of temp files due to error: %v", err)
		return
	}

	go func() {
		for _, file := range files {
			fname := path.Join(dirpath, file.Name())
			if strings.Contains(fname, "scan-backfill") ||
				strings.Contains(fname, BACKFILLPREFIX) {

				l.Infof("GSI client: Removing old temp file %v ...", fname)
				err := os.Remove(fname)
				if err != nil {
					l.Errorf("GSI client: Error while removing old temp file %v: %v", fname, err)
				}
			}
		}
	}()
}

func (c *indexConfig) getConfig() map[string]interface{} {

	conf := c.config.Load()
	if conf != nil {
		return conf.(map[string]interface{})
	} else {
		return nil
	}

}

func (gsi *gsiKeyspace) getTmpSpaceDir() string {

	conf := gIndexConfig.getConfig()

	if conf == nil {
		return getDefaultTmpDir()
	}

	if v, ok := conf[gConfigKeyTmpSpaceDir]; ok {
		return v.(string)
	} else {
		return getDefaultTmpDir()
	}

}

func (gsi *gsiKeyspace) getTmpSpaceLimit() int64 {

	conf := gIndexConfig.getConfig()

	if conf == nil {
		return int64(gsi.gsiClient.Settings().BackfillLimit())
	}

	if v, ok := conf[gConfigKeyTmpSpaceLimit]; ok {
		return v.(int64)
	} else {
		return int64(gsi.gsiClient.Settings().BackfillLimit())
	}

}

func getDefaultTmpDir() string {
	file, err := ioutil.TempFile("" /*dir*/, BACKFILLPREFIX)
	if err != nil {
		return ""
	}

	default_temp_dir := path.Dir(file.Name())
	os.Remove(file.Name()) // remove this file.

	return default_temp_dir
}

//-------------------------------------
// IndexConfig Implementation End
//-------------------------------------

//-------------------------------------
// Internal Version Handler
//-------------------------------------

type InternalVersionHandler struct {
}

func NewInternalVersionHandler() *InternalVersionHandler {
	return &InternalVersionHandler{}
}

func (ivh *InternalVersionHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	l.Debugf("InternalVersionHandler: ServeHTTP ... ")

	data, err := common.GetMarshalledInternalVersion()
	if err != nil {
		l.Debugf("InternalVersionHandler: ServeHTTP error %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	l.Debugf("InternalVersionHandler: ServeHTTP data %s", data)

	w.WriteHeader(http.StatusOK)
	w.Write(data)
}
