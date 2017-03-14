// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS"
// BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing
// permissions and limitations under the License.

package n1ql

import "fmt"
import "os"
import "sync"
import "time"
import "path"
import "strings"
import "encoding/gob"
import "strconv"
import "io/ioutil"
import "sync/atomic"

import l "github.com/couchbase/indexing/secondary/logging"
import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/collatejson"
import qclient "github.com/couchbase/indexing/secondary/queryport/client"
import mclient "github.com/couchbase/indexing/secondary/manager/client"
import "github.com/couchbase/query/datastore"
import "github.com/couchbase/query/errors"
import "github.com/couchbase/query/expression"
import "github.com/couchbase/query/expression/parser"
import "github.com/couchbase/query/timestamp"
import "github.com/couchbase/query/value"
import qlog "github.com/couchbase/query/logging"

const DONEREQUEST = 1

// ErrorIndexEmpty is index not initialized.
var ErrorIndexEmpty = errors.NewError(
	fmt.Errorf("gsi.indexEmpty"), "Fatal null reference to index")

// ErrorIndexNotAvailable means client indexes list needs to be
// refreshed.
var ErrorIndexNotAvailable = fmt.Errorf("index not available")

var n1ql2GsiInclusion = map[datastore.Inclusion]qclient.Inclusion{
	datastore.NEITHER: qclient.Neither,
	datastore.LOW:     qclient.Low,
	datastore.HIGH:    qclient.High,
	datastore.BOTH:    qclient.Both,
}
var gsi2N1QLState = map[c.IndexState]datastore.IndexState{
	c.INDEX_STATE_CREATED: datastore.PENDING, // might also be DEFERRED
	c.INDEX_STATE_READY:   datastore.PENDING, // might also be DEFERRED
	c.INDEX_STATE_INITIAL: datastore.BUILDING,
	c.INDEX_STATE_CATCHUP: datastore.BUILDING,
	c.INDEX_STATE_ACTIVE:  datastore.ONLINE,
	c.INDEX_STATE_DELETED: datastore.OFFLINE,
	c.INDEX_STATE_ERROR:   datastore.OFFLINE,
	// c.INDEX_STATE_NIL:     datastore.OFFLINE, TODO: uncomment this.
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

	rw             sync.RWMutex
	clusterURL     string
	namespace      string // aka pool
	keyspace       string // aka bucket
	gsiClient      *qclient.GsiClient
	config         c.Config
	indexes        map[uint64]*secondaryIndex // defnID -> index
	primaryIndexes map[uint64]*secondaryIndex
	logPrefix      string
}

// NewGSIIndexer manage new set of indexes under namespace->keyspace,
// also called as, pool->bucket.
// will return an error when,
// - GSI cluster is not available.
// - network partitions / errors.
func NewGSIIndexer(
	clusterURL, namespace, keyspace string) (datastore.Indexer, errors.Error) {

	l.SetLogLevel(l.Info)

	gsi := &gsiKeyspace{
		clusterURL:     clusterURL,
		namespace:      namespace,
		keyspace:       keyspace,
		indexes:        make(map[uint64]*secondaryIndex), // defnID -> index
		primaryIndexes: make(map[uint64]*secondaryIndex),
	}
	tm := time.Now().UnixNano()
	gsi.logPrefix = fmt.Sprintf("GSIC[%s/%s-%v]", namespace, keyspace, tm)

	// get the singleton-client
	conf, err := c.GetSettingsConfig(c.SystemConfig)
	if err != nil {
		return nil, errors.NewError(err, "GSI config instantiation failed")
	}
	qconf := conf.SectionConfig("queryport.client.", true /*trim*/)
	client, err := getSingletonClient(clusterURL, qconf)
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
	go gsi.logstats(logtick)
	go gsi.backfillMonitor(5 * time.Second)
	return gsi, nil
}

// KeyspaceId implements datastore.Indexer{} interface.
// Id of the keyspace to which this indexer belongs
func (gsi *gsiKeyspace) KeyspaceId() string {
	return gsi.keyspace
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
			return nil, err
		}
	}
	l.Debugf("%v IndexById %v = %v", gsi.logPrefix, id, index)
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
	return nil, err
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

	l.Debugf("%v gsiKeySpace.Indexes(): %v", gsi.logPrefix, indexes)
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
	l.Debugf("%v gsiKeySpace.PrimaryIndexes(): %v", gsi.logPrefix, indexes)
	return indexes, nil
}

// CreatePrimaryIndex implements datastore.Indexer{} interface. Create or
// return a primary index on this keyspace
func (gsi *gsiKeyspace) CreatePrimaryIndex(
	requestId, name string, with value.Value) (datastore.PrimaryIndex, errors.Error) {

	var withJSON []byte
	var err error
	if with != nil {
		if withJSON, err = with.MarshalJSON(); err != nil {
			return nil, errors.NewError(err, "GSI error marshalling WITH clause")
		}
	}
	defnID, err := gsi.gsiClient.CreateIndex(
		name,
		gsi.keyspace, /*bucket-name*/
		"GSI",        /*using*/
		"N1QL",       /*exprType*/
		"",           /*partnStr*/
		"",           /*whereStr*/
		nil,          /*secStrs*/
		true,         /*isPrimary*/
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
	datastore.Index, errors.Error) {

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
		gsi.keyspace, /*bucket-name*/
		"GSI",        /*using*/
		"N1QL",       /*exprType*/
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

	var partnStr string
	if seekKey != nil && len(seekKey) > 0 {
		partnStr = expression.NewStringer().Visit(seekKey[0])
	}

	var whereStr string
	if where != nil {
		whereStr = expression.NewStringer().Visit(where)
	}

	secStrs := make([]string, len(rangeKey))
	desc := make([]bool, len(rangeKey))

	for i, key := range rangeKey {
		s := expression.NewStringer().Visit(key.Expr)
		secStrs[i] = s
		desc[i] = key.Desc
	}

	var withJSON []byte
	var err error
	if with != nil {
		if withJSON, err = with.MarshalJSON(); err != nil {
			return nil, errors.NewError(err, "GSI error marshalling WITH clause")
		}
	}
	defnID, err := gsi.gsiClient.CreateIndex2(
		name,
		gsi.keyspace, /*bucket-name*/
		"GSI",        /*using*/
		"N1QL",       /*exprType*/
		partnStr, whereStr, secStrs, desc,
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

// BuildIndexes implements datastore.Indexer{} interface.
func (gsi *gsiKeyspace) BuildIndexes(requestId string, names ...string) errors.Error {
	defnIDs := make([]uint64, len(names))
	for i, name := range names {
		index, err := gsi.IndexByName(name)
		if err != nil {
			return errors.NewError(err, "BuildIndexes")
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
	indexes, err := gsi.gsiClient.Refresh()
	if err != nil {
		return errors.NewError(err, "GSI Refresh()")
	}
	si_s := make([]*secondaryIndex, 0, len(indexes))
	for _, index := range indexes {
		if index.Definition.Bucket != gsi.keyspace {
			continue
		}
		si, err := newSecondaryIndexFromMetaData(gsi, index)
		if err != nil {
			return err
		}
		si_s = append(si_s, si)
	}
	if err := gsi.setIndexes(si_s); err != nil {
		return err
	}
	return nil
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

//------------------------------------------
// private functions for datastore.Indexer{}
//------------------------------------------

func (gsi *gsiKeyspace) setIndexes(si []*secondaryIndex) errors.Error {
	gsi.rw.Lock()
	defer gsi.rw.Unlock()
	gsi.indexes = make(map[uint64]*secondaryIndex)        // defnID -> index
	gsi.primaryIndexes = make(map[uint64]*secondaryIndex) // defnID -> index
	for _, si := range si {
		if si.isPrimary {
			gsi.primaryIndexes[si.defnID] = si
		} else {
			gsi.indexes[si.defnID] = si
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
	partnExpr string
	secExprs  []string
	whereExpr string
	state     datastore.IndexState
	err       string
	deferred  bool
}

// for metadata-provider.
func newSecondaryIndexFromMetaData(
	gsi *gsiKeyspace,
	imd *mclient.IndexMetadata) (si *secondaryIndex, err errors.Error) {

	if len(imd.Instances) < 1 && len(imd.InstsInRebalance) < 1 {
		return nil, errors.NewError(nil, "no instance are created by GSI")
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
		partnExpr: indexDefn.PartitionKey,
		secExprs:  indexDefn.SecExprs,
		whereExpr: indexDefn.WhereExpr,
		state:     gsi2N1QLState[imd.State],
		err:       imd.Error,
		deferred:  indexDefn.Deferred,
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
	return si.bucketn
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
	if si != nil && si.partnExpr != "" {
		expr, _ := parser.Parse(si.partnExpr)
		return expression.Expressions{expr}
	}
	return nil
}

// RangeKey implement Index{} interface.
func (si *secondaryIndex) RangeKey() expression.Expressions {
	if si != nil && si.secExprs != nil {
		exprs := make(expression.Expressions, 0, len(si.secExprs))
		for _, exprS := range si.secExprs {
			expr, _ := parser.Parse(exprS)
			exprs = append(exprs, expr)
		}
		return exprs
	}
	return nil
}

// Condition implement Index{} interface.
func (si *secondaryIndex) Condition() expression.Expression {
	if si != nil && si.whereExpr != "" {
		expr, _ := parser.Parse(si.whereExpr)
		return expr
	}
	return nil
}

// IsPrimary implements Index{} interface.
func (si *secondaryIndex) IsPrimary() bool {
	return si.isPrimary
}

// State implement Index{} interface.
func (si *secondaryIndex) State() (datastore.IndexState, string, errors.Error) {
	if si.err != "" {
		// if err is not empty, return OFFLINE with error reason
		// and the state in which the error occured.
		msg := fmt.Sprintf("error: %s. Index %s(%s). Index state: %s",
			si.err, si.Name(), si.RangeKey().String(), si.state.String())
		return datastore.OFFLINE, msg, errors.NewCbIndexStateError(msg)
	}
	return si.state, "", nil
}

// Statistics implement Index{} interface.
func (si *secondaryIndex) Statistics(
	requestId string, span *datastore.Span) (datastore.Statistics, errors.Error) {

	if si == nil {
		return nil, ErrorIndexEmpty
	}
	client := si.gsi.gsiClient

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

// Scan implement old Index{} interface.
func (si *secondaryIndex) Scan(
	requestId string, span *datastore.Span, distinct bool, limit int64,
	cons datastore.ScanConsistency, vector timestamp.Vector,
	conn *datastore.IndexConnection) {

	entryChannel := conn.EntryChannel()
	var tmpfile *os.File
	var backfillSync int64

	syncCh := make(chan bool)

	defer close(entryChannel)
	defer func() { // cleanup tmpfile
		if tmpfile != nil {
			<-syncCh
			tmpfile.Close()
			name := tmpfile.Name()
			fmsg := "%v Scan(%v) removing backfill file %v ...\n"
			l.Infof(fmsg, si.gsi.logPrefix, requestId, name)
			if err := os.Remove(name); err != nil {
				fmsg := "%v remove backfill file %v unexpected failure: %v\n"
				l.Errorf(fmsg, si.gsi.logPrefix, name, err)
			}
			atomic.AddInt64(&si.gsi.totalbackfills, 1)
		}
	}()
	defer func() {
		atomic.StoreInt64(&backfillSync, DONEREQUEST)
	}()

	starttm := time.Now()

	client, cnf := si.gsi.gsiClient, si.gsi.config
	if span.Seek != nil {
		seek := values2SKey(span.Seek)
		client.Lookup(
			si.defnID, requestId, []c.SecondaryKey{seek}, distinct, limit,
			n1ql2GsiConsistency[cons], vector2ts(vector),
			makeResponsehandler(
				requestId,
				si, client, conn, &tmpfile, &backfillSync, syncCh, cnf))

	} else {
		low, high := values2SKey(span.Range.Low), values2SKey(span.Range.High)
		incl := n1ql2GsiInclusion[span.Range.Inclusion]
		client.Range(
			si.defnID, requestId, low, high, incl, distinct, limit,
			n1ql2GsiConsistency[cons], vector2ts(vector),
			makeResponsehandler(
				requestId,
				si, client, conn, &tmpfile, &backfillSync, syncCh, cnf))
	}
	atomic.AddInt64(&si.gsi.totalscans, 1)
	atomic.AddInt64(&si.gsi.scandur, int64(time.Since(starttm)))
}

// Scan2 implement Index2 interface.
func (si *secondaryIndex) Scan2(
	requestId string, spans datastore.Spans2, reverse, distinct, ordered bool,
	projection *datastore.IndexProjection, offset, limit int64,
	cons datastore.ScanConsistency, vector timestamp.Vector,
	conn *datastore.IndexConnection) {

	entryChannel := conn.EntryChannel()
	var tmpfile *os.File
	var backfillSync int64

	syncCh := make(chan bool)

	defer close(entryChannel)
	defer func() { // cleanup tmpfile
		if tmpfile != nil {
			<-syncCh
			tmpfile.Close()
			name := tmpfile.Name()
			fmsg := "%v Scan(%v) removing backfill file %v ...\n"
			l.Infof(fmsg, si.gsi.logPrefix, requestId, name)
			if err := os.Remove(name); err != nil {
				fmsg := "%v remove backfill file %v unexpected failure: %v\n"
				l.Errorf(fmsg, si.gsi.logPrefix, name, err)
			}
			atomic.AddInt64(&si.gsi.totalbackfills, 1)
		}
	}()
	defer func() {
		atomic.StoreInt64(&backfillSync, DONEREQUEST)
	}()

	starttm := time.Now()

	client, cnf := si.gsi.gsiClient, si.gsi.config

	gsiscans := n1qlspanstogsi(spans)
	gsiprojection := n1qlprojectiontogsi(projection)
	client.MultiScan(
		si.defnID, requestId, gsiscans, reverse, distinct,
		gsiprojection, offset, limit,
		n1ql2GsiConsistency[cons], vector2ts(vector),
		makeResponsehandler(
			requestId,
			si, client, conn, &tmpfile, &backfillSync, syncCh, cnf))

	atomic.AddInt64(&si.gsi.totalscans, 1)
	atomic.AddInt64(&si.gsi.scandur, int64(time.Since(starttm)))
}

// RangeKey2 implements Index2{} interface.
func (si *secondaryIndex) RangeKey2() datastore.IndexKeys {
	if si != nil && si.secExprs != nil {
		idxkeys := make(datastore.IndexKeys, 0, len(si.secExprs))
		for _, exprS := range si.secExprs {
			expr, _ := parser.Parse(exprS)
			idxkey := &datastore.IndexKey{
				Expr: expr,
				Desc: false,
			}
			idxkeys = append(idxkeys, idxkey)
		}
		return idxkeys
	}
	return nil
}

// Count2 implement CountIndex2 interface.
func (si *secondaryIndex) Count2(requestId string, spans datastore.Spans2,
	cons datastore.ScanConsistency, vector timestamp.Vector) (int64, errors.Error) {

	if si == nil {
		return 0, ErrorIndexEmpty
	}
	client := si.gsi.gsiClient

	gsiscans := n1qlspanstogsi(spans)

	count, e := client.MultiScanCount(si.defnID, requestId, gsiscans, false,
		n1ql2GsiConsistency[cons], vector2ts(vector))
	if e != nil {
		return 0, n1qlError(client, e)
	}
	return count, nil
}

// CanCountDistinct implement CountIndex2 interface.
func (si *secondaryIndex) CanCountDistinct() bool {
	return true
}

// CountDistinct implement CountIndex2 interface.
func (si *secondaryIndex) CountDistinct(requestId string, spans datastore.Spans2,
	cons datastore.ScanConsistency, vector timestamp.Vector) (int64, errors.Error) {

	if si == nil {
		return 0, ErrorIndexEmpty
	}
	client := si.gsi.gsiClient

	gsiscans := n1qlspanstogsi(spans)

	count, e := client.MultiScanCount(si.defnID, requestId, gsiscans, true,
		n1ql2GsiConsistency[cons], vector2ts(vector))
	if e != nil {
		return 0, n1qlError(client, e)
	}
	return count, nil
}

// Scan implement PrimaryIndex{} interface.
func (si *secondaryIndex) ScanEntries(
	requestId string, limit int64, cons datastore.ScanConsistency,
	vector timestamp.Vector, conn *datastore.IndexConnection) {

	entryChannel := conn.EntryChannel()
	var tmpfile *os.File
	var backfillSync int64

	syncCh := make(chan bool)

	defer close(entryChannel)
	defer func() {
		if tmpfile != nil {
			<-syncCh
			tmpfile.Close()
			name := tmpfile.Name()
			fmsg := "%v ScanEntries(%v) removing backfill file %v ...\n"
			l.Infof(fmsg, si.gsi.logPrefix, requestId, name)
			if err := os.Remove(name); err != nil {
				fmsg := "%v remove backfill file %v unexpected failure: %v\n"
				l.Errorf(fmsg, si.gsi.logPrefix, name, err)
			}
			atomic.AddInt64(&si.gsi.totalbackfills, 1)
		}
	}()
	defer func() {
		atomic.StoreInt64(&backfillSync, DONEREQUEST)
	}()

	starttm := time.Now()

	client, cnf := si.gsi.gsiClient, si.gsi.config
	client.ScanAll(
		si.defnID, requestId, limit,
		n1ql2GsiConsistency[cons], vector2ts(vector),
		makeResponsehandler(
			requestId,
			si, client, conn, &tmpfile, &backfillSync, syncCh, cnf))

	atomic.AddInt64(&si.gsi.totalscans, 1)
	atomic.AddInt64(&si.gsi.scandur, int64(time.Since(starttm)))
}

//-------------------------------------
// private functions for secondaryIndex
//-------------------------------------

func makeResponsehandler(
	requestId string,
	si *secondaryIndex,
	client *qclient.GsiClient,
	conn *datastore.IndexConnection,
	tmpfile **os.File, backfillSync *int64, syncCh chan bool,
	config c.Config) qclient.ResponseHandler {

	entryChannel := conn.EntryChannel()

	var enc *gob.Encoder
	var dec *gob.Decoder
	var readfd *os.File
	var backfillFin, backfillEntries int64

	backfillLimit := int64(config["backfillLimit"].Int())
	primed, starttm, ticktm := false, time.Now(), time.Now()
	lprefix := si.gsi.logPrefix

	backfill := func() {
		name := (*tmpfile).Name()
		defer func() {
			if readfd != nil {
				readfd.Close()
			}
			close(syncCh)
			atomic.AddInt64(&backfillFin, 1)
			l.Debugf(
				"%v %q finished backfill for %v ...\n",
				lprefix, requestId, name)
			recover() // need this because entryChannel() would have closed
		}()
		l.Debugf(
			"%v %q started backfill for %v ...\n", lprefix, requestId, name)
		for {
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
				fmsg := "%q backfill exceeded limit %v, %v"
				err := fmt.Errorf(fmsg, requestId, backfillLimit, cummsize)
				conn.Error(n1qlError(client, err))
				return
			}

			skeys := make([]c.SecondaryKey, 0)
			if err := dec.Decode(&skeys); err != nil {
				fmsg := "%v %q decoding from backfill %v: %v\n"
				l.Errorf(fmsg, lprefix, requestId, name, err)
				conn.Error(n1qlError(client, err))
				return
			}
			pkeys := make([][]byte, 0)
			if err := dec.Decode(&pkeys); err != nil {
				fmsg := "%v %q decoding from backfill %v: %v\n"
				l.Errorf(fmsg, lprefix, requestId, name, err)
				conn.Error(n1qlError(client, err))
				return
			}
			l.Tracef("%v backfill read %v entries\n", lprefix, len(skeys))
			if primed == false {
				atomic.AddInt64(&si.gsi.primedur, int64(time.Since(starttm)))
				primed = true
			}
			if len(entryChannel) > 0 && len(skeys) > 0 {
				atomic.AddInt64(&si.gsi.throttledur, int64(time.Since(ticktm)))
			}
			if sendEntries(si, pkeys, skeys, conn) == "stop" {
				return
			}
			ticktm = time.Now()
		}
	}

	return func(data qclient.ResponseReader) bool {
		err := data.Error()
		if err != nil {
			conn.Error(n1qlError(client, err))
			return false
		}
		skeys, pkeys, err := data.GetEntries()
		if err != nil {
			conn.Error(n1qlError(client, err))
			return false
		}
		cp, ln := cap(entryChannel), len(entryChannel)
		if backfillLimit > 0 && *tmpfile == nil && ((cp - ln) < len(skeys)) {
			prefix := "scan-backfill" + strconv.Itoa(os.Getpid())
			*tmpfile, err = ioutil.TempFile(n1ql_backfill_temp_dir, prefix)
			name := ""
			if *tmpfile != nil {
				name = (*tmpfile).Name()
			}
			if err != nil {
				fmsg := "%v %q creating backfill file %v : %v\n"
				l.Errorf(fmsg, lprefix, requestId, name, err)
				*tmpfile = nil
				conn.Error(n1qlError(client, err))
				return false

			} else {
				fmsg := "%v %v new backfill file ... %v\n"
				l.Infof(fmsg, lprefix, requestId, name)
				// encoder
				enc = gob.NewEncoder(*tmpfile)
				readfd, err = os.OpenFile(name, os.O_RDONLY, 0666)
				if err != nil {
					fmsg := "%v %v reading backfill file %v: %v\n"
					l.Errorf(fmsg, lprefix, requestId, name, err)
					conn.Error(n1qlError(client, err))
					return false
				}
				// decoder
				dec = gob.NewDecoder(readfd)
				go backfill()
			}
		}

		if *tmpfile != nil {
			// whether temp-file is exhausted the limit.
			cummsize := atomic.LoadInt64(&si.gsi.backfillSize) / (1024 * 1024)
			if cummsize > backfillLimit {
				fmsg := "%q backfill exceeded limit %v, %v"
				err := fmt.Errorf(fmsg, requestId, backfillLimit, cummsize)
				conn.Error(n1qlError(client, err))
				return false
			}

			l.Tracef("%v backfill %v entries\n", lprefix, len(skeys))
			if atomic.LoadInt64(&backfillFin) > 0 {
				return false
			}
			if err := enc.Encode(skeys); err != nil {
				conn.Error(n1qlError(client, err))
				return false
			}
			if err := enc.Encode(pkeys); err != nil {
				conn.Error(n1qlError(client, err))
				return false
			}
			atomic.AddInt64(&backfillEntries, 1)

		} else {
			fmsg := "%v response cap:%v len:%v entries:%v\n"
			l.Tracef(fmsg, lprefix, cp, ln, len(skeys))
			if primed == false {
				atomic.AddInt64(&si.gsi.primedur, int64(time.Since(starttm)))
				primed = true
			}
			if len(entryChannel) > 0 && len(skeys) > 0 {
				atomic.AddInt64(&si.gsi.throttledur, int64(time.Since(ticktm)))
			}
			if sendEntries(si, pkeys, skeys, conn) == "stop" {
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
	switch err.Error() {
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
		skey = append(skey, val.Actual())
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
	if vector == nil {
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

func getSingletonClient(
	clusterURL string, qconf c.Config) (*qclient.GsiClient, error) {

	muclient.Lock()
	defer muclient.Unlock()
	if singletonClient == nil {
		l.Debugf("creating singleton for URL %v", clusterURL)
		client, err := qclient.NewGsiClientWithSettings(clusterURL, qconf, true)
		if err != nil {
			return nil, fmt.Errorf("in NewGsiClient(): %v", err)
		}
		singletonClient = client
	}
	return singletonClient, nil
}

var n1ql_backfill_temp_dir string

func init() {
	// register gob objects for complex composite keys.
	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})

	file, err := ioutil.TempFile("" /*dir*/, "scan-backfill")
	if err != nil {
		return
	}

	n1ql_backfill_temp_dir = path.Dir(file.Name())
	os.Remove(file.Name()) // remove this file.

	files, err := ioutil.ReadDir(n1ql_backfill_temp_dir)
	if err != nil {
		return
	}

	conf, _ := c.GetSettingsConfig(c.SystemConfig)
	scantm := conf["indexer.settings.scan_timeout"].Int() // in ms.

	for _, file := range files {
		fname := path.Join(n1ql_backfill_temp_dir, file.Name())
		mtime := file.ModTime()
		since := (time.Since(mtime).Seconds() * 1000) * 2 // twice the lng scan
		if strings.Contains(fname, "scan-backfill") && int(since) > scantm {
			fmsg := "GSI client: removing old file %v last-modified @ %v"
			l.Infof(fmsg, fname, mtime)
			os.Remove(fname)
		}
	}
}

func sendEntries(
	si *secondaryIndex, pkeys [][]byte, skeys []c.SecondaryKey,
	conn *datastore.IndexConnection) string {

	if len(skeys) == 0 {
		return "empty"
	}

	var start time.Time
	blockedtm, blocked := int64(0), false

	entryChannel := conn.EntryChannel()
	stopChannel := conn.StopChannel()

	for i, skey := range skeys {
		// Primary-key is mandatory.
		e := &datastore.IndexEntry{PrimaryKey: string(pkeys[i])}
		e.EntryKey = skey2Values(skey)
		cp, ln := cap(entryChannel), len(entryChannel)
		if ln == cp {
			start, blocked = time.Now(), true
		}
		select {
		case entryChannel <- e:
		case <-stopChannel:
			return "stop"
		}
		if blocked {
			blockedtm += int64(time.Since(start))
			blocked = false
		}
	}
	atomic.AddInt64(&si.gsi.blockeddur, blockedtm)
	return "ok"
}

func (gsi *gsiKeyspace) logstats(logtick time.Duration) {
	tick := time.NewTicker(logtick)
	defer func() {
		tick.Stop()
	}()

	sofar := int64(0)
	for {
		<-tick.C
		scandur := atomic.LoadInt64(&gsi.scandur)
		blockeddur := atomic.LoadInt64(&gsi.blockeddur)
		throttledur := atomic.LoadInt64(&gsi.throttledur)
		primedur := atomic.LoadInt64(&gsi.primedur)
		totalscans := atomic.LoadInt64(&gsi.totalscans)
		totalbackfills := atomic.LoadInt64(&gsi.totalbackfills)
		if totalscans > sofar {
			fmsg := `%v logstats %q {` +
				`"gsi_scan_count":%v,"gsi_scan_duration":%v,` +
				`"gsi_throttle_duration":%v,` +
				`"gsi_prime_duration":%v,"gsi_blocked_duration":%v,` +
				`"gsi_totalbackfills":%v}`
			l.Infof(
				fmsg, gsi.logPrefix, gsi.keyspace, totalscans, scandur,
				throttledur, primedur, blockeddur, totalbackfills)
		}
		sofar = totalscans
	}
}

func (gsi *gsiKeyspace) backfillMonitor(period time.Duration) {
	tick := time.NewTicker(period)
	defer func() {
		tick.Stop()
	}()

	for {
		<-tick.C
		files, err := ioutil.ReadDir(n1ql_backfill_temp_dir)
		if err != nil {
			return
		}

		size := int64(0)
		for _, file := range files {
			fname := path.Join(n1ql_backfill_temp_dir, file.Name())
			if strings.Contains(fname, "scan-backfill") {
				size += int64(file.Size())
			}
		}
		atomic.StoreInt64(&gsi.backfillSize, size)
	}
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
			l = r.Low.Actual()
		}

		if r.High == nil {
			h = c.MaxUnbounded
		} else {
			h = r.High.Actual()
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
