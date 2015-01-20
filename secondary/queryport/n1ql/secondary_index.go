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
import "sync"
import "strconv"

import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/collatejson"
import qclient "github.com/couchbase/indexing/secondary/queryport/client"
import mclient "github.com/couchbase/indexing/secondary/manager/client"
import "github.com/couchbaselabs/query/datastore"
import "github.com/couchbaselabs/query/errors"
import "github.com/couchbaselabs/query/expression"
import "github.com/couchbaselabs/query/expression/parser"
import "github.com/couchbaselabs/query/value"

// ErrorIndexEmpty is index not initialized.
var ErrorIndexEmpty = errors.NewError(nil, "gsi.empty")

// ErrorEmptyHost is no valid node hosting an index.
var ErrorEmptyHost = errors.NewError(nil, "gsi.emptyHost")

// PRIMARY_INDEX index name.
var PRIMARY_INDEX = "#primary"

var n1ql2GsiInclusion = map[datastore.Inclusion]qclient.Inclusion{
	datastore.NEITHER: qclient.Neither,
	datastore.LOW:     qclient.Low,
	datastore.HIGH:    qclient.High,
	datastore.BOTH:    qclient.Both,
}
var gsi2N1QLState = map[c.IndexState]datastore.IndexState{
	c.INDEX_STATE_CREATED: datastore.PENDING,
	c.INDEX_STATE_READY:   datastore.PENDING,
	c.INDEX_STATE_INITIAL: datastore.PENDING,
	c.INDEX_STATE_CATCHUP: datastore.PENDING,
	c.INDEX_STATE_ACTIVE:  datastore.ONLINE,
	c.INDEX_STATE_DELETED: datastore.OFFLINE,
	c.INDEX_STATE_ERROR:   datastore.OFFLINE,
}

//--------------------
// datastore.Indexer{}
//--------------------

// contains all index loaded via gsi cluster.
type gsiKeyspace struct {
	rw          sync.RWMutex
	clusterURL  string
	serviceAddr string
	namespace   string // aka pool
	keyspace    string // aka bucket
	gsiClient   *qclient.GsiClient
	indexes     map[uint64]*secondaryIndex // defnID -> index
}

// NewGSIIndexer manage new set of indexes under namespace->keyspace,
// also called as, pool->bucket.
func NewGSIIndexer(
	clusterURL, namespace, keyspace string) (datastore.Indexer, errors.Error) {

	gsi := &gsiKeyspace{
		clusterURL: clusterURL,
		namespace:  namespace,
		keyspace:   keyspace,
		indexes:    make(map[uint64]*secondaryIndex), // defnID -> index
	}

	serviceAddr, e := gsi.getLocalServiceAddr(clusterURL)
	if e != nil {
		return nil, e
	}
	gsi.serviceAddr = serviceAddr
	if err := gsi.Restart(); err != nil {
		return nil, err
	}
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
	return names, nil
}

// IndexById implements datastore.Indexer{} interface. Find an index on this
// keyspace using the index's id.
func (gsi *gsiKeyspace) IndexById(id string) (datastore.Index, errors.Error) {
	gsi.rw.Lock()
	defer gsi.rw.Unlock()
	defnID := string2defnID(id)
	index, ok := gsi.indexes[defnID]
	if !ok {
		errmsg := fmt.Sprintf("GSI index id %v not found.", id)
		err := errors.NewError(nil, errmsg)
		return nil, err
	}
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
	return indexes, nil
}

// IndexByPrimary implements datastore.Indexer{} interface. Returns the
// server-recommended primary index
func (gsi *gsiKeyspace) IndexByPrimary() (datastore.PrimaryIndex, errors.Error) {
	si, err := gsi.IndexByName(PRIMARY_INDEX)
	if err != nil {
		return nil, err
	}
	return si.(datastore.PrimaryIndex), nil
}

// CreatePrimaryIndex implements datastore.Indexer{} interface. Create or
// return a primary index on this keyspace
func (gsi *gsiKeyspace) CreatePrimaryIndex() (datastore.PrimaryIndex, errors.Error) {
	_, err := gsi.gsiClient.CreateIndex(
		PRIMARY_INDEX,
		gsi.keyspace,          /*bucket-name*/
		string(datastore.GSI), /*using*/
		"N1QL",                /*exprType*/
		"",                    /*partnStr*/
		"",                    /*whereStr*/
		nil,                   /*secStrs*/
		true,                  /*isPrimary*/
		nil /*TODO: tie this with CREATE INDEX stmt*/)
	if err != nil {
		return nil, errors.NewError(err, "GSI CreatePrimaryIndex()")
	}
	// refresh to get back the newly created index.
	if err := gsi.Refresh(); err != nil {
		return nil, err
	}
	return gsi.IndexByPrimary()
}

// CreateIndex implements datastore.Indexer{} interface. Create a secondary
// index on this keyspace
func (gsi *gsiKeyspace) CreateIndex(
	name string, seekKey, rangeKey expression.Expressions,
	where expression.Expression) (datastore.Index, errors.Error) {

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
	defnID, err := gsi.gsiClient.CreateIndex(
		name,
		gsi.keyspace,          /*bucket-name*/
		string(datastore.GSI), /*using*/
		"N1QL",                /*exprType*/
		partnStr, whereStr, secStrs,
		false, /*isPrimary*/
		nil /*TODO: tie this with CREATE INDEX stmt*/)
	if err != nil {
		return nil, errors.NewError(err, "GSI CreatePrimaryIndex()")
	}
	// refresh to get back the newly created index.
	if err := gsi.Refresh(); err != nil {
		return nil, err
	}
	return gsi.IndexById(defnID2String(uint64(defnID)))
}

// Refresh list of indexes and scanner clients.
func (gsi *gsiKeyspace) Refresh() errors.Error {
	indexes, err := gsi.gsiClient.Refresh()
	if err != nil {
		return errors.NewError(err, "GSI Refresh()")
	}
	gsi.clearIndexes()
	for _, index := range indexes {
		si, err := newSecondaryIndexFromMetaData(gsi, index)
		if err != nil {
			return err
		}
		if err := gsi.setIndex(si); err != nil {
			return err
		}
	}
	return nil
}

// Restart will close existing gsiClient and restart.
func (gsi *gsiKeyspace) Restart() errors.Error {
	if gsi.gsiClient != nil {
		gsi.gsiClient.Close()
	}
	qconf := c.SystemConfig.SectionConfig("queryport.client.", true /*trim*/)
	c, err := qclient.NewGsiClient(gsi.clusterURL, gsi.serviceAddr, qconf)
	if err != nil {
		return errors.NewError(err, "NewGsiClient()")
	}
	gsi.gsiClient = c
	return gsi.Refresh()
}

//------------------------------------------
// private functions for datastore.Indexer{}
//------------------------------------------

func (gsi *gsiKeyspace) setIndex(si *secondaryIndex) errors.Error {
	gsi.rw.Lock()
	defer gsi.rw.Unlock()
	gsi.indexes[si.defnID] = si
	return nil
}

// for getIndex() use IndexById()

func (gsi *gsiKeyspace) delIndex(id string) {
	gsi.rw.Lock()
	defer gsi.rw.Unlock()
	defnID := string2defnID(id)
	delete(gsi.indexes, defnID)
}

func (gsi *gsiKeyspace) clearIndexes() {
	gsi.rw.Lock()
	defer gsi.rw.Unlock()
	gsi.indexes = make(map[uint64]*secondaryIndex) // defnID -> index
}

// return n1ql's service address, called once during bootstrap
func (gsi *gsiKeyspace) getLocalServiceAddr(
	cluster string) (string, errors.Error) {

	cinfo, err := getClusterInfo(cluster, "default" /*pool*/) // TODO:nomagic
	if err != nil {
		return "", err
	}
	nodeID := cinfo.GetCurrentNode()
	serviceAddr, e := cinfo.GetServiceAddress(nodeID, "n1ql")
	if e != nil {
		return "", errors.NewError(e, fmt.Sprintf("ClusterInfo() failed"))
	}
	return serviceAddr, nil
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
	using     datastore.IndexType
	partnExpr string
	secExprs  []string
	whereExpr string
	state     datastore.IndexState
}

// for metadata-provider.
func newSecondaryIndexFromMetaData(
	gsi *gsiKeyspace,
	imd *mclient.IndexMetadata) (si *secondaryIndex, err errors.Error) {

	if len(imd.Instances) < 1 {
		return nil, errors.NewError(nil, "no instance created by metadata")
	}
	state, indexDefn := imd.Instances[0].State, imd.Definition
	defnID := uint64(indexDefn.DefnId)
	si = &secondaryIndex{
		gsi:       gsi,
		bucketn:   indexDefn.Bucket,
		name:      indexDefn.Name,
		defnID:    defnID,
		isPrimary: indexDefn.IsPrimary,
		using:     datastore.IndexType(indexDefn.Using),
		partnExpr: indexDefn.PartitionKey,
		secExprs:  indexDefn.SecExprs,
		whereExpr: "", // TODO: where-clause.
		state:     gsi2N1QLState[state],
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
	return si.using
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

// State implement Index{} interface.
func (si *secondaryIndex) State() (datastore.IndexState, errors.Error) {
	return si.state, nil
}

// Statistics implement Index{} interface.
func (si *secondaryIndex) Statistics(
	span *datastore.Span) (datastore.Statistics, errors.Error) {

	if si == nil {
		return nil, ErrorIndexEmpty
	}
	client := si.gsi.gsiClient

	defnID := si.defnID
	if span.Seek != nil {
		seek := values2SKey(span.Seek)
		pstats, err := client.LookupStatistics(defnID, seek)
		if err != nil {
			return nil, errors.NewError(err, "GSI Statistics()")
		}
		return newStatistics(pstats), nil
	}
	low := values2SKey(span.Range.Low)
	high := values2SKey(span.Range.High)
	incl := n1ql2GsiInclusion[span.Range.Inclusion]
	pstats, err := client.RangeStatistics(defnID, low, high, incl)
	if err != nil {
		return nil, errors.NewError(err, "GSI Statistics()")
	}
	return newStatistics(pstats), nil
}

// Count implement Index{} interface.
func (si *secondaryIndex) Count(
	span *datastore.Span) (count int64, err errors.Error) {

	if si == nil {
		return 0, ErrorIndexEmpty
	}
	client := si.gsi.gsiClient

	var e error
	if span.Seek != nil {
		seek := values2SKey(span.Seek)
		count, e = client.CountLookup(si.defnID, []c.SecondaryKey{seek})
		if e != nil {
			err = errors.NewError(err, "GSI CountLookup()")
		}

	} else {
		low, high := values2SKey(span.Range.Low), values2SKey(span.Range.High)
		incl := n1ql2GsiInclusion[span.Range.Inclusion]
		count, e = client.CountRange(si.defnID, low, high, incl)
		if e != nil {
			err = errors.NewError(err, "GSI CountRange()")
		}
	}
	return count, err
}

// Drop implement Index{} interface.
func (si *secondaryIndex) Drop() errors.Error {
	if si == nil {
		return ErrorIndexEmpty
	}
	if err := si.gsi.gsiClient.DropIndex(si.defnID); err != nil {
		return errors.NewError(err, "GSI Drop()")
	}
	si.gsi.delIndex(si.Id())
	return nil
}

// Scan implement Index{} interface.
func (si *secondaryIndex) Scan(
	span *datastore.Span, distinct bool, limit int64,
	conn *datastore.IndexConnection) {

	entryChannel := conn.EntryChannel()
	defer close(entryChannel)

	client := si.gsi.gsiClient
	if span.Seek != nil {
		seek := values2SKey(span.Seek)
		client.Lookup(
			si.defnID, []c.SecondaryKey{seek}, distinct, limit,
			makeResponsehandler(conn))

	} else {
		low, high := values2SKey(span.Range.Low), values2SKey(span.Range.High)
		incl := n1ql2GsiInclusion[span.Range.Inclusion]
		client.Range(
			si.defnID, low, high, incl, distinct, limit,
			makeResponsehandler(conn))
	}
}

// Scan implement PrimaryIndex{} interface.
func (si *secondaryIndex) ScanEntries(
	limit int64, conn *datastore.IndexConnection) {

	entryChannel := conn.EntryChannel()
	defer close(entryChannel)

	client := si.gsi.gsiClient
	client.ScanAll(si.defnID, limit, makeResponsehandler(conn))
}

//-------------------------------------
// private functions for secondaryIndex
//-------------------------------------

func makeResponsehandler(
	conn *datastore.IndexConnection) qclient.ResponseHandler {

	entryChannel := conn.EntryChannel()
	stopChannel := conn.StopChannel()

	return func(data qclient.ResponseReader) bool {
		if err := data.Error(); err != nil {
			conn.Error(errors.NewError(nil, err.Error()))
			return false

		}
		skeys, pkeys, err := data.GetEntries()
		if err == nil {
			for i, skey := range skeys {
				// Primary-key is mandatory.
				e := &datastore.IndexEntry{
					PrimaryKey: string(pkeys[i]),
				}
				e.EntryKey = skey2Values(skey)

				select {
				case entryChannel <- e:
				case <-stopChannel:
					return false
				}
			}
			return true
		}
		conn.Error(errors.NewError(nil, err.Error()))
		return false
	}
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

	cinfo, err := c.NewClusterInfoCache(c.ClusterUrl(cluster), pooln)
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
