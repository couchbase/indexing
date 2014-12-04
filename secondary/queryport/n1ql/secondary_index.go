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

package couchbase

import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/collatejson"
import qclient "github.com/couchbase/indexing/secondary/queryport/client"
import "github.com/couchbaselabs/query/datastore"
import "github.com/couchbaselabs/query/errors"
import "github.com/couchbaselabs/query/expression"
import "github.com/couchbaselabs/query/expression/parser"
import "github.com/couchbaselabs/query/value"

// ErrorIndexEmpty is index not initialized.
var ErrorIndexEmpty = errors.NewError(nil, "secondaryIndex.empty")

// ErrorEmptyHost is no valid node hosting an index.
var ErrorEmptyHost = errors.NewError(nil, "secondaryIndex.emptyHost")

// ErrorEmptyStatistics is index-statistics not available.
var ErrorEmptyStatistics = errors.NewError(nil, "secondaryIndex.emptyStatistics")

// PRIMARY_INDEX index name.
var PRIMARY_INDEX = "#primary"

// secondaryIndex to hold meta data information, network-address for
// a single secondary-index.
type secondaryIndex struct {
	name      string // name of the index
	defnID    string
	bucketn   string
	isPrimary bool
	using     datastore.IndexType
	partnExpr string
	secExprs  []string
	whereExpr string
	state     datastore.IndexState

	// remote node hosting this index.
	hosts       []string
	hostClients []*qclient.Client
}

// TODO: keep upto date with couchbase/indexing/secondary/indexer pkg.
var twoiInclusion = map[datastore.Inclusion]qclient.Inclusion{
	datastore.NEITHER: qclient.Neither,
	datastore.LOW:     qclient.Low,
	datastore.HIGH:    qclient.High,
	datastore.BOTH:    qclient.Both,
}

func (si *secondaryIndex) getHostClient() (*qclient.Client, errors.Error) {
	if si.hostClients == nil || len(si.hostClients) == 0 {
		return nil, ErrorEmptyHost
	}
	// TODO: use round-robin or other statistical heuristics to load balance.
	client := si.hostClients[0]
	return client, nil
}

// KeyspaceId implement Index{} interface.
func (si *secondaryIndex) KeyspaceId() string {
	return si.bucketn
}

// Id implement Index{} interface.
func (si *secondaryIndex) Id() string {
	return si.Name()
}

// Name implement Index{} interface.
func (si *secondaryIndex) Name() string {
	return si.name
}

// Type implement Index{} interface.
func (si *secondaryIndex) Type() datastore.IndexType {
	return si.using
}

// IsPrimary implement Index{} interface.
func (si *secondaryIndex) IsPrimary() bool {
	return false
}

// EqualKey implement Index{} interface.
func (si *secondaryIndex) EqualKey() expression.Expressions {
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

	client, err := si.getHostClient()
	if err != nil {
		return nil, err
	}

	var pstats c.IndexStatistics
	var e error

	indexn, bucketn := si.name, si.bucketn
	if span.Equal != nil {
		equal := values2SKey(span.Equal)
		pstats, e = client.LookupStatistics(indexn, bucketn, equal)

	} else {
		low := values2SKey(span.Range.Low)
		high := values2SKey(span.Range.High)
		incl := twoiInclusion[span.Range.Inclusion]
		pstats, e = client.RangeStatistics(indexn, bucketn, low, high, incl)
	}
	if e != nil {
		return nil, errors.NewError(nil, e.Error())
	}
	return newStatistics(pstats), nil
}

// Drop implement Index{} interface.
func (si *secondaryIndex) Drop() errors.Error {
	if si == nil {
		return ErrorIndexEmpty
	}
	client := qclient.NewClusterClient(ClusterManagerAddr)
	err := client.DropIndex(si.defnID)
	if err != nil {
		return errors.NewError(nil, err.Error())
	}
	// TODO: sync with cluster-manager ?
	// TODO: assume this index is already deleted from keyspace.
	return nil
}

// Scan implement Index{} interface.
func (si *secondaryIndex) Scan(
	span *datastore.Span, distinct bool, limit int64,
	conn *datastore.IndexConnection) {

	entryChannel := conn.EntryChannel()
	defer close(entryChannel)

	client, err := si.getHostClient()
	if err != nil {
		return
	}

	indexn, bucketn := si.name, si.bucketn
	if span.Equal != nil {
		equal := values2SKey(span.Equal)
		client.Lookup(
			indexn, bucketn, []c.SecondaryKey{equal}, distinct, limit,
			makeResponsehandler(conn))

	} else {
		low, high := values2SKey(span.Range.Low), values2SKey(span.Range.High)
		incl := twoiInclusion[span.Range.Inclusion]
		client.Range(
			indexn, bucketn, low, high, incl, distinct, limit,
			makeResponsehandler(conn))
	}
}

// Scan implement PrimaryIndex{} interface.
func (si *secondaryIndex) ScanEntries(
	limit int64, conn *datastore.IndexConnection) {

	entryChannel := conn.EntryChannel()
	defer close(entryChannel)

	client, err := si.getHostClient()
	if err != nil {
		return
	}

	indexn, bucketn := si.name, si.bucketn
	client.ScanAll(indexn, bucketn, limit, makeResponsehandler(conn))
}

func makeResponsehandler(
	conn *datastore.IndexConnection) qclient.ResponseHandler {

	entryChannel := conn.EntryChannel()
	stopChannel := conn.StopChannel()

	return func(data qclient.ResponseReader) bool {
		if err := data.Error(); err != nil {
			conn.Error(errors.NewError(nil, err.Error()))
			return false

		} else if skeys, pkeys, err := data.GetEntries(); err != nil {
			conn.Error(errors.NewError(nil, err.Error()))
			return false

		} else {
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
		return false
	}
}

type statistics struct {
	count      int64
	uniqueKeys int64
	min        value.Values
	max        value.Values
}

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

// local function that can be used to asynchronously update
// meta-data information, host network-address from coordinator
// notifications.

// create a queryport client connected to `host`.
func (si *secondaryIndex) setHost(hosts []string) {
	si.hosts = hosts
	config := c.SystemConfig.SectionConfig("queryport.client.", true)
	if len(hosts) > 0 {
		si.hostClients = make([]*qclient.Client, 0, len(hosts))
		for _, host := range hosts {
			c := qclient.NewClient(qclient.Remoteaddr(host), config)
			si.hostClients = append(si.hostClients, c)
		}
	}
}

// shape of key passed to scan-coordinator (indexer node) is,
//      [key1, key2, ... keyN]
// where N expressions supplied in CREATE INDEX
// to evaluate secondary-key.
func values2SKey(vals value.Values) c.SecondaryKey {
	if vals == nil {
		return nil
	}
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

// ClusterManagerAddr is temporary hard-coded address for cluster-manager-agent
const ClusterManagerAddr = "localhost:9101"

// IndexerAddr is temporary hard-coded address for indexer node.
const IndexerAddr = "localhost:7000"

// Load2iIndexes and remember them as part of keyspace.indexes.
// TODO: pointer to interface is double indirection, have no clue.
func Load2iIndexes(bucketn string) ([]*datastore.Index, error) {
	indexes := make([]*datastore.Index, 0)
	client := qclient.NewClusterClient(ClusterManagerAddr)
	infos, err := client.List()
	if err != nil {
		return nil, err
	} else if infos == nil { // empty list of indexes
		return nil, nil
	}

	var index datastore.Index

	for _, info := range infos {
		if info.Bucket != bucketn {
			continue
		}
		using := datastore.IndexType(info.Using)
		if info.Name == "#primary" {
			index, err = new2iPrimaryIndex(using, &info)
			if err != nil {
				return nil, err
			}

		} else {
			index, err = new2iIndex(&info)
			if err != nil {
				return nil, err
			}
		}
		indexes = append(indexes, &index)
	}
	return indexes, nil
}

// Create2iPrimaryIndex will create a new primary index for `keyspace`.
func Create2iPrimaryIndex(
	bucketn string, using datastore.IndexType) (*secondaryIndex, errors.Error) {

	client := qclient.NewClusterClient(ClusterManagerAddr)
	// update meta-data.
	info, err := client.CreateIndex(
		PRIMARY_INDEX, bucketn, string(using), "N1QL", "", "", nil, true)
	if err != nil {
		return nil, errors.NewError(err, " Primary CreateIndex() with 2i failed")
	} else if info == nil {
		return nil, errors.NewError(nil, " primary CreateIndex() with 2i failed")
	}
	// TODO: make another call to cluster-manager for topology information,
	// so that info will contain the nodes that host this index.
	return new2iPrimaryIndex(using, info)
}

// new2iPrimaryIndex will create a new instance of primary index.
func new2iPrimaryIndex(
	using datastore.IndexType,
	info *qclient.IndexInfo) (*secondaryIndex, errors.Error) {

	index := &secondaryIndex{
		name:      PRIMARY_INDEX,
		defnID:    info.DefnID,
		bucketn:   info.Bucket,
		isPrimary: true,
		using:     datastore.LSM,
		// remote node hosting this index.
		hosts: nil, // to becomputed by coordinator
	}
	// TODO: info will contain the nodes that host this index.
	index.setHost([]string{IndexerAddr})
	return index, nil
}

// Create2iIndex will create a new index for `keyspace`.
func Create2iIndex(
	name, bucketn string,
	equalKey, rangeKey expression.Expressions, where expression.Expression,
	using datastore.IndexType) (*secondaryIndex, errors.Error) {

	var partnStr string
	if equalKey != nil && len(equalKey) > 0 {
		partnStr = expression.NewStringer().Visit(equalKey[0])
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

	client := qclient.NewClusterClient(ClusterManagerAddr)
	info, err := client.CreateIndex(
		name, bucketn, string(using), "N1QL", partnStr, whereStr, secStrs, false)
	if err != nil {
		return nil, errors.NewError(nil, err.Error())
	} else if info == nil {
		return nil, errors.NewError(nil, "2i CreateIndex() failed")
	}
	// TODO: make another call to cluster-manager for topology information.
	// so that info will contain the nodes that host this index.
	return new2iIndex(info)
}

// new 2i index.
func new2iIndex(info *qclient.IndexInfo) (*secondaryIndex, errors.Error) {
	index := &secondaryIndex{
		name:      info.Name,
		defnID:    info.DefnID,
		isPrimary: info.IsPrimary,
		using:     datastore.IndexType(info.Using),
		partnExpr: info.PartnExpr,
		secExprs:  info.SecExprs,
		whereExpr: info.WhereExpr,
		// remote node hosting this index.
		hosts: nil, // to becomputed by coordinator
	}
	// TODO: info will contain the nodes that host this index.
	index.setHost([]string{IndexerAddr})
	return index, nil
}

func parseExprs(exprs []string) (expression.Expressions, error) {
	keys := expression.Expressions(nil)
	if len(exprs) > 0 {
		for _, expr := range exprs {
			if len(expr) > 0 {
				key, err := parser.Parse(expr)
				if err != nil {
					return nil, err
				}
				keys = append(keys, key)
			}
		}
	}
	return keys, nil
}
