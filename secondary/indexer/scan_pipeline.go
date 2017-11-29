// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package indexer

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/couchbase/indexing/secondary/collatejson"
	c "github.com/couchbase/indexing/secondary/common"
	l "github.com/couchbase/indexing/secondary/logging"
	p "github.com/couchbase/indexing/secondary/pipeline"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"
)

var (
	ErrLimitReached = errors.New("Row limit reached")
)

type ScanPipeline struct {
	src    p.Source
	object p.Pipeline
	req    *ScanRequest
	config c.Config

	aggrRes *aggrResult

	rowsReturned uint64
	bytesRead    uint64
}

func (p *ScanPipeline) Cancel(err error) {
	p.src.Shutdown(err)
}

func (p *ScanPipeline) Execute() error {
	return p.object.Execute()
}

func (p ScanPipeline) RowsReturned() uint64 {
	return p.rowsReturned
}

func (p ScanPipeline) BytesRead() uint64 {
	return p.bytesRead
}

func NewScanPipeline(req *ScanRequest, w ScanResponseWriter, is IndexSnapshot, cfg c.Config) *ScanPipeline {
	scanPipeline := new(ScanPipeline)
	scanPipeline.req = req
	scanPipeline.config = cfg

	src := &IndexScanSource{is: is, p: scanPipeline}
	src.InitWriter()
	dec := &IndexScanDecoder{p: scanPipeline}
	dec.InitReadWriter()
	wr := &IndexScanWriter{w: w, p: scanPipeline}
	wr.InitReader()

	dec.SetSource(src)
	wr.SetSource(dec)

	scanPipeline.src = src
	scanPipeline.object.AddSource("source", src)
	scanPipeline.object.AddFilter("decoder", dec)
	scanPipeline.object.AddSink("writer", wr)

	if req.GroupAggr != nil {
		scanPipeline.aggrRes = &aggrResult{}
	}

	return scanPipeline

}

type IndexScanSource struct {
	p.ItemWriter
	is IndexSnapshot
	p  *ScanPipeline
}

type IndexScanDecoder struct {
	p.ItemReadWriter
	p *ScanPipeline
}

type IndexScanWriter struct {
	p.ItemReader
	w ScanResponseWriter
	p *ScanPipeline
}

func (s *IndexScanSource) Routine() error {
	var err error
	defer s.CloseWrite()

	r := s.p.req
	var currentScan Scan
	currOffset := int64(0)
	count := 1
	checkDistinct := r.Distinct && !r.isPrimary

	buf := secKeyBufPool.Get()
	r.keyBufList = append(r.keyBufList, buf)
	buf2 := secKeyBufPool.Get()
	r.keyBufList = append(r.keyBufList, buf2)
	previousRow := (*buf2)[:0]
	docidbuf := make([]byte, 1024)
	revbuf := secKeyBufPool.Get()
	r.keyBufList = append(r.keyBufList, revbuf)

	iterCount := 0
	fn := func(entry []byte) error {
		if iterCount%SCAN_ROLLBACK_ERROR_BATCHSIZE == 0 && r.hasRollback != nil && r.hasRollback.Load() == true {
			return ErrIndexRollback
		}
		iterCount++

		skipRow := false
		var ck [][]byte

		//get the key in original format
		if s.p.req.IndexInst.Defn.Desc != nil {
			revbuf := (*revbuf)[:0]
			//copy is required, otherwise storage may get updated if storage
			//returns pointer to original item(e.g. memdb)
			revbuf = append(revbuf, entry...)
			jsonEncoder.ReverseCollate(revbuf, s.p.req.IndexInst.Defn.Desc)
			entry = revbuf
		}

		if currentScan.ScanType == FilterRangeReq {
			if len(entry) > cap(*buf) {
				*buf = make([]byte, 0, len(entry)+1024)
			}
			skipRow, ck, err = filterScanRow(entry, currentScan, (*buf)[:0])
			if err != nil {
				return err
			}
		}

		if skipRow {
			return nil
		}

		if r.GroupAggr != nil {
			r.GroupAggr.cv = value.NewScopeValue(make(map[string]interface{}), nil)
			r.GroupAggr.av = value.NewAnnotatedValue(r.GroupAggr.cv)
			r.GroupAggr.exprContext = expression.NewIndexContext()

			if ck == nil && len(entry) > cap(*buf) {
				*buf = make([]byte, 0, len(entry)+1024)
			}

			var docid []byte
			if r.GroupAggr.DependsOnPrimaryKey {
				docid, err = secondaryIndexEntry(entry).ReadDocId((docidbuf)[:0]) //docid for N1QLExpr evaluation for Group/Aggr
				if err != nil {
					return err
				}
			}

			err = computeGroupAggr(ck, docid, entry, (*buf)[:0], s.p.aggrRes, r.GroupAggr)
			if err != nil {
				return err
			}
			l.Infof("ScanPipeline::computeGroupAggr %v", s.p.aggrRes)
		}

		if !r.isPrimary && r.Indexprojection != nil && r.Indexprojection.projectSecKeys {
			if ck == nil && len(entry) > cap(*buf) {
				*buf = make([]byte, 0, len(entry)+1024)
			}

			if r.GroupAggr != nil {
				entry, err = projectGroupAggr((*buf)[:0], r.Indexprojection, s.p.aggrRes)
				if entry == nil {
					return nil
				}
			} else {
				entry, err = projectKeys(ck, entry, (*buf)[:0], r.Indexprojection)
			}
			if err != nil {
				return err
			}
		}

		if checkDistinct {
			if len(previousRow) != 0 && distinctCompare(entry, previousRow) {
				return nil // Ignore the entry as it is same as previous entry
			}
		}

		if !r.isPrimary {
			e := secondaryIndexEntry(entry)
			count = e.Count()
		}

		if r.GroupAggr != nil {
			//TODO fix this for array index support
			count = 1
		}

		for i := 0; i < count; i++ {
			if r.Distinct && i > 0 {
				break
			}
			if currOffset >= r.Offset {
				s.p.rowsReturned++
				wrErr := s.WriteItem(entry)
				if wrErr != nil {
					return wrErr
				}
				if s.p.rowsReturned == uint64(r.Limit) {
					return ErrLimitReached
				}
			} else {
				currOffset++
			}
		}

		if checkDistinct {
			previousRow = append(previousRow[:0], entry...)
		}

		return nil
	}

	sliceSnapshots, err1 := GetSliceSnapshots(s.is, s.p.req.PartitionIds)
	if err1 != nil {
		return err1
	}

	//TODO add to config
	if r.GroupAggr != nil {
		if r.GroupAggr.IsLeadingGroup {
			s.p.aggrRes.SetMaxRows(1)
		} else {
			s.p.aggrRes.SetMaxRows(10)
		}
	}

loop:
	for _, scan := range r.Scans {
		currentScan = scan
		err = scatter(r, scan, sliceSnapshots, fn, s.p.config)
		switch err {
		case nil:
		case p.ErrSupervisorKill, ErrLimitReached:
			break loop
		default:
			s.CloseWithError(err)
			break loop
		}
	}

	if r.GroupAggr != nil {

		for _, r := range s.p.aggrRes.rows {
			r.SetFlush(true)
		}

		for {
			entry, err := projectGroupAggr((*buf)[:0], r.Indexprojection, s.p.aggrRes)
			if entry == nil {
				return nil
			}

			if err != nil {
				return err
			}

			wrErr := s.WriteItem(entry)
			if wrErr != nil {
				return wrErr
			}
		}

	}

	return nil
}

func (d *IndexScanDecoder) Routine() error {
	defer d.CloseWrite()
	defer d.CloseRead()

	var sk, docid []byte
	tmpBuf := p.GetBlock()
	defer p.PutBlock(tmpBuf)

loop:
	for {
		row, err := d.ReadItem()
		switch err {
		case nil:
		case p.ErrNoMoreItem, p.ErrSupervisorKill:
			break loop
		default:
			d.CloseWithError(err)
			break loop
		}

		if len(row)*3 > cap(*tmpBuf) {
			(*tmpBuf) = make([]byte, len(row)*3, len(row)*3)
		}

		t := (*tmpBuf)[:0]
		if d.p.req.isPrimary {
			sk, docid = piSplitEntry(row, t)
		} else if d.p.req.GroupAggr != nil {
			codec := collatejson.NewCodec(16)
			sk, _ = codec.Decode(row, t)
		} else {
			sk, docid, _ = siSplitEntry(row, t)
		}

		d.p.bytesRead += uint64(len(sk) + len(docid))
		if !d.p.req.isPrimary && !d.p.req.projectPrimaryKey {
			docid = nil
		}
		err = d.WriteItem(sk, docid)
		if err != nil {
			break // TODO: Old code. Should it be ClosedWithError?
		}
	}

	return nil
}

func (d *IndexScanWriter) Routine() error {
	var err error
	var sk, pk []byte

	defer func() {
		// Send error to the client if not client requested cancel.
		if err != nil && err.Error() != c.ErrClientCancel.Error() {
			d.w.Error(err)
		}
		d.CloseRead()
	}()

loop:
	for {
		sk, err = d.ReadItem()
		switch err {
		case nil:
		case p.ErrNoMoreItem:
			err = nil
			break loop
		default:
			break loop
		}

		pk, err = d.ReadItem()
		if err != nil {
			return err
		}

		if err = d.w.Row(pk, sk); err != nil {
			return err
		}

		/*
		   TODO(sarath): Use block chunk send protocol
		   Instead of collecting rows and encoding into protobuf,
		   we can send full 16kb block.

		       b, err := d.PeekBlock()
		       if err == p.ErrNoMoreItem {
		           d.CloseRead()
		           return nil
		       }

		       d.W.RawBytes(b)
		       d.FlushBlock()
		*/
	}

	return err
}

func piSplitEntry(entry []byte, tmp []byte) ([]byte, []byte) {
	e := primaryIndexEntry(entry)
	sk, err := e.ReadSecKey(tmp)
	c.CrashOnError(err)
	docid, err := e.ReadDocId(sk)
	return sk, docid[len(sk):]
}

func siSplitEntry(entry []byte, tmp []byte) ([]byte, []byte, int) {
	e := secondaryIndexEntry(entry)
	sk, err := e.ReadSecKey(tmp)
	c.CrashOnError(err)
	docid, err := e.ReadDocId(sk)
	c.CrashOnError(err)
	count := e.Count()
	return sk, docid[len(sk):], count
}

// Return true if the row needs to be skipped based on the filter
func filterScanRow(key []byte, scan Scan, buf []byte) (bool, [][]byte, error) {
	codec := collatejson.NewCodec(16)
	compositekeys, err := codec.ExplodeArray(key, buf)
	if err != nil {
		return false, nil, err
	}

	var filtermatch bool
	for _, filtercollection := range scan.Filters {
		if len(filtercollection.CompositeFilters) > len(compositekeys) {
			// There cannot be more ranges than number of composite keys
			err = errors.New("There are more ranges than number of composite elements in the index")
			return false, nil, err
		}
		filtermatch = applyFilter(compositekeys, filtercollection.CompositeFilters)
		if filtermatch {
			return false, compositekeys, nil
		}
	}

	return true, compositekeys, nil
}

// Return true if filter matches the composite keys
func applyFilter(compositekeys [][]byte, compositefilters []CompositeElementFilter) bool {

	for i, filter := range compositefilters {
		ck := compositekeys[i]
		checkLow := (filter.Low != MinIndexKey)
		checkHigh := (filter.High != MaxIndexKey)

		switch filter.Inclusion {
		case Neither:
			// if ck > low and ck < high
			if checkLow {
				if !(bytes.Compare(ck, filter.Low.Bytes()) > 0) {
					return false
				}
			}
			if checkHigh {
				if !(bytes.Compare(ck, filter.High.Bytes()) < 0) {
					return false
				}
			}
		case Low:
			// if ck >= low and ck < high
			if checkLow {
				if !(bytes.Compare(ck, filter.Low.Bytes()) >= 0) {
					return false
				}
			}
			if checkHigh {
				if !(bytes.Compare(ck, filter.High.Bytes()) < 0) {
					return false
				}
			}
		case High:
			// if ck > low and ck <= high
			if checkLow {
				if !(bytes.Compare(ck, filter.Low.Bytes()) > 0) {
					return false
				}
			}
			if checkHigh {
				if !(bytes.Compare(ck, filter.High.Bytes()) <= 0) {
					return false
				}
			}
		case Both:
			// if ck >= low and ck <= high
			if checkLow {
				if !(bytes.Compare(ck, filter.Low.Bytes()) >= 0) {
					return false
				}
			}
			if checkHigh {
				if !(bytes.Compare(ck, filter.High.Bytes()) <= 0) {
					return false
				}
			}
		}
	}

	return true
}

// Compare secondary entries and return true
// if the secondary keys of entries are equal
func distinctCompare(entryBytes1, entryBytes2 []byte) bool {
	entry1 := secondaryIndexEntry(entryBytes1)
	entry2 := secondaryIndexEntry(entryBytes2)
	if bytes.Compare(entryBytes1[:entry1.lenKey()], entryBytes2[:entry2.lenKey()]) == 0 {
		return true
	}
	return false
}

func projectKeys(compositekeys [][]byte, key, buf []byte, projection *Projection) ([]byte, error) {
	var err error

	if projection.entryKeysEmpty {
		entry := secondaryIndexEntry(key)
		buf = append(buf, key[entry.lenKey():]...)
		return buf, nil
	}

	codec := collatejson.NewCodec(16)
	if compositekeys == nil {
		compositekeys, err = codec.ExplodeArray(key, buf)
		if err != nil {
			return nil, err
		}
	}

	var keysToJoin [][]byte
	for i, projectKey := range projection.projectionKeys {
		if projectKey {
			keysToJoin = append(keysToJoin, compositekeys[i])
		}
	}
	// Note: Reusing the same buf used for Explode in JoinArray as well
	// This is because we always project in order and hence avoiding two
	// different buffers for Explode and Join
	if buf, err = codec.JoinArray(keysToJoin, buf); err != nil {
		return nil, err
	}

	entry := secondaryIndexEntry(key)
	buf = append(buf, key[entry.lenKey():]...)
	return buf, nil
}

func projectLeadingKey(compositekeys [][]byte, key []byte, buf *[]byte) ([]byte, error) {
	var err error

	codec := collatejson.NewCodec(16)
	if compositekeys == nil {
		if len(key) > cap(*buf) {
			*buf = make([]byte, 0, len(key)+RESIZE_PAD)
		}
		compositekeys, err = codec.ExplodeArray(key, (*buf)[:0])
		if err != nil {
			return nil, err
		}
	}

	var keysToJoin [][]byte
	keysToJoin = append(keysToJoin, compositekeys[0])
	if *buf, err = codec.JoinArray(keysToJoin, (*buf)[:0]); err != nil {
		return nil, err
	}

	entry := secondaryIndexEntry(key)
	*buf = append(*buf, key[entry.lenKey():]...)
	return *buf, nil
}

/////////////////////////////////////////////////////////////////////////
//
// group by/aggregate implementation
//
/////////////////////////////////////////////////////////////////////////

type groupKey struct {
	key       []byte
	projectId int32
}

type aggrVal struct {
	fn        c.AggrFunc
	projectId int32
}

type aggrRow struct {
	groups []*groupKey
	aggrs  []*aggrVal
	flush  bool
}

type aggrResult struct {
	rows    []*aggrRow
	partial bool
	maxRows int
}

func (g groupKey) String() string {
	return fmt.Sprintf("%v", g.key)
}

func (a aggrVal) String() string {
	return fmt.Sprintf("%v", a.fn)
}

func (a aggrRow) String() string {
	return fmt.Sprintf("group %v aggrs %v flush %v", a.groups, a.aggrs, a.flush)
}

func (a aggrResult) String() string {
	var res string
	for i, r := range a.rows {
		res += fmt.Sprintf("Row %v %v\n", i, r)
	}
	return res
}

func computeGroupAggr(compositekeys [][]byte, docid []byte, key,
	buf []byte, aggrRes *aggrResult, groupAggr *GroupAggr) error {

	var err error
	codec := collatejson.NewCodec(16)
	if compositekeys == nil {
		compositekeys, err = codec.ExplodeArray(key, buf)
		if err != nil {
			return err
		}
	}

	groups := make([]*groupKey, len(groupAggr.Group))
	aggrs := make([]*aggrVal, len(groupAggr.Aggrs))

	for i, gk := range groupAggr.Group {
		g, err := computeGroupKey(groupAggr, gk, compositekeys, docid)
		if err != nil {
			return err
		}
		groups[i] = g
	}

	for i, ak := range groupAggr.Aggrs {
		a, err := computeAggrVal(groupAggr, ak, compositekeys, docid, buf)
		if err != nil {
			return err
		}
		aggrs[i] = a
	}

	aggrRes.AddNewGroup(groups, aggrs)
	return nil

}

func computeGroupKey(groupAggr *GroupAggr, gk *GroupKey, compositekeys [][]byte, docid []byte) (*groupKey, error) {
	var g *groupKey
	if gk.KeyPos >= 0 {
		newKey := make([]byte, len(compositekeys[gk.KeyPos]))
		copy(newKey, compositekeys[gk.KeyPos])
		g = &groupKey{key: newKey,
			projectId: gk.EntryKeyId,
		}
	} else {
		scalar, err := evaluateN1QLExpresssion(groupAggr, gk.Expr, compositekeys, docid)
		if err != nil {
			return nil, err
		}

		// TODO: MB-27049 - Encoding not needed here
		codec := collatejson.NewCodec(16)
		encodeBuf := make([]byte, 1024) // TODO MB-27049 fix buffer size and avoid garbage
		encoded, err := codec.EncodeN1QLValue(scalar, encodeBuf[:0])
		if err != nil {
			return nil, err
		}
		g = &groupKey{key: encoded,
			projectId: gk.EntryKeyId,
		}
	}
	return g, nil
}

func computeAggrVal(groupAggr *GroupAggr, ak *Aggregate,
	compositekeys [][]byte, docid []byte, buf []byte) (*aggrVal, error) {

	var a *aggrVal
	if ak.KeyPos >= 0 {
		actualVal, err := decodeValue(compositekeys[ak.KeyPos], buf)
		if err != nil {
			return nil, err
		}

		a = &aggrVal{fn: c.NewAggrFunc(ak.AggrFunc, actualVal),
			projectId: ak.EntryKeyId,
		}
	} else {
		//process expr
		scalar, err := evaluateN1QLExpresssion(groupAggr, ak.Expr, compositekeys, docid)
		if err != nil {
			return nil, err
		}
		a = &aggrVal{fn: c.NewAggrFunc(ak.AggrFunc, scalar.ActualForIndex()),
			projectId: ak.EntryKeyId,
		}
	}
	return a, nil

}

func evaluateN1QLExpresssion(groupAggr *GroupAggr, expr expression.Expression,
	compositekeys [][]byte, docid []byte) (value.Value, error) {

	for i, ik := range groupAggr.DependsOnIndexKeys {
		if int(ik) == len(compositekeys) {
			groupAggr.av.SetCover(groupAggr.IndexKeyNames[i], value.NewValue(string(docid)))
		} else {
			buf := make([]byte, len(compositekeys[ik])*3) // TODO: MB-27049 avoid garbage
			actualVal, err := decodeValue(compositekeys[ik], buf)
			if err != nil {
				return nil, err
			}
			groupAggr.av.SetCover(groupAggr.IndexKeyNames[i], value.NewValue(actualVal))
		}
	}
	scalar, _, err := expr.EvaluateForIndex(groupAggr.av, groupAggr.exprContext) // TODO: Ignore vector for now
	if err != nil {
		return nil, err
	}
	return scalar, nil
}

func (ar *aggrResult) AddNewGroup(groups []*groupKey, aggrs []*aggrVal) error {

	var err error

	nomatch := true
	for _, row := range ar.rows {
		if row.CheckEqualGroup(groups) {
			nomatch = false
			l.Infof("ScanPipeline::AddNewGroup Add to Same Group %v", row)
			err = row.AddAggregate(aggrs)
			if err != nil {
				return err
			}
			break
		}
	}

	if nomatch {
		newRow := &aggrRow{groups: groups,
			aggrs: make([]*aggrVal, len(aggrs))}
		newRow.AddAggregate(aggrs)

		l.Infof("ScanPipeline::AddNewGroup Add New Group %v", newRow)

		//flush the first row
		if len(ar.rows) >= ar.maxRows {
			ar.rows[0].SetFlush(true)
		}
		ar.rows = append(ar.rows, newRow)
	}

	return nil

}

func (a *aggrResult) SetMaxRows(n int) {
	a.maxRows = n
}

func (ar *aggrRow) CheckEqualGroup(groups []*groupKey) bool {

	if len(ar.groups) != len(groups) {
		return false
	}

	for i, gk := range ar.groups {
		if !gk.Equals(groups[i]) {
			return false
		}
	}

	return true
}

func (ar *aggrRow) AddAggregate(aggrs []*aggrVal) error {

	for i, agg := range aggrs {
		if ar.aggrs[i] == nil {
			ar.aggrs[i] = agg
		} else {
			ar.aggrs[i].fn.AddDelta(agg.fn.Value())
		}
	}
	return nil
}

func (ar *aggrRow) SetFlush(f bool) {
	ar.flush = f
	return
}

func (ar *aggrRow) Flush() bool {
	return ar.flush
}

func (gk *groupKey) Equals(ok *groupKey) bool {

	return bytes.Equal(gk.key, ok.key)
}

func projectGroupAggr(buf []byte, projection *Projection, aggrRes *aggrResult) ([]byte, error) {
	var err error

	var row *aggrRow

	for i, r := range aggrRes.rows {
		if r.Flush() {
			row = r
			//TODO - mark the flushed row and discard in one go
			aggrRes.rows = append(aggrRes.rows[:i], aggrRes.rows[i+1:]...)
			break
		}
	}

	if row == nil {
		return nil, nil
	}

	var keysToJoin [][]byte
	for _, projGroup := range projection.projectGroupKeys {
		if projGroup.grpKey {
			keysToJoin = append(keysToJoin, row.groups[projGroup.pos].key)
		} else {
			val, err := encodeValue(row.aggrs[projGroup.pos].fn.Value())
			if err != nil {
				l.Infof("ScanPipeline::projectGroupAggr encodeValue error %v", err)
				return nil, err
			}
			keysToJoin = append(keysToJoin, val)
		}
	}

	codec := collatejson.NewCodec(16)
	if buf, err = codec.JoinArray(keysToJoin, buf); err != nil {
		l.Infof("ScanPipeline::projectGroupAggr join array error %v", err)
		return nil, err
	}

	return buf, nil
}

//TODO: Scan pipeline decodes the values twice. Optimize to single decode.
func decodeValue(raw []byte, buf []byte) (interface{}, error) {

	var actualVal interface{}

	//decode the aggr val
	codec := collatejson.NewCodec(16)
	dval, err := codec.Decode(raw, buf)
	if err != nil {
		return nil, err
	}

	//json unmarshal to go type
	err = json.Unmarshal(dval, &actualVal)
	if err != nil {
		return nil, err
	}
	return actualVal, nil
}

func encodeValue(raw interface{}) ([]byte, error) {

	jsonraw, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}

	encbuf := make([]byte, 3*len(jsonraw)+collatejson.MinBufferSize)
	codec := collatejson.NewCodec(16)
	encval, err := codec.Encode(jsonraw, encbuf)
	if err != nil {
		return nil, err
	}

	return encval, nil
}
