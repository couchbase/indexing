// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/couchbase/indexing/secondary/collatejson"
	c "github.com/couchbase/indexing/secondary/common"
	l "github.com/couchbase/indexing/secondary/logging"
	p "github.com/couchbase/indexing/secondary/pipeline"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"
)

var (
	ErrLimitReached = errors.New("Row limit reached")
	encodedNull     = []byte{2, 0}
	encodedZero     = []byte{5, 48, 0}
)

type ScanPipeline struct {
	src    p.Source
	object p.Pipeline
	req    *ScanRequest
	config c.Config

	aggrRes         *aggrResult
	stopAggregation bool

	rowsReturned  uint64
	bytesRead     uint64
	rowsScanned   uint64
	rowsFiltered  uint64
	rowsReranked  uint64
	cacheHitRatio int
	exprEvalDur   time.Duration
	exprEvalNum   int64

	//vector index specific
	decodeDur  int64
	decodeCnt  int64
	distCmpDur int64
	distCmpCnt int64
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

func (p ScanPipeline) RowsScanned() uint64 {
	return p.rowsScanned
}

func (p ScanPipeline) RowsFiltered() uint64 {
	return p.rowsFiltered
}

func (p ScanPipeline) RowsReranked() uint64 {
	return p.rowsReranked
}

func (p ScanPipeline) CacheHitRatio() int {
	return p.cacheHitRatio
}

func (p ScanPipeline) AvgExprEvalDur() time.Duration {

	if p.exprEvalNum != 0 {
		return time.Duration(int64(p.exprEvalDur) / p.exprEvalNum)
	}
	return time.Duration(0)
}

// vector specific
func (p ScanPipeline) AvgDecodeDur() time.Duration {

	if p.decodeCnt != 0 {
		return time.Duration(p.decodeDur / p.decodeCnt)
	}
	return time.Duration(0)
}

// vector specific
func (p ScanPipeline) AvgDistCmpDur() time.Duration {

	if p.distCmpCnt != 0 {
		return time.Duration(p.distCmpDur / p.distCmpCnt)
	}
	return time.Duration(0)
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

	defer func() {
		if r := recover(); r != nil {
			l.Fatalf("IndexScanSource - panic detected while processing %s", s.p.req)
			l.Fatalf("%s", l.StackTraceAll())
			panic(r)
		}
	}()

	var err error
	defer s.CloseWrite()

	r := s.p.req
	var currentScan Scan
	currOffset := int64(0)
	count := 1
	checkDistinct := r.Distinct && !r.isPrimary

	var buf, buf2, revbuf *[]byte
	var previousRow, docidbuf []byte
	var cktmp [][]byte
	var cachedEntry entryCache
	var dktmp value.Values

	initTempBuf := func() {
		buf = secKeyBufPool.Get() //Composite element filtering
		r.keyBufList = append(r.keyBufList, buf)
		cktmp = make([][]byte, len(s.p.req.IndexInst.Defn.SecExprs))
	}

	if checkDistinct {
		buf2 = secKeyBufPool.Get() //Tracking for distinct
		r.keyBufList = append(r.keyBufList, buf2)
		previousRow = (*buf2)[:0]
	}

	hasDesc := s.p.req.IndexInst.Defn.HasDescending()
	if hasDesc {
		revbuf = secKeyBufPool.Get() //Reverse collation buffer
		r.keyBufList = append(r.keyBufList, revbuf)
	}

	if r.GroupAggr != nil {
		r.GroupAggr.groups = make([]*groupKey, len(r.GroupAggr.Group))
		for i, _ := range r.GroupAggr.Group {
			r.GroupAggr.groups[i] = new(groupKey)
		}

		r.GroupAggr.aggrs = make([]*aggrVal, len(r.GroupAggr.Aggrs))
		for i, _ := range r.GroupAggr.Aggrs {
			r.GroupAggr.aggrs[i] = new(aggrVal)
		}

		if r.GroupAggr.NeedDecode {
			dktmp = make(value.Values, len(s.p.req.IndexInst.Defn.SecExprs))
		}

		if r.GroupAggr.DependsOnPrimaryKey {
			docidbuf = make([]byte, 1024)
		}

	}

	iterCount := 0
	fn := func(entry, val []byte) error {
		if iterCount%SCAN_ROLLBACK_ERROR_BATCHSIZE == 0 && r.hasRollback != nil && r.hasRollback.Load() == true {
			return ErrIndexRollback
		} else if iterCount%SCAN_SHUTDOWN_ERROR_BATCHSIZE == 0 {
			hasErr := s.HasShutdown()
			if hasErr != nil {
				l.Verbosef("Index scan source - Exiting scan as err observed, err: %v", hasErr)
				return hasErr
			}
		}

		iterCount++
		s.p.rowsScanned++

		skipRow := false
		var ck [][]byte
		var dk value.Values

		//get the key in original format
		if hasDesc {
			revbuf := (*revbuf)[:0]
			//copy is required, otherwise storage may get updated if storage
			//returns pointer to original item(e.g. memdb)
			revbuf = append(revbuf, entry...)
			_, err = jsonEncoder.ReverseCollate(revbuf, s.p.req.IndexInst.Defn.Desc)
			if err != nil {
				return err
			}
			entry = revbuf
		}

		if currentScan.ScanType == FilterRangeReq {
			if buf == nil {
				initTempBuf()
			}
			if len(entry) > cap(*buf) {
				*buf = make([]byte, 0, len(entry)+1024)
			}

			skipRow, ck, dk, err = filterScanRow2(entry, currentScan,
				(*buf)[:0], cktmp, dktmp, r, &cachedEntry)
			if err != nil {
				return err
			}
		}

		if skipRow {
			return nil
		}

		if !r.isPrimary {
			if r.GroupAggr == nil ||
				(r.GroupAggr != nil && !r.GroupAggr.OnePerPrimaryKey) {
				e := secondaryIndexEntry(entry)
				count = e.Count()
			}
		}

		if r.GroupAggr != nil {

			if buf == nil {
				initTempBuf()
			}

			if ck == nil && len(entry) > cap(*buf) {
				*buf = make([]byte, 0, len(entry)+1024)
			}

			var docid []byte
			if r.isPrimary {
				docid = entry
			} else if r.GroupAggr.DependsOnPrimaryKey {
				docid, err = secondaryIndexEntry(entry).ReadDocId((docidbuf)[:0]) //docid for N1QLExpr evaluation for Group/Aggr
				if err != nil {
					return err
				}
			}

			err = computeGroupAggr(ck, dk, count, docid, entry, (*buf)[:0], s.p.aggrRes, r.GroupAggr, cktmp, dktmp, &cachedEntry, s.p)
			if err != nil {
				return err
			}
			count = 1 //reset count; count is used for aggregates computation
		}

		if r.Indexprojection != nil && r.Indexprojection.projectSecKeys {

			if buf == nil {
				initTempBuf()
			}

			if r.GroupAggr != nil {
				entry, err = projectGroupAggr((*buf)[:0], r.Indexprojection, s.p.aggrRes, r.isPrimary)
				if entry == nil {
					return err
				}
			} else if !r.isPrimary {
				if ck == nil && len(entry) > cap(*buf) {
					*buf = make([]byte, 0, len(entry)+1024)
				}

				entry, err = projectKeys(ck, entry, (*buf)[:0], r, cktmp)
			}
			if err != nil {
				return err
			}
		}

		if checkDistinct {
			if len(previousRow) != 0 && distinctCompare(entry, previousRow, false) {
				return nil // Ignore the entry as it is same as previous entry
			}
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
				if s.p.rowsReturned == uint64(r.Limit) || s.p.stopAggregation {
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

	if r.GroupAggr != nil {
		if r.GroupAggr.IsLeadingGroup {
			s.p.aggrRes.SetMaxRows(1)
		} else {
			s.p.aggrRes.SetMaxRows(s.p.config["scan.partial_group_buffer_size"].Int())
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

	s.p.cacheHitRatio = cachedEntry.CacheHitRatio()

	if r.GroupAggr != nil && err == nil {
		if buf == nil {
			buf = secKeyBufPool.Get()
			r.keyBufList = append(r.keyBufList, buf)
		}

		for _, r := range s.p.aggrRes.rows {
			r.SetFlush(true)
		}

		for {
			entry, err := projectGroupAggr((*buf)[:0], r.Indexprojection, s.p.aggrRes, r.isPrimary)
			if err != nil {
				s.CloseWithError(err)
				break
			}

			if entry == nil {

				if s.p.rowsReturned == 0 {

					//handle special group rules
					entry, err = projectEmptyResult((*buf)[:0], r.Indexprojection, r.GroupAggr)
					if err != nil {
						s.CloseWithError(err)
						break
					}

					if entry == nil {
						return nil
					}

					s.p.rowsReturned++
					wrErr := s.WriteItem(entry)
					if wrErr != nil {
						s.CloseWithError(wrErr)
						break
					}
				}
				return nil
			}

			if err != nil {
				s.CloseWithError(err)
				break
			}

			if currOffset >= r.Offset {
				s.p.rowsReturned++
				wrErr := s.WriteItem(entry)
				if wrErr != nil {
					s.CloseWithError(wrErr)
					break
				}
				if s.p.rowsReturned == uint64(r.Limit) {
					return nil
				}
			} else {
				currOffset++
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

		dataEncFmt := d.p.req.dataEncFmt

		if dataEncFmt == c.DATA_ENC_JSON {
			if len(row)*3 > cap(*tmpBuf) {
				(*tmpBuf) = make([]byte, len(row)*3, len(row)*3)
			}
		}
		t := (*tmpBuf)[:0]

		if d.p.req.GroupAggr != nil {
			if dataEncFmt == c.DATA_ENC_COLLATEJSON {
				sk = row
			} else if dataEncFmt == c.DATA_ENC_JSON {
				sk, err = jsonEncoder.Decode(row, t)
				if err != nil {
					err = fmt.Errorf("Collatejson decode error: %v", err)
					l.Errorf("Error (%v) in Decode for row %v, "+
						"req = %s", err, row, d.p.req)
					d.CloseWithError(err)
					break loop
				}
			} else {
				err = c.ErrUnexpectedDataEncFmt
				d.CloseWithError(err)
				break loop
			}
		} else if d.p.req.isPrimary {
			sk, docid, err = piSplitEntry(row, t)
			if err != nil {
				d.CloseWithError(err)
				break loop
			}
		} else if d.p.req.isBhiveScan {
			if d.p.req.projectVectorDist || d.p.req.Indexprojection.projectInclude {
				sk, docid, err = siSplitEntryCJson(row)
				if err != nil {
					d.CloseWithError(err)
					break loop
				}
			} else {
				// For bhive index, sk would be nil and row would be docid when distance is not projected
				docid = row
			}
		} else {
			if dataEncFmt == c.DATA_ENC_COLLATEJSON {
				sk, docid, err = siSplitEntryCJson(row)
				if err != nil {
					d.CloseWithError(err)
					break loop
				}
			} else if dataEncFmt == c.DATA_ENC_JSON {
				sk, docid, _, err = siSplitEntry(row, t)
				if err != nil {
					l.Errorf("Error (%v) in siSplitEntry for row %v, "+
						"req = %s", err, row, d.p.req)
					d.CloseWithError(err)
					break loop
				}
			} else {
				err = c.ErrUnexpectedDataEncFmt
				d.CloseWithError(err)
				break loop
			}
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

func piSplitEntry(entry []byte, tmp []byte) ([]byte, []byte, error) {
	e := primaryIndexEntry(entry)
	sk, err := e.ReadSecKey(tmp)
	if err != nil {
		return nil, nil, err
	}
	docid, err := e.ReadDocId(sk)
	if err != nil {
		return nil, nil, err
	}
	return sk, docid[len(sk):], nil
}

func siSplitEntry(entry []byte, tmp []byte) ([]byte, []byte, int, error) {
	e := secondaryIndexEntry(entry)
	sk, err := e.ReadSecKey(tmp)
	if err != nil {
		return nil, nil, 0, err
	}
	docid, err := e.ReadDocId(sk)
	if err != nil {
		return nil, nil, 0, err
	}
	count := e.Count()
	return sk, docid[len(sk):], count, nil
}

func siSplitEntryCJson(entry []byte) ([]byte, []byte, error) {
	e := secondaryIndexEntry(entry)
	sk := e.ReadSecKeyCJson()
	docid, err := e.ReadDocId(sk)
	if err != nil {
		return nil, nil, err
	}
	return sk, docid[len(sk):], nil
}

// Return true if the row needs to be skipped based on the filter
func filterScanRow(key []byte, scan Scan, buf []byte) (bool, [][]byte, error) {
	var compositekeys [][]byte
	var err error

	compositekeys, err = jsonEncoder.ExplodeArray(key, buf)
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

// Return true if the row needs to be skipped based on the filter
func filterScanRow2(key []byte, scan Scan, buf []byte, cktmp [][]byte,
	dktmp value.Values, r *ScanRequest, cachedEntry *entryCache) (bool, [][]byte, value.Values, error) {

	var compositekeys [][]byte
	var decodedkeys value.Values
	var err error

	if !r.isPrimary && cachedEntry != nil && cachedEntry.Exists() {
		if cachedEntry.EqualsEntry(key) {
			compositekeys, decodedkeys = cachedEntry.Get()
			cachedEntry.SetValid(true)
		} else {
			cachedEntry.SetValid(false)
		}
	}

	if compositekeys == nil {
		compositekeys, decodedkeys, err = jsonEncoder.ExplodeArray3(key, buf, cktmp, dktmp,
			r.explodePositions, r.decodePositions, r.explodeUpto)
		if err != nil {
			if err == collatejson.ErrorOutputLen {
				newBuf := make([]byte, 0, len(key)*3)
				compositekeys, decodedkeys, err = jsonEncoder.ExplodeArray3(key, newBuf, cktmp, dktmp,
					r.explodePositions, r.decodePositions, r.explodeUpto)
			}
			if err != nil {
				return false, nil, nil, err
			}
		}
	}

	if cachedEntry != nil && !cachedEntry.Exists() {
		cachedEntry.Init(r)
	}
	if cachedEntry != nil && !cachedEntry.Valid() {
		cachedEntry.Update(key, compositekeys, decodedkeys)
	}

	var filtermatch bool
	for _, filtercollection := range scan.Filters {
		if len(filtercollection.CompositeFilters) > len(compositekeys) {
			// There cannot be more ranges than number of composite keys
			err = errors.New("There are more ranges than number of composite elements in the index")
			return false, nil, nil, err
		}
		filtermatch = applyFilter(compositekeys, filtercollection.CompositeFilters)
		if filtermatch {
			return false, compositekeys, decodedkeys, nil
		}
	}

	return true, compositekeys, decodedkeys, nil
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
// if the secondary keys of entries are equal, when compareDocIDs is true compare docIds in addition to keys.
func distinctCompare(entryBytes1, entryBytes2 []byte, compareDocIDs bool) bool {
	entry1 := secondaryIndexEntry(entryBytes1)
	entry2 := secondaryIndexEntry(entryBytes2)
	lenKey1 := entry1.lenKey()
	lenKey2 := entry2.lenKey()
	if compareDocIDs {
		lenKey1 += entry1.lenDocId()
		lenKey2 += entry2.lenDocId()
	}
	if bytes.Compare(entryBytes1[:lenKey1], entryBytes2[:lenKey2]) == 0 {
		return true
	}
	return false
}

func projectKeys(compositekeys [][]byte, key, buf []byte, r *ScanRequest, cktmp [][]byte) ([]byte, error) {
	var err error

	if r.Indexprojection.entryKeysEmpty {
		if r.isBhiveScan && r.projectVectorDist == false {
			return key, nil
		} else {
			entry := secondaryIndexEntry(key)
			buf = append(buf, key[entry.lenKey():]...)
		}
		return buf, nil
	}

	if compositekeys == nil {
		compositekeys, _, err = jsonEncoder.ExplodeArray3(key, buf, cktmp, nil,
			r.explodePositions, nil, r.explodeUpto)
		if err != nil {
			if err == collatejson.ErrorOutputLen {
				newBuf := make([]byte, 0, len(key)*3)
				compositekeys, _, err = jsonEncoder.ExplodeArray3(key, newBuf, cktmp, nil,
					r.explodePositions, nil, r.explodeUpto)
			}
			if err != nil {
				return nil, err
			}
		}
	}

	var keysToJoin [][]byte
	for i, projectKey := range r.Indexprojection.projectionKeys {
		if projectKey {
			keysToJoin = append(keysToJoin, compositekeys[i])
		}
	}
	// Note: Reusing the same buf used for Explode in JoinArray as well
	// This is because we always project in order and hence avoiding two
	// different buffers for Explode and Join
	if buf, err = jsonEncoder.JoinArray(keysToJoin, buf); err != nil {
		return nil, err
	}

	entry := secondaryIndexEntry(key)
	buf = append(buf, key[entry.lenKey():]...)
	return buf, nil
}

func projectLeadingKey(compositekeys [][]byte, key []byte, buf *[]byte) ([]byte, error) {
	var err error

	if compositekeys == nil {
		if len(key) > cap(*buf) {
			*buf = make([]byte, 0, len(key)+RESIZE_PAD)
		}
		compositekeys, err = jsonEncoder.ExplodeArray(key, (*buf)[:0])
		if err != nil {
			return nil, err
		}
	}

	var keysToJoin [][]byte
	keysToJoin = append(keysToJoin, compositekeys[0])
	if *buf, err = jsonEncoder.JoinArray(keysToJoin, (*buf)[:0]); err != nil {
		return nil, err
	}

	entry := secondaryIndexEntry(key)
	*buf = append(*buf, key[entry.lenKey():]...)
	return *buf, nil
}

func projectAllKeys(compositekeys [][]byte, key, include []byte, buf []byte, r *ScanRequest) ([]byte, error) {
	var err error

	entry := secondaryIndexEntry(key)
	docid := key[entry.lenKey():]
	secKey := entry[:entry.lenKey()]

	var keysToJoin [][]byte
	keysToJoin = append(keysToJoin, secKey[1:len(secKey)-1])   // Strip of first and last symbol
	keysToJoin = append(keysToJoin, include[1:len(include)-1]) // Strip of first and last symbol

	// Note: Reusing the same buf used for Explode in JoinArray as well
	// This is because we always project in order and hence avoiding two
	// different buffers for Explode and Join
	if buf, err = jsonEncoder.JoinArray(keysToJoin, buf); err != nil {
		return nil, err
	}

	buf = append(buf, docid...)
	return buf, nil
}

func projectSecKeysAndInclude(compositekeys [][]byte, key, include, buf []byte, includeBuf []byte, r *ScanRequest, cktmp, includecktmp [][]byte) ([]byte, error) {
	var err error

	secKeyExplode := true
	entry := secondaryIndexEntry(key)
	if r.isBhiveScan && r.projectVectorDist == false {
		secKeyExplode = false
	}

	var keysToJoin [][]byte

	if r.Indexprojection.projectSecKeys == false { // project all secondary keys
		secKey := entry[1 : entry.lenKey()-1] // strip first and last symbol
		keysToJoin = append(keysToJoin, secKey)
	} else { // project specific keys as per explodePositions unless projection is disabled for secKeys
		if compositekeys == nil && secKeyExplode {
			compositekeys, _, err = jsonEncoder.ExplodeArray3(key, buf, cktmp, nil,
				r.explodePositions, nil, r.explodeUpto)
			if err != nil {
				if err == collatejson.ErrorOutputLen {
					newBuf := make([]byte, 0, len(key)*3)
					compositekeys, _, err = jsonEncoder.ExplodeArray3(key, newBuf, cktmp, nil,
						r.explodePositions, nil, r.explodeUpto)
				}
				if err != nil {
					return nil, err
				}
			}
		}

		for i, projectKey := range r.Indexprojection.projectionKeys {
			if projectKey {
				keysToJoin = append(keysToJoin, compositekeys[i])
			}
		}
	}

	if r.Indexprojection.projectAllInclude {
		includeval := include[1 : len(include)-1] // strip the first and last symbol
		keysToJoin = append(keysToJoin, includeval)
	} else {
		var includekeys [][]byte

		includekeys, _, err = jsonEncoder.ExplodeArray3(include, includeBuf, includecktmp, nil,
			r.Indexprojection.projectIncludeKeys, nil, len(r.IndexInst.Defn.Include))
		if err != nil {
			if err == collatejson.ErrorOutputLen {
				newBuf := make([]byte, 0, len(key)*3)
				includekeys, _, err = jsonEncoder.ExplodeArray3(include, newBuf, includecktmp, nil,
					r.Indexprojection.projectIncludeKeys, nil, len(r.IndexInst.Defn.Include))
			}
			if err != nil {
				return nil, err
			}
		}

		for i, projectKey := range r.Indexprojection.projectIncludeKeys {
			if projectKey {
				keysToJoin = append(keysToJoin, includekeys[i])
			}
		}
	}

	// Note: Reusing the same buf used for Explode in JoinArray as well
	// This is because we always project in order and hence avoiding two
	// different buffers for Explode and Join
	if buf, err = jsonEncoder.JoinArray(keysToJoin, buf); err != nil {
		return nil, err
	}

	if !secKeyExplode {
		// If for BHIVE scan, distance is not projected, key would be docid as
		// distance is not substituted. Hence, use key directly. Encode the
		// length of key as well when returning the buf to adhere to CJSON format
		buf = append(buf, key...)
		buf = append(buf, []byte{0, 0}...)
		offset := len(buf) - 2
		binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(key)))
	} else {
		buf = append(buf, key[entry.lenKey():]...)
	}
	return buf, nil
}

/////////////////////////////////////////////////////////////////////////
//
// group by/aggregate implementation
//
/////////////////////////////////////////////////////////////////////////

type groupKey struct {
	raw       []byte
	obj       value.Value
	n1qlValue bool

	projectId int32
}

type aggrVal struct {
	fn      c.AggrFunc
	raw     []byte
	obj     value.Value
	decoded interface{}

	typ       c.AggrFuncType
	projectId int32
	distinct  bool
	count     int

	n1qlValue bool
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
	if g.n1qlValue {
		return fmt.Sprintf("%v", g.obj)
	} else {
		return fmt.Sprintf("%v", g.raw)
	}
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

func computeGroupAggr(compositekeys [][]byte, decodedkeys value.Values, count int, docid, key,
	buf []byte, aggrRes *aggrResult, groupAggr *GroupAggr, cktmp [][]byte, dktmp value.Values, cachedEntry *entryCache, p *ScanPipeline) error {

	var err error

	if groupAggr.IsPrimary {
		compositekeys = make([][]byte, 1)
		compositekeys[0] = key
	} else if compositekeys == nil {
		if groupAggr.NeedExplode {
			if cachedEntry.Exists() {
				if cachedEntry.EqualsEntry(key) {
					compositekeys, decodedkeys = cachedEntry.Get()
					cachedEntry.SetValid(true)
				} else {
					cachedEntry.SetValid(false)
				}
			} else {
				cachedEntry.Init(p.req)
			}

			if !cachedEntry.Valid() {
				compositekeys, decodedkeys, err = jsonEncoder.ExplodeArray3(key, buf, cktmp, dktmp,
					p.req.explodePositions, p.req.decodePositions, p.req.explodeUpto)
				if err != nil {
					if err == collatejson.ErrorOutputLen {
						newBuf := make([]byte, 0, len(key)*3)
						compositekeys, decodedkeys, err = jsonEncoder.ExplodeArray3(key, newBuf, cktmp, dktmp,
							p.req.explodePositions, p.req.decodePositions, p.req.explodeUpto)
					}
					if err != nil {
						return err
					}
				}
				cachedEntry.Update(key, compositekeys, decodedkeys)
			}
		}
	}

	// SetCover for Annotated Value
	setCoverForExprEval(groupAggr, decodedkeys, docid)

	for i, gk := range groupAggr.Group {
		err := computeGroupKey(groupAggr, gk, compositekeys, decodedkeys, docid, i, p, cachedEntry.Valid())
		if err != nil {
			return err
		}
	}

	for i, ak := range groupAggr.Aggrs {
		err := computeAggrVal(groupAggr, ak, compositekeys, decodedkeys, docid, count, buf, i, p, cachedEntry.Valid())
		if err != nil {
			return err
		}
	}

	err = aggrRes.AddNewGroup(groupAggr, p, cachedEntry.Valid())
	return err

}

func computeGroupKey(groupAggr *GroupAggr, gk *GroupKey, compositekeys [][]byte,
	decodedkeys value.Values, docid []byte, pos int, p *ScanPipeline, cacheValid bool) error {

	g := groupAggr.groups[pos]
	if gk.KeyPos >= 0 {
		g.raw = compositekeys[gk.KeyPos]
		g.projectId = gk.EntryKeyId

	} else if !cacheValid || groupAggr.DependsOnPrimaryKey {
		var scalar value.Value
		if gk.ExprValue != nil {
			scalar = gk.ExprValue // It is a constant expression
		} else {
			var err error
			scalar, err = evaluateN1QLExpresssion(groupAggr, gk.Expr, decodedkeys, docid, p)
			if err != nil {
				return err
			}
		}

		g.obj = scalar
		g.projectId = gk.EntryKeyId
		g.n1qlValue = true
	}
	return nil
}

func computeAggrVal(groupAggr *GroupAggr, ak *Aggregate,
	compositekeys [][]byte, decodedkeys value.Values, docid []byte,
	count int, buf []byte, pos int, p *ScanPipeline, cacheValid bool) error {

	a := groupAggr.aggrs[pos]
	if ak.KeyPos >= 0 {
		if ak.AggrFunc == c.AGG_SUM && !groupAggr.IsPrimary {
			a.decoded = decodedkeys[ak.KeyPos].ActualForIndex()
		} else {
			a.raw = compositekeys[ak.KeyPos]
		}
	} else if !cacheValid || groupAggr.DependsOnPrimaryKey {
		//process expr
		var scalar value.Value
		if ak.ExprValue != nil {
			scalar = ak.ExprValue // It is a constant expression
		} else {
			var err error
			scalar, err = evaluateN1QLExpresssion(groupAggr, ak.Expr, decodedkeys, docid, p)
			if err != nil {
				return err
			}
		}
		a.obj = scalar
		a.n1qlValue = true
	}

	a.typ = ak.AggrFunc
	a.projectId = ak.EntryKeyId
	a.distinct = ak.Distinct
	a.count = count
	return nil
}

func setCoverForExprEval(groupAggr *GroupAggr, decodedkeys value.Values, docid []byte) {
	if groupAggr.HasExpr {
		if groupAggr.IsPrimary {
			for _, ik := range groupAggr.DependsOnIndexKeys {
				groupAggr.av.SetCover(groupAggr.IndexKeyNames[ik], value.NewValue(string(docid)))
			}
		} else {
			for _, ik := range groupAggr.DependsOnIndexKeys {
				if int(ik) == len(decodedkeys) {
					groupAggr.av.SetCover(groupAggr.IndexKeyNames[ik], value.NewValue(string(docid)))
				} else {
					groupAggr.av.SetCover(groupAggr.IndexKeyNames[ik], decodedkeys[ik])
				}
			}
		}
	}
}

func evaluateN1QLExpresssion(groupAggr *GroupAggr, expr expression.Expression,
	decodedkeys value.Values, docid []byte, p *ScanPipeline) (value.Value, error) {

	t0 := time.Now()
	scalar, _, err := expr.EvaluateForIndex(groupAggr.av, groupAggr.exprContext) // TODO: Ignore vector for now
	if err != nil {
		return nil, err
	}

	p.exprEvalDur += time.Since(t0)
	p.exprEvalNum++

	return scalar, nil
}

func checkFirstValidRow(row *aggrRow, groupAggr *GroupAggr, p *ScanPipeline) {
	if groupAggr.FirstValidAggrOnly {
		agg := row.aggrs[0]
		if agg.typ == c.AGG_MIN || agg.typ == c.AGG_MAX {
			if agg.fn.IsValid() {
				row.SetFlush(true)
				p.stopAggregation = true
			}
		}
		if agg.typ == c.AGG_COUNT {
			if agg.fn.Value() == 1 {
				row.SetFlush(true)
				p.stopAggregation = true
			}
		}
	}
}

func (ar *aggrResult) AddNewGroup(groupAggr *GroupAggr, p *ScanPipeline, cacheValid bool) error {

	var err error

	if cacheValid && len(ar.rows) == 1 {
		err = ar.rows[0].AddAggregate(groupAggr.aggrs)
		if err != nil {
			return err
		}
		checkFirstValidRow(ar.rows[0], groupAggr, p)
		return nil
	}

	nomatch := true
	for _, row := range ar.rows {
		if row.CheckEqualGroup(groupAggr.groups) {
			nomatch = false
			err = row.AddAggregate(groupAggr.aggrs)
			if err != nil {
				return err
			}
			checkFirstValidRow(ar.rows[0], groupAggr, p)
			break
		}
	}

	if nomatch {
		newRow := &aggrRow{groups: make([]*groupKey, len(groupAggr.groups)),
			aggrs: make([]*aggrVal, len(groupAggr.aggrs))}

		for i, g := range groupAggr.groups {
			if g.n1qlValue {
				newRow.groups[i] = &groupKey{obj: g.obj, projectId: g.projectId, n1qlValue: true}
			} else {
				newKey := make([]byte, len(g.raw))
				copy(newKey, g.raw)
				newRow.groups[i] = &groupKey{raw: newKey, projectId: g.projectId}
			}
		}

		newRow.AddAggregate(groupAggr.aggrs)

		//flush the first row
		if len(ar.rows) >= ar.maxRows {
			ar.rows[0].SetFlush(true)
		}
		ar.rows = append(ar.rows, newRow)
		checkFirstValidRow(ar.rows[0], groupAggr, p)
	}

	return nil
}

func (a *aggrResult) SetMaxRows(n int) {
	a.maxRows = n
}

func (ar *aggrRow) CheckEqualGroup(groups []*groupKey) bool {

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
			if agg.n1qlValue {
				ar.aggrs[i] = &aggrVal{fn: c.NewAggrFunc(agg.typ, agg.obj, agg.distinct, true),
					projectId: agg.projectId}
			} else {
				if agg.typ == c.AGG_SUM {
					ar.aggrs[i] = &aggrVal{fn: c.NewAggrFunc(agg.typ, agg.decoded, agg.distinct, false),
						projectId: agg.projectId}
				} else {
					ar.aggrs[i] = &aggrVal{fn: c.NewAggrFunc(agg.typ, agg.raw, agg.distinct, false),
						projectId: agg.projectId}
				}
			}
		} else {
			if agg.n1qlValue {
				ar.aggrs[i].fn.AddDeltaObj(agg.obj)
			} else {
				if agg.typ == c.AGG_SUM {
					ar.aggrs[i].fn.AddDelta(agg.decoded)
				} else {
					ar.aggrs[i].fn.AddDeltaRaw(agg.raw)
				}
			}
		}
		if agg.count > 1 && (agg.typ == c.AGG_SUM || agg.typ == c.AGG_COUNT ||
			agg.typ == c.AGG_COUNTN) {
			for j := 1; j <= agg.count-1; j++ {
				if agg.n1qlValue {
					ar.aggrs[i].fn.AddDeltaObj(agg.obj)
				} else if agg.typ == c.AGG_SUM {
					ar.aggrs[i].fn.AddDelta(agg.decoded)
				} else {
					ar.aggrs[i].fn.AddDeltaRaw(agg.raw)
				}
			}
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
	if gk.n1qlValue {
		return gk.obj.EquivalentTo(ok.obj)
	} else {
		return bytes.Equal(gk.raw, ok.raw)
	}
}

func projectEmptyResult(buf []byte, projection *Projection, groupAggr *GroupAggr) ([]byte, error) {

	var err error
	//If no group by and no documents qualify, COUNT aggregate
	//should return 0 and all other aggregates should return NULL
	if len(groupAggr.Group) == 0 {

		aggrs := make([][]byte, len(groupAggr.Aggrs))

		for i, ak := range groupAggr.Aggrs {
			if ak.AggrFunc == c.AGG_COUNT || ak.AggrFunc == c.AGG_COUNTN {
				aggrs[i] = encodedZero
			} else {
				aggrs[i] = encodedNull
			}
		}

		var keysToJoin [][]byte
		for _, projGroup := range projection.projectGroupKeys {
			keysToJoin = append(keysToJoin, aggrs[projGroup.pos])
		}

		if buf, err = jsonEncoder.JoinArray(keysToJoin, buf); err != nil {
			l.Errorf("ScanPipeline::projectEmptyResult join array error %v", err)
			return nil, err
		}

		return buf, nil

	} else {
		//If group is not nil and if none of the documents qualify,
		//the aggregate should not return anything
		return nil, nil
	}

	return nil, nil

}

func projectGroupAggr(buf []byte, projection *Projection,
	aggrRes *aggrResult, isPrimary bool) ([]byte, error) {

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
			gk := row.groups[projGroup.pos]
			if gk.n1qlValue {
				var newKey []byte
				newKey, err = encodeN1qlVal(gk.obj)
				if err != nil {
					return nil, err
				}
				keysToJoin = append(keysToJoin, newKey)
			} else {
				if isPrimary {
					//TODO: will be optimized as part of overall pipeline optimization
					val, err := encodeValue(string(gk.raw))
					if err != nil {
						l.Errorf("ScanPipeline::projectGroupAggr encodeValue error %v", err)
						return nil, err
					}
					keysToJoin = append(keysToJoin, val)
				} else {
					keysToJoin = append(keysToJoin, gk.raw)
				}
			}
		} else {
			if row.aggrs[projGroup.pos].fn.Type() == c.AGG_SUM ||
				row.aggrs[projGroup.pos].fn.Type() == c.AGG_COUNT ||
				row.aggrs[projGroup.pos].fn.Type() == c.AGG_COUNTN {
				val, err := encodeValue(row.aggrs[projGroup.pos].fn.Value())
				if err != nil {
					l.Errorf("ScanPipeline::projectGroupAggr encodeValue error %v", err)
					return nil, err
				}
				keysToJoin = append(keysToJoin, val)
			} else {
				val := row.aggrs[projGroup.pos].fn.Value()
				switch v := val.(type) {

				case []byte:
					if isPrimary && !isEncodedNull(v) {
						val, err := encodeValue(string(v))
						if err != nil {
							l.Errorf("ScanPipeline::projectGroupAggr encodeValue error %v", err)
							return nil, err
						}
						keysToJoin = append(keysToJoin, val)
					} else {
						keysToJoin = append(keysToJoin, v)
					}

				case value.Value:
					eval, err := encodeValue(v.ActualForIndex())
					if err != nil {
						l.Errorf("ScanPipeline::projectGroupAggr encodeValue error %v", err)
						return nil, err
					}
					keysToJoin = append(keysToJoin, eval)
				}
			}
		}
	}

	if buf, err = jsonEncoder.JoinArray(keysToJoin, buf); err != nil {
		l.Errorf("ScanPipeline::projectGroupAggr join array error %v", err)
		return nil, err
	}

	return buf, nil
}

func unmarshalValue(dec []byte) (interface{}, error) {

	var actualVal interface{}

	//json unmarshal to go type
	err := json.Unmarshal(dec, &actualVal)
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
	encval, err := jsonEncoder.Encode(jsonraw, encbuf)
	if err != nil {
		return nil, err
	}

	return encval, nil
}

func encodeN1qlVal(val value.Value) ([]byte, error) {
	encodeBuf := make([]byte, 1024)
	encoded, err := jsonEncoder.EncodeN1QLValue(val, encodeBuf[:0])
	if err != nil && err.Error() == collatejson.ErrorOutputLen.Error() {
		valBytes, e1 := val.MarshalJSON()
		if e1 != nil {
			return encoded, err
		}
		newBuf := make([]byte, 0, len(valBytes)*3)
		enc, e2 := jsonEncoder.EncodeN1QLValue(val, newBuf)
		return enc, e2
	}
	return encoded, err
}

func isEncodedNull(v []byte) bool {
	return bytes.Equal(v, encodedNull)
}

/////////////////////////////////////////////////////////////////////////
//
// entry cache implementation
//
/////////////////////////////////////////////////////////////////////////

type entryCache struct {
	entry       []byte
	compkeys    [][]byte
	decodedkeys value.Values

	compkeybuf []byte
	valid      bool

	hit           int64
	miss          int64
	compareDocIDs bool
}

func (e *entryCache) Init(r *ScanRequest) {

	entrybuf := secKeyBufPool.Get()
	r.keyBufList = append(r.keyBufList, entrybuf)

	compkeybuf := secKeyBufPool.Get()
	r.keyBufList = append(r.keyBufList, compkeybuf)

	e.entry = (*entrybuf)[:0]
	e.compkeybuf = (*compkeybuf)[:0]
	e.compareDocIDs = false
	//Expr depends on meta().id, we need to include docid in key commparision
	if r.GroupAggr != nil && r.GroupAggr.DependsOnPrimaryKey {
		e.compareDocIDs = true
	}
}

func (e *entryCache) EqualsEntry(other []byte) bool {

	defer func() {
		if r := recover(); r != nil {
			l.Fatalf("EntryCache - panic detected")
			e1 := secondaryIndexEntry(e.entry)
			e2 := secondaryIndexEntry(other)
			l.Fatalf("Cached - Raw %v Entry %s", e1.Bytes(), e1)
			l.Fatalf("Other - Raw %v Entry %s", e2.Bytes(), e2)
			panic(r)
		}
	}()
	return distinctCompare(e.entry, other, e.compareDocIDs)
}

func (e *entryCache) Get() ([][]byte, value.Values) {
	return e.compkeys, e.decodedkeys
}

func (e *entryCache) Update(entry []byte, compositekeys [][]byte, decodedkeys value.Values) {

	e.entry = append(e.entry[:0], entry...)

	if len(entry) > cap(e.compkeybuf) {
		e.compkeybuf = make([]byte, 0, len(entry)+1024)
	}

	if e.compkeys == nil {
		e.compkeys = make([][]byte, len(compositekeys))
	}

	tmpbuf := e.compkeybuf[:0]
	for i, k := range compositekeys {
		tmpbuf = append(tmpbuf[:0], k...)
		e.compkeys[i] = tmpbuf[:len(k)]
		tmpbuf = tmpbuf[len(k):]
	}

	if decodedkeys != nil {
		if e.decodedkeys == nil {
			e.decodedkeys = make(value.Values, len(decodedkeys))
		}
		for i, k := range decodedkeys {
			if k != nil {
				e.decodedkeys[i] = k.Copy()
			}
		}
	}

}

func (e *entryCache) SetValid(valid bool) {
	if valid {
		e.hit++
	} else {
		e.miss++
	}
	e.valid = valid
}

func (e *entryCache) Exists() bool {
	return e.compkeybuf != nil
}

func (e *entryCache) Valid() bool {
	return e.valid
}

func (e *entryCache) Stats() string {
	return fmt.Sprintf("Hit %v Miss %v", e.hit, e.miss)

}

func (e *entryCache) Hits() int64 {
	return e.hit
}

func (e *entryCache) Misses() int64 {
	return e.miss
}

func (e *entryCache) CacheHitRatio() int {

	if e.hit+e.miss != 0 {
		return int((e.hit * 100) / (e.miss + e.hit))
	} else {
		return 0
	}

}
