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
	"errors"
	"github.com/couchbase/indexing/secondary/collatejson"
	c "github.com/couchbase/indexing/secondary/common"
	p "github.com/couchbase/indexing/secondary/pipeline"
)

var (
	ErrLimitReached = errors.New("Row limit reached")
)

type ScanPipeline struct {
	src    p.Source
	object p.Pipeline
	req    *ScanRequest

	rowsRead  uint64
	bytesRead uint64
}

func (p *ScanPipeline) Cancel(err error) {
	p.src.Shutdown(err)
}

func (p *ScanPipeline) Execute() error {
	return p.object.Execute()
}

func (p ScanPipeline) RowsRead() uint64 {
	return p.rowsRead
}

func (p ScanPipeline) BytesRead() uint64 {
	return p.bytesRead
}

func NewScanPipeline(req *ScanRequest, w ScanResponseWriter, is IndexSnapshot) *ScanPipeline {
	scanPipeline := new(ScanPipeline)
	scanPipeline.req = req

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
	buf := secKeyBufPool.Get()
	r.keyBufList = append(r.keyBufList, buf)

	fn := func(entry []byte) error {

		skipRow := false
		if currentScan.ScanType == FilterRangeReq {
			skipRow, err = filterScanRow(entry, currentScan, (*buf)[:0])
			if err != nil {
				return err
			}
		}

		if !skipRow {
			s.p.rowsRead++

			wrErr := s.WriteItem(entry)
			if wrErr != nil {
				return wrErr
			}

			if s.p.rowsRead == uint64(s.p.req.Limit) {
				return ErrLimitReached
			}
		}

		return nil
	}

	sliceSnapshots := GetSliceSnapshots(s.is)

loop:
	for _, scan := range r.Scans {
		currentScan = scan
		for _, snap := range sliceSnapshots {
			if scan.ScanType == AllReq {
				err = snap.Snapshot().All(fn)
			} else if scan.ScanType == LookupReq {
				err = snap.Snapshot().Lookup(scan.Equals, fn)
			} else if scan.ScanType == RangeReq || scan.ScanType == FilterRangeReq {
				err = snap.Snapshot().Range(scan.Low, scan.High, scan.Incl, fn)
			}
			switch err {
			case nil:
			case p.ErrSupervisorKill, ErrLimitReached:
				break loop
			default:
				s.CloseWithError(err)
				break loop
			}
		}
	}

	return nil
}

func (d *IndexScanDecoder) Routine() error {
	defer d.CloseWrite()
	defer d.CloseRead()

	var sk, docid []byte
	var count int
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

		t := (*tmpBuf)[:0]
		count = 1
		if d.p.req.isPrimary {
			sk, docid = piSplitEntry(row, t)
		} else {
			sk, docid, count = siSplitEntry(row, t)
		}

		d.p.bytesRead += uint64(len(sk) + len(docid))
		for i := 0; i < count; i++ {
			err = d.WriteItem(sk, docid)
			if err != nil {
				break
			}
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
func filterScanRow(key []byte, scan Scan, buf []byte) (bool, error) {
	codec := collatejson.NewCodec(16)
	compositekeys, err := codec.ExplodeArray(key, buf)
	if err != nil {
		return false, err
	}

	var filtermatch bool
	for _, filtercollection := range scan.Filters {
		if len(filtercollection.CompositeFilters) > len(compositekeys) {
			// There cannot be more ranges than number of composite keys
			err = errors.New("There are more ranges than number of composite elements in the index")
			return false, err
		}
		filtermatch = applyFilter(compositekeys, filtercollection.CompositeFilters)
		if filtermatch {
			return false, nil
		}
	}

	return true, nil
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
