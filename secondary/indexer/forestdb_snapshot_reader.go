// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

// This file implements IndexReader interface
import (
	"errors"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

var (
	ErrUnsupportedInclusion = errors.New("Unsupported range inclusion option")
)

type CmpEntry func(IndexKey, IndexEntry) int
type EntryCallback func(key, value []byte) error
type FinishCallback func()

// Approximate items count
func (s *fdbSnapshot) StatCountTotal() (uint64, error) {
	c := s.slice.GetCommittedCount()
	return c, nil
}

func (s *fdbSnapshot) CountTotal(ctx IndexReaderContext, stopch StopChannel) (uint64, error) {
	return s.CountRange(ctx, MinIndexKey, MaxIndexKey, Both, stopch)
}

func (s *fdbSnapshot) CountRange(ctx IndexReaderContext, low, high IndexKey, inclusion Inclusion,
	stopch StopChannel) (uint64, error) {

	var count uint64
	callb := func(key, value []byte) error {
		select {
		case <-stopch:
			return common.ErrClientCancel
		default:
			count++
		}

		return nil
	}

	err := s.Range(ctx, low, high, inclusion, callb, nil)
	return count, err
}

func (s *fdbSnapshot) MultiScanCount(ctx IndexReaderContext, low, high IndexKey, inclusion Inclusion,
	scan Scan, distinct bool,
	stopch StopChannel) (uint64, error) {

	var err error
	var scancount uint64
	count := 1
	checkDistinct := distinct && !s.isPrimary()
	isIndexComposite := len(s.slice.idxDefn.SecExprs) > 1

	buf := secKeyBufPool.Get()
	defer secKeyBufPool.Put(buf)

	previousRow := ctx.GetCursorKey()

	callb := func(entry, value []byte) error {
		select {
		case <-stopch:
			return common.ErrClientCancel
		default:
			skipRow := false
			var ck [][]byte

			//get the key in original format
			if s.slice.idxDefn.Desc != nil {
				_, err = jsonEncoder.ReverseCollate(entry, s.slice.idxDefn.Desc)
				if err != nil {
					return err
				}
			}
			if scan.ScanType == FilterRangeReq {
				if len(entry) > cap(*buf) {
					*buf = make([]byte, 0, len(entry)+RESIZE_PAD)
				}

				skipRow, ck, err = filterScanRow(entry, scan, (*buf)[:0])
				if err != nil {
					return err
				}
			}
			if skipRow {
				return nil
			}

			if checkDistinct {
				if isIndexComposite {
					entry, err = projectLeadingKey(ck, entry, buf)
					if err != nil {
						return err
					}
				}
				if len(*previousRow) != 0 && distinctCompare(entry, *previousRow, false) {
					return nil // Ignore the entry as it is same as previous entry
				}
			}

			if !s.isPrimary() {
				e := secondaryIndexEntry(entry)
				count = e.Count()
			}

			if checkDistinct {
				scancount++
				*previousRow = append((*previousRow)[:0], entry...)
			} else {
				scancount += uint64(count)
			}
		}
		return nil
	}

	e := s.Range(ctx, low, high, inclusion, callb, nil)
	return scancount, e
}

func (s *fdbSnapshot) CountLookup(ctx IndexReaderContext, keys []IndexKey, stopch StopChannel) (uint64, error) {
	var err error
	var count uint64

	callb := func(key, value []byte) error {
		select {
		case <-stopch:
			return common.ErrClientCancel
		default:
			count++
		}

		return nil
	}

	for _, k := range keys {
		if err = s.Lookup(ctx, k, callb, nil); err != nil {
			break
		}
	}

	return count, err
}

func (s *fdbSnapshot) Exists(ctx IndexReaderContext, key IndexKey, stopch StopChannel) (bool, error) {
	var count uint64
	callb := func(key, value []byte) error {
		select {
		case <-stopch:
			return common.ErrClientCancel
		default:
			count++
		}

		return nil
	}

	err := s.Lookup(ctx, key, callb, nil)
	return count != 0, err
}

func (s *fdbSnapshot) Lookup(ctx IndexReaderContext, key IndexKey,
	callb EntryCallback, fincb FinishCallback) error {
	return s.Iterate(ctx, key, key, Both, compareExact, callb)
}

func (s *fdbSnapshot) Range(ctx IndexReaderContext, low, high IndexKey,
	inclusion Inclusion, callb EntryCallback, fincb FinishCallback) error {

	var cmpFn CmpEntry
	if s.isPrimary() {
		cmpFn = compareExact
	} else {
		cmpFn = comparePrefix
	}

	return s.Iterate(ctx, low, high, inclusion, cmpFn, callb)
}

func (s *fdbSnapshot) All(ctx IndexReaderContext, callb EntryCallback, fincb FinishCallback) error {
	return s.Range(ctx, MinIndexKey, MaxIndexKey, Both, callb, fincb)
}

func (s *fdbSnapshot) Iterate(ctx IndexReaderContext, low, high IndexKey, inclusion Inclusion,
	cmpFn CmpEntry, callback EntryCallback) error {

	ttime := time.Now()

	var entry IndexEntry
	it, err := newFDBSnapshotIterator(s)
	if err != nil {
		return err
	}
	defer func() {
		go closeIterator(it)
	}()

	defer func() {
		s.slice.idxStats.Timings.stScanPipelineIterate.Put(time.Now().Sub(ttime))
	}()

	if low.Bytes() == nil {
		it.SeekFirst()
	} else {
		it.Seek(low.Bytes())

		// Discard equal keys if low inclusion is requested
		if inclusion == Neither || inclusion == High {
			err = s.iterEqualKeys(low, it, cmpFn, nil)
			if err != nil {
				return err
			}
		}
	}

loop:
	for ; it.Valid(); it.Next() {
		s.newIndexEntry(it.Key(), &entry)

		// Iterator has reached past the high key, no need to scan further
		if cmpFn(high, entry) <= 0 {
			break loop
		}

		err = callback(it.Key(), it.Value())
		if err != nil {
			return err
		}
	}

	// Include equal keys if high inclusion is requested
	if inclusion == Both || inclusion == High {
		err = s.iterEqualKeys(high, it, cmpFn, callback)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *fdbSnapshot) isPrimary() bool {
	return s.slice.isPrimary
}

func closeIterator(it *ForestDBIterator) {
	err := it.Close()
	if err != nil {
		logging.Errorf("ForestDB iterator: dealloc failed (%v)", err)
	}
}

func (s *fdbSnapshot) newIndexEntry(b []byte, entry *IndexEntry) {
	var err error

	if s.slice.isPrimary {
		*entry, err = BytesToPrimaryIndexEntry(b)
	} else {
		*entry, err = BytesToSecondaryIndexEntry(b)
	}
	common.CrashOnError(err)
}

func (s *fdbSnapshot) iterEqualKeys(k IndexKey, it *ForestDBIterator,
	cmpFn CmpEntry, callback EntryCallback) error {
	var err error

	var entry IndexEntry
	for ; it.Valid(); it.Next() {
		s.newIndexEntry(it.Key(), &entry)
		if cmpFn(k, entry) == 0 {
			if callback != nil {
				err = callback(it.Key(), it.Value())
				if err != nil {
					return err
				}
			}
		} else {
			break
		}
	}

	return err
}

func (s *fdbSnapshot) DecodeMeta(meta []byte) (uint64, uint64, []byte) {
	return 0, 0, nil
}

func (s *fdbSnapshot) FetchValue(ctx IndexReaderContext, storeId uint64, recordId uint64, cid []byte, buf []byte) ([]byte, error) {
	return nil, nil
}

func compareExact(k IndexKey, entry IndexEntry) int {
	return k.Compare(entry)
}

func comparePrefix(k IndexKey, entry IndexEntry) int {
	return k.ComparePrefixFields(entry)
}
