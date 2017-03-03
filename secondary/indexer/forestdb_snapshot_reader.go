// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package indexer

// This file implements IndexReader interface
import (
	"errors"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"time"
)

var (
	ErrUnsupportedInclusion = errors.New("Unsupported range inclusion option")
)

type CmpEntry func(IndexKey, IndexEntry) int
type EntryCallback func([]byte) error

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
	callb := func([]byte) error {
		select {
		case <-stopch:
			return common.ErrClientCancel
		default:
			count++
		}

		return nil
	}

	err := s.Range(ctx, low, high, inclusion, callb)
	return count, err
}

func (s *fdbSnapshot) MultiScanCount(ctx IndexReaderContext, low, high IndexKey, inclusion Inclusion,
	scan Scan, distinct bool,
	stopch StopChannel) (uint64, error) {

	var err error
	var scancount uint64
	count := 1
	checkDistinct := distinct && !s.isPrimary()

	buf := secKeyBufPool.Get()
	defer secKeyBufPool.Put(buf)
	buf2 := secKeyBufPool.Get()
	defer secKeyBufPool.Put(buf2)
	previousRow := (*buf2)[:0]

	callb := func(entry []byte) error {
		select {
		case <-stopch:
			return common.ErrClientCancel
		default:
			skipRow := false
			if scan.ScanType == FilterRangeReq {
				skipRow, _, err = filterScanRow(entry, scan, (*buf)[:0])
				if err != nil {
					return err
				}
			}
			if skipRow {
				return nil
			}

			if checkDistinct {
				if len(previousRow) != 0 && distinctCompare(entry, previousRow) {
					return nil // Ignore the entry as it is same as previous entry
				}
			}

			if !s.isPrimary() {
				e := secondaryIndexEntry(entry)
				count = e.Count()
			}

			if checkDistinct {
				scancount++
				previousRow = append(previousRow[:0], entry...)
			} else {
				scancount += uint64(count)
			}
		}
		return nil
	}

	e := s.Range(ctx, low, high, inclusion, callb)
	return scancount, e
}

func (s *fdbSnapshot) CountLookup(ctx IndexReaderContext, keys []IndexKey, stopch StopChannel) (uint64, error) {
	var err error
	var count uint64

	callb := func([]byte) error {
		select {
		case <-stopch:
			return common.ErrClientCancel
		default:
			count++
		}

		return nil
	}

	for _, k := range keys {
		if err = s.Lookup(ctx, k, callb); err != nil {
			break
		}
	}

	return count, err
}

func (s *fdbSnapshot) Exists(ctx IndexReaderContext, key IndexKey, stopch StopChannel) (bool, error) {
	var count uint64
	callb := func([]byte) error {
		select {
		case <-stopch:
			return common.ErrClientCancel
		default:
			count++
		}

		return nil
	}

	err := s.Lookup(ctx, key, callb)
	return count != 0, err
}

func (s *fdbSnapshot) Lookup(ctx IndexReaderContext, key IndexKey, callb EntryCallback) error {
	return s.Iterate(ctx, key, key, Both, compareExact, callb)
}

func (s *fdbSnapshot) Range(ctx IndexReaderContext, low, high IndexKey, inclusion Inclusion,
	callb EntryCallback) error {

	var cmpFn CmpEntry
	if s.isPrimary() {
		cmpFn = compareExact
	} else {
		cmpFn = comparePrefix
	}

	return s.Iterate(ctx, low, high, inclusion, cmpFn, callb)
}

func (s *fdbSnapshot) All(ctx IndexReaderContext, callb EntryCallback) error {
	return s.Range(ctx, MinIndexKey, MaxIndexKey, Both, callb)
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
		entry = s.newIndexEntry(it.Key())

		// Iterator has reached past the high key, no need to scan further
		if cmpFn(high, entry) <= 0 {
			break loop
		}

		err = callback(it.Key())
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

func (s *fdbSnapshot) newIndexEntry(b []byte) IndexEntry {
	var entry IndexEntry
	var err error

	if s.slice.isPrimary {
		entry, err = BytesToPrimaryIndexEntry(b)
	} else {
		entry, err = BytesToSecondaryIndexEntry(b)
	}
	common.CrashOnError(err)
	return entry
}

func (s *fdbSnapshot) iterEqualKeys(k IndexKey, it *ForestDBIterator,
	cmpFn CmpEntry, callback func([]byte) error) error {
	var err error

	var entry IndexEntry
	for ; it.Valid(); it.Next() {
		entry = s.newIndexEntry(it.Key())
		if cmpFn(k, entry) == 0 {
			if callback != nil {
				err = callback(it.Key())
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

func compareExact(k IndexKey, entry IndexEntry) int {
	return k.Compare(entry)
}

func comparePrefix(k IndexKey, entry IndexEntry) int {
	return k.ComparePrefixFields(entry)
}
