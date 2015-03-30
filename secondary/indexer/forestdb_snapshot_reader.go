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
	"errors"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

var (
	ErrUnsupportedInclusion = errors.New("Unsupported range inclusion option")
	nilKey                  = &NilIndexKey{}
)

//Counter interface
func (s *fdbSnapshot) CountTotal(stopch StopChannel) (uint64, error) {
	return s.CountRange(nilKey, nilKey, Both, stopch)
}

// Approximate items count
func (s *fdbSnapshot) StatCountTotal() (uint64, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	info, err := s.main.Info()
	if err != nil {
		return 0, err
	}
	return info.DocCount(), nil
}

//Exister interface
func (s *fdbSnapshot) Exists(key IndexKey, stopch StopChannel) (bool, error) {

	var totalRows uint64
	var err error
	if totalRows, err = s.CountRange(key, key, Both, stopch); err != nil {
		return false, nil
	} else {
		return false, err
	}

	if totalRows > 0 {
		return true, nil
	}
	return false, nil
}

//Looker interface
func (s *fdbSnapshot) Lookup(key IndexKey, stopch StopChannel) (chan IndexEntry, chan error) {
	chentry := make(chan IndexEntry)
	cherr := make(chan error)

	go s.GetEntriesForKeyRange(key, key, Both, false, chentry, cherr, stopch)
	return chentry, cherr
}

func (s *fdbSnapshot) KeySet(stopch StopChannel) (chan IndexEntry, chan error) {
	chentry := make(chan IndexEntry)
	cherr := make(chan error)

	go s.GetEntriesForKeyRange(nilKey, nilKey, Both, true, chentry, cherr, stopch)
	return chentry, cherr
}

func (s *fdbSnapshot) KeyRange(low, high IndexKey, inclusion Inclusion,
	stopch StopChannel) (chan IndexEntry, chan error, SortOrder) {

	chentry := make(chan IndexEntry)
	cherr := make(chan error)

	go s.GetEntriesForKeyRange(low, high, inclusion, true, chentry, cherr, stopch)
	return chentry, cherr, Asc
}

func (s *fdbSnapshot) newIndexEntry(b []byte) IndexEntry {
	var entry IndexEntry
	var err error

	if s.slice.(*fdbSlice).isPrimary {
		entry, err = BytesToPrimaryIndexEntry(b)
	} else {
		entry, err = BytesToSecondaryIndexEntry(b)
	}
	common.CrashOnError(err)
	return entry
}

func (s *fdbSnapshot) GetEntriesForKeyRange(low, high IndexKey, inclusion Inclusion,
	prefixCmp bool, chentry chan IndexEntry, cherr chan error, stopch StopChannel) {

	defer close(chentry)

	logging.Debugf("ForestDB Received Key Low - %s High - %s for Scan",
		low.String(), high.String())

	it, err := newFDBSnapshotIterator(s)
	if err != nil {
		cherr <- err
		return
	}
	defer closeIterator(it)

	if low.Bytes() == nil {
		it.SeekFirst()
	} else {
		it.Seek(low.Bytes())

		// Discard equal keys if low inclusion is requested
		if inclusion == Neither || inclusion == High {
			s.readEqualKeys(low, it, prefixCmp, chentry, cherr, stopch, true)
		}
	}

	var entry IndexEntry
loop:
	for ; it.Valid(); it.Next() {
		select {
		case <-stopch:
			// stop signalled, end processing
			return

		default:
			logging.Tracef("ForestDB Got Key - %s", string(it.Key()))

			entry = s.newIndexEntry(it.Key())

			var highcmp int
			if high.Bytes() == nil {
				// if high key is nil, iterate through the fullset
				highcmp = 1
			} else {
				highcmp = keyCmp(prefixCmp, high, entry)
			}

			// if we have reached past the high key, no need to scan further
			if highcmp <= 0 {
				logging.Tracef("ForestDB Discarding Key - %s since >= high", string(it.Key()))
				break loop
			}

			chentry <- entry
		}
	}

	// Include equal keys if high inclusion is requested
	if inclusion == Both || inclusion == High {
		s.readEqualKeys(high, it, prefixCmp, chentry, cherr, stopch, false)
	}
}

//RangeCounter interface
func (s *fdbSnapshot) CountRange(low, high IndexKey, inclusion Inclusion,
	stopch StopChannel) (uint64, error) {

	logging.Debugf("ForestDB Received Key Low - %s High - %s for Scan",
		low.String(), high.String())

	chentry := make(chan IndexEntry)
	cherr := make(chan error)
	go s.GetEntriesForKeyRange(low, high, inclusion, true, chentry, cherr, stopch)

	var count uint64
loop:
	for {
		select {
		case _, ok := <-chentry:
			if !ok {
				break loop
			}
			count++
		case err := <-cherr:
			return 0, err
		}
	}
	return count, nil
}

func (s *fdbSnapshot) readEqualKeys(k IndexKey, it *ForestDBIterator, prefixCmp bool,
	chentry chan IndexEntry, cherr chan error, stopch StopChannel, discard bool) {

	var entry IndexEntry
	for ; it.Valid(); it.Next() {
		entry = s.newIndexEntry(it.Key())
		if keyCmp(prefixCmp, k, entry) == 0 {
			if !discard {
				chentry <- entry
			}
		} else {
			break
		}
	}
}

func closeIterator(it *ForestDBIterator) {
	err := it.Close()
	if err != nil {
		logging.Errorf("ForestDB iterator: dealloc failed (%v)", err)
	}
}

func keyCmp(prefixCmp bool, k IndexKey, entry IndexEntry) int {
	var cmp int
	if prefixCmp {
		cmp = k.ComparePrefixFields(entry)
	} else {
		cmp = k.Compare(entry)
	}
	return cmp
}
