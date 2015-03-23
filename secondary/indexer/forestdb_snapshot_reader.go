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
	"github.com/couchbase/indexing/secondary/logging"
)

var (
	ErrUnsupportedInclusion = errors.New("Unsupported range inclusion option")
)

//Counter interface
func (s *fdbSnapshot) CountTotal(stopch StopChannel) (uint64, error) {

	var nilKey Key
	var err error
	if nilKey, err = NewKeyFromEncodedBytes(nil); err != nil {
		return 0, err
	}

	return s.CountRange(nilKey, nilKey, Both, stopch)
}

// Approximate items count
func (s *fdbSnapshot) StatCount() (uint64, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	info, err := s.main.Info()
	if err != nil {
		return 0, err
	}
	return info.DocCount(), nil
}

//Exister interface
func (s *fdbSnapshot) Exists(key Key, stopch StopChannel) (bool, error) {

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
func (s *fdbSnapshot) Lookup(key Key, stopch StopChannel) (chan Value, chan error) {
	chval := make(chan Value)
	cherr := make(chan error)

	logging.Debugf("FdbSnapshot: Received Lookup Query for Key %s", key.String())
	go s.GetValueSetForKeyRange(key, key, Both, chval, cherr, stopch)
	return chval, cherr
}

func (s *fdbSnapshot) KeySet(stopch StopChannel) (chan Key, chan error) {
	chkey := make(chan Key)
	cherr := make(chan error)

	nilKey, _ := NewKeyFromEncodedBytes(nil)
	go s.GetKeySetForKeyRange(nilKey, nilKey, Both, chkey, cherr, stopch)
	return chkey, cherr
}

func (s *fdbSnapshot) ValueSet(stopch StopChannel) (chan Value, chan error) {
	chval := make(chan Value)
	cherr := make(chan error)

	nilKey, _ := NewKeyFromEncodedBytes(nil)
	go s.GetValueSetForKeyRange(nilKey, nilKey, Both, chval, cherr, stopch)
	return chval, cherr
}

//Ranger
func (s *fdbSnapshot) KeyRange(low, high Key, inclusion Inclusion,
	stopch StopChannel) (chan Key, chan error, SortOrder) {

	chkey := make(chan Key)
	cherr := make(chan error)

	go s.GetKeySetForKeyRange(low, high, inclusion, chkey, cherr, stopch)
	return chkey, cherr, Asc
}

func (s *fdbSnapshot) ValueRange(low, high Key, inclusion Inclusion,
	stopch StopChannel) (chan Value, chan error, SortOrder) {

	chval := make(chan Value)
	cherr := make(chan error)

	go s.GetValueSetForKeyRange(low, high, inclusion, chval, cherr, stopch)
	return chval, cherr, Asc
}

// TODO: Refactor db scan to support inclusion options
// Currently, for the given low, high predicates, it will return rows
// for which row >= low and row < high
func (s *fdbSnapshot) GetKeySetForKeyRange(low Key, high Key,
	inclusion Inclusion, chkey chan Key, cherr chan error, stopch StopChannel) {

	defer close(chkey)

	logging.Debugf("ForestDB Received Key Low - %s High - %s for Scan",
		low.String(), high.String())

	it, err := newFDBSnapshotIterator(s)
	if err != nil {
		cherr <- err
		return
	}
	defer closeIterator(it)

	var lowkey, highkey []byte

	if lowkey = low.Encoded(); lowkey == nil {
		it.SeekFirst()
	} else {
		// Low key prefix computed by removing last byte
		lowkey = lowkey[:len(lowkey)-1]
		it.Seek(lowkey)

		// Discard equal keys if low inclusion is requested
		if inclusion == Neither || inclusion == High {
			readEqualKeys(low, lowkey, it, chkey, cherr, stopch, true)
		}
	}

	highkey = high.Encoded()
	if highkey != nil {
		// High key prefix computed by removing last byte
		highkey = highkey[:len(highkey)-1]
	}

	var key Key
loop:
	for ; it.Valid(); it.Next() {

		select {

		case <-stopch:
			// stop signalled, end processing
			return

		default:
			logging.Tracef("ForestDB Got Key - %s", string(it.Key()))

			var highcmp int
			if highkey == nil {
				highcmp = -1 // if high key is nil, iterate through the fullset
			} else {
				highcmp = bytes.Compare(it.Key(), highkey)
			}

			if key, err = NewKeyFromEncodedBytes(it.Key()); err != nil {
				logging.Errorf("Error Converting from bytes %v to key %v. Skipping row",
					it.Key(), err)
				panic(err)
			}

			// if we have reached past the high key, no need to scan further
			if highcmp > 0 {
				logging.Tracef("ForestDB Discarding Key - %s since >= high", string(it.Key()))
				break loop
			}

			chkey <- key
		}
	}

	// Include equal keys if high inclusion is requested
	if inclusion == Both || inclusion == High {
		readEqualKeys(high, highkey, it, chkey, cherr, stopch, false)
	}
}

func (s *fdbSnapshot) GetValueSetForKeyRange(low Key, high Key,
	inclusion Inclusion, chval chan Value, cherr chan error, stopch StopChannel) {

	defer close(chval)
	defer close(cherr)

	logging.Debugf("ForestDB Received Key Low - %s High - %s Inclusion - %v for Scan",
		low.String(), high.String(), inclusion)

	it, err := newFDBSnapshotIterator(s)
	if err != nil {
		cherr <- err
		return
	}
	defer closeIterator(it)

	var lowkey []byte

	if lowkey = low.Encoded(); lowkey == nil {
		it.SeekFirst()
	} else {
		it.Seek(lowkey)
	}

	var key Key
	var val Value
	for ; it.Valid(); it.Next() {

		select {

		case <-stopch:
			//stop signalled, end processing
			return

		default:
			if key, err = NewKeyFromEncodedBytes(it.Key()); err != nil {
				logging.Errorf("Error Converting from bytes %v to key %v. Skipping row",
					it.Key(), err)
				continue
			}

			if val, err = NewValueFromEncodedBytes(it.Value()); err != nil {
				logging.Errorf("Error Converting from bytes %v to value %v, Skipping row",
					it.Value(), err)
				continue
			}

			logging.Tracef("ForestDB Got Value - %s", val.String())

			var highcmp int
			if high.Encoded() == nil {
				highcmp = -1 //if high key is nil, iterate through the fullset
			} else {
				highcmp = key.Compare(high)
			}

			var lowcmp int
			if low.Encoded() == nil {
				lowcmp = 1 //all keys are greater than nil
			} else {
				lowcmp = key.Compare(low)
			}

			if highcmp == 0 && (inclusion == Both || inclusion == High) {
				logging.Tracef("ForestDB Sending Value Equal to High Key")
				chval <- val
			} else if lowcmp == 0 && (inclusion == Both || inclusion == Low) {
				logging.Tracef("ForestDB Sending Value Equal to Low Key")
				chval <- val
			} else if (highcmp == -1) && (lowcmp == 1) { //key is between high and low
				if highcmp == -1 {
					logging.Tracef("ForestDB Sending Value Lesser Than High Key")
				} else if lowcmp == 1 {
					logging.Tracef("ForestDB Sending Value Greater Than Low Key")
				}
				chval <- val
			} else {
				logging.Tracef("ForestDB not Sending Value")
				//if we have reached past the high key, no need to scan further
				if highcmp == 1 {
					break
				}
			}
		}
	}

}

//RangeCounter interface
func (s *fdbSnapshot) CountRange(low Key, high Key, inclusion Inclusion,
	stopch StopChannel) (uint64, error) {

	logging.Debugf("ForestDB Received Key Low - %s High - %s for Scan",
		low.String(), high.String())

	chkey := make(chan Key)
	cherr := make(chan error)
	go s.GetKeySetForKeyRange(low, high, inclusion, chkey, cherr, stopch)

	var count uint64
loop:
	for {
		select {
		case _, ok := <-chkey:
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

// Keys are encoded in the form of an array [..., primaryKey]
// Scannable key is the subarray with [0:l] where l is the max prefix fields
// This method is used to transform key bytes received from index storage
func ReadScanKey(kbytes []byte, nfields int) (k []byte, err error) {
	var tmp []interface{}

	err = json.Unmarshal(kbytes, &tmp)
	if err != nil {
		err = errors.New(fmt.Sprintf("Decode failed: %v (%v)", string(kbytes), err))
		return
	}

	l := len(tmp)

	if l == 0 {
		err = errors.New("Decode failed: Invalid key")
		return
	}

	tmp = tmp[:nfields]

	k, err = json.Marshal(tmp)
	return
}

// For checking equality, its a two step operation:
// 1. Compare jsoncollate encoded prefix
// 2. If encoded prefix is equal, compare decoded full keys
func readEqualKeys(k Key, kPrefix []byte, it *ForestDBIterator,
	chkey chan Key, cherr chan error, stopch StopChannel, discard bool) {

	var err error
	var t []interface{}
	jsonKey := k.Raw()
	if jsonKey == nil {
		return
	}
	err = json.Unmarshal(jsonKey, &t)
	if err != nil {
		cherr <- err
		return
	}

	// Number of interested prefix fields for scan
	nfields := len(t)

loop:
	for ; it.Valid(); it.Next() {
		l := len(kPrefix)
		if len(it.Key()) < l {
			break loop
		}

		currKeyPrefix := it.Key()[:l]
		cmp := bytes.Compare(currKeyPrefix, kPrefix)
		select {
		case <-stopch:
			return
		default:
			// Is prefix equal ?
			if cmp == 0 {
				key, err1 := NewKeyFromEncodedBytes(it.Key())
				if err1 != nil {
					cherr <- err1
					return
				}
				jsonCurrKey, err2 := ReadScanKey(key.Raw(), nfields)
				if err != nil {
					cherr <- err2
					return
				}

				// Compare full json key
				cmp = bytes.Compare(jsonKey, jsonCurrKey)
				if cmp == 0 {
					if !discard {
						chkey <- key
					}
				} else {
					break loop
				}
			} else {
				break loop
			}
		}
	}
}

func closeIterator(it *ForestDBIterator) {
	err := it.Close()
	if err != nil {
		logging.Errorf("ForestDB iterator: dealloc failed (%v)", err)
	}
}
