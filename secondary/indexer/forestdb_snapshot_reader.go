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
	"log"
)

//Counter interface
func (s *fdbSnapshot) CountTotal() (uint64, error) {

	var nilKey Key
	var err error
	if nilKey, err = NewKeyFromEncodedBytes(nil); err != nil {
		return 0, err
	}

	return s.CountRange(nilKey, nilKey, Both)
}

//Exister interface
func (s *fdbSnapshot) Exists(key Key) bool {

	var totalRows uint64
	var err error
	if totalRows, err = s.CountRange(key, key, Both); err != nil {
		return false
	}
	if totalRows > 0 {
		return true
	}
	return false
}

//Looker interface
func (s *fdbSnapshot) Lookup(key Key) (chan Value, chan error) {
	chval := make(chan Value)
	cherr := make(chan error)

	log.Println("Received Lookup Query for Key %s", key.String())
	go s.GetValueSetForKeyRange(key, key, Both, chval, cherr)
	return chval, cherr
}

func (s *fdbSnapshot) KeySet() (chan Key, chan error) {
	chkey := make(chan Key)
	cherr := make(chan error)

	nilKey, _ := NewKeyFromEncodedBytes(nil)
	go s.GetKeySetForKeyRange(nilKey, nilKey, Both, chkey, cherr)
	return chkey, cherr
}

func (s *fdbSnapshot) ValueSet() (chan Value, chan error) {
	chval := make(chan Value)
	cherr := make(chan error)

	nilKey, _ := NewKeyFromEncodedBytes(nil)
	go s.GetValueSetForKeyRange(nilKey, nilKey, Both, chval, cherr)
	return chval, cherr
}

//Ranger
func (s *fdbSnapshot) KeyRange(low, high Key, inclusion Inclusion) (
	chan Key, chan error, SortOrder) {

	chkey := make(chan Key)
	cherr := make(chan error)

	go s.GetKeySetForKeyRange(low, high, inclusion, chkey, cherr)
	return chkey, cherr, Asc
}

func (s *fdbSnapshot) ValueRange(low, high Key, inclusion Inclusion) (
	chan Value, chan error, SortOrder) {

	chval := make(chan Value)
	cherr := make(chan error)

	go s.GetValueSetForKeyRange(low, high, inclusion, chval, cherr)
	return chval, cherr, Asc
}

func (s *fdbSnapshot) GetKeySetForKeyRange(low Key, high Key,
	inclusion Inclusion, chkey chan Key, cherr chan error) {

	defer close(chkey)
	defer close(cherr)

	log.Printf("ForestDB Received Key Low - %s High - %s for Scan", low.String(), high.String())

	it := newForestDBIterator(s.main)
	defer it.Close()

	var lowkey []byte
	var err error

	if lowkey = low.EncodedBytes(); lowkey == nil {
		it.SeekFirst()
	} else {
		it.Seek(lowkey)
	}

	var key Key
	for it = it; it.Valid(); it.Next() {
		if key, err = NewKeyFromEncodedBytes(it.Key()); err != nil {
			log.Printf("Error Converting from bytes %v to key %v. Skipping row", it.Key(), err)
			continue
		}

		log.Printf("ForestDB Got Key - %s", key.String())

		var highcmp int
		if high.EncodedBytes() == nil {
			highcmp = -1 //if high key is nil, iterate through the fullset
		} else {
			highcmp = key.Compare(high)
		}

		var lowcmp int
		if low.EncodedBytes() == nil {
			lowcmp = 1 //all keys are greater than nil
		} else {
			lowcmp = key.Compare(low)
		}

		if highcmp == 0 && (inclusion == Both || inclusion == High) {
			log.Printf("ForestDB Sending Key Equal to High Key")
			chkey <- key
		} else if lowcmp == 0 && (inclusion == Both || inclusion == Low) {
			log.Printf("ForestDB Sending Key Equal to Low Key")
			chkey <- key
		} else if (highcmp == -1) && (lowcmp == 1) { //key is between high and low
			if highcmp == -1 {
				log.Printf("ForestDB Sending Key Lesser Than High Key")
			} else if lowcmp == 1 {
				log.Printf("ForestDB Sending Key Greater Than Low Key")
			}
			chkey <- key
		} else {
			log.Printf("ForestDB not Sending Key")
			//if we have reached past the high key, no need to scan further
			if highcmp == 1 {
				break
			}
		}
	}

}

func (s *fdbSnapshot) GetValueSetForKeyRange(low Key, high Key,
	inclusion Inclusion, chval chan Value, cherr chan error) {

	defer close(chval)
	defer close(cherr)

	log.Printf("ForestDB Received Key Low - %s High - %s Inclusion - %v for Scan", low.String(), high.String(), inclusion)

	it := newForestDBIterator(s.main)
	defer it.Close()

	var lowkey []byte
	var err error

	if lowkey = low.EncodedBytes(); lowkey == nil {
		it.SeekFirst()
	} else {
		it.Seek(lowkey)
	}

	var key Key
	var val Value
	for it = it; it.Valid(); it.Next() {
		if key, err = NewKeyFromEncodedBytes(it.Key()); err != nil {
			log.Printf("Error Converting from bytes %v to key %v. Skipping row", it.Key(), err)
			continue
		}

		if val, err = NewValueFromEncodedBytes(it.Value()); err != nil {
			log.Printf("Error Converting from bytes %v to value %v, Skipping row", it.Value(), err)
			continue
		}

		log.Printf("ForestDB Got Value - %s", val.String())

		var highcmp int
		if high.EncodedBytes() == nil {
			highcmp = -1 //if high key is nil, iterate through the fullset
		} else {
			highcmp = key.Compare(high)
		}

		var lowcmp int
		if low.EncodedBytes() == nil {
			lowcmp = 1 //all keys are greater than nil
		} else {
			lowcmp = key.Compare(low)
		}

		if highcmp == 0 && (inclusion == Both || inclusion == High) {
			log.Printf("ForestDB Sending Value Equal to High Key")
			chval <- val
		} else if lowcmp == 0 && (inclusion == Both || inclusion == Low) {
			log.Printf("ForestDB Sending Value Equal to Low Key")
			chval <- val
		} else if (highcmp == -1) && (lowcmp == 1) { //key is between high and low
			if highcmp == -1 {
				log.Printf("ForestDB Sending Value Lesser Than High Key")
			} else if lowcmp == 1 {
				log.Printf("ForestDB Sending Value Greater Than Low Key")
			}
			chval <- val
		} else {
			log.Printf("ForestDB not Sending Value")
			//if we have reached past the high key, no need to scan further
			if highcmp == 1 {
				break
			}
		}
	}

}

//RangeCounter interface
func (s *fdbSnapshot) CountRange(low Key, high Key, inclusion Inclusion) (
	uint64, error) {

	var count uint64

	it := newForestDBIterator(s.main)
	defer it.Close()

	log.Printf("ForestDB Received Key Low - %s High - %s for Scan", low.String(), high.String())

	var lowkey []byte
	var err error

	if lowkey = low.EncodedBytes(); lowkey == nil {
		it.SeekFirst()
	} else {
		it.Seek(lowkey)
	}

	var key Key
	for it = it; it.Valid(); it.Next() {
		if key, err = NewKeyFromEncodedBytes(it.Key()); err != nil {
			log.Printf("Error Converting from bytes %v to key %v. Skipping row", it.Key(), err)
			continue
		}

		log.Printf("ForestDB Got Key - %s", key.String())

		var highcmp int
		if high.EncodedBytes() == nil {
			highcmp = -1 //if high key is nil, iterate through the fullset
		} else {
			highcmp = key.Compare(high)
		}

		var lowcmp int
		if low.EncodedBytes() == nil {
			lowcmp = 1 //all keys are greater than nil
		} else {
			lowcmp = key.Compare(low)
		}

		if highcmp == 0 && (inclusion == Both || inclusion == High) {
			log.Printf("ForestDB Got Value Equal to High Key")
			count++
		} else if lowcmp == 0 && (inclusion == Both || inclusion == Low) {
			log.Printf("ForestDB Got Value Equal to Low Key")
			count++
		} else if (highcmp == -1) && (lowcmp == 1) { //key is between high and low
			if highcmp == -1 {
				log.Printf("ForestDB Got Value Lesser Than High Key")
			} else if lowcmp == 1 {
				log.Printf("ForestDB Got Value Greater Than Low Key")
			}
			count++
		} else {
			log.Printf("ForestDB not Sending Value")
			//if we have reached past the high key, no need to scan further
			if highcmp == 1 {
				break
			}
		}
	}

	return count, nil
}
