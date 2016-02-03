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
	"github.com/couchbase/indexing/secondary/collatejson"
	"github.com/couchbase/indexing/secondary/common"
)

// Given the input secondary key, this method creates the product of array items
// with all other items in the composite secondary key
// Example: if input key is [35, ["Dave", "Ann", "Pete"]] and arrayPos = 1, this generates the product as:
// [30, "Dave"] , [30, "Ann"], [30, "Pete"]
func splitSecondaryArrayKey(key []byte, arrayPos int, tmpBuf []byte) ([][][]byte, error) {
	var arrayLen int
	var arrayItem [][]byte
	var err2 error

	codec := collatejson.NewCodec(16)
	bufPtr := encBufPool.Get()
	defer encBufPool.Put(bufPtr)
	secKeyObject, err := codec.ExplodeArray(key, tmpBuf)
	common.CrashOnError(err)

	hasArray := false
	insideArr := secKeyObject[arrayPos]
	if arrayItem, err2 = codec.ExplodeArray(insideArr, tmpBuf); err2 == nil {
		arrayLen = len(arrayItem)
		hasArray = true
	}

	arrayIndexEntries := make([][][]byte, 0, len(secKeyObject))

	// Handle empty array
	if arrayLen == 0 {
		element := make([][]byte, len(secKeyObject))
		for i, item := range secKeyObject {
			if i == arrayPos {
				element[i] = nil // Todo: Is it nil or Byte version of "[]" ?
			} else {
				element[i] = item
			}
		}
		arrayIndexEntries = append(arrayIndexEntries, element)
		return arrayIndexEntries, nil
	}

	if hasArray {
		for _, element := range arrayItem {
			element2 := make([][]byte, len(secKeyObject))
			copy(element2, secKeyObject)
			element2[arrayPos] = element
			arrayIndexEntries = append(arrayIndexEntries, element2)
		}
	}

	return arrayIndexEntries, nil
}

func ArrayIndexItems(bs []byte, arrPos int, buf []byte) ([][]byte, error) {
	var items [][]byte
	var err error

	itemArrays, err := splitSecondaryArrayKey(bs, arrPos, buf)
	if err != nil {
		return nil, err
	}

	codec := collatejson.NewCodec(16)
	for _, arr := range itemArrays {
		if bs, err = codec.JoinArray(arr, buf); err != nil {
			return nil, err
		}
		items = append(items, bs)
		l := len(bs)
		buf = buf[l:l]
	}

	return items, nil
}

// Compare two arrays of byte arrays and find out diff of which byte entry needs to be deleted and which needs to be inserted
func CompareArrayEntryBytes(newKey, oldKey [][]byte) ([][]byte, [][]byte) {
	// Find out all entries to be added
	for i := 0; i < len(newKey); i++ {
		found := false
		for j := 0; j < len(oldKey); j++ {
			if bytes.Compare(newKey[i], oldKey[j]) == 0 {
				// The item is present in both old and new. Mark the element in old as nil
				oldKey[j] = nil
				found = true
			}
		}
		if found == true {
			newKey[i] = nil // The item is present in both old and new. Mark the element in new as nil
		}
	}
	return newKey, oldKey
}
