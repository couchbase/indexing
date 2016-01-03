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
)

// Given raw Json bytes of a key and arrayPosition,
// this method creates the product of array items with other items
func SplitSecondaryArrayKey(rawKey []byte, arrayPosition int) ([][]interface{}, error) {
	var secKeyObject []interface{}
	if err := json.Unmarshal(rawKey, &secKeyObject); err != nil {
		return nil, err
	}
	return SplitSecondaryArrayKey2(secKeyObject, arrayPosition), nil
}

// Given the input secondary key, this method creates the product of array items
// with all other items in the composite secondary key
// Example: if input key is [35, ["Dave", "Ann", "Pete"]] and arrayPos = 1, this generates the product as:
// [30, "Dave"] , [30, "Ann"], [30, "Pete"]]
func SplitSecondaryArrayKey2(secKeyObject []interface{}, arrayPosition int) [][]interface{} {
	arrayIndexEntries := make([][]interface{}, 0, len(secKeyObject))

	var arrayLen int
	if arrayItem, ok := secKeyObject[arrayPosition].([]interface{}); ok {
		arrayLen = len(arrayItem)
	}

	// Handle empty array
	if arrayLen == 0 {
		element := make([]interface{}, len(secKeyObject))
		for i, item := range secKeyObject {
			if i == arrayPosition {
				element[i] = nil // Todo: Is it nil or Byte version of "[]" ?
			} else {
				element[i] = item
			}
		}
		arrayIndexEntries = append(arrayIndexEntries, element)
		return arrayIndexEntries
	}

	if elements, ok := secKeyObject[arrayPosition].([]interface{}); ok {
		for _, element := range elements {
			element2 := make([]interface{}, len(secKeyObject))
			copy(element2, secKeyObject)
			element2[arrayPosition] = element
			arrayIndexEntries = append(arrayIndexEntries, element2)
		}
	}

	return arrayIndexEntries
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
