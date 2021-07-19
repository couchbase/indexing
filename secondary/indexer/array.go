// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"errors"

	"github.com/couchbase/indexing/secondary/collatejson"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

var (
	ErrArrayItemKeyTooLong = errors.New("Array item key too long")
	ErrArrayKeyTooLong     = errors.New("Array to be indexed too long")
)

// Given the input secondary key, this method creates the product of array items
// with all other items in the composite secondary key
// Example: if input key is [35, ["Dave", "Ann", "Pete"]] and arrayPos = 1, this generates the product as:
// [30, "Dave"] , [30, "Ann"], [30, "Pete"]
//
// When index is created with "FLATTEN_KEYS" keyword, the array items are flattened
// E.g., create index idx on default(org, DISTINCT ARRAY FLATTEN_KEYS(v.name, v.age) for v in dob END)
// If the input key is: ["Couchbase", [["Alice", 24], ["Bob", 24]]], then this method generates
// output as ["Couchbase", "Alice", 24], ["Couchbase", "Bob", 24].
//
// With out FLATTEN support, for the input key: ["Couchbase", [["Alice", 24], ["Bob", 24]]],
// the output would be ["Couchbase", ["Alice", 24]], ["Couchbase", ["Bob", 24]]
func splitSecondaryArrayKey(key []byte, arrayPos int, isFlatten bool, tmpBuf []byte) ([][][]byte, error) {
	var arrayLen int
	var arrayItem [][]byte

	codec := collatejson.NewCodec(16)
	secKeyObject, err := codec.ExplodeArray4(key, tmpBuf)
	if err != nil {
		if err == collatejson.ErrorOutputLen {
			newBuf1 := make([]byte, 0, len(key)*3)
			secKeyObject, err = codec.ExplodeArray4(key, newBuf1)
		}
		if err != nil {
			return nil, err
		}
	}

	hasArray := false
	insideArr := secKeyObject[arrayPos]
	arrayItem, err = codec.ExplodeArray4(insideArr, tmpBuf)
	if err != nil {
		if err == collatejson.ErrorOutputLen {
			newBuf2 := make([]byte, 0, len(insideArr)*3)
			arrayItem, err = codec.ExplodeArray4(insideArr, newBuf2)
		}
		if err != nil {
			return nil, err
		}
	}
	arrayLen = len(arrayItem)
	hasArray = true

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

	if isFlatten {
		arrayItem, err = FlattenArray(arrayItem, tmpBuf, codec)
		if err != nil {
			return nil, err
		}
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

func ArrayIndexItems(bs []byte, arrPos int, buf []byte,
	isDistinct, isFlatten, checkSize bool, sz keySizeConfig) ([][]byte, []int, int, error) {
	var items [][]byte
	var err error

	itemArrays, err := splitSecondaryArrayKey(bs, arrPos, isFlatten, buf)
	if err != nil {
		return nil, nil, len(buf), err
	}

	// The exploded keys in itemArrays point to incoming encoded key.
	// The buffer is free to be re-used at this point. The buffer is used to
	// form joined keys to be indexed. Check if size of buffer is sufficient
	// to hold all joined entries. If not, reallocate the buffer. This is to avoid
	// overallocation through append in collatejson JoinArray
	totalSz := 0
	for i := range itemArrays {
		for j := range itemArrays[i] {
			totalSz += len(itemArrays[i][j]) + 2 // add 2 for TypeArray and Terminator
		}
	}

	if totalSz > cap(buf) {
		buf = make([]byte, 0, totalSz+RESIZE_PAD)
	}

	codec := collatejson.NewCodec(16)
	for _, arr := range itemArrays {
		from := len(buf)
		if buf, err = codec.JoinArray(arr, buf); err != nil {
			return nil, nil, len(buf), err
		}
		l := len(buf)
		if checkSize && (l-from) > sz.maxSecKeyBufferLen {
			logging.Errorf("Encoded array item key too long. Length of key = %v, Limit = %v", l-from, sz.maxSecKeyBufferLen)
			return nil, nil, len(buf), ErrArrayItemKeyTooLong
		}
		if checkSize && l > sz.maxArrayIndexEntrySize {
			logging.Errorf("Encoded array key too long. Length of key = %v, Limit = %v", l, sz.maxArrayIndexEntrySize)
			return nil, nil, len(buf), ErrArrayKeyTooLong
		}

		items = append(items, buf[from:l])
	}

	arrayItemsWithCount := make([][]byte, 0, len(items))
	keyCount := make([]int, 0, len(items))

	// Note: This map is built on top of converting byte slice to string without
	// allocating additional memory. So, escaping this map out of this method
	// can lead of violation of immutability property of the string. Hence,
	// using this map here only as a placeholder to compute arrayItemsWithCount
	// optimally even though some optimisations are possible by escaping this map
	// out of this method (E.g., there is no need to do additional conversion in
	// CompareArrayEntriesWithCount method, if we return map here)
	// key -> item as string; value -> item's position in arrayItemsWithCount
	groupedItems := make(map[string]int)

	for _, item := range items {
		str := common.ByteSliceToString(item)
		if index, ok := groupedItems[str]; !ok {
			index = len(arrayItemsWithCount)
			arrayItemsWithCount = append(arrayItemsWithCount, item)
			keyCount = append(keyCount, 1)
			groupedItems[str] = index
		} else {
			if !isDistinct {
				keyCount[index]++
			}
		}
	}

	return arrayItemsWithCount, keyCount, len(buf), nil

}

// Compare two arrays of byte arrays
// and find out diff of which byte entry
// needs to be deleted and which needs to be inserted
// Both the byte arrays are expected to have unique entries
func CompareArrayEntriesWithCount(newKey, oldKey [][]byte, newKeyCount, oldKeyCount []int) ([][]byte, [][]byte) {
	// Convert newKey into map[string]int
	// key to this map -> byte slice converted to string
	// value of this map -> index of entry in newKey
	newKeyMap := make(map[string]int)
	for newItemIndex, newItem := range newKey {
		str := common.ByteSliceToString(newItem)
		newKeyMap[str] = newItemIndex
	}

	newEntriesToBeDeleted := make([]int, 0)
	for oldItemIndex, oldItem := range oldKey {
		str := common.ByteSliceToString(oldItem)
		if newItemIndex, ok := newKeyMap[str]; ok && newKeyCount[newItemIndex] == oldKeyCount[oldItemIndex] {
			// Item exists in both oldKey and newKey
			newEntriesToBeDeleted = append(newEntriesToBeDeleted, newItemIndex)
			oldKey[oldItemIndex] = nil
		}
	}

	for _, index := range newEntriesToBeDeleted {
		newKey[index] = nil
	}
	return newKey, oldKey
}

func FlattenArray(arrayItem [][]byte, tmpBuf []byte, codec *collatejson.Codec) ([][]byte, error) {

	for itemIndex, arrItem := range arrayItem {
		flattenedEntries, err := codec.ExplodeArray4(arrItem, tmpBuf)
		if err != nil {
			if err == collatejson.ErrorOutputLen {
				newBuf2 := make([]byte, 0, len(arrItem)*3)
				flattenedEntries, err = codec.ExplodeArray4(arrItem, newBuf2)
			}
			if err != nil {
				return nil, err
			}
		}
		joinBuf := make([]byte, 0, len(arrItem))
		// Join all the flattened entries
		for _, entry := range flattenedEntries {
			joinBuf = append(joinBuf, entry...)
		}
		arrayItem[itemIndex] = joinBuf
	}
	return arrayItem, nil
}
