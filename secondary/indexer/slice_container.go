// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"hash/crc32"
)

//SliceContainer contains all slices for an index partition
//and provides methods to determine how data
//is distributed in multiple slices for a single partition
type SliceContainer interface {
	//Add Slice to container
	AddSlice(SliceId, Slice)

	//Update existing slice
	UpdateSlice(SliceId, Slice)

	//Remove existing slice
	RemoveSlice(SliceId)

	//Return Slice for the given IndexKey
	GetSliceByIndexKey(common.IndexKey) Slice

	//Return SliceId for the given IndexKey
	GetSliceIdByIndexKey(common.IndexKey) SliceId

	//Return Slice for the given SliceId
	GetSliceById(SliceId) Slice

	//Return all Slices
	GetAllSlices() []Slice
}

//hashedSliceContainer provides a hash based implementation
//for SliceContainer. Each IndexKey is hashed to determine
//which slice it belongs to.
type HashedSliceContainer struct {
	SliceMap  map[SliceId]Slice
	NumSlices int
}

//NewHashedSliceContainer initializes a new HashedSliceContainer and returns
func NewHashedSliceContainer() *HashedSliceContainer {

	hsc := &HashedSliceContainer{SliceMap: make(map[SliceId]Slice),
		NumSlices: 0}
	return hsc

}

//AddSlice adds a slice to the container
func (sc *HashedSliceContainer) AddSlice(id SliceId, s Slice) {
	sc.SliceMap[id] = s
	sc.NumSlices++
}

//UpdateSlice updates an existing slice to the container
func (sc *HashedSliceContainer) UpdateSlice(id SliceId, s Slice) {
	sc.SliceMap[id] = s
}

//RemoveSlice removes a slice from the container
func (sc *HashedSliceContainer) RemoveSlice(id SliceId) {
	delete(sc.SliceMap, id)
	sc.NumSlices--
}

//GetSliceByIndexKey returns Slice for the given IndexKey
//This is a convenience method which calls other interface methods
//to first determine the sliceId from IndexKey and then the slice from
//sliceId
func (sc *HashedSliceContainer) GetSliceByIndexKey(key common.IndexKey) Slice {

	id := sc.GetSliceIdByIndexKey(key)
	return sc.GetSliceById(id)

}

//GetSliceIdByIndexKey returns SliceId for the given IndexKey
func (sc *HashedSliceContainer) GetSliceIdByIndexKey(key common.IndexKey) SliceId {

	//run hash function on index key and return slice id
	hash := crc32.ChecksumIEEE([]byte(key))
	sliceId := int(hash) % sc.NumSlices

	return SliceId(sliceId)
}

//GetSliceById returns Slice for the given SliceId
func (sc *HashedSliceContainer) GetSliceById(id SliceId) Slice {

	if s, ok := sc.SliceMap[id]; ok && s != nil {
		return s
	} else {
		logging.Warnf("HashedSliceContainer: Invalid Slice Id %v", id)
		return nil
	}
}

//GetAllSlices returns all slices from the container
func (sc *HashedSliceContainer) GetAllSlices() []Slice {

	var sliceList []Slice

	for _, slice := range sc.SliceMap {
		sliceList = append(sliceList, slice)
	}

	return sliceList

}
