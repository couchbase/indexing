// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package common

import (
	"hash/crc32"

	"github.com/couchbase/indexing/secondary/logging"
)

//KeyPartitionDefn defines a key based partition in terms of topology
//ie its Id and Indexer Endpoints hosting the partition
type KeyPartitionDefn struct {
	Id       PartitionId
	Version  int
	Endpts   []Endpoint
	ShardIds []ShardId
}

func (kp KeyPartitionDefn) GetPartitionId() PartitionId {
	return kp.Id
}

func (kp KeyPartitionDefn) GetVersion() int {
	return kp.Version
}

func (kp KeyPartitionDefn) Endpoints() []Endpoint {
	return kp.Endpts
}

func (kp KeyPartitionDefn) GetShardIds() []ShardId {
	return kp.ShardIds
}

func (kp KeyPartitionDefn) AddShardIds(sids []ShardId) {
	kp.ShardIds = sids
}

//KeyPartitionContainer implements PartitionContainer interface
//for key based partitioning
type KeyPartitionContainer struct {
	PartitionMap  map[PartitionId]KeyPartitionDefn
	NumPartitions int
	scheme        PartitionScheme
	hash          HashScheme
}

//NewKeyPartitionContainer initializes a new KeyPartitionContainer and returns
func NewKeyPartitionContainer(numPartitions int, scheme PartitionScheme, hash HashScheme) PartitionContainer {

	if !IsPartitioned(scheme) {
		numPartitions = 1
	}

	kpc := &KeyPartitionContainer{
		PartitionMap:  make(map[PartitionId]KeyPartitionDefn),
		NumPartitions: numPartitions,
		scheme:        scheme,
		hash:          hash,
	}
	return kpc

}

//AddPartition adds a partition to the container
func (pc *KeyPartitionContainer) AddPartition(id PartitionId, p PartitionDefn) {
	pc.PartitionMap[id] = p.(KeyPartitionDefn)
}

//UpdatePartition updates an existing partition to the container
func (pc *KeyPartitionContainer) UpdatePartition(id PartitionId, p PartitionDefn) {
	pc.PartitionMap[id] = p.(KeyPartitionDefn)
}

//RemovePartition removes a partition from the container
func (pc *KeyPartitionContainer) RemovePartition(id PartitionId) {
	delete(pc.PartitionMap, id)
}

//GetEndpointsByPartitionKey is a convenience method which calls other interface methods
//to first determine the partitionId from PartitionKey and then the endpoints from
//partitionId
func (pc *KeyPartitionContainer) GetEndpointsByPartitionKey(key PartitionKey) []Endpoint {

	id := pc.GetPartitionIdByPartitionKey(key)
	return pc.GetEndpointsByPartitionId(id)

}

//GetPartitionIdByPartitionKey returns the partitionId for the partition to which the
//partitionKey belongs.
func (pc *KeyPartitionContainer) GetPartitionIdByPartitionKey(key PartitionKey) PartitionId {

	if pc.scheme == KEY {
		return HashKeyPartition(key, pc.NumPartitions, pc.hash)
	}

	return PartitionId(NON_PARTITION_ID)
}

//GetEndpointsByPartitionId returns the list of Endpoints hosting the give partitionId
//or nil if partitionId is not found
func (pc *KeyPartitionContainer) GetEndpointsByPartitionId(id PartitionId) []Endpoint {

	if p, ok := pc.PartitionMap[id]; ok {
		return p.Endpoints()
	} else {
		logging.Warnf("KeyPartitionContainer: Invalid Partition Id %v", id)
		return nil
	}
}

//GetAllPartitions returns all the partitions in this partitionContainer
func (pc *KeyPartitionContainer) GetAllPartitions() []PartitionDefn {

	var partDefnList []PartitionDefn
	for _, p := range pc.PartitionMap {
		partDefnList = append(partDefnList, p)
	}
	return partDefnList
}

func (pc *KeyPartitionContainer) GetAllPartitionIds() ([]PartitionId, []int) {

	partnIds := make([]PartitionId, 0, len(pc.PartitionMap))
	versions := make([]int, 0, len(pc.PartitionMap))
	for _, partition := range pc.PartitionMap {
		partnIds = append(partnIds, partition.GetPartitionId())
		versions = append(versions, partition.GetVersion())
	}

	return partnIds, versions
}

//GetPartitionById returns the partition for the given partitionId
//or nil if partitionId is not found
func (pc *KeyPartitionContainer) GetPartitionById(id PartitionId) PartitionDefn {
	if p, ok := pc.PartitionMap[id]; ok {
		return p
	} else {
		logging.Warnf("KeyPartitionContainer: Invalid Partition Id %v", id)
		return nil
	}
}

// Check if partition with specified id exists
func (pc *KeyPartitionContainer) CheckPartitionExists(id PartitionId) bool {
	if _, ok := pc.PartitionMap[id]; ok {
		return true
	} else {
		return false
	}
}

//GetNumPartitions returns the number of partitions in this container
func (pc *KeyPartitionContainer) GetNumPartitions() int {
	return pc.NumPartitions
}

func (pc *KeyPartitionContainer) Clone() PartitionContainer {
	clone := NewKeyPartitionContainer(pc.NumPartitions, pc.scheme, pc.hash)

	for id, partition := range pc.PartitionMap {
		clone.AddPartition(id, partition)
	}

	return clone
}

func HashKeyPartition(key []byte, numPartitions int, scheme HashScheme) PartitionId {

	//run hash function on partition key and return partition id
	hash := crc32.ChecksumIEEE([]byte(key))
	partnId := (int(hash) % numPartitions) + 1
	return PartitionId(partnId)
}
