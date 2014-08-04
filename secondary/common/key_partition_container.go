// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package common

import (
	"hash/crc32"
	"log"
)

//KeyPartitionDefn defines a key based partition in terms of topology
//ie its Id and Indexer Endpoints hosting the partition
type KeyPartitionDefn struct {
	Id     PartitionId
	Endpts []Endpoint
}

func (kp KeyPartitionDefn) GetPartitionId() PartitionId {
	return kp.Id
}

func (kp KeyPartitionDefn) Endpoints() []Endpoint {
	return kp.Endpts
}

//KeyPartitionContainer implements PartitionContainer interface
//for key based partitioning
type KeyPartitionContainer struct {
	PartitionMap  map[PartitionId]KeyPartitionDefn
	NumPartitions int
}

//NewKeyPartitionContainer initializes a new KeyPartitionContainer and returns
func NewKeyPartitionContainer() PartitionContainer {

	kpc := &KeyPartitionContainer{PartitionMap: make(map[PartitionId]KeyPartitionDefn),
		NumPartitions: 0}
	return kpc

}

//AddPartition adds a partition to the container
func (pc *KeyPartitionContainer) AddPartition(id PartitionId, p PartitionDefn) {
	pc.PartitionMap[id] = p.(KeyPartitionDefn)
	pc.NumPartitions++
}

//UpdatePartition updates an existing partition to the container
func (pc *KeyPartitionContainer) UpdatePartition(id PartitionId, p PartitionDefn) {
	pc.PartitionMap[id] = p.(KeyPartitionDefn)
}

//RemovePartition removes a partition from the container
func (pc *KeyPartitionContainer) RemovePartition(id PartitionId) {
	delete(pc.PartitionMap, id)
	pc.NumPartitions--
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
	//run hash function on partition key and return partition id
	hash := crc32.ChecksumIEEE([]byte(key))
	partnId := int(hash) % pc.NumPartitions
	return PartitionId(partnId)
}

//GetEndpointsByPartitionId returns the list of Endpoints hosting the give partitionId
//or nil if partitionId is not found
func (pc *KeyPartitionContainer) GetEndpointsByPartitionId(id PartitionId) []Endpoint {

	if p, ok := pc.PartitionMap[id]; ok {
		return p.Endpoints()
	} else {
		log.Printf("KeyPartitionContainer: Invalid Partition Id %v", id)
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

//GetPartitionById returns the partition for the given partitionId
//or nil if partitionId is not found
func (pc *KeyPartitionContainer) GetPartitionById(id PartitionId) PartitionDefn {
	if p, ok := pc.PartitionMap[id]; ok {
		return p
	} else {
		log.Printf("KeyPartitionContainer: Invalid Partition Id %v", id)
		return nil
	}
}

//GetNumPartitions returns the number of partitions in this container
func (pc *KeyPartitionContainer) GetNumPartitions() int {
	return pc.NumPartitions
}
