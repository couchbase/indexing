// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package common

type PartitionKey []byte
type PartitionId int

//Endpoint provides an Indexer address(host:port)
//which is hosting a partition
type Endpoint string //host:port

//PartitionContainer contains all the partitions for an index instance
//and provides methods to lookup topology information(i.e. endpoints)
//based on the PartitionKey
type PartitionContainer interface {
	AddPartition(PartitionId, PartitionDefn)
	UpdatePartition(PartitionId, PartitionDefn)
	RemovePartition(PartitionId)

	GetEndpointsByPartitionKey(PartitionKey) []Endpoint
	GetPartitionIdByPartitionKey(PartitionKey) PartitionId
	GetEndpointsByPartitionId(PartitionId) []Endpoint

	GetAllPartitions() []PartitionDefn
	GetAllPartitionIds() ([]PartitionId, []int)
	GetPartitionById(PartitionId) PartitionDefn
	GetNumPartitions() int

	Clone() PartitionContainer
	CheckPartitionExists(PartitionId) bool
}

//PartitionDefn is a generic interface which defines
//a partition
type PartitionDefn interface {
	GetPartitionId() PartitionId
	GetVersion() int
	Endpoints() []Endpoint
	GetShardIds() []ShardId
	AddShardIds([]ShardId)
}
