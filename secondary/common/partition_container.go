// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

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
	GetAllPartitionIds() []PartitionId
	GetPartitionById(PartitionId) PartitionDefn
	GetNumPartitions() int
}

//PartitionDefn is a generic interface which defines
//a partition
type PartitionDefn interface {
	GetPartitionId() PartitionId
	Endpoints() []Endpoint
}
