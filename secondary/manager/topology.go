// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package manager

import (
	"github.com/couchbase/indexing/secondary/common"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
	"github.com/couchbaselabs/goprotobuf/proto"
	"net"
)

/////////////////////////////////////////////////////////////////////////
// Type Definition
////////////////////////////////////////////////////////////////////////

type GlobalTopology struct {
	TopologyKeys []string `json:"topologyKeys,omitempty"`
}

type IndexTopology struct {
	Version     uint64                  `json:"version,omitempty"`
	Bucket      string                  `json:"bucket,omitempty"`
	Definitions []IndexDefnDistribution `json:"definitions,omitempty"`
}

type IndexDefnDistribution struct {
	Bucket    string                  `json:"bucket,omitempty"`
	Name      string                  `json:"name,omitempty"`
	DefnId    uint64                  `json:"defnId,omitempty"`
	Instances []IndexInstDistribution `json:"instances,omitempty"`
}

type IndexInstDistribution struct {
	InstId     uint64                  `json:"instId,omitempty"`
	State      uint32                  `json:"state,omitempty"`
	StreamId   uint32                  `json:"steamId,omitempty"`
	Error      string                  `json:"error,omitempty"`
	Partitions []IndexPartDistribution `json:"partitions,omitempty"`
}

type IndexPartDistribution struct {
	PartId          uint64                      `json:"partId,omitempty"`
	SinglePartition IndexSinglePartDistribution `json:"singlePartition,omitempty"`
	KeyPartition    IndexKeyPartDistribution    `json:"keyPartition,omitempty"`
}

type IndexSinglePartDistribution struct {
	Slices []IndexSliceLocator `json:"slices,omitempty"`
}

type IndexKeyPartDistribution struct {
	Keys             []string                      `json:"keys,omitempty"`
	SinglePartitions []IndexSinglePartDistribution `json:"singlePartitions,omitempty"`
}

type IndexSliceLocator struct {
	SliceId uint64 `json:"sliceId,omitempty"`
	State   uint32 `json:"state,omitempty"`
	Host    string `json:"host,omitempty"`
}

//
// topologyChange captures changes in a topology
//
type changeRecord struct {
	definition *IndexDefnDistribution
	instance   *IndexInstDistribution
}

/////////////////////////////////////////////////////////////////////////
// Global Topology Maintenance
////////////////////////////////////////////////////////////////////////

// Add a topology key
func (g *GlobalTopology) AddTopologyKeyIfNecessary(key string) bool {
	for _, topkey := range g.TopologyKeys {
		if topkey == key {
			return false
		}
	}

	g.TopologyKeys = append(g.TopologyKeys, key)
	return true
}

// Remove a topology key
func (g *GlobalTopology) RemoveTopologyKey(key string) {
	for i, topkey := range g.TopologyKeys {
		if topkey == key {
			if i < len(g.TopologyKeys)-1 {
				g.TopologyKeys = append(g.TopologyKeys[:i], g.TopologyKeys[i+1:]...)
			} else {
				g.TopologyKeys = g.TopologyKeys[:i]
			}
			break
		}
	}
}

/////////////////////////////////////////////////////////////////////////
// Topology Maintenance
////////////////////////////////////////////////////////////////////////

//
// Add an index definition to Topology.
//
func (t *IndexTopology) AddIndexDefinition(bucket string, name string, defnId uint64, instId uint64, state uint32, host string) {

	t.RemoveIndexDefinition(bucket, name)

	slice := new(IndexSliceLocator)
	slice.SliceId = 0
	slice.Host = host
	slice.State = state

	part := new(IndexPartDistribution)
	part.PartId = 0
	part.SinglePartition.Slices = append(part.SinglePartition.Slices, *slice)

	inst := new(IndexInstDistribution)
	inst.InstId = instId
	inst.State = state
	inst.Partitions = append(inst.Partitions, *part)

	defn := new(IndexDefnDistribution)
	defn.Bucket = bucket
	defn.Name = name
	defn.DefnId = defnId
	defn.Instances = append(defn.Instances, *inst)

	t.Definitions = append(t.Definitions, *defn)
}

//
// Remove an index definition to Topology.
//
func (t *IndexTopology) RemoveIndexDefinition(bucket string, name string) {

	for i, defnRef := range t.Definitions {
		if defnRef.Bucket == bucket && defnRef.Name == name {
			if i == len(t.Definitions)-1 {
				t.Definitions = t.Definitions[:i]
			} else {
				t.Definitions = append(t.Definitions[0:i], t.Definitions[i+1:]...)
			}
			return
		}
	}
}

func (t *IndexTopology) RemoveIndexDefinitionById(id common.IndexDefnId) {

	for i, defnRef := range t.Definitions {
		if common.IndexDefnId(defnRef.DefnId) == id {
			if i == len(t.Definitions)-1 {
				t.Definitions = t.Definitions[:i]
			} else {
				t.Definitions = append(t.Definitions[0:i], t.Definitions[i+1:]...)
			}
			return
		}
	}
}

//
// Get all index instance Id's for a specific defnition
//
func (t *IndexTopology) FindIndexDefinition(bucket string, name string) *IndexDefnDistribution {

	for _, defnRef := range t.Definitions {
		if defnRef.Bucket == bucket && defnRef.Name == name {
			return &defnRef
		}
	}
	return nil
}

//
// Get all index instance Id's for a specific defnition
//
func (t *IndexTopology) FindIndexDefinitionById(id common.IndexDefnId) *IndexDefnDistribution {

	for _, defnRef := range t.Definitions {
		if defnRef.DefnId == uint64(id) {
			return &defnRef
		}
	}
	return nil
}

//
// Update Index Status on instance
//
func (t *IndexTopology) GetIndexInstByDefn(defnId common.IndexDefnId) *IndexInstDistribution {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			for _, inst := range t.Definitions[i].Instances {
				return &inst
			}
		}
	}

	return nil
}

//
// Update Index Status on instance
//
func (t *IndexTopology) UpdateStateForIndexInstByDefn(defnId common.IndexDefnId, state common.IndexState) {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			for j, _ := range t.Definitions[i].Instances {
				t.Definitions[i].Instances[j].State = uint32(state)
				common.Debugf("IndexTopology.UpdateStateForIndexInstByDefn(): Update index '%v' inst '%v' state to '%v'",
					defnId, t.Definitions[i].Instances[j].InstId, t.Definitions[i].Instances[j].State)
			}
		}
	}
}

//
// Update StreamId on instance
//
func (t *IndexTopology) UpdateStreamForIndexInstByDefn(defnId common.IndexDefnId, stream common.StreamId) {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			for j, _ := range t.Definitions[i].Instances {
				t.Definitions[i].Instances[j].StreamId = uint32(stream)
				common.Debugf("IndexTopology.UpdateStreamForIndexInstByDefn(): Update index '%v' inst '%v stream to '%v'",
					defnId, t.Definitions[i].Instances[j].InstId, t.Definitions[i].Instances[j].StreamId)
			}
		}
	}
}

//
// Set Error on instance
//
func (t *IndexTopology) SetErrorForIndexInstByDefn(defnId common.IndexDefnId, errorStr string) {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			for j, _ := range t.Definitions[i].Instances {
				t.Definitions[i].Instances[j].Error = errorStr
				common.Debugf("IndexTopology.SetErrorForIndexInstByDefn(): Set error for index '%v' inst '%v.  Error = '%v'",
					defnId, t.Definitions[i].Instances[j].InstId, t.Definitions[i].Instances[j].Error)
			}
		}
	}
}

//
// Update Index Status on instance
//
func (t *IndexTopology) ChangeStateForIndexInstByDefn(defnId common.IndexDefnId, fromState, toState common.IndexState) {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			for j, _ := range t.Definitions[i].Instances {
				if t.Definitions[i].Instances[j].State == uint32(fromState) {
					t.Definitions[i].Instances[j].State = uint32(toState)
					common.Debugf("IndexTopology.UpdateStateForIndexInstByDefn(): Update index '%v' inst '%v' state to '%v'",
						defnId, t.Definitions[i].Instances[j].InstId, t.Definitions[i].Instances[j].State)
				}
			}
		}
	}
}

//
// Update Index Status on instance
//
func (t *IndexTopology) GetHostForIndexInstByDefn(defnId common.IndexDefnId) []string {

	var endpoints []string = nil

	for _, defnRef := range t.Definitions {
		if defnRef.DefnId == uint64(defnId) {
			for _, inst := range defnRef.Instances {
				for _, partition := range inst.Partitions {
					for _, slice := range partition.SinglePartition.Slices {
						endpoints = append(endpoints, slice.Host)
					}
				}
			}
		}
	}

	return endpoints
}

//
// Get all index instance Id's for a specific defnition
//
func GetIndexInstancesIdByDefn(mgr *IndexManager, bucket string, defnId common.IndexDefnId) ([]uint64, error) {
	// Get the topology from the dictionary
	topology, err := mgr.GetTopologyByBucket(bucket)
	if err != nil {
		// TODO: Determine if it is a real error, or just topology does not exist in dictionary
		// If there is an error, return an empty array.  This assume that the topology does not exist.
		common.Debugf("GetIndexInstancesByDefn(): Cannot find topology for bucket %s.  Skip.", bucket)
		return nil, nil
	}

	var result []uint64 = nil

	for _, defnRef := range topology.Definitions {
		if defnRef.DefnId == uint64(defnId) {
			for _, inst := range defnRef.Instances {
				result = append(result, inst.InstId)
			}
			break
		}
	}

	return result, nil
}

//
// Get all deleted index instance Id's
//
func GetAllDeletedIndexInstancesId(mgr *IndexManager, buckets []string) ([]uint64, error) {

	var result []uint64 = nil

	// Get the topology from the dictionary
	for _, bucket := range buckets {
		topology, err := mgr.GetTopologyByBucket(bucket)
		if err != nil {
			// TODO: Determine if it is a real error, or just topology does not exist in dictionary
			// If there is an error, return an empty array.  This assume that the topology does not exist.
			common.Debugf("GetAllDeletedIndexInstances(): Cannot find topology for bucket %s.  Skip.", bucket)
			continue
		}

		for _, defnRef := range topology.Definitions {
			for _, inst := range defnRef.Instances {
				if common.IndexState(inst.State) == common.INDEX_STATE_DELETED {
					result = append(result, inst.InstId)
				}
			}
		}
	}

	return result, nil
}

/////////////////////////////////////////////////////////////////////////
// Protobuf message Conversion
////////////////////////////////////////////////////////////////////////

//
// Get all index instances for the topology as protobuf message
//
func GetTopologyAsInstanceProtoMsg(mgr *IndexManager,
	bucket string,
	port string) ([]*protobuf.Instance, *IndexTopology, error) {

	// Get the topology from the dictionary
	topology, err := mgr.GetTopologyByBucket(bucket)
	if err != nil {
		// TODO: Determine if it is a real error, or just topology does not exist in dictionary
		// If there is an error, return an empty array.  This assume that the topology does not exist.
		common.Debugf("GetTopologyAsInstanceProtoMsg(): Cannot find topology for bucket %s.  Skip.", bucket)
		return nil, nil, nil
	}

	instance, err := convertTopologyToIndexInstProtoMsg(mgr, topology, port)
	return instance, topology, err
}

//
// Get all index instances for a specific defnition as protobuf message
//
func GetIndexInstanceAsProtoMsg(mgr *IndexManager,
	bucket string,
	defnId common.IndexDefnId,
	port string) ([]*protobuf.Instance, error) {

	topology, err := mgr.GetTopologyByBucket(bucket)
	if err != nil {
		// TODO: Determine if it is a real error, or just topology does not exist in dictionary
		// If there is an error, return an empty array.  This assume that the topology does not exist.
		return nil, nil
	}

	var result []*protobuf.Instance = nil

	for _, defnRef := range topology.Definitions {

		if defnRef.DefnId == uint64(defnId) {

			// look up the index definition from dictionary
			defn, err := mgr.GetIndexDefnById(common.IndexDefnId(defnRef.DefnId))
			if err != nil {
				common.Debugf("GetIndexInstanceAsProtoMsg(): Cannot find definition id = %v.", defnId)
				return nil, err
			}

			// Convert definition to protobuf msg
			defn_proto := convertIndexDefnToProtoMsg(defn)

			// iterate through the index inst for this defnition
			for _, inst := range defnRef.Instances {
				result = append(result, convertIndexInstToProtoMsg(&inst, defn_proto, port))
			}
		}
	}

	return result, nil
}

//
// This function creates a protobuf message for the index instance in the list of change record.
//
func GetChangeRecordAsProtoMsg(mgr *IndexManager, changes []*changeRecord, port string) ([]*protobuf.Instance, error) {

	var result []*protobuf.Instance = nil

	for _, change := range changes {

		// look up the index definition from dictionary
		defn, err := mgr.GetIndexDefnById(common.IndexDefnId(change.definition.DefnId))
		if err != nil {
			common.Debugf("GetChangeRecordAsProtoMsg(): Cannot find definition id = %v.", change.definition.DefnId)
			return nil, err
		}

		// Convert definition to protobuf msg
		defn_proto := convertIndexDefnToProtoMsg(defn)

		// create protobuf message for the given instance
		result = append(result, convertIndexInstToProtoMsg(change.instance, defn_proto, port))
	}

	return result, nil
}

//
// Serialize topology into a protobuf message format
//
func convertTopologyToIndexInstProtoMsg(mgr *IndexManager,
	topology *IndexTopology,
	port string) ([]*protobuf.Instance, error) {

	var result []*protobuf.Instance = nil

	for _, defnRef := range topology.Definitions {

		// look up the index definition from dictionary
		defn, err := mgr.GetIndexDefnById(common.IndexDefnId(defnRef.DefnId))
		if err != nil {
			common.Debugf("convertTopologyToIndexInstProtoMsg(): Cannot find definition id = %v. Skip", defnRef.DefnId)
			continue
		}

		// Convert definition to protobuf msg
		defn_proto := convertIndexDefnToProtoMsg(defn)

		// iterate through the index inst for this defnition
		// TODO: Remove CREATED state from the if-stmt
		for _, inst := range defnRef.Instances {
			if common.IndexState(inst.State) == common.INDEX_STATE_READY ||
				common.IndexState(inst.State) == common.INDEX_STATE_INITIAL ||
				common.IndexState(inst.State) == common.INDEX_STATE_CREATED ||
				common.IndexState(inst.State) == common.INDEX_STATE_ACTIVE {
				result = append(result, convertIndexInstToProtoMsg(&inst, defn_proto, port))
			}
		}
	}

	return result, nil
}

//
// convert IndexDefn to protobuf format
//
func convertIndexDefnToProtoMsg(indexDefn *common.IndexDefn) *protobuf.IndexDefn {

	using := protobuf.StorageType(
		protobuf.StorageType_value[string(indexDefn.Using)]).Enum()
	exprType := protobuf.ExprType(
		protobuf.ExprType_value[string(indexDefn.ExprType)]).Enum()
	partnScheme := protobuf.PartitionScheme(
		protobuf.PartitionScheme_value[string(indexDefn.PartitionScheme)]).Enum()

	//
	// message IndexDefn {
	//  required uint64          defnID          = 1; // unique index id across the secondary index cluster
	//  required string          bucket          = 2; // bucket on which index is defined
	//  required bool            isPrimary       = 3; // whether index secondary-key == docid
	//  required string          name            = 4; // Name of the index
	//  required StorageType     using           = 5; // indexing algorithm
	//  required PartitionScheme partitionScheme = 6;
	//  required string          partnExpression = 7; // use expressions to evaluate doc
	//  required ExprType        exprType        = 8; // how to interpret `expressions` strings
	//  repeated string          secExpressions  = 9; // use expressions to evaluate doc
	//
	defn := &protobuf.IndexDefn{
		DefnID:          proto.Uint64(uint64(indexDefn.DefnId)),
		Bucket:          proto.String(indexDefn.Bucket),
		IsPrimary:       proto.Bool(indexDefn.IsPrimary),
		Name:            proto.String(indexDefn.Name),
		Using:           using,
		ExprType:        exprType,
		SecExpressions:  indexDefn.SecExprs,
		PartitionScheme: partnScheme,
		PartnExpression: proto.String(indexDefn.PartitionKey),
	}

	return defn
}

//
// convert IndexInst to protobuf format
//
func convertIndexInstToProtoMsg(inst *IndexInstDistribution,
	protoDefn *protobuf.IndexDefn,
	port string) *protobuf.Instance {

	//
	// message IndexInst {
	//	required uint64          instId     = 1;
	//	required IndexState      state      = 2;
	//	required IndexDefn       definition = 3; // contains DDL
	//	optional TestPartition   tp         = 4;
	// }
	//
	instance := &protobuf.IndexInst{
		InstId:     proto.Uint64(uint64(inst.InstId)),
		State:      protobuf.IndexState(inst.State).Enum(),
		Definition: protoDefn}

	// accumulate endpoints for this instance
	var endpoints []string
	for _, partition := range inst.Partitions {
		for _, slice := range partition.SinglePartition.Slices {
			endpoints = append(endpoints, net.JoinHostPort(slice.Host, port))
		}
	}

	//
	//	message TestPartition {
	//		repeated string endpoints     = 1; // endpoint address
	//		optional string coordEndpoint = 2;
	//  }
	//
	instance.Tp = &protobuf.TestPartition{
		Endpoints: endpoints,
	}

	return &protobuf.Instance{IndexInstance: instance}
}
