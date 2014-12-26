// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package client

import (
	"encoding/json"
	"github.com/couchbase/indexing/secondary/common"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
	"github.com/couchbaselabs/goprotobuf/proto"
)

/////////////////////////////////////////////////////////////////////////
// Topology Definition
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

/////////////////////////////////////////////////////////////////////////
// private method : unmarshalling
////////////////////////////////////////////////////////////////////////

func unmarshallIndexDefn(data []byte) (*common.IndexDefn, error) {

	pDefn := new(protobuf.IndexDefn)
	if err := proto.Unmarshal(data, pDefn); err != nil {
		return nil, err
	}

	using := common.IndexType(pDefn.GetUsing().String())
	exprType := common.ExprType(pDefn.GetExprType().String())
	partnScheme := common.PartitionScheme(pDefn.GetPartitionScheme().String())

	idxDefn := &common.IndexDefn{
		DefnId:          common.IndexDefnId(pDefn.GetDefnID()),
		Name:            pDefn.GetName(),
		Using:           using,
		Bucket:          pDefn.GetBucket(),
		IsPrimary:       pDefn.GetIsPrimary(),
		SecExprs:        pDefn.GetSecExpressions(),
		ExprType:        exprType,
		PartitionScheme: partnScheme,
		PartitionKey:    pDefn.GetPartnExpression()}

	return idxDefn, nil
}

func marshallIndexDefn(defn *common.IndexDefn) ([]byte, error) {

	using := protobuf.StorageType(
		protobuf.StorageType_value[string(defn.Using)]).Enum()

	exprType := protobuf.ExprType(
		protobuf.ExprType_value[string(defn.ExprType)]).Enum()

	partnScheme := protobuf.PartitionScheme(
		protobuf.PartitionScheme_value[string(defn.PartitionScheme)]).Enum()

	pDefn := &protobuf.IndexDefn{
		DefnID:          proto.Uint64(uint64(defn.DefnId)),
		Bucket:          proto.String(defn.Bucket),
		IsPrimary:       proto.Bool(defn.IsPrimary),
		Name:            proto.String(defn.Name),
		Using:           using,
		ExprType:        exprType,
		SecExpressions:  defn.SecExprs,
		PartitionScheme: partnScheme,
		PartnExpression: proto.String(defn.PartitionKey),
	}

	return proto.Marshal(pDefn)
}

func unmarshallIndexTopology(data []byte) (*IndexTopology, error) {

	topology := new(IndexTopology)
	if err := json.Unmarshal(data, topology); err != nil {
		return nil, err
	}

	return topology, nil
}

func marshallIndexTopology(topology *IndexTopology) ([]byte, error) {

	buf, err := json.Marshal(&topology)
	if err != nil {
		return nil, err
	}

	return buf, nil
}
