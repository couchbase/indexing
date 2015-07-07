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
	"github.com/couchbase/gometa/common"
	c "github.com/couchbase/indexing/secondary/common"
	logging "github.com/couchbase/indexing/secondary/logging"
)

/////////////////////////////////////////////////////////////////////////
// OpCode
////////////////////////////////////////////////////////////////////////

const (
	OPCODE_CREATE_INDEX      common.OpCode = common.OPCODE_CUSTOM + 1
	OPCODE_DROP_INDEX                      = OPCODE_CREATE_INDEX + 1
	OPCODE_BUILD_INDEX                     = OPCODE_DROP_INDEX + 1
	OPCODE_UPDATE_INDEX_INST               = OPCODE_BUILD_INDEX + 1
	OPCODE_SERVICE_MAP                     = OPCODE_UPDATE_INDEX_INST + 1
	OPCODE_DELETE_BUCKET                   = OPCODE_SERVICE_MAP + 1
	OPCODE_INDEXER_READY                   = OPCODE_DELETE_BUCKET + 1
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
	StreamId   uint32                  `json:"streamId,omitempty"`
	Error      string                  `json:"error,omitempty"`
	BuildTime  []uint64                `json:"buildTime,omitempty"`
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
	SliceId   uint64 `json:"sliceId,omitempty"`
	State     uint32 `json:"state,omitempty"`
	IndexerId string `json:"indexerId,omitempty"`
}

/////////////////////////////////////////////////////////////////////////
// Index List
////////////////////////////////////////////////////////////////////////

type IndexIdList struct {
	DefnIds []uint64 `json:"defnIds,omitempty"`
}

/////////////////////////////////////////////////////////////////////////
// Service Map
////////////////////////////////////////////////////////////////////////

type ServiceMap struct {
	IndexerId string `json:"indexerId,omitempty"`
	ScanAddr  string `json:"scanAddr,omitempty"`
	HttpAddr  string `json:"httpAddr,omitempty"`
	AdminAddr string `json:"adminAddr,omitempty"`
	NodeAddr  string `json:"nodeAddr,omitempty"`
}

/////////////////////////////////////////////////////////////////////////
// private method : unmarshalling
////////////////////////////////////////////////////////////////////////

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

func BuildIndexIdList(ids []c.IndexDefnId) *IndexIdList {
	list := new(IndexIdList)
	list.DefnIds = make([]uint64, len(ids))
	for i, id := range ids {
		list.DefnIds[i] = uint64(id)
	}
	return list
}

func UnmarshallIndexIdList(data []byte) (*IndexIdList, error) {

	list := new(IndexIdList)
	if err := json.Unmarshal(data, list); err != nil {
		return nil, err
	}

	return list, nil
}

func MarshallIndexIdList(list *IndexIdList) ([]byte, error) {

	buf, err := json.Marshal(&list)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func UnmarshallServiceMap(data []byte) (*ServiceMap, error) {

	logging.Debugf("UnmarshallServiceMap: %v", string(data))

	list := new(ServiceMap)
	if err := json.Unmarshal(data, list); err != nil {
		return nil, err
	}

	return list, nil
}

func MarshallServiceMap(srvMap *ServiceMap) ([]byte, error) {

	buf, err := json.Marshal(&srvMap)
	if err != nil {
		return nil, err
	}

	logging.Debugf("MarshallServiceMap: %v", string(buf))

	return buf, nil
}
