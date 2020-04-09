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
	c "github.com/couchbase/indexing/secondary/common"
)

/////////////////////////////////////////////////////////////////////////
// Topology Definition
////////////////////////////////////////////////////////////////////////

type GlobalTopology struct {
	TopologyKeys []string `json:"topologyKeys,omitempty"`
}

// TODO: Need to add Collection specific information in IndexTopology
// This has dependency on Lifecycle Manager
type IndexTopology struct {
	Version     uint64                  `json:"version,omitempty"`
	Bucket      string                  `json:"bucket,omitempty"`
	Definitions []IndexDefnDistribution `json:"definitions,omitempty"`
}

// TODO: Need to add Collection specific information in IndexDefnDistribution
// This has dependency on Lifecycle Manager
type IndexDefnDistribution struct {
	Bucket     string                  `json:"bucket,omitempty"`
	Scope      string                  `json:"scope,omitempty"`
	Collection string                  `json:"collection,omitempty"`
	Name       string                  `json:"name,omitempty"`
	DefnId     uint64                  `json:"defnId,omitempty"`
	Instances  []IndexInstDistribution `json:"instances,omitempty"`
}

type IndexInstDistribution struct {
	InstId         uint64                  `json:"instId,omitempty"`
	State          uint32                  `json:"state,omitempty"`
	StreamId       uint32                  `json:"streamId,omitempty"`
	Error          string                  `json:"error,omitempty"`
	Partitions     []IndexPartDistribution `json:"partitions,omitempty"`
	NumPartitions  uint32                  `json:"numPartitions,omitempty"`
	RState         uint32                  `json:"rRtate,omitempty"`
	Version        uint64                  `json:"version,omitempty"`
	ReplicaId      uint64                  `json:"replicaId,omitempty"`
	Scheduled      bool                    `json:"scheduled,omitempty"`
	StorageMode    string                  `json:"storageMode,omitempty"`
	OldStorageMode string                  `json:"oldStorageMode,omitempty"`
	RealInstId     uint64                  `json:"realInstId,omitempty"`
}

type IndexPartDistribution struct {
	PartId          uint64                      `json:"partId,omitempty"`
	Version         uint64                      `json:"version,omitempty"`
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
// Topology
////////////////////////////////////////////////////////////////////////

func (t *IndexTopology) FindIndexerId() string {

	for _, defn := range t.Definitions {
		for _, inst := range defn.Instances {
			indexerId := inst.FindIndexerId()
			if len(indexerId) != 0 {
				return indexerId
			}
		}
	}

	return ""
}

func (inst IndexInstDistribution) FindIndexerId() string {

	for _, part := range inst.Partitions {
		for _, slice := range part.SinglePartition.Slices {
			if len(slice.IndexerId) != 0 {
				return slice.IndexerId
			}
		}
	}

	return ""
}

func (t *IndexTopology) GetIndexInstancesByDefn(defnId c.IndexDefnId) []IndexInstDistribution {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			return t.Definitions[i].Instances
		}
	}
	return nil
}

func (t *IndexTopology) GetStatusByInst(defnId c.IndexDefnId, instId c.IndexInstId) (c.IndexState, string) {

	for i, _ := range t.Definitions {
		if t.Definitions[i].DefnId == uint64(defnId) {
			for j, _ := range t.Definitions[i].Instances {
				if t.Definitions[i].Instances[j].InstId == uint64(instId) {
					return c.IndexState(t.Definitions[i].Instances[j].State), t.Definitions[i].Instances[j].Error
				}
			}
		}
	}
	return c.INDEX_STATE_NIL, ""
}
