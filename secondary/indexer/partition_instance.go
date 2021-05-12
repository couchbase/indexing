// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"fmt"

	"github.com/couchbase/indexing/secondary/common"
)

//PartitionInst contains the partition definition and a SliceContainer
//to manage all the slices storing the partition's data
type PartitionInst struct {
	Defn common.PartitionDefn
	Sc   SliceContainer
}

type partitionInstList []PartitionInst

func (s partitionInstList) Len() int      { return len(s) }
func (s partitionInstList) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s partitionInstList) Less(i, j int) bool {
	return s[i].Defn.GetPartitionId() < s[j].Defn.GetPartitionId()
}

//IndexPartnMap maps a IndexInstId to PartitionInstMap
type IndexPartnMap map[common.IndexInstId]PartitionInstMap

//PartitionInstMap maps a PartitionId to PartitionInst
type PartitionInstMap map[common.PartitionId]PartitionInst

func (fp PartitionInstMap) Add(partnId common.PartitionId, inst PartitionInst) PartitionInstMap {
	if fp == nil {
		fp = make(PartitionInstMap)
	}
	fp[partnId] = inst
	return fp
}

func (pm IndexPartnMap) String() string {

	str := "\n"
	for i, pi := range pm {
		str += fmt.Sprintf("\tInstanceId: %v ", i)
		for j, p := range pi {
			str += fmt.Sprintf("PartitionId: %v ", j)
			str += fmt.Sprintf("Endpoints: %v ", p.Defn.Endpoints())
		}
		str += "\n"
	}
	return str
}

func (pi PartitionInst) String() string {

	str := fmt.Sprintf("PartitionId: %v ", pi.Defn.GetPartitionId())
	str += fmt.Sprintf("Endpoints: %v ", pi.Defn.Endpoints())

	return str

}

func CopyIndexPartnMap(inMap IndexPartnMap) IndexPartnMap {

	outMap := make(IndexPartnMap)
	for k, v := range inMap {

		pmap := make(PartitionInstMap)
		for id, inst := range v {
			pmap[id] = inst
		}

		outMap[k] = pmap
	}
	return outMap
}
