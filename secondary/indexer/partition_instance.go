// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

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

//IndexPartnMap maps a IndexInstId to PartitionInstMap
type IndexPartnMap map[common.IndexInstId]PartitionInstMap

//PartitionInstMap maps a PartitionId to PartitionInst
type PartitionInstMap map[common.PartitionId]PartitionInst

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
