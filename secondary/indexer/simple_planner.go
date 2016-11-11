// @author Couchbase <info@couchbase.com>
// @copyright 2016 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package indexer

import (
	"fmt"
	"github.com/couchbase/cbauth/service"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager"
)

type SimplePlanner struct {
	topology *manager.ClusterIndexMetadata
	change   service.TopologyChange
	nodeId   string
}

func NewSimplePlanner(topology *manager.ClusterIndexMetadata,
	change service.TopologyChange, nodeId string) *SimplePlanner {

	return &SimplePlanner{topology: topology,
		change: change,
		nodeId: nodeId}

}

func (p *SimplePlanner) PlanIndexMoves() map[string]*c.TransferToken {

	var nodeList []string

	for _, node := range p.change.KeepNodes {
		nodeList = append(nodeList, string(node.NodeInfo.NodeID))
	}

	transferTokens := make(map[string]*c.TransferToken)
	icount := 0

	for _, localMeta := range p.topology.Metadata {

		for _, index := range localMeta.IndexDefinitions {

			destLoc := icount % len(nodeList)
			ttid, tt := p.genTransferToken(index, c.IndexInstId(index.DefnId), localMeta.IndexerId,
				nodeList[destLoc])

			icount++

			if tt.SourceId == tt.DestId {
				logging.Infof("Planner::PlanIndexMoves Skip No-op TransferToken %v", tt)
				continue
			}

			logging.Infof("Planner::PlanIndexMoves Generated TransferToken %v %v", ttid, tt)
			transferTokens[ttid] = tt
		}
	}

	return transferTokens
}

func (p *SimplePlanner) genTransferToken(indexDefn c.IndexDefn, instId c.IndexInstId,
	sourceId string, destId string) (string, *c.TransferToken) {

	ustr, err := c.NewUUID()

	if err != nil {
		//TODO handle error
	}

	indexInst := c.IndexInst{InstId: instId, Defn: indexDefn}

	ttid := fmt.Sprintf("TransferToken%s", ustr.Str())
	return ttid, &c.TransferToken{
		MasterId:  p.nodeId,
		SourceId:  sourceId,
		DestId:    destId,
		RebalId:   p.change.ID,
		State:     c.TransferTokenCreated,
		InstId:    instId,
		IndexInst: indexInst,
	}

}
