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
	"encoding/binary"
	"github.com/couchbase/cbauth/service"
	c "github.com/couchbase/indexing/secondary/common"
)

const RebalanceRunning = "RebalanceRunning"
const RebalanceTokenTag = "RebalanceToken"
const MoveIndexTokenTag = "MoveIndexToken"
const TransferTokenTag = "TransferToken"

const RebalanceMetakvDir = c.IndexingMetaDir + "rebalance/"
const RebalanceTokenPath = RebalanceMetakvDir + RebalanceTokenTag
const MoveIndexTokenPath = RebalanceMetakvDir + MoveIndexTokenTag

const StartPhaseBeginTimeout = 60
const RebalanceTokenWaitTimeout = 60

type RebalSource byte

const (
	RebalSourceClusterOp RebalSource = iota
	RebalSourceMoveIndex
)

func (rs RebalSource) String() string {

	switch rs {
	case RebalSourceClusterOp:
		return "Rebalance"
	case RebalSourceMoveIndex:
		return "MoveIndex"
	}
	return "Unknown"
}

type RebalanceToken struct {
	MasterId string
	RebalId  string
	Source   RebalSource
}

type RebalTokens struct {
	RT *RebalanceToken             `json:"rebalancetoken,omitempty"`
	MT *RebalanceToken             `json:"moveindextoken,omitempty"`
	TT map[string]*c.TransferToken `json:"transfertokens,omitempty"`
}

func EncodeRev(rev uint64) service.Revision {
	ext := make(service.Revision, 8)
	binary.BigEndian.PutUint64(ext, rev)

	return ext
}

func DecodeRev(ext service.Revision) uint64 {
	return binary.BigEndian.Uint64(ext)
}

/////////////////////////////////////////////////////////////////////////
//
//  cleanup helper
//
/////////////////////////////////////////////////////////////////////////

type Cleanup struct {
	canceled bool
	f        func()
}

func NewCleanup(f func()) *Cleanup {
	return &Cleanup{
		canceled: false,
		f:        f,
	}
}

func (c *Cleanup) Run() {
	if !c.canceled {
		c.f()
		c.Cancel()
	}
}

func (c *Cleanup) Cancel() {
	c.canceled = true
}
