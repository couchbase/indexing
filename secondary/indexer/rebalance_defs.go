// @author Couchbase <info@couchbase.com>
// @copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

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
	Error    string
	MasterIP string // real IP address of master node, not 127.0.0.1, so followers can reach it

	// Only used for DDL during rebalance
	Version    c.DDLDuringRebalanceVersion
	RebalPhase c.RebalancePhase

	// Tracking active rebalancers
	ActiveRebalancer c.RebalancerType
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
