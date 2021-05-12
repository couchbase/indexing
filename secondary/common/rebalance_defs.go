// @author Couchbase <info@couchbase.com>
// @copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package common

import (
	"fmt"
)

type TokenState byte

const (
	//
	// Transfer token states labeled with their flow sequence #s (or "x" for unused)
	// and categorized as Master (non-move bookkeeping), move Source, or move Dest.
	// Owners are defined by function getTransferTokenOwner. Processing and state changes
	// are mainly in functions processTokenAsMaster, processTokenAsSource,
	// processTokenAsDest, and tokenMergeOrReady.
	//
	TransferTokenCreated TokenState = iota // 1. Dest: Clone source index metadata into
		// local metadata; change state to TTAccepted. (Original design is first check
		// if enough resources and if not, change state to TTRefused and rerun planner,
		// but this is not implemented.)
	TransferTokenAccepted // 2. Master: Launch progress updater. Change state to TTInitiate.
	TransferTokenRefused // x. Master (but really unused): No-op.
	TransferTokenInitiate // 3. Dest: Initiate index build if non-deferred and change state.
		// to TTInProgress; if build is deferred it may change state to TTTokenMerge instead.
	TransferTokenInProgress // 4. Dest: No-op in processTokenAsDest; processed in
		// tokenMergeOrReady. Build in progress. May pass through state TTMerge. Change state
		// to TTReady when done.
	TransferTokenReady // 5. Source: Ready to delete source idx (dest idx now taking all
		// traffic). Queue source index for later async drop. Drop processing will change
		// state to TTCommit after the drop is complete.
	TransferTokenCommit // 6. Master: Source index is deleted. Change master's in-mem token
		// state to TTCommit amd metakv token state to TTDeleted.
	TransferTokenDeleted // 7. Master: All TT processing done. Delete the TT from metakv.
	TransferTokenError // x. Unused; kept so down-level iotas match.
	TransferTokenMerge // 4.5 Dest (partitioned indexes only): No-op in processTokenAsDest;
		// processed in tokenMergeOrReady. Tells indexer to merge dest partn's temp "proxy"
		// IndexDefn w/ "real" IndexDefn for that index. Change state to TTReady when done.
)

func (ts TokenState) String() string {

	switch ts {
	case TransferTokenCreated:
		return "TransferTokenCreated"
	case TransferTokenAccepted:
		return "TransferTokenAccepted"
	case TransferTokenRefused:
		return "TransferTokenRefused"
	case TransferTokenInitiate:
		return "TransferTokenInitiate"
	case TransferTokenInProgress:
		return "TransferTokenInProgress"
	case TransferTokenReady:
		return "TransferTokenReady"
	case TransferTokenCommit:
		return "TransferTokenCommit"
	case TransferTokenDeleted:
		return "TransferTokenDeleted"
	case TransferTokenError:
		return "TransferTokenError"
	case TransferTokenMerge:
		return "TransferTokenMerge"
	}

	return "unknown"

}

type TokenBuildSource byte

const (
	TokenBuildSourceDcp TokenBuildSource = iota
	TokenBuildSourcePeer
)

func (bs TokenBuildSource) String() string {

	switch bs {
	case TokenBuildSourceDcp:
		return "Dcp"
	case TokenBuildSourcePeer:
		return "Peer"
	}
	return "unknown"
}

type TokenTransferMode byte

const (
	TokenTransferModeMove TokenTransferMode = iota // moving idx from source to dest
	TokenTransferModeCopy // no source node; idx created on dest during rebalance (replica repair)
)

func (tm TokenTransferMode) String() string {

	switch tm {
	case TokenTransferModeMove:
		return "Move"
	case TokenTransferModeCopy:
		return "Copy"
	}
	return "unknown"
}

type TransferToken struct {
	MasterId     string
	SourceId     string
	DestId       string
	RebalId      string
	State        TokenState
	InstId       IndexInstId
	RealInstId   IndexInstId
	IndexInst    IndexInst
	Error        string
	BuildSource  TokenBuildSource
	TransferMode TokenTransferMode

	//used for logging
	SourceHost string
	DestHost   string
}

func (tt TransferToken) Clone() TransferToken {

	var ttc TransferToken
	ttc.MasterId = tt.MasterId
	ttc.SourceId = tt.SourceId
	ttc.DestId = tt.DestId
	ttc.RebalId = tt.RebalId
	ttc.State = tt.State
	ttc.InstId = tt.InstId
	ttc.RealInstId = tt.RealInstId
	ttc.IndexInst = tt.IndexInst
	ttc.Error = tt.Error
	ttc.BuildSource = tt.BuildSource
	ttc.TransferMode = tt.TransferMode

	ttc.SourceHost = tt.SourceHost
	ttc.DestHost = tt.DestHost

	return ttc

}

func (tt TransferToken) String() string {

	str := fmt.Sprintf(" MasterId: %v ", tt.MasterId)
	str += fmt.Sprintf("SourceId: %v ", tt.SourceId)
	if len(tt.SourceHost) != 0 {
		str += fmt.Sprintf("(%v) ", tt.SourceHost)
	}
	str += fmt.Sprintf("DestId: %v ", tt.DestId)
	if len(tt.DestHost) != 0 {
		str += fmt.Sprintf("(%v) ", tt.DestHost)
	}
	str += fmt.Sprintf("RebalId: %v ", tt.RebalId)
	str += fmt.Sprintf("State: %v ", tt.State)
	str += fmt.Sprintf("BuildSource: %v ", tt.BuildSource)
	str += fmt.Sprintf("TransferMode: %v ", tt.TransferMode)
	if tt.Error != "" {
		str += fmt.Sprintf("Error: %v ", tt.Error)
	}
	str += fmt.Sprintf("InstId: %v ", tt.InstId)
	str += fmt.Sprintf("RealInstId: %v ", tt.RealInstId)
	str += fmt.Sprintf("Partitions: %v ", tt.IndexInst.Defn.Partitions)
	str += fmt.Sprintf("Versions: %v ", tt.IndexInst.Defn.Versions)
	str += fmt.Sprintf("Inst: %v \n", tt.IndexInst)
	return str

}
