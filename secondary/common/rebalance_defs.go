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
	"strings"
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
	TransferTokenAccepted // 2. Master: Change state to TTInitiate.
	TransferTokenRefused  // x. Master (but really unused): No-op.
	TransferTokenInitiate // 3. Dest: Initiate index build if non-deferred and change state.
	// to TTInProgress; if build is deferred it may change state to TTTokenMerge instead.
	TransferTokenInProgress // 4. Dest: No-op in processTokenAsDest; processed in
	// tokenMergeOrReady. Build in progress or staged for start. May pass through state
	// TTMerge. Change state to TTReady when done.
	TransferTokenReady // 5. Source: Ready to delete source idx (dest idx now taking all
	// traffic). Queue source index for later async drop. Drop processing will change
	// state to TTCommit after the drop is complete.
	TransferTokenCommit // 6. Master: Source index is deleted. Change master's in-mem token
	// state to TTCommit amd metakv token state to TTDeleted.
	TransferTokenDeleted // 7. Master: All TT processing done. Delete the TT from metakv.
	TransferTokenError   // x. Unused; kept so down-level iotas match.
	TransferTokenMerge   // 4.5 Dest (partitioned indexes only): No-op in processTokenAsDest;
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

type ShardTokenState int

// Token states for shard rebalance
const (
	// Initial state of the tokens: ShardTokenCreated
	//
	// After planner run, rebalance master posts all transfer tokens
	// to metaKV with "ShardTokenCreated" state. This is the default
	// state of the transfer token
	ShardTokenCreated ShardTokenState = iota + 10000

	// Rebalance Source node will read tokens (that are in
	// "ShardTokenCreated" state) updates in-memory book-keeping
	// with all the potential movements during rebalance and changes
	// the transfer token state to "ShardTokenScheduledOnSource"
	ShardTokenScheduledOnSource

	// Rebalance destination node will read the transfer token in
	// state "ShardTokenScheduledOnSource", updates its in-memory
	// book keeping with all the potential movements during index
	// rebalance and changes the transfer token state to
	// "ShardTokenScheduleAck" for further processing.
	ShardTokenScheduleAck

	// Rebalance master would wait for all the shard tokens to reach
	// the "ShardTokenScheduleAck" state. After all tokens reach this
	// state, then master node would group the transfer tokens into
	// multiple batches. The state of the transfer tokens in each batch
	// is changed to "ShardTokenTransferShard". Source node will read
	// this state and start transferring indexed data to S3
	//
	// A metaKV token in this state means that the source node is yet
	// to start transferring shard (or) it has already initiated transferring
	// shard
	ShardTokenTransferShard

	// Rebalance source node, on a successful transfer will update the
	// state of transfer token to "ShardTokenRestoreShard", along with
	// the path from which destination node can download the shard data
	// to local file system.
	//
	// A metaKV token in this state means that the destination node is yet
	// to start downloading shard (or) it has already initiated downloading
	// shards
	ShardTokenRestoreShard

	// On a successful download, rebalance destination node will change
	// the transfer token state to "ShardTokenRecoverShard" which
	// indicates that the download  is successful and indexer should now
	// start to establih streams with KV nodes for index build catchup
	//
	// A metaKV token in this state means that the destination node has
	// succecssfully downloaded shard data and is yet to start establishing
	// KV streams
	ShardTokenRecoverShard

	// When empty node batching is enabled, shard rebalancer will move
	// the transfer token state to "ShardTokenMerge". Destination
	// would read this token and change the RState of all the indexes in
	// the transfer token
	ShardTokenMerge

	// After all the indexes in a shard are recovered successfully, then the
	// state of the transfer token is moved to "ShardTokenReady". In case of
	// any errors during restore/recovery, index movements belonging to this
	// shard will be cancelled and rebalance would fail.
	//
	// Source node on seeing this state change will drop the shard and move
	// the state to "ShardTokenCommit".
	//
	// A metaKV token in this state means that all indexes belonging to the
	// shard have successfully caught up with KV.
	ShardTokenReady

	// After transfer token and its sibling move to ShardTokenReady state,
	// rebalance master will generate a new token with this state and post
	// it to metaKV. The new token will contian the transfer token ID's of
	// both the sibling.
	//
	// A metaKV token in this state means that index instances on source
	// nodes can be dropped as the destination indexes have successfully
	// caught up with KV nodes
	ShardTokenDropOnSource

	// This token state means that index instances on source node have
	// been successfully deleted. The in-mem tokens would move to this
	// state and metaKV tokens would move to ShardTokenDeleted state
	ShardTokenCommit

	// This states indicates that the token processing is done and
	// the token can be deleted from metaKV
	ShardTokenDeleted

	// If any error is encountered during rebalance, the state of the token
	// can be updated to ShardTokenError
	ShardTokenError
)

func (sts ShardTokenState) String() string {

	switch sts {
	case ShardTokenCreated:
		return "ShardTokenCreated"
	case ShardTokenScheduledOnSource:
		return "ShardTokenScheduledOnSource"
	case ShardTokenScheduleAck:
		return "ShardTokenScheduleAck"
	case ShardTokenTransferShard:
		return "ShardTokenTransferShard"
	case ShardTokenRestoreShard:
		return "ShardTokenRestoreShard"
	case ShardTokenRecoverShard:
		return "ShardTokenRecoverShard"
	case ShardTokenReady:
		return "ShardTokenReady"
	case ShardTokenDropOnSource:
		return "ShardTokenDropOnSource"
	case ShardTokenMerge:
		return "ShardTokenMerge"
	case ShardTokenCommit:
		return "ShardTokenCommit"
	case ShardTokenDeleted:
		return "ShardTokenDeleted"
	case ShardTokenError:
		return "ShardTokenError"
	}

	return "unknown"

}

// TokenBuildSource is the type of the TransferToken.BuildSource field, which is currently unused
// but will be used in future when rebalance is done using file copy rather than DCP.
type TokenBuildSource byte

const (
	TokenBuildSourceDcp TokenBuildSource = iota
	TokenBuildSourcePeer
	TokenBuildSourceS3
)

func (bs TokenBuildSource) String() string {

	switch bs {
	case TokenBuildSourceDcp:
		return "Dcp"
	case TokenBuildSourcePeer:
		return "Peer"
	case TokenBuildSourceS3:
		return "S3"
	}
	return "unknown"
}

type TokenTransferMode byte

const (
	TokenTransferModeMove TokenTransferMode = iota // moving idx from source to dest
	TokenTransferModeCopy                          // no source node; idx created on dest during rebalance (replica repair)
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

type TransferTokenVersion byte

const (
	// Used for moving single instance per transfer token
	// when the index instance is built using DCP on destination node
	// Default mode pre 7.2.0 versions
	SINGLE_INST_DCP_BUILD TransferTokenVersion = iota

	// Used when moving multiple index instanes per transfer token
	// Data of all the index instances on disk (a.k.a shard) is
	// transferred to the destination node and index is rebuilt from
	// the data on disk
	MULTI_INST_SHARD_TRANSFER
)

func (ttv TransferTokenVersion) String() string {

	switch ttv {
	case SINGLE_INST_DCP_BUILD:
		return "SINGLE_INST_DCP_BUILD"
	case MULTI_INST_SHARD_TRANSFER:
		return "MULTI_INST_SHARD_TRANSFER"
	}
	return "unknown"
}

// TransferToken represents a sindgle index partition movement for rebalance or move index.
// These get stored in metakv, which makes callbacks on creation and each change.
type TransferToken struct {
	MasterId     string // rebal master nodeUUID (32-digit random hex)
	SourceId     string // index source nodeUUID
	DestId       string // index dest   nodeUUID
	RebalId      string
	State        TokenState  // current TT state; usually tells the NEXT thing to be done
	InstId       IndexInstId // true instance ID for non-partitioned; may be proxy ID for partitioned
	RealInstId   IndexInstId // 0 for non-partitioned or non-proxy partitioned, else true instId to merge partn to
	IndexInst    IndexInst
	Error        string            // English error text; empty if no error
	BuildSource  TokenBuildSource  // unused
	TransferMode TokenTransferMode // move (rebalance) vs copy (replica repair)

	IsEmptyNodeBatch bool //indicates the token is part of batch for empty node

	//indicates the token is pending ready as the full batch is not complete.
	//used only for empty node batching.
	IsPendingReady bool

	//used for logging
	SourceHost string
	DestHost   string

	// For shard transfer based rebalance

	Version TransferTokenVersion

	// If shard based rebalance is being used, then the state
	// of transfer token is captured in shardTokenState variable
	ShardTransferTokenState ShardTokenState

	// Set of index instances belonging to a shard
	InstIds     []IndexInstId
	RealInstIds []IndexInstId
	IndexInsts  []IndexInst

	// IDs of the main and back index shards for the group
	// of index instances being moved. Source node may use
	// this information to initiate transfer of corresponding
	// shard  to destination node
	ShardIds []ShardId

	// Location on S3 from which destination node can download
	// the shard data
	Destination string

	// Region at which the S3 bucket is hosted
	Region string

	// TokenId of the source node on which drop index instances
	// has to be initiated
	SourceTokenId string

	// For shard rebalance, indexes move from one sub-cluster to another
	// As one transfer token is for one node to another, atleast 2 tokens
	// are required to move both master and replica indexes. The index
	// instances in source subcluster can be dropped only when both
	// master and replica indexes move to destination sub-cluster.  This
	// variable captures the tokenID of the sibling (For master token,
	// this will contain token ID of the replica and vice-versa). This
	// information is used to read the state of the sibling token to take
	// an action of dropping the indexes in source subcluster
	SiblingTokenId string

	// shardId -> Location on S3 where the shard is uploaded
	// shardPath is a sub-directory of "destination"
	ShardPaths map[ShardId]string

	// Used by shard rebalancer during replica repair. Contains
	// the paths which plasma has to repair in shard.json file
	// during replica repair
	InstRenameMap map[ShardId]map[string]string

	// During shard repair, a shard from source node will be copied
	// to destination node. The alternate shardId in the shard meta
	// has to be updated to refer to the alternate shardIds of the
	// instances being repaired - Not the alternate id of the source
	// node
	NewAlternateShardIds []string

	// Special case handling
	IsDummy bool
}

// TransferToken.Clone returns a copy of the transfer token it is called on. Since the type is
// only one layer deep, this can be done by returning the value receiver as Go already copied it.
func (tt *TransferToken) Clone() *TransferToken {
	tt1 := *tt
	tt2 := tt1
	return &tt2
}

// TransferToken.IsUserDeferred returns whether the transfer token represents an index
// that was created by the user as deferred (with {"defer_build":true}) and not yet built.
// Rebalance will move the index metadata from source to dest but will NOT build these.
// All deferred indexes have flag setting
//
//	tt.IndexInst.Defn.Deferred == true
//
// User-deferred indexes additionally have state value
//
//	tt.IndexInst.State == INDEX_STATE_READY
//
// whereas system-deferred indexes have a different State (usually INDEX_STATE_ACTIVE).
func (tt *TransferToken) IsUserDeferred() bool {
	return tt.IndexInst.Defn.Deferred && tt.IndexInst.State == INDEX_STATE_READY
}

// TransferToken.String returns a human-friendly string representation of a TT.
func (tt *TransferToken) String() string {
	var sb strings.Builder
	sbp := &sb

	fmt.Fprintf(sbp, " MasterId: %v ", tt.MasterId)
	fmt.Fprintf(sbp, "SourceId: %v ", tt.SourceId)
	if len(tt.SourceHost) != 0 {
		fmt.Fprintf(sbp, "(%v) ", tt.SourceHost)
	}
	fmt.Fprintf(sbp, "DestId: %v ", tt.DestId)
	if len(tt.DestHost) != 0 {
		fmt.Fprintf(sbp, "(%v) ", tt.DestHost)
	}
	fmt.Fprintf(sbp, "RebalId: %v ", tt.RebalId)

	if tt.IsShardTransferToken() {
		fmt.Fprintf(sbp, "ShardTokenState: %v ", tt.ShardTransferTokenState)
	} else {
		fmt.Fprintf(sbp, "State: %v ", tt.State)
	}

	fmt.Fprintf(sbp, "BuildSource: %v ", tt.BuildSource)
	fmt.Fprintf(sbp, "TransferMode: %v ", tt.TransferMode)
	if tt.Error != "" {
		fmt.Fprintf(sbp, "Error: %v ", tt.Error)
	}

	if tt.SourceTokenId != "" { // Used only for ShardTokenDropOnSource state
		fmt.Fprintf(sbp, "SourceTokenId: %v ", tt.SourceTokenId)
	}

	// If more than one index instance exists, print all of them
	if tt.IsShardTransferToken() {
		fmt.Fprintf(sbp, "SiblingTokenId: %v ", tt.SiblingTokenId)
		fmt.Fprintf(sbp, "Shards: %v\n", tt.ShardIds)
		fmt.Fprintf(sbp, "Destination: %v\n", tt.Destination)
		fmt.Fprintf(sbp, "Region: %v\n", tt.Region)

		if len(tt.InstRenameMap) > 0 {
			fmt.Fprintf(sbp, "InstRenameMap: %v\n", tt.InstRenameMap)
		}

		for i := range tt.IndexInsts {
			fmt.Fprintf(sbp, "InstId: %v ", tt.InstIds[i])
			fmt.Fprintf(sbp, "RealInstId: %v ", tt.RealInstIds[i])
			fmt.Fprintf(sbp, "Partitions: %v ", tt.IndexInsts[i].Defn.Partitions)
			fmt.Fprintf(sbp, "Versions: %v ", tt.IndexInsts[i].Defn.Versions)
			fmt.Fprintf(sbp, "Inst: %v\n", tt.IndexInsts[i])
		}
		fmt.Fprintf(sbp, "IsEmptyNodeBatch: %v ", tt.IsEmptyNodeBatch)
		fmt.Fprintf(sbp, "IsPendingReady: %v ", tt.IsPendingReady)
		fmt.Fprintf(sbp, "IsDummy: %v ", tt.IsDummy)
	} else {
		fmt.Fprintf(sbp, "InstId: %v ", tt.InstId)
		fmt.Fprintf(sbp, "RealInstId: %v ", tt.RealInstId)
		fmt.Fprintf(sbp, "Partitions: %v ", tt.IndexInst.Defn.Partitions)
		fmt.Fprintf(sbp, "Versions: %v ", tt.IndexInst.Defn.Versions)
		fmt.Fprintf(sbp, "Inst: %v\n", tt.IndexInst)
		fmt.Fprintf(sbp, "IsEmptyNodeBatch: %v ", tt.IsEmptyNodeBatch)
		fmt.Fprintf(sbp, "IsPendingReady: %v ", tt.IsPendingReady)
	}

	return sb.String()
}

func (tt *TransferToken) LessVerboseString() string {
	var sb strings.Builder
	sbp := &sb

	fmt.Fprintf(sbp, " MasterId: %v ", tt.MasterId)
	fmt.Fprintf(sbp, "SourceId: %v ", tt.SourceId)
	if len(tt.SourceHost) != 0 {
		fmt.Fprintf(sbp, "(%v) ", tt.SourceHost)
	}
	fmt.Fprintf(sbp, "DestId: %v ", tt.DestId)
	if len(tt.DestHost) != 0 {
		fmt.Fprintf(sbp, "(%v) ", tt.DestHost)
	}
	fmt.Fprintf(sbp, "RebalId: %v ", tt.RebalId)

	if tt.IsShardTransferToken() {
		fmt.Fprintf(sbp, "ShardTokenState: %v ", tt.ShardTransferTokenState)
	} else {
		fmt.Fprintf(sbp, "State: %v ", tt.State)
	}

	fmt.Fprintf(sbp, "BuildSource: %v ", tt.BuildSource)
	fmt.Fprintf(sbp, "TransferMode: %v ", tt.TransferMode)
	if tt.Error != "" {
		fmt.Fprintf(sbp, "Error: %v ", tt.Error)
	}

	if tt.SourceTokenId != "" { // Used only for ShardTokenDropOnSource state
		fmt.Fprintf(sbp, "SourceTokenId: %v ", tt.SourceTokenId)
	}
	// If more than one index instance exists, print all of them
	if tt.IsShardTransferToken() {
		fmt.Fprintf(sbp, "SiblingTokenId: %v ", tt.SiblingTokenId)
		fmt.Fprintf(sbp, "Shards: %v\n", tt.ShardIds)
		// tt.Destination contains same info as DestHost + TransferPort for on-prem file based.
		// Omit printing if tt.DestHost is already printed
		if tt.Destination != "" && (len(tt.DestHost) == 0 || GetDeploymentModel() == SERVERLESS_DEPLOYMENT) {
			fmt.Fprintf(sbp, "Destination: %v\n", tt.Destination)
		}
		if tt.Region != "" {
			fmt.Fprintf(sbp, "Region: %v\n", tt.Region)
		}

		for i := range tt.IndexInsts {
			fmt.Fprintf(sbp, "\t%v, %v, ", tt.InstIds[i], tt.RealInstIds[i])
			fmt.Fprintf(sbp, "%v, %v, %v/%v, %v/%v, ",
				tt.IndexInsts[i].Defn.Name, tt.IndexInsts[i].Defn.Bucket,
				tt.IndexInsts[i].Defn.Scope, tt.IndexInsts[i].Defn.ScopeId,
				tt.IndexInsts[i].Defn.Collection, tt.IndexInsts[i].Defn.CollectionId)
			fmt.Fprintf(sbp, "%v, %v\n", tt.IndexInsts[i].Defn.Partitions, tt.IndexInsts[i].Defn.Versions)
		}
		fmt.Fprintf(sbp, "IsEmptyNodeBatch: %v ", tt.IsEmptyNodeBatch)
		fmt.Fprintf(sbp, "IsPendingReady: %v ", tt.IsPendingReady)
		fmt.Fprintf(sbp, "IsDummy: %v", tt.IsDummy)
	} else {
		fmt.Fprintf(sbp, "InstId: %v ", tt.InstId)
		fmt.Fprintf(sbp, "RealInstId: %v ", tt.RealInstId)
		fmt.Fprintf(sbp, "Partitions: %v ", tt.IndexInst.Defn.Partitions)
		fmt.Fprintf(sbp, "Versions: %v ", tt.IndexInst.Defn.Versions)
		fmt.Fprintf(sbp, "IsEmptyNodeBatch: %v ", tt.IsEmptyNodeBatch)
		fmt.Fprintf(sbp, "IsPendingReady: %v ", tt.IsPendingReady)
	}

	return sb.String()
}

func (tt *TransferToken) CompactString() string {
	var sb strings.Builder
	sbp := &sb

	fmt.Fprintf(sbp, " CompactStr:: ")
	fmt.Fprintf(sbp, "[%v", tt.MasterId)
	fmt.Fprintf(sbp, ", %v", tt.SourceId)
	if len(tt.SourceHost) != 0 {
		fmt.Fprintf(sbp, " (%v)", tt.SourceHost)
	}
	fmt.Fprintf(sbp, ", %v", tt.DestId)
	if len(tt.DestHost) != 0 {
		fmt.Fprintf(sbp, " (%v)", tt.DestHost)
	}
	fmt.Fprintf(sbp, ", %v] ", tt.RebalId)

	if tt.IsShardTransferToken() {
		fmt.Fprintf(sbp, "ShardTokenState: %v ", tt.ShardTransferTokenState)
	} else {
		fmt.Fprintf(sbp, "State: %v ", tt.State)
	}

	fmt.Fprintf(sbp, "%v ", tt.BuildSource)
	fmt.Fprintf(sbp, "%v ", tt.TransferMode)
	if tt.Error != "" {
		fmt.Fprintf(sbp, "Error: %v ", tt.Error)
	}

	if tt.SourceTokenId != "" { // Used only for ShardTokenDropOnSource state
		fmt.Fprintf(sbp, "SourceTokenId: %v ", tt.SourceTokenId)
	}
	// If more than one index instance exists, print all of them
	if tt.IsShardTransferToken() {
		fmt.Fprintf(sbp, "(%v, ", tt.SiblingTokenId)
		fmt.Fprintf(sbp, "%v, ", tt.ShardIds)
		// tt.Destination contains same info as DestHost + TransferPort for on-prem file based.
		// Omit printing if tt.DestHost is already printed
		if tt.Destination != "" && (len(tt.DestHost) == 0 || GetDeploymentModel() == SERVERLESS_DEPLOYMENT) {
			fmt.Fprintf(sbp, "Dest: %v, ", tt.Destination)
		}
		if tt.Region != "" {
			fmt.Fprintf(sbp, "Region: %v, ", tt.Region)
		}
		fmt.Fprintf(sbp, "%v, ", tt.IsEmptyNodeBatch)
		fmt.Fprintf(sbp, "%v, ", tt.IsPendingReady)
		fmt.Fprintf(sbp, "%v)", tt.IsDummy)
	} else {
		fmt.Fprintf(sbp, "(%v, ", tt.InstId)
		fmt.Fprintf(sbp, "%v, ", tt.RealInstId)
		fmt.Fprintf(sbp, "%v, ", tt.IndexInst.Defn.Partitions)
		fmt.Fprintf(sbp, "%v, ", tt.IndexInst.Defn.Versions)
		fmt.Fprintf(sbp, "%v, ", tt.IsEmptyNodeBatch)
		fmt.Fprintf(sbp, "%v)", tt.IsPendingReady)
	}

	return sb.String()
}

func (tt *TransferToken) IsShardTransferToken() bool {
	return (tt.Version == MULTI_INST_SHARD_TRANSFER)
}

func (tt *TransferToken) IsDcpTransferToken() bool {
	return tt.Version == SINGLE_INST_DCP_BUILD || tt.BuildSource == TokenBuildSourceDcp
}

func (tt *TransferToken) SiblingExists() bool {
	return tt.SiblingTokenId != ""
}

type RebalancePhase byte

const (
	RebalanceNotRunning RebalancePhase = iota
	RebalanceInitated
	RebalanceTransferInProgress // Per bucket phase - Not used currently
	RebalanceTransferDone
	RebalanceRestoreInProgress  // Per bucket phase - Not used currently
	RebalanceRestoreDone        // Per bucket phase - Not used currently
	RebalanceRecoveryInProgress // Per bucket phase - Not used currently
	RebalanceRecoveryDone       // Per bucket phase - Not used currently
	RebalanceDone
)

func (rp RebalancePhase) String() string {

	switch rp {
	case RebalanceNotRunning:
		return "REBALANCE_NOT_RUNNING"
	case RebalanceInitated:
		return "REBALANCE_INITIATED"
	case RebalanceTransferInProgress:
		return "REBALANCE_TRANSFER_IN_PROGRESS"
	case RebalanceTransferDone:
		return "REBALANCE_TRANSFER_DONE"
	case RebalanceRestoreInProgress:
		return "REBALANCE_RESTORE_IN_PROGRESS"
	case RebalanceRestoreDone:
		return "REBALANCE_RESTORE_DONE"
	case RebalanceRecoveryInProgress:
		return "REBALANCE_RECOVERY_IN_PROGRESS"
	case RebalanceRecoveryDone:
		return "REBALANCE_RECOVERY_DONE"
	case RebalanceDone:
		return "REBALANCE_DONE"
	}
	return "unknown"
}

func (rp RebalancePhase) ShortString() string {

	switch rp {
	case RebalanceNotRunning:
		return "RNR"
	case RebalanceInitated:
		return "RI"
	case RebalanceTransferInProgress:
		return "RTIP"
	case RebalanceTransferDone:
		return "RTD"
	case RebalanceRestoreInProgress:
		return "RRIP"
	case RebalanceRestoreDone:
		return "RRD"
	case RebalanceRecoveryInProgress:
		return "RReIP"
	case RebalanceRecoveryDone:
		return "RReD"
	case RebalanceDone:
		return "RD"
	}
	return "unknown"
}

type RebalancePhaseRequest struct {
	GlobalRebalPhase    RebalancePhase            `json:"globalRebalPhase,omitempty"`    // Overall rebalance phase
	BucketTransferPhase map[string]RebalancePhase `json:"bucketTransferPhase,omitempty"` // Per-bucket rebalance phase
}

type DDLDuringRebalanceVersion byte

const (
	BlockDDLDuringRebalance DDLDuringRebalanceVersion = iota
	AllowDDLDuringRebalance_v1
)

type ShardRebalanceSchedulingVersion string

const (
	GLOBAL_TRANSFER_LIMIT   ShardRebalanceSchedulingVersion = "v1"
	PER_NODE_TRANSFER_LIMIT                                 = "v2"
)

type RebalancerType byte

const (
	INVALID_REBALANCER RebalancerType = iota
	DCP_REBALANCER
	SHARD_REBALANCER
)

func (rt RebalancerType) String() string {
	switch rt {
	case INVALID_REBALANCER:
		return "INVALID"
	case DCP_REBALANCER:
		return "DCP"
	case SHARD_REBALANCER:
		return "SHARD"
	}
	return "UNKNOWN"
}
