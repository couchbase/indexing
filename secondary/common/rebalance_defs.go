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

package common

import (
	"fmt"
)

type TokenState byte

const (
	TransferTokenCreated TokenState = iota
	TransferTokenAccepted
	TransferTokenRefused
	TransferTokenInitate
	TransferTokenInProgress
	TransferTokenReady
	TransferTokenCommit
	TransferTokenDeleted
	TransferTokenError
)

func (ts TokenState) String() string {

	switch ts {
	case TransferTokenCreated:
		return "TransferTokenCreated"
	case TransferTokenAccepted:
		return "TransferTokenAccepted"
	case TransferTokenRefused:
		return "TransferTokenRefused"
	case TransferTokenInitate:
		return "TransferTokenInitate"
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
	TokenTransferModeMove TokenTransferMode = iota
	TokenTransferModeCopy
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
	IndexInst    IndexInst
	Error        string
	BuildSource  TokenBuildSource
	TransferMode TokenTransferMode
}

func (tt TransferToken) Clone() TransferToken {

	var ttc TransferToken
	ttc.MasterId = tt.MasterId
	ttc.SourceId = tt.SourceId
	ttc.DestId = tt.DestId
	ttc.RebalId = tt.RebalId
	ttc.State = tt.State
	ttc.InstId = tt.InstId
	ttc.IndexInst = tt.IndexInst
	ttc.Error = tt.Error
	ttc.BuildSource = tt.BuildSource
	ttc.TransferMode = tt.TransferMode

	return ttc

}

func (tt TransferToken) String() string {

	str := fmt.Sprintf(" MasterId: %v ", tt.MasterId)
	str += fmt.Sprintf("SourceId: %v ", tt.SourceId)
	str += fmt.Sprintf("DestId: %v ", tt.DestId)
	str += fmt.Sprintf("RebalId: %v ", tt.RebalId)
	str += fmt.Sprintf("State: %v ", tt.State)
	str += fmt.Sprintf("BuildSource: %v ", tt.BuildSource)
	str += fmt.Sprintf("TransferMode: %v ", tt.TransferMode)
	if tt.Error != "" {
		str += fmt.Sprintf("Error: %v ", tt.Error)
	}
	str += fmt.Sprintf("InstId: %v ", tt.InstId)
	str += fmt.Sprintf("Inst: %v \n", tt.IndexInst)
	return str

}
