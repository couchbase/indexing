//go:build community
// +build community

package indexer

import "github.com/couchbase/indexing/secondary/common"

type ShardTransferManager struct {
	config common.Config
	cmdCh  chan Message
}

func NewShardTransferManager(config common.Config) *ShardTransferManager {
	return nil
}

func (stm *ShardTransferManager) ProcessCommand(cmd Message) {
	// no-op
}
