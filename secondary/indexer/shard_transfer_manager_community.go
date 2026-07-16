//go:build community
// +build community

package indexer

import "github.com/couchbase/indexing/secondary/common"

type ShardTransferManager struct {
	config common.Config
	cmdCh  chan Message
}

func NewShardTransferManager(config common.Config, supvWrkrCh chan Message) *ShardTransferManager {
	return nil
}

func (stm *ShardTransferManager) ProcessCommand(cmd Message) {
	// no-op
}

func (stm *ShardTransferManager) fetchShardKeys(
	shardIDs []common.ShardId,
	shardType common.ShardType,
) ([]common.ShardKeyBundle, error) {

	return nil, nil
}
