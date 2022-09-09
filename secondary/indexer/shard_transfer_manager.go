package indexer

import (
	"sync"

	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/plasma"
)

type ShardTransferStatistics struct {
	shardId      uint64
	totalBytes   int64
	bytesWritten int64
	transferRate float64
}

type ShardTransferManager struct {
}

func NewShardTransferManager() *ShardTransferManager {
	return &ShardTransferManager{}
}

func (stm *ShardTransferManager) processShardTransferMessage(cmd Message) {

	logging.Infof("ShardTransferManager::processShardTransferMessage Initiating command: %v", cmd)
	msg := cmd.(*MsgStartShardTransfer)

	shardIds := msg.GetShardIds()
	rebalanceId := msg.GetRebalanceId()
	ttid := msg.GetTransferTokenId()
	rebalCancelCh := msg.GetCancelCh()
	destination := msg.GetDestination()
	respCh := msg.GetRespCh()
	progressCh := msg.GetProgressCh()

	// Used by plasma to construct a path on S3
	meta := make(map[string]interface{})
	meta[plasma.GSIRebalanceId] = rebalanceId
	meta[plasma.GSIRebalanceTransferToken] = ttid

	// Closed when all shards are done processing
	doneCh := make(chan bool)

	// cancelCh is shared between both mainStore and backStore
	// transfer shard routines. As transfer happens asyncronously,
	// if any transfer of one shard fails, then closing this channel
	// will abort the transfer of other store
	cancelCh := make(chan bool)
	isClosed := false

	var mu sync.Mutex
	var wg sync.WaitGroup

	errMap := make(map[uint64]error)
	shardPaths := make(map[uint64]string)

	closeCancelCh := func() {
		mu.Lock()
		defer mu.Unlock()
		if !isClosed {
			isClosed = true
			close(cancelCh)
		}
	}

	doneCb := func(err error, shardId plasma.ShardId, shardPath string) {
		defer wg.Done()

		mu.Lock()
		defer mu.Unlock()

		errMap[uint64(shardId)] = err
		shardPaths[uint64(shardId)] = shardPath

		if err != nil && !isClosed {
			isClosed = true
			close(cancelCh)
		}
	}

	progressCb := func(transferStats plasma.ShardTransferStatistics) {
		// Send the progress to rebalancer
		progressCh <- &ShardTransferStatistics{
			totalBytes:   transferStats.TotalBytes,
			bytesWritten: transferStats.BytesWritten,
			transferRate: transferStats.AvgXferRate,
			shardId:      uint64(transferStats.ShardId),
		}
	}

	go func() {
		for i := range shardIds {

			// TODO: Add a configurable setting to enable or disbale disk snapshotting
			// before transferring the shard

			wg.Add(1)
			// Lock shard to prevent any index instance mapping while transfer is in progress
			// Unlock of the shard happens:
			// (a) after shard transfer is successful & destination node has recovered the shard
			// (b) If any error is encountered, clean-up from indexer will unlock the shards
			plasma.LockShard(plasma.ShardId(shardIds[i]))

			if err := plasma.TransferShard(plasma.ShardId(shardIds[i]), destination, doneCb, progressCb, cancelCh, meta); err != nil {

				func() { // update errMap for this shard
					mu.Lock()
					defer mu.Unlock()

					errMap[shardIds[i]] = err
				}()

				wg.Done()
				logging.Errorf("ShardTransferManager::processShardTransferMessage: Error when starting to transfer shard: %v", shardIds[i])

				closeCancelCh() // Abort already initiated transfers
				break           // Do not initiate transfer for remaining shards
			}
		}

		wg.Wait() // Wait for all transfer shard go-routines to complete execution
		close(doneCh)
	}()

	sendResponse := func() {
		logging.Infof("ShardTransferManager::processShardTransferMessage All shards processing done. Sending response "+
			"errMap: %v, shardPaths: %v, destination: %v", errMap, shardPaths, destination)

		respMsg := &MsgShardTransferResp{
			errMap:     errMap,
			shardPaths: shardPaths,
		}

		respCh <- respMsg
	}

	select {
	case <-rebalCancelCh: // This cancel channel is sent by rebalancer
		closeCancelCh()

	case <-doneCh: // All shards are done processing
		sendResponse()
		return
	}

	// Incase rebalCancelCh is closed first, then wait for all
	// transfers to invoke doneCb with the uploaded paths so
	// that rebalancer can initiate cleanup. If doneCh is invoked
	// first in the above select, then this code will not be
	// executed
	select {
	case <-doneCh:
		sendResponse()
		return
	}

}
