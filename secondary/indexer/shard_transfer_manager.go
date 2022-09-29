package indexer

import (
	"sync"

	"github.com/couchbase/indexing/secondary/common"
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
	config common.Config
}

func NewShardTransferManager(config common.Config) *ShardTransferManager {
	return &ShardTransferManager{
		config: config,
	}
}

func (stm *ShardTransferManager) processShardTransferMessage(cmd Message) {

	msg := cmd.(*MsgStartShardTransfer)
	logging.Infof("ShardTransferManager::processShardTransferMessage Initiating command: %v", msg)

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

		logging.Infof("ShardTransferManager::processShardTransferMessage doneCb invoked for shardId: %v, path: %v, err: %v", shardId, shardPath, err)

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

func (stm *ShardTransferManager) processTransferCleanupMessage(cmd Message) {

	msg := cmd.(*MsgShardTransferCleanup)
	logging.Infof("ShardTransferManager::processTransferCleanupMessage Initiating command: %v", msg)

	shardPaths := msg.GetShardPaths()
	destination := msg.GetDestination()
	rebalanceId := msg.GetRebalanceId()
	ttid := msg.GetTransferTokenId()
	respCh := msg.GetRespCh()

	for shardId, shardPath := range shardPaths {
		meta := make(map[string]interface{})
		meta[plasma.GSIRebalanceId] = rebalanceId
		meta[plasma.GSIRebalanceTransferToken] = ttid
		meta[plasma.GSIShardID] = int64(shardId)
		meta[plasma.GSIShardUploadPath] = shardPath

		plasma.UnlockShard(plasma.ShardId(shardId))
		err := plasma.DoCleanup(destination, meta)
		if err != nil {
			logging.Errorf("ShardTransferManager::processTransferCleanupMessage Error initiating "+
				"cleanup for destination: %v, meta: %v, err: %v", destination, meta, err)
		}
	}

	logging.Infof("ShardTransferManager::processTransferCleanupMessage Clean-up initiated for all shards")
	// Notify the caller that cleanup has been initiated for all shards
	respCh <- true
}

func (stm *ShardTransferManager) processShardRestoreMessage(cmd Message) {
	msg := cmd.(*MsgStartShardRestore)
	logging.Infof("ShardTransferManager::processShardRestoreMessage Initiating command: %v", msg)

	shardPaths := msg.GetShardPaths()
	rebalanceId := msg.GetRebalanceId()
	ttid := msg.GetTransferTokenId()
	destination := msg.GetDestination()
	rebalCancelCh := msg.GetCancelCh()
	respCh := msg.GetRespCh()
	progressCh := msg.GetProgressCh()

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

		logging.Infof("ShardTransferManager::processShardRestoreMessage doneCb invoked for shardId: %v, path: %v, err: %v", shardId, shardPath, err)

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
		for shardId, shardPath := range shardPaths {
			wg.Add(1)

			meta := make(map[string]interface{})
			meta[plasma.GSIRebalanceId] = rebalanceId
			meta[plasma.GSIRebalanceTransferToken] = ttid
			meta[plasma.GSIShardID] = shardId
			meta[plasma.GSIShardUploadPath] = shardPath
			meta[plasma.GSIStorageDir] = stm.config["storage_dir"].String()

			if err := plasma.RestoreShard(destination, doneCb, progressCb, cancelCh, meta); err != nil {

				func() { // update errMap with error due to failure
					mu.Lock()
					defer mu.Unlock()

					errMap[shardId] = err
				}()

				wg.Done()
				logging.Errorf("ShardTransferManager::processShardRestoreMessage: Error when restoring shard: %v from path: %v", shardId, shardPath)

				closeCancelCh() // Abort already initiated transfers
				break           // Do not initiate transfer for remaining shards
			}
		}

		wg.Wait() // Wait for all transfer shard go-routines to complete execution
		close(doneCh)
	}()

	sendResponse := func() {
		logging.Infof("ShardTransferManager::processShardRestoreMessage All shards are restored. Sending response "+
			"errMap: %v, shardPaths: %v, destination: %v", errMap, shardPaths, destination)

		respMsg := &MsgShardTransferResp{
			errMap:     errMap,
			shardPaths: shardPaths, // Used by rebalancer to invoke local cleanup of shards
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

func (stm *ShardTransferManager) processDestroyLocalShardMessage(cmd Message) {

	msg := cmd.(*MsgDestroyLocalShardData)
	logging.Infof("ShardTransferManager::processDestroyLocalShardMessage processing command: %v", msg)

	shardIds := msg.GetShardIds()
	respCh := msg.GetRespCh()

	for _, shardId := range shardIds {
		if err := plasma.DestroyShardID(plasma.ShardId(shardId)); err != nil {
			logging.Errorf("ShardTransferManager::processDestroyLocalShardMessage Error cleaning-up shardId: %v from "+
				"local file system, err: %v", shardId, err)
		}
	}

	logging.Errorf("ShardTransferManager::processDestroyLocalShardMessage Done clean-up for shards: %v", shardIds)

	respCh <- true
}
