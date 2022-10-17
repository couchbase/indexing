//go:build !community
// +build !community

package indexer

import (
	"sync"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/plasma"
)

type ShardTransferManager struct {
	config common.Config
	cmdCh  chan Message
}

func NewShardTransferManager(config common.Config) *ShardTransferManager {
	stm := &ShardTransferManager{
		config: config,
		cmdCh:  make(chan Message),
	}

	go stm.run()
	return stm
}

func (stm *ShardTransferManager) run() {
	//main Storage Manager loop
loop:
	for {
		select {

		case cmd, ok := <-stm.cmdCh:
			if ok {
				if cmd.GetMsgType() == STORAGE_MGR_SHUTDOWN {
					logging.Infof("ShardTransferManager::run Storage manager shutting Down. Close ShardTransfer Manager as well")
					break loop
				}
				stm.handleStorageMgrCommands(cmd)
			} else {
				//supervisor channel closed. exit
				break loop
			}

		}
	}
}

func (stm *ShardTransferManager) ProcessCommand(cmd Message) {
	stm.cmdCh <- cmd
}

func (stm *ShardTransferManager) handleStorageMgrCommands(cmd Message) {

	switch cmd.GetMsgType() {

	case START_SHARD_TRANSFER:
		go stm.processShardTransferMessage(cmd)

	case SHARD_TRANSFER_CLEANUP:
		go stm.processTransferCleanupMessage(cmd)

	case START_SHARD_RESTORE:
		go stm.processShardRestoreMessage(cmd)

	case DESTROY_LOCAL_SHARD:
		go stm.processDestroyLocalShardMessage(cmd)
	}
}

func (stm *ShardTransferManager) processShardTransferMessage(cmd Message) {

	msg := cmd.(*MsgStartShardTransfer)
	logging.Infof("ShardTransferManager::processShardTransferMessage Initiating command: %v", msg)

	start := time.Now()

	shardIds := msg.GetShardIds()
	rebalanceId := msg.GetRebalanceId()
	ttid := msg.GetTransferTokenId()
	rebalCancelCh := msg.GetCancelCh()
	rebalDoneCh := msg.GetDoneCh()
	destination := msg.GetDestination()
	respCh := msg.GetRespCh()
	progressCh := msg.GetProgressCh()

	// Used by plasma to construct a path on S3
	meta := make(map[string]interface{})
	meta[plasma.GSIRebalanceId] = rebalanceId
	meta[plasma.GSIRebalanceTransferToken] = ttid

	// Closed when all shards are done processing
	transferDoneCh := make(chan bool)

	// cancelCh is shared between both mainStore and backStore
	// transfer shard routines. As transfer happens asyncronously,
	// if any transfer of one shard fails, then closing this channel
	// will abort the transfer of other store
	cancelCh := make(chan bool)
	isClosed := false

	var mu sync.Mutex
	var wg sync.WaitGroup

	errMap := make(map[common.ShardId]error)
	shardPaths := make(map[common.ShardId]string)

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

		elapsed := time.Since(start).Seconds()
		logging.Infof("ShardTransferManager::processShardTransferMessage doneCb invoked for shardId: %v, path: %v, err: %v, elapsed(sec): %v", shardId, shardPath, err, elapsed)

		errMap[common.ShardId(shardId)] = err
		shardPaths[common.ShardId(shardId)] = shardPath

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
			shardId:      common.ShardId(transferStats.ShardId),
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
		close(transferDoneCh)
	}()

	sendResponse := func() {
		elapsed := time.Since(start).Seconds()
		logging.Infof("ShardTransferManager::processShardTransferMessage All shards processing done. Sending response "+
			"errMap: %v, shardPaths: %v, destination: %v, elapsed(sec): %v", errMap, shardPaths, destination, elapsed)

		respMsg := &MsgShardTransferResp{
			errMap:     errMap,
			shardPaths: shardPaths,
		}

		respCh <- respMsg
	}

	select {
	case <-rebalCancelCh: // This cancel channel is sent by rebalancer
		closeCancelCh()

	case <-rebalDoneCh:
		closeCancelCh()

	case <-transferDoneCh: // All shards are done processing
		sendResponse()
		return
	}

	// Incase rebalCancelCh or rebalDoneCh is closed first, then
	// wait for plasma to finish processing and then send response
	// to caller
	select {
	case <-transferDoneCh:
		sendResponse()
		return
	}

}

func (stm *ShardTransferManager) processTransferCleanupMessage(cmd Message) {

	msg := cmd.(*MsgShardTransferCleanup)
	logging.Infof("ShardTransferManager::processTransferCleanupMessage Initiating command: %v", msg)
	start := time.Now()

	shardPaths := msg.GetShardPaths()
	destination := msg.GetDestination()
	rebalanceId := msg.GetRebalanceId()
	ttid := msg.GetTransferTokenId()
	respCh := msg.GetRespCh()

	// For cleanup cases where indexer does not have the information about
	// the shardPaths
	if len(shardPaths) == 0 {
		meta := make(map[string]interface{})
		meta[plasma.GSIRebalanceId] = rebalanceId
		meta[plasma.GSIRebalanceTransferToken] = ttid

		err := plasma.DoCleanup(destination, meta)
		if err != nil {
			logging.Errorf("ShardTransferManager::processTransferCleanupMessage Error initiating "+
				"cleanup for destination: %v, meta: %v, err: %v", destination, meta, err)
		}

		elapsed := time.Since(start).Seconds()
		logging.Infof("ShardTransferManager::processTransferCleanupMessage Clean-up initiated for all shards, elapsed(sec): %v", elapsed)
		// Notify the caller that cleanup has been initiated for all shards
		respCh <- true
		return
	}

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

	elapsed := time.Since(start).Seconds()
	logging.Infof("ShardTransferManager::processTransferCleanupMessage Clean-up initiated for all shards, elapsed(sec): %v", elapsed)
	// Notify the caller that cleanup has been initiated for all shards
	respCh <- true
}

func (stm *ShardTransferManager) processShardRestoreMessage(cmd Message) {
	msg := cmd.(*MsgStartShardRestore)
	logging.Infof("ShardTransferManager::processShardRestoreMessage Initiating command: %v", msg)

	start := time.Now()
	shardPaths := msg.GetShardPaths()
	rebalanceId := msg.GetRebalanceId()
	ttid := msg.GetTransferTokenId()
	destination := msg.GetDestination()
	rebalCancelCh := msg.GetCancelCh()
	rebalDoneCh := msg.GetDoneCh()
	respCh := msg.GetRespCh()
	progressCh := msg.GetProgressCh()

	// Closed when all shards are done processing
	restoreDoneCh := make(chan bool)

	// cancelCh is shared between both mainStore and backStore
	// transfer shard routines. As transfer happens asyncronously,
	// if any transfer of one shard fails, then closing this channel
	// will abort the transfer of other store
	cancelCh := make(chan bool)
	isClosed := false

	var mu sync.Mutex
	var wg sync.WaitGroup

	errMap := make(map[common.ShardId]error)

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

		elapsed := time.Since(start).Seconds()
		logging.Infof("ShardTransferManager::processShardRestoreMessage doneCb invoked for shardId: %v, path: %v, err: %v, elapsed(sec): %v", shardId, shardPath, err, elapsed)

		errMap[common.ShardId(shardId)] = err
		shardPaths[common.ShardId(shardId)] = shardPath

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
			shardId:      common.ShardId(transferStats.ShardId),
		}
	}

	go func() {
		for shardId, shardPath := range shardPaths {
			wg.Add(1)

			meta := make(map[string]interface{})
			meta[plasma.GSIRebalanceId] = rebalanceId
			meta[plasma.GSIRebalanceTransferToken] = ttid
			meta[plasma.GSIShardID] = uint64(shardId)
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
		close(restoreDoneCh)
	}()

	sendResponse := func() {
		elapsed := time.Since(start).Seconds()
		logging.Infof("ShardTransferManager::processShardRestoreMessage All shards are restored. Sending response "+
			"errMap: %v, shardPaths: %v, destination: %v, elapsed(sec): %v", errMap, shardPaths, destination, elapsed)

		respMsg := &MsgShardTransferResp{
			errMap:     errMap,
			shardPaths: shardPaths, // Used by rebalancer to invoke local cleanup of shards
		}

		respCh <- respMsg
	}

	select {
	case <-rebalCancelCh: // This cancel channel is sent by rebalancer
		closeCancelCh()

	case <-rebalDoneCh:
		closeCancelCh()

	case <-restoreDoneCh: // All shards are done processing
		sendResponse()
		return
	}

	// Incase rebalCancelCh or rebalDoneCh is closed first, then
	// wait for plasma to finish processing and then send response
	// to caller
	select {
	case <-restoreDoneCh:
		sendResponse()
		return
	}

}

func (stm *ShardTransferManager) processDestroyLocalShardMessage(cmd Message) {

	start := time.Now()

	storageDir := stm.config["storage_dir"].String()
	plasma.SetStorageDir(storageDir)

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

	elapsed := time.Since(start).Seconds()
	logging.Infof("ShardTransferManager::processDestroyLocalShardMessage Done clean-up for shards: %v, elapsed(sec): %v", shardIds, elapsed)
	respCh <- true
}
