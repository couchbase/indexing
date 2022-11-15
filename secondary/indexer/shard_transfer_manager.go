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

	sliceList          []Slice
	sliceCloseNotifier map[common.ShardId]MsgChannel
}

func NewShardTransferManager(config common.Config) *ShardTransferManager {
	stm := &ShardTransferManager{
		config:             config,
		cmdCh:              make(chan Message),
		sliceCloseNotifier: make(map[common.ShardId]MsgChannel),
	}

	go stm.run()
	return stm
}

func (stm *ShardTransferManager) run() {
	//main ShardTransferManager loop
	ticker := time.NewTicker(time.Duration(1 * time.Second))
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

		case <-ticker.C:
			stm.updateSliceStatus()
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
		// Indexer guarantees that all indexs of a shard will be dropped before
		// destroying a shard. Therefore, it is safe to assume that all the slices
		// have gone through MONITOR_SLICE_STATUS message before reaching here
		shardIds := cmd.(*MsgDestroyLocalShardData).GetShardIds()
		notifyChMap := make(map[common.ShardId]MsgChannel)
		for _, shardId := range shardIds {
			notifyCh := make(MsgChannel)
			stm.sliceCloseNotifier[shardId] = notifyCh
			notifyChMap[shardId] = notifyCh
		}

		go stm.processDestroyLocalShardMessage(cmd, notifyChMap)

	case MONITOR_SLICE_STATUS:
		stm.handleMonitorSliceStatusCommand(cmd)
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
	instRenameMap := msg.GetInstRenameMap()
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
			if instRenameMap != nil && len(instRenameMap[shardId]) > 0 {
				meta[plasma.GSIReplicaRepair] = instRenameMap[shardId]
			}

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

func (stm *ShardTransferManager) waitForSliceClose(shardId common.ShardId, notifyCh MsgChannel, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(time.Duration(30 * time.Second))
	for {
		select {
		case <-notifyCh:
			logging.Infof("ShardTranferManager::waitForSliceClose - Exiting wait as all slices are closed for shard: %v", shardId)
			return
		case <-ticker.C:
			logging.Infof("ShardTranferManager::waitForSliceClose - Waiting for all slices to be closed on shard: %v to be closed", shardId)
		}
	}
}

func (stm *ShardTransferManager) processDestroyLocalShardMessage(cmd Message, notifyChMap map[common.ShardId]MsgChannel) {

	start := time.Now()

	storageDir := stm.config["storage_dir"].String()
	plasma.SetStorageDir(storageDir)

	msg := cmd.(*MsgDestroyLocalShardData)
	logging.Infof("ShardTransferManager::processDestroyLocalShardMessage processing command: %v", msg)

	shardIds := msg.GetShardIds()
	respCh := msg.GetRespCh()

	var wg sync.WaitGroup
	for shardId, notifyCh := range notifyChMap {
		wg.Add(1)
		go stm.waitForSliceClose(shardId, notifyCh, &wg)
	}
	wg.Wait()
	logging.Infof("ShardTransferManager::processDestroyLocalShardMessage All slices closed. Initiating shard destroy for shards: %v, elapsed: %v", shardIds, time.Since(start))

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

// Update shard transfer manager's book-keeping with the new slices that
// are about to be closed
func (stm *ShardTransferManager) handleMonitorSliceStatusCommand(cmd Message) {
	sliceList := cmd.(*MsgMonitorSliceStatus).GetSliceList()

	stm.sliceList = append(stm.sliceList, sliceList...)
}

func (stm *ShardTransferManager) updateSliceStatus() {
	newSliceList := make([]Slice, 0)
	pendingSliceCloseMap := make(map[common.ShardId]bool)

	for i, slice := range stm.sliceList {
		if slice != nil && slice.IsCleanupDone() {
			stm.sliceList[i] = nil
		} else if slice != nil {
			newSliceList = append(newSliceList, slice)
			shardIds := slice.GetShardIds()
			for _, shardId := range shardIds {
				pendingSliceCloseMap[shardId] = true
			}
		}
	}

	// If all slices of a shard are closed and book-keeping is updated
	// before DestroyShardId message is sent, then the shardId will not
	// be found in pendingSliceCloseMap list. In that case, close any pending
	// notifier and update the book-keeping
	for shardId, notifyCh := range stm.sliceCloseNotifier {
		if _, ok := pendingSliceCloseMap[shardId]; !ok {

			if notifyCh != nil {
				close(notifyCh)
			}
			logging.Infof("ShardTransferManager::updateSliceStatus Closing the notifyCh for shardId: %v", shardId)
			delete(stm.sliceCloseNotifier, shardId)
		}
	}

	stm.sliceList = newSliceList
}
