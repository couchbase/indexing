//go:build !community
// +build !community

package indexer

import (
	"errors"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"
	"github.com/couchbase/plasma"
)

type ShardRefCount struct {
	refCount int

	// If the shard has been locked for recovery, then this flag will be set to true
	lockedForRecovery bool
}

type ShardTransferManager struct {
	config common.Config
	cmdCh  chan Message

	// Storage manager command channel. Used to route
	// the response of rebalance transfer status
	supvWrkrCh chan Message

	// lockedShards represent the list of shards that are locked
	// for rebalance and are yet to be unlocked. Whenever shard
	// rebalancer acquires lock, this map is updated. The entires
	// in this map is cleared either when the shard is destroyed
	// (or) when the shard is unlocked
	lockedShards map[common.ShardId]*ShardRefCount
	mu           sync.Mutex

	maxDiskBW int

	sliceList          []Slice
	sliceCloseNotifier map[common.ShardId]MsgChannel

	// rpc server
	rpcMutex            sync.Mutex
	rpcSrv              plasma.RPCServer
	shouldRpcSrvBeAlive atomic.Bool // true when rpc server is started; false when it is supposed to be shutdown
}

func NewShardTransferManager(config common.Config, supvWrkrCh chan Message) *ShardTransferManager {
	stm := &ShardTransferManager{
		config:             config,
		cmdCh:              make(chan Message),
		lockedShards:       make(map[common.ShardId]*ShardRefCount),
		sliceCloseNotifier: make(map[common.ShardId]MsgChannel),
		supvWrkrCh:         supvWrkrCh,

		shouldRpcSrvBeAlive: atomic.Bool{},
	}
	stm.shouldRpcSrvBeAlive.Store(false)

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

	case CONFIG_SETTINGS_UPDATE:
		cfgUpdate := cmd.(*MsgConfigUpdate)
		if val, ok := cfgUpdate.cfg["rebalance.serverless.maxDiskBW"]; ok {
			newDiskBw := val.Int() * 1024 * 1024 // Convert to bytes/sec as plasma expects in bytes/sec
			if newDiskBw != stm.maxDiskBW {
				logging.Infof("ShardTransferManager::ConfigUpdate - Updating maxDiskBw to %v, prev value: %v", newDiskBw, stm.maxDiskBW)
				stm.maxDiskBW = newDiskBw
				plasma.SetOpRateLimit(plasma.GSIRebalanceId, int64(stm.maxDiskBW))
			}
		}

	case START_SHARD_TRANSFER:
		go stm.processShardTransferMessage(cmd)

	case SHARD_TRANSFER_CLEANUP:
		go stm.processTransferCleanupMessage(cmd)

	case SHARD_TRANSFER_STAGING_CLEANUP:
		go stm.processShardTransferStagingCleanupMessage(cmd)

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

	case LOCK_SHARDS:
		stm.handleLockShardsCommand(cmd)

	case UNLOCK_SHARDS:
		stm.handleUnlockShardsCommand(cmd)

	case RESTORE_SHARD_DONE:
		go stm.handleRestoreShardDone(cmd)

	case RESTORE_AND_UNLOCK_LOCKED_SHARDS:
		go stm.handleRestoreAndUnlockShards(cmd)

	case INDEXER_SECURITY_CHANGE:
		stm.handleSecurityChange(cmd)

	case START_PEER_SERVER:
		stm.handleStartPeerServer(cmd)

	case STOP_PEER_SERVER:
		stm.handleStopPeerServer(cmd)
	}
}

func copyMeta(meta map[string]interface{}) map[string]interface{} {
	metaCpy := make(map[string]interface{})
	for k, v := range meta {
		metaCpy[k] = v
	}
	return metaCpy
}

func (stm *ShardTransferManager) processShardTransferMessage(cmd Message) {

	msg := cmd.(*MsgStartShardTransfer)
	logging.Infof("ShardTransferManager::processShardTransferMessage Initiating command: %v", msg)

	start := time.Now()

	shardIds := msg.GetShardIds()
	taskType := msg.GetTaskType()
	taskCancelCh := msg.GetCancelCh()
	taskDoneCh := msg.GetDoneCh()
	destination := msg.GetDestination()
	region := msg.GetRegion()
	respCh := msg.GetRespCh()
	progressCh := msg.GetProgressCh()
	storageMgrCancelCh := msg.GetStorageMgrCancelCh()
	storageMgrRespCh := msg.GetStorageMgrRespCh()
	newAlternateShardIds := msg.GetNewAlternateShardIds()

	// If storage manager is the once cancelling transfer, then this flag
	// is set to true. In such a case, the errMap returned to caller will
	// be modified with 'ErrIndexRollback' so that rebalancer can continue
	// rebalance for other buckets and fail rebalance at the end. This is
	// a no-op for pause-resume codepaths
	isStorageMgrCancel := false

	// Used by plasma to construct a path on S3
	meta := make(map[string]interface{})

	switch taskType {
	case common.RebalanceTask:
		rebalanceId := msg.GetRebalanceId()
		ttid := msg.GetTransferTokenId()
		meta[plasma.GSIRebalanceId] = rebalanceId
		meta[plasma.GSIRebalanceTransferToken] = ttid
		if region != "" {
			meta[plasma.GSIBucketRegion] = region
		}
		if msg.IsPeerTransfer() {
			meta[plasma.RPCClientTLSConfig] = msg.GetTLSConfig()
			meta[plasma.RPCHTTPSetReqAuthCb] = (plasma.HTTPSetReqAuthCb)(msg.GetAuthCallback())
		}
	case common.PauseResumeTask:
		bucket := msg.GetBucket()
		meta[plasma.GSIPauseResume] = bucket
	}

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
		if progressCh != nil {
			// Send the progress to rebalancer
			progressCh <- &ShardTransferStatistics{
				totalBytes:   transferStats.TotalBytes,
				bytesWritten: transferStats.BytesWritten,
				transferRate: transferStats.AvgXferRate,
				shardId:      common.ShardId(transferStats.ShardId),
			}
		}
	}

	go func() {
		for i := range shardIds {

			metaCpy := copyMeta(meta)
			if len(newAlternateShardIds) > 0 {
				metaCpy[plasma.GSINewAlternateID] = newAlternateShardIds[i]
			}

			// TODO: Add a configurable setting to enable or disbale disk snapshotting
			// before transferring the shard
			wg.Add(1)

			if err := plasma.TransferShard(plasma.ShardId(shardIds[i]), destination, doneCb, progressCb, cancelCh, metaCpy); err != nil {

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

		if isStorageMgrCancel {
			logging.Infof("ShardTransferManager::processShardTransferMessage All shards processing done. "+
				"Updating errMap as IndexRollback due to transfer cancellation invoked by storage manager. ShardIds: %v", shardIds)
			for shardId := range errMap {
				errMap[shardId] = ErrIndexRollback
			}
		}

		respMsg := &MsgShardTransferResp{
			errMap:     errMap,
			shardPaths: shardPaths,
			shardIds:   shardIds,
			respCh:     respCh,
		}

		close(storageMgrRespCh)
		stm.supvWrkrCh <- respMsg
	}

	select {
	case <-taskCancelCh: // This cancel channel is sent by orchestrator task
		closeCancelCh()

	case <-taskDoneCh:
		closeCancelCh()

	case <-transferDoneCh: // All shards are done processing
		sendResponse()
		return

	case <-storageMgrCancelCh:
		isStorageMgrCancel = true
		closeCancelCh()
	}

	// Incase taskCancelCh or taskDoneCh is closed first, then
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

	destination := msg.GetDestination()
	region := msg.GetRegion()
	rebalanceId := msg.GetRebalanceId()
	ttid := msg.GetTransferTokenId()
	respCh := msg.GetRespCh()
	isSyncCleanup := msg.IsSyncCleanup()

	meta := make(map[string]interface{})
	meta[plasma.GSIRebalanceId] = rebalanceId
	meta[plasma.GSIRebalanceTransferToken] = ttid
	if region != "" {
		meta[plasma.GSIBucketRegion] = region
	}
	if msg.IsPeerTransfer() {
		meta[plasma.RPCClientTLSConfig] = msg.GetTLSConfig()
		meta[plasma.RPCHTTPSetReqAuthCb] = (plasma.HTTPSetReqAuthCb)(msg.GetAuthCallback())
	}

	if !isSyncCleanup { // Invoke asynchronous cleanup
		err := plasma.DoCleanup(destination, meta, nil)
		if err != nil {
			logging.Errorf("ShardTransferManager::processTransferCleanupMessage Error initiating "+
				"cleanup for destination: %v, meta: %v, err: %v", destination, meta, err)
		}
	} else { // Wait for cleanup to finish
		var wg sync.WaitGroup
		doneCb := func(err error) {
			logging.Infof("ShardTransferManager::processTransferCleanupMessage doneCb invoked for "+
				"ttid: %v, rebalanceId: %v", ttid, rebalanceId)
			if err != nil {
				logging.Errorf("ShardTransferManager::processTransferCleanupMessage error observed during "+
					"transfer cleanup, ttid: %v, rebalanceId: %v, err: %v", ttid, rebalanceId, err)
			}
			wg.Done()
		}

		wg.Add(1)
		err := plasma.DoCleanup(destination, meta, doneCb)
		if err != nil {
			logging.Errorf("ShardTransferManager::processTransferCleanupMessage Error initiating "+
				"cleanup for destination: %v, meta: %v, err: %v", destination, meta, err)
		}
		wg.Wait()
	}

	elapsed := time.Since(start).Seconds()
	logging.Infof("ShardTransferManager::processTransferCleanupMessage Clean-up initiated for all shards, elapsed(sec): %v", elapsed)
	// Notify the caller that cleanup has been initiated for all shards
	respCh <- true
	return
}

func (stm *ShardTransferManager) processShardTransferStagingCleanupMessage(cmd Message) {
	stm.cleanupStagingDirOnRestore(cmd)
	respCh := cmd.(*MsgShardTransferStagingCleanup).GetRespCh()
	respCh <- &MsgSuccess{}
}

func (stm *ShardTransferManager) cleanupStagingDirOnRestore(cmd Message) {
	msg := cmd.(*MsgStartShardRestore)

	cleanupStart := time.Now()
	var wg sync.WaitGroup
	var taskId, transferId string

	taskType := msg.GetTaskType()
	destination := msg.GetDestination()
	region := msg.GetRegion()

	meta := make(map[string]interface{})
	if taskType == common.RebalanceTask {
		taskId = msg.GetRebalanceId()
		transferId = msg.GetTransferTokenId()
		meta[plasma.GSIRebalanceId] = taskId
		meta[plasma.GSIRebalanceTransferToken] = transferId
		if msg.IsPeerTransfer() {
			meta[plasma.RPCClientTLSConfig] = msg.GetTLSConfig()
			meta[plasma.RPCHTTPSetReqAuthCb] = (plasma.HTTPSetReqAuthCb)(msg.GetAuthCallback())
		}
	} else if taskType == common.PauseResumeTask {
		taskId = msg.GetPauseResumeId()
		transferId = msg.GetBucket()
		meta[plasma.GSIPauseResume] = transferId
	} else {
		logging.Fatalf("ShardTransferManager::cleanupStagingDirOnRestore Invalid taskType seen, taskType: %v, "+
			"taskId: %v, transferId: %v", taskType, taskId, transferId)
		return // no-op for other task types
	}

	if region != "" {
		meta[plasma.GSIBucketRegion] = region
	}

	doneCb := func(err error) {
		defer wg.Done()
		logging.Infof("ShardTransferManager::cleanupStagingDirOnRestore Invoked doneCb for taskType: %v, "+
			"taskId: %v, transferId: %v", taskType, taskId, transferId)
		if err != nil {
			logging.Infof("ShardTransferManager::cleanupStagingDirOnRestore Error observed during cleanup of local staging "+
				" directory for taskType: %v, taskId: %v, transferId: %v, err: %v", taskType, taskId, transferId, err)
		}
	}

	wg.Add(1)
	err := plasma.DoCleanupStaging(destination, meta, doneCb)
	if err != nil {
		wg.Done()
		logging.Errorf("ShardTransferManager::cleanupStagingDirOnRestore Error initiating "+
			"cleanup for destination: %v, meta: %v, err: %v", destination, meta, err)
		return
	}
	wg.Wait()

	elapsed := time.Since(cleanupStart).Seconds()
	logging.Infof("ShardTransferManager::cleanupStagingDirOnRestore Clean-up done for staging directory for taskType: %v, "+
		"taskId: %v, transferId: %v, elapsed(sec): %v", taskType, taskId, transferId, elapsed)
}

func (stm *ShardTransferManager) processShardRestoreMessage(cmd Message) {
	msg := cmd.(*MsgStartShardRestore)
	logging.Infof("ShardTransferManager::processShardRestoreMessage Initiating command: %v", msg)

	start := time.Now()
	shardPaths := msg.GetShardPaths()
	var taskId, transferId string
	taskType := msg.GetTaskType()
	destination := msg.GetDestination()
	region := msg.GetRegion()
	instRenameMap := msg.GetInstRenameMap()
	rebalCancelCh := msg.GetCancelCh()
	rebalDoneCh := msg.GetDoneCh()
	respCh := msg.GetRespCh()
	progressCh := msg.GetProgressCh()

	switch taskType {
	case common.RebalanceTask:
		taskId = msg.GetRebalanceId()
		transferId = msg.GetTransferTokenId()
	case common.PauseResumeTask:
		taskId = msg.GetPauseResumeId()
		transferId = msg.GetBucket()
	}

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
		if progressCh != nil {
			// Send the progress to caller
			progressCh <- &ShardTransferStatistics{
				totalBytes:   transferStats.TotalBytes,
				bytesWritten: transferStats.BytesWritten,
				transferRate: transferStats.AvgXferRate,
				shardId:      common.ShardId(transferStats.ShardId),
			}
		}
	}

	go func() {
		for shardId, shardPath := range shardPaths {
			wg.Add(1)

			meta := make(map[string]interface{})
			switch taskType {
			case common.RebalanceTask:
				meta[plasma.GSIRebalanceId] = taskId
				meta[plasma.GSIRebalanceTransferToken] = transferId
				if msg.IsPeerTransfer() {
					meta[plasma.RPCClientTLSConfig] = msg.GetTLSConfig()
					meta[plasma.RPCHTTPSetReqAuthCb] = (plasma.HTTPSetReqAuthCb)(msg.GetAuthCallback())
				}
			case common.PauseResumeTask:
				meta[plasma.GSIPauseResume] = transferId
			}
			meta[plasma.GSIShardID] = uint64(shardId)
			meta[plasma.GSIShardUploadPath] = shardPath
			meta[plasma.GSIStorageDir] = stm.config["storage_dir"].String()

			if region != "" {
				meta[plasma.GSIBucketRegion] = region
			}

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

		// Upon completion of restore, cleanup the transferred data. Cleanup is a
		// best effort call. So, ignore any errors arising out during Cleanup

		// TODO: Does pause-resume need to handle any errors arising out of staging
		// cleanup during resume(?)
		stm.cleanupStagingDirOnRestore(cmd)

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
		} else {
			// Since the shard is being destroyed, delete the shard from book-keeping as
			// there is no need to unlock a deleted shard
			func() {
				stm.mu.Lock()
				defer stm.mu.Unlock()

				delete(stm.lockedShards, shardId)
			}()
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

func (stm *ShardTransferManager) handleLockShardsCommand(cmd Message) {
	lockMsg := cmd.(*MsgLockUnlockShards)

	stm.mu.Lock()
	defer stm.mu.Unlock()

	shardIds := lockMsg.GetShardIds()
	respCh := lockMsg.GetRespCh()
	isLockedForRecovery := lockMsg.IsLockedForRecovery()

	logging.Infof("ShardTransferManager::handleLockShardCommands Initiating shard locking for shards: %v, isLockedForRecovery: %v", shardIds, isLockedForRecovery)
	start := time.Now()

	errMap := make(map[common.ShardId]error)
	for _, shardId := range shardIds {
		err := plasma.LockShard(plasma.ShardId(shardId))
		if err != nil {
			logging.Errorf("ShardTransferManager::handleLockShardsCommand Error observed while locking shard: %v, err: %v", shardId, err)
		} else {
			if shardRefCount, ok := stm.lockedShards[shardId]; ok && shardRefCount != nil {
				shardRefCount.refCount++
				shardRefCount.lockedForRecovery = shardRefCount.lockedForRecovery || isLockedForRecovery
			} else {
				stm.lockedShards[shardId] = &ShardRefCount{
					refCount:          1,
					lockedForRecovery: isLockedForRecovery,
				}
			}

		}
		errMap[shardId] = err
	}

	logging.Infof("ShardTransferManager::handleLockShardCommands Done with shard locking for shardIds: %v, errMap: %v, elapsed: %v", shardIds, errMap, time.Since(start))

	respCh <- errMap
}

func (stm *ShardTransferManager) handleUnlockShardsCommand(cmd Message) {
	lockMsg := cmd.(*MsgLockUnlockShards)

	stm.mu.Lock()
	defer stm.mu.Unlock()

	shardIds := lockMsg.GetShardIds()
	respCh := lockMsg.GetRespCh()

	logging.Infof("ShardTransferManager::handleUnlockShardCommands Initiating shard unlock for shards: %v", shardIds)
	start := time.Now()

	errMap := make(map[common.ShardId]error)
	for _, shardId := range shardIds {
		err := plasma.UnlockShard(plasma.ShardId(shardId))
		if err != nil {
			logging.Errorf("ShardTransferManager::handleUnlockShardsCommand Error observed while unlocking shard: %v, err: %v", shardId, err)
		} else {
			if shardRefCount, ok := stm.lockedShards[shardId]; ok && shardRefCount != nil {
				shardRefCount.refCount--
				if shardRefCount.refCount <= 0 {
					logging.Infof("ShardTransferManager::handleUnlockShardCommands Clearing the book-keeping for shard: %v, refCount: %v", shardId, shardRefCount.refCount)
					delete(stm.lockedShards, shardId)
				}
			} else {
				delete(stm.lockedShards, shardId) // clear the book-keeping
			}
		}
		errMap[shardId] = err
	}

	logging.Infof("ShardTransferManager::handleUnlockShardCommands Done with shard unlock for shardIds: %v, errMap: %v, elapsed: %v", shardIds, errMap, time.Since(start))

	respCh <- errMap
}

func (stm *ShardTransferManager) handleRestoreShardDone(cmd Message) {
	restoreShardDoneMsg := cmd.(*MsgRestoreShardDone)
	shardIds := restoreShardDoneMsg.GetShardIds()
	respCh := restoreShardDoneMsg.GetRespCh()

	logging.Infof("ShardTransferManager::handleRestoreShardDone Initiating RestoreShardDone for shards: %v", shardIds)
	start := time.Now()
	for _, shardId := range shardIds {
		plasma.RestoreShardDone(plasma.ShardId(shardId))
	}
	logging.Infof("ShardTransferManager::handleRestoreShardDone Finished RestoreShardDone for shards: %v, elapsed: %v", shardIds, time.Since(start))
	respCh <- true
}

func (stm *ShardTransferManager) handleRestoreAndUnlockShards(cmd Message) {
	clone := make(map[common.ShardId]*ShardRefCount)

	msg := cmd.(*MsgRestoreAndUnlockShards)
	skipShards := msg.GetSkipShards()
	respCh := msg.GetRespCh()
	func() {
		stm.mu.Lock()
		defer stm.mu.Unlock()

		for shardId, shardRefCount := range stm.lockedShards {
			if skipShards != nil {
				if _, ok := skipShards[shardId]; ok {
					logging.Infof("ShardTransferManager::handleRestoreAndUnlockShards Skipping shard: %v from restore and unlock", shardId)
					delete(stm.lockedShards, shardId) // Clear the book-keeping
					continue
				}
			}
			clone[shardId] = shardRefCount
		}
	}()

	for shardId, shardRefCount := range clone {
		if shardRefCount == nil {
			logging.Infof("ShardTransferManager::handleRestoreAndUnlockShards shardRefCount is nil for shardId: %v", shardId)
			continue
		}

		logging.Infof("ShardTransferManager::handleRestoreAndUnlockShards shardId: %v, refCount: %v", shardId, shardRefCount.refCount)

		if shardRefCount.lockedForRecovery {
			logging.Infof("ShardTransferManager::handleRestoreAndUnlockShards Initiating RestoreShardDone for shardId: %v", shardId)
			plasma.RestoreShardDone(plasma.ShardId(shardId))
		}

		logging.Infof("ShardTransferManager::handleRestoreAndUnlockShards Initiating unlock for shardId: %v, refCount: %v", shardId, shardRefCount.refCount)
		refCount := shardRefCount.refCount
		for i := 0; i < refCount; i++ {
			if err := plasma.UnlockShard(plasma.ShardId(shardId)); err != nil {
				logging.Errorf("ShardTransferManager::handleRestoreAndUnlockShards Error observed while unlocking shard: %v, err: %v", shardId, err)
			} else {
				shardRefCount.refCount--
				logging.Infof("ShardTransferManager::handleRestoreAndUnlockShards Unlock successful for shardId: %v, remaining: %v", shardId, shardRefCount.refCount)
			}
		}

		if shardRefCount.refCount <= 0 {
			logging.Infof("ShardTransferManager::handleUnlockShardCommands Clearing the book-keeping for shard: %v, shardRefCount: %v", shardId, shardRefCount.refCount)
			delete(stm.lockedShards, shardId) // Clean the book-keeping
		}

	}

	respCh <- true
}

// caller should hold stm.rpcMutex.Lock
func (stm *ShardTransferManager) initPeerRPCServerNoLock(rebalId string) error {
	if stm.rpcSrv != nil {
		if stm.shouldRpcSrvBeAlive.Load() {
			logging.Warnf("ShardTransferManager::initPeerRPCServerNoLock peer server object is not nil and is expected to be running. will skip starting server again...")
			return nil
		}
		logging.Warnf("ShardTransferManager::initPeerRPCServerNoLock received start server msg for rebalance %v while a server was already running. cleaning up old server...",
			rebalId)

		if err := stm.rpcSrv.Shutdown(); err != nil {
			logging.Errorf("ShardTransferManager::initPeerRPCServerNoLock failed to shutdown running RPC server with err %v for rebalance %v",
				err, rebalId)
			return err
		}

		stm.rpcSrv = nil
	}
	port := stm.config["shardTransferServerPort"].String()
	nodeAddr := net.JoinHostPort("", port)

	dir := stm.config["storage_dir"].String()

	cfg := loadRPCServerConfig(stm.config)
	cfg.RPCHttpServerCfg.DoServe = false

	httpSrv := &http.Server{
		Addr: nodeAddr,
	}

	lst, err := security.MakeAndSecureTCPListener(nodeAddr)
	lstClose := func() {
		if lst != nil {
			if errLstClose := lst.Close(); errLstClose != nil {
				logging.Errorf("ShardTransferManager::initRPCServerNoLock failed to close TCP listener with error %v",
					errLstClose)
			}
		}
	}
	if err != nil {
		lstClose()
		logging.Errorf("ShardTransferManager::initPeerRPCServerNoLock failed to create a secure listener with error %v",
			err)
		return err
	}

	mux := http.NewServeMux()
	rpcSrv, err := plasma.NewRPCServerWithHTTP(nodeAddr, httpSrv, lst, mux,
		dir, plasma.DefaultConfig().Environment, plasma.GetOpRateLimiter(plasma.GSIRebalanceId), cfg.RPCHttpServerCfg)
	if err != nil {
		lstClose()
		logging.Errorf("ShardTransferManager::initPeerRPCServerNoLock failed to create Plasma RPC server with error %v",
			err)
		return err
	}

	mux.HandleFunc(rpcSrv.Url, authMiddlewareForShardTransfer(rpcSrv.RPCHandler))

	if err := rpcSrv.Start(); err != nil {
		lstClose()
		logging.Errorf("ShardTransferManager::initPeerRPCServerNoLock failed to start RPC server with error %v",
			err)
		return err
	}
	stm.rpcSrv = rpcSrv

	go func() {
		if err := rpcSrv.HttpSrv.Serve(lst); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logging.Errorf("ShardTransferManager::initPeerRPCServerNoLock server failed with error %v. shuting down RPC server...",
				err)
			// rpcSrv shutsdown both http server and listener
			rpcSrv.Shutdown()

			// if the server is expected to be alive
			if stm.shouldRpcSrvBeAlive.Load() {
				// locked call expected here as the parent go-routine of Serve could be running
				// when the lock was removed
				go stm.initPeerRPCServer(rebalId)
			}
		}
	}()

	return nil
}

func (stm *ShardTransferManager) initPeerRPCServer(rebalId string) error {
	stm.rpcMutex.Lock()
	defer stm.rpcMutex.Unlock()

	return stm.initPeerRPCServerNoLock(rebalId)
}

func (stm *ShardTransferManager) destroyPeerRPCServerNoLock(rebalId string) error {
	if stm.rpcSrv == nil {
		logging.Warnf("ShardTransferManager::destroyPeerRPCServerNoLock received stop command for rebalance %v while server was not running",
			rebalId)
		return nil
	}

	err := stm.rpcSrv.Shutdown()
	if err == nil {
		stm.rpcSrv = nil
	} else {
		logging.Errorf("ShardTransferManager::destroyPeerRPCServerNoLock failed to stop RPC server with error %v",
			err)
	}
	return err
}

func (stm *ShardTransferManager) destroyPeerRPCServer(rebalId string) error {
	stm.rpcMutex.Lock()
	defer stm.rpcMutex.Unlock()

	return stm.destroyPeerRPCServerNoLock(rebalId)
}

func (stm *ShardTransferManager) handleSecurityChange(cmd Message) {
	secChange := cmd.(*MsgSecurityChange)

	if !stm.shouldRpcSrvBeAlive.Load() {
		// rpc srv could be marked for closed but not yet destroyed
		// we can skip security change for that instance and let destroy shutdown the server
		return
	}

	stm.rpcMutex.Lock()
	defer stm.rpcMutex.Unlock()

	if secChange.refreshCert && stm.rpcSrv != nil {
		// restart rpc server for certificate change

		// shutdown should trigger srv.httpSrv.Serve to fail with error ErrServerClosed
		shutdownWithRetries := func(attempts int, lastErr error) error {
			if lastErr != nil {
				logging.Warnf("ShardTransferManager::handleSecurityChange: failed %v attempt to shutdown RPC server with error %v",
					attempts, lastErr)
			}
			return stm.destroyPeerRPCServerNoLock("security_change")
		}
		rh := c.NewRetryHelper(10, 1*time.Millisecond, 10, shutdownWithRetries)
		err := rh.Run()
		if err != nil {
			logging.Fatalf("ShardTransferManager::handleSecurityChange: failed to stop RPC server with retries on error %v",
				err)
			c.CrashOnError(err)
		}

		restartWithRetries := func(attempts int, lastErr error) error {
			if lastErr != nil {
				logging.Warnf("ShardTransferManager::handleSecurityChange: failed %v attempt to restart RPC server with error %v",
					attempts, lastErr)
			}
			return stm.initPeerRPCServerNoLock("security_change")
		}
		rh = c.NewRetryHelper(10, 1*time.Millisecond, 10, restartWithRetries)
		err = rh.Run()
		if err != nil {
			logging.Fatalf("ShardTransferManager::handleSecurityManager: failed to restart RPC server with retries on error %v",
				err)
			c.CrashOnError(err)
		}
	}
}

func (stm *ShardTransferManager) handleStartPeerServer(cmd Message) {
	msg := cmd.(*MsgPeerServerCommand)

	rebalId := msg.GetRebalanceId()
	respCh := msg.GetRespCh()

	startServer := func(attempts int, lastErr error) error {
		if lastErr != nil {
			logging.Errorf("ShardTransferManager::handleStartPeerServer: %v attempt to start peer server for rebalance %v failed with err %v",
				attempts, rebalId, lastErr)
		}
		return stm.initPeerRPCServerNoLock(rebalId)
	}

	// keeping the lock here so that we don't stop rpc server while a start is being attempted
	stm.rpcMutex.Lock()
	defer stm.rpcMutex.Unlock()

	rh := c.NewRetryHelper(10, 1*time.Millisecond, 10, startServer)
	err := rh.Run()

	if err == nil {
		stm.shouldRpcSrvBeAlive.Store(true)
	}
	respCh <- err

}

func (stm *ShardTransferManager) handleStopPeerServer(cmd Message) {
	msg := cmd.(*MsgPeerServerCommand)

	rebalId := msg.GetRebalanceId()
	respCh := msg.GetRespCh()

	stopServer := func(attempts int, lastErr error) error {
		if lastErr != nil {
			logging.Errorf("ShardTransferManager::handleStopPeerServer: %v attempt to stop peer server for rebalance %v failed with err %v",
				attempts, rebalId, lastErr)
		}
		return stm.destroyPeerRPCServerNoLock(rebalId)
	}

	// keeping the lock here so that we don't start rpc server while a stop is being attempted
	stm.rpcMutex.Lock()
	defer stm.rpcMutex.Unlock()

	stm.shouldRpcSrvBeAlive.Store(false)

	rh := c.NewRetryHelper(10, 10*time.Millisecond, 10, stopServer)
	respCh <- rh.Run()
}

func authMiddlewareForShardTransfer(next http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		creds, valid, err2 := c.IsAuthValid(r)
		if err2 != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err2.Error() + "\n"))
			return
		} else if !valid {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write(c.HTTP_STATUS_UNAUTHORIZED)
			return
		} else if creds != nil {
			allowed, err := creds.IsAllowed("cluster.admin.internal.index!read")
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				return
			} else if !allowed {
				w.WriteHeader(http.StatusForbidden)
				w.Write(c.HTTP_STATUS_FORBIDDEN)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
