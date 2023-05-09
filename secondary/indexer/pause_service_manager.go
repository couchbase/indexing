// @copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	forestdb "github.com/couchbase/indexing/secondary/fdb"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/common"
	couchbase "github.com/couchbase/indexing/secondary/dcp"
	"github.com/couchbase/indexing/secondary/logging"
	mc "github.com/couchbase/indexing/secondary/manager/common"
	"github.com/couchbase/plasma"
)

////////////////////////////////////////////////////////////////////////////////////////////////////
// PauseServiceManager class
////////////////////////////////////////////////////////////////////////////////////////////////////

// PauseMgr points to the singleton of this class, which will still be nil early in user
// ScanCoordinator's lifecycle, hence the need for atomics. It is really type *PauseServiceManager.
var PauseMgr unsafe.Pointer

// PauseServiceManager provides the implementation of the Pause-Resume-specific APIs of
// ns_server RPC Manager interface (defined in cbauth/service/interface.go).
type PauseServiceManager struct {
	genericMgr *GenericServiceManager // pointer to our parent
	httpAddr   string                 // local host:port for HTTP: "127.0.0.1:9102", 9108, ...

	config common.ConfigHolder

	// bucketStates is a map from bucket to its Pause-Resume state. The design supports concurrent
	// pauses and resumes of different buckets, although this may not be done in practice.
	bucketStates map[string]bucketStateEnum

	// bucketStatesMu protects bucketStates
	bucketStatesMu sync.RWMutex

	// tasks is the set of Pause-Resume tasks that are running, if any, mapped by taskId
	tasks map[string]*taskObj

	// tasksMu protects tasks
	tasksMu sync.RWMutex

	supvMsgch MsgChannel //channel to send msg to supervisor for normal handling (idx.wrkrRecvCh)
	supvCmdch MsgChannel //channel to receive command msg from supervisor (idx.prMgrCmdCh)

	// Track global PauseTokens
	pauseTokensById map[string]*PauseToken
	pauseTokenMapMu sync.RWMutex

	// Track Pausers
	pausersById  map[string]*Pauser
	pausersMapMu sync.Mutex

	// Track Resumers
	resumersById  map[string]*Resumer
	resumersMapMu sync.Mutex

	nodeInfo *service.NodeInfo

	cleanupPending int32

	pauseResumeRunningById *PauseResumeRunningMap
}

// NewPauseServiceManager is the constructor for the PauseServiceManager class.
// GenericServiceManager constructs a singleton at boot.
//
//	genericMgr - pointer to our parent
//	mux - Indexer's HTTP server
//	httpAddr - host:port of the local node for Index Service HTTP calls
func NewPauseServiceManager(genericMgr *GenericServiceManager, mux *http.ServeMux, supvCmdch,
	supvMsgch MsgChannel, httpAddr string, config common.Config, nodeInfo *service.NodeInfo,
	pauseResumeRunningById *PauseResumeRunningMap, pauseTokens map[string]*PauseToken) *PauseServiceManager {

	m := &PauseServiceManager{
		genericMgr: genericMgr,
		httpAddr:   httpAddr,

		bucketStates: make(map[string]bucketStateEnum),
		tasks:        make(map[string]*taskObj),

		supvMsgch: supvMsgch,
		supvCmdch: supvCmdch,

		pauseTokensById: make(map[string]*PauseToken),
		pausersById:     make(map[string]*Pauser),
		resumersById:    make(map[string]*Resumer),

		nodeInfo: nodeInfo,

		pauseResumeRunningById: NewPauseResumeRunningMap(),
	}
	m.config.Store(config)

	for pauseId, pt := range pauseTokens {
		m.pauseTokensById[pauseId] = pt
	}
	m.setCleanupPending(len(m.pauseTokensById) > 0)

	pauseResumeRunningById.ForEveryKey(func(rMeta *pauseResumeRunningMeta, id string) {
		m.pauseResumeRunningById.SetRunning(rMeta.Typ, rMeta.BucketName, id)
	})

	// Save the singleton
	SetPauseMgr(m)

	// Internal REST APIs
	mux.HandleFunc("/pauseMgr/Pause", m.RestHandlePause)
	mux.HandleFunc("/pauseMgr/Progress", m.RestHandleGetProgress)

	// Unit test REST APIs -- THESE MUST STILL DO AUTHENTICATION!!
	mux.HandleFunc("/test/Pause", m.testPause)
	mux.HandleFunc("/test/PreparePause", m.testPreparePause)
	mux.HandleFunc("/test/PrepareResume", m.testPrepareResume)
	mux.HandleFunc("/test/Resume", m.testResume)
	mux.HandleFunc("/test/DestroyShards", m.testDestroyShards)

	go m.run()

	return m
}

func (m *PauseServiceManager) isCleanupPending() bool {
	return atomic.LoadInt32(&m.cleanupPending) == 1
}

func (m *PauseServiceManager) setCleanupPending(val bool) {
	if val {
		atomic.StoreInt32(&m.cleanupPending, 1)
	} else {
		atomic.StoreInt32(&m.cleanupPending, 0)
	}
}

// Track Pauser based on pauseId. Pauser can be deleted by calling with nil Pauser. If there is already a Pauser with
// the pauseId, then an error is returned.
func (m *PauseServiceManager) setPauser(pauseId string, p *Pauser) error {
	m.pausersMapMu.Lock()
	defer m.pausersMapMu.Unlock()

	if oldPauser, ok := m.pausersById[pauseId]; ok {

		if p == nil {
			delete(m.pausersById, pauseId)
		} else {
			return fmt.Errorf("conflict: Pauser[%v] with pauseId[%v] already present!", oldPauser, pauseId)
		}

	} else if p != nil {
		m.pausersById[pauseId] = p
	}

	return nil
}

// Get pauser based on pauseId. If there is no such Pauser, then the returned boolean will be false.
func (m *PauseServiceManager) getPauser(pauseId string) (*Pauser, bool) {
	m.pausersMapMu.Lock()
	defer m.pausersMapMu.Unlock()

	pauser, exists := m.pausersById[pauseId]

	return pauser, exists
}

// Track Resumer based on resumeId. Resumer can be deleted by calling with nil Resumer. If there is already a Resumer
// with the resumeId, then an error is returned.
func (m *PauseServiceManager) setResumer(resumeId string, r *Resumer) error {
	m.resumersMapMu.Lock()
	defer m.resumersMapMu.Unlock()

	if oldResumer, ok := m.resumersById[resumeId]; ok {

		if r == nil {
			delete(m.resumersById, resumeId)
		} else {
			return fmt.Errorf("conflict: Resumer[%v] with resumeId[%v] already present!", oldResumer, resumeId)
		}

	} else if r != nil {
		m.resumersById[resumeId] = r
	}

	return nil
}

// Get Resumer based on resumeId. If there is no such Resumer, then the returned boolean will be false.
func (m *PauseServiceManager) getResumer(resumeId string) (*Resumer, bool) {
	m.resumersMapMu.Lock()
	defer m.resumersMapMu.Unlock()

	resumer, exists := m.resumersById[resumeId]

	return resumer, exists
}

// GetPauseMgr atomically gets the global PauseMgr pointer. When called from ScanCoordinator it may
// still be nil as ScanCoordinator is constructed long before PauseServiceManager.
func GetPauseMgr() *PauseServiceManager {
	return (*PauseServiceManager)(atomic.LoadPointer(&PauseMgr))
}

// SetPauseMgr atomically sets the global PauseMgr pointer. This is done only once, when the
// PauseServiceManager singleton is constructed.
func SetPauseMgr(pauseMgr *PauseServiceManager) {
	atomic.StorePointer(&PauseMgr, unsafe.Pointer(pauseMgr))
}

// run is a blocking thread that runs forever and listens for commands from
// parent to update internal operations
func (psm *PauseServiceManager) run() {
	for {
		select {
		case cmd, ok := <-psm.supvCmdch:
			if ok {
				if cmd.GetMsgType() == ADMIN_MGR_SHUTDOWN {
					logging.Infof("PauseServiceManager::listenForCommands Shutting Down")
					psm.supvCmdch <- &MsgSuccess{}
					return
				}
				psm.handleSupervisorCommands(cmd)
			} else {
				//supervisor channel closed. exit
				return
			}
		}
	}
}

func (psm *PauseServiceManager) handleSupervisorCommands(cmd Message) {
	switch cmd.GetMsgType() {

	case CONFIG_SETTINGS_UPDATE:
		psm.handleConfigUpdate(cmd)

	case CLUST_MGR_INDEXER_READY:
		psm.handleIndexerReady(cmd)

	default:
		logging.Fatalf("PauseServiceManager::handleSupervisorCommands Unknown Message %+v", cmd)
		common.CrashOnError(errors.New("Unknown Msg On Supv Channel"))
	}
}

func (psm *PauseServiceManager) handleConfigUpdate(cmd Message) {
	cfgUpdate := cmd.(*MsgConfigUpdate)
	psm.config.Store(cfgUpdate.GetConfig())
	psm.supvCmdch <- &MsgSuccess{}
}

func (psm *PauseServiceManager) handleIndexerReady(cmd Message) {

	psm.supvCmdch <- &MsgSuccess{}

	go psm.recoverPauseResume()

}

func (m *PauseServiceManager) recoverPauseResume() {

	defer logging.Infof("PauseServiceManager::recoverPauseResume: Done cleanup")

	if !m.isCleanupPending() {
		return
	}

	// If there are any errors during cleanup, they are ignored. Any subsequent pause/resume may fail due to
	// conflicts with yet to be cleaned tokens, if they cannot be cleanup during prepare phase. It would be good
	// to have a janitor that cleans up such orphans so that prepare does not need to.

	for pauseId, pt := range m.pauseTokensById {
		logging.Infof("PauseServiceManager::recoverPauseResume: Init Pending Cleanup for pauseId[%v] mpt[%v]",
			pauseId, pt)

		switch pt.Type {

		case PauseTokenPause:
			ptFilter, putFilter := getPauseTokenFiltersByPauseId(pauseId)
			mpt, _, err := m.getCurrPauseTokens(ptFilter, putFilter)
			if err != nil || mpt == nil {
				logging.Errorf("PauseServiceManager::recoverPauseResume: Error Fetching Pause Metakv Tokens:"+
					"err[%v]", err)
				continue
			}

			if mpt.MasterIP == string(m.nodeInfo.NodeID) {
				if err := m.runPauseCleanupPhase(mpt.BucketName, mpt.PauseId, true); err != nil {
					logging.Errorf("PauseServiceManager::recoverPauseResume: Failed to cleanup pause master:"+
						"err[%v]", err)
					continue
				}
			} else {
				if err := m.runPauseCleanupPhase(mpt.BucketName, mpt.PauseId, false); err != nil {
					logging.Errorf("PauseServiceManager::recoverPauseResume: Failed to cleanup pause follower:"+
						"err[%v]", err)
					continue
				}
			}

		case PauseTokenResume:
			ptFilter, rdtFilter := getResumeTokenFiltersByResumeId(pauseId)
			mpt, _, err := m.getCurrResumeTokens(ptFilter, rdtFilter)
			if err != nil || mpt == nil {
				logging.Errorf("PauseServiceManager::recoverPauseResume: Error Fetching Resume Metakv Tokens:"+
					"err[%v]", err)
				continue
			}

			if mpt.MasterIP == string(m.nodeInfo.NodeID) {
				if err := m.runResumeCleanupPhase(mpt.BucketName, mpt.PauseId, true); err != nil {
					logging.Errorf("PauseServiceManager::recoverPauseResume: Failed to cleanup resume master:"+
						"err[%v]", err)
					continue
				}
			} else {
				if err := m.runResumeCleanupPhase(mpt.BucketName, mpt.PauseId, false); err != nil {
					logging.Errorf("PauseServiceManager::recoverPauseResume: Failed to cleanup resume follower:"+
						"err[%v]", err)
					continue
				}
				respCh := make(chan Message)
				m.supvMsgch <- &MsgShardTransferStagingCleanup{
					respCh:      respCh,
					destination: pt.ArchivePath,
					region:      pt.Region,
					taskId:      pt.PauseId,
					transferId:  pt.BucketName,
					taskType:    common.PauseResumeTask,
				}
				<-respCh

				logging.Infof("PauseServiceManager::recoverPauseResume: Cleaned staging shard dirs for resume id: %v",
					pt.PauseId)
			}

		}
	}

	m.pauseResumeRunningById.ForEveryKey(func(rMeta *pauseResumeRunningMeta, id string) {
		logging.Infof("PauseServiceManager::recoverPauseResume: Init Pending Cleanup for id[%v] rMeta[%v]",
			id, rMeta)

		switch rMeta.Typ {

		case PauseTokenPause:
			if err := m.runPauseCleanupPhase(rMeta.BucketName, id, false); err != nil {
				logging.Errorf("PauseServiceManager::recoverPauseResume: Failed to cleanup PauseResummeRunning:"+
					"err[%v] rMeta[%v] id[%s]", err, rMeta, id)
				return
			}

		case PauseTokenResume:
			if err := m.runResumeCleanupPhase(rMeta.BucketName, id, false); err != nil {
				logging.Errorf("PauseServiceManager::recoverPauseResume: Failed to cleanup PauseResummeRunning:"+
					"err[%v] rMeta[%v] id[%s]", err, rMeta, id)
				return
			}

		}
	})

	m.setCleanupPending(false)
}

func (psm *PauseServiceManager) lockShards(shardIds []common.ShardId) error {
	respCh := make(chan map[common.ShardId]error)

	msg := &MsgLockUnlockShards{
		mType:    LOCK_SHARDS,
		shardIds: shardIds,
		respCh:   respCh,
	}

	psm.supvMsgch <- msg

	errMap := <-respCh
	errBuilder := new(strings.Builder)
	for shardId, err := range errMap {
		if err != nil {
			errBuilder.WriteString(fmt.Sprintf("Failed to lock shard %v err: %v", shardId, err))
		}
	}
	if errBuilder.Len() == 0 {
		return nil
	}

	return errors.New(errBuilder.String())
}

func (psm *PauseServiceManager) unlockShards(shardIds []common.ShardId) error {
	respCh := make(chan map[common.ShardId]error)

	msg := &MsgLockUnlockShards{
		mType:    UNLOCK_SHARDS,
		shardIds: shardIds,
		respCh:   respCh,
	}

	psm.supvMsgch <- msg

	errMap := <-respCh
	errBuilder := new(strings.Builder)
	for shardId, err := range errMap {
		if err != nil {
			errBuilder.WriteString(fmt.Sprintf("Failed to unlock shard %v err: %v", shardId, err))
		}
	}
	if errBuilder.Len() == 0 {
		return nil
	}

	return errors.New(errBuilder.String())
}

func (psm *PauseServiceManager) copyShardsWithLock(
	shardIds []common.ShardId, taskId, bucket, destination string,
	cancelCh <-chan struct{}, progressUpdate func(float64),
) (map[common.ShardId]string, error) {
	err := psm.lockShards(shardIds)
	if err != nil {
		logging.Errorf("PauseServiceManager::copyShardsWithLock: locking shards failed. err -> %v for taskId %v", err, taskId)
		return nil, err
	}
	defer func() {
		err := psm.unlockShards(shardIds)
		if err != nil {
			logging.Errorf("PauseServiceManager::copyShardsWithLock: unlocking shards failed. err -> %v for taskId %v", err, taskId)
		}
	}()
	logging.Infof("PauseServiceManager::copyShardsWithLock: locked shards with id %v for taskId: %v",
		shardIds, taskId)

	respCh := make(chan Message)
	progressCh := make(chan *ShardTransferStatistics, 1000)
	lastReportedProgress := make(map[common.ShardId]int64)

	msg := &MsgStartShardTransfer{
		shardIds:    shardIds,
		transferId:  bucket,
		destination: destination,
		taskType:    common.PauseResumeTask,
		taskId:      taskId,
		respCh:      respCh,
		doneCh:      cancelCh, // abort transfer if msg received on done
		cancelCh:    cancelCh,
		progressCh:  progressCh,
	}

	psm.supvMsgch <- msg

	for {
		select {
		case stats := <-progressCh:
			if stats.totalBytes == 0 {
				continue
			}
			currProg := (stats.bytesWritten * 100) / stats.totalBytes
			progressUpdate(float64(currProg-lastReportedProgress[stats.shardId]) / 100.0)
			lastReportedProgress[stats.shardId] = currProg
		case respMsg := <-respCh:
			resp, ok := respMsg.(*MsgShardTransferResp)
			if !ok || resp == nil {
				err = fmt.Errorf("either response channel got closed or sent an invalid response")
				logging.Errorf("PauseServiceManager::copyShardsWithLock: %v for taskId %v",
					err, taskId)
				return nil, err
			}
			errMap := resp.GetErrorMap()
			errBuilder := new(strings.Builder)
			for shardId, errInCopy := range errMap {
				if errInCopy != nil {
					errBuilder.WriteString(fmt.Sprintf("Failed to copy shard %v, err: %v",
						shardId, errInCopy))
				}
			}
			if errBuilder.Len() != 0 {
				err = errors.New(errBuilder.String())
				logging.Errorf("PauseServiceManager::copyShardsWithLock: %v for taskId: %v",
					err, taskId)
				return nil, err
			}

			return resp.GetShardPaths(), nil
		}
	}
}

func (psm *PauseServiceManager) downloadShardsWithoutLock(
	shardPaths map[common.ShardId]string,
	taskId, bucket, origin, region string,
	cancelCh <-chan struct{}, progressUpdate func(incr float64),
) (map[common.ShardId]string, error) {

	logging.Infof("PauseServiceManager::downloadShardsWithoutLock: downloading shards %v for taskId %v",
		shardPaths, taskId)

	respCh := make(chan Message)
	progressCh := make(chan *ShardTransferStatistics, 1000)
	lastReportedProgress := make(map[common.ShardId]int64)

	msg := &MsgStartShardRestore{
		taskType:    common.PauseResumeTask,
		taskId:      taskId,
		transferId:  bucket,
		shardPaths:  shardPaths,
		destination: origin,
		region:      region,
		cancelCh:    cancelCh,
		doneCh:      cancelCh,
		progressCh:  progressCh,
		respCh:      respCh,
	}

	psm.supvMsgch <- msg

	for {
		select {
		case stats := <-progressCh:
			if stats.totalBytes == 0 {
				continue
			}
			currProg := (stats.bytesWritten * 100) / stats.totalBytes
			progressUpdate(float64(currProg-lastReportedProgress[stats.shardId]) / 100.0)
			lastReportedProgress[stats.shardId] = currProg
		case respMsg := <-respCh:
			resp, ok := respMsg.(*MsgShardTransferResp)

			if !ok || resp == nil {
				err := fmt.Errorf("either response channel got closed or sent an invalid response")
				logging.Errorf("PauseServiceManager::downloadShardsWithLock: %v for taskId %v", err, taskId)
				return nil, err
			}
			errMap := resp.GetErrorMap()
			errBuilder := new(strings.Builder)
			for shardId, errInCopy := range errMap {
				if errInCopy != nil {
					errBuilder.WriteString(fmt.Sprintf("Failed to download shard %v, err: %v", shardId, errInCopy))
				}
			}
			if errBuilder.Len() != 0 {
				err := errors.New(errBuilder.String())
				logging.Errorf("PauseServiceManager::downloadShardsWithLock: %v for taskId %v",
					err, taskId)
				return nil, err
			}
			return resp.GetShardPaths(), nil
		}
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Constants
////////////////////////////////////////////////////////////////////////////////////////////////////

// ARCHIVE_VERSION is the version of the archive format used for Pause and Resume. Later versions
// are expected to understand earlier versions but not vice versa. This version needs to be
// increased when a breaking change is made to anything that gets persisted in the archive.
//
// For future-proofing, the initial version is 100, and we expect to increment by 100. This allows
// us to easily insert new versions in the middle of the history, which could happen e.g. if a prior
// release branch diverges from the main branch due to a bug fix.
const ARCHIVE_VERSION int = 100 // increment by +100 -- see above

// FILENAME_METADATA is the name of the file to write containing the index metadata from ONE node.
const FILENAME_METADATA = "indexMetadata.json"

// FILENAME_STATS is the name of the file to write containing persisted index stats from ONE node.
const FILENAME_STATS = "indexStats.json"

// FILENAME_PAUSE_METADATA is the name of the file to write containing the PauseMetadata.
const FILENAME_PAUSE_METADATA = "pauseMetadata.json"

////////////////////////////////////////////////////////////////////////////////////////////////////
// Enums
////////////////////////////////////////////////////////////////////////////////////////////////////

// type bucketStateEnum defines the possible Pause-Resume states of a bucket (following constants).
// bucketStates map holds these. If a bucket is not in the map, it is not pausing or resuming. This
// is equivalent to state bst_NIL.
type bucketStateEnum int

const (
	// bst_NIL is the zero-value state, indicating a bucket has no Pause-Resume state.
	bst_NIL bucketStateEnum = iota

	// bst_PREPARE_PAUSE is entered when ns_server calls PreparePause. Nothing is happening yet,
	// but the state must be tracked to verify legality of state transitions.
	bst_PREPARE_PAUSE

	// bst_PAUSING state is entered when ns_server calls Pause (after PreparePause already
	// succeeded). It means Pause actions such as rejecting scans and closing DCP streams for the
	// bucket have begun, and thus canceling the Pause requires reviving these. Pause-related tasks
	// still exist.
	bst_PAUSING

	// bst_PAUSED state is entered once all Pause work is completed and Pause-related tasks have
	// been removed, but ns_server has not yet dropped the bucket. During this period ns_server may
	// decide to "cancel" the Pause, but since the tasks are gone this can't happen via CancelTask.
	// Instead we must monitor the /pools/default/buckets API for buckets in this state and revive
	// such a bucket if it gets marked as active again. This revival is not a Resume from S3 but
	// rather the same as reviving from a CancelTask during bst_PAUSING state, i.e. just reopen DCP
	// connections and stop blocking scans. Pause-related tasks do NOT exist. This state ends when
	// either the bucket is dropped or marked active.
	bst_PAUSED

	// bst_PREPARE_RESUME is entered when ns_server calls PrepareResume. Nothing is happening yet,
	// but the state must be tracked to verify legality of state transitions.
	bst_PREPARE_RESUME

	// bst_RESUMING state is entered when ns_server calls Resume (after PrepareResume already
	// succeeded). Resume work is ongoing. Resume-related tasks still exist.
	bst_RESUMING

	// bst_RESUMED state is entered once all Resume work is completed and Resume-related tasks have
	// been removed, but ns_server has not yet marked the bucket active. During this period
	// ns_server may decide to "cancel" the Resume, but since the tasks are gone this can't happen
	// via CancelTask. Instead we must monitor the /pools/default/buckets API for buckets in this
	// state and only open such a bucket for business if it gets marked as active. Resume-related
	// tasks do NOT exist. This state ends when either the bucket is marked active or is dropped.
	bst_RESUMED

	bst_ONLINE
)

func (this bucketStateEnum) IsPausing() bool {

	if this == bst_PREPARE_PAUSE ||
		this == bst_PAUSING ||
		this == bst_PAUSED {
		return true
	} else {
		return false
	}

}

func (this bucketStateEnum) IsResuming() bool {

	if this == bst_PREPARE_RESUME ||
		this == bst_RESUMING ||
		this == bst_RESUMED {
		return true
	} else {
		return false
	}

}

func (this bucketStateEnum) IsHibernating() bool {

	if this.IsPausing() || this.IsResuming() {
		return true
	} else {
		return false
	}

}

// String converter for bucketStateEnum type.
func (this bucketStateEnum) String() string {
	switch this {
	case bst_NIL:
		return "NilBucketState"
	case bst_PREPARE_PAUSE:
		return "PreparePause"
	case bst_PAUSING:
		return "Pausing"
	case bst_PAUSED:
		return "Paused"
	case bst_PREPARE_RESUME:
		return "PrepareResume"
	case bst_RESUMING:
		return "Resuming"
	case bst_RESUMED:
		return "Resumed"
	case bst_ONLINE:
		return "Online"
	default:
		return fmt.Sprintf("undefinedBucketStateEnum_%v", int(this))
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Type definitions
////////////////////////////////////////////////////////////////////////////////////////////////////

// this information should be considered read only during resume
// created by pause leader node
type PauseMetadata struct {
	// nodeId gives node info
	// map[nodeId]->shardIds gives information about shardIds copied from node
	// map[shardId] -> string are obj store paths where each shard is saved
	Data map[service.NodeID]map[common.ShardId]string `json:"metadata"`

	// cluster Version during data creation
	Version string `json:"version"`

	lock *sync.RWMutex
}

func NewPauseMetadata() *PauseMetadata {
	return &PauseMetadata{
		// size value should be max nodes in a subcluster
		Data: make(map[service.NodeID]map[common.ShardId]string, 2),
		lock: &sync.RWMutex{},
	}
}

func (pm *PauseMetadata) setVersionNoLock(ver string) {
	pm.Version = ver
}

func (pm *PauseMetadata) addShardPathNoLock(nodeId service.NodeID, shardId common.ShardId,
	shardPath string) {
	_, ok := pm.Data[nodeId]
	if !ok {
		// size value should be no of shards per bucket
		pm.Data[nodeId] = make(map[common.ShardId]string, 2)
	}
	pm.Data[nodeId][shardId] = shardPath
}

func (pm *PauseMetadata) addShardPaths(nodeId service.NodeID, shardPaths map[common.ShardId]string) {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	pm.Data[nodeId] = shardPaths
}

// taskObj represents one task of any type in the taskType enum below. This is the GSI internal
// representation, not the GetTaskList return format.
//
// For GetTaskList response to ns_server, we convert a taskObj to a service.Task struct (shared
// with Rebalance).
type taskObj struct {
	rev    uint64        // rev - revision of the task
	taskMu *sync.RWMutex // protects this taskObj; pointer as taskObj may be cloned

	taskType        service.TaskType
	taskId          string             // opaque ns_server unique ID for this task
	taskStatus      service.TaskStatus // local analog of service.TaskStatus
	progress        float64            // completion progress [0.0, 1.0]
	perNodeProgress map[string]float64 // map of service.nodeID to progress
	errorMessage    string             // only if a failure occurred

	bucket string // bucket name being Paused or Resumed
	dryRun bool   // is task for Resume dry run (cluster capacity check)?
	region string // region of the object store bucket
	// ns_server does not differentiate between PreparePause and PrepareResume
	// this flag is to internally track if the request if for PreparePause/PrepareResume
	// when the taskType = service.TaskTypePrepared
	isPause bool

	// archivePath is path to top-level "directory" to use to write/read Pause/Resume images. For:
	//   S3 format:       "s3://<s3_bucket>/index/"
	//   Local FS format: "/absolute/path/" or "relative/path/"
	// The trailing slash is always present. For FS the original "file://" prefix has been removed.
	archivePath string
	archiveType ArchiveEnum // type of storage archive used for this task

	// master gets set to true on the master node for this task once ns_server calls Pause or
	// Resume, which it only does on the master node. At PreparePause and PrepareResume time we do
	// not yet know what node will be master.
	master bool

	// pauser is the async object executing this task iff it is a task_PAUSE, else nil
	pauser *Pauser

	// resumer is the async object executing this task iff it is a task_RESUME, else nil
	resumer *Resumer

	// ctx holds the context object that can be used for task context
	ctx context.Context
	// other packages are not supposed to call this. use Cancel/cancelNoLock
	// cancelFunc should be called only to cancel any ongoing observers and uploads
	// it is does not modify the task status or any other task related operations
	cancelFunc context.CancelFunc

	// PauseMetadata stores the metadata about the pause
	// this is only created and saved by the master
	pauseMetadata *PauseMetadata
}

// NewTaskObj is the constructor for the taskObj class. If the parameters are not valid, it will
// return (nil, error) rather than create an unsupported taskObj.
func NewTaskObj(taskType service.TaskType, taskId, bucket, region, remotePath string,
	isPause, dryRun bool) (*taskObj, error) {

	archiveType, archivePath, err := ArchiveInfoFromRemotePath(remotePath)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &taskObj{
		rev:             0,
		taskMu:          &sync.RWMutex{},
		taskId:          taskId,
		taskType:        taskType,
		taskStatus:      service.TaskStatusRunning,
		isPause:         isPause,
		bucket:          bucket,
		region:          region,
		dryRun:          dryRun,
		archivePath:     archivePath,
		archiveType:     archiveType,
		ctx:             ctx,
		cancelFunc:      cancel,
		perNodeProgress: make(map[string]float64),
	}, nil
}

func (t *taskObj) GetTaskRev() uint64 {
	t.taskMu.RLock()
	rev := t.rev
	t.taskMu.RUnlock()
	return rev
}

func (t *taskObj) incRevNoLock() {
	t.rev++
}

func (t *taskObj) IncRev() {
	t.taskMu.Lock()
	defer t.taskMu.Unlock()
	t.incRevNoLock()
}

// TaskObjSetFailed sets task.status to service.TaskStatusFailed and task.errMessage to errMsg.
// this applies lock and internally calls TaskObjSetFailedNoLock
func (this *taskObj) TaskObjSetFailed(errMsg string, status service.TaskStatus) {
	this.taskMu.Lock()
	this.TaskObjSetFailedNoLock(errMsg, status)
	this.taskMu.Unlock()
}

// TaskObjSetFailedNoLock sets the status to service.TaskStatusFailed and errorMessage to errMsg
// callers should hold the lock
func (t *taskObj) TaskObjSetFailedNoLock(errMsg string, status service.TaskStatus) {
	t.errorMessage = errMsg
	t.taskStatus = status
	t.incRevNoLock()
}

func (t *taskObj) AddTaskProgress(incr float64) {
	t.taskMu.Lock()
	defer t.taskMu.Unlock()
	t.progress += incr
}

func (t *taskObj) UpdateNodeProgress(nodeId service.NodeID, newProgress float64) {
	t.taskMu.Lock()
	defer t.taskMu.Unlock()
	t.perNodeProgress[string(nodeId)] = newProgress
}

func (t *taskObj) updateTaskProgressNoLock(progress float64, perNodeProgress map[string]float64) {
	didUpdate := false
	if progress > t.progress {
		t.progress = progress
		didUpdate = true
	}
	for nodeId, nodeProgress := range perNodeProgress {
		if nodeProgress > t.perNodeProgress[nodeId] {
			didUpdate = true
			t.perNodeProgress[nodeId] = nodeProgress
		}
	}
	if didUpdate {
		t.incRevNoLock()
	}
}

func (t *taskObj) UpdateTaskProgress(progress float64, perNodeProgress map[string]float64) {
	t.taskMu.Lock()
	defer t.taskMu.Unlock()
	t.updateTaskProgressNoLock(progress, perNodeProgress)
}

func (this *taskObj) setMasterNoLock() {
	this.master = true
	this.pauseMetadata = NewPauseMetadata()
}

func (this *taskObj) updateTaskTypeNoLock(newTaskType service.TaskType) {
	this.taskType = newTaskType
}

// taskObjToServiceTask creates the ns_server service.Task (cbauth/service/interface.go)
// representation of a taskObj.
func (this *taskObj) taskObjToServiceTask() []service.Task {
	this.taskMu.RLock()
	defer this.taskMu.RUnlock()

	tasks := make([]service.Task, 0, 2)

	nsTask := service.Task{
		Rev:          EncodeRev(this.rev),
		ID:           this.taskId,
		Type:         this.taskType,
		Status:       this.taskStatus,
		IsCancelable: true,
		Progress:     this.progress,
		ErrorMessage: this.errorMessage,
		Extra:        make(map[string]interface{}),
	}

	// Add task parameters to service.Extra map for supportability (ns_server will ignore these)
	nsTask.Extra["bucket"] = this.bucket
	if this.hasDryRun() {
		nsTask.Extra["dryRun"] = this.dryRun
	}
	nsTask.Extra["archivePath"] = this.archivePath
	nsTask.Extra["archiveType"] = this.archiveType.String()
	nsTask.Extra["master"] = this.master

	if this.master {
		prepTask := service.Task{
			Rev:          EncodeRev(0),
			ID:           nsTask.ID,
			Type:         service.TaskTypePrepared,
			Status:       nsTask.Status,
			IsCancelable: true,
			Progress:     100,
			ErrorMessage: nsTask.ErrorMessage,
			Extra:        make(map[string]interface{}, len(nsTask.Extra)),
		}
		for key, value := range nsTask.Extra {
			prepTask.Extra[key] = value
		}
		tasks = append(tasks, prepTask)
	}
	tasks = append(tasks, nsTask)

	return tasks
}

// cancelNoLock stops any ongoing work by the task
// caller should hold lock. `Cancel` calls this function internally
func (this *taskObj) cancelNoLock() {
	if this.ctx == nil || this.cancelFunc == nil {
		logging.Warnf("taskObj::cancelNoLock: cancel called on already cancelled task %v", this.taskId)
		return
	}
	this.cancelFunc()
	this.ctx = nil
	this.cancelFunc = nil
}

// Cancel stops any ongoing work by the task
func (this *taskObj) Cancel() {
	this.taskMu.Lock()
	defer this.taskMu.Unlock()
	this.cancelNoLock()
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//
//  Implementation of Pause-Resume-specific APIs of the service.Manager interface
//  defined in cbauth/service/interface.go.
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// PreparePause is an external API called by ns_server (via cbauth) on all index nodes before
// calling Pause on leader only.
//
// Phase 1 (dry run) does not exist for Pause.
//
// Phase 2. ns_server will poll progress on leader only via GetTaskList.
//
//  3. All: PreparePause - Create PreparePause tasks on all nodes.
//
//  4. Leader: Pause - Create Pause task on leader. Orchestrate Index pause.
//
//     On Pause success:
//     o Leader: Remove Pause and PreparePause tasks from ITSELF ONLY.
//     o Followers: ns_server will call CancelTask(PreparePause) on all followers, which then
//     remove their own PreparePause tasks.
//
//     On Pause failure:
//     o Leader: Change its own service.Task.Status to TaskStatusFailed and add failure info to
//     service.Task.ErrorMessage.
//     o All: ns_server will call CancelTask for Pause on leader and PreparePause on all nodes
//     and abort the Pause. Index nodes should remove their respective tasks.
//
// Method arguments
// params service.PauseParams - cbauth object providing
//   - ID: task ID
//   - Bucket: name of the bucket to be paused
//   - RemotePath: object store path
func (m *PauseServiceManager) PreparePause(params service.PauseParams) (err error) {
	const _PreparePause = "PauseServiceManager::PreparePause:"

	const args = "taskId: %v, bucket: %v, remotePath: %v"
	logging.Infof("%v Called. "+args, _PreparePause, params.ID, params.Bucket, params.RemotePath)
	defer logging.Infof("%v Returned %v. "+args, _PreparePause, err, params.ID, params.Bucket, params.RemotePath)

	// Check if calling prepare before cancelling/cleaning up previous attempt)
	if bucketTasks := m.taskFindForBucket(params.Bucket); len(bucketTasks) > 0 {
		logging.Errorf("PauseServiceManager::PreparePause: Previous attempt not cleaned up:"+
			" err[%v] found tasks[%v] for bucket[%v]", service.ErrConflict, bucketTasks, params.Bucket)
		return service.ErrConflict
	}

	// Fail Prepare if post bootstrap cleanup is still pending
	if m.isCleanupPending() {
		err := fmt.Errorf("cleanup pending from previous failed/aborted pause/resume")
		logging.Errorf("PauseServiceManager::PreparePause: err[%v]", err)
		return err
	}

	// If master pauseToken is present, pause is still running, a pause must not be attempted by caller.
	if opts, exists := m.findPauseTokensForBucket(params.Bucket); exists {
		// Cleanup anyway and continue.

		logging.Warnf("PauseServiceManager::PreparePause: master pause token already present for"+
			" bucket[%v] old pts[%v] during prepare pause. Attempting cleanup", params.Bucket, opts)

		for _, opt := range opts {
			if err := m.runPauseCleanupPhase(
				params.Bucket, opt.PauseId, opt.MasterId == string(m.nodeInfo.NodeID),
			); err != nil {
				return err
			}
		}
	}

	// Check if PauseResumeRunning is set
	if running := m.pauseResumeRunningById.IsRunning(params.Bucket, params.ID); len(running) > 0 {
		// Cleanup anyway and continue.

		logging.Warnf("PauseServiceManager::PreparePause: pauseResumeRunning set for"+
			" bucket[%v] id[%v] during prepare pause. Attempting cleanup", params.Bucket, params.ID)

		for id, rMeta := range running {
			if err := m.runPauseCleanupPhase(rMeta.BucketName, id, false); err != nil {
				return err
			}
		}
	}

	// There maybe some orphaned tokens, for example, due to failover, cleanup and continue.
	if idsToClean, err := m.checkLocalPauseCleanupPending(params.Bucket); err != nil {
		return err
	} else if len(idsToClean) > 0 {
		logging.Warnf("PauseServiceManager::PreparePause: Found tokens for local cleanup, running cleanup:"+
			" idsToClean[%v]", idsToClean)

		for pauseId, _ := range idsToClean {
			if err := m.runPauseCleanupPhase(params.Bucket, pauseId, false); err != nil {
				return err
			}
		}

		if idsToClean, err = m.checkLocalPauseCleanupPending(params.Bucket); err != nil {
			return err
		} else if len(idsToClean) > 0 {
			err = fmt.Errorf("found tokens for local cleanup, after cleanup: idsToClean[%v]", idsToClean)
			logging.Errorf("PauseServiceManager::PreparePause: Failed local cleanup: err[%v]", err)
			return err
		}
	}

	// Check for initial or catchup state
	if ddlRunning, inProgressIndexName := m.checkDDLRunningForBucket(params.Bucket); ddlRunning {
		err = fmt.Errorf("DDL is running for indexes [%v]", inProgressIndexName)
		logging.Errorf("PauseServiceManager::PreparePause: Found indexes with DDL in progress: err[%v]", err)
		return err
	}

	// Check for DDL command tokens
	if inProg, inProgDefns, err := mc.CheckInProgressCommandTokensForBucket(params.Bucket); err != nil {
		return err
	} else if inProg {
		err = fmt.Errorf("found in progress DDL command tokens for bucket[%v] defns[%v]",
			params.Bucket, inProgDefns)
		logging.Errorf("PauseServiceManager::PreparePause: err[%v]", err)

		return err
	}

	// Caller is not expected to start Pause if rebalance is running
	// This check also covers replica repair
	if rebalanceRunning, err := m.checkRebalanceRunning(); err != nil {
		return err
	} else if rebalanceRunning {
		err = fmt.Errorf("found in-progress rebalance")
		logging.Errorf("PauseServiceManager::PreparePause: err[%v]", err)

		return err
	}

	// Make sure all indexes are caught up
	if caughtUp, indexes := m.checkIndexesCaughtUp(params.Bucket); !caughtUp {
		err = fmt.Errorf("found indexes[%v]len[%d] with non-zero pending or queued mutations", indexes, len(indexes))
		logging.Errorf("PauseServiceManager::PreparePause: err[%v]", err)

		return err
	}

	// TODO: Check remotePath access?

	// Set PauseResumeRunning flag
	if err := m.initPreparePhasePauseResume(PauseTokenPause, params.Bucket, params.ID); err != nil {
		logging.Errorf("PauseServiceManager::PreparePause: Failed to init prepare phase: err[%v]", err)
		return err
	}

	// Set bst_PREPARE_PAUSE state
	err = m.bucketStateSet(_PreparePause, params.Bucket, bst_NIL, bst_PREPARE_PAUSE)
	if err != nil {
		err = m.bucketStateSet(_PreparePause, params.Bucket, bst_ONLINE, bst_PREPARE_PAUSE)
		return err
	}

	//update supv about bucket pause state
	m.supvMsgch <- &MsgPauseUpdateBucketState{
		bucket:           params.Bucket,
		bucketPauseState: bst_PREPARE_PAUSE,
	}

	// Record the task in progress
	return m.taskAddPrepare(params.ID, params.Bucket, params.BlobStorageRegion, params.RemotePath,
		true, false)
}

// Pause is an external API called by ns_server (via cbauth) only on the GSI master node to initiate
// a pause (formerly hibernate) of a bucket in the serverless offering.
//
//		params service.PauseParams - cbauth object providing
//	  - ID: task ID
//	  - Bucket: name of the bucket to be paused
//	  - RemotePath: object store path
func (m *PauseServiceManager) Pause(params service.PauseParams) (err error) {
	const _Pause = "PauseServiceManager::Pause:"

	const args = "taskId: %v, bucket: %v, remotePath: %v"
	logging.Infof("%v Called. "+args, _Pause, params.ID, params.Bucket, params.RemotePath)
	defer logging.Infof("%v Returned %v. "+args, _Pause, err, params.ID, params.Bucket, params.RemotePath)

	// Update the task to set this node as master
	task := m.taskSetMasterAndUpdateType(params.ID, service.TaskTypeBucketPause)
	if task == nil || !task.isPause {
		logging.Errorf("%v taskId %v (from PreparePause) not found", _Pause, params.ID)
		return service.ErrNotFound
	}

	// Set bst_PAUSING state
	err = m.bucketStateSet(_Pause, params.Bucket, bst_PREPARE_PAUSE, bst_PAUSING)
	if err != nil {
		return err
	}

	if err = m.initStartPhase(params.Bucket, params.ID, task.archivePath, task.region, PauseTokenPause); err != nil {
		if cerr := m.runPauseCleanupPhase(params.Bucket, params.ID, task.isMaster()); cerr != nil {
			logging.Errorf("PauseServiceManager::Pause: Encountered cerr[%v] during cleanup for err[%v]", cerr, err)
			return cerr
		}

		return err
	}

	// Create a Pauser object to run the master orchestration loop.

	pt, exists := m.getPauseToken(params.ID)
	if !exists {
		err := fmt.Errorf("master pause token not found for id[%v] during pause", params.ID)

		if cerr := m.runPauseCleanupPhase(params.Bucket, params.ID, task.isMaster()); cerr != nil {
			logging.Errorf("PauseServiceManager::Pause: Encountered cerr[%v] during cleanup for err[%v]", cerr, err)
			return cerr
		}

		return err
	}

	pauser := NewPauser(m, task, pt, m.pauseDoneCallback,
		m.updateProgressCallback)

	if err = m.setPauser(params.ID, pauser); err != nil {
		if cerr := m.runPauseCleanupPhase(params.Bucket, params.ID, task.isMaster()); cerr != nil {
			logging.Errorf("PauseServiceManager::Pause: Encountered cerr[%v] during cleanup for err[%v]", cerr, err)
			return cerr
		}

		return err
	}

	pauser.startWorkers()

	return nil
}

func (m *PauseServiceManager) initStartPhase(bucketName, pauseId, archivePath, region string,
	typ PauseTokenType) (err error) {

	err = m.genericMgr.cinfo.FetchNodesAndSvsInfoWithLock()
	if err != nil {
		logging.Errorf("PauseServiceManager::initStartPhase: Failed to fetch nodes data via cinfo: err[%v]",
			err)
		return err
	}

	var masterIP string
	masterIP, err = func() (string, error) {
		m.genericMgr.cinfo.RLock()
		defer m.genericMgr.cinfo.RUnlock()
		return m.genericMgr.cinfo.GetLocalHostname()
	}()
	if err != nil {
		logging.Errorf("PauseServiceManager::initStartPhase: Failed to get local host name via cinfo: err[%v]",
			err)
		return err
	}

	pauseToken := m.genPauseToken(masterIP, bucketName, pauseId, archivePath, region, typ)
	logging.Infof("PauseServiceManager::initStartPhase Generated PauseToken[%v]", pauseToken)

	if err = m.setPauseToken(pauseId, pauseToken); err != nil {
		return err
	}

	// Add to local metadata
	if err = m.registerLocalPauseToken(pauseToken); err != nil {
		return err
	}

	// Add to metaKV
	if err = m.registerPauseTokenInMetakv(pauseToken); err != nil {
		return err
	}

	// Register via /pauseMgr/Pause
	if err = m.registerGlobalPauseToken(pauseToken); err != nil {
		return err
	}

	return nil
}

// pauseDoneCallback is the Pauser.cb.done callback function.
// Upload work is interrupted based on pauseId, using cancel ctx from task in pauser.
func (m *PauseServiceManager) pauseDoneCallback(pauseId string, err error) {

	pauser, exists := m.getPauser(pauseId)
	if !exists {
		logging.Errorf("PauseServiceManager::pauseDoneCallback: Failed to find Pauser with pauseId[%v]", pauseId)
		return
	}

	// If there is an error, set it in the task, otherwise, delete task from task list.
	isMaster := pauser.task.isMaster()

	if isMaster {
		m.endTask(err, pauseId)
	}

	go m.monitorBucketForPauseResume(pauser.task.bucket, pauser.task.taskId, isMaster, true, pauser.shardIds)

	if err := m.runPauseCleanupPhase(pauser.task.bucket, pauseId, isMaster); err != nil {
		logging.Errorf("PauseServiceManager::pauseDoneCallback: Failed to run cleanup: err[%v]", err)
		return
	}

	if err := m.setPauser(pauseId, nil); err != nil {
		logging.Errorf("PauseServiceManager::pauseDoneCallback: Failed to run cleanup: err[%v]", err)
		return
	}

	logging.Infof("PauseServiceManager::pauseDoneCallback Pause Done: isMaster %v, err: %v",
		isMaster, err)
}

func (m *PauseServiceManager) runPauseCleanupPhase(bucket, pauseId string, isMaster bool) error {

	logging.Infof("PauseServiceManager::runPauseCleanupPhase: pauseId[%v] isMaster[%v]", pauseId, isMaster)

	m.bucketStateDelete(bucket)

	if isMaster {
		if err := m.cleanupPauseTokenInMetakv(pauseId); err != nil {
			logging.Errorf("PauseServiceManager::runPauseCleanupPhase: Failed to cleanup PauseToken in metkv:"+
				" err[%v]", err)
			return err
		}
	}

	ptFilter, putFilter := getPauseTokenFiltersByPauseId(pauseId)
	_, puts, err := m.getCurrPauseTokens(ptFilter, putFilter)
	if err != nil {
		logging.Errorf("PauseServiceManager::runPauseCleanupPhase Error Fetching Metakv Tokens: err[%v]", err)
		return err
	}

	if len(puts) != 0 {
		if err := m.cleanupPauseUploadTokens(puts); err != nil {
			logging.Errorf("PauseServiceManager::runPauseCleanupPhase Error Cleaning Tokens: err[%v]", err)
			return err
		}
	}

	if err := m.cleanupLocalPauseToken(pauseId); err != nil {
		logging.Errorf("PauseServiceManager::runPauseCleanupPhase: Failed to cleanup PauseToken in local"+
			" meta: err[%v]", err)
		return err
	}

	if err := m.cleanupPauseResumeRunning(pauseId); err != nil {
		logging.Errorf("PauseServiceManager::runPauseCleanupPhase: Failed to cleanup PauseResumeRunning in local"+
			" meta: err[%v]", err)
		return err
	}

	return nil
}

type putFilterFn func(*common.PauseUploadToken) bool

func getPauseTokenFiltersByPauseId(pauseId string) (ptFilterFn, putFilterFn) {
	return func(pt *PauseToken) bool {
			return pt.PauseId == pauseId
		}, func(put *common.PauseUploadToken) bool {
			return put.PauseId == pauseId
		}
}

func getPauseTokenFiltersByBucket(bucketName string) (ptFilterFn, putFilterFn) {
	return func(pt *PauseToken) bool {
			return pt.BucketName == bucketName
		}, func(put *common.PauseUploadToken) bool {
			return put.BucketName == bucketName
		}
}

func (m *PauseServiceManager) getCurrPauseTokens(ptFilter ptFilterFn, putFilter putFilterFn) (pt *PauseToken, puts map[string]*common.PauseUploadToken,
	err error) {

	metaInfo, err := metakv.ListAllChildren(PauseMetakvDir)
	if err != nil {
		return nil, nil, err
	}

	if len(metaInfo) == 0 {
		return nil, nil, nil
	}

	puts = make(map[string]*common.PauseUploadToken)

	for _, kv := range metaInfo {

		if strings.Contains(kv.Path, PauseTokenTag) {
			var mpt PauseToken
			if err = json.Unmarshal(kv.Value, &mpt); err != nil {
				return nil, nil, err
			}

			if ptFilter(&mpt) && mpt.Type == PauseTokenPause {
				if pt != nil {
					return nil, nil, fmt.Errorf("encountered duplicate PauseToken for pauseId[%v]"+
						" pt[%v] mpt[%v]", mpt.PauseId, pt, mpt)
				}

				pt = &mpt
			}

		} else if strings.Contains(kv.Path, common.PauseUploadTokenTag) {
			putId, put, err := decodePauseUploadToken(kv.Path, kv.Value)
			if err != nil {
				return nil, nil, err
			}

			if putFilter(put) {
				if oldPUT, ok := puts[putId]; ok {
					return nil, nil, fmt.Errorf("encountered duplicate PauseUploadToken for"+
						" pauseId[%v] oldPUT[%v] PUT[%v] putId[%v]", put.PauseId, oldPUT, put, putId)
				}

				puts[putId] = put
			}

		} else {
			logging.Warnf("PauseServiceManager::getCurrPauseTokens Unknown Token %v. Ignored.", kv)

		}

	}

	return pt, puts, nil
}

func (m *PauseServiceManager) cleanupPauseUploadTokens(puts map[string]*common.PauseUploadToken) error {

	if puts == nil || len(puts) == 0 {
		logging.Infof("PauseServiceManager::cleanupPauseUploadTokens: No Tokens Found For Cleanup")
		return nil
	}

	for putId, put := range puts {
		logging.Infof("PauseServiceManager::cleanupPauseUploadTokens: Cleaning Up %v %v", putId, put)

		if put.MasterId == string(m.nodeInfo.NodeID) {
			if err := m.cleanupPauseUploadTokenForMaster(putId, put); err != nil {
				return err
			}
		}

		if put.FollowerId == string(m.nodeInfo.NodeID) {
			if err := m.cleanupPauseUploadTokenForFollower(putId, put); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *PauseServiceManager) cleanupPauseUploadTokenForMaster(putId string, put *common.PauseUploadToken) error {

	switch put.State {
	case common.PauseUploadTokenProcessed, common.PauseUploadTokenError:

		logging.Infof("PauseServiceManager::cleanupPauseUploadTokenForMaster: Cleanup Token %v %v", putId, put)

		if err := common.MetakvDel(PauseMetakvDir + putId); err != nil {
			logging.Errorf("PauseServiceManager::cleanupPauseUploadTokenForMaster: Unable to delete"+
				"PauseUploadToken[%v] In Meta Storage: err[%v]", put, err)
			return err
		}

	}

	return nil
}

func (m *PauseServiceManager) cleanupPauseUploadTokenForFollower(putId string, put *common.PauseUploadToken) error {

	switch put.State {

	case common.PauseUploadTokenPosted:
		// Followers just acknowledged the token, just delete the token from metakv.

		err := common.MetakvDel(PauseMetakvDir + putId)
		if err != nil {
			logging.Errorf("PauseServiceManager::cleanupPauseUploadTokenForFollower: Unable to delete[%v] in "+
				"Meta Storage: err[%v]", put, err)
			return err
		}

	case common.PauseUploadTokenInProgess:
		// Follower node might be uploading the data

		logging.Infof("PauseServiceManager::cleanupPauseUploadTokenForFollower: Initiating clean-up for"+
			" putId[%v], put[%v]", putId, put)

		pauser, exists := m.getPauser(put.PauseId)
		if !exists {
			err := fmt.Errorf("Pauser with pauseId[%v] not found", put.PauseId)
			logging.Errorf("PauseServiceManager::cleanupPauseUploadTokenForFollower: Failed to find pauser:"+
				" err[%v]", err)

			return err
		}

		// Cancel pause upload work using task ctx
		if doCancelUpload := pauser.task.cancelFunc; doCancelUpload != nil {
			doCancelUpload()
		} else {
			logging.Warnf("PauseServiceManager::cleanupPauseUploadTokenForFollower: Task already cancelled")
		}

		if err := common.MetakvDel(common.PauseMetakvDir + putId); err != nil {
			logging.Errorf("PauseServiceManager::cleanupPauseUploadTokenForFollower: Unable to delete"+
				" PauseUploadToken[%v] In Meta Storage: err[%v]", put, err)
			return err
		}

		logging.Infof("PauseServiceManager::cleanupPauseUploadTokenForFollower: Deleted putId[%v] from metakv",
			putId)

	}

	return nil
}

func (m *PauseServiceManager) checkLocalPauseCleanupPending(bucketName string) (map[string]bool, error) {

	ptFilter, putFilter := getPauseTokenFiltersByBucket(bucketName)
	_, puts, err := m.getCurrPauseTokens(ptFilter, putFilter)

	if err != nil {
		logging.Errorf("PauseServiceManager::checkLocalPauseCleanupPending: Failed to fetch from Metakv:"+
			" err[%v]", err)

		return nil, err
	}

	idsToClean := make(map[string]bool)
	for _, put := range puts {
		ownerId := m.getPauseUploadTokenOwner(put)
		if ownerId == string(m.nodeInfo.NodeID) {
			logging.Infof("PauseServiceManager::checkLocalPauseCleanupPending: Found local token"+
				" pending cleanup: put[%v]", put)
			idsToClean[put.PauseId] = true
		}
	}

	return idsToClean, nil
}

func (m *PauseServiceManager) getPauseUploadTokenOwner(put *common.PauseUploadToken) string {

	switch put.State {
	case common.PauseUploadTokenPosted, common.PauseUploadTokenInProgess:
		return put.FollowerId
	case common.PauseUploadTokenProcessed, common.PauseUploadTokenError:
		return put.MasterId
	}

	return ""
}

// PrepareResume is an external API called by ns_server (via cbauth) on all index nodes before
// calling Resume on leader only. The Resume call following PrepareResume will be given the SAME
// dryRun value. The full sequence is:
//
// Phase 1. ns_server will poll progress on leader only via GetTaskList.
//
//  1. All: PrepareResume(dryRun: true) - Create dry run PrepareResume tasks on all nodes.
//
//  2. Leader: Resume(dryRun: true) - Create dry run Resume task on leader, do dry run
//     computations (est # of addl nodes needed to fit on this cluster; may be 0)
//
//     If dry run determines Resume is possible:
//     o Leader: Remove Resume and PrepareResume tasks from ITSELF ONLY.
//     o Followers: ns_server will call CancelTask(PrepareResume) on all followers, which then
//     remove their own PrepareResume tasks.
//
//     If dry run determines Resume is not possible:
//     o Leader: Change its own service.Task.Status to TaskStatusCannotResume and optionally set
//     service.Task.Extra["additionalNodesNeeded"] = <int>
//     o All: ns_server will call CancelTask for Resume on leader and PrepareResume on all nodes
//     and abort the Resume. Index nodes should remove their respective tasks.
//
//     On dry run failure:
//     o Leader: Change its own service.Task.Status to TaskStatusFailed and add failure info to
//     service.Task.ErrorMessage.
//     o All: ns_server will call CancelTask for Resume on leader and PrepareResume on all nodes
//     and abort the Resume. Index nodes should remove their respective tasks.
//
// Phase 2. ns_server will poll progress on leader only via GetTaskList.
//
//  3. All: PrepareResume(dryRun: false) - Create real PrepareResume tasks on all nodes.
//
//  4. Leader: Resume(dryRun: false) - Create real Resume task on leader. Orchestrate Index resume.
//
//     On Resume success:
//     o Leader: Remove Resume and PrepareResume tasks from ITSELF ONLY.
//     o Followers: ns_server will call CancelTask(PrepareResume) on all followers, which then
//     remove their own PrepareResume tasks.
//
//     On Resume failure:
//     o Leader: Change its own service.Task.Status to TaskStatusFailed and add failure info to
//     service.Task.ErrorMessage.
//     o All: ns_server will call CancelTask for Resume on leader and PrepareResume on all nodes
//     and abort the Resume. Index nodes should remove their respective tasks.
//
// Method arguments
//
//		params service.ResumeParams - cbauth object providing
//	  - ID: task ID
//	  - Bucket: name of the bucket to be paused
//	  - RemotePath: object store path
//		- DryRun: if this is a dryRun of resume
func (m *PauseServiceManager) PrepareResume(params service.ResumeParams) (err error) {
	const _PrepareResume = "PauseServiceManager::PrepareResume:"

	const args = "taskId: %v, bucket: %v, remotePath: %v, dryRun: %v"
	logging.Infof("%v Called. "+args, _PrepareResume, params.ID, params.Bucket, params.RemotePath, params.DryRun)
	defer logging.Infof("%v Returned %v. "+args, _PrepareResume, err, params.ID, params.Bucket, params.RemotePath, params.DryRun)

	// Check if calling prepare before cancelling/cleaning up previous attempt
	if bucketTasks := m.taskFindForBucket(params.Bucket); len(bucketTasks) > 0 {
		logging.Errorf("PauseServiceManager::PrepareResume: Previous attempt not cleaned up:"+
			" err[%v] found tasks[%v] for bucket[%v]", service.ErrConflict, bucketTasks, params.Bucket)
		return service.ErrConflict
	}

	// Fail Prepare if post bootstrap cleanup is still pending
	if m.isCleanupPending() {
		err := fmt.Errorf("cleanup pending from previous failed/aborted pause/resume")
		logging.Errorf("PauseServiceManager::PrepareResume: err[%v]", err)
		return err
	}

	// If master pauseToken is present, pause is still running, a pause must not be attempted by caller.
	if opts, exists := m.findPauseTokensForBucket(params.Bucket); exists {
		// Cleanup anyway and continue.

		logging.Warnf("PauseServiceManager::PrepareResume: master pause token already present for"+
			" bucket[%v] old pts[%v] during prepare resume. Attempting cleanup", params.Bucket, opts)

		for _, opt := range opts {
			if err := m.runResumeCleanupPhase(
				params.Bucket, opt.PauseId, opt.MasterId == string(m.nodeInfo.NodeID),
			); err != nil {
				return err
			}
		}
	}

	// Check if PauseResumeRunning is set
	if running := m.pauseResumeRunningById.IsRunning(params.Bucket, params.ID); len(running) > 0 {
		// Cleanup anyway and continue.

		logging.Warnf("PauseServiceManager::PrepareResume: pauseResumeRunning set for"+
			" bucket[%v] id[%v] during prepare resume. Attempting cleanup", params.Bucket, params.ID)

		for id, rMeta := range running {
			if err := m.runResumeCleanupPhase(rMeta.BucketName, id, false); err != nil {
				return err
			}
		}
	}

	// There maybe some orphaned tokens, for example, due to failover, cleanup and continue.
	if idsToClean, err := m.checkLocalResumeCleanupPending(params.Bucket); err != nil {
		return err
	} else if len(idsToClean) > 0 {
		logging.Warnf("PauseServiceManager::PrepareResume: Found tokens for local cleanup, running cleanup:"+
			" idsToClean[%v]", idsToClean)

		for resumeId, _ := range idsToClean {
			if err := m.runResumeCleanupPhase(params.Bucket, resumeId, false); err != nil {
				return err
			}
		}

		if idsToClean, err = m.checkLocalResumeCleanupPending(params.Bucket); err != nil {
			return err
		} else if len(idsToClean) > 0 {
			err = fmt.Errorf("found tokens for local cleanup, after cleanup: idsToClean[%v]", idsToClean)
			logging.Errorf("PauseServiceManager::PrepareResume: Failed local cleanup: err[%v]", err)
			return err
		}
	}

	// Check for initial or catchup state
	if ddlRunning, inProgressIndexName := m.checkDDLRunningForBucket(params.Bucket); ddlRunning {
		err = fmt.Errorf("DDL is running for indexes [%v]", inProgressIndexName)
		logging.Errorf("PauseServiceManager::PrepareResume: Found indexes with DDL in progress: err[%v]", err)
		return err
	}

	// Check for DDL command tokens
	if inProg, inProgDefns, err := mc.CheckInProgressCommandTokensForBucket(params.Bucket); err != nil {
		return err
	} else if inProg {
		err = fmt.Errorf("found in progress DDL command tokens for bucket[%v] defns[%v]",
			params.Bucket, inProgDefns)
		logging.Errorf("PauseServiceManager::PrepareResume: err[%v]", err)

		return err
	}

	// Caller is not expected to start Resume if rebalance is running
	if rebalanceRunning, err := m.checkRebalanceRunning(); err != nil {
		return err
	} else if rebalanceRunning {
		err = fmt.Errorf("found in-progress rebalance")
		logging.Errorf("PauseServiceManager::PrepareResume: err[%v]", err)

		return err
	}

	// Indexes for this bucket do not exist yet, no need to check if they are caught up

	// TODO: Check remotePath access?

	if !params.DryRun {

		// Set PauseResumeRunning flag
		if err := m.initPreparePhasePauseResume(PauseTokenResume, params.Bucket, params.ID); err != nil {
			logging.Errorf("PauseServiceManager::PrepareResume: Failed to init prepare phase: err[%v]", err)
			return err
		}

		// Set bst_PREPARE_RESUME state
		err = m.bucketStateSet(_PrepareResume, params.Bucket, bst_NIL, bst_PREPARE_RESUME)
		if err != nil {
			m.bucketStateSet(_PrepareResume, params.Bucket, bst_ONLINE, bst_PREPARE_RESUME)
			return err
		}

		m.supvMsgch <- &MsgPauseUpdateBucketState{
			bucket:           params.Bucket,
			bucketPauseState: bst_PREPARE_RESUME,
		}

	}

	// Record the task in progress
	return m.taskAddPrepare(params.ID, params.Bucket, params.BlobStorageRegion, params.RemotePath,
		false, params.DryRun)
}

// Resume is an external API called by ns_server (via cbauth) only on the GSI master node to
// initiate a resume (formerly unhibernate or rehydrate) of a bucket in the serverless offering.
//
// params service.ResumeParams - cbauth object providing
//   - ID: task ID
//   - Bucket: name of the bucket to be paused
//   - RemotePath: object store path
//   - DryRun: if this is a dryRun of resume
func (m *PauseServiceManager) Resume(params service.ResumeParams) error {
	const _Resume = "PauseServiceManager::Resume:"

	const args = "taskId: %v, bucket: %v, remotePath: %v, dryRun: %v"
	logging.Infof("%v Called. "+args, _Resume, params.ID, params.Bucket, params.RemotePath, params.DryRun)
	// Update the task to set this node as master
	task := m.taskSetMasterAndUpdateType(params.ID, service.TaskTypeBucketResume)
	if task == nil || task.isPause {
		err := service.ErrNotFound
		logging.Errorf("%v taskId %v (from PrepareResume) not found", _Resume, params.ID)
		return err
	}

	// Don't update any state/register resume if this is a dryRun
	if !params.DryRun {

		// Set bst_RESUMING state
		err := m.bucketStateSet(_Resume, params.Bucket, bst_PREPARE_RESUME, bst_RESUMING)
		if err != nil {
			logging.Errorf("%v failed to set bucket state; err: %v for task ID: %v", _Resume,
				err, params.ID)
			return err
		}

		if err := m.initStartPhase(params.Bucket, params.ID, task.archivePath, task.region, PauseTokenResume); err != nil {
			logging.Errorf("%v couldn't start resume; err: %v for task ID: %v", _Resume, err, params.ID)
			if cerr := m.runResumeCleanupPhase(params.Bucket, params.ID, task.isMaster()); cerr != nil {
				logging.Errorf("PauseServiceManager::Resume: Encountered cerr[%v] during cleanup for err[%v]", cerr, err)
				return cerr
			}

			return err
		}

	}

	// Create a Resumer object to run the master orchestration loop.

	pt, exists := m.getPauseToken(params.ID)

	// For DryRun=true, initStartPhase is not run and pauseToken is not generated
	if !params.DryRun && !exists {
		err := fmt.Errorf("master pause token not found for id[%v] during resume", params.ID)

		if cerr := m.runResumeCleanupPhase(params.Bucket, params.ID, task.isMaster()); cerr != nil {
			logging.Errorf("PauseServiceManager::Resume: Encountered cerr[%v] during cleanup for err[%v]", cerr, err)
			return cerr
		}

		return err
	}

	resumer := NewResumer(m, task, pt, m.resumeDoneCallback,
		m.updateProgressCallback)

	if err := m.setResumer(params.ID, resumer); err != nil {
		logging.Errorf("%v couldn't set resume; err: %v for task ID: %v", _Resume, err, params.ID)
		if cerr := m.runResumeCleanupPhase(params.Bucket, params.ID, task.isMaster()); cerr != nil {
			logging.Errorf("PauseServiceManager::Resume: Encountered cerr[%v] during cleanup for err[%v]", cerr, err)
			return cerr
		}

		return err
	}

	resumer.startWorkers()

	logging.Infof("%v started resume with task ID %v", _Resume, params.ID)
	return nil
}

// resumeDoneCallback is the Resumer.doneCb callback function.
// Download work is interrupted based on resumeId, using cancel ctx from task in resumer.
func (m *PauseServiceManager) resumeDoneCallback(resumeId string, err error) {

	resumer, exists := m.getResumer(resumeId)
	if !exists {
		logging.Errorf("PauseServiceManager::resumeDoneCallback: Failed to find Resumer with resumeId[%v]", resumeId)
		return
	}

	// If there is an error, set it in the task, otherwise, delete task from task list.
	isMaster := resumer.task.isMaster()

	if isMaster {
		m.endTask(err, resumeId)
	}

	if err == nil {
		//resume successful, update state in supv to resumed
		m.supvMsgch <- &MsgPauseUpdateBucketState{
			bucket:           resumer.task.bucket,
			bucketPauseState: bst_RESUMED,
		}
		go m.monitorBucketForPauseResume(resumer.task.bucket, resumer.task.taskId, isMaster, false, resumer.shardIds)
	} else {
		//resume has failed, reset bucketPauseState
		m.supvMsgch <- &MsgPauseUpdateBucketState{
			bucket:           resumer.task.bucket,
			bucketPauseState: bst_NIL,
		}
	}

	if err := m.runResumeCleanupPhase(resumer.task.bucket, resumeId, isMaster); err != nil {
		logging.Errorf("PauseServiceManager::resumeDoneCallback: Failed to run cleanup: err[%v]", err)
		return
	}

	if err := m.setResumer(resumeId, nil); err != nil {
		logging.Errorf("PauseServiceManager::resumeDoneCallback: Failed to unset Resumer: err[%v]", err)
		return
	}

	logging.Infof("PauseServiceManager::resumeDoneCallback Resume Done: isMaster %v, err: %v",
		isMaster, err)
}

func (m *PauseServiceManager) runResumeCleanupPhase(bucket, resumeId string, isMaster bool) error {

	logging.Infof("PauseServiceManager::runResumeCleanupPhase: resumeId[%v] isMaster[%v]", resumeId, isMaster)

	m.bucketStateDelete(bucket)

	if isMaster {
		if err := m.cleanupPauseTokenInMetakv(resumeId); err != nil {
			logging.Errorf("PauseServiceManager::runResumeCleanupPhase: Failed to cleanup PauseToken in metkv:"+
				" err[%v]", err)
			return err
		}
	}

	ptFilter, rdtFilter := getResumeTokenFiltersByResumeId(resumeId)
	_, rdts, err := m.getCurrResumeTokens(ptFilter, rdtFilter)
	if err != nil {
		logging.Errorf("PauseServiceManager::runResumeCleanupPhase: Error Fetching Metakv Tokens: err[%v]", err)
		return err
	}

	if len(rdts) != 0 {
		if err := m.cleanupResumeDownloadTokens(rdts); err != nil {
			logging.Errorf("PauseServiceManager::runResumeCleanupPhase: Error Cleaning Tokens: err[%v]", err)
			return err
		}
	}

	if err := m.cleanupLocalPauseToken(resumeId); err != nil {
		logging.Errorf("PauseServiceManager::runResumeCleanupPhase: Failed to cleanup PauseToken in local"+
			" meta: err[%v]", err)
		return err
	}

	if err := m.cleanupPauseResumeRunning(resumeId); err != nil {
		logging.Errorf("PauseServiceManager::runResumeCleanupPhase: Failed to cleanup PauseResumeRunning in local"+
			" meta: err[%v]", err)
		return err
	}

	return nil
}

type rdtFilterFn func(*common.ResumeDownloadToken) bool

func getResumeTokenFiltersByResumeId(resumeId string) (ptFilterFn, rdtFilterFn) {
	return func(pt *PauseToken) bool {
			return pt.PauseId == resumeId
		}, func(rdt *common.ResumeDownloadToken) bool {
			return rdt.ResumeId == resumeId
		}
}

func getResumeTokenFiltersByBucket(bucketName string) (ptFilterFn, rdtFilterFn) {
	return func(pt *PauseToken) bool {
			return pt.BucketName == bucketName
		}, func(rdt *common.ResumeDownloadToken) bool {
			return rdt.BucketName == bucketName
		}
}

func (m *PauseServiceManager) getCurrResumeTokens(ptFilter ptFilterFn, rdtFilter rdtFilterFn) (pt *PauseToken,
	rdts map[string]*common.ResumeDownloadToken, err error) {

	metaInfo, err := metakv.ListAllChildren(PauseMetakvDir)
	if err != nil {
		return nil, nil, err
	}

	if len(metaInfo) == 0 {
		return nil, nil, nil
	}

	rdts = make(map[string]*common.ResumeDownloadToken)

	for _, kv := range metaInfo {

		if strings.Contains(kv.Path, PauseTokenTag) {
			var mpt PauseToken
			if err = json.Unmarshal(kv.Value, &mpt); err != nil {
				return nil, nil, err
			}

			if ptFilter(&mpt) && mpt.Type == PauseTokenResume {
				if pt != nil {
					return nil, nil, fmt.Errorf("encountered duplicate PauseToken for resumeId[%v]"+
						" pt[%v] mpt[%v]", mpt.PauseId, pt, mpt)
				}

				pt = &mpt
			}

		} else if strings.Contains(kv.Path, common.ResumeDownloadTokenTag) {
			rdtId, rdt, err := decodeResumeDownloadToken(kv.Path, kv.Value)
			if err != nil {
				return nil, nil, err
			}

			if rdtFilter(rdt) {
				if oldRDT, ok := rdts[rdtId]; ok {
					return nil, nil, fmt.Errorf("encountered duplicate ResumeDownloadToken for"+
						" resumeId[%v] oldRDT[%v] rdt[%v] rdtId[%v]", rdt.ResumeId, oldRDT, rdt, rdtId)
				}

				rdts[rdtId] = rdt
			}

		} else {
			logging.Warnf("PauseServiceManager::getCurrResumeTokens: Unknown Token %v. Ignored.", kv)

		}

	}

	return pt, rdts, nil
}

func (m *PauseServiceManager) cleanupResumeDownloadTokens(rdts map[string]*common.ResumeDownloadToken) error {

	if rdts == nil || len(rdts) <= 0 {
		logging.Infof("PauseServiceManager::cleanupResumeDownloadTokens: No Tokens Found For Cleanup")
		return nil
	}

	for rdtId, rdt := range rdts {
		logging.Infof("PauseServiceManager::cleanupResumeDownloadTokens: Cleaning Up %v %v", rdtId, rdt)

		if rdt.MasterId == string(m.nodeInfo.NodeID) {
			if err := m.cleanupResumeDownloadTokenForMaster(rdtId, rdt); err != nil {
				return err
			}
		}

		if rdt.FollowerId == string(m.nodeInfo.NodeID) {
			if err := m.cleanupResumeDownloadTokenForFollower(rdtId, rdt); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *PauseServiceManager) cleanupResumeDownloadTokenForMaster(rdtId string, rdt *common.ResumeDownloadToken) error {

	switch rdt.State {
	case common.ResumeDownloadTokenProcessed, common.ResumeDownloadTokenError:

		logging.Infof("PauseServiceManager::cleanupResumeDownloadTokenForMaster: Cleanup Token %v %v",
			rdtId, rdt)

		if err := common.MetakvDel(PauseMetakvDir + rdtId); err != nil {
			logging.Errorf("PauseServiceManager::cleanupResumeDownloadTokenForMaster: Unable to delete"+
				"ResumeDownloadToken[%v] In Meta Storage: err[%v]", rdt, err)
			return err
		}

	}

	return nil
}

func (m *PauseServiceManager) cleanupResumeDownloadTokenForFollower(rdtId string, rdt *common.ResumeDownloadToken) error {

	switch rdt.State {

	case common.ResumeDownloadTokenPosted:
		// Followers just acknowledged the token, just delete the token from metakv.

		err := common.MetakvDel(PauseMetakvDir + rdtId)
		if err != nil {
			logging.Errorf("PauseServiceManager::cleanupResumeDownloadTokenForFollower: Unable to delete[%v]"+
				" in Meta Storage: err[%v]", rdt, err)
			return err
		}

	case common.ResumeDownloadTokenInProgess:
		// Follower node might be uploading the data

		logging.Infof("PauseServiceManager::cleanupResumeDownloadTokenForFollower: Initiating cleanup for"+
			" rdtId[%v], rdt[%v]", rdtId, rdt)

		resumer, exists := m.getResumer(rdt.ResumeId)
		if !exists {
			err := fmt.Errorf("Resumer with resumeId[%v] not found", rdt.ResumeId)
			logging.Errorf("PauseServiceManager::cleanupResumeDownloadTokenForFollower: Failed to find"+
				" resumer: err[%v]", err)

			return err
		}

		// Cancel resume download work using task ctx
		if doCancelDownload := resumer.task.cancelFunc; doCancelDownload != nil {
			doCancelDownload()
			// TODO: Call new plasma API to cleanup local staging.
		} else {
			logging.Warnf("PauseServiceManager::cleanupResumeDownloadTokenForFollower: Task already cancelled")
		}

		if err := common.MetakvDel(PauseMetakvDir + rdtId); err != nil {
			logging.Errorf("PauseServiceManager::cleanupResumeDownloadTokenForFollower: Unable to delete"+
				" ResumeDownloadToken[%v] In Meta Storage: err[%v]", rdt, err)
			return err
		}

		logging.Infof("PauseServiceManager::cleanupResumeDownloadTokenForFollower: Deleted rdtId[%v] from"+
			" metakv", rdtId)

	}

	return nil
}

func (m *PauseServiceManager) checkLocalResumeCleanupPending(bucketName string) (map[string]bool, error) {

	ptFilter, rdtFilter := getResumeTokenFiltersByBucket(bucketName)
	_, rdts, err := m.getCurrResumeTokens(ptFilter, rdtFilter)
	if err != nil {
		logging.Errorf("PauseServiceManager::checkLocalResumeCleanupPending: Failed to fetch from Metakv:"+
			" err[%v]", err)

		return nil, err
	}

	idsToClean := make(map[string]bool)
	for _, rdt := range rdts {
		ownerId := m.getResumeDownloadTokenOwner(rdt)
		if ownerId == string(m.nodeInfo.NodeID) {
			logging.Infof("PauseServiceManager::checkLocalResumeCleanupPending: Found local token"+
				" pending cleanup: rdt[%v]", rdt)
			idsToClean[rdt.ResumeId] = true
		}
	}

	return idsToClean, nil
}

func (m *PauseServiceManager) getResumeDownloadTokenOwner(rdt *common.ResumeDownloadToken) string {

	switch rdt.State {
	case common.ResumeDownloadTokenPosted, common.ResumeDownloadTokenInProgess:
		return rdt.FollowerId
	case common.ResumeDownloadTokenProcessed, common.ResumeDownloadTokenError:
		return rdt.MasterId
	}

	return ""
}

// endTask is the endpoint of pause resume
func (m *PauseServiceManager) endTask(opErr error, taskId string) *taskObj {
	var task *taskObj

	logging.Infof("PauseServiceManager::endTask: called with err %v for taskId %v", opErr, taskId)
	if opErr != nil {
		// if caller has passed an error, we don't want to delete the task from task list
		// but the task could be nil
		task = m.taskFind(taskId)
	} else {
		task = m.taskDelete(taskId)
	}
	if task == nil {
		logging.Infof("PauseServiceManager::endTask task with ID %v already cleaned up", taskId)
		return nil
	}

	if opErr != nil {
		status := service.TaskStatusFailed
		errStr := opErr.Error()
		if !task.isPause && strings.Contains(errStr, "Not Enough Capacity To Place Tenant") ||
			strings.Contains(errStr, "No SubCluster Below Low Usage Threshold") {
			status = service.TaskStatusCannotResume
		}
		if !m.taskSetFailed(taskId, errStr, status) {
			logging.Errorf("PauseServiceManager::endTask: Failed to find task while setting failed status")
		}

		logging.Infof("PauseServiceManager::endTask: skipping task cleanup now for task ID: %v", taskId)
	}

	task.Cancel()

	logging.Infof("PauseServiceManager::endTask stopped task %v", task)

	return task
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Delegates for GenericServiceManager RPC APIs
////////////////////////////////////////////////////////////////////////////////////////////////////

// PauseResumeGetTaskList is a delegate of GenericServiceManager.PauseResumeGetTaskList which is an external API
// called by ns_server (via cbauth). It gets the Pause-Resume task list of the current node. Since
// service.Task is a struct, the returned tasks are copies of the originals.
func (m *PauseServiceManager) PauseResumeGetTaskList() (tasks []service.Task) {
	m.tasksMu.RLock()
	defer m.tasksMu.RUnlock()
	for _, taskObj := range m.tasks {
		tasks = append(tasks, taskObj.taskObjToServiceTask()...)
	}
	return tasks
}

// PauseResumeCancelTask is a delegate of GenericServiceManager.PauseResumeCancelTask which is an
// external API called by ns_server (via cbauth). It cancels a Pause-Resume task on the current node
func (m *PauseServiceManager) PauseResumeCancelTask(id string) error {
	task := m.endTask(nil, id)
	if task != nil {
		logging.Errorf("PauseServiceManager::PauseResumeCancelTask: couldn't find a task with ID %v", id)
		return service.ErrNotFound
	}
	logging.Infof("PauseServiceManager::PauseResumeCancelTask: removed all tasks with ID %v", id)
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// REST orchestration sender methods. These are all called by the master to instruct the followers.
////////////////////////////////////////////////////////////////////////////////////////////////////

// RestNotifyFailedTask calls REST API /pauseMgr/FailedTask to tell followers (otherIndexAddrs
// "host:port") that task has failed for the reason given in errMsg.
func (m *PauseServiceManager) RestNotifyFailedTask(otherIndexAddrs []string, task *taskObj,
	errMsg string) {
	const _RestNotifyFailedTask = "PauseServiceManager::RestNotifyFailedTask:"

	bodyStr := fmt.Sprintf("{\"errMsg\":%v}", errMsg)
	bodyBuf := bytes.NewBuffer([]byte(bodyStr))
	for _, indexAddr := range otherIndexAddrs {
		url := fmt.Sprintf("%v/pauseMgr/FailedTask?id=%v", indexAddr, task.taskId)
		go postWithAuthWrapper(_RestNotifyFailedTask, url, bodyBuf, task)
	}
}

// RestNotifyPause calls REST API /pauseMgr/Pause to tell followers (otherIndexAddrs "host:port")
// to initiate Pause work for task.
func (m *PauseServiceManager) RestNotifyPause(otherIndexAddrs []string, task *taskObj) {
	const _RestNotifyPause = "PauseServiceManager::RestNotifyPause:"

	for _, indexAddr := range otherIndexAddrs {
		url := fmt.Sprintf("%v/pauseMgr/Pause?id=%v", indexAddr, task.taskId)
		go postWithAuthWrapper(_RestNotifyPause, url, bytes.NewBuffer([]byte("{}")), task)
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// REST orchestration receiver methods. These are all invoked on followers to handle and respond to
// instructions from the master.
////////////////////////////////////////////////////////////////////////////////////////////////////

// RestHandlePause handles REST API /pauseMgr/Pause by initiating work on the specified taskId on
// this follower node.
func (m *PauseServiceManager) RestHandlePause(w http.ResponseWriter, r *http.Request) {

	// Authenticate
	creds, ok := doAuth(r, w, "PauseServiceManager::RestHandlePause:")
	if !ok {
		err := fmt.Errorf("either invalid credentials or bad request")
		logging.Errorf("PauseServiceManager::RestHandlePause: Failed to authenticate pause register request,"+
			" err[%v]", err)
		return
	}

	if !isAllowed(creds, []string{"cluster.admin.internal.index!write"}, r, w, "PauseServiceManager::RestHandlePause:") {
		return
	}

	writeError := func(w http.ResponseWriter, err error) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error() + "\n"))
	}

	writeOk := func(w http.ResponseWriter) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK\n"))
	}

	var pauseToken PauseToken
	if r.Method == "POST" {
		bytes, _ := ioutil.ReadAll(r.Body)
		if err := json.Unmarshal(bytes, &pauseToken); err != nil {
			logging.Errorf("PauseServiceManager::RestHandlePause: Failed to unmarshal pause token in request"+
				" body: err[%v] bytes[%v]", err, string(bytes))
			writeError(w, err)

			return
		}

		logging.Infof("PauseServiceManager::RestHandlePause: New Pause Token [%v]", pauseToken)

		if m.observeGlobalPauseToken(pauseToken) {
			// Pause token from rest and metakv are the same.

			var task *taskObj
			if task = m.taskFind(pauseToken.PauseId); task == nil {
				err := fmt.Errorf("failed to find task with id[%v]", pauseToken.PauseId)
				logging.Errorf("PauseServiceManager::RestHandlePause: Node[%v] not in Prepared State for pause"+
					": err[%v]", string(m.nodeInfo.NodeID), err)
				writeError(w, err)

				return
			}

			if running := m.pauseResumeRunningById.IsRunning(pauseToken.BucketName, pauseToken.PauseId); len(running) != 1 {

				err := fmt.Errorf("node[%v] not in prepared state for pause-resume", string(m.nodeInfo.NodeID))
				logging.Errorf("PauseServiceManager::RestHandlePause: err[%v]", err)
				writeError(w, err)

				return
			}

			if err := m.setPauseToken(pauseToken.PauseId, &pauseToken); err != nil {
				logging.Errorf("PauseServiceManager::RestHandlePause: Failed to set pause token: err[%v]", err)
				writeError(w, err)

				return
			}

			if err := m.registerLocalPauseToken(&pauseToken); err != nil {
				logging.Errorf("PauseServiceManager::RestHandlePause: Failed to store pause token in local"+
					" meta: err[%v]", err)
				writeError(w, err)

				return
			}

			// TODO: Move task from prepared to running

			if pauseToken.Type == PauseTokenPause {

				// Move bucket to bst_PAUSING state
				if err := m.bucketStateSet("PauseServiceManager::RestHandlePause", task.bucket, bst_PREPARE_PAUSE, bst_PAUSING); err != nil {
					logging.Errorf("PauseServiceManager::RestHandlePause: Failed to change bucketState to pausing"+
						" err[%v]", err)
					writeError(w, err)
					return
				}

				pauser := NewPauser(m, task, &pauseToken, m.pauseDoneCallback,
					m.updateProgressCallback)

				if err := m.setPauser(pauseToken.PauseId, pauser); err != nil {
					logging.Errorf("PauseServiceManager::RestHandlePause: Failed to set Pauser in bookkeeping"+
						" err[%v]", err)
					writeError(w, err)
					return
				}

				pauser.startWorkers()

			} else if pauseToken.Type == PauseTokenResume {

				// Move bucket to bst_RESUMING state
				if err := m.bucketStateSet("PauseServiceManager::RestHandlePause", task.bucket, bst_PREPARE_RESUME, bst_RESUMING); err != nil {
					logging.Errorf("PauseServiceManager::RestHandlePause: Failed to change bucketState to resuming"+
						" err[%v]", err)
					writeError(w, err)
					return
				}

				resumer := NewResumer(m, task, &pauseToken, m.resumeDoneCallback,
					m.updateProgressCallback)

				if err := m.setResumer(pauseToken.PauseId, resumer); err != nil {
					logging.Errorf("PauseServiceManager::RestHandlePause: Failed to set Resumer in bookkeeping"+
						" err[%v]", err)
					writeError(w, err)
					return
				}

				resumer.startWorkers()

			}

			writeOk(w)
			return

		} else {
			// Timed out waiting to see the token in metaKV

			err := fmt.Errorf("pause token wait timeout")
			logging.Errorf("PauseServiceManager::RestHandlePause: Failed to observe token: err[%v]", err)
			writeError(w, err)

			return
		}

	} else {
		writeError(w, fmt.Errorf("PauseServiceManager::RestHandlePause: Unsupported method, use only POST"))
		return
	}
}

// RestHandleGetProgress - it is a route to get the low level pauser/resumer follower progress for a task
func (psm *PauseServiceManager) RestHandleGetProgress(w http.ResponseWriter, r *http.Request) {
	const method = "PauseServiceManager::RestHandleGetProgress:"
	// Authenticate
	creds, ok := doAuth(r, w, method)
	if !ok {
		err := fmt.Errorf("either invalid credentials or bad request")
		logging.Errorf("%v Failed to authenticate pause register request err[%v]", method, err)
		return
	}

	if !isAllowed(creds, []string{"cluster.admin.internal.index!read"}, r, w, method) {
		return
	}

	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Method invalid"))
		logging.Errorf("%v invalid method %v for /pauseMgr/Progress route", method, r.Method)
		return
	}

	taskId := r.URL.Query().Get("id")
	if taskId == "" {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("`id` not found"))
		logging.Errorf("%v invalid id %v", method, taskId)
		return
	}

	task := psm.taskFind(taskId)
	if task == nil {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("`id` not found"))
		logging.Errorf("%v task for id %v not found", method, taskId)
		return
	}

	var progress float64
	// it is possible that a follower has finished the pause/resume but others are still executing;
	// in such cases, pauser/resumer could be cleaned up; if a task exists and pauser/resumer
	// is nil, we can safely assume that the work is finished and return 100.0
	if (task.isPause && task.pauser == nil) ||
		(task.resumer == nil) {
		progress = 100.0
	} else if task.isPause {
		progress = task.pauser.followerProgress.GetFloat64()
	} else {
		progress = task.resumer.followerProgress.GetFloat64()
	}

	progressBytes, err := json.Marshal(progress)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(progressBytes)

	logging.Tracef("%v reported progress as %v for task ID %v", method, progress, taskId)
	return
}

func (m *PauseServiceManager) observeGlobalPauseToken(pauseToken PauseToken) bool {

	// TODO: Make timeout configurable
	// Time in seconds to fail pause if metaKV doesn't deliver the token
	globalTokenWaitTimeout := 60 * time.Second

	checkInterval := 1 * time.Second
	var elapsed time.Duration
	var pToken PauseToken
	path := buildMetakvPathForPauseToken(pauseToken.PauseId)

	for elapsed < globalTokenWaitTimeout {

		found, err := common.MetakvGet(path, &pToken)
		if err != nil {
			logging.Errorf("PauseServiceManager::observeGlobalPauseToken Error Checking Pause Token In Metakv: err[%v] path[%v]",
				err, path)

			time.Sleep(checkInterval)
			elapsed += checkInterval

			continue
		}

		if found {
			if reflect.DeepEqual(pToken, pauseToken) {
				logging.Infof("PauseServiceManager::observeGlobalPauseToken Global And Local Pause Tokens Match: [%v]",
					pauseToken)
				return true

			} else {
				logging.Errorf("PauseServiceManager::observeGlobalPauseToken Mismatch in Global and Local Pause Token: Global[%v] Local[%v]",
					pToken, pauseToken)
				return false

			}
		}

		logging.Infof("PauseServiceManager::observeGlobalPauseToken Waiting for Global Pause Token In Metakv")

		time.Sleep(checkInterval)
		elapsed += checkInterval
	}

	logging.Errorf("PauseServiceManager::observeGlobalPauseToken Timeout Waiting for Global Pause Token In Metakv: path[%v]",
		path)

	return false

}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Handlers for unit test REST APIs (/test/methodName) -- MUST STILL DO AUTHENTICATION!!
////////////////////////////////////////////////////////////////////////////////////////////////////

// testPreparePause handles unit test REST API "/test/PreparePause" by calling the PreparePause API
// directly, which is normally called by ns_server via cbauth RPC.
func (m *PauseServiceManager) testPreparePause(w http.ResponseWriter, r *http.Request) {
	const _testPreparePause = "PauseServiceManager::testPreparePause:"

	m.testPauseOrResume(w, r, _testPreparePause, service.TaskTypePrepared, true)
}

// testPause handles unit test REST API "/test/Pause" by calling the Pause API directly, which is
// normally called by ns_server via cbauth RPC.
func (m *PauseServiceManager) testPause(w http.ResponseWriter, r *http.Request) {
	const _testPause = "PauseServiceManager::testPause:"

	m.testPauseOrResume(w, r, _testPause, service.TaskTypeBucketPause, false)
}

// testPrepareResume handles unit test REST API "/test/PrepareResume" by calling the PreparePause
// API directly, which is normally called by ns_server via cbauth RPC.
func (m *PauseServiceManager) testPrepareResume(w http.ResponseWriter, r *http.Request) {
	const _testPrepareResume = "PauseServiceManager::testPrepareResume:"

	m.testPauseOrResume(w, r, _testPrepareResume, service.TaskTypePrepared, false)
}

// testResume handles unit test REST API "/test/Resume" by calling the Resume API directly, which is
// normally called by ns_server via cbauth RPC.
func (m *PauseServiceManager) testResume(w http.ResponseWriter, r *http.Request) {
	const _testResume = "PauseServiceManager::testResume:"

	m.testPauseOrResume(w, r, _testResume, service.TaskTypeBucketResume, false)
}

// testPauseOrResume is the delegate of testPreparePause, testPause, testPrepareResume, testResume
// since their logic differs only in a few parameters and which API they call.
func (m *PauseServiceManager) testPauseOrResume(w http.ResponseWriter, r *http.Request,
	logPrefix string, taskType service.TaskType, pause bool) {

	logging.Infof("%v called", logPrefix)
	defer logging.Infof("%v returned", logPrefix)

	// Authenticate
	creds, ok := doAuth(r, w, logPrefix)
	if !ok {
		return
	}

	if !isAllowed(creds, []string{"cluster.admin.internal.index!write"}, r, w, logPrefix) {
		return
	}

	// Parameters
	id := r.FormValue("id")
	bucket := r.FormValue("bucket")
	remotePath := r.FormValue("remotePath") // e.g. S3 bucket or filesystem path

	var dryRun bool
	dryRunStr := r.FormValue("dryRun") // PrepareResume, Resume only
	if dryRunStr == "true" {
		dryRun = true
	}

	var err error
	switch taskType {
	case service.TaskTypePrepared:
		if pause {
			err = m.PreparePause(service.PauseParams{ID: id, Bucket: bucket, RemotePath: remotePath})
		} else {
			err = m.PrepareResume(service.ResumeParams{ID: id, Bucket: bucket, RemotePath: remotePath, DryRun: dryRun})
		}
	case service.TaskTypeBucketPause:
		err = m.Pause(service.PauseParams{ID: id, Bucket: bucket, RemotePath: remotePath})
	case service.TaskTypeBucketResume:
		err = m.Resume(service.ResumeParams{ID: id, Bucket: bucket, RemotePath: remotePath, DryRun: dryRun})
	default:
		err = fmt.Errorf("%v invalid taskType %v", logPrefix, taskType)
	}
	if err == nil {
		resp := &TaskResponse{Code: RESP_SUCCESS, TaskId: id}
		rhSend(http.StatusOK, w, resp)
		return
	}
	err = fmt.Errorf("%v %v RPC returned error: %v", logPrefix, taskType, err)
	resp := &TaskResponse{Code: RESP_ERROR, Error: err.Error()}
	rhSend(http.StatusInternalServerError, w, resp)
}

func (psm *PauseServiceManager) testDestroyShards(w http.ResponseWriter, r *http.Request) {
	const _method = "PauseServiceManager::testDestroyShards"

	logging.Infof("%v called", _method)

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	creds, ok := doAuth(r, w, _method)
	if !ok {
		w.Write([]byte("Invalid"))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if !isAllowed(creds, []string{"cluster.admin.internal.index!write"}, r, w, _method) {
		w.Write([]byte("Unauthorized"))
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var shardIds []common.ShardId
	err = json.Unmarshal(body, &shardIds)
	if err != nil {
		w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	respCh := make(chan bool)
	psm.supvMsgch <- &MsgDestroyLocalShardData{shardIds: shardIds, respCh: respCh}
	<-respCh

	w.Write([]byte("Ok"))
	w.WriteHeader(http.StatusOK)

	logging.Infof("%v destroyed shards %v", _method, shardIds)

}

////////////////////////////////////////////////////////////////////////////////////////////////////
// General methods and functions
////////////////////////////////////////////////////////////////////////////////////////////////////

// BucketScansBlocked checks whether a bucket must not currently allow scans due to its Pause-
// Resume state. If scans are blocked, this returns the specific blocking state the bucket is in for
// logging, otherwise it returns bst_NIL. Note that bucket state bst_PREPARE_PAUSE does not block
// scans, so bst_NIL will be returned in this case, thus this method is NOT meant to look up the
// current bucket state but only to check the boolean condition of whether scans should be blocked
// and if so return the blocking state for logging in the caller. bst_RESUMED state still blocks
// scans as this state ends (reverting to bst_NIL) only when ns_server marks the bucket active.
func (m *PauseServiceManager) BucketScansBlocked(bucket string) bucketStateEnum {
	m.bucketStatesMu.RLock()
	defer m.bucketStatesMu.RUnlock()
	bucketState := m.bucketStates[bucket]
	if bucketState >= bst_PAUSING {
		return bucketState
	}
	return bst_NIL
}

// bucketStateCompareAndSwap is a helper for bucketStateSet. It sets m.bucketState to newState iff
// the bucket was in oldState, else it does nothing. It returns both the prior state and whether it
// set newState.
func (m *PauseServiceManager) bucketStateCompareAndSwap(bucket string,
	oldState, newState bucketStateEnum) (priorState bucketStateEnum, swapped bool) {

	m.bucketStatesMu.Lock()
	defer m.bucketStatesMu.Unlock()

	priorState = m.bucketStates[bucket]
	if priorState == oldState {
		m.bucketStates[bucket] = newState
		return priorState, true
	}
	return priorState, false
}

// bucketStateDelete deletes a bucket state from m.bucketState, no matter the prior state.
func (m *PauseServiceManager) bucketStateDelete(bucket string) {
	m.bucketStatesMu.Lock()
	delete(m.bucketStates, bucket)
	m.bucketStatesMu.Unlock()
}

// bucketStateSet sets m.bucketState to newState iff the bucket was in oldState, else it does
// nothing but log and return an error with caller's log prefix.
func (m *PauseServiceManager) bucketStateSet(logPrefix, bucket string,
	oldState, newState bucketStateEnum) error {

	priorState, swapped := m.bucketStateCompareAndSwap(bucket, oldState, newState)
	if !swapped {
		err := service.ErrConflict
		logging.Errorf("%v Cannot set bucket %v to Pause-Resume state %v as it already has conflicting state %v", logPrefix,
			bucket, newState, priorState)
		return err
	}
	return nil
}

// GetIndexerNodeAddresses returns a slice of "host:port" for all the current Indexer nodes
// EXCLUDING the one passed in (used to exclude the current node). This does a cinfo.FetchWithLock
// so it can be expensive. It will return a nil slice if this class has never been able to get a
// valid cinfoClient or if there are no Indexer nodes other than excludeAddr.
func (m *PauseServiceManager) GetIndexerNodeAddresses(excludeAddr string) (nodeAddrs []string) {
	const _GetIndexerNodeAddresses = "PauseCinfo::GetIndexerNodeAddresses:"

	if err := m.genericMgr.cinfo.FetchWithLock(); err != nil {
		logging.Warnf("%v Using potentially stale CIC data as FetchWithLock returned err: %v", _GetIndexerNodeAddresses, err)
	}

	m.genericMgr.cinfo.RLock()
	defer m.genericMgr.cinfo.RUnlock()
	nids := m.genericMgr.cinfo.GetNodeIdsByServiceType(common.INDEX_HTTP_SERVICE)
	for _, nid := range nids {
		nodeAddr, err := m.genericMgr.cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, true)
		if err == nil {
			if nodeAddr != excludeAddr {
				nodeAddrs = append(nodeAddrs, nodeAddr)
			}
		} else { // err != nil
			logging.Errorf("%v Skipping nid %v as GetServiceAddress returned error: %v", _GetIndexerNodeAddresses, nid, err)
		}
	}
	return nodeAddrs
}

// taskAdd adds a task to m.tasks.
func (m *PauseServiceManager) taskAdd(task *taskObj) {
	m.tasksMu.Lock()
	m.tasks[task.taskId] = task
	m.tasksMu.Unlock()
	m.genericMgr.incRev()
}

// taskAddPreparePause constructs and adds a Pause task to m.tasks.
func (m *PauseServiceManager) taskAddPrepare(taskId, bucket, region, remotePath string,
	isPause, dryRun bool) error {
	task, err := NewTaskObj(service.TaskTypePrepared, taskId, bucket, region, remotePath,
		isPause, dryRun)
	if err != nil {
		return err
	}
	m.taskAdd(task)
	return nil
}

// taskClone looks up a task by taskId and if found returns a pointer to a CLONE of it, else nil.
// The clone has no mutex and thus should not be shared.
func (m *PauseServiceManager) taskClone(taskId string) *taskObj {
	task := m.taskFind(taskId)
	if task != nil {
		task.taskMu.RLock()
		taskClone := *task
		task.taskMu.RUnlock()

		taskClone.taskMu = nil // don't share original mutex; nil as clone does not need locking
		return &taskClone
	}
	return nil
}

// taskDelete deletes a task by taskId if it exists. If the task existed, this method returns the
// deleted task, else nil (no-op).
func (m *PauseServiceManager) taskDelete(taskId string) *taskObj {
	m.tasksMu.Lock()
	defer m.tasksMu.Unlock()
	task := m.tasks[taskId]
	if task != nil {
		delete(m.tasks, taskId)
		m.genericMgr.incRev()
	}
	return task
}

// taskFind looks up a task by taskId and if found returns a pointer to it (not a copy), else nil.
func (m *PauseServiceManager) taskFind(taskId string) *taskObj {
	m.tasksMu.RLock()
	defer m.tasksMu.RUnlock()
	return m.tasks[taskId]
}

func (m *PauseServiceManager) taskFindForBucket(bucketName string) map[string]*taskObj {
	m.tasksMu.RLock()
	defer m.tasksMu.RUnlock()

	bucketTasks := make(map[string]*taskObj)
	for taskId, task := range m.tasks {
		if task.bucket == bucketName {
			bucketTasks[taskId] = task
		}
	}

	return bucketTasks
}

// taskSetFailed looks up a task by taskId and if found marks it as failed with the given errMsg and
// returns true, else returns false (not found).
func (m *PauseServiceManager) taskSetFailed(taskId, errMsg string, status service.TaskStatus) bool {
	task := m.taskFind(taskId)
	if task != nil {
		task.TaskObjSetFailed(errMsg, status)
		m.genericMgr.incRev()
		return true
	}
	return false
}

func (m *PauseServiceManager) taskSetMasterAndUpdateType(taskId string, newType service.TaskType) *taskObj {
	task := m.taskFind(taskId)
	if task != nil {
		task.taskMu.Lock()
		task.setMasterNoLock()
		task.updateTaskTypeNoLock(newType)
		task.taskMu.Unlock()
		m.genericMgr.incRev()
	}
	return task
}

func (m *PauseServiceManager) updateProgressCallback(taskId string, progress float64,
	perNodeProgress map[string]float64) {
	task := m.taskFind(taskId)
	if task != nil {
		task.taskMu.Lock()

		oldRev := task.rev
		task.updateTaskProgressNoLock(progress, perNodeProgress)
		newRev := task.rev

		task.taskMu.Unlock()

		if newRev > oldRev {
			m.genericMgr.incRev()
			logging.Infof("PauseServiceManager::updateProgressCallback: updated progress to %v-%v for task ID %v", progress, perNodeProgress, taskId)
		}
	}
}

// hasDryRun returns whether this task has the dryRun parameter.
func (this *taskObj) hasDryRun() bool {
	this.taskMu.RLock()
	defer this.taskMu.RUnlock()
	if this.taskType == service.TaskTypeBucketResume || this.taskType == service.TaskTypePrepared {
		return true
	}
	return false
}

func (task *taskObj) isMaster() bool {
	task.taskMu.RLock()
	defer task.taskMu.RUnlock()

	return task.master
}

// postWithAuthWrapper wraps postWithAuth so it can be called as a goroutine for parallel scatter.
// Errors are logged with caller's logPrefix. bodyBuf contains JSON POST body. If anything fails
// this will mark the task as failed here on the master and also notify the workers of the failure.
func postWithAuthWrapper(logPrefix string, url string, bodyBuf *bytes.Buffer, task *taskObj) {

	resp, err := postWithAuth(url, "application/json", bodyBuf)
	if err != nil {
		err = fmt.Errorf("%v postWithAuth to url: %v returned error: %v", logPrefix, url, err)
		logging.Errorf(err.Error())
		task.TaskObjSetFailed(err.Error(), service.TaskStatusFailed)
		return
	}
	defer resp.Body.Close()

	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		err = fmt.Errorf("%v ReadAll(resp.Body) from url: %v returned error: %v", logPrefix,
			url, err)
		logging.Errorf(err.Error())
		task.TaskObjSetFailed(err.Error(), service.TaskStatusFailed)
		return
	}

	var taskResponse TaskResponse
	err = json.Unmarshal(bytes, &taskResponse)
	if err != nil {
		err = fmt.Errorf("%v Unmarshal from url: %v returned error: %v", logPrefix, url, err)
		logging.Errorf(err.Error())
		task.TaskObjSetFailed(err.Error(), service.TaskStatusFailed)
		return
	}

	// Check if taskResponse reports an error, which would be from GSI code, not HTTP
	if taskResponse.Code == RESP_ERROR {
		err = fmt.Errorf("%v TaskResponse from url: %v reports error: %v", logPrefix,
			url, taskResponse.Error)
		logging.Errorf(err.Error())
		task.TaskObjSetFailed(err.Error(), service.TaskStatusFailed)
		return
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// PauseToken
////////////////////////////////////////////////////////////////////////////////////////////////////

const PauseTokenTag = "PauseToken"
const PauseMetakvDir = common.IndexingMetaDir + "pause/"
const PauseTokenPathPrefix = PauseMetakvDir + PauseTokenTag

type ptFilterFn func(*PauseToken) bool

type PauseTokenType uint8

const (
	PauseTokenInvalid PauseTokenType = iota
	PauseTokenPause
	PauseTokenResume
)

type PauseToken struct {
	MasterId string
	MasterIP string

	BucketName string
	PauseId    string

	Type  PauseTokenType
	Error string

	ArchivePath string
	Region      string
}

func (pt *PauseToken) String() string {
	return fmt.Sprintf("PT[pauseId[%s] bucketName[%s] typ[%v] masterIp[%s] archivePath[%v] region[%v]]",
		pt.PauseId, pt.BucketName, pt.Type, pt.MasterIP, pt.ArchivePath, pt.Region)
}

func (m *PauseServiceManager) genPauseToken(masterIP, bucketName, pauseId, archivePath,
	region string, typ PauseTokenType) *PauseToken {
	cfg := m.config.Load()
	return &PauseToken{
		MasterId:    cfg["nodeuuid"].String(),
		MasterIP:    masterIP,
		BucketName:  bucketName,
		PauseId:     pauseId,
		Type:        typ,
		ArchivePath: archivePath,
		Region:      region,
	}
}

func (m *PauseServiceManager) getPauseToken(id string) (*PauseToken, bool) {
	m.pauseTokenMapMu.RLock()
	defer m.pauseTokenMapMu.RUnlock()

	pt, exists := m.pauseTokensById[id]

	return pt, exists
}

func (m *PauseServiceManager) setPauseToken(id string, pt *PauseToken) error {
	m.pauseTokenMapMu.Lock()
	defer m.pauseTokenMapMu.Unlock()

	if oldPT, ok := m.pauseTokensById[id]; ok {

		if pt == nil {
			delete(m.pauseTokensById, id)
		} else {
			return fmt.Errorf("conflict: PauseToken[%v] with id[%v] already present!", oldPT, id)
		}

	} else if pt != nil {
		m.pauseTokensById[id] = pt
	}

	return nil
}

func (m *PauseServiceManager) findPauseTokensForBucket(bucketName string) (pts []*PauseToken, exists bool) {
	m.pauseTokenMapMu.RLock()
	defer m.pauseTokenMapMu.RUnlock()

	for _, pt := range m.pauseTokensById {
		if pt.BucketName == bucketName {
			pts = append(pts, pt)
		}
	}

	return pts, len(pts) > 0
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// PauseToken - Lifecycle
////////////////////////////////////////////////////////////////////////////////////////////////////

func buildKeyForLocalPauseToken(pauseId string) string {
	return fmt.Sprintf("%s_%s", PauseTokenTag, pauseId)
}

func buildMetakvPathForPauseToken(pauseId string) string {
	return fmt.Sprintf("%s_%s", PauseTokenPathPrefix, pauseId)
}

func (m *PauseServiceManager) registerLocalPauseToken(pauseToken *PauseToken) error {

	pToken, err := json.Marshal(pauseToken)
	if err != nil {
		logging.Errorf("PauseServiceManager::registerLocalPauseToken: Failed to marshal pauseToken[%v]: err[%v]",
			pauseToken, err)
		return err
	}

	respch := make(MsgChannel)
	m.supvMsgch <- &MsgClustMgrLocal{
		mType:  CLUST_MGR_SET_LOCAL,
		key:    buildKeyForLocalPauseToken(pauseToken.PauseId),
		value:  string(pToken),
		respch: respch,
	}

	respMsg := <-respch
	resp := respMsg.(*MsgClustMgrLocal)

	if err = resp.GetError(); err != nil {
		logging.Errorf("PauseServiceManager::registerLocalPauseToken: Unable to set PauseToken In Local Meta"+
			"Storage: [%v]", err)
		return err
	}

	logging.Infof("PauseServiceManager::registerLocalPauseToken: Registered Pause Token In Local Meta: [%v]",
		string(pToken))

	return nil
}

func (m *PauseServiceManager) cleanupLocalPauseToken(pauseId string) error {

	logging.Infof("PauseServiceManager::cleanupLocalPauseToken: Cleanup PauseToken[%v]", pauseId)

	key := buildKeyForLocalPauseToken(pauseId)

	respch := make(MsgChannel)
	m.supvMsgch <- &MsgClustMgrLocal{
		mType:  CLUST_MGR_DEL_LOCAL,
		key:    key,
		respch: respch,
	}

	respMsg := <-respch
	resp := respMsg.(*MsgClustMgrLocal)

	if err := resp.GetError(); err != nil {
		logging.Fatalf("PauseServiceManager::cleanupLocalPauseToken: Unable to delete Pause Token In Local"+
			"Meta Storage. Path[%v] Err[%v]", key, err)
		common.CrashOnError(err)
	}

	if err := m.setPauseToken(pauseId, nil); err != nil {
		return err
	}

	return nil
}

func (m *PauseServiceManager) registerPauseTokenInMetakv(pauseToken *PauseToken) error {

	path := buildMetakvPathForPauseToken(pauseToken.PauseId)

	err := common.MetakvSet(path, pauseToken)
	if err != nil {
		logging.Errorf("PauseServiceManager::registerPauseTokenInMetakv: Unable to set PauseToken In Metakv Storage: Err[%v]", err)
		return err
	}

	logging.Infof("PauseServiceManager::registerPauseTokenInMetakv: Registered Global PauseToken[%v] In Metakv at path[%v]", pauseToken, path)

	return nil
}

func (m *PauseServiceManager) cleanupPauseTokenInMetakv(pauseId string) error {

	path := buildMetakvPathForPauseToken(pauseId)
	var ptoken PauseToken

	if found, err := common.MetakvGet(path, &ptoken); err != nil {
		logging.Errorf("PauseServiceManager::cleanupPauseTokenInMetakv Error Fetching Pause Token From Metakv"+
			" err[%v] path[%v]", err, path)

		return err

	} else if found {
		logging.Infof("PauseServiceManager::cleanupPauseTokenInMetakv Delete Global Pause Token %v", ptoken)

		if err := common.MetakvDel(path); err != nil {
			logging.Fatalf("PauseServiceManager::cleanupPauseTokenInMetakv Unable to delete PauseToken"+
				" from Meta Storage. %v. Err %v", ptoken, err)

			return err
		}

	}

	return nil
}

func (m *PauseServiceManager) registerGlobalPauseToken(pauseToken *PauseToken) (err error) {

	m.genericMgr.cinfo.Lock()
	defer m.genericMgr.cinfo.Unlock()

	nids := m.genericMgr.cinfo.GetNodeIdsByServiceType(common.INDEX_HTTP_SERVICE)
	if len(nids) < 1 {
		err := fmt.Errorf("got too few indexer nodes[%d]", len(nids))
		logging.Errorf("PauseServiceManager::registerGlobalPauseToken: Failed to get NodeIds, err[%v]", err)

		return err
	}

	url := "/pauseMgr/Pause"
	for _, nid := range nids {

		addr, err := m.genericMgr.cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, true)
		if err == nil {

			localaddr, err := m.genericMgr.cinfo.GetLocalServiceAddress(common.INDEX_HTTP_SERVICE, true)
			if err != nil {
				logging.Errorf("PauseServiceManager::registerGlobalPauseToken: Error Fetching Local Service"+
					" Address [%v]", err)
				return err
			}

			if addr == localaddr {
				logging.Infof("PauseServiceManager::registerGlobalPauseToken: Skip local service [%v]", addr)
				continue
			}

			body, err := json.Marshal(pauseToken)
			if err != nil {
				logging.Errorf("PauseServiceManager::registerGlobalPauseToken: Failed to marshal pause token:"+
					" err[%v] pauseToken[%v]", err, pauseToken)
				return err
			}

			bodyBuf := bytes.NewBuffer(body)
			resp, err := postWithAuth(addr+url, "application/json", bodyBuf)
			if err != nil {
				logging.Errorf("PauseServiceManager::registerGlobalPauseToken: Error registering pause token,"+
					" err[%v] addr[%v]", err, addr+url)
				return err
			}

			ioutil.ReadAll(resp.Body)
			resp.Body.Close()

		} else {
			logging.Errorf("PauseServiceManager::registerGlobalPauseToken: Failed to Fetch Service Address:"+
				" [%v]", err)
			return err
		}

		logging.Infof("PauseServiceManager::registerGlobalPauseToken: Successfully registered pause token on"+
			" [%v]", addr+url)
	}

	return nil
}

// monitorBucketForPauseResume - starts monitoring bucket endpoints for state changes to the bucket
//
// not a part of Pauser/Resumer object as it can be garbage collected while this is running
func (psm *PauseServiceManager) monitorBucketForPauseResume(bucketName, taskId string,
	isMaster, isPause bool, shardIds []common.ShardId) {
	// this function should not have any panics as this could get called during bootstrap on crash
	// recovery. If the panic cannot be resolved, indexer will go in crash loop
	defer func() {
		if err := recover(); err != nil {
			logging.Fatalf("PauseServiceManager::monitorBucketForPauseResume: recovered from panic %v", err)
		}
	}()

	config := psm.config.Load()
	clusterAddr, err := common.ClusterAuthUrl(config["clusterAddr"].String())
	if err != nil {
		logging.Errorf("PauseServiceManager::monitorBucketForPauseResume: failed to generate cluster auth url. err: %v for task ID: %v",
			err, taskId)
		return
	}
	client, err := couchbase.Connect(clusterAddr)
	if err != nil {
		logging.Warnf("PauseServiceManager::monitorBucketForPauseResume: creation of couchbase client failed. err: %v",
			err)
	}

	closeCh := make(chan bool)
	deleteShards := false

	processStateUpdate := func(data interface{}) error {
		bucket, ok := data.(*couchbase.Bucket)
		if !ok {
			return errors.New("bucket extraction error. failed to read bucket from data")
		}
		logging.Infof("PauseServicerManager::monitorBucketForPauseResume: got bucket updates %v",
			bucket)
		if len(bucket.HibernationState) == 0 {
			// bucket no more in hibernation
			if isPause {
				psm.rollbackPause(bucketName)
			}
			psm.startBucketStreams(bucketName)
			close(closeCh)
		}
		switch strings.ToLower(bucket.HibernationState) {
		case "resuming", "pausing":
			// no action states
		case "resumed":
			// bucket resumed
			if !isPause {
				psm.startBucketStreams(bucketName)
				close(closeCh)
			}
		case "paused":
			// bucket paused
			if isPause {
				close(closeCh)
				deleteShards = true
			}
		default:
			logging.Warnf("PauseServiceManager::monitorBucketForPauseResume:processStateUpdate: saw unkown hibernation_state `%v` on bucket `%v`",
				bucket.HibernationState, bucketName)
		}

		return nil
	}

	// we could potentially get io.EOF on network drops/server shutdowns too; it is better to retry
	// on streaming endpoint and encounter a `404` for given bucket to be sure that the bucket is
	// deleted from ns_server;
	err = io.EOF
	// can users create bucket with same name on cluster if the original is hibernated?
	// users can have same bucket names in serverless ,but the bucketNames we get from APIS are
	// actually bucket-ids (usually of the form "<bucket-name>-id") which are expected to be unique
	for i := 0; err != nil && (i < 10 || err == io.EOF); i++ {
		err = client.RunObserveCollectionManifestChanges("default", bucketName, processStateUpdate,
			closeCh)
		if err != nil {
			if strings.Contains(err.Error(), "HTTP error 404") {
				// bucket deleted
				if !isPause {
					psm.rollbackResume(bucketName)
				}
				// delete shards for the bucket
				deleteShards = true
			} else {
				// Network error/marshalling error?
				logging.Errorf("PauseServiceManager::monitorBucketForPauseResume: failed to observe bucket (%v) streaming endpoint. err: %v. Retrying (%v)",
					bucketName, err, i)
			}
		}
	}
	if deleteShards {
		start := time.Now()

		respCh := make(chan bool)
		psm.supvMsgch <- &MsgDestroyLocalShardData{
			shardIds: shardIds,
			respCh:   respCh,
		}

		<-respCh

		logging.Infof("PauseServiceManager::monitorBucketForPauseResume: destroyed shards %v for bucket %v (time taken %v)", shardIds, bucketName, time.Since(start).Seconds())
	}
	logging.Infof("PauseServiceManager::monitorBucketForPauseResume: exiting bucket monitor for bucket %v; err %v for task ID: %v (for pause? -> %v)",
		bucketName, err, taskId, isPause)
}

func (psm *PauseServiceManager) startBucketStreams(bucketName string) {
	psm.supvMsgch <- &MsgPauseUpdateBucketState{
		bucket:           bucketName,
		bucketPauseState: bst_ONLINE,
	}
}

func (psm *PauseServiceManager) rollbackPause(bucketName string) {
	psm.bucketStateDelete(bucketName)
}

func (psm *PauseServiceManager) rollbackResume(bucketName string) {
	psm.bucketStateDelete(bucketName)
}

func CancellableTaskRunnerWithContext(ctx context.Context, cancelledError error) func(func() error) error {
	return func(executor func() error) error {
		if ctx == nil {
			return cancelledError
		}
		closeCh := ctx.Done()
		return CancellableTaskRunnerWithChannel(closeCh, cancelledError)(executor)
	}
}

func CancellableTaskRunnerWithChannel(closeCh <-chan struct{}, cancelledError error) func(func() error) error {
	return func(executor func() error) error {
		select {
		case <-closeCh:
			return cancelledError
		default:
			return executor()
		}
	}
}

// generateNodeDir joins archivePath and nodeId to create a node directory used during pause resume
// nodeDir ends with filepath.Seperator(/)
func generateNodeDir(archivePath string, nodeId service.NodeID) string {
	separator := string(filepath.Separator)
	archivePath = strings.TrimSuffix(archivePath, separator)
	return strings.Join([]string{archivePath, fmt.Sprintf("node_%v", nodeId), ""}, separator)
}

// generateShardPath generates a location to download shards from
// shardPath ends with filepath.Seperator(/)
func generateShardPath(nodeDir string, trimmedShardPath string) string {
	separator := string(filepath.Separator)
	trimmedShardPath = strings.TrimPrefix(trimmedShardPath, separator)
	nodeDir = strings.TrimSuffix(nodeDir, separator)
	return strings.Join([]string{nodeDir, trimmedShardPath, ""}, separator)
}

func generatePlasmaCopierConfig(task *taskObj) *plasma.Config {
	cfg := plasma.DefaultConfig()
	cfg.CopyConfig.KeyEncoding = true
	cfg.CopyConfig.KeyPrefix = task.archivePath
	cfg.CopyConfig.Region = task.region
	return &cfg
}

func (m *PauseServiceManager) checkDDLRunningForBucket(bucketName string) (bool, []string) {

	respCh := make(MsgChannel)
	m.supvMsgch <- &MsgCheckDDLInProgress{respCh: respCh, bucketName: bucketName}
	msg := <-respCh

	ddlInProgress := msg.(*MsgDDLInProgressResponse).GetDDLInProgress()
	inProgressIndexNames := msg.(*MsgDDLInProgressResponse).GetInProgressIndexNames()

	return ddlInProgress, inProgressIndexNames
}

func (m *PauseServiceManager) checkRebalanceRunning() (rebalanceRunning bool, err error) {

	respch := make(MsgChannel)
	m.supvMsgch <- &MsgClustMgrLocal{
		mType:  CLUST_MGR_GET_LOCAL,
		key:    RebalanceRunning,
		respch: respch,
	}

	respMsg := <-respch
	resp := respMsg.(*MsgClustMgrLocal)

	if err = resp.GetError(); err != nil {
		if strings.Contains(err.Error(), forestdb.FDB_RESULT_KEY_NOT_FOUND.Error()) {
			return false, nil
		}

		logging.Errorf("PauseServiceManager::checkRebalanceRunning: Failed to get rebalanceRunning from"+
			" local meta: [%v]", err)
		return false, err
	}

	return true, nil
}

// checkIndexesCaughtUp will check index stats for num_docs_pending, num_docs_queued and flush_queue_size.
// If any of them indicate that mutations are still being processed, then indexes are not yet caught up.
func (m *PauseServiceManager) checkIndexesCaughtUp(bucketName string) (_ bool, notCaughtUpIndexes []string) {

	allStats := m.genericMgr.statsMgr.stats.Get()

	for _, idxSts := range allStats.indexes {
		if idxSts.bucket == bucketName &&
			(idxSts.numDocsPending.Value() > 0 ||
				idxSts.numDocsQueued.Value() > 0 ||
				(idxSts.numDocsFlushQueued.Value()-idxSts.numDocsIndexed.Value()) > 0) {

			notCaughtUpIndexes = append(notCaughtUpIndexes, fmt.Sprintf("bkt[%v]idx[%v]pen[%v]que[%v]fqs[%v]",
				bucketName, idxSts.name, idxSts.numDocsPending.Value(), idxSts.numDocsQueued.Value(),
				(idxSts.numDocsFlushQueued.Value()-idxSts.numDocsIndexed.Value())))

		}
	}

	return len(notCaughtUpIndexes) <= 0, notCaughtUpIndexes
}

// PauseResumeRunning flag

func (m *PauseServiceManager) initPreparePhasePauseResume(typ PauseTokenType, bucketName, id string) error {

	m.pauseResumeRunningById.SetRunning(typ, bucketName, id)

	if err := m.registerPauseResumeRunning(id); err != nil {
		m.pauseResumeRunningById.SetNotRunning(id)
		return err
	}

	// TODO: implement start phase monitor

	return nil
}

const PauseResumeRunning = "PauseResumeRunning"
const PauseResumeRunningKepSep = "_"

func buildPauseResumeRunningKey(id string) string {
	return fmt.Sprintf("%s%s%s", PauseResumeRunning, PauseResumeRunningKepSep, id)
}

func decodePauseResumeRunningKey(key string) (string, string) {
	sepIdx := strings.LastIndex(key, PauseResumeRunningKepSep)
	if sepIdx < 0 {
		return "", ""
	}

	return key[:sepIdx], key[sepIdx+1:]
}

func (m *PauseServiceManager) registerPauseResumeRunning(id string) error {

	respch := make(MsgChannel)

	rMeta := m.pauseResumeRunningById.GetMeta(id)
	metaBtyes, err := json.Marshal(rMeta)
	if err != nil {
		return err
	}

	m.supvMsgch <- &MsgClustMgrLocal{
		mType:  CLUST_MGR_SET_LOCAL,
		key:    buildPauseResumeRunningKey(id),
		value:  string(metaBtyes),
		respch: respch,
	}

	respMsg := <-respch
	resp := respMsg.(*MsgClustMgrLocal)

	if errMsg := resp.GetError(); errMsg != nil {
		logging.Errorf("PauseServiceManager::registerPauseResumeRunning: Unable to set PauseResumeRunning In Local"+
			"Meta Storage. Err %v", errMsg)

		return errMsg
	}

	// Notify DDL Service Mgr to stop
	stopDDLProcessing()

	return nil
}

func (m *PauseServiceManager) cleanupPauseResumeRunning(id string) error {

	logging.Infof("PauseServiceManager::cleanupPauseResumeRunning Cleanup")

	respch := make(MsgChannel)
	m.supvMsgch <- &MsgClustMgrLocal{
		mType:  CLUST_MGR_DEL_LOCAL,
		key:    buildPauseResumeRunningKey(id),
		respch: respch,
	}

	respMsg := <-respch
	resp := respMsg.(*MsgClustMgrLocal)

	errMsg := resp.GetError()
	if errMsg != nil {
		logging.Fatalf("PauseServiceManager::cleanupPauseResumeRunning Unable to delete PauseResumeRunning In Local"+
			"Meta Storage. Err %v", errMsg)
		common.CrashOnError(errMsg)
	}

	m.pauseResumeRunningById.SetNotRunning(id)

	// Notify DDL Service Mgr to resume
	resumeDDLProcessing()

	return nil

}

type PauseResumeRunningMap struct {
	sync.RWMutex
	runningMap map[string]*pauseResumeRunningMeta
}

type pauseResumeRunningMeta struct {
	BucketName string
	Typ        PauseTokenType
}

func NewPauseResumeRunningMap() *PauseResumeRunningMap {
	return &PauseResumeRunningMap{
		runningMap: make(map[string]*pauseResumeRunningMeta),
	}
}

func (p *PauseResumeRunningMap) SetRunning(typ PauseTokenType, bucketName, id string) {
	p.Lock()
	defer p.Unlock()

	p.runningMap[id] = &pauseResumeRunningMeta{
		BucketName: bucketName,
		Typ:        typ}
}

func (p *PauseResumeRunningMap) SetNotRunning(id string) (PauseTokenType, string) {
	p.Lock()
	defer p.Unlock()

	rMeta, ok := p.runningMap[id]
	if !ok {
		return PauseTokenInvalid, ""
	}

	delete(p.runningMap, id)

	return rMeta.Typ, rMeta.BucketName
}

func (p *PauseResumeRunningMap) GetMeta(id string) *pauseResumeRunningMeta {
	p.RLock()
	defer p.RUnlock()

	rMeta, _ := p.runningMap[id]

	return rMeta
}

func (p *PauseResumeRunningMap) IsRunning(bucketName, id string) map[string]*pauseResumeRunningMeta {
	running := make(map[string]*pauseResumeRunningMeta)

	p.RLock()
	defer p.RUnlock()

	for rId, rprrMeta := range p.runningMap {
		if rId == id || rprrMeta.BucketName == bucketName {
			running[rId] = rprrMeta
		}
	}

	return running
}

func (prrm *PauseResumeRunningMap) ForEveryKey(callb func(*pauseResumeRunningMeta, string)) {
	prrm.RLock()
	defer prrm.RUnlock()

	for id, rMeta := range prrm.runningMap {
		callb(rMeta, id)
	}
}
