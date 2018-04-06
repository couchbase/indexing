// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package indexer

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/fdb"
	"github.com/couchbase/indexing/secondary/logging"
	mc "github.com/couchbase/indexing/secondary/manager/common"
	"github.com/couchbase/indexing/secondary/memdb"
	"github.com/couchbase/indexing/secondary/memdb/nodetable"
	projClient "github.com/couchbase/indexing/secondary/projector/client"
	"github.com/couchbase/indexing/secondary/stubs/nitro/mm"
	"github.com/couchbase/indexing/secondary/stubs/nitro/plasma"
)

type Indexer interface {
	Shutdown() Message
}

var StreamAddrMap StreamAddressMap
var StreamTopicName map[common.StreamId]string
var ServiceAddrMap map[string]string

type BucketIndexCountMap map[string]int
type BucketFlushInProgressMap map[string]bool
type BucketObserveFlushDoneMap map[string]MsgChannel
type BucketRequestStopCh map[string]StopChannel
type BucketRollbackTs map[string]*common.TsVbuuid

//mem stats
var (
	gMemstatCache            runtime.MemStats
	gMemstatCacheLastUpdated time.Time
	gMemstatLock             sync.RWMutex
)

// Errors
var (
	ErrFatalComm                = errors.New("Fatal Internal Communication Error")
	ErrInconsistentState        = errors.New("Inconsistent Internal State")
	ErrKVRollbackForInitRequest = errors.New("KV Rollback Received For Initial Build Request")
	ErrMaintStreamMissingBucket = errors.New("Bucket Missing in Maint Stream")
	ErrInvalidStream            = errors.New("Invalid Stream")
	ErrIndexerInRecovery        = errors.New("Indexer In Recovery")
	ErrKVConnect                = errors.New("Error Connecting KV")
	ErrUnknownBucket            = errors.New("Unknown Bucket")
	ErrIndexerNotActive         = errors.New("Indexer Not Active")
	ErrInvalidMetadata          = errors.New("Invalid Metadata")
	ErrBucketEphemeral          = errors.New("Ephemeral Buckets Must Use MOI Storage")
)

type indexer struct {
	id    string
	state common.IndexerState

	indexInstMap  common.IndexInstMap //map of indexInstId to IndexInst
	indexPartnMap IndexPartnMap       //map of indexInstId to PartitionInst

	streamBucketStatus map[common.StreamId]BucketStatus

	streamBucketFlushInProgress  map[common.StreamId]BucketFlushInProgressMap
	streamBucketObserveFlushDone map[common.StreamId]BucketObserveFlushDoneMap

	streamBucketRequestStopCh map[common.StreamId]BucketRequestStopCh
	streamBucketRollbackTs    map[common.StreamId]BucketRollbackTs
	streamBucketRequestQueue  map[common.StreamId]map[string]chan *kvRequest
	streamBucketRequestLock   map[common.StreamId]map[string]chan *sync.Mutex

	bucketRollbackTimes map[string]int64

	bucketBuildTs map[string]Timestamp

	//TODO Remove this once cbq bridge support goes away
	bucketCreateClientChMap map[string]MsgChannel

	bucketStreamMergeInProgress map[string]bool

	wrkrRecvCh          MsgChannel //channel to receive messages from workers
	internalRecvCh      MsgChannel //buffered channel to queue worker requests
	adminRecvCh         MsgChannel //channel to receive admin messages
	internalAdminRecvCh MsgChannel //internal channel to receive admin messages
	internalAdminRespCh chan bool  //internal channel to respond admin messages
	shutdownInitCh      MsgChannel //internal shutdown channel for indexer
	shutdownCompleteCh  MsgChannel //indicate shutdown completion

	mutMgrCmdCh        MsgChannel //channel to send commands to mutation manager
	storageMgrCmdCh    MsgChannel //channel to send commands to storage manager
	tkCmdCh            MsgChannel //channel to send commands to timekeeper
	adminMgrCmdCh      MsgChannel //channel to send commands to admin port manager
	rebalMgrCmdCh      MsgChannel //channel to send commands to rebalance manager
	ddlSrvMgrCmdCh     MsgChannel //channel to send commands to ddl service manager
	compactMgrCmdCh    MsgChannel //channel to send commands to compaction manager
	clustMgrAgentCmdCh MsgChannel //channel to send messages to index coordinator
	kvSenderCmdCh      MsgChannel //channel to send messages to kv sender
	settingsMgrCmdCh   MsgChannel
	statsMgrCmdCh      MsgChannel
	scanCoordCmdCh     MsgChannel //chhannel to send messages to scan coordinator

	mutMgrExitCh MsgChannel //channel to indicate mutation manager exited

	tk            Timekeeper        //handle to timekeeper
	storageMgr    StorageManager    //handle to storage manager
	compactMgr    CompactionManager //handle to compaction manager
	mutMgr        MutationManager   //handle to mutation manager
	adminMgr      AdminManager      //handle to admin port manager
	rebalMgr      RebalanceMgr      //handle to rebalance manager
	ddlSrvMgr     *DDLServiceMgr    //handle to ddl service manager
	clustMgrAgent ClustMgrAgent     //handle to ClustMgrAgent
	kvSender      KVSender          //handle to KVSender
	settingsMgr   settingsManager
	statsMgr      *statsManager
	scanCoord     ScanCoordinator //handle to ScanCoordinator
	config        common.Config

	kvlock    sync.Mutex   //fine-grain lock for KVSender
	stateLock sync.RWMutex //lock to protect the bucketStatus map

	stats *IndexerStats

	enableManager bool
	cpuProfFd     *os.File

	rebalanceRunning bool
	rebalanceToken   *RebalanceToken

	mergePartitionList []mergeSpec
	prunePartitionList []pruneSpec
	merged             map[common.IndexInstId]common.IndexInst
	pruned             map[common.IndexInstId]common.IndexInst
	lastStreamUpdate   int64

	bootstrapStorageMode common.StorageMode

	testServRunning bool
}

type kvRequest struct {
	lock     *sync.Mutex
	bucket   string
	streamId common.StreamId
	grantCh  chan bool
}

type mergeSpec struct {
	srcInstId  common.IndexInstId
	tgtInstId  common.IndexInstId
	rebalState common.RebalanceState
	respch     chan error
}

type pruneSpec struct {
	instId     common.IndexInstId
	partitions []common.PartitionId
}

func NewIndexer(config common.Config) (Indexer, Message) {

	idx := &indexer{
		wrkrRecvCh:          make(MsgChannel, WORKER_RECV_QUEUE_LEN),
		internalRecvCh:      make(MsgChannel, WORKER_MSG_QUEUE_LEN),
		adminRecvCh:         make(MsgChannel, WORKER_MSG_QUEUE_LEN),
		internalAdminRecvCh: make(MsgChannel),
		internalAdminRespCh: make(chan bool),
		shutdownInitCh:      make(MsgChannel),
		shutdownCompleteCh:  make(MsgChannel),

		mutMgrCmdCh:        make(MsgChannel),
		storageMgrCmdCh:    make(MsgChannel),
		tkCmdCh:            make(MsgChannel),
		adminMgrCmdCh:      make(MsgChannel),
		rebalMgrCmdCh:      make(MsgChannel),
		ddlSrvMgrCmdCh:     make(MsgChannel),
		compactMgrCmdCh:    make(MsgChannel),
		clustMgrAgentCmdCh: make(MsgChannel),
		kvSenderCmdCh:      make(MsgChannel),
		settingsMgrCmdCh:   make(MsgChannel),
		statsMgrCmdCh:      make(MsgChannel),
		scanCoordCmdCh:     make(MsgChannel),

		mutMgrExitCh: make(MsgChannel),

		indexInstMap:  make(common.IndexInstMap),
		indexPartnMap: make(IndexPartnMap),

		merged: make(map[common.IndexInstId]common.IndexInst),
		pruned: make(map[common.IndexInstId]common.IndexInst),

		streamBucketStatus:           make(map[common.StreamId]BucketStatus),
		streamBucketFlushInProgress:  make(map[common.StreamId]BucketFlushInProgressMap),
		streamBucketObserveFlushDone: make(map[common.StreamId]BucketObserveFlushDoneMap),
		streamBucketRequestStopCh:    make(map[common.StreamId]BucketRequestStopCh),
		streamBucketRollbackTs:       make(map[common.StreamId]BucketRollbackTs),
		streamBucketRequestQueue:     make(map[common.StreamId]map[string]chan *kvRequest),
		streamBucketRequestLock:      make(map[common.StreamId]map[string]chan *sync.Mutex),
		bucketBuildTs:                make(map[string]Timestamp),
		bucketRollbackTimes:          make(map[string]int64),
		bucketCreateClientChMap:      make(map[string]MsgChannel),
		bucketStreamMergeInProgress:  make(map[string]bool),
	}

	logging.Infof("Indexer::NewIndexer Status Warmup")
	snapshotNotifych := make(chan IndexSnapshot, 100)

	var res Message
	idx.settingsMgr, idx.config, res = NewSettingsManager(idx.settingsMgrCmdCh, idx.wrkrRecvCh, config)
	if res.GetMsgType() != MSG_SUCCESS {
		logging.Fatalf("Indexer::NewIndexer settingsMgr Init Error %+v", res)
		return nil, res
	}

	idx.stats = NewIndexerStats()

	// Read memquota setting
	memQuota := int64(idx.config["settings.memory_quota"].Uint64())
	idx.stats.memoryQuota.Set(memQuota)
	plasma.SetMemoryQuota(int64(float64(memQuota) * PLASMA_MEMQUOTA_FRAC))
	memdb.Debug(idx.config["settings.moi.debug"].Bool())
	updateMOIWriters(idx.config["settings.moi.persistence_threads"].Int())
	reclaimBlockSize := int64(idx.config["plasma.LSSReclaimBlockSize"].Int())
	plasma.SetLogReclaimBlockSize(reclaimBlockSize)

	logging.Infof("Indexer::NewIndexer Starting with Vbuckets %v", idx.config["numVbuckets"].Int())

	idx.initStreamAddressMap()
	idx.initStreamFlushMap()
	idx.initServiceAddressMap()

	isEnterprise := idx.config["isEnterprise"].Bool()
	if isEnterprise {
		common.SetBuildMode(common.ENTERPRISE)
	} else {
		common.SetBuildMode(common.COMMUNITY)
	}
	logging.Infof("Indexer::NewIndexer Build Mode Set %v", common.GetBuildMode())

	//Start Mutation Manager
	idx.mutMgr, res = NewMutationManager(idx.mutMgrCmdCh, idx.wrkrRecvCh, idx.config)
	if res.GetMsgType() != MSG_SUCCESS {
		logging.Fatalf("Indexer::NewIndexer Mutation Manager Init Error %+v", res)
		return nil, res
	}

	//Start KV Sender
	idx.kvSender, res = NewKVSender(idx.kvSenderCmdCh, idx.wrkrRecvCh, idx.config)
	if res.GetMsgType() != MSG_SUCCESS {
		logging.Fatalf("Indexer::NewIndexer KVSender Init Error %+v", res)
		return nil, res
	}

	//Start Timekeeper
	idx.tk, res = NewTimekeeper(idx.tkCmdCh, idx.wrkrRecvCh, idx.config)
	if res.GetMsgType() != MSG_SUCCESS {
		logging.Fatalf("Indexer::NewIndexer Timekeeper Init Error %+v", res)
		return nil, res
	}

	//Start Scan Coordinator
	idx.scanCoord, res = NewScanCoordinator(idx.scanCoordCmdCh, idx.wrkrRecvCh, idx.config, snapshotNotifych)
	if res.GetMsgType() != MSG_SUCCESS {
		logging.Fatalf("Indexer::NewIndexer Scan Coordinator Init Error %+v", res)
		return nil, res
	}

	idx.enableManager = idx.config["enableManager"].Bool()

	idx.bootstrapStorageMode = idx.getBootstrapStorageMode(idx.config)
	logging.Infof("bootstrap storage mode %v", idx.bootstrapStorageMode)
	if idx.enableManager {
		idx.clustMgrAgent, res = NewClustMgrAgent(idx.clustMgrAgentCmdCh, idx.adminRecvCh, idx.config, idx.bootstrapStorageMode)
		if res.GetMsgType() != MSG_SUCCESS {
			logging.Fatalf("Indexer::NewIndexer ClusterMgrAgent Init Error %+v", res)
			return nil, res
		}
	}

	idx.statsMgr, res = NewStatsManager(idx.statsMgrCmdCh, idx.wrkrRecvCh, idx.config)
	if res.GetMsgType() != MSG_SUCCESS {
		logging.Fatalf("Indexer::NewIndexer statsMgr Init Error %+v", res)
		return nil, res
	}

	idx.setIndexerState(common.INDEXER_BOOTSTRAP)
	idx.stats.indexerState.Set(int64(common.INDEXER_BOOTSTRAP))
	msgUpdateIndexInstMap := idx.newIndexInstMsg(nil)

	if err := idx.sendUpdatedIndexMapToWorker(msgUpdateIndexInstMap, nil, idx.statsMgrCmdCh, "statsMgr"); err != nil {
		common.CrashOnError(err)
	}

	idx.scanCoordCmdCh <- &MsgIndexerState{mType: INDEXER_BOOTSTRAP}
	<-idx.scanCoordCmdCh

	// Setup http server
	addr := net.JoinHostPort("", idx.config["httpPort"].String())
	logging.PeriodicProfile(logging.Debug, addr, "goroutine")
	go func() {
		srv := &http.Server{
			ReadTimeout:  time.Duration(idx.config["http.readTimeout"].Int()) * time.Second,
			WriteTimeout: time.Duration(idx.config["http.writeTimeout"].Int()) * time.Second,
			Addr:         addr,
			Handler:      nil,
		}
		if err := srv.ListenAndServe(); err != nil {
			logging.Fatalf("indexer:: Error Starting Http Server: %v", err)
			common.CrashOnError(err)
		}
	}()

	sslPort := idx.config["httpsPort"].String()
	if sslPort != "" {
		certFile := idx.config["certFile"].String()
		keyFile := idx.config["keyFile"].String()
		sslAddr := net.JoinHostPort("", sslPort)

		var reload bool = false
		var tlslsnr *net.Listener = nil

		cbauth.RegisterTLSRefreshCallback(func() error {
			if tlslsnr != nil {
				reload = true
				(*tlslsnr).Close()
			}
			return nil
		})

		go func() {
			for {
				cert, err := tls.LoadX509KeyPair(certFile, keyFile)
				if err != nil {
					logging.Fatalf("indexer:: Error in loading SSL certificate: %v", err)
					return
				}

				clientAuthType, err := cbauth.GetClientCertAuthType()
				if err != nil {
					logging.Fatalf("indexer:: Failed to get client cert auth type from cbauth, err: %v", err)
					return
				}

				config := &tls.Config{
					Certificates:             []tls.Certificate{cert},
					CipherSuites:             []uint16{tls.TLS_RSA_WITH_AES_256_CBC_SHA},
					MinVersion:               tls.VersionTLS12,
					PreferServerCipherSuites: true,
					ClientAuth:               clientAuthType,
				}

				if clientAuthType != tls.NoClientCert {
					caCert, err := ioutil.ReadFile(certFile)
					if err != nil {
						logging.Fatalf("indexer:: Error in reading cacert file, err: %v", err)
						return
					}
					caCertPool := x509.NewCertPool()
					caCertPool.AppendCertsFromPEM(caCert)
					config.ClientCAs = caCertPool
				}

				// allow only strong ssl as this is an internal API and interop is not a concern
				sslsrv := &http.Server{
					Addr:         sslAddr,
					TLSNextProto: make(map[string]func(*http.Server, *tls.Conn, http.Handler), 0),
					TLSConfig:    config,
				}
				// replace below with ListenAndServeTLS on moving to go1.8
				lsnr, err := net.Listen("tcp", sslAddr)
				if err != nil {
					logging.Fatalf("indexer:: Error in listenting to SSL port: %v", err)
					return
				}
				val := tls.NewListener(lsnr, sslsrv.TLSConfig)
				tlslsnr = &val
				reload = false
				logging.Infof("indexer:: SSL server started: %v", sslAddr)
				err = http.Serve(*tlslsnr, nil)
				if reload {
					logging.Warnf("indexer:: SSL certificate change: %v", err)
				} else {
					logging.Fatalf("indexer:: Error in SSL Server: %v", err)
					return
				}
			}
		}()
	}

	//read persisted indexer state
	needsRestart, err := idx.bootstrap(snapshotNotifych)
	if err != nil {
		logging.Fatalf("Indexer::Unable to Bootstrap Indexer from Persisted Metadata %v", err)
		return nil, &MsgError{err: Error{cause: err}}
	}

	if needsRestart {
		logging.Infof("Restarting indexer after storage upgrade")
		idx.stats.needsRestart.Set(true)
	}

	//if storageMode has changed in settings while bootstrap was in progress
	//indexer needs to restart
	idx.updateStorageMode(idx.config)

	//Register with Index Coordinator
	if err := idx.registerWithCoordinator(); err != nil {
		//log error and exit
	}

	//sync topology
	if err := idx.syncTopologyWithCoordinator(); err != nil {
		//log error and exit
	}

	//Start Admin port listener
	idx.adminMgr, res = NewAdminManager(idx.adminMgrCmdCh, idx.adminRecvCh)
	if res.GetMsgType() != MSG_SUCCESS {
		logging.Fatalf("Indexer::NewIndexer Admin Manager Init Error %+v", res)
		return nil, res
	}

	//Start DDL Service Manager
	//Initialize DDL Service Manager before rebalance manager so DDL service manager is ready
	//when Rebalancing manager receives ns_server rebalancing callback.
	idx.ddlSrvMgr, res = NewDDLServiceMgr(common.IndexerId(idx.id), idx.ddlSrvMgrCmdCh, idx.wrkrRecvCh, idx.config)
	if res.GetMsgType() != MSG_SUCCESS {
		logging.Fatalf("Indexer::NewIndexer DDL Service Manager Init Error %+v", res)
		return nil, res
	}

	//Start Rebalance Manager
	idx.rebalMgr, res = NewRebalanceMgr(idx.rebalMgrCmdCh, idx.wrkrRecvCh, idx.config, idx.rebalanceRunning, idx.rebalanceToken)
	if res.GetMsgType() != MSG_SUCCESS {
		logging.Fatalf("Indexer::NewIndexer Rebalance Manager Init Error %+v", res)
		return nil, res
	}

	if idx.getIndexerState() == common.INDEXER_BOOTSTRAP {
		idx.setIndexerState(common.INDEXER_ACTIVE)
		idx.stats.indexerState.Set(int64(common.INDEXER_ACTIVE))
	}

	idx.scanCoordCmdCh <- &MsgIndexerState{mType: INDEXER_RESUME, rollbackTimes: idx.bucketRollbackTimes}
	<-idx.scanCoordCmdCh

	// Persist node uuid in Metadata store
	idx.clustMgrAgentCmdCh <- &MsgClustMgrLocal{
		mType: CLUST_MGR_SET_LOCAL,
		key:   INDEXER_NODE_UUID,
		value: idx.config["nodeuuid"].String(),
	}

	respMsg := <-idx.clustMgrAgentCmdCh
	resp := respMsg.(*MsgClustMgrLocal)

	errMsg := resp.GetError()
	if errMsg != nil {
		logging.Fatalf("Indexer::NewIndexer Unable to set INDEXER_NODE_UUID In Local"+
			"Meta Storage. Err %v", errMsg)
		common.CrashOnError(errMsg)
	}

	logging.Infof("Indexer::NewIndexer Status %v", idx.getIndexerState())

	idx.compactMgr, res = NewCompactionManager(idx.compactMgrCmdCh, idx.wrkrRecvCh, idx.config)
	if res.GetMsgType() != MSG_SUCCESS {
		logging.Fatalf("Indexer::NewCompactionmanager Init Error %+v", res)
		return nil, res
	}

	// Initialize the public REST API server after indexer bootstrap is completed
	NewRestServer(idx.config["clusterAddr"].String(), idx.statsMgr)

	go idx.monitorMemUsage()
	go idx.logMemstats()
	go idx.collectProgressStats(true)

	//start the main indexer loop
	idx.run()

	return idx, &MsgSuccess{}

}

func (idx *indexer) collectProgressStats(fetchDcp bool) {

	respCh := make(chan bool)
	idx.internalRecvCh <- &MsgStatsRequest{
		mType:    INDEX_PROGRESS_STATS,
		respch:   respCh,
		fetchDcp: fetchDcp,
	}
	<-respCh

	logging.Infof("progress stats collection done.")

	idx.sendProgressStats()
}

func (idx *indexer) sendProgressStats() {

	idx.internalRecvCh <- &MsgStatsRequest{
		mType:  INDEX_STATS_DONE,
		respch: nil,
	}

	logging.Infof("send progress stats to clients")
}

func (idx *indexer) acquireStreamRequestLock(bucket string, streamId common.StreamId) *kvRequest {

	// queue request
	request := &kvRequest{grantCh: make(chan bool, 1), lock: nil, bucket: bucket, streamId: streamId}

	idx.kvlock.Lock()
	defer idx.kvlock.Unlock()

	// allocate the request queue
	rq, ok := idx.streamBucketRequestQueue[streamId][bucket]
	if !ok {
		rq = make(chan *kvRequest, 5000)

		if _, ok = idx.streamBucketRequestQueue[streamId]; !ok {
			idx.streamBucketRequestQueue[streamId] = make(map[string]chan *kvRequest)
		}
		idx.streamBucketRequestQueue[streamId][bucket] = rq
	}

	// allocate the lock
	lq, ok := idx.streamBucketRequestLock[streamId][bucket]
	if !ok {
		lq = make(chan *sync.Mutex, 1) // hold one lock

		if _, ok = idx.streamBucketRequestLock[streamId]; !ok {
			idx.streamBucketRequestLock[streamId] = make(map[string]chan *sync.Mutex)
		}
		idx.streamBucketRequestLock[streamId][bucket] = lq

		// seed the lock
		lq <- new(sync.Mutex)
	}

	// acquire the lock if it is available and there is no other request ahead of me
	if len(rq) == 0 && len(lq) == 1 {
		request.lock = <-lq
		request.grantCh <- true
	} else if len(rq) < 5000 {
		rq <- request
	} else {
		common.CrashOnError(errors.New("acquireStreamRequestLock: too many requests acquiring stream request lock"))
	}

	return request
}

func (idx *indexer) waitStreamRequestLock(req *kvRequest) {

	<-req.grantCh
}

func (idx *indexer) releaseStreamRequestLock(req *kvRequest) {

	if req.lock == nil {
		return
	}

	idx.kvlock.Lock()
	defer idx.kvlock.Unlock()

	streamId := req.streamId
	bucket := req.bucket

	rq, ok := idx.streamBucketRequestQueue[streamId][bucket]
	if ok && len(rq) != 0 {
		next := <-rq
		next.lock = req.lock
		next.grantCh <- true
	} else {
		if lq, ok := idx.streamBucketRequestLock[streamId][bucket]; ok {
			lq <- req.lock
		} else {
			common.CrashOnError(errors.New("releaseStreamRequestLock: streamBucketRequestLock is not initialized"))
		}
	}
}

func (idx *indexer) registerWithCoordinator() error {

	//get the IndexerId from persistence and send it to Index Coordinator

	//if there is no IndexerId, send an empty one. Coordinator will assign
	//a new IndexerId in that case and treat this as a fresh node.
	return nil

}

func (idx *indexer) syncTopologyWithCoordinator() error {

	//get the latest topology from coordinator
	return nil
}

func (idx *indexer) recoverPersistedSnapshots() error {

	//recover persisted snapshots from disk
	return nil

}

//run starts the main loop for the indexer
func (idx *indexer) run() {

	go idx.listenWorkerMsgs()
	go idx.listenAdminMsgs()

	for {

		select {

		case msg, ok := <-idx.internalRecvCh:
			if ok {
				idx.handleWorkerMsgs(msg)
			}

		case msg, ok := <-idx.internalAdminRecvCh:
			if ok {
				idx.handleAdminMsgs(msg)
				idx.internalAdminRespCh <- true
			}

		case <-idx.shutdownInitCh:
			//send shutdown to all workers

			idx.shutdownWorkers()
			//close the shutdown complete channel to indicate
			//all workers are shutdown
			close(idx.shutdownCompleteCh)
			return

		}

	}

}

//run starts the main loop for the indexer
func (idx *indexer) listenAdminMsgs() {

	waitForStream := true

	for {
		select {
		case msg, ok := <-idx.adminRecvCh:
			if ok {
				// internalAdminRecvCh size is 1.   So it will blocked if the previous msg is being
				// processed.
				idx.internalAdminRecvCh <- msg
				<-idx.internalAdminRespCh

				if waitForStream {
					// now that indexer has processed the message.  Let's make sure that
					// the stream request is finished before processing the next admin
					// msg.  This is done by acquiring a lock on the stream request for each
					// bucket (on both streams).   The lock is FIFO, so if this function
					// can get a lock, it will mean that previous stream request would have
					// been cleared.
					buckets := idx.getBucketForAdminMsg(msg)
					for _, bucket := range buckets {
						f := func(streamId common.StreamId, bucket string) {
							lock := idx.acquireStreamRequestLock(bucket, streamId)
							defer idx.releaseStreamRequestLock(lock)
							idx.waitStreamRequestLock(lock)
						}

						f(common.INIT_STREAM, bucket)
						f(common.MAINT_STREAM, bucket)
					}
				}
			}
		case <-idx.shutdownInitCh:
			return
		}
	}
}

func (idx *indexer) getBucketForAdminMsg(msg Message) []string {
	switch msg.GetMsgType() {

	case CLUST_MGR_CREATE_INDEX_DDL, CBQ_CREATE_INDEX_DDL:
		createMsg := msg.(*MsgCreateIndex)
		return []string{createMsg.GetIndexInst().Defn.Bucket}

	case CLUST_MGR_BUILD_INDEX_DDL:
		buildMsg := msg.(*MsgBuildIndex)
		return buildMsg.GetBucketList()

	case CLUST_MGR_DROP_INDEX_DDL, CBQ_DROP_INDEX_DDL:
		dropMsg := msg.(*MsgDropIndex)
		return []string{dropMsg.GetBucket()}

	default:
		return nil
	}
}

func (idx *indexer) listenWorkerMsgs() {

	//listen to worker messages
	for {

		select {

		case msg, ok := <-idx.wrkrRecvCh:
			if ok {
				//handle high priority messages
				switch msg.GetMsgType() {
				case MSG_ERROR:
					err := msg.(*MsgError).GetError()
					if err.code == ERROR_MUT_MGR_PANIC {
						close(idx.mutMgrExitCh)
					}
				}
				idx.internalRecvCh <- msg
			}

		case <-idx.shutdownInitCh:
			//exit the loop
			return
		}
	}

}

func (idx *indexer) handleWorkerMsgs(msg Message) {

	switch msg.GetMsgType() {

	case STREAM_READER_HWT:
		//fwd the message to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

	case STREAM_READER_STREAM_BEGIN:

		//fwd the message to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

	case STREAM_READER_STREAM_END:

		//fwd the message to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

	case STREAM_READER_STREAM_DROP_DATA:

		logging.Warnf("Indexer::handleWorkerMsgs Received Drop Data "+
			"From Mutation Mgr %v. Ignored.", msg)

	case STREAM_READER_SNAPSHOT_MARKER:
		//fwd the message to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

	case STREAM_READER_CONN_ERROR:

		//fwd the message to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

	case TK_STABILITY_TIMESTAMP:
		//send TS to Mutation Manager
		ts := msg.(*MsgTKStabilityTS).GetTimestamp()
		bucket := msg.(*MsgTKStabilityTS).GetBucket()
		streamId := msg.(*MsgTKStabilityTS).GetStreamId()
		changeVec := msg.(*MsgTKStabilityTS).GetChangeVector()

		if idx.getStreamBucketState(streamId, bucket) == STREAM_INACTIVE {
			logging.Warnf("Indexer: Skipped PersistTs for %v %v. "+
				"STREAM_INACTIVE", streamId, bucket)
			return
		}

		idx.streamBucketFlushInProgress[streamId][bucket] = true

		if ts.GetSnapType() == common.FORCE_COMMIT {
			idx.storageMgrCmdCh <- &MsgMutMgrFlushDone{mType: MUT_MGR_FLUSH_DONE,
				streamId: streamId,
				bucket:   bucket,
				ts:       ts}
			<-idx.storageMgrCmdCh
		} else {
			idx.mutMgrCmdCh <- &MsgMutMgrFlushMutationQueue{
				mType:     MUT_MGR_PERSIST_MUTATION_QUEUE,
				bucket:    bucket,
				ts:        ts,
				streamId:  streamId,
				changeVec: changeVec}

			<-idx.mutMgrCmdCh
		}

	case MUT_MGR_ABORT_PERSIST:

		idx.mutMgrCmdCh <- msg
		<-idx.mutMgrCmdCh

	case MUT_MGR_FLUSH_DONE:

		idx.storageMgrCmdCh <- msg
		<-idx.storageMgrCmdCh

	case MUT_MGR_ABORT_DONE:

		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

	case STORAGE_SNAP_DONE:

		bucket := msg.(*MsgMutMgrFlushDone).GetBucket()
		streamId := msg.(*MsgMutMgrFlushDone).GetStreamId()

		// consolidate partitions now
		if streamId == common.MAINT_STREAM {
			idx.mergePartitions(bucket)
			idx.prunePartitions(bucket)
		}

		idx.streamBucketFlushInProgress[streamId][bucket] = false

		//if there is any observer for flush done, notify
		idx.notifyFlushObserver(msg)

		//fwd the message to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

	case TK_INIT_BUILD_DONE:
		idx.handleInitialBuildDone(msg)

	case TK_MERGE_STREAM:
		idx.handleMergeStream(msg)

	case INDEXER_PREPARE_RECOVERY:
		idx.handlePrepareRecovery(msg)

	case INDEXER_INITIATE_RECOVERY:
		idx.handleInitRecovery(msg)

	case STORAGE_INDEX_SNAP_REQUEST,
		STORAGE_INDEX_STORAGE_STATS,
		STORAGE_INDEX_COMPACT:
		idx.storageMgrCmdCh <- msg
		<-idx.storageMgrCmdCh

	case CONFIG_SETTINGS_UPDATE:
		idx.handleConfigUpdate(msg)

	case INDEXER_INIT_PREP_RECOVERY:
		idx.handleInitPrepRecovery(msg)

	case INDEXER_PREPARE_UNPAUSE:
		idx.handlePrepareUnpause(msg)

	case INDEXER_UNPAUSE:
		idx.handleUnpause(msg)

	case INDEXER_PREPARE_DONE:
		idx.handlePrepareDone(msg)

	case INDEXER_RECOVERY_DONE:
		idx.handleRecoveryDone(msg)

	case KV_STREAM_REPAIR:
		idx.handleKVStreamRepair(msg)

	case TK_INIT_BUILD_DONE_ACK:
		idx.handleInitBuildDoneAck(msg)

	case TK_MERGE_STREAM_ACK:
		idx.handleMergeStreamAck(msg)

	case STREAM_REQUEST_DONE:
		idx.handleStreamRequestDone(msg)

	case KV_SENDER_RESTART_VBUCKETS:

		//fwd the message to kv_sender
		idx.sendMsgToKVSender(msg)

	case STORAGE_STATS:
		idx.storageMgrCmdCh <- msg
		<-idx.storageMgrCmdCh

	case SCAN_STATS:
		idx.scanCoordCmdCh <- msg
		<-idx.scanCoordCmdCh

	case INDEX_PROGRESS_STATS:
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

	case INDEX_STATS_DONE:
		idx.clustMgrAgentCmdCh <- msg
		<-idx.clustMgrAgentCmdCh

	case INDEXER_BUCKET_NOT_FOUND:
		idx.handleBucketNotFound(msg)

	case INDEXER_MTR_FAIL:
		idx.handleMTRFail(msg)

	case INDEXER_STATS:
		idx.handleStats(msg)

	case MSG_ERROR,
		STREAM_READER_ERROR:
		//crash for all errors by default
		logging.Fatalf("Indexer::handleWorkerMsgs Fatal Error On Worker Channel %+v", msg)
		err := msg.(*MsgError).GetError()
		common.CrashOnError(err.cause)

	case STATS_RESET:
		idx.handleResetStats()

	case INDEXER_PAUSE:
		idx.handleIndexerPause(msg)

	case INDEXER_RESUME:
		idx.handleIndexerResume(msg)

	case CLUST_MGR_SET_LOCAL:
		idx.handleSetLocalMeta(msg)

	case CLUST_MGR_GET_LOCAL:
		idx.handleGetLocalMeta(msg)

	case CLUST_MGR_DEL_LOCAL:
		idx.handleDelLocalMeta(msg)

	case INDEXER_CHECK_DDL_IN_PROGRESS:
		idx.handleCheckDDLInProgress(msg)

	case INDEXER_UPDATE_RSTATE:
		idx.handleUpdateIndexRState(msg)

	case INDEXER_MERGE_PARTITION:
		idx.handleMergePartition(msg)

	case INDEXER_CANCEL_MERGE_PARTITION:
		idx.handleCancelMergePartition(msg)

	default:
		logging.Fatalf("Indexer::handleWorkerMsgs Unknown Message %+v", msg)
		common.CrashOnError(errors.New("Unknown Msg On Worker Channel"))
	}

}

func (idx *indexer) updateStorageMode(newConfig common.Config) {

	newConfig.SetValue("nodeuuid", idx.config["nodeuuid"].String())
	confStorageMode := strings.ToLower(newConfig["settings.storage_mode"].String())

	if confStorageMode != "" && confStorageMode != common.GetClusterStorageMode().String() {
		common.SetClusterStorageModeStr(confStorageMode)
	}

	s := common.IndexTypeToStorageMode(common.IndexType(confStorageMode))
	s = idx.promoteStorageModeIfNecessary(s, newConfig)
	confStorageMode = string(common.StorageModeToIndexType(s))
	logging.Infof("Indexer::updateStorageMode Try setting storage mode to %v", confStorageMode)

	if common.GetStorageMode() == common.NOT_SET {
		if confStorageMode != "" {
			if idx.canSetStorageMode(confStorageMode) {
				if common.SetStorageModeStr(confStorageMode) {
					logging.Infof("Indexer::updateStorageMode Storage Mode Set %v", common.GetStorageMode())
					initStorageSettings(newConfig)
					idx.stats.needsRestart.Set(true)
				} else {
					logging.Infof("Indexer::updateStorageMode Invalid Storage Mode %v", confStorageMode)
				}
			}
		}

	} else {
		if confStorageMode != "" && confStorageMode != common.GetStorageMode().String() {
			if idx.checkAnyValidIndex() {
				logging.Warnf("Indexer::updateStorageMode Ignore New Storage Mode %v. Already Set %v. Valid Indexes Found.",
					confStorageMode, common.GetStorageMode())
			} else {
				if common.SetStorageModeStr(confStorageMode) {
					logging.Infof("Indexer::updateStorageMode Storage Mode Set %v", common.GetStorageMode())
					initStorageSettings(newConfig)
					idx.stats.needsRestart.Set(true)
				} else {
					logging.Infof("Indexer::updateStorageMode Invalid Storage Mode %v", confStorageMode)
				}
			}
		}
	}
}

func (idx *indexer) handleConfigUpdate(msg Message) {

	cfgUpdate := msg.(*MsgConfigUpdate)
	newConfig := cfgUpdate.GetConfig()

	idx.updateStorageMode(newConfig)

	if newConfig["settings.memory_quota"].Uint64() !=
		idx.config["settings.memory_quota"].Uint64() {

		memQuota := int64(newConfig["settings.memory_quota"].Uint64())
		idx.stats.memoryQuota.Set(memQuota)
		plasma.SetMemoryQuota(int64(float64(memQuota) * PLASMA_MEMQUOTA_FRAC))

		if common.GetStorageMode() == common.FORESTDB ||
			common.GetStorageMode() == common.NOT_SET {
			logging.Infof("Indexer::handleConfigUpdate restart indexer due to memory_quota")
			idx.stats.needsRestart.Set(true)
		}
	}
	if common.GetStorageMode() == common.MOI {
		if moiPersisters := newConfig["settings.moi.persistence_threads"].Int(); moiPersisters != idx.config["settings.moi.persistence_threads"].Int() {
			if moiPersisters <= cap(moiWriterSemaphoreCh) {
				logging.Infof("Indexer: Setting MOI persisters to %v",
					moiPersisters)
			} else {
				logging.Infof(
					"Indexer: Limiting MOI persisters to %v instead of %v",
					cap(moiWriterSemaphoreCh), moiPersisters)
			}
			go updateMOIWriters(moiPersisters)
		}
	}

	if newConfig["settings.max_array_seckey_size"].Int() !=
		idx.config["settings.max_array_seckey_size"].Int() {
		logging.Infof("Indexer::handleConfigUpdate restart indexer due to max_array_seckey_size")
		idx.stats.needsRestart.Set(true)
	}

	if newConfig["settings.max_seckey_size"].Int() !=
		idx.config["settings.max_seckey_size"].Int() {
		logging.Infof("Indexer::handleConfigUpdate restart indexer due to max_seckey_size")
		idx.stats.needsRestart.Set(true)
	}

	if newConfig["settings.allow_large_keys"].Bool() !=
		idx.config["settings.allow_large_keys"].Bool() {
		logging.Infof("Indexer::handleConfigUpdate restart indexer due to allow_large_keys")
		idx.stats.needsRestart.Set(true)
	}

	if percent, ok := newConfig["settings.gc_percent"]; ok && percent.Int() > 0 {
		logging.Infof("Indexer: Setting GC percent to %v", percent.Int())
		debug.SetGCPercent(percent.Int())
	}

	if newConfig["api.enableTestServer"].Bool() && !idx.testServRunning {
		// Start indexer endpoints for CRUD operations.
		// Initialize the QE REST server on config change.
		NewTestServer(idx.config["clusterAddr"].String())
		idx.testServRunning = true
	}

	memdb.Debug(idx.config["settings.moi.debug"].Bool())
	idx.setProfilerOptions(newConfig)
	idx.config = newConfig
	idx.compactMgrCmdCh <- msg
	<-idx.compactMgrCmdCh
	idx.tkCmdCh <- msg
	<-idx.tkCmdCh
	idx.scanCoordCmdCh <- msg
	<-idx.scanCoordCmdCh
	idx.kvSenderCmdCh <- msg
	<-idx.kvSenderCmdCh
	idx.mutMgrCmdCh <- msg
	<-idx.mutMgrCmdCh
	idx.statsMgrCmdCh <- msg
	<-idx.statsMgrCmdCh
	idx.rebalMgrCmdCh <- msg
	<-idx.rebalMgrCmdCh
	idx.ddlSrvMgrCmdCh <- msg
	<-idx.ddlSrvMgrCmdCh
	idx.clustMgrAgentCmdCh <- msg
	<-idx.clustMgrAgentCmdCh
	idx.updateSliceWithConfig(newConfig)
}

func (idx *indexer) handleAdminMsgs(msg Message) {

	switch msg.GetMsgType() {

	case CLUST_MGR_CREATE_INDEX_DDL,
		CBQ_CREATE_INDEX_DDL:

		idx.handleCreateIndex(msg)

	case CLUST_MGR_BUILD_INDEX_DDL:
		idx.handleBuildIndex(msg)

	case CLUST_MGR_DROP_INDEX_DDL,
		CBQ_DROP_INDEX_DDL:

		idx.handleDropIndex(msg)

	case CLUST_MGR_PRUNE_PARTITION:
		idx.handlePrunePartition(msg)

	case MSG_ERROR:

		logging.Fatalf("Indexer::handleAdminMsgs Fatal Error On Admin Channel %+v", msg)
		err := msg.(*MsgError).GetError()
		common.CrashOnError(err.cause)

	default:
		logging.Errorf("Indexer::handleAdminMsgs Unknown Message %+v", msg)
		common.CrashOnError(errors.New("Unknown Msg On Admin Channel"))

	}

}

//TODO handle panic, otherwise main loop will get shutdown
func (idx *indexer) handleCreateIndex(msg Message) {

	indexInst := msg.(*MsgCreateIndex).GetIndexInst()
	clientCh := msg.(*MsgCreateIndex).GetResponseChannel()

	logging.Infof("Indexer::handleCreateIndex %v", indexInst)

	// NOTE
	// If this function adds new validation or changes error message, need
	// to update lifecycle mgr and ddl service mgr.
	//

	is := idx.getIndexerState()
	if is != common.INDEXER_ACTIVE {

		errStr := fmt.Sprintf("Indexer Cannot Process Create Index In %v State", is)
		logging.Errorf("Indexer::handleCreateIndex %v", errStr)

		if clientCh != nil {
			clientCh <- &MsgError{
				err: Error{code: ERROR_INDEXER_NOT_ACTIVE,
					severity: FATAL,
					cause:    errors.New(errStr),
					category: INDEXER}}

		}
		return
	}

	if !ValidateBucket(idx.config["clusterAddr"].String(), indexInst.Defn.Bucket, []string{indexInst.Defn.BucketUUID}) {
		logging.Errorf("Indexer::handleCreateIndex \n\t Bucket %v Not Found")

		if clientCh != nil {
			clientCh <- &MsgError{
				err: Error{code: ERROR_INDEXER_UNKNOWN_BUCKET,
					severity: FATAL,
					cause:    ErrUnknownBucket,
					category: INDEXER}}

		}
		return
	}

	ephemeral, err := IsEphemeral(idx.config["clusterAddr"].String(), indexInst.Defn.Bucket)
	if err != nil {
		errStr := fmt.Sprintf("Cannot Query Bucket Type of %v", indexInst.Defn.Bucket)
		logging.Errorf(errStr)
		if clientCh != nil {
			clientCh <- &MsgError{
				err: Error{severity: FATAL,
					cause:    errors.New(errStr),
					category: INDEXER}}

		}
		return
	}
	if ephemeral && common.GetStorageMode() != common.MOI {
		logging.Errorf("Indexer::handleCreateIndex \n\t Bucket %v is Ephemeral but GSI storage is not MOI")
		if clientCh != nil {
			clientCh <- &MsgError{
				err: Error{code: ERROR_BUCKET_EPHEMERAL,
					severity: FATAL,
					cause:    ErrBucketEphemeral,
					category: INDEXER}}

		}
		return
	}

	if idx.rebalanceRunning || idx.rebalanceToken != nil {

		reqCtx := msg.(*MsgCreateIndex).GetRequestCtx()

		if reqCtx != nil && reqCtx.ReqSource == common.DDLRequestSourceUser {

			errStr := fmt.Sprintf("Indexer Cannot Process Create Index - Rebalance In Progress")
			logging.Errorf("Indexer::handleCreateIndex %v", errStr)

			if clientCh != nil {
				clientCh <- &MsgError{
					err: Error{code: ERROR_INDEXER_REBALANCE_IN_PROGRESS,
						severity: FATAL,
						cause:    errors.New(errStr),
						category: INDEXER}}

			}
			return
		}
	}

	initState := idx.getStreamBucketState(common.INIT_STREAM, indexInst.Defn.Bucket)
	maintState := idx.getStreamBucketState(common.MAINT_STREAM, indexInst.Defn.Bucket)

	if initState == STREAM_RECOVERY ||
		initState == STREAM_PREPARE_RECOVERY ||
		maintState == STREAM_RECOVERY ||
		maintState == STREAM_PREPARE_RECOVERY {
		logging.Errorf("Indexer::handleCreateIndex \n\tCannot Process Create Index " +
			"In Recovery Mode.")

		if clientCh != nil {
			clientCh <- &MsgError{
				err: Error{code: ERROR_INDEXER_IN_RECOVERY,
					severity: FATAL,
					cause:    ErrIndexerInRecovery,
					category: INDEXER}}

		}
		return
	}

	//check if this is duplicate index instance
	if ok := idx.checkDuplicateIndex(indexInst, clientCh); !ok {
		return
	}

	//validate storage mode with using specified in CreateIndex
	if common.GetStorageMode() == common.NOT_SET {
		errStr := "Please Set Indexer Storage Mode Before Create Index"
		logging.Errorf(errStr)

		if clientCh != nil {
			clientCh <- &MsgError{
				err: Error{severity: FATAL,
					cause:    errors.New(errStr),
					category: INDEXER}}

		}
		return
	} else {
		if common.IndexTypeToStorageMode(indexInst.Defn.Using) != common.GetStorageMode() {

			errStr := fmt.Sprintf("Cannot Create Index with Using %v. Indexer "+
				"Storage Mode %v", indexInst.Defn.Using, common.GetStorageMode())

			logging.Errorf(errStr)

			if clientCh != nil {
				clientCh <- &MsgError{
					err: Error{severity: FATAL,
						cause:    errors.New(errStr),
						category: INDEXER}}

			}
			return
		}
	}

	partitions := indexInst.Pc.GetAllPartitions()
	for _, partnDefn := range partitions {
		idx.stats.AddPartition(indexInst.InstId, indexInst.Defn.Bucket, indexInst.Defn.Name, indexInst.ReplicaId, partnDefn.GetPartitionId())
	}

	//allocate partition/slice
	var partnInstMap PartitionInstMap
	if partnInstMap, err = idx.initPartnInstance(indexInst, clientCh); err != nil {
		return
	}

	// update rollback time for the bucket
	if _, ok := idx.bucketRollbackTimes[indexInst.Defn.Bucket]; !ok {
		idx.bucketRollbackTimes[indexInst.Defn.Bucket] = time.Now().UnixNano()
	}

	//update index maps with this index
	idx.indexInstMap[indexInst.InstId] = indexInst
	idx.indexPartnMap[indexInst.InstId] = partnInstMap

	msgUpdateIndexInstMap := idx.newIndexInstMsg(idx.indexInstMap)
	msgUpdateIndexPartnMap := &MsgUpdatePartnMap{indexPartnMap: idx.indexPartnMap}

	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, msgUpdateIndexPartnMap); err != nil {
		if clientCh != nil {
			clientCh <- &MsgError{
				err: Error{code: ERROR_INDEXER_INTERNAL_ERROR,
					severity: FATAL,
					cause:    err,
					category: INDEXER}}
		}
		common.CrashOnError(err)
	}

	if idx.enableManager {
		clientCh <- &MsgSuccess{}
	} else {
		//for cbq bridge, simulate build index
		idx.handleBuildIndex(&MsgBuildIndex{indexInstList: []common.IndexInstId{indexInst.InstId},
			respCh: clientCh})
	}

}

func (idx *indexer) handleCancelMergePartition(msg Message) {

	indexStateMap := msg.(*MsgCancelMergePartition).GetIndexStateMap()
	respch := msg.(*MsgCancelMergePartition).GetResponseChannel()

	for instId, _ := range indexStateMap {
		if inst, ok := idx.indexInstMap[instId]; ok {
			indexStateMap[instId] = inst.RState
		}
	}

	idx.mergePartitionList = nil

	logging.Infof("indexer::CancelMergePartition. IndexStateMap %v", indexStateMap)

	if respch != nil {
		close(respch)
	}
}

func (idx *indexer) handleMergePartition(msg Message) {

	srcInstId := msg.(*MsgMergePartition).GetSourceInstId()
	tgtInstId := msg.(*MsgMergePartition).GetTargetInstId()
	rebalState := msg.(*MsgMergePartition).GetRebalanceState()
	respch := msg.(*MsgMergePartition).GetResponseChannel()

	if err := idx.preValidateMergePartition(srcInstId, tgtInstId); err != nil {
		if respch != nil {
			respch <- err
		}
		return
	}

	idx.updateRStateOrMergePartition(srcInstId, tgtInstId, rebalState, respch)
}

func (idx *indexer) updateRStateOrMergePartition(srcInstId common.IndexInstId, tgtInstId common.IndexInstId,
	rebalState common.RebalanceState, respch chan error) {

	if _, ok := idx.indexInstMap[srcInstId]; ok {

		spec := mergeSpec{
			srcInstId:  srcInstId,
			tgtInstId:  tgtInstId,
			rebalState: rebalState,
			respch:     respch,
		}

		idx.mergePartitionList = append(idx.mergePartitionList, spec)

	} else {
		// This is not a proxy index instance.  No need to merge.  Just update RState.
		if inst, ok := idx.indexInstMap[tgtInstId]; ok {
			inst.RState = rebalState
			idx.indexInstMap[tgtInstId] = inst

			instIds := []common.IndexInstId{tgtInstId}
			idx.updateMetaInfoForIndexList(instIds, false, false, false, false, true, true, false, false, respch)

			logging.Infof("MergePartition: sent async request to update index instance %v rstate moved to ACTIVE", tgtInstId)
		}
	}

	idx.mergePartitionForIdleBuckets()
}

func (idx *indexer) mergePartitionForIdleBuckets() {

	buckets := make(map[string]bool)
	for _, spec := range idx.mergePartitionList {
		sourceId := spec.srcInstId
		if source, ok := idx.indexInstMap[sourceId]; ok {
			buckets[source.Defn.Bucket] = true
		}
	}

	for bucket, _ := range buckets {
		if !idx.streamBucketFlushInProgress[common.MAINT_STREAM][bucket] {
			idx.mergePartitions(bucket)
		}
	}
}

func (idx *indexer) preValidateMergePartition(srcInstId common.IndexInstId, tgtInstId common.IndexInstId) error {

	inst, ok := idx.indexInstMap[srcInstId]
	if !ok {
		if tgtInstId != 0 {
			if _, ok := idx.indexInstMap[tgtInstId]; !ok {
				err := fmt.Errorf("MergePartition: Both proxy index Instance %v and real index instance %v are not found",
					srcInstId, tgtInstId)
				logging.Errorf(err.Error())
				return err
			}

			return nil
		}

		err := fmt.Errorf("MergePartition: Index Instance %v not found", srcInstId)
		logging.Errorf(err.Error())
		return err
	}

	// Verify if it is a proxy.
	if inst.IsProxy() {
		if tgtInstId != inst.RealInstId {
			err := fmt.Errorf("MergePartition: Real index Instance in transer token %v does not match proxy real index instance (%v != %v)",
				srcInstId, tgtInstId, inst.RealInstId)
			logging.Errorf(err.Error())
			return err
		}

		// Find the real index
		if _, ok := idx.indexInstMap[tgtInstId]; !ok {
			err := fmt.Errorf("MergePartition: Real index Instance not found %v", tgtInstId)
			logging.Errorf(err.Error())
			return err
		}
	}

	return nil
}

//
// This function merge the partitions from a source index instance to a target index instance.
// Prior to this point, the source index instance has been treated as an independent index instance.
//
// This function should be called when indexer is in quiescent point, e.g. when flush is done.
//
// This function is idempotent.  For example, after the source index instance is merged to the
// target, the indexer crash before the source index instance is cleaned up.   This function can
// be re-run.
//
// This function has one of the possible outcomes:
// 1) The source index is successfully merged to the target.
//    - source instance is still REBAL_MERGED state and it could be deleted
//    - target instance has the new partition.  It may be in ACTIVE or PENDING state.
// 2) The merge is skipped (e.g. source index or target is deleted)
// 3) The merge is delayed (e.g. target index is not ready to merge)
// 4) An error is returned through respch.   This means that the merge
//    may be in progress, but it has not yet committed yet.   The
//    indexer can be in an inconsistent state and needs restart.
// 3) If there is any transient error during commit or after commit,
//    the indexer can panic.
//
func (idx *indexer) mergePartitions(bucket string) {

	// Do not merge when indexer is not active
	if is := idx.getIndexerState(); is != common.INDEXER_ACTIVE {
		return
	}

	// nothing to merge
	if len(idx.mergePartitionList) == 0 {
		return
	}

	logging.Infof("MergePartitions: bucket %v", bucket)

	var remaining []mergeSpec
	if len(idx.mergePartitionList) != 0 {
		remaining = make([]mergeSpec, 0, len(idx.mergePartitionList))
	}

	for _, spec := range idx.mergePartitionList {

		sourceId := spec.srcInstId
		targetId := spec.tgtInstId
		respch := spec.respch

		if !idx.mergePartition(bucket, sourceId, targetId, idx.merged, respch) {
			remaining = append(remaining, spec)
		}
	}

	idx.mergePartitionList = remaining

	idx.updateStreamForRebalance(false)
}

func (idx *indexer) removeMergedIndexesFromStream(merged map[common.IndexInstId]common.IndexInst) {

	sorted := make(map[string]map[string]map[common.StreamId]map[common.IndexState][]common.IndexInst)
	for _, inst := range merged {
		if _, ok := sorted[inst.Defn.Bucket]; !ok {
			sorted[inst.Defn.Bucket] = make(map[string]map[common.StreamId]map[common.IndexState][]common.IndexInst)
		}

		if _, ok := sorted[inst.Defn.Bucket][inst.Defn.BucketUUID]; !ok {
			sorted[inst.Defn.Bucket][inst.Defn.BucketUUID] = make(map[common.StreamId]map[common.IndexState][]common.IndexInst)
		}

		if _, ok := sorted[inst.Defn.Bucket][inst.Defn.BucketUUID][inst.Stream]; !ok {
			sorted[inst.Defn.Bucket][inst.Defn.BucketUUID][inst.Stream] = make(map[common.IndexState][]common.IndexInst)
		}

		sorted[inst.Defn.Bucket][inst.Defn.BucketUUID][inst.Stream][inst.State] =
			append(sorted[inst.Defn.Bucket][inst.Defn.BucketUUID][inst.Stream][inst.State], inst)
	}

	for bucket, list1 := range sorted {
		for bucketUUID, list2 := range list1 {
			for streamId, list3 := range list2 {
				for state, list4 := range list3 {
					// removeIndexesFromStream() will send update to projector.  This function spawn a go-routine after
					// acquiring the stream lock.   The stream lock ensure the projector update is executed on serial fashion.
					// Therefore, if mergePartitions() is called multiple times, the projector update will be executed serially.
					idx.removeIndexesFromStream(list4, bucket, bucketUUID, streamId, state, nil)
				}
			}
		}
	}
}

func (idx *indexer) mergePartition(bucket string, sourceId common.IndexInstId, targetId common.IndexInstId,
	merged map[common.IndexInstId]common.IndexInst, respch chan error) bool {

	if source, ok := idx.indexInstMap[sourceId]; ok {

		logging.Infof("MergePartition: Merge instance %v to instance %v", source.InstId, targetId)

		// Only merge partition from the given bucket
		if source.Defn.Bucket != bucket {
			logging.Warnf("MergePartition: Source Index Instance %v is not in bucket %v.  Do not merge now.",
				source.InstId, bucket)
			return false
		}

		// Only merge when source is in MAINT_STREAM or NIL_STREAM (deferred index)
		if source.Stream != common.MAINT_STREAM && source.Stream != common.NIL_STREAM {
			logging.Warnf("MergePartition: Source Index Instance %v is not in MAINT_STREAM or NIL_STREAM.  Do not merge now.",
				source.InstId)
			return false
		}

		// The index has been explicitly dropped before merge can happen.
		if source.State == common.INDEX_STATE_DELETED {
			logging.Warnf("MergePartition: Source Index Instance %v is in DELETED state.  Nothinge to merge.", source.InstId)
			if respch != nil {
				respch <- error(nil)
			}
			return true
		}

		// The source index cannot be in REBAL_ACTIVE state.
		if source.RState == common.REBAL_ACTIVE {
			err := fmt.Errorf("Source Index Instance %v is in REBAL_ACTIVE state.", source.InstId)
			logging.Errorf("Merge Partition: %v", err)
			if respch != nil {
				respch <- err
			}
			return true
		}

		// The source index cannot be in REBAL_MERGED state.
		if source.RState == common.REBAL_MERGED || source.RState == common.REBAL_PENDING_DELETE {
			err := fmt.Errorf("Source Index Instance %v is in REBAL_MERGED or REBAL_PENDING_DELETE state.", source.InstId)
			logging.Errorf("Merge Partition: %v", err)
			if respch != nil {
				respch <- err
			}
			return true
		}

		// Source index must be in CREATED (deferred index) or ACTIVE state
		if source.State != common.INDEX_STATE_CREATED && source.State != common.INDEX_STATE_ACTIVE {
			logging.Warnf("MergePartition: Source Index Instance %v is not in CREATED or ACTIVE state (%v).  Do not merge now.",
				source.InstId, source.State)
			return false
		}

		// Do not merge if recovery is going on.
		if source.Stream != common.NIL_STREAM && idx.getStreamBucketState(source.Stream, source.Defn.Bucket) != STREAM_ACTIVE {
			logging.Warnf("MergePartition: Source Index Instance %v with bucket stream in recovery.  Do not merge now.", source.InstId)
			return false
		}

		// The target instance can either be
		// 1) An index instance created due to rebalance
		// 2) An index instance that is already residing on this node prior to rebalance
		if target, ok := idx.indexInstMap[targetId]; ok {

			if source.InstId == target.InstId {
				logging.Warnf("MergePartition: Source Index Instance %v and target index instance is the same.  Nothinge to merge.", source.InstId)
				if respch != nil {
					respch <- error(nil)
				}
				return true
			}

			if source.Defn.DefnId != target.Defn.DefnId {
				err := fmt.Errorf("Source Index Instance %v and target index instance %v have different definition (%v != %v)",
					source.InstId, target.InstId, source.Defn.DefnId, target.Defn.DefnId)
				logging.Errorf("Merge Partition: %v", err)
				if respch != nil {
					respch <- err
				}
				return true
			}

			// The index has been explicitly dropped before merge can happen.
			if target.State == common.INDEX_STATE_DELETED {
				logging.Warnf("MergePartition: Target Index Instance %v is in DELETED state.  Remove target index instance %v.",
					target.InstId, source.InstId)
				idx.cleanupIndexMetadata(source)
				idx.cleanupIndex(source, nil)
				if respch != nil {
					respch <- error(nil)
				}
				return true
			}

			// The target has to be in REBAL_ACTIVE.   This is to ensure that onece it is merged, the target will not be removed
			// by rebalancer clean up.  Once merged, the transfer token is moved to Ready or committed state.   The original index
			// will be deleted.
			if target.RState != common.REBAL_ACTIVE {
				logging.Warnf("Merge Partition: Target Index Instance %v is not in REBAL_ACTIVE. Do not merge now.", target.InstId)
				return false
			}

			// The target may become ACTIVE before the source
			if target.State != common.INDEX_STATE_CREATED && target.State != common.INDEX_STATE_ACTIVE {
				logging.Warnf("MergePartition: Target Index Instance %v is not in CREATED or ACTIVE state (%v).  Do not merge now.",
					target.InstId, target.State)
				return false
			}

			// This is to check against merging a deferred index (before build) into an active index, or vice versa.
			if source.State != target.State {
				err := fmt.Errorf("Source Index Instance %v and target index instance %v does not have the same state (%v != %v)",
					source.InstId, target.InstId, source.State, target.State)
				logging.Errorf("Merge Partition: %v", err)
				if respch != nil {
					respch <- err
				}
				return true
			}

			// The source and target must be on the same stream.
			if source.Stream != target.Stream {
				logging.Warnf("MergePartition: Source Index Instance stream %v and target index instance stream %v are not on the same.  Do not merge now.",
					target.Stream, source.Stream)
				return false
			}

			// Merge Partitions in runtime data structures:
			// 1) index instance partition container
			// 2) indexer partition map
			// 3) index instance partition stats
			partitions := source.Pc.GetAllPartitions()
			partnIds := make([]common.PartitionId, 0, len(partitions))
			versions := make([]int, 0, len(partitions))
			for _, partnDef := range partitions {
				partnId := partnDef.GetPartitionId()
				version := partnDef.GetVersion()

				// Do not merge if the target index inst has this partition
				if target.Pc.GetPartitionById(partnId) == nil {
					// Add partiton to instance definition
					target.Pc.AddPartition(partnId, partnDef)

					// Add partiition to partition map
					idx.indexPartnMap[target.InstId][partnId] = idx.indexPartnMap[source.InstId][partnId]

					// Add to stats
					if stats := idx.stats.GetPartitionStats(source.InstId, partnId); stats != nil {
						idx.stats.SetPartitionStats(target.InstId, partnId, stats)
					}

					partnIds = append(partnIds, partnId)
					versions = append(versions, version)

				} else {
					err := fmt.Errorf("Duplicate partition %v found when merging from source instance %v to target instance %v.",
						partnId, source.InstId, target.InstId)
					logging.Errorf("Merge Partition: %v", err)
					if respch != nil {
						respch <- err
					}
					return true
				}
			}

			// Merge partitions in storage manager snapshot.  This must be done before metadata is updated.
			// This is to make sure that once the metadata is published to client, scan will not fail since
			// client may see the new partition list from metadata.   Once this operation is successful,
			idx.storageMgrCmdCh <- &MsgIndexMergeSnapshot{
				srcInstId:  source.InstId,
				tgtInstId:  target.InstId,
				partitions: partnIds,
			}
			if resp := <-idx.storageMgrCmdCh; resp.GetMsgType() != MSG_SUCCESS {
				respErr := resp.(*MsgError).GetError()
				if respch != nil {
					respch <- respErr.cause
				}
				return true
			}

			// At this point, we are going to commit the metadata change.   Once past this point, the indexer
			// can no longer send any error to the respch.   The indexer may crash to let recovery code to kick in.
			target.Version = source.Version
			idx.indexInstMap[targetId] = target

			// Metadata update is atomic, for both the source instance and target instance.
			// 1) The source instance will be deleted in metadata.
			// 2) The target instance will be updated with new partitions and versions
			clustMgrRespch := make(chan error)
			msg := &MsgClustMgrMergePartition{
				defnId:         source.Defn.DefnId,
				srcInstId:      source.InstId,
				srcRState:      common.REBAL_MERGED,
				tgtInstId:      target.InstId,
				tgtPartitions:  partnIds,
				tgtVersions:    versions,
				tgtInstVersion: uint64(target.Version),
				respch:         clustMgrRespch,
			}
			if err := idx.sendMsgToClusterMgr(msg); err != nil {
				common.CrashOnError(err)
			}

			go func() {

				// The metadata commit is done asynchronously to avoid deadlock.  If metadata update fails,
				// indexer will restart so it can restore to a consistent state.
				err := <-clustMgrRespch
				if err != nil {
					common.CrashOnError(err)
				}

				logging.Infof("MergePartition: instance %v merged to instance %v", source.InstId, targetId)

				// Once metadata is committed, reply to the rebalancer.  This is the point where both data
				// and metadata has been moved.
				if respch != nil {
					respch <- error(nil)
				}
			}()

			if idx.lastStreamUpdate == 0 {
				idx.lastStreamUpdate = time.Now().UnixNano()
			}

			// Cleanup the source instance. This is done in parallel clean up will proceed in parallel to metadata commit.
			// Cleanup comprise of removing the source instance from indexer runtime data structure, as well as removing
			// the instance from stream.  These are transient states that can be recovered upon bootstrap if metadata commit
			// fails.
			idx.cleanupIndexAfterMerge(source, merged)

		} else {
			// preValidateMergePartition must already been called prior to this.
			// This means the source inst or the target inst must have existed before.
			// If we do not find the real inst now, it could mean that the target inst might
			// have been dropped explicitly.  In this case, skip the merge.
			logging.Warnf("MergePartition.  Target instance %v not found. Remove source %v.", source.RealInstId, source.InstId)
			idx.cleanupIndexMetadata(source)
			idx.cleanupIndex(source, nil)

			if respch != nil {
				respch <- error(nil)
			}
		}

	} else {
		// preValidateMergePartition must already been called prior to this.
		// This means the source inst or the target inst must have existed before.
		// If we do not find the source inst now, it could mean that the source might
		// have been dropped explicitly.  In this case, skip the merge.
		logging.Warnf("MergePartition.  Source instance %v not found. Skip", sourceId)

		if respch != nil {
			respch <- error(nil)
		}
	}

	return true
}

//
// Clean up index instance without removing the data.
// Note that the source instance is already marked as DELETED in metadata
// (through MsgClustMgrMergePartition).
//
func (idx *indexer) cleanupIndexAfterMerge(inst common.IndexInst, merged map[common.IndexInstId]common.IndexInst) {

	// remove stream if index is active.  For deferred index, index state would not be active (CREATED).
	if inst.State == common.INDEX_STATE_ACTIVE ||
		inst.State == common.INDEX_STATE_CATCHUP ||
		inst.State == common.INDEX_STATE_INITIAL {
		merged[inst.InstId] = inst
	}

	inst.State = common.INDEX_STATE_DELETED
	idx.indexInstMap[inst.InstId] = inst

	// Remove the inst
	delete(idx.indexInstMap, inst.InstId)
	delete(idx.indexPartnMap, inst.InstId)

	// remove stats
	idx.stats.RemoveIndex(inst.InstId)

	// Update index maps with this index
	msgUpdateIndexInstMap := idx.newIndexInstMsg(idx.indexInstMap)
	msgUpdateIndexPartnMap := &MsgUpdatePartnMap{indexPartnMap: idx.indexPartnMap}
	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, msgUpdateIndexPartnMap); err != nil {
		common.CrashOnError(err)
	}
}

//
// Prune Partition.
//
func (idx *indexer) handlePrunePartition(msg Message) {

	instId := msg.(*MsgClustMgrPrunePartition).GetInstId()
	partitions := msg.(*MsgClustMgrPrunePartition).GetPartitions()
	respch := msg.(*MsgClustMgrPrunePartition).GetRespCh()

	if inst, ok := idx.indexInstMap[instId]; ok {

		spec := pruneSpec{
			instId:     instId,
			partitions: partitions,
		}

		idx.prunePartitionList = append(idx.prunePartitionList, spec)

		if ok, _ := idx.streamBucketFlushInProgress[common.MAINT_STREAM][inst.Defn.Bucket]; !ok {
			idx.prunePartitions(inst.Defn.Bucket)
		}
	} else {
		logging.Warnf("PrunePartition.  Index instance %v not found. Skip", instId)
	}

	respch <- &MsgSuccess{}
}

func (idx *indexer) prunePartitions(bucket string) {

	// Do not merge when indexer is not active
	if is := idx.getIndexerState(); is != common.INDEXER_ACTIVE {
		if len(idx.prunePartitionList) != 0 {
			logging.Warnf("PrunePartition.  Indexer inactive.  Cannot prune instance.")
		}
		return
	}

	// nothing to prune
	if len(idx.prunePartitionList) == 0 {
		return
	}

	logging.Infof("PrunePartitions: bucket %v", bucket)

	var remaining []pruneSpec
	if len(idx.prunePartitionList) != 0 {
		remaining = make([]pruneSpec, 0, len(idx.prunePartitionList))
	}

	for _, spec := range idx.prunePartitionList {

		instId := spec.instId
		partitions := spec.partitions

		if !idx.prunePartition(bucket, instId, partitions, idx.pruned) {
			remaining = append(remaining, spec)
		}
	}

	idx.prunePartitionList = remaining

	idx.updateStreamForRebalance(false)
}

func (idx *indexer) removePrunedIndexesFromStream(pruned map[common.IndexInstId]common.IndexInst) {

	sorted := make(map[string]map[string]map[common.StreamId][]common.IndexInst)
	for instId, _ := range pruned {
		inst, ok := idx.indexInstMap[instId]
		if !ok {
			logging.Warnf("removePrunedIndexesFromStream:  inst %v does not exist in indexInstMap. Skip stream update.", instId)
			continue
		}

		if _, ok := sorted[inst.Defn.Bucket]; !ok {
			sorted[inst.Defn.Bucket] = make(map[string]map[common.StreamId][]common.IndexInst)
		}

		if _, ok := sorted[inst.Defn.Bucket][inst.Defn.BucketUUID]; !ok {
			sorted[inst.Defn.Bucket][inst.Defn.BucketUUID] = make(map[common.StreamId][]common.IndexInst)
		}

		sorted[inst.Defn.Bucket][inst.Defn.BucketUUID][inst.Stream] =
			append(sorted[inst.Defn.Bucket][inst.Defn.BucketUUID][inst.Stream], inst)
	}

	for bucket, list1 := range sorted {
		for bucketUUID, list2 := range list1 {
			for streamId, list3 := range list2 {
				// sendStreamUpdateForIndex() will send update to projector.  This function spawn a go-routine after
				// acquiring the stream lock.   The stream lock ensure the projector update is executed on serial fashion.
				// Therefore, if prunePartitions() is called multiple times, the projector update will be executed serially.
				// This is important since this operation requiring the indexer sends a copy of index inst to the projector.
				// If it executes out-of-sequence, then we cannot assure that the projector will eventually receive the most
				// up-to-date index instance.
				//
				// sendstreamUpdateForIndex will only retry for 10 times before it aborts.   If the indexer fails to update
				// the projector, at worst, the projector will send more mutations to the indexer, but it will not skip mutation.
				idx.sendStreamUpdateForIndex(list3, bucket, bucketUUID, streamId)
			}
		}
	}
}

//
// Remove partitions from runtime data structure.  This function is idempotent.
// This function will not remove the slices from the partition.  Those pruned partitions
// are put into a proxy partition with DELETED state, and they will be periodically clean up
// asynchronously.
//
func (idx *indexer) prunePartition(bucket string, instId common.IndexInstId, partitions []common.PartitionId,
	prunedInst map[common.IndexInstId]common.IndexInst) bool {

	if inst, ok := idx.indexInstMap[instId]; ok {

		logging.Infof("PrunePartition: Prune instance %v partitions %v", instId, partitions)

		// Only prune partition from the given bucket
		if inst.Defn.Bucket != bucket {
			logging.Warnf("PurnePartition: Index Instance %v is not in bucket %v.  Do not prune now.",
				inst.InstId, bucket)
			return false
		}

		// Only prune when source is in MAINT_STREAM or NIL_STREAM (deferred index)
		// A index instance is pruned when some of its partition has moved during rebalance.   Rebalance can happen if
		// there is no index build (no index on INIT_STREAM).
		if inst.Stream != common.MAINT_STREAM && inst.Stream != common.NIL_STREAM {
			logging.Warnf("PrunePartition: Index Instance %v is not in MAINT_STREAM or NIL_STREAM.  Do not prune now.",
				inst.InstId)
			return false
		}

		// The index has been explicitly dropped before prune can happen.
		if inst.State == common.INDEX_STATE_DELETED {
			logging.Warnf("PrunePartition:  Index Instance %v is in DELETED state.  Nothinge to prune.", instId)
			return true
		}

		// Do not prune if recovery is going on.
		if inst.Stream != common.NIL_STREAM && idx.getStreamBucketState(inst.Stream, inst.Defn.Bucket) != STREAM_ACTIVE {
			logging.Warnf("PrunePartition: Index Instance %v with bucket stream in recovery.  Do not prune now.", inst.InstId)
			return false
		}

		if inst.RState == common.REBAL_MERGED || inst.RState == common.REBAL_PENDING_DELETE {
			logging.Warnf("PrunePartition:  Index Instance %v is in REBAL_MERGED or REBAL_DELETED or REBAL_PENDING_DELETE state.  Nothinge to prune.", instId)
			return true
		}

		// Prune Partitions from runtime structure
		// 1) index instance partition container
		// 2) indexer partition map
		// 3) index instance partition stats
		pruned := make([]PartitionInst, 0, len(partitions))
		for _, partnId := range partitions {

			partition := inst.Pc.GetPartitionById(partnId)
			if partition != nil {
				// Remove partiton from instance definition
				inst.Pc.RemovePartition(partnId)

				// Remove partiition from partition map
				pruned = append(pruned, idx.indexPartnMap[inst.InstId][partnId])
				delete(idx.indexPartnMap[inst.InstId], partnId)

				// Remove stats
				idx.stats.RemovePartitionStats(inst.InstId, partnId)

			} else {
				logging.Warnf("PrunePartition.  Index instance %v does not have partition %v. Skip", inst.InstId, partnId)
			}
		}

		if len(pruned) != 0 {

			idx.indexInstMap[instId] = inst

			// Prune partitions in storage manager snapshot.  This must be done before metadata is updated.
			// This is to make sure that once the metadata is published to client, scan will not fail since
			// client may see the new partition list from metadata.
			idx.storageMgrCmdCh <- &MsgIndexPruneSnapshot{
				instId:     inst.InstId,
				partitions: partitions,
			}
			if resp := <-idx.storageMgrCmdCh; resp.GetMsgType() != MSG_SUCCESS {
				respErr := resp.(*MsgError).GetError()
				logging.Errorf("PrunePartition.  Fail to prune index snapshot for index %v. Cause: %v", inst.InstId, respErr.cause)
				common.CrashOnError(respErr.cause)
			}

			// Update index maps with this index
			msgUpdateIndexInstMap := idx.newIndexInstMsg(idx.indexInstMap)
			msgUpdateIndexPartnMap := &MsgUpdatePartnMap{indexPartnMap: idx.indexPartnMap}
			if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, msgUpdateIndexPartnMap); err != nil {
				common.CrashOnError(err)
			}

			// Soft delete the slice
			for _, partnInst := range pruned {
				//close all the slices
				for _, slice := range partnInst.Sc.GetAllSlices() {
					go func(partnInst PartitionInst, slice Slice) {
						slice.Close()
						//wipe the physical files
						slice.Destroy()
						logging.Infof("Prune Partiiton: destroy slice inst %v partn %v path %v",
							slice.IndexInstId(), partnInst.Defn.GetPartitionId(), slice.Path())
					}(partnInst, slice)
				}
			}

			if idx.lastStreamUpdate == 0 {
				idx.lastStreamUpdate = time.Now().UnixNano()
			}

			// If we are on the MAINT stream, then remove the old partitions from projector.
			// For a single invocation of prunePartitions(), an inst can be pruned multiple times.
			// This map will store the last copy of the inst after all the pruning.  The indexer
			// will then send the final copy to the projector.    Note that the prunePartitions()
			// can be invoked many times, and each invocation can be pruning the same instance.
			// In this case, indexer will make multiple calls to the projector.
			if inst.Stream == common.MAINT_STREAM {
				prunedInst[inst.InstId] = inst
			}
		}

	} else {
		logging.Warnf("PrunePartition.  Index instance %v not found. Skip", instId)
	}

	return true
}

func (idx *indexer) updateStreamForRebalance(force bool) {

	interval := int64(idx.config["rebalance.stream_update.interval"].Int()) * int64(time.Second)

	if force || (time.Now().UnixNano()-idx.lastStreamUpdate) > interval {

		if len(idx.merged) != 0 {
			logging.Infof("MergePartitions: number of instances merged: %v", len(idx.merged))
			idx.removeMergedIndexesFromStream(idx.merged)
			idx.merged = make(map[common.IndexInstId]common.IndexInst)
		}

		if len(idx.pruned) != 0 {
			logging.Infof("updateStreamAfterRebalance: number of instances pruned : %v", len(idx.pruned))
			idx.removePrunedIndexesFromStream(idx.pruned)
			idx.pruned = make(map[common.IndexInstId]common.IndexInst)
		}

		idx.lastStreamUpdate = 0
	}
}

func (idx *indexer) handleBuildIndex(msg Message) {

	instIdList := msg.(*MsgBuildIndex).GetIndexList()
	clientCh := msg.(*MsgBuildIndex).GetRespCh()

	logging.Infof("Indexer::handleBuildIndex %v", instIdList)

	// NOTE
	// If this function adds new validation or changes error message, need
	// to update lifecycle mgr and ddl service mgr.
	//

	if len(instIdList) == 0 {
		logging.Warnf("Indexer::handleBuildIndex Nothing To Build")
		if clientCh != nil {
			clientCh <- &MsgSuccess{}
		}
	}

	is := idx.getIndexerState()
	if is != common.INDEXER_ACTIVE {

		errStr := fmt.Sprintf("Indexer Cannot Process Build Index In %v State", is)
		logging.Errorf("Indexer::handleBuildIndex %v", errStr)

		if clientCh != nil {
			clientCh <- &MsgError{
				err: Error{code: ERROR_INDEXER_NOT_ACTIVE,
					severity: FATAL,
					cause:    errors.New(errStr),
					category: INDEXER}}

		}
		return
	}

	if idx.rebalanceRunning || idx.rebalanceToken != nil {

		reqCtx := msg.(*MsgBuildIndex).GetRequestCtx()

		if reqCtx != nil && reqCtx.ReqSource == common.DDLRequestSourceUser {

			errStr := fmt.Sprintf("Indexer Cannot Process Build Index - Rebalance In Progress")
			logging.Errorf("Indexer::handleBuildIndex %v", errStr)

			if clientCh != nil {
				clientCh <- &MsgError{
					err: Error{code: ERROR_INDEXER_REBALANCE_IN_PROGRESS,
						severity: FATAL,
						cause:    errors.New(errStr),
						category: INDEXER}}

			}
			return
		}

	}

	bucketIndexList := idx.groupIndexListByBucket(instIdList)
	errMap := make(map[common.IndexInstId]error)

	initialBuildReqd := true
	for bucket, instIdList := range bucketIndexList {

		instIdList, ok := idx.checkValidIndexInst(bucket, instIdList, clientCh, errMap)
		if !ok {
			logging.Errorf("Indexer::handleBuildIndex \n\tInvalid Index List "+
				"Bucket %v. Index in error %v.", bucket, errMap)
			if idx.enableManager {
				if len(instIdList) == 0 {
					delete(bucketIndexList, bucket)
					continue
				}
			} else {
				return
			}
		}

		instIdList, ok = idx.checkBucketExists(bucket, instIdList, clientCh, errMap)
		if !ok {
			logging.Errorf("Indexer::handleBuildIndex \n\tCannot Process Build Index."+
				"Unknown Bucket %v.", bucket)
			if idx.enableManager {
				if len(instIdList) == 0 {
					delete(bucketIndexList, bucket)
					continue
				}
			} else {
				return
			}
		} else {
			logging.Infof("Indexer::handleBuildIndex Bucket %v validation successful", bucket)
		}

		if ok := idx.checkBucketInRecovery(bucket, instIdList, clientCh, errMap); ok {
			logging.Errorf("Indexer::handleBuildIndex \n\tCannot Process Build Index "+
				"In Recovery Mode. Bucket %v. Index In Error %v", bucket, errMap)
			if idx.enableManager {
				delete(bucketIndexList, bucket)
				continue
			} else {
				return
			}
		}

		//check if Initial Build is already running for this index's bucket
		if ok := idx.checkDuplicateInitialBuildRequest(bucket, instIdList, clientCh, errMap); !ok {
			logging.Errorf("Indexer::handleBuildIndex \n\tBuild Already In"+
				"Progress. Bucket %v. Index in error %v", bucket, errMap)
			if idx.enableManager {
				delete(bucketIndexList, bucket)
				continue
			} else {
				return
			}
		}

		cluster := idx.config["clusterAddr"].String()
		numVbuckets := idx.config["numVbuckets"].Int()
		buildTs, err := GetCurrentKVTs(cluster, "default", bucket, numVbuckets)
		if err != nil {
			errStr := fmt.Sprintf("Error Connecting KV %v Err %v",
				idx.config["clusterAddr"].String(), err)
			logging.Errorf("Indexer::handleBuildIndex %v", errStr)
			if idx.enableManager {
				idx.bulkUpdateError(instIdList, errStr)
				for _, instId := range instIdList {
					errMap[instId] = &common.IndexerError{Reason: errStr, Code: common.TransientError}
				}
				delete(bucketIndexList, bucket)
				continue
			} else if clientCh != nil {
				clientCh <- &MsgError{
					err: Error{code: ERROR_INDEXER_IN_RECOVERY,
						severity: FATAL,
						cause:    errors.New(errStr),
						category: INDEXER}}
				return
			}
		}

		if len(instIdList) != 0 {
			bucketIndexList[bucket] = instIdList
		} else {
			delete(bucketIndexList, bucket)
			continue
		}

		//if there is already an index for this bucket in MAINT_STREAM,
		//add this index to INIT_STREAM
		var buildStream common.StreamId
		if idx.checkBucketExistsInStream(bucket, common.MAINT_STREAM, false) {
			buildStream = common.INIT_STREAM
		} else {
			buildStream = common.MAINT_STREAM
		}

		idx.bulkUpdateStream(instIdList, buildStream)

		//if initial build TS is zero and index belongs to MAINT_STREAM
		//initial build is not required.
		var buildState common.IndexState
		if buildTs.IsZeroTs() && buildStream == common.MAINT_STREAM {
			initialBuildReqd = false
		}
		//always set state to Initial, once stream request/build is done,
		//this will get changed to active
		buildState = common.INDEX_STATE_INITIAL

		idx.bulkUpdateState(instIdList, buildState)
		idx.bulkUpdateRState(instIdList, msg.(*MsgBuildIndex).GetRequestCtx())

		logging.Infof("Indexer::handleBuildIndex \n\tAdded Index: %v to Stream: %v State: %v",
			instIdList, buildStream, buildState)

		msgUpdateIndexInstMap := idx.newIndexInstMsg(idx.indexInstMap)

		if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, nil); err != nil {
			if clientCh != nil {
				clientCh <- &MsgError{
					err: Error{code: ERROR_INDEXER_INTERNAL_ERROR,
						severity: FATAL,
						cause:    err,
						category: INDEXER}}
			}
			common.CrashOnError(err)
		}

		//send Stream Update to workers
		idx.sendStreamUpdateForBuildIndex(instIdList, buildStream, bucket, buildTs, clientCh)

		idx.stateLock.Lock()
		if _, ok := idx.streamBucketStatus[buildStream]; !ok {
			idx.streamBucketStatus[buildStream] = make(BucketStatus)
		}
		idx.stateLock.Unlock()

		idx.setStreamBucketState(buildStream, bucket, STREAM_ACTIVE)

		//store updated state and streamId in meta store
		if idx.enableManager {
			if err := idx.updateMetaInfoForIndexList(instIdList, true,
				true, false, true, true, false, false, false, nil); err != nil {
				common.CrashOnError(err)
			}
		} else {

			//if initial build is not being done, send success response,
			//otherwise success response will be sent when initial build gets done
			if !initialBuildReqd {
				clientCh <- &MsgSuccess{}
			} else {
				idx.bucketCreateClientChMap[bucket] = clientCh
			}
			return
		}
	}

	if idx.enableManager {
		clientCh <- &MsgBuildIndexResponse{errMap: errMap}
	} else {
		clientCh <- &MsgSuccess{}
	}

}

//TODO handle panic, otherwise main loop will get shutdown
func (idx *indexer) handleDropIndex(msg Message) {

	indexInstId := msg.(*MsgDropIndex).GetIndexInstId()
	clientCh := msg.(*MsgDropIndex).GetResponseChannel()

	logging.Infof("Indexer::handleDropIndex - IndexInstId %v", indexInstId)

	var indexInst common.IndexInst
	var ok bool
	if indexInst, ok = idx.indexInstMap[indexInstId]; !ok {

		errStr := fmt.Sprintf("Unknown Index Instance %v", indexInstId)
		logging.Errorf("Indexer::handleDropIndex %v", errStr)

		if clientCh != nil {
			clientCh <- &MsgError{
				err: Error{code: ERROR_INDEXER_UNKNOWN_INDEX,
					severity: FATAL,
					cause:    errors.New(errStr),
					category: INDEXER}}
		}
		return
	}

	is := idx.getIndexerState()
	if is == common.INDEXER_PREPARE_UNPAUSE {
		logging.Errorf("Indexer::handleDropIndex Cannot Process DropIndex "+
			"In %v state", is)

		if clientCh != nil {
			clientCh <- &MsgError{
				err: Error{code: ERROR_INDEXER_NOT_ACTIVE,
					severity: FATAL,
					cause:    ErrIndexerNotActive,
					category: INDEXER}}

		}
		return
	}

	if idx.rebalanceRunning || idx.rebalanceToken != nil {

		reqCtx := msg.(*MsgDropIndex).GetRequestCtx()

		if reqCtx != nil && reqCtx.ReqSource == common.DDLRequestSourceUser {

			errStr := fmt.Sprintf("Indexer Cannot Process Drop Index - Rebalance In Progress")
			logging.Errorf("Indexer::handleDropIndex %v", errStr)

			if clientCh != nil {
				clientCh <- &MsgError{
					err: Error{code: ERROR_INDEXER_REBALANCE_IN_PROGRESS,
						severity: FATAL,
						cause:    errors.New(errStr),
						category: INDEXER}}

			}
			return
		}

	}

	idx.stats.RemoveIndex(indexInst.InstId)

	//if the index state is Created/Ready/Deleted, only data cleanup is
	//required. No stream updates are required.
	if indexInst.State == common.INDEX_STATE_CREATED ||
		indexInst.State == common.INDEX_STATE_READY ||
		indexInst.State == common.INDEX_STATE_DELETED {

		idx.cleanupIndexData(indexInst, clientCh)
		logging.Infof("Indexer::handleDropIndex Cleanup Successful for "+
			"Index Data %v", indexInst)
		clientCh <- &MsgSuccess{}
		return
	}

	//Drop is a two step process. First set the index state as DELETED.
	//Then all the workers are notified about this state change. If this
	//step is successful, no mutation/scan request for the index will be processed.
	//Second step, is the actual cleanup of index instance from internal maps
	//and purging of physical slice files.

	indexInst.State = common.INDEX_STATE_DELETED
	idx.indexInstMap[indexInst.InstId] = indexInst

	msgUpdateIndexInstMap := idx.newIndexInstMsg(idx.indexInstMap)

	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, nil); err != nil {
		clientCh <- &MsgError{
			err: Error{code: ERROR_INDEXER_INTERNAL_ERROR,
				severity: FATAL,
				cause:    err,
				category: INDEXER}}
		common.CrashOnError(err)
	}

	//if this is the last index for the bucket in MaintStream and the bucket exists
	//in InitStream, don't cleanup bucket from stream. It is needed for merge to
	//happen.
	if indexInst.Stream == common.MAINT_STREAM &&
		!idx.checkBucketExistsInStream(indexInst.Defn.Bucket, common.MAINT_STREAM, false) &&
		idx.checkBucketExistsInStream(indexInst.Defn.Bucket, common.INIT_STREAM, false) {
		logging.Infof("Indexer::handleDropIndex Pre-Catchup Index Found for %v "+
			"%v. Stream Cleanup Skipped.", indexInst.Stream, indexInst.Defn.Bucket)
		clientCh <- &MsgSuccess{}
		return
	}

	maintState := idx.getStreamBucketState(common.MAINT_STREAM, indexInst.Defn.Bucket)
	initState := idx.getStreamBucketState(common.INIT_STREAM, indexInst.Defn.Bucket)

	if maintState == STREAM_RECOVERY ||
		maintState == STREAM_PREPARE_RECOVERY ||
		initState == STREAM_RECOVERY ||
		initState == STREAM_PREPARE_RECOVERY {

		logging.Errorf("Indexer::handleDropIndex Cannot Process Drop Index " +
			"In Recovery Mode.")

		if clientCh != nil {
			clientCh <- &MsgError{
				err: Error{code: ERROR_INDEXER_IN_RECOVERY,
					severity: FATAL,
					cause:    ErrIndexerInRecovery,
					category: INDEXER}}

		}
		return
	}

	//check if there is already a drop request waiting on this bucket
	if ok := idx.checkDuplicateDropRequest(indexInst, clientCh); ok {
		return
	}

	//if there is a flush in progress for this index's bucket and stream
	//wait for the flush to finish before drop
	streamId := indexInst.Stream
	bucket := indexInst.Defn.Bucket

	if ok, _ := idx.streamBucketFlushInProgress[streamId][bucket]; ok {
		notifyCh := make(MsgChannel)
		idx.streamBucketObserveFlushDone[streamId][bucket] = notifyCh
		go idx.processDropAfterFlushDone(indexInst, notifyCh, clientCh)
	} else {
		idx.cleanupIndex(indexInst, clientCh)
	}

}

func (idx *indexer) handlePrepareRecovery(msg Message) {

	streamId := msg.(*MsgRecovery).GetStreamId()
	bucket := msg.(*MsgRecovery).GetBucket()

	logging.Infof("Indexer::handlePrepareRecovery StreamId %v Bucket %v",
		streamId, bucket)

	idx.stopBucketStream(streamId, bucket)

}

func (idx *indexer) handleInitPrepRecovery(msg Message) {

	bucket := msg.(*MsgRecovery).GetBucket()
	streamId := msg.(*MsgRecovery).GetStreamId()
	rollbackTs := msg.(*MsgRecovery).GetRestartTs()

	if rollbackTs != nil {
		if _, ok := idx.streamBucketRollbackTs[streamId]; ok {
			idx.streamBucketRollbackTs[streamId][bucket] = rollbackTs
		} else {
			bucketRollbackTs := make(BucketRollbackTs)
			bucketRollbackTs[bucket] = rollbackTs
			idx.streamBucketRollbackTs[streamId] = bucketRollbackTs
		}
	}

	idx.setStreamBucketState(streamId, bucket, STREAM_PREPARE_RECOVERY)

	logging.Infof("Indexer::handleInitPrepRecovery StreamId %v Bucket %v %v",
		streamId, bucket, STREAM_PREPARE_RECOVERY)

	//fwd the msg to timekeeper
	idx.tkCmdCh <- msg
	<-idx.tkCmdCh
}

func (idx *indexer) handlePrepareUnpause(msg Message) {

	logging.Infof("Indexer::handlePrepareUnpause %v", idx.getIndexerState())

	idx.tkCmdCh <- msg
	<-idx.tkCmdCh

}

func (idx *indexer) handleUnpause(msg Message) {

	logging.Infof("Indexer::handleUnpause %v", idx.getIndexerState())

	idx.doUnpause()

}

func (idx *indexer) handlePrepareDone(msg Message) {

	bucket := msg.(*MsgRecovery).GetBucket()
	streamId := msg.(*MsgRecovery).GetStreamId()

	logging.Infof("Indexer::handlePrepareDone StreamId %v Bucket %v",
		streamId, bucket)

	delete(idx.streamBucketRequestStopCh[streamId], bucket)

	//fwd the msg to timekeeper
	idx.tkCmdCh <- msg
	<-idx.tkCmdCh

}

func (idx *indexer) handleInitRecovery(msg Message) {

	streamId := msg.(*MsgRecovery).GetStreamId()
	bucket := msg.(*MsgRecovery).GetBucket()
	restartTs := msg.(*MsgRecovery).GetRestartTs()

	idx.setStreamBucketState(streamId, bucket, STREAM_RECOVERY)

	logging.Infof("Indexer::handleInitRecovery StreamId %v Bucket %v %v",
		streamId, bucket, STREAM_RECOVERY)

	//if there is a rollbackTs, process rollback
	if ts, ok := idx.streamBucketRollbackTs[streamId][bucket]; ok && ts != nil {
		restartTs, err := idx.processRollback(streamId, bucket, ts)
		if err != nil {
			common.CrashOnError(err)
		}
		idx.startBucketStream(streamId, bucket, restartTs)

		go idx.collectProgressStats(true)
	} else {
		idx.startBucketStream(streamId, bucket, restartTs)
	}

}

func (idx *indexer) handleRecoveryDone(msg Message) {

	bucket := msg.(*MsgRecovery).GetBucket()
	streamId := msg.(*MsgRecovery).GetStreamId()
	buildTs := msg.(*MsgRecovery).GetBuildTs()

	logging.Infof("Indexer::handleRecoveryDone StreamId %v Bucket %v ",
		streamId, bucket)

	//send the msg to timekeeper
	idx.tkCmdCh <- msg
	<-idx.tkCmdCh

	delete(idx.streamBucketRequestStopCh[streamId], bucket)
	delete(idx.streamBucketRollbackTs[streamId], bucket)

	idx.bucketBuildTs[bucket] = buildTs

	//during recovery, if all indexes of a bucket gets dropped,
	//the stream needs to be stopped for that bucket.
	if !idx.checkBucketExistsInStream(bucket, streamId, false) {
		if idx.getStreamBucketState(streamId, bucket) != STREAM_INACTIVE {
			logging.Infof("Indexer::handleRecoveryDone StreamId %v Bucket %v State %v. No Index Found."+
				"Cleaning up.", streamId, bucket, idx.getStreamBucketState(streamId, bucket))
			idx.stopBucketStream(streamId, bucket)

			idx.setStreamBucketState(streamId, bucket, STREAM_INACTIVE)
		}
	} else {
		//change status to Active
		idx.setStreamBucketState(streamId, bucket, STREAM_ACTIVE)
	}

	logging.Infof("Indexer::handleRecoveryDone StreamId %v Bucket %v %v",
		streamId, bucket, idx.getStreamBucketState(streamId, bucket))

}

func (idx *indexer) handleKVStreamRepair(msg Message) {

	bucket := msg.(*MsgKVStreamRepair).GetBucket()
	streamId := msg.(*MsgKVStreamRepair).GetStreamId()
	restartTs := msg.(*MsgKVStreamRepair).GetRestartTs()

	is := idx.getIndexerState()
	if is == common.INDEXER_PREPARE_UNPAUSE {
		logging.Warnf("Indexer::handleKVStreamRepair Skipped Repair "+
			"In %v state", is)
		return
	}

	//repair is not required for inactive bucket streams
	if idx.getStreamBucketState(streamId, bucket) == STREAM_INACTIVE {
		logging.Infof("Indexer::handleKVStreamRepair Skip Stream Repair %v Inactive Bucket %v",
			streamId, bucket)
		return
	}

	//if there is already a repair in progress for this bucket stream
	//ignore the request
	if idx.checkStreamRequestPending(streamId, bucket) == false {
		logging.Infof("Indexer::handleKVStreamRepair Initiate Stream Repair %v Bucket %v",
			streamId, bucket)
		idx.startBucketStream(streamId, bucket, restartTs)
	} else {
		logging.Infof("Indexer::handleKVStreamRepair Ignore Stream Repair Request for Stream "+
			"%v Bucket %v. Request In Progress.", streamId, bucket)
	}

}

func (idx *indexer) handleInitBuildDoneAck(msg Message) {

	streamId := msg.(*MsgTKInitBuildDone).GetStreamId()
	bucket := msg.(*MsgTKInitBuildDone).GetBucket()

	//skip processing initial build done ack for inactive or recovery streams.
	//the streams would be restarted and then build done would get recomputed.
	state := idx.getStreamBucketState(streamId, bucket)

	if state == STREAM_INACTIVE ||
		state == STREAM_PREPARE_RECOVERY ||
		state == STREAM_RECOVERY {
		logging.Infof("Indexer::handleInitBuildDoneAck Skip InitBuildDoneAck %v %v %v",
			streamId, bucket, state)
		return
	}

	logging.Infof("Indexer::handleInitBuildDoneAck StreamId %v Bucket %v",
		streamId, bucket)

	switch streamId {

	case common.INIT_STREAM:

		//send the ack to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

	default:
		logging.Fatalf("Indexer::handleInitBuildDoneAck Unexpected Initial Build Ack Done "+
			"Received for Stream %v Bucket %v", streamId, bucket)
		common.CrashOnError(errors.New("Unexpected Initial Build Ack Done"))
	}

}

func (idx *indexer) handleMergeStreamAck(msg Message) {

	streamId := msg.(*MsgTKMergeStream).GetStreamId()
	bucket := msg.(*MsgTKMergeStream).GetBucket()
	mergeList := msg.(*MsgTKMergeStream).GetMergeList()

	logging.Infof("Indexer::handleMergeStreamAck StreamId %v Bucket %v",
		streamId, bucket)

	switch streamId {

	case common.INIT_STREAM:
		delete(idx.streamBucketRequestStopCh[streamId], bucket)

		state := idx.getStreamBucketState(streamId, bucket)

		//skip processing merge ack for inactive or recovery streams.
		if state == STREAM_PREPARE_RECOVERY ||
			state == STREAM_RECOVERY {
			logging.Infof("Indexer::handleMergeStreamAck Skip MergeStreamAck %v %v %v",
				streamId, bucket, state)
			idx.bucketStreamMergeInProgress[bucket] = false
			return
		}

		idx.setStreamBucketState(streamId, bucket, STREAM_INACTIVE)

		//enable flush for this bucket in MAINT_STREAM
		//TODO shall this be moved to timekeeper now?
		idx.tkCmdCh <- &MsgTKToggleFlush{mType: TK_ENABLE_FLUSH,
			streamId: common.MAINT_STREAM,
			bucket:   bucket}
		<-idx.tkCmdCh

		idx.bucketStreamMergeInProgress[bucket] = false

		//send the ack to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

		//for cbq bridge, return response after merge is done and
		//index is ready to query
		if !idx.enableManager {
			if clientCh, ok := idx.bucketCreateClientChMap[bucket]; ok {
				if clientCh != nil {
					clientCh <- &MsgSuccess{}
				}
				delete(idx.bucketCreateClientChMap, bucket)
			}
		} else {
			var instIdList []common.IndexInstId
			for _, inst := range mergeList {
				instIdList = append(instIdList, inst.InstId)
			}

			if err := idx.updateMetaInfoForIndexList(instIdList, true, true, false, false, true, false, false, false, nil); err != nil {
				common.CrashOnError(err)
			}
		}

	default:
		logging.Fatalf("Indexer::handleMergeStreamAck Unexpected Merge Stream Ack "+
			"Received for Stream %v Bucket %v", streamId, bucket)
		common.CrashOnError(errors.New("Unexpected Merge Stream Ack"))
	}

}

func (idx *indexer) handleStreamRequestDone(msg Message) {

	streamId := msg.(*MsgStreamInfo).GetStreamId()
	bucket := msg.(*MsgStreamInfo).GetBucket()
	buildTs := msg.(*MsgStreamInfo).GetBuildTs()

	logging.Infof("Indexer::handleStreamRequestDone StreamId %v Bucket %v",
		streamId, bucket)

	//send the ack to timekeeper
	idx.tkCmdCh <- msg
	<-idx.tkCmdCh

	delete(idx.streamBucketRequestStopCh[streamId], bucket)
	idx.bucketBuildTs[bucket] = buildTs

}

func (idx *indexer) handleMTRFail(msg Message) {

	streamId := msg.(*MsgRecovery).GetStreamId()
	bucket := msg.(*MsgRecovery).GetBucket()

	logging.Infof("Indexer::handleMTRFail StreamId %v Bucket %v",
		streamId, bucket)

	// if in MTR, MTR must have stopped when recieving this message.
	// Cleanup any lock that indexer may be holding.
	delete(idx.streamBucketRequestStopCh[streamId], bucket)
}

func (idx *indexer) handleBucketNotFound(msg Message) {

	streamId := msg.(*MsgRecovery).GetStreamId()
	bucket := msg.(*MsgRecovery).GetBucket()
	inMTR := msg.(*MsgRecovery).InMTR()

	logging.Infof("Indexer::handleBucketNotFound StreamId %v Bucket %v",
		streamId, bucket)

	// if in MTR, MTR must have stopped when recieving this message.
	// Cleanup any lock that indexer may be holding.
	if inMTR {
		delete(idx.streamBucketRequestStopCh[streamId], bucket)
	}

	is := idx.getIndexerState()
	if is == common.INDEXER_PREPARE_UNPAUSE {
		logging.Warnf("Indexer::handleBucketNotFound Skipped Bucket Cleanup "+
			"In %v state", is)
		return
	}

	//If stream is inactive, no cleanup is required.
	//If stream is prepare_recovery, recovery will take care of
	//validating the bucket and taking corrective action.
	state := idx.getStreamBucketState(streamId, bucket)

	if state == STREAM_INACTIVE ||
		state == STREAM_PREPARE_RECOVERY {
		logging.Infof("Indexer::handleBucketNotFound Skip %v %v %v",
			streamId, bucket, state)
		return
	}

	// delete index inst on the bucket from metadata repository and
	// return the list of deleted inst
	instIdList := idx.deleteIndexInstOnDeletedBucket(bucket, streamId)

	if len(instIdList) == 0 {
		logging.Infof("Indexer::handleBucketNotFound Empty IndexList %v %v. Nothing to do.",
			streamId, bucket)
		return
	}

	idx.bulkUpdateState(instIdList, common.INDEX_STATE_DELETED)
	logging.Infof("Indexer::handleBucketNotFound Updated Index State to DELETED %v",
		instIdList)

	msgUpdateIndexInstMap := idx.newIndexInstMsg(idx.indexInstMap)

	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, nil); err != nil {
		common.CrashOnError(err)
	}

	idx.stopBucketStream(streamId, bucket)

	//cleanup index data for all indexes in the bucket
	for _, instId := range instIdList {
		index := idx.indexInstMap[instId]
		idx.cleanupIndexData(index, nil)
	}

	idx.setStreamBucketState(streamId, bucket, STREAM_INACTIVE)

	logging.Infof("Indexer::handleBucketNotFound %v %v %v",
		streamId, bucket, STREAM_INACTIVE)

}

func (idx indexer) newIndexInstMsg(m common.IndexInstMap) *MsgUpdateInstMap {
	return &MsgUpdateInstMap{indexInstMap: m, stats: idx.stats.Clone(), rollbackTimes: idx.bucketRollbackTimes}
}

func (idx *indexer) cleanupIndexData(indexInst common.IndexInst,
	clientCh MsgChannel) {

	indexInstId := indexInst.InstId
	idxPartnInfo := idx.indexPartnMap[indexInstId]

	//update internal maps
	delete(idx.indexInstMap, indexInstId)
	delete(idx.indexPartnMap, indexInstId)

	msgUpdateIndexInstMap := idx.newIndexInstMsg(idx.indexInstMap)
	msgUpdateIndexPartnMap := &MsgUpdatePartnMap{indexPartnMap: idx.indexPartnMap}

	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, msgUpdateIndexPartnMap); err != nil {
		if clientCh != nil {
			clientCh <- &MsgError{
				err: Error{code: ERROR_INDEXER_INTERNAL_ERROR,
					severity: FATAL,
					cause:    err,
					category: INDEXER}}
		}
		common.CrashOnError(err)
	}

	//for all partitions managed by this indexer
	if indexInst.RState != common.REBAL_MERGED {
		for _, partnInst := range idxPartnInfo {
			sc := partnInst.Sc
			//close all the slices
			for _, slice := range sc.GetAllSlices() {
				go func() {
					slice.Close()
					logging.Infof("Indexer::cleanupIndexData %v Close Done", slice.IndexInstId())
					//wipe the physical files
					slice.Destroy()
					logging.Infof("Indexer::cleanupIndexData %v Destroy Done", slice.IndexInstId())
				}()
			}
		}
	}

}

func (idx *indexer) cleanupIndex(indexInst common.IndexInst,
	clientCh MsgChannel) {

	idx.cleanupIndexData(indexInst, clientCh)

	//send Stream update to workers
	if ok := idx.sendStreamUpdateForDropIndex(indexInst, clientCh); !ok {
		return
	}

	// If a proxy is deleted before merge has happened, we have to make sure that the real instance is updated.
	// If a proxy is already merged to the real index inst, this function will not be called (since the proxy
	// will no longer hold real data).
	if indexInst.RealInstId != 0 && indexInst.RealInstId != indexInst.InstId {
		// Proxy is in CATCHUP or ACTIVe state.   This means index build is done.
		// The projector could be sending mutations to the real index inst on those partitions from the proxy.
		// We have to remove those proxy partitions from the real index inst when the proxy is deleted.
		if indexInst.State == common.INDEX_STATE_CATCHUP || indexInst.State == common.INDEX_STATE_ACTIVE {
			if realInst, ok := idx.indexInstMap[indexInst.RealInstId]; ok {
				if realInst.Stream == indexInst.Stream {
					if realInst.State == common.INDEX_STATE_CATCHUP || realInst.State == common.INDEX_STATE_ACTIVE {
						idx.pruned[realInst.InstId] = realInst
					}
				}
			}
		}
	}

	clientCh <- &MsgSuccess{}
}

func (idx *indexer) sendStreamUpdateForIndex(indexInstList []common.IndexInst, bucket string, bucketUUID string, streamId common.StreamId) {

	respCh := make(MsgChannel)
	stopCh := make(StopChannel)

	cmd := &MsgStreamUpdate{
		mType:     ADD_INDEX_LIST_TO_STREAM,
		streamId:  streamId,
		bucket:    bucket,
		indexList: indexInstList,
		respCh:    respCh,
		stopCh:    stopCh}

	clustAddr := idx.config["clusterAddr"].String()
	retryCount := 0

	reqLock := idx.acquireStreamRequestLock(bucket, streamId)
	go func(reqLock *kvRequest) {
		defer idx.releaseStreamRequestLock(reqLock)
		idx.waitStreamRequestLock(reqLock)
	retryloop:
		for {
			if !ValidateBucket(clustAddr, bucket, []string{bucketUUID}) {
				logging.Errorf("Indexer::sendStreamUpdateForIndex \n\tBucket Not Found "+
					"For Stream %v Bucket %v", streamId, bucket)
				break retryloop
			}
			idx.sendMsgToKVSender(cmd)

			if resp, ok := <-respCh; ok {

				switch resp.GetMsgType() {

				case MSG_SUCCESS:
					logging.Infof("Indexer::sendStreamUpdateForIndex Success Stream %v Bucket %v ",
						streamId, bucket)
					break retryloop

				default:
					//log and retry for all other responses
					respErr := resp.(*MsgError).GetError()

					//If projector returns TopicMissing/GenServerClosed, AddInstance
					//cannot succeed. This needs to be aborted so that stream lock is
					//released and MTR can proceed to repair the Topic.
					if respErr.cause.Error() == common.ErrorClosed.Error() ||
						respErr.cause.Error() == projClient.ErrorTopicMissing.Error() {
						logging.Warnf("Indexer::sendStreamUpdateForIndex Stream %v Bucket %v "+
							"Error from Projector %v. Aborting.", streamId, bucket, respErr.cause)
						break retryloop
					} else if retryCount < 10 {
						logging.Errorf("Indexer::sendStreamUpdateForIndex Stream %v Bucket %v "+
							"Error from Projector %v. Retrying.", streamId, bucket, respErr.cause)
						retryCount++
						time.Sleep(KV_RETRY_INTERVAL * time.Millisecond)
					} else {
						logging.Errorf("Indexer::sendStreamUpdateForIndex Stream %v Bucket %v "+
							"Error from Projector %v. Reach max retry count.  Stop retrying.",
							streamId, bucket, respErr.cause)
						break retryloop
					}
				}
			}
		}
	}(reqLock)
}

func (idx *indexer) shutdownWorkers() {

	//shutdown mutation manager
	idx.mutMgrCmdCh <- &MsgGeneral{mType: MUT_MGR_SHUTDOWN}
	<-idx.mutMgrCmdCh

	//shutdown scan coordinator
	idx.scanCoordCmdCh <- &MsgGeneral{mType: SCAN_COORD_SHUTDOWN}
	<-idx.scanCoordCmdCh

	//shutdown storage manager
	idx.storageMgrCmdCh <- &MsgGeneral{mType: STORAGE_MGR_SHUTDOWN}
	<-idx.storageMgrCmdCh

	//shutdown timekeeper
	idx.tkCmdCh <- &MsgGeneral{mType: TK_SHUTDOWN}
	<-idx.tkCmdCh

	//shutdown admin manager
	idx.adminMgrCmdCh <- &MsgGeneral{mType: ADMIN_MGR_SHUTDOWN}
	<-idx.adminMgrCmdCh

	if idx.enableManager {
		//shutdown cluster manager
		idx.clustMgrAgentCmdCh <- &MsgGeneral{mType: CLUST_MGR_AGENT_SHUTDOWN}
		<-idx.clustMgrAgentCmdCh
	}

	//shutdown kv sender
	idx.kvSenderCmdCh <- &MsgGeneral{mType: KV_SENDER_SHUTDOWN}
	<-idx.kvSenderCmdCh

	// shutdown ddl manager
	idx.ddlSrvMgrCmdCh <- &MsgGeneral{mType: ADMIN_MGR_SHUTDOWN}
	<-idx.ddlSrvMgrCmdCh
}

func (idx *indexer) Shutdown() Message {

	logging.Infof("Indexer::Shutdown -  Shutting Down")
	//close the internal shutdown channel
	close(idx.shutdownInitCh)
	<-idx.shutdownCompleteCh
	logging.Infof("Indexer:Shutdown - Shutdown Complete")
	return nil
}

func (idx *indexer) sendStreamUpdateForBuildIndex(instIdList []common.IndexInstId,
	buildStream common.StreamId, bucket string, buildTs Timestamp, clientCh MsgChannel) bool {

	var cmd Message
	var indexList []common.IndexInst
	var bucketUUIDList []string
	for _, instId := range instIdList {
		indexInst := idx.indexInstMap[instId]
		indexList = append(indexList, indexInst)
		bucketUUIDList = append(bucketUUIDList, indexInst.Defn.BucketUUID)
	}

	respCh := make(MsgChannel)

	cmd = &MsgStreamUpdate{mType: OPEN_STREAM,
		streamId:     buildStream,
		bucket:       bucket,
		indexList:    indexList,
		buildTs:      buildTs,
		respCh:       respCh,
		restartTs:    nil,
		rollbackTime: idx.bucketRollbackTimes[bucket]}

	//send stream update to timekeeper
	if resp := idx.sendStreamUpdateToWorker(cmd, idx.tkCmdCh,
		"Timekeeper"); resp.GetMsgType() != MSG_SUCCESS {
		if clientCh != nil {
			clientCh <- resp
		}
		respErr := resp.(*MsgError).GetError()
		common.CrashOnError(respErr.cause)
	}

	//send stream update to mutation manager
	if resp := idx.sendStreamUpdateToWorker(cmd, idx.mutMgrCmdCh,
		"MutationMgr"); resp.GetMsgType() != MSG_SUCCESS {
		if clientCh != nil {
			clientCh <- resp
		}
		respErr := resp.(*MsgError).GetError()
		common.CrashOnError(respErr.cause)
	}

	stopCh := make(StopChannel)

	if _, ok := idx.streamBucketRequestStopCh[buildStream]; !ok {
		idx.streamBucketRequestStopCh[buildStream] = make(BucketRequestStopCh)
	}
	idx.streamBucketRequestStopCh[buildStream][bucket] = stopCh

	clustAddr := idx.config["clusterAddr"].String()
	numVb := idx.config["numVbuckets"].Int()

	reqLock := idx.acquireStreamRequestLock(bucket, buildStream)
	go func(reqLock *kvRequest) {
		defer idx.releaseStreamRequestLock(reqLock)
		idx.waitStreamRequestLock(reqLock)

	retryloop:
		for {
			if !ValidateBucket(clustAddr, bucket, bucketUUIDList) {
				logging.Errorf("Indexer::sendStreamUpdateForBuildIndex \n\tBucket Not Found "+
					"For Stream %v Bucket %v", buildStream, bucket)
				idx.internalRecvCh <- &MsgRecovery{mType: INDEXER_BUCKET_NOT_FOUND,
					streamId: buildStream,
					bucket:   bucket,
					inMTR:    true}
				break retryloop
			}
			idx.sendMsgToKVSender(cmd)

			if resp, ok := <-respCh; ok {

				switch resp.GetMsgType() {

				case MSG_SUCCESS_OPEN_STREAM:
					logging.Infof("Indexer::sendStreamUpdateForBuildIndex Stream Request Success For "+
						"Stream %v Bucket %v.", buildStream, bucket)

					//once stream request is successful re-calculate the KV timestamp.
					//This makes sure indexer doesn't use a timestamp which can never
					//be caught up to (due to kv rollback).
					//if there is a failover after this, it will be observed as a rollback
					buildTs, err := computeBucketBuildTs(clustAddr, bucket, numVb)
					if err == nil {
						idx.internalRecvCh <- &MsgStreamInfo{mType: STREAM_REQUEST_DONE,
							streamId: buildStream,
							bucket:   bucket,
							buildTs:  buildTs,
							activeTs: resp.(*MsgSuccessOpenStream).GetActiveTs(),
						}
						break retryloop
					}

				case INDEXER_ROLLBACK:
					//an initial build request should never receive rollback message
					logging.Errorf("Indexer::sendStreamUpdateForBuildIndex Unexpected Rollback from "+
						"Projector during Initial Stream Request %v", resp)
					common.CrashOnError(ErrKVRollbackForInitRequest)

				default:
					//log and retry for all other responses
					respErr := resp.(*MsgError).GetError()

					state := idx.getStreamBucketState(buildStream, bucket)

					if state == STREAM_PREPARE_RECOVERY || state == STREAM_INACTIVE {
						logging.Errorf("Indexer::sendStreamUpdateForBuildIndex Stream %v Bucket %v "+
							"Error from Projector %v. Not Retrying. State %v", buildStream, bucket,
							respErr.cause, state)

						idx.internalRecvCh <- &MsgRecovery{mType: INDEXER_MTR_FAIL,
							streamId: buildStream,
							bucket:   bucket,
							inMTR:    true}

						break retryloop

					} else {
						logging.Errorf("Indexer::sendStreamUpdateForBuildIndex Stream %v Bucket %v "+
							"Error from Projector %v. Retrying.", buildStream, bucket,
							respErr.cause)
						time.Sleep(KV_RETRY_INTERVAL * time.Millisecond)
					}
				}
			}
		}
	}(reqLock)

	return true

}

func (idx *indexer) sendMsgToKVSender(cmd Message) {
	idx.kvlock.Lock()
	defer idx.kvlock.Unlock()

	//send stream update to kv sender
	idx.kvSenderCmdCh <- cmd
	<-idx.kvSenderCmdCh
}

func (idx *indexer) sendStreamUpdateToWorker(cmd Message, workerCmdCh MsgChannel,
	workerStr string) Message {

	//send message to worker
	workerCmdCh <- cmd
	if resp, ok := <-workerCmdCh; ok {
		if resp.GetMsgType() != MSG_SUCCESS {

			logging.Errorf("Indexer::sendStreamUpdateToWorker - Error received from %v "+
				"processing Msg %v Err %v. Aborted.", workerStr, cmd, resp)

			return &MsgError{
				err: Error{code: ERROR_INDEXER_INTERNAL_ERROR,
					severity: FATAL,
					cause:    ErrInconsistentState,
					category: INDEXER}}
		}
	} else {
		logging.Errorf("Indexer::sendStreamUpdateToWorker - Error communicating with %v "+
			"processing Msg %v Err %v. Aborted.", workerStr, cmd, resp)

		return &MsgError{
			err: Error{code: ERROR_INDEXER_INTERNAL_ERROR,
				severity: FATAL,
				cause:    ErrFatalComm,
				category: INDEXER}}
	}
	return &MsgSuccess{}
}

func (idx *indexer) sendStreamUpdateForDropIndex(indexInst common.IndexInst,
	clientCh MsgChannel) bool {

	var indexList []common.IndexInst
	indexList = append(indexList, indexInst)

	return idx.removeIndexesFromStream(indexList, indexInst.Defn.Bucket, indexInst.Defn.BucketUUID, indexInst.Stream, indexInst.State, clientCh)
}

func (idx *indexer) removeIndexesFromStream(indexList []common.IndexInst,
	bucket string,
	bucketUUID string,
	streamId common.StreamId,
	state common.IndexState,
	clientCh MsgChannel) bool {

	var cmd Message

	var indexStreamIds []common.StreamId

	//index in INIT_STREAM needs to be removed from MAINT_STREAM as well
	//if the state is CATCHUP
	switch streamId {

	case common.MAINT_STREAM:
		indexStreamIds = append(indexStreamIds, common.MAINT_STREAM)

	case common.INIT_STREAM:
		indexStreamIds = append(indexStreamIds, common.INIT_STREAM)
		if state == common.INDEX_STATE_CATCHUP {
			indexStreamIds = append(indexStreamIds, common.MAINT_STREAM)
		}

	default:
		logging.Fatalf("Indexer::removeIndexesFromStream \n\t Unsupported StreamId %v", streamId)
		common.CrashOnError(ErrInvalidStream)
	}

	for _, streamId := range indexStreamIds {

		respCh := make(MsgChannel)

		if idx.checkBucketExistsInStream(bucket, streamId, false) {

			cmd = &MsgStreamUpdate{mType: REMOVE_INDEX_LIST_FROM_STREAM,
				streamId:  streamId,
				indexList: indexList,
				respCh:    respCh}
		} else {
			cmd = &MsgStreamUpdate{mType: REMOVE_BUCKET_FROM_STREAM,
				streamId: streamId,
				bucket:   bucket,
				respCh:   respCh}
			idx.setStreamBucketState(streamId, bucket, STREAM_INACTIVE)
		}

		//send stream update to mutation manager
		if resp := idx.sendStreamUpdateToWorker(cmd, idx.mutMgrCmdCh,
			"MutationMgr"); resp.GetMsgType() != MSG_SUCCESS {
			if clientCh != nil {
				clientCh <- resp
			}
			respErr := resp.(*MsgError).GetError()
			common.CrashOnError(respErr.cause)
		}

		//send stream update to timekeeper
		if resp := idx.sendStreamUpdateToWorker(cmd, idx.tkCmdCh,
			"Timekeeper"); resp.GetMsgType() != MSG_SUCCESS {
			if clientCh != nil {
				clientCh <- resp
			}
			respErr := resp.(*MsgError).GetError()
			common.CrashOnError(respErr.cause)
		}
		clustAddr := idx.config["clusterAddr"].String()

		reqLock := idx.acquireStreamRequestLock(bucket, streamId)
		go func(reqLock *kvRequest) {
			defer idx.releaseStreamRequestLock(reqLock)
			idx.waitStreamRequestLock(reqLock)
		retryloop:
			for {

				if !ValidateBucket(clustAddr, bucket, []string{bucketUUID}) {
					logging.Errorf("Indexer::removeIndexesFromStream \n\tBucket Not Found "+
						"For Stream %v Bucket %v", streamId, bucket)
					idx.internalRecvCh <- &MsgRecovery{mType: INDEXER_BUCKET_NOT_FOUND,
						streamId: streamId,
						bucket:   bucket}
					break retryloop
				}

				idx.sendMsgToKVSender(cmd)

				if resp, ok := <-respCh; ok {

					switch resp.GetMsgType() {

					case MSG_SUCCESS:
						logging.Infof("Indexer::removeIndexesFromStream Success Stream %v Bucket %v",
							streamId, bucket)
						break retryloop

					default:
						//log and retry for all other responses
						respErr := resp.(*MsgError).GetError()
						logging.Errorf("Indexer::removeIndexesFromStream - Stream %v Bucket %v"+
							"Error from Projector %v. Retrying.", streamId, bucket, respErr.cause)
						time.Sleep(KV_RETRY_INTERVAL * time.Millisecond)

					}
				}
			}
		}(reqLock)
	}

	return true

}

func (idx *indexer) initPartnInstance(indexInst common.IndexInst,
	respCh MsgChannel) (PartitionInstMap, error) {

	//initialize partitionInstMap for this index
	partnInstMap := make(PartitionInstMap)

	//get all partitions for this index
	partnDefnList := indexInst.Pc.GetAllPartitions()

	for _, partnDefn := range partnDefnList {
		//TODO: Ignore partitions which do not belong to this
		//indexer node(based on the endpoints)
		partnInst := PartitionInst{Defn: partnDefn,
			Sc: NewHashedSliceContainer()}

		logging.Infof("Indexer::initPartnInstance Initialized Partition: \n\t Index: %v Partition: %v",
			indexInst.InstId, partnInst)

		//add a single slice per partition for now
		if slice, err := NewSlice(SliceId(0), &indexInst, &partnInst, idx.config, idx.stats); err == nil {
			partnInst.Sc.AddSlice(0, slice)
			logging.Infof("Indexer::initPartnInstance Initialized Slice: \n\t Index: %v Slice: %v",
				indexInst.InstId, slice)

			partnInstMap[partnDefn.GetPartitionId()] = partnInst
		} else {
			errStr := fmt.Sprintf("Error creating slice %v", err)
			logging.Errorf("Indexer::initPartnInstance %v. Abort.", errStr)
			err1 := errors.New(errStr)

			if respCh != nil {
				respCh <- &MsgError{
					err: Error{code: ERROR_INDEXER_INTERNAL_ERROR,
						severity: FATAL,
						cause:    err1,
						category: INDEXER}}
			}
			return nil, err1
		}
	}

	return partnInstMap, nil
}

func (idx *indexer) distributeIndexMapsToWorkers(msgUpdateIndexInstMap Message,
	msgUpdateIndexPartnMap Message) error {

	//update index map in storage manager
	if err := idx.sendUpdatedIndexMapToWorker(msgUpdateIndexInstMap, msgUpdateIndexPartnMap, idx.storageMgrCmdCh,
		"StorageMgr"); err != nil {
		return err
	}

	//update index map in mutation manager
	if err := idx.sendUpdatedIndexMapToWorker(msgUpdateIndexInstMap, msgUpdateIndexPartnMap, idx.mutMgrCmdCh,
		"MutationMgr"); err != nil {
		return err
	}

	//update index map in scan coordinator
	if err := idx.sendUpdatedIndexMapToWorker(msgUpdateIndexInstMap, msgUpdateIndexPartnMap, idx.scanCoordCmdCh,
		"ScanCoordinator"); err != nil {
		return err
	}

	if err := idx.sendUpdatedIndexMapToWorker(msgUpdateIndexInstMap, msgUpdateIndexPartnMap, idx.tkCmdCh,
		"Timekeeper"); err != nil {
		return err
	}

	if err := idx.sendUpdatedIndexMapToWorker(msgUpdateIndexInstMap, nil, idx.statsMgrCmdCh,
		"statsMgr"); err != nil {
		return err
	}

	if err := idx.sendUpdatedIndexMapToWorker(msgUpdateIndexInstMap, nil, idx.clustMgrAgentCmdCh,
		"clusterMgrAgent"); err != nil {
		return err
	}

	return nil
}

func (idx *indexer) sendUpdatedIndexMapToWorker(msgUpdateIndexInstMap Message,
	msgUpdateIndexPartnMap Message, workerCmdCh chan Message, workerStr string) error {

	if msgUpdateIndexInstMap != nil {
		workerCmdCh <- msgUpdateIndexInstMap

		if resp, ok := <-workerCmdCh; ok {

			if resp.GetMsgType() == MSG_ERROR {
				logging.Errorf("Indexer::sendUpdatedIndexMapToWorker - Error received from %v processing "+
					"Msg %v Err %v. Aborted.", workerStr, msgUpdateIndexInstMap, resp)
				respErr := resp.(*MsgError).GetError()
				return respErr.cause
			}
		} else {
			logging.Errorf("Indexer::sendUpdatedIndexMapToWorker - Error communicating with %v "+
				"processing Msg %v. Aborted.", workerStr, msgUpdateIndexInstMap)
			return ErrFatalComm
		}
	}

	if msgUpdateIndexPartnMap != nil {
		workerCmdCh <- msgUpdateIndexPartnMap
		if resp, ok := <-workerCmdCh; ok {

			if resp.GetMsgType() == MSG_ERROR {
				logging.Errorf("Indexer::sendUpdatedIndexMapToWorker - Error received from %v processing "+
					"Msg %v Err %v. Aborted.", workerStr, msgUpdateIndexPartnMap, resp)
				respErr := resp.(*MsgError).GetError()
				return respErr.cause
			}
		} else {
			logging.Errorf("Indexer::sendUpdatedIndexMapToWorker - Error communicating with %v "+
				"processing Msg %v. Aborted.", workerStr, msgUpdateIndexPartnMap, resp)
			return ErrFatalComm
		}
	}

	return nil

}

func (idx *indexer) initStreamAddressMap() {
	StreamAddrMap = make(StreamAddressMap)

	port2addr := func(p string) string {
		return net.JoinHostPort("", idx.config[p].String())
	}

	StreamAddrMap[common.MAINT_STREAM] = common.Endpoint(port2addr("streamMaintPort"))
	StreamAddrMap[common.CATCHUP_STREAM] = common.Endpoint(port2addr("streamCatchupPort"))
	StreamAddrMap[common.INIT_STREAM] = common.Endpoint(port2addr("streamInitPort"))
}

func (idx *indexer) initServiceAddressMap() {
	ServiceAddrMap = make(map[string]string)

	ServiceAddrMap[common.INDEX_ADMIN_SERVICE] = idx.config["adminPort"].String()
	ServiceAddrMap[common.INDEX_SCAN_SERVICE] = idx.config["scanPort"].String()
	ServiceAddrMap[common.INDEX_HTTP_SERVICE] = idx.config["httpPort"].String()

	common.SetServicePorts(ServiceAddrMap)
}

func (idx *indexer) initStreamTopicName() {
	StreamTopicName = make(map[common.StreamId]string)

	StreamTopicName[common.MAINT_STREAM] = MAINT_TOPIC + "_" + idx.id
	StreamTopicName[common.CATCHUP_STREAM] = CATCHUP_TOPIC + "_" + idx.id
	StreamTopicName[common.INIT_STREAM] = INIT_TOPIC + "_" + idx.id
}

//checkDuplicateIndex checks if an index with the given indexInstId
// or name already exists
func (idx *indexer) checkDuplicateIndex(indexInst common.IndexInst,
	respCh MsgChannel) bool {

	//if the indexInstId already exists, return error
	if index, ok := idx.indexInstMap[indexInst.InstId]; ok {
		logging.Errorf("Indexer::checkDuplicateIndex Duplicate Index Instance. "+
			"IndexInstId: %v, Index: %v", indexInst.InstId, index)

		if respCh != nil {
			respCh <- &MsgError{
				err: Error{code: ERROR_INDEX_ALREADY_EXISTS,
					severity: FATAL,
					cause:    errors.New("Duplicate Index Instance"),
					category: INDEXER}}
		}
		return false
	}

	//if the index name already exists for the same bucket,
	//return error
	if !common.IsPartitioned(indexInst.Defn.PartitionScheme) {
		for _, index := range idx.indexInstMap {

			if index.Defn.Name == indexInst.Defn.Name &&
				index.Defn.Bucket == indexInst.Defn.Bucket &&
				index.State != common.INDEX_STATE_DELETED {

				logging.Errorf("Indexer::checkDuplicateIndex Duplicate Index Name. "+
					"Name: %v, Duplicate Index: %v", indexInst.Defn.Name, index)

				if respCh != nil {
					respCh <- &MsgError{
						err: Error{code: ERROR_INDEX_ALREADY_EXISTS,
							severity: FATAL,
							cause:    errors.New("Duplicate Index Name"),
							category: INDEXER}}
				}
				return false
			}

		}
	}
	return true
}

//checkDuplicateInitialBuildRequest check if any other index on the given bucket
//is already building
func (idx *indexer) checkDuplicateInitialBuildRequest(bucket string,
	instIdList []common.IndexInstId, respCh MsgChannel, errMap map[common.IndexInstId]error) bool {

	// Check if stream merge is in progress for this bucket.
	if inProg, ok := idx.bucketStreamMergeInProgress[bucket]; ok {
		if inProg {
			logging.Errorf("Indexer::checkDuplicateInitialBuildRequest Stream Merge" +
							" for bucket %v is in progress", bucket)
			return false
		}
	}

	//if initial build is already running for some other index on this bucket,
	//cannot start another one
	for _, index := range idx.indexInstMap {

		if ((index.State == common.INDEX_STATE_INITIAL ||
			index.State == common.INDEX_STATE_CATCHUP) &&
			index.Defn.Bucket == bucket) ||
			idx.checkStreamRequestPending(index.Stream, bucket) {

			errStr := fmt.Sprintf("Build Already In Progress. Bucket %v", bucket)
			logging.Errorf("Indexer::checkDuplicateInitialBuildRequest %v, %v", index, bucket)
			if idx.enableManager {
				idx.bulkUpdateError(instIdList, errStr)
				for _, instId := range instIdList {
					errMap[instId] = &common.IndexerError{Reason: errStr, Code: common.IndexBuildInProgress}
				}
			} else if respCh != nil {
				respCh <- &MsgError{
					err: Error{code: ERROR_INDEX_BUILD_IN_PROGRESS,
						severity: FATAL,
						cause:    errors.New(errStr),
						category: INDEXER}}
			}
			return false
		}
	}

	return true
}

func (idx *indexer) handleCheckDDLInProgress(msg Message) {

	ddlMsg := msg.(*MsgCheckDDLInProgress)
	respCh := ddlMsg.GetRespCh()

	respCh <- idx.checkDDLInProgress()

	return
}

func (idx *indexer) checkDDLInProgress() bool {

	ddlInProgress := false
	for _, index := range idx.indexInstMap {

		if (index.State == common.INDEX_STATE_INITIAL ||
			index.State == common.INDEX_STATE_CATCHUP) ||
			idx.checkStreamRequestPending(common.INIT_STREAM, index.Defn.Bucket) {
			ddlInProgress = true
		}
	}
	return ddlInProgress
}

func (idx *indexer) handleUpdateIndexRState(msg Message) {

	updateMsg := msg.(*MsgUpdateIndexRState)
	respCh := updateMsg.GetRespCh()
	instId := updateMsg.GetInstId()
	rstate := updateMsg.GetRState()

	inst, ok := idx.indexInstMap[instId]
	if !ok {
		logging.Errorf("Indexer::handleUpdateIndexRState Unable to find Index %v", instId)
		respCh <- ErrInconsistentState
		return
	}

	inst.RState = rstate
	idx.indexInstMap[instId] = inst

	instIds := []common.IndexInstId{instId}
	if err := idx.updateMetaInfoForIndexList(instIds, false, false, false, false, true, true, false, false, respCh); err != nil {
		common.CrashOnError(err)
	}

	logging.Infof("handleUpdateIndexRState: Index instance %v rstate moved to ACTIVE", instId)
}

//TODO If this function gets error before its finished, the state
//can be inconsistent. This needs to be fixed.
func (idx *indexer) handleInitialBuildDone(msg Message) {

	bucket := msg.(*MsgTKInitBuildDone).GetBucket()
	streamId := msg.(*MsgTKInitBuildDone).GetStreamId()

	//skip processing initial build done for inactive or recovery streams.
	//the streams would be restarted and then build done would get recomputed.
	state := idx.getStreamBucketState(streamId, bucket)

	if state == STREAM_INACTIVE ||
		state == STREAM_PREPARE_RECOVERY ||
		state == STREAM_RECOVERY {
		logging.Infof("Indexer::handleInitialBuildDone Skip InitBuildDone %v %v %v",
			streamId, bucket, state)
		return
	}

	logging.Infof("Indexer::handleInitialBuildDone Bucket: %v Stream: %v", bucket, streamId)

	//MAINT_STREAM should already be running for this bucket,
	//as first index gets added to MAINT_STREAM always
	if idx.checkBucketExistsInStream(bucket, common.MAINT_STREAM, true) == false {
		logging.Fatalf("Indexer::handleInitialBuildDone MAINT_STREAM not enabled for Bucket: %v. "+
			"Cannot Process Initial Build Done.", bucket)
		common.CrashOnError(ErrMaintStreamMissingBucket)
	}

	//get the list of indexes for this bucket and stream in INITIAL state
	var indexList []common.IndexInst
	var instIdList []common.IndexInstId
	var bucketUUIDList []string
	for _, index := range idx.indexInstMap {
		if index.Defn.Bucket == bucket && index.Stream == streamId &&
			index.State == common.INDEX_STATE_INITIAL {
			//index in INIT_STREAM move to Catchup state
			if streamId == common.INIT_STREAM {
				index.State = common.INDEX_STATE_CATCHUP
			} else {
				index.State = common.INDEX_STATE_ACTIVE
			}
			indexList = append(indexList, index)
			instIdList = append(instIdList, index.InstId)
			bucketUUIDList = append(bucketUUIDList, index.Defn.BucketUUID)
		}
	}

	if len(instIdList) == 0 {
		logging.Infof("Indexer::handleInitialBuildDone Empty IndexList %v %v. Nothing to do.",
			streamId, bucket)
		return
	}

	//update the IndexInstMap
	for _, index := range indexList {
		idx.indexInstMap[index.InstId] = index
	}

	//send updated maps to all workers
	msgUpdateIndexInstMap := idx.newIndexInstMsg(idx.indexInstMap)

	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, nil); err != nil {
		common.CrashOnError(err)
	}

	//if index is already in MAINT_STREAM, nothing more needs to be done
	if err := idx.updateMetaInfoForIndexList(instIdList, true, true, false, false, true, false, false, false, nil); err != nil {
		common.CrashOnError(err)
	}

	// collect progress stats after initial is done or transition to catchup phase. This must be done
	// after updating metaInfo.
	idx.sendProgressStats()

	if streamId == common.MAINT_STREAM {

		//for cbq bridge, return response as index is ready to query
		if !idx.enableManager {
			if clientCh, ok := idx.bucketCreateClientChMap[bucket]; ok {
				if clientCh != nil {
					clientCh <- &MsgSuccess{}
				}
				delete(idx.bucketCreateClientChMap, bucket)
			}
		}
		return
	}

	// If index is a proxy, add the real index instance to the list.  This will
	// also update the partition list of the real index instance in projector.
	// This will ensure that the projector will be sending partition mutations to the
	// real inst in the MAINT stream, when the proxy has become active.
	// Note that the real inst should be active or being built at the same time as the proxy.
	for _, index := range indexList {
		if common.IsPartitioned(index.Defn.PartitionScheme) && index.RealInstId != 0 && index.InstId != index.RealInstId {
			if realInst, ok := idx.indexInstMap[index.RealInstId]; ok {
				indexList = append(indexList, realInst)
			} else {
				err := fmt.Errorf("Fail to find real index instance %v", index.RealInstId)
				logging.Errorf("Indexer::handleInitialBuildDone. %v", err)
				common.CrashOnError(err)
			}
		}
	}

	//Add index to MAINT_STREAM in Catchup State,
	//so mutations for this index are already in queue to
	//allow convergence with INIT_STREAM.
	respCh := make(MsgChannel)
	stopCh := make(StopChannel)

	cmd := &MsgStreamUpdate{mType: ADD_INDEX_LIST_TO_STREAM,
		streamId:  common.MAINT_STREAM,
		bucket:    bucket,
		indexList: indexList,
		respCh:    respCh,
		stopCh:    stopCh}

	//send stream update to timekeeper
	if resp := idx.sendStreamUpdateToWorker(cmd, idx.tkCmdCh, "Timekeeper"); resp.GetMsgType() != MSG_SUCCESS {
		respErr := resp.(*MsgError).GetError()
		common.CrashOnError(respErr.cause)
	}

	//send stream update to mutation manager
	if resp := idx.sendStreamUpdateToWorker(cmd, idx.mutMgrCmdCh, "MutationMgr"); resp.GetMsgType() != MSG_SUCCESS {
		respErr := resp.(*MsgError).GetError()
		common.CrashOnError(respErr.cause)
	}

	clustAddr := idx.config["clusterAddr"].String()

	reqLock := idx.acquireStreamRequestLock(bucket, streamId)
	go func(reqLock *kvRequest) {
		defer idx.releaseStreamRequestLock(reqLock)
		idx.waitStreamRequestLock(reqLock)
	retryloop:
		for {
			if !ValidateBucket(clustAddr, bucket, bucketUUIDList) {
				logging.Errorf("Indexer::handleInitialBuildDone \n\tBucket Not Found "+
					"For Stream %v Bucket %v", streamId, bucket)
				break retryloop
			}
			idx.sendMsgToKVSender(cmd)

			if resp, ok := <-respCh; ok {

				switch resp.GetMsgType() {

				case MSG_SUCCESS:
					logging.Infof("Indexer::handleInitialBuildDone Success Stream %v Bucket %v ",
						streamId, bucket)

					mergeTs := resp.(*MsgStreamUpdate).GetRestartTs()

					idx.internalRecvCh <- &MsgTKInitBuildDone{
						mType:    TK_INIT_BUILD_DONE_ACK,
						streamId: streamId,
						bucket:   bucket,
						mergeTs:  mergeTs}
					break retryloop

				default:
					//log and retry for all other responses
					respErr := resp.(*MsgError).GetError()

					//If projector returns TopicMissing/GenServerClosed, AddInstance
					//cannot succeed. This needs to be aborted so that stream lock is
					//released and MTR can proceed to repair the Topic.
					if respErr.cause.Error() == common.ErrorClosed.Error() ||
						respErr.cause.Error() == projClient.ErrorTopicMissing.Error() {
						logging.Warnf("Indexer::handleInitialBuildDone Stream %v Bucket %v "+
							"Error from Projector %v. Aborting.", streamId, bucket, respErr.cause)
						break retryloop
					} else {

						logging.Errorf("Indexer::handleInitialBuildDone Stream %v Bucket %v "+
							"Error from Projector %v. Retrying.", streamId, bucket, respErr.cause)
						time.Sleep(KV_RETRY_INTERVAL * time.Millisecond)
					}
				}
			}
		}
	}(reqLock)

}

func (idx *indexer) handleMergeStream(msg Message) {

	bucket := msg.(*MsgTKMergeStream).GetBucket()
	streamId := msg.(*MsgTKMergeStream).GetStreamId()

	//skip processing stream merge for inactive or recovery streams.
	state := idx.getStreamBucketState(streamId, bucket)

	if state == STREAM_INACTIVE ||
		state == STREAM_PREPARE_RECOVERY ||
		state == STREAM_RECOVERY {
		logging.Infof("Indexer::handleMergeStream Skip MergeStream %v %v %v",
			streamId, bucket, state)
		return
	}

	//MAINT_STREAM should already be running for this bucket,
	//as first index gets added to MAINT_STREAM always
	if idx.checkBucketExistsInStream(bucket, common.MAINT_STREAM, true) == false {
		logging.Fatalf("Indexer::handleMergeStream \n\tMAINT_STREAM not enabled for Bucket: %v ."+
			"Cannot Process Merge Stream", bucket)
		common.CrashOnError(ErrMaintStreamMissingBucket)
	}

	switch streamId {

	case common.INIT_STREAM:
		idx.handleMergeInitStream(msg)

	default:
		logging.Fatalf("Indexer::handleMergeStream \n\tOnly INIT_STREAM can be merged "+
			"to MAINT_STREAM. Found Stream: %v.", streamId)
		common.CrashOnError(ErrInvalidStream)
	}
}

//TODO If this function gets error before its finished, the state
//can be inconsistent. This needs to be fixed.
func (idx *indexer) handleMergeInitStream(msg Message) {

	bucket := msg.(*MsgTKMergeStream).GetBucket()
	streamId := msg.(*MsgTKMergeStream).GetStreamId()

	idx.bucketStreamMergeInProgress[bucket] = true

	logging.Infof("Indexer::handleMergeInitStream Bucket: %v Stream: %v", bucket, streamId)

	//get the list of indexes for this bucket in CATCHUP state
	var indexList []common.IndexInst
	var bucketUUIDList []string
	for _, index := range idx.indexInstMap {
		if index.Defn.Bucket == bucket && index.Stream == streamId &&
			index.State == common.INDEX_STATE_CATCHUP {

			index.State = common.INDEX_STATE_ACTIVE
			index.Stream = common.MAINT_STREAM
			indexList = append(indexList, index)
			bucketUUIDList = append(bucketUUIDList, index.Defn.BucketUUID)
		}
	}

	//update the IndexInstMap
	for _, index := range indexList {
		idx.indexInstMap[index.InstId] = index
	}

	//send updated maps to all workers
	msgUpdateIndexInstMap := idx.newIndexInstMsg(idx.indexInstMap)

	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, nil); err != nil {
		common.CrashOnError(err)
	}

	respCh := make(MsgChannel)
	stopCh := make(StopChannel)

	//remove bucket from INIT_STREAM
	var cmd Message
	cmd = &MsgStreamUpdate{mType: REMOVE_BUCKET_FROM_STREAM,
		streamId: streamId,
		bucket:   bucket,
		respCh:   respCh,
		stopCh:   stopCh,
	}

	//send stream update to timekeeper
	if resp := idx.sendStreamUpdateToWorker(cmd, idx.tkCmdCh, "Timekeeper"); resp.GetMsgType() != MSG_SUCCESS {
		respErr := resp.(*MsgError).GetError()
		common.CrashOnError(respErr.cause)
	}

	//send stream update to mutation manager
	if resp := idx.sendStreamUpdateToWorker(cmd, idx.mutMgrCmdCh, "MutationMgr"); resp.GetMsgType() != MSG_SUCCESS {
		respErr := resp.(*MsgError).GetError()
		common.CrashOnError(respErr.cause)
	}

	//at this point, the stream is inactive in all sub-components, so the status
	//can be set to inactive.
	idx.setStreamBucketState(streamId, bucket, STREAM_INACTIVE)

	if _, ok := idx.streamBucketRequestStopCh[streamId]; !ok {
		idx.streamBucketRequestStopCh[streamId] = make(BucketRequestStopCh)
	}
	idx.streamBucketRequestStopCh[streamId][bucket] = stopCh

	reqLock := idx.acquireStreamRequestLock(bucket, streamId)
	go func(reqLock *kvRequest) {
		defer idx.releaseStreamRequestLock(reqLock)
		idx.waitStreamRequestLock(reqLock)
	retryloop:
		for {

			idx.sendMsgToKVSender(cmd)

			if resp, ok := <-respCh; ok {

				switch resp.GetMsgType() {

				case MSG_SUCCESS:
					logging.Infof("Indexer::handleMergeInitStream Success Stream %v Bucket %v ",
						streamId, bucket)
					idx.internalRecvCh <- &MsgTKMergeStream{
						mType:     TK_MERGE_STREAM_ACK,
						streamId:  streamId,
						bucket:    bucket,
						mergeList: indexList}
					break retryloop

				default:
					//log and retry for all other responses
					respErr := resp.(*MsgError).GetError()
					logging.Errorf("Indexer::handleMergeInitStream Stream %v Bucket %v "+
						"Error from Projector %v. Retrying.", streamId, bucket, respErr.cause)
					time.Sleep(KV_RETRY_INTERVAL * time.Millisecond)
				}
			}
		}
	}(reqLock)

	logging.Infof("Indexer::handleMergeInitStream Merge Done Bucket: %v Stream: %v",
		bucket, streamId)
}

//checkBucketExistsInStream returns true if there is no index in the given stream
//which belongs to the given bucket, else false
func (idx *indexer) checkBucketExistsInStream(bucket string, streamId common.StreamId, checkDelete bool) bool {

	//check if any index of the given bucket is in the Stream
	for _, index := range idx.indexInstMap {

		// use checkDelete to verify index in DELETED status.   If an index is dropped while
		// there is concurrent build, the stream will not be cleaned up.
		if index.Defn.Bucket == bucket && index.Stream == streamId &&
			(index.State == common.INDEX_STATE_ACTIVE ||
				index.State == common.INDEX_STATE_CATCHUP ||
				index.State == common.INDEX_STATE_INITIAL ||
				(index.State == common.INDEX_STATE_DELETED && checkDelete)) {
			return true
		}
	}

	return false

}

//checkLastBucketInStream returns true if the given bucket is the only bucket
//active in the given stream, else false
func (idx *indexer) checkLastBucketInStream(bucket string, streamId common.StreamId) bool {

	for _, index := range idx.indexInstMap {

		if index.Defn.Bucket != bucket && index.Stream == streamId &&
			(index.State == common.INDEX_STATE_ACTIVE ||
				index.State == common.INDEX_STATE_CATCHUP ||
				index.State == common.INDEX_STATE_INITIAL) {
			return false
		}
	}

	return true

}

//checkStreamEmpty return true if there is no index currently in the
//give stream, else false
func (idx *indexer) checkStreamEmpty(streamId common.StreamId) bool {

	for _, index := range idx.indexInstMap {
		if index.Stream == streamId {
			logging.Tracef("Indexer::checkStreamEmpty Found Index %v Stream %v",
				index.InstId, streamId)
			return false
		}
	}
	logging.Tracef("Indexer::checkStreamEmpty Stream %v Empty", streamId)

	return true

}

func (idx *indexer) getIndexListForBucketAndStream(streamId common.StreamId,
	bucket string) []common.IndexInst {

	indexList := make([]common.IndexInst, 0)
	for _, idx := range idx.indexInstMap {

		if idx.Stream == streamId && idx.Defn.Bucket == bucket {

			indexList = append(indexList, idx)

		}
	}

	return indexList

}

func (idx *indexer) stopBucketStream(streamId common.StreamId, bucket string) {

	logging.Infof("Indexer::stopBucketStream Stream: %v Bucket %v", streamId, bucket)

	respCh := make(MsgChannel)
	stopCh := make(StopChannel)

	var cmd Message
	cmd = &MsgStreamUpdate{mType: REMOVE_BUCKET_FROM_STREAM,
		streamId: streamId,
		bucket:   bucket,
		respCh:   respCh,
		stopCh:   stopCh}

	//send stream update to mutation manager
	if resp := idx.sendStreamUpdateToWorker(cmd, idx.mutMgrCmdCh,
		"MutationMgr"); resp.GetMsgType() != MSG_SUCCESS {
		respErr := resp.(*MsgError).GetError()
		common.CrashOnError(respErr.cause)
	}

	//send stream update to timekeeper
	if resp := idx.sendStreamUpdateToWorker(cmd, idx.tkCmdCh,
		"Timekeeper"); resp.GetMsgType() != MSG_SUCCESS {
		respErr := resp.(*MsgError).GetError()
		common.CrashOnError(respErr.cause)
	}

	if _, ok := idx.streamBucketRequestStopCh[streamId]; !ok {
		idx.streamBucketRequestStopCh[streamId] = make(BucketRequestStopCh)
	}
	idx.streamBucketRequestStopCh[streamId][bucket] = stopCh

	reqLock := idx.acquireStreamRequestLock(bucket, streamId)
	go func(reqLock *kvRequest) {
		defer idx.releaseStreamRequestLock(reqLock)
		idx.waitStreamRequestLock(reqLock)
	retryloop:
		for {

			idx.sendMsgToKVSender(cmd)

			if resp, ok := <-respCh; ok {

				switch resp.GetMsgType() {

				case MSG_SUCCESS:
					logging.Errorf("Indexer::stopBucketStream Success Stream %v Bucket %v ",
						streamId, bucket)
					idx.internalRecvCh <- &MsgRecovery{mType: INDEXER_PREPARE_DONE,
						streamId: streamId,
						bucket:   bucket}
					break retryloop

				default:
					//log and retry for all other responses
					respErr := resp.(*MsgError).GetError()
					logging.Errorf("Indexer::stopBucketStream Stream %v Bucket %v "+
						"Error from Projector %v. Retrying.", streamId, bucket, respErr.cause)
					time.Sleep(KV_RETRY_INTERVAL * time.Millisecond)

				}
			}
		}
	}(reqLock)
}

func (idx *indexer) startBucketStream(streamId common.StreamId, bucket string,
	restartTs *common.TsVbuuid) {

	logging.Infof("Indexer::startBucketStream Stream: %v Bucket: %v RestartTS %v",
		streamId, bucket, restartTs)

	var indexList []common.IndexInst
	var bucketUUIDList []string

	switch streamId {

	case common.MAINT_STREAM:

		for _, indexInst := range idx.indexInstMap {

			if indexInst.Defn.Bucket == bucket {
				switch indexInst.State {
				case common.INDEX_STATE_ACTIVE,
					common.INDEX_STATE_INITIAL:
					if indexInst.Stream == streamId {
						indexList = append(indexList, indexInst)
						bucketUUIDList = append(bucketUUIDList, indexInst.Defn.BucketUUID)
					}
				case common.INDEX_STATE_CATCHUP:
					indexList = append(indexList, indexInst)
					bucketUUIDList = append(bucketUUIDList, indexInst.Defn.BucketUUID)
				}
			}
		}

	case common.INIT_STREAM:

		for _, indexInst := range idx.indexInstMap {
			if indexInst.Defn.Bucket == bucket &&
				indexInst.Stream == streamId {
				switch indexInst.State {
				case common.INDEX_STATE_INITIAL,
					common.INDEX_STATE_CATCHUP:
					indexList = append(indexList, indexInst)
					bucketUUIDList = append(bucketUUIDList, indexInst.Defn.BucketUUID)
				}
			}
		}

	default:
		logging.Fatalf("Indexer::startBucketStream \n\t Unsupported StreamId %v", streamId)
		common.CrashOnError(ErrInvalidStream)

	}

	if len(indexList) == 0 {
		logging.Infof("Indexer::startBucketStream Nothing to Start. Stream: %v Bucket: %v",
			streamId, bucket)
		idx.setStreamBucketState(streamId, bucket, STREAM_INACTIVE)
		return
	}

	respCh := make(MsgChannel)
	stopCh := make(StopChannel)

	cmd := &MsgStreamUpdate{mType: OPEN_STREAM,
		streamId:     streamId,
		bucket:       bucket,
		indexList:    indexList,
		restartTs:    restartTs,
		buildTs:      idx.bucketBuildTs[bucket],
		respCh:       respCh,
		stopCh:       stopCh,
		rollbackTime: idx.bucketRollbackTimes[bucket]}

	//send stream update to timekeeper
	if resp := idx.sendStreamUpdateToWorker(cmd, idx.tkCmdCh,
		"Timekeeper"); resp.GetMsgType() != MSG_SUCCESS {
		respErr := resp.(*MsgError).GetError()
		common.CrashOnError(respErr.cause)
	}

	//send stream update to mutation manager
	if resp := idx.sendStreamUpdateToWorker(cmd, idx.mutMgrCmdCh,
		"MutationMgr"); resp.GetMsgType() != MSG_SUCCESS {
		respErr := resp.(*MsgError).GetError()
		common.CrashOnError(respErr.cause)
	}

	if _, ok := idx.streamBucketRequestStopCh[streamId]; !ok {
		idx.streamBucketRequestStopCh[streamId] = make(BucketRequestStopCh)
	}
	idx.streamBucketRequestStopCh[streamId][bucket] = stopCh

	clustAddr := idx.config["clusterAddr"].String()
	numVb := idx.config["numVbuckets"].Int()

	reqLock := idx.acquireStreamRequestLock(bucket, streamId)
	go func(reqLock *kvRequest) {
		defer idx.releaseStreamRequestLock(reqLock)
		idx.waitStreamRequestLock(reqLock)
	retryloop:
		for {
			//validate bucket before every try
			if !ValidateBucket(clustAddr, bucket, bucketUUIDList) {
				logging.Errorf("Indexer::startBucketStream \n\tBucket Not Found "+
					"For Stream %v Bucket %v", streamId, bucket)
				idx.internalRecvCh <- &MsgRecovery{mType: INDEXER_BUCKET_NOT_FOUND,
					streamId: streamId,
					bucket:   bucket,
					inMTR:    true}
				break retryloop
			}

			idx.sendMsgToKVSender(cmd)

			if resp, ok := <-respCh; ok {

				switch resp.GetMsgType() {

				case MSG_SUCCESS_OPEN_STREAM:

					logging.Infof("Indexer::startBucketStream Success "+
						"Stream %v Bucket %v", streamId, bucket)

					//once stream request is successful re-calculate the KV timestamp.
					//This makes sure indexer doesn't use a timestamp which can never
					//be caught up to (due to kv rollback).
					//if there is a failover after this, it will be observed as a rollback
					buildTs, err := computeBucketBuildTs(clustAddr, bucket, numVb)
					if err == nil {
						idx.internalRecvCh <- &MsgRecovery{mType: INDEXER_RECOVERY_DONE,
							streamId:  streamId,
							bucket:    bucket,
							buildTs:   buildTs,
							restartTs: restartTs,
							activeTs:  resp.(*MsgSuccessOpenStream).GetActiveTs()}
						break retryloop
					}

				case INDEXER_ROLLBACK:
					logging.Infof("Indexer::startBucketStream Rollback from "+
						"Projector For Stream %v Bucket %v", streamId, bucket)
					rollbackTs := resp.(*MsgRollback).GetRollbackTs()
					idx.internalRecvCh <- &MsgRecovery{mType: INDEXER_INIT_PREP_RECOVERY,
						streamId:  streamId,
						bucket:    bucket,
						restartTs: rollbackTs}
					break retryloop

				default:
					//log and retry for all other responses
					respErr := resp.(*MsgError).GetError()

					state := idx.getStreamBucketState(streamId, bucket)

					if state == STREAM_PREPARE_RECOVERY || state == STREAM_INACTIVE {
						logging.Errorf("Indexer::startBucketStream Stream %v Bucket %v "+
							"Error from Projector %v. Not Retrying. State %v", streamId, bucket,
							respErr.cause, state)

						idx.internalRecvCh <- &MsgRecovery{mType: INDEXER_MTR_FAIL,
							streamId: streamId,
							bucket:   bucket,
							inMTR:    true}

						break retryloop

					} else {
						logging.Errorf("Indexer::startBucketStream Stream %v Bucket %v "+
							"Error from Projector %v. Retrying.", streamId, bucket,
							respErr.cause)
						time.Sleep(KV_RETRY_INTERVAL * time.Millisecond)
					}
				}
			}
		}
	}(reqLock)
}

func (idx *indexer) processRollback(streamId common.StreamId,
	bucket string, rollbackTs *common.TsVbuuid) (*common.TsVbuuid, error) {

	if streamId == common.MAINT_STREAM {
		idx.bucketRollbackTimes[bucket] = time.Now().UnixNano()
	}

	//send to storage manager to rollback
	msg := &MsgRollback{streamId: streamId,
		bucket:       bucket,
		rollbackTs:   rollbackTs,
		rollbackTime: idx.bucketRollbackTimes[bucket]}

	if streamId == common.MAINT_STREAM {
		idx.scanCoordCmdCh <- msg
		<-idx.scanCoordCmdCh
	}

	idx.storageMgrCmdCh <- msg
	res := <-idx.storageMgrCmdCh

	//notify storage rollback done
	if streamId == common.MAINT_STREAM {
		msg.rollbackTime = 0
		idx.scanCoordCmdCh <- msg
		<-idx.scanCoordCmdCh
	}

	if res.GetMsgType() != MSG_ERROR {
		rollbackTs := res.(*MsgRollback).GetRollbackTs()
		return rollbackTs, nil
	} else {
		logging.Fatalf("Indexer::processRollback Error during Rollback %v", res)
		respErr := res.(*MsgError).GetError()
		return nil, respErr.cause
	}

}

//helper function to init streamFlush map for all streams
func (idx *indexer) initStreamFlushMap() {

	for i := 0; i < int(common.ALL_STREAMS); i++ {
		idx.streamBucketFlushInProgress[common.StreamId(i)] = make(BucketFlushInProgressMap)
		idx.streamBucketObserveFlushDone[common.StreamId(i)] = make(BucketObserveFlushDoneMap)
	}
}

func (idx *indexer) notifyFlushObserver(msg Message) {

	//if there is any observer for flush, notify
	bucket := msg.(*MsgMutMgrFlushDone).GetBucket()
	streamId := msg.(*MsgMutMgrFlushDone).GetStreamId()

	if notifyCh, ok := idx.streamBucketObserveFlushDone[streamId][bucket]; ok {
		if notifyCh != nil {
			notifyCh <- msg
			//wait for a sync response that cleanup is done.
			//notification is sent one by one as there is no lock
			<-notifyCh
		}
	}
	return
}

func (idx *indexer) processDropAfterFlushDone(indexInst common.IndexInst,
	notifyCh MsgChannel, clientCh MsgChannel) {

	select {
	case <-notifyCh:
		idx.cleanupIndex(indexInst, clientCh)
	}

	streamId := indexInst.Stream
	bucket := indexInst.Defn.Bucket
	idx.streamBucketObserveFlushDone[streamId][bucket] = nil

	//indicate done
	close(notifyCh)
}

func (idx *indexer) checkDuplicateDropRequest(indexInst common.IndexInst,
	respCh MsgChannel) bool {

	//if there is any observer for flush done for this stream/bucket,
	//drop is already in progress
	stream := indexInst.Stream
	bucket := indexInst.Defn.Bucket
	if obs, ok := idx.streamBucketObserveFlushDone[stream][bucket]; ok && obs != nil {

		errStr := "Index Drop Already In Progress."

		logging.Errorf(errStr)
		if respCh != nil {
			respCh <- &MsgError{
				err: Error{code: ERROR_INDEX_DROP_IN_PROGRESS,
					severity: FATAL,
					cause:    errors.New(errStr),
					category: INDEXER}}

		}
		return true
	}
	return false
}

func (idx *indexer) bootstrap(snapshotNotifych chan IndexSnapshot) (bool, error) {

	logging.Infof("Indexer::indexer version %v", common.INDEXER_CUR_VERSION)
	idx.genIndexerId()

	//set topic names based on indexer id
	idx.initStreamTopicName()

	//close any old streams with projector
	idx.closeAllStreams()

	idx.recoverRebalanceState()

	//recover indexes from local metadata
	needsRestart, err := idx.initFromPersistedState()
	if err != nil {
		return needsRestart, err
	}

	//Start Storage Manager
	var res Message
	idx.storageMgr, res = NewStorageManager(idx.storageMgrCmdCh, idx.wrkrRecvCh,
		idx.indexPartnMap, idx.config, snapshotNotifych)
	if res.GetMsgType() == MSG_ERROR {
		err := res.(*MsgError).GetError()
		logging.Fatalf("Indexer::NewIndexer Storage Manager Init Error %v", err)
		return needsRestart, err.cause
	}

	//send updated maps
	msgUpdateIndexInstMap := idx.newIndexInstMsg(idx.indexInstMap)
	msgUpdateIndexPartnMap := &MsgUpdatePartnMap{indexPartnMap: idx.indexPartnMap}

	// Distribute current stats object and index information
	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, msgUpdateIndexPartnMap); err != nil {
		common.CrashOnError(err)
	}

	if common.GetStorageMode() == common.MOI {
		idx.clustMgrAgentCmdCh <- &MsgClustMgrLocal{
			mType: CLUST_MGR_GET_LOCAL,
			key:   INDEXER_STATE_KEY,
		}

		respMsg := <-idx.clustMgrAgentCmdCh
		resp := respMsg.(*MsgClustMgrLocal)

		val := resp.GetValue()
		err := resp.GetError()

		if err == nil {
			if val == fmt.Sprintf("%s", common.INDEXER_PAUSED) {
				idx.handleIndexerPause(&MsgIndexerState{mType: INDEXER_PAUSE})
			}
			logging.Infof("Indexer::bootstrap Recovered Indexer State %v", val)

		} else if strings.Contains(err.Error(), forestdb.FDB_RESULT_KEY_NOT_FOUND.Error()) {
			//if there is no IndexerState, nothing to do
			logging.Infof("Indexer::bootstrap No Previous Indexer State Recovered")

		} else {
			logging.Fatalf("Indexer::bootstrap Error Fetching IndexerState From Local"+
				"Meta Storage. Err %v", err)
			common.CrashOnError(err)
		}

		//check if Paused state is required
		memory_quota := idx.config["settings.memory_quota"].Uint64()
		high_mem_mark := idx.config["high_mem_mark"].Float64()

		//free memory after bootstrap before deciding to pause
		start := time.Now()
		debug.FreeOSMemory()
		elapsed := time.Since(start)
		logging.Infof("Indexer::bootstrap ManualGC Time Taken %v", elapsed)
		mm.FreeOSMemory()

		mem_used, _, _ := idx.memoryUsed(true)
		if float64(mem_used) > (high_mem_mark * float64(memory_quota)) {
			logging.Infof("Indexer::bootstrap MemoryUsed %v", mem_used)
			idx.handleIndexerPause(&MsgIndexerState{mType: INDEXER_PAUSE})
		}
	}

	// ready to process DDL
	msg := &MsgClustMgrUpdate{mType: CLUST_MGR_INDEXER_READY}
	if err := idx.sendMsgToClusterMgr(msg); err != nil {
		return needsRestart, err
	}

	//if there are no indexes, return from here
	if len(idx.indexInstMap) == 0 {
		return needsRestart, nil
	}

	if ok := idx.startStreams(); !ok {
		return needsRestart, errors.New("Unable To Start DCP Streams")
	}

	return needsRestart, nil
}

func (idx *indexer) recoverRebalanceState() {

	idx.clustMgrAgentCmdCh <- &MsgClustMgrLocal{
		mType: CLUST_MGR_GET_LOCAL,
		key:   RebalanceRunning,
	}

	respMsg := <-idx.clustMgrAgentCmdCh
	resp := respMsg.(*MsgClustMgrLocal)

	val := resp.GetValue()
	err := resp.GetError()

	if err == nil {
		idx.rebalanceRunning = true
	} else if strings.Contains(err.Error(), forestdb.FDB_RESULT_KEY_NOT_FOUND.Error()) {
		idx.rebalanceRunning = false
	} else {
		logging.Fatalf("Indexer::recoverRebalanceState Error Fetching RebalanceRunning From Local "+
			"Meta Storage. Err %v", err)
		idx.rebalanceRunning = false
	}

	idx.clustMgrAgentCmdCh <- &MsgClustMgrLocal{
		mType: CLUST_MGR_GET_LOCAL,
		key:   RebalanceTokenTag,
	}

	respMsg = <-idx.clustMgrAgentCmdCh
	resp = respMsg.(*MsgClustMgrLocal)

	val = resp.GetValue()
	err = resp.GetError()

	if err == nil {
		var rebalToken RebalanceToken
		err = json.Unmarshal([]byte(val), &rebalToken)
		if err != nil {
			logging.Errorf("Indexer::recoverRebalanceState Error Unmarshalling RebalanceToken %v", err)
			common.CrashOnError(err)
		}
		idx.rebalanceToken = &rebalToken
	} else if strings.Contains(err.Error(), forestdb.FDB_RESULT_KEY_NOT_FOUND.Error()) {
		idx.rebalanceToken = nil
	} else {
		logging.Fatalf("Indexer::recoverRebalanceState Error Fetching RebalanceToken From Local "+
			"Meta Storage. Err %v", err)
		idx.rebalanceToken = nil
	}

	logging.Infof("Indexer::recoverRebalanceState RebalanceRunning %v RebalanceToken %v", idx.rebalanceRunning, idx.rebalanceToken)
}

func (idx *indexer) genIndexerId() {

	if idx.enableManager {

		//try to fetch IndexerId from manager
		idx.clustMgrAgentCmdCh <- &MsgClustMgrLocal{
			mType: CLUST_MGR_GET_LOCAL,
			key:   INDEXER_ID_KEY,
		}

		respMsg := <-idx.clustMgrAgentCmdCh
		resp := respMsg.(*MsgClustMgrLocal)

		val := resp.GetValue()
		err := resp.GetError()

		if err == nil {
			idx.id = val
		} else if strings.Contains(err.Error(), forestdb.FDB_RESULT_KEY_NOT_FOUND.Error()) {
			//if there is no IndexerId, generate and store in manager

			//id, err := common.NewUUID()
			//if err == nil {
			//	idx.id = id.Str()
			//} else {
			//	idx.id = strconv.Itoa(rand.Int())
			//}

			idx.id = idx.config["nodeuuid"].String()
			idx.clustMgrAgentCmdCh <- &MsgClustMgrLocal{
				mType: CLUST_MGR_SET_LOCAL,
				key:   INDEXER_ID_KEY,
				value: idx.id,
			}

			respMsg := <-idx.clustMgrAgentCmdCh
			resp := respMsg.(*MsgClustMgrLocal)

			errMsg := resp.GetError()
			if errMsg != nil {
				logging.Fatalf("Indexer::genIndexerId Unable to set IndexerId In Local"+
					"Meta Storage. Err %v", errMsg)
				common.CrashOnError(errMsg)
			}

		} else {
			logging.Fatalf("Indexer::genIndexerId Error Fetching IndexerId From Local"+
				"Meta Storage. Err %v", err)
			common.CrashOnError(err)
		}
	} else {
		//assume 1 without manager
		idx.id = "1"
	}

	logging.Infof("Indexer Id %v", idx.id)

}

func (idx *indexer) initFromPersistedState() (bool, error) {

	err := idx.recoverIndexInstMap()
	if err != nil {
		logging.Fatalf("Indexer::initFromPersistedState Error Recovering IndexInstMap %v", err)
		return false, err
	}

	logging.Infof("Indexer::initFromPersistedState Recovered IndexInstMap %v", idx.indexInstMap)

	idx.validateIndexInstMap()

	needsRestart := idx.upgradeStorage()

	// Set the storage mode specific to this indexer node
	common.SetStorageMode(idx.getLocalStorageMode(idx.config))
	initStorageSettings(idx.config)
	logging.Infof("Indexer::local storage mode %v", common.GetStorageMode().String())

	for _, inst := range idx.indexInstMap {

		if inst.State != common.INDEX_STATE_DELETED {
			for _, partnDefn := range inst.Pc.GetAllPartitions() {
				idx.stats.AddPartition(inst.InstId, inst.Defn.Bucket, inst.Defn.Name, inst.ReplicaId, partnDefn.GetPartitionId())
			}
		}

		//allocate partition/slice
		var partnInstMap PartitionInstMap
		var err error
		if partnInstMap, err = idx.initPartnInstance(inst, nil); err != nil {
			return needsRestart, err
		}

		idx.indexInstMap[inst.InstId] = inst
		idx.indexPartnMap[inst.InstId] = partnInstMap
	}

	return needsRestart, nil

}

func (idx *indexer) recoverIndexInstMap() error {

	if idx.enableManager {
		return idx.recoverInstMapFromManager()
	} else {
		return idx.recoverInstMapFromFile()
	}

}

func (idx *indexer) recoverInstMapFromManager() error {

	idx.clustMgrAgentCmdCh <- &MsgClustMgrTopology{}

	resp := <-idx.clustMgrAgentCmdCh

	switch resp.GetMsgType() {

	case CLUST_MGR_GET_GLOBAL_TOPOLOGY:
		idx.indexInstMap = resp.(*MsgClustMgrTopology).GetInstMap()

	case MSG_ERROR:
		err := resp.(*MsgError).GetError()
		common.CrashOnError(err.cause)

	default:
		common.CrashOnError(errors.New("Unknown Response"))
	}
	return nil
}

func (idx *indexer) recoverInstMapFromFile() error {

	var dbfile *forestdb.File
	var meta *forestdb.KVStore
	var err error

	//read indexer state and local state context
	config := forestdb.DefaultConfig()

	if dbfile, err = forestdb.Open("meta", config); err != nil {
		return err
	}
	defer dbfile.Close()

	kvconfig := forestdb.DefaultKVStoreConfig()
	// Make use of default kvstore provided by forestdb
	if meta, err = dbfile.OpenKVStore("default", kvconfig); err != nil {
		return err
	}

	defer meta.Close()

	//read the instance map
	var instBytes []byte
	instBytes, err = meta.GetKV([]byte(INST_MAP_KEY_NAME))

	//forestdb reports get in a non-existent key as an
	//error, skip that
	if err != nil && err != forestdb.FDB_RESULT_KEY_NOT_FOUND {
		return err
	}

	//if there is no instance map available, proceed with
	//normal init
	if len(instBytes) == 0 {
		return nil
	}

	decBuf := bytes.NewBuffer(instBytes)
	dec := gob.NewDecoder(decBuf)
	err = dec.Decode(&idx.indexInstMap)

	if err != nil {
		logging.Fatalf("Indexer::recoverInstMapFromFile Decode Error %v", err)
		return err
	}
	return nil
}

func (idx *indexer) upgradeStorage() bool {

	if common.GetBuildMode() != common.ENTERPRISE {
		return false
	}

	disable := idx.config["settings.storage_mode.disable_upgrade"].Bool()
	override := idx.getStorageModeOverride(idx.config)
	logging.Infof("indexer.upgradeStorage: check index for storage upgrade.   disable %v overrride %v", disable, override)

	//
	// First try to upgrade/downgrade storage mode of each index, based on index's current storage mode
	//
	for instId, index := range idx.indexInstMap {

		if index.State != common.INDEX_STATE_DELETED || index.State != common.INDEX_STATE_ERROR {

			indexStorageMode := common.IndexTypeToStorageMode(index.Defn.Using)
			targetStorageMode := idx.promoteStorageModeIfNecessaryInternal(indexStorageMode, disable, override)

			if indexStorageMode != targetStorageMode {
				logging.Warnf("Indexer::upgradeStorage: Index (%v, %v) storage mode %v need upgrade/downgrade. Upgrade/Downgrade index storge mode to %v.",
					index.Defn.Bucket, index.Defn.Name, indexStorageMode, targetStorageMode)

				idx.upgradeSingleIndex(&index, targetStorageMode)
				idx.indexInstMap[instId] = index
			}
		}
	}

	// Sanity Check.  Make sure that all indexes have the same storage mode.   Indexes could end up having different storage mode if
	// 1) When this funtion is run, user changes disable_upgrade.   Indexer then restart/crash.   Some indexes could have changed their storge mode
	//    while some still haven't.
	// 2) When indexer restarts, this function will be run again.  But since disable_upgrade, it could leave some indexes in their original storage mode.
	//
	// The following logic is to detect if indexes are in mixed storage mode, it will try to force them to converge to a single storage mode.
	//
	if idx.getIndexStorageMode() == common.MIXED {

		for instId, index := range idx.indexInstMap {

			if index.State != common.INDEX_STATE_DELETED || index.State != common.INDEX_STATE_ERROR {

				indexStorageMode := common.IndexTypeToStorageMode(index.Defn.Using)

				if disable && indexStorageMode == common.PLASMA {
					indexStorageMode = common.FORESTDB
				} else if !disable && indexStorageMode == common.FORESTDB {
					indexStorageMode = common.PLASMA
				}

				targetStorageMode := idx.promoteStorageModeIfNecessaryInternal(indexStorageMode, disable, override)
				indexStorageMode = common.IndexTypeToStorageMode(index.Defn.Using)

				if indexStorageMode != targetStorageMode {
					logging.Warnf("Indexer::upgradeStorage: Index (%v, %v) storage mode %v need upgrade/downgrade. Upgrade/Downgrade index storge mode to %v.",
						index.Defn.Bucket, index.Defn.Name, indexStorageMode, targetStorageMode)

					idx.upgradeSingleIndex(&index, targetStorageMode)
					idx.indexInstMap[instId] = index
				}
			}
		}
	}

	// If storage mode is different from bootstrap, then have to restart indexer.
	s := idx.getIndexStorageMode()

	if s == common.MIXED {
		logging.Errorf("Indexer is mixed storage mode after storage upgrade")

	} else if s != common.NOT_SET {
		if s != idx.bootstrapStorageMode {
			logging.Infof("Updating bootstrap storage mode to %v", s)
			idx.postIndexStorageModeForBootstrap(idx.config, s)
			return true
		}
	}

	return false
}

func (idx *indexer) upgradeSingleIndex(inst *common.IndexInst, storageMode common.StorageMode) {

	logging.Infof("Indexer::upgradeSingleIndex: Upgrade index (%v, %v) to new storage (%v)",
		inst.Defn.Bucket, inst.Defn.Name, storageMode)

	// update index instance
	inst.Defn.Using = common.StorageModeToIndexType(storageMode)
	inst.State = common.INDEX_STATE_CREATED
	inst.Stream = common.NIL_STREAM
	inst.Error = ""

	// remove old files
	storage_dir := idx.config["storage_dir"].String()

	partnDefnList := inst.Pc.GetAllPartitions()
	for _, partnDefn := range partnDefnList {
		path := filepath.Join(storage_dir, IndexPath(inst, partnDefn.GetPartitionId(), SliceId(0)))
		if err := os.RemoveAll(path); err != nil {
			common.CrashOnError(err)
		}
	}

	// update metadata
	msg := &MsgClustMgrResetIndex{
		inst: *inst,
	}
	idx.sendMsgToClusterMgr(msg)
}

func (idx *indexer) validateIndexInstMap() {

	bucketUUIDMap := make(map[string]bool)
	bucketValid := make(map[string]bool)

	for instId, index := range idx.indexInstMap {

		//if an index has NIL_STREAM:
		//for non-deferred index,this means the Indexer
		//failed while processing the request, cleanup the index.
		//for deferred index in CREATED state, update the state of the index
		//to READY in manager, so that build index request can be processed.
		if index.Stream == common.NIL_STREAM {
			if index.Defn.Deferred || index.Scheduled {
				if index.State == common.INDEX_STATE_CREATED {
					logging.Warnf("Indexer::validateIndexInstMap State %v Stream %v Deferred %v Found. "+
						"Updating State to Ready %v", index.State, index.Stream, index.Defn.Deferred, index)
					index.State = common.INDEX_STATE_READY
					idx.indexInstMap[instId] = index

					instIds := []common.IndexInstId{index.InstId}
					if err := idx.updateMetaInfoForIndexList(instIds, true, false, false, false, true, false, false, false, nil); err != nil {
						common.CrashOnError(err)
					}
				}
			} else {
				logging.Warnf("Indexer::validateIndexInstMap State %v Stream %v Deferred %v Not Valid For Recovery. "+
					"Cleanup Index %v", index.State, index.Stream, index.Defn.Deferred, index)
				idx.cleanupIndexMetadata(index)
				delete(idx.indexInstMap, instId)
				continue
			}

		}

		//for indexer, Ready state doesn't matter. Till build index is received,
		//the index stays in Created state.
		if index.State == common.INDEX_STATE_READY {
			index.State = common.INDEX_STATE_CREATED
			idx.indexInstMap[instId] = index
		}

		//only indexes in created, initial, catchup, active state
		//are valid for recovery
		if !isValidRecoveryState(index.State) {
			logging.Warnf("Indexer::validateIndexInstMap State %v Not Recoverable. "+
				"Not Recovering Index %v", index.State, index)

			if index.State == common.INDEX_STATE_DELETED {
				logging.Warnf("Indexer::validateIndexInstMap Found Index in State %v. "+
					"Cleaning up Index Data %v", index.State, index)
				err := idx.forceCleanupIndexData(&index, SliceId(0))
				if err == nil {
					idx.cleanupIndexMetadata(index)
				}
			} else {
				idx.cleanupIndexMetadata(index)
			}
			delete(idx.indexInstMap, instId)
			continue
		}

		//if bucket doesn't exist, cleanup
		bucketUUID := index.Defn.Bucket + "::" + index.Defn.BucketUUID
		if _, ok := bucketUUIDMap[bucketUUID]; !ok {

			bucket := index.Defn.Bucket
			bucketUUIDValid := ValidateBucket(idx.config["clusterAddr"].String(), bucket, []string{index.Defn.BucketUUID})
			bucketUUIDMap[bucketUUID] = bucketUUIDValid

			if _, ok := bucketValid[bucket]; ok {
				bucketValid[bucket] = bucketValid[bucket] && bucketUUIDValid
			} else {
				bucketValid[bucket] = bucketUUIDValid
			}

			//also set the buildTs for initial state index.
			//TODO buildTs to be part of index instance
			if bucketUUIDValid {
				cluster := idx.config["clusterAddr"].String()
				numVbuckets := idx.config["numVbuckets"].Int()

				var buildTs Timestamp
				fn := func(r int, err error) error {
					if r > 0 {
						logging.Warnf("Indexer::validateIndexInstMap Bucket %s is not yet ready (err = %v) Retrying(%d)..", bucket, err, r)
					}
					buildTs, err = GetCurrentKVTs(cluster, "default", bucket, numVbuckets)
					return err
				}
				rh := common.NewRetryHelper(MAX_KVWARMUP_RETRIES, time.Second, 1, fn)
				err := rh.Run()
				if err != nil {
					logging.Fatalf("Indexer::validateIndexInstMap Bucket %s not ready even after max retries. Restarting indexer.", bucket)
					os.Exit(1)
				} else {
					idx.bucketBuildTs[bucket] = buildTs
				}
			}
		}
	}

	// handle bucket that fails validation
	for bucket, valid := range bucketValid {
		if !valid {
			instList := idx.deleteIndexInstOnDeletedBucket(bucket, common.NIL_STREAM)
			for _, instId := range instList {
				index := idx.indexInstMap[instId]
				logging.Warnf("Indexer::validateIndexInstMap \n\t Bucket %v Not Found."+
					"Not Recovering Index %v", bucket, index)
				delete(idx.indexInstMap, instId)
			}
		}
	}

	idx.checkMissingMaintBucket()

}

//force cleanup of index data should only be used when storage manager has not yet
//been initialized
func (idx *indexer) forceCleanupIndexData(inst *common.IndexInst, sliceId SliceId) error {

	if inst.RState != common.REBAL_MERGED {
		storage_dir := idx.config["storage_dir"].String()

		partnDefnList := inst.Pc.GetAllPartitions()
		for _, partnDefn := range partnDefnList {
			path := filepath.Join(storage_dir, IndexPath(inst, partnDefn.GetPartitionId(), sliceId))

			logging.Infof("Indexer::forceCleanupIndexData Cleaning Up partition %v, "+
				"IndexInstId %v, IndexDefnId %v ", partnDefn.GetPartitionId(), inst.InstId, inst.Defn.DefnId)

			//cleanup the disk directory
			if err := os.RemoveAll(path); err != nil {
				logging.Errorf("Indexer::forceCleanupIndexData Error Cleaning Up partition %v, "+
					"IndexInstId %v, IndexDefnId %v. Error %v", partnDefn.GetPartitionId(), inst.InstId, inst.Defn.DefnId, err)
				return err
			}
		}
	}
	return nil

}

//On recovery, deleted indexes are ignored. There can be
//a case where the last maint stream index was dropped and
//indexer crashes while there is an index in Init stream.
//Such indexes need to be moved to Maint Stream.
func (idx *indexer) checkMissingMaintBucket() {

	missingBucket := make(map[string]bool)

	//get all unique buckets in init stream
	for _, index := range idx.indexInstMap {
		if index.Stream == common.INIT_STREAM {
			missingBucket[index.Defn.Bucket] = true
		}
	}

	//remove those present in maint stream
	for _, index := range idx.indexInstMap {
		if index.Stream == common.MAINT_STREAM {
			if _, ok := missingBucket[index.Defn.Bucket]; ok {
				delete(missingBucket, index.Defn.Bucket)
			}
		}
	}

	//move indexes of these buckets to Maint Stream
	if len(missingBucket) > 0 {
		var updatedList []common.IndexInstId
		for bucket, _ := range missingBucket {
			//for all indexes for this bucket
			for instId, index := range idx.indexInstMap {
				if index.Defn.Bucket == bucket {
					//state is set to Initial, no catchup in Maint
					index.State = common.INDEX_STATE_INITIAL
					index.Stream = common.MAINT_STREAM
					idx.indexInstMap[instId] = index
					updatedList = append(updatedList, instId)
				}
			}
		}

		if idx.enableManager {
			if err := idx.updateMetaInfoForIndexList(updatedList,
				true, true, false, false, true, false, false, false, nil); err != nil {
				common.CrashOnError(err)
			}
		}
	}
}

func isValidRecoveryState(state common.IndexState) bool {

	switch state {

	case common.INDEX_STATE_CREATED,
		common.INDEX_STATE_INITIAL,
		common.INDEX_STATE_CATCHUP,
		common.INDEX_STATE_ACTIVE:
		return true

	default:
		return false

	}

}

func (idx *indexer) startStreams() bool {

	//Start MAINT_STREAM
	restartTs := idx.makeRestartTs(common.MAINT_STREAM)

	idx.stateLock.Lock()
	idx.streamBucketStatus[common.MAINT_STREAM] = make(BucketStatus)
	idx.stateLock.Unlock()

	for bucket, ts := range restartTs {
		idx.bucketRollbackTimes[bucket] = time.Now().UnixNano()
		idx.startBucketStream(common.MAINT_STREAM, bucket, ts)
		idx.setStreamBucketState(common.MAINT_STREAM, bucket, STREAM_ACTIVE)
	}

	//Start INIT_STREAM
	restartTs = idx.makeRestartTs(common.INIT_STREAM)

	idx.stateLock.Lock()
	idx.streamBucketStatus[common.INIT_STREAM] = make(BucketStatus)
	idx.stateLock.Unlock()

	for bucket, ts := range restartTs {
		idx.startBucketStream(common.INIT_STREAM, bucket, ts)
		idx.setStreamBucketState(common.INIT_STREAM, bucket, STREAM_ACTIVE)
	}

	return true

}

func (idx *indexer) makeRestartTs(streamId common.StreamId) map[string]*common.TsVbuuid {

	restartTs := make(map[string]*common.TsVbuuid)

	for idxInstId, partnMap := range idx.indexPartnMap {
		idxInst := idx.indexInstMap[idxInstId]

		if idxInst.Stream == streamId {

			for _, partnInst := range partnMap {

				sc := partnInst.Sc

				//there is only one slice for now
				slice := sc.GetSliceById(0)

				infos, err := slice.GetSnapshots()
				// TODO: Proper error handling if possible
				if err != nil {
					panic("Unable read snapinfo -" + err.Error())
				}

				s := NewSnapshotInfoContainer(infos)
				latestSnapInfo := s.GetLatest()

				//There may not be a valid snapshot info if no flush
				//happened for this index
				if latestSnapInfo != nil {
					ts := latestSnapInfo.Timestamp()
					if oldTs, ok := restartTs[idxInst.Defn.Bucket]; ok {
						if !ts.AsRecent(oldTs) {
							restartTs[idxInst.Defn.Bucket] = ts
						}
					} else {
						restartTs[idxInst.Defn.Bucket] = ts
					}
				} else {
					//set restartTs to nil for this bucket
					if _, ok := restartTs[idxInst.Defn.Bucket]; !ok {
						restartTs[idxInst.Defn.Bucket] = nil
					}
				}
			}
		}
	}
	return restartTs
}

func (idx *indexer) closeAllStreams() {

	respCh := make(MsgChannel)

	for i := 0; i < int(common.ALL_STREAMS); i++ {

		//skip for nil and catchup stream
		if i == int(common.NIL_STREAM) ||
			i == int(common.CATCHUP_STREAM) {
			continue
		}

		cmd := &MsgStreamUpdate{mType: CLOSE_STREAM,
			streamId: common.StreamId(i),
			respCh:   respCh,
		}

	retryloop:
		for {
			idx.sendMsgToKVSender(cmd)

			if resp, ok := <-respCh; ok {

				switch resp.GetMsgType() {

				case MSG_SUCCESS:
					break retryloop

				default:
					//log and retry for all other responses
					respErr := resp.(*MsgError).GetError()
					logging.Fatalf("Indexer::closeAllStreams Stream %v "+
						"Projector health check needed, indexer can not proceed, Error received %v. Retrying.",
						common.StreamId(i), respErr.cause)
					time.Sleep(KV_RETRY_INTERVAL * time.Millisecond)
				}
			}
		}
	}
}

func (idx *indexer) checkStreamRequestPending(streamId common.StreamId, bucket string) bool {

	if bmap, ok := idx.streamBucketRequestStopCh[streamId]; ok {
		if stopCh, ok := bmap[bucket]; ok {
			if stopCh != nil {
				logging.Errorf("Indexer::checkStreamRequestPending %v %v %v", bucket, streamId, idx.streamBucketRequestStopCh)
				return true
			}
		}
	}

	return false
}

func (idx *indexer) updateMetaInfoForBucket(bucket string,
	updateState bool, updateStream bool, updateError bool,
	updateRState bool, updatePartition bool, updateVersion bool) error {

	var instIdList []common.IndexInstId
	for _, inst := range idx.indexInstMap {
		if inst.Defn.Bucket == bucket {
			instIdList = append(instIdList, inst.InstId)
		}
	}

	if len(instIdList) != 0 {
		return idx.updateMetaInfoForIndexList(instIdList, updateState,
			updateStream, updateError, false, updateRState, false, updatePartition, updateVersion, nil)
	} else {
		return nil
	}

}

func (idx *indexer) updateMetaInfoForIndexList(instIdList []common.IndexInstId,
	updateState bool, updateStream bool, updateError bool,
	updateBuildTs bool, updateRState bool, syncUpdate bool,
	updatePartitions bool, updateVersion bool, respCh chan error) error {

	var indexList []common.IndexInst
	for _, instId := range instIdList {
		indexList = append(indexList, idx.indexInstMap[instId])
	}

	updatedFields := MetaUpdateFields{
		state:      updateState,
		stream:     updateStream,
		err:        updateError,
		buildTs:    updateBuildTs,
		rstate:     updateRState,
		partitions: updatePartitions,
		version:    updateVersion,
	}

	msg := &MsgClustMgrUpdate{
		mType:         CLUST_MGR_UPDATE_TOPOLOGY_FOR_INDEX,
		indexList:     indexList,
		updatedFields: updatedFields,
		syncUpdate:    syncUpdate,
		respCh:        respCh}

	return idx.sendMsgToClusterMgr(msg)

}

func (idx *indexer) updateMetaInfoForDeleteBucket(bucket string, streamId common.StreamId) error {

	msg := &MsgClustMgrUpdate{mType: CLUST_MGR_DEL_BUCKET, bucket: bucket, streamId: streamId}
	return idx.sendMsgToClusterMgr(msg)
}

func (idx *indexer) cleanupIndexMetadata(indexInst common.IndexInst) error {

	temp := indexInst
	temp.Pc = nil
	msg := &MsgClustMgrUpdate{mType: CLUST_MGR_CLEANUP_INDEX, indexList: []common.IndexInst{temp}}
	return idx.sendMsgToClusterMgr(msg)
}

func (idx *indexer) sendMsgToClusterMgr(msg Message) error {

	idx.clustMgrAgentCmdCh <- msg

	if res, ok := <-idx.clustMgrAgentCmdCh; ok {

		switch res.GetMsgType() {

		case MSG_SUCCESS:
			return nil

		case MSG_ERROR:
			logging.Errorf("Indexer::sendMsgToClusterMgr Error "+
				"from Cluster Manager %v", res)
			err := res.(*MsgError).GetError()
			return err.cause

		default:
			logging.Fatalf("Indexer::sendMsgToClusterMgr Unknown Response "+
				"from Cluster Manager %v", res)
			common.CrashOnError(errors.New("Unknown Response"))

		}

	} else {

		logging.Fatalf("clustMgrAgent::sendMsgToClusterMgr Unexpected Channel Close " +
			"from Cluster Manager")
		common.CrashOnError(errors.New("Unknown Response"))

	}

	return nil
}

func (idx *indexer) handleSetLocalMeta(msg Message) {

	key := msg.(*MsgClustMgrLocal).GetKey()
	value := msg.(*MsgClustMgrLocal).GetValue()

	respch := msg.(*MsgClustMgrLocal).GetRespCh()
	checkDDL := msg.(*MsgClustMgrLocal).GetCheckDDL()

	if key == RebalanceRunning && checkDDL {
		if idx.checkDDLInProgress() {
			logging.Errorf("ServiceMgr::handleSetLocalMeta Found DDL Running. Key %v", key)
			err := errors.New("indexer rebalance failure - ddl in progress")
			respch <- &MsgClustMgrLocal{
				mType: CLUST_MGR_SET_LOCAL,
				key:   key,
				value: value,
				err:   err,
			}
			return
		}
	}

	idx.clustMgrAgentCmdCh <- msg
	respMsg := <-idx.clustMgrAgentCmdCh

	err := respMsg.(*MsgClustMgrLocal).GetError()
	if err == nil {
		if key == RebalanceRunning {
			idx.rebalanceRunning = true

			msg := &MsgClustMgrUpdate{mType: CLUST_MGR_REBALANCE_RUNNING}
			idx.sendMsgToClusterMgr(msg)

		} else if key == RebalanceTokenTag {
			var rebalToken RebalanceToken
			if err := json.Unmarshal([]byte(value), &rebalToken); err == nil {
				idx.rebalanceToken = &rebalToken
			}
		}
	}

	respch <- respMsg
}

func (idx *indexer) handleGetLocalMeta(msg Message) {

	idx.clustMgrAgentCmdCh <- msg
	respMsg := <-idx.clustMgrAgentCmdCh
	respch := msg.(*MsgClustMgrLocal).GetRespCh()
	respch <- respMsg

}

func (idx *indexer) handleDelLocalMeta(msg Message) {

	idx.clustMgrAgentCmdCh <- msg
	respMsg := <-idx.clustMgrAgentCmdCh

	key := msg.(*MsgClustMgrLocal).GetKey()

	respch := msg.(*MsgClustMgrLocal).GetRespCh()
	err := respMsg.(*MsgClustMgrLocal).GetError()

	if err == nil {
		if key == RebalanceRunning {
			// clean up the projector stream
			// 1) If proxy inst is merged, it will be removed from stream.
			// 2) If real inst is pruned, it will be updated with correct partition list.
			idx.updateStreamForRebalance(true)

			idx.rebalanceRunning = false
		} else if key == RebalanceTokenTag {
			idx.rebalanceToken = nil
		}
	}

	respch <- respMsg
}

func (idx *indexer) bulkUpdateError(instIdList []common.IndexInstId,
	errStr string) {

	for _, instId := range instIdList {
		idxInst := idx.indexInstMap[instId]
		idxInst.Error = errStr
		idx.indexInstMap[instId] = idxInst
	}

}

func (idx *indexer) updateError(instId common.IndexInstId,
	errStr string) {

	idxInst := idx.indexInstMap[instId]
	idxInst.Error = errStr
	idx.indexInstMap[instId] = idxInst

}

func (idx *indexer) bulkUpdateState(instIdList []common.IndexInstId,
	state common.IndexState) {

	for _, instId := range instIdList {
		idxInst := idx.indexInstMap[instId]
		idxInst.State = state
		idx.indexInstMap[instId] = idxInst
	}
}

func (idx *indexer) bulkUpdateRState(instIdList []common.IndexInstId, reqCtx *common.MetadataRequestContext) {

	for _, instId := range instIdList {
		idxInst := idx.indexInstMap[instId]
		if reqCtx.ReqSource == common.DDLRequestSourceRebalance && idxInst.Version != 0 {
			idxInst.RState = common.REBAL_PENDING
		} else {
			idxInst.RState = common.REBAL_ACTIVE
			logging.Infof("bulkUpdateRState: Index instance %v rstate moved to ACTIVE", idxInst.RState)
		}
		idx.indexInstMap[instId] = idxInst
	}
}

func (idx *indexer) bulkUpdateStream(instIdList []common.IndexInstId,
	stream common.StreamId) {

	for _, instId := range instIdList {
		idxInst := idx.indexInstMap[instId]
		idxInst.Stream = stream
		idx.indexInstMap[instId] = idxInst
	}
}

func (idx *indexer) bulkUpdateBuildTs(instIdList []common.IndexInstId,
	buildTs Timestamp) {

	for _, instId := range instIdList {
		idxInst := idx.indexInstMap[instId]
		buildTs := make([]uint64, len(buildTs))
		for i, ts := range buildTs {
			buildTs[i] = uint64(ts)
		}
		idxInst.BuildTs = buildTs
		idx.indexInstMap[instId] = idxInst
	}
}

func (idx *indexer) checkBucketInRecovery(bucket string,
	instIdList []common.IndexInstId, clientCh MsgChannel, errMap map[common.IndexInstId]error) bool {

	initState := idx.getStreamBucketState(common.INIT_STREAM, bucket)
	maintState := idx.getStreamBucketState(common.MAINT_STREAM, bucket)

	if initState == STREAM_RECOVERY ||
		initState == STREAM_PREPARE_RECOVERY ||
		maintState == STREAM_RECOVERY ||
		maintState == STREAM_PREPARE_RECOVERY {

		if idx.enableManager {
			errStr := fmt.Sprintf("Bucket %v In Recovery", bucket)
			idx.bulkUpdateError(instIdList, errStr)
			for _, instId := range instIdList {
				errMap[instId] = &common.IndexerError{Reason: errStr, Code: common.IndexerInRecovery}
			}
		} else if clientCh != nil {
			clientCh <- &MsgError{
				err: Error{code: ERROR_INDEXER_IN_RECOVERY,
					severity: FATAL,
					cause:    ErrIndexerInRecovery,
					category: INDEXER}}
		}
		return true
	}
	return false
}

func (idx *indexer) checkValidIndexInst(bucket string, instIdList []common.IndexInstId,
	clientCh MsgChannel, errMap map[common.IndexInstId]error) ([]common.IndexInstId, bool) {

	if len(instIdList) == 0 {
		return instIdList, true
	}

	newList := make([]common.IndexInstId, len(instIdList))
	count := 0

	//validate instance list
	for _, instId := range instIdList {
		if index, ok := idx.indexInstMap[instId]; !ok {
			if idx.enableManager {
				errStr := fmt.Sprintf("Unknown Index Instance %v In Build Request", instId)
				idx.updateError(instId, errStr)
				errMap[instId] = &common.IndexerError{Reason: errStr, Code: common.IndexNotExist}
			} else if clientCh != nil {
				clientCh <- &MsgError{
					err: Error{code: ERROR_INDEXER_UNKNOWN_INDEX,
						severity: FATAL,
						cause:    common.ErrIndexNotFound,
						category: INDEXER}}
				return instIdList, false
			}
		} else {

			if index.State == common.INDEX_STATE_CREATED ||
				index.State == common.INDEX_STATE_READY ||
				index.State == common.INDEX_STATE_ERROR {
				newList[count] = instId
				count++
			} else {
				errStr := fmt.Sprintf("Invalid Index State %v for %v In Build Request", index.State, instId)
				idx.updateError(instId, errStr)
				errMap[instId] = &common.IndexerError{Reason: errStr, Code: common.IndexInvalidState}
			}
		}
	}

	newList = newList[0:count]
	return newList, len(newList) == len(instIdList)
}

func (idx *indexer) groupIndexListByBucket(instIdList []common.IndexInstId) map[string][]common.IndexInstId {

	bucketInstList := make(map[string][]common.IndexInstId)
	for _, instId := range instIdList {
		indexInst := idx.indexInstMap[instId]
		if instList, ok := bucketInstList[indexInst.Defn.Bucket]; ok {
			instList = append(instList, indexInst.InstId)
			bucketInstList[indexInst.Defn.Bucket] = instList
		} else {
			var newInstList []common.IndexInstId
			newInstList = append(newInstList, indexInst.InstId)
			bucketInstList[indexInst.Defn.Bucket] = newInstList
		}
	}
	return bucketInstList

}

func (idx *indexer) checkBucketExists(bucket string,
	instIdList []common.IndexInstId, clientCh MsgChannel, errMap map[common.IndexInstId]error) ([]common.IndexInstId, bool) {

	if len(instIdList) == 0 {
		return instIdList, false
	}

	newList := make([]common.IndexInstId, len(instIdList))
	count := 0

	currUUID := GetBucketUUID(idx.config["clusterAddr"].String(), bucket)
	for _, instId := range instIdList {
		indexInst := idx.indexInstMap[instId]
		if indexInst.Defn.Bucket != bucket || indexInst.Defn.BucketUUID != currUUID {

			if idx.enableManager {
				errStr := fmt.Sprintf("Unknown Bucket %v In Build Request", bucket)
				idx.updateError(instId, errStr)
				errMap[instId] = &common.IndexerError{Reason: errStr, Code: common.InvalidBucket}
			} else if clientCh != nil {
				clientCh <- &MsgError{
					err: Error{code: ERROR_INDEXER_UNKNOWN_BUCKET,
						severity: FATAL,
						cause:    ErrUnknownBucket,
						category: INDEXER}}
				return instIdList, false
			}
		} else {
			newList[count] = instId
			count++
		}
	}

	newList = newList[0:count]
	return newList, len(newList) == len(instIdList)
}

func (idx *indexer) handleStats(cmd Message) {
	req := cmd.(*MsgStatsRequest)
	replych := req.GetReplyChannel()
	total, idle, storage := idx.memoryUsed(false)
	used := total - idle
	idx.stats.memoryUsed.Set(int64(used))
	idx.stats.memoryTotalStorage.Set(int64(storage))
	idx.stats.memoryUsedStorage.Set(idx.memoryUsedStorage())
	replych <- true
}

func (idx *indexer) handleResetStats() {
	idx.stats.Reset()
	msgUpdateIndexInstMap := idx.newIndexInstMsg(idx.indexInstMap)
	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, nil); err != nil {
		common.CrashOnError(err)
	}
}

func (idx *indexer) memoryUsedStorage() int64 {
	mem_used := int64(forestdb.BufferCacheUsed()) + int64(memdb.MemoryInUse()) + int64(plasma.MemoryInUse()) + int64(nodetable.MemoryInUse())
	return mem_used
}

func NewSlice(id SliceId, indInst *common.IndexInst, partnInst *PartitionInst,
	conf common.Config, stats *IndexerStats) (slice Slice, err error) {
	// Default storage is forestdb
	storage_dir := conf["storage_dir"].String()
	os.Mkdir(storage_dir, 0755)
	if _, e := os.Stat(storage_dir); e != nil {
		common.CrashOnError(e)
	}
	path := filepath.Join(storage_dir, IndexPath(indInst, partnInst.Defn.GetPartitionId(), id))

	ephemeral, err := IsEphemeral(conf["clusterAddr"].String(), indInst.Defn.Bucket)
	if err != nil {
		logging.Errorf("Indexer::NewSlice Failed to check bucket type ephemeral: %v\n", err)
		return nil, err
	}
	switch indInst.Defn.Using {
	case common.MemDB, common.MemoryOptimized:
		slice, err = NewMemDBSlice(path, id, indInst.Defn, indInst.InstId, indInst.Defn.IsPrimary, !ephemeral, conf,
			stats.GetPartitionStats(indInst.InstId, partnInst.Defn.GetPartitionId()))
	case common.ForestDB:
		slice, err = NewForestDBSlice(path, id, indInst.Defn, indInst.InstId, indInst.Defn.IsPrimary, conf,
			stats.GetPartitionStats(indInst.InstId, partnInst.Defn.GetPartitionId()))
	case common.PlasmaDB:
		slice, err = NewPlasmaSlice(path, id, indInst.Defn, indInst.InstId, indInst.Defn.IsPrimary, conf,
			stats.GetPartitionStats(indInst.InstId, partnInst.Defn.GetPartitionId()))
	}

	return
}

func (idx *indexer) setProfilerOptions(config common.Config) {
	// CPU-profiling
	cpuProfile, ok := config["settings.cpuProfile"]
	if ok && cpuProfile.Bool() && idx.cpuProfFd == nil {
		cpuProfFname, ok := config["settings.cpuProfFname"]
		if ok {
			fname := cpuProfFname.String()
			logging.Infof("Indexer:: cpu profiling => %q\n", fname)
			idx.cpuProfFd = startCPUProfile(fname)

		} else {
			logging.Errorf("Indexer:: Missing cpu-profile o/p filename\n")
		}

	} else if ok && !cpuProfile.Bool() {
		if idx.cpuProfFd != nil {
			pprof.StopCPUProfile()
			logging.Infof("Indexer:: cpu profiling stopped\n")
		}
		idx.cpuProfFd = nil

	}

	// MEM-profiling
	memProfile, ok := config["settings.memProfile"]
	if ok && memProfile.Bool() {
		memProfFname, ok := config["settings.memProfFname"]
		if ok {
			fname := memProfFname.String()
			if dumpMemProfile(fname) {
				logging.Infof("Indexer:: mem profile => %q\n", fname)
			}
		} else {
			logging.Errorf("Indexer:: Missing mem-profile o/p filename\n")
		}
	}
}

func (idx *indexer) getIndexInstForBucket(bucket string) ([]common.IndexInstId, error) {

	idx.clustMgrAgentCmdCh <- &MsgClustMgrTopology{}
	resp := <-idx.clustMgrAgentCmdCh

	var result []common.IndexInstId = nil

	switch resp.GetMsgType() {
	case CLUST_MGR_GET_GLOBAL_TOPOLOGY:
		instMap := resp.(*MsgClustMgrTopology).GetInstMap()
		for id, inst := range instMap {
			if inst.Defn.Bucket == bucket {
				result = append(result, id)
			}
		}
	default:
		return nil, errors.New("Fail to read Metadata")
	}

	return result, nil
}

func (idx *indexer) deleteIndexInstOnDeletedBucket(bucket string, streamId common.StreamId) []common.IndexInstId {

	var instIdList []common.IndexInstId = nil

	if idx.enableManager {
		if err := idx.updateMetaInfoForDeleteBucket(bucket, streamId); err != nil {
			common.CrashOnError(err)
		}
	}

	// Only mark index inst as DELETED if it is actually got deleted in metadata.
	for _, index := range idx.indexInstMap {
		if index.Defn.Bucket == bucket &&
			(streamId == common.NIL_STREAM || (index.Stream == streamId ||
				index.Stream == common.NIL_STREAM)) {

			instIdList = append(instIdList, index.InstId)

			idx.stats.RemoveIndex(index.InstId)
		}
	}

	return instIdList
}

// start cpu profiling.
func startCPUProfile(filename string) *os.File {
	if filename == "" {
		fmsg := "Indexer:: empty cpu profile filename\n"
		logging.Errorf(fmsg, filename)
		return nil
	}
	fd, err := os.Create(filename)
	if err != nil {
		logging.Errorf("Indexer:: unable to create %q: %v\n", filename, err)
	}
	pprof.StartCPUProfile(fd)
	return fd
}

func dumpMemProfile(filename string) bool {
	if filename == "" {
		fmsg := "Indexer:: empty mem profile filename\n"
		logging.Errorf(fmsg, filename)
		return false
	}
	fd, err := os.Create(filename)
	if err != nil {
		logging.Errorf("Indexer:: unable to create %q: %v\n", filename, err)
		return false
	}
	pprof.WriteHeapProfile(fd)
	defer fd.Close()
	return true
}

//calculates buildTs for bucket. This is a blocking call
//which will keep trying till success as indexer cannot work
//without a buildts.
func computeBucketBuildTs(clustAddr string, bucket string, numVb int) (buildTs Timestamp, err error) {
kvtsloop:
	for {
		buildTs, err = GetCurrentKVTs(clustAddr, "default", bucket, numVb)
		if err != nil {
			logging.Errorf("Indexer::computeBucketBuildTs Error Fetching BuildTs %v", err)
			time.Sleep(KV_RETRY_INTERVAL * time.Millisecond)
		} else {
			break kvtsloop
		}
	}
	return
}

func (idx *indexer) updateSliceWithConfig(config common.Config) {

	//for every index managed by this indexer
	for _, partnMap := range idx.indexPartnMap {

		//for all partitions managed by this indexer
		for _, partnInst := range partnMap {

			sc := partnInst.Sc

			//update config for all the slices
			for _, slice := range sc.GetAllSlices() {
				slice.UpdateConfig(config)
			}
		}
	}

}

func (idx *indexer) getStreamBucketState(streamId common.StreamId, bucket string) StreamStatus {

	idx.stateLock.RLock()
	defer idx.stateLock.RUnlock()
	return idx.streamBucketStatus[streamId][bucket]

}

func (idx *indexer) setStreamBucketState(streamId common.StreamId, bucket string, status StreamStatus) {

	idx.stateLock.Lock()
	defer idx.stateLock.Unlock()
	idx.streamBucketStatus[streamId][bucket] = status

}

func (idx *indexer) getIndexerState() common.IndexerState {
	idx.stateLock.RLock()
	defer idx.stateLock.RUnlock()
	return idx.state
}

func (idx *indexer) setIndexerState(s common.IndexerState) {
	idx.stateLock.Lock()
	defer idx.stateLock.Unlock()
	idx.state = s
}

//monitor memory usage, if more than specified quota
//generate message to pause Indexer
func (idx *indexer) monitorMemUsage() {

	logging.Infof("Indexer::monitorMemUsage started...")

	var canResume bool
	if idx.getIndexerState() == common.INDEXER_PAUSED {
		canResume = true
	}

	monitorInterval := idx.config["mem_usage_check_interval"].Int()

	for {

		pause_if_oom := idx.config["pause_if_memory_full"].Bool()

		if common.GetStorageMode() == common.MOI && pause_if_oom {

			memory_quota := idx.config["settings.memory_quota"].Uint64()
			high_mem_mark := idx.config["high_mem_mark"].Float64()
			low_mem_mark := idx.config["low_mem_mark"].Float64()
			min_oom_mem := idx.config["min_oom_memory"].Uint64()

			gcDone := false
			if idx.needsGCMoi() {
				start := time.Now()
				debug.FreeOSMemory()
				elapsed := time.Since(start)
				logging.Infof("Indexer::monitorMemUsage ManualGC Time Taken %v", elapsed)
				mm.FreeOSMemory()
				gcDone = true
			}

			var mem_used uint64
			var idle uint64
			if idx.getIndexerState() == common.INDEXER_PAUSED || gcDone {
				mem_used, idle, _ = idx.memoryUsed(true)
			} else {
				mem_used, idle, _ = idx.memoryUsed(false)
			}

			logging.Infof("Indexer::monitorMemUsage MemoryUsed Total %v Idle %v", mem_used, idle)

			switch idx.getIndexerState() {

			case common.INDEXER_ACTIVE:
				if float64(mem_used) > (high_mem_mark*float64(memory_quota)) &&
					!canResume && mem_used > min_oom_mem {
					idx.internalRecvCh <- &MsgIndexerState{mType: INDEXER_PAUSE}
					canResume = true
				}

			case common.INDEXER_PAUSED:
				if float64(mem_used) < (low_mem_mark*float64(memory_quota)) && canResume {
					idx.internalRecvCh <- &MsgIndexerState{mType: INDEXER_RESUME}
					canResume = false
				}
			}
		} else if common.GetStorageMode() == common.FORESTDB {

			if idx.needsGCFdb() {
				start := time.Now()
				debug.FreeOSMemory()
				elapsed := time.Since(start)
				logging.Infof("Indexer::monitorMemUsage ManualGC Time Taken %v", elapsed)
				mm.FreeOSMemory()
			}

		}

		time.Sleep(time.Second * time.Duration(monitorInterval))
	}

}

func (idx *indexer) handleIndexerPause(msg Message) {

	logging.Infof("Indexer::handleIndexerPause")

	if idx.getIndexerState() != common.INDEXER_ACTIVE {
		logging.Infof("Indexer::handleIndexerPause Ignoring request to "+
			"pause indexer in %v state", idx.getIndexerState())
		return
	}

	//Send message to index manager to update the internal state
	idx.clustMgrAgentCmdCh <- &MsgClustMgrLocal{
		mType: CLUST_MGR_SET_LOCAL,
		key:   INDEXER_STATE_KEY,
		value: fmt.Sprintf("%s", common.INDEXER_PAUSED),
	}

	respMsg := <-idx.clustMgrAgentCmdCh
	resp := respMsg.(*MsgClustMgrLocal)

	errMsg := resp.GetError()
	if errMsg != nil {
		logging.Fatalf("Indexer::handleIndexerPause Unable to set IndexerState In Local"+
			"Meta Storage. Err %v", errMsg)
		common.CrashOnError(errMsg)
	}

	idx.setIndexerState(common.INDEXER_PAUSED)
	idx.stats.indexerState.Set(int64(common.INDEXER_PAUSED))
	logging.Infof("Indexer::handleIndexerPause Indexer State Changed to "+
		"%v", idx.getIndexerState())

	//Notify Scan Coordinator
	idx.scanCoordCmdCh <- msg
	<-idx.scanCoordCmdCh

	//Notify Timekeeper
	idx.tkCmdCh <- msg
	<-idx.tkCmdCh

	//Notify Mutation Manager
	idx.mutMgrCmdCh <- msg
	<-idx.mutMgrCmdCh

}

func (idx *indexer) handleIndexerResume(msg Message) {

	logging.Infof("Indexer::handleIndexerResume")

	idx.setIndexerState(common.INDEXER_PREPARE_UNPAUSE)
	go idx.doPrepareUnpause()

}

func (idx *indexer) doPrepareUnpause() {

	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()

	for _ = range ticker.C {

		//check if indexer can be resumed i.e.
		//no recovery, no pending stream request
		if idx.checkAnyStreamRequestPending() ||
			idx.checkRecoveryInProgress() {
			logging.Infof("Indexer::doPrepareUnpause Dropping Request to Unpause Indexer. " +
				"Next Try In 1 Second... ")
			continue
		}
		idx.internalRecvCh <- &MsgIndexerState{mType: INDEXER_PREPARE_UNPAUSE}
		return
	}
}

func (idx *indexer) doUnpause() {

	idx.setIndexerState(common.INDEXER_ACTIVE)
	idx.stats.indexerState.Set(int64(common.INDEXER_ACTIVE))

	msg := &MsgIndexerState{mType: INDEXER_RESUME}

	//Notify Scan Coordinator
	idx.scanCoordCmdCh <- msg
	<-idx.scanCoordCmdCh

	//Notify Mutation Manager
	idx.mutMgrCmdCh <- msg
	<-idx.mutMgrCmdCh

	//Notify Timekeeper
	idx.tkCmdCh <- msg
	<-idx.tkCmdCh

	//Notify Index Manager
	//TODO Need to make sure the DDLs don't start getting
	//processed before stream requests
	idx.clustMgrAgentCmdCh <- &MsgClustMgrLocal{
		mType: CLUST_MGR_SET_LOCAL,
		key:   INDEXER_STATE_KEY,
		value: fmt.Sprintf("%s", common.INDEXER_ACTIVE),
	}

	respMsg := <-idx.clustMgrAgentCmdCh
	resp := respMsg.(*MsgClustMgrLocal)

	errMsg := resp.GetError()
	if errMsg != nil {
		logging.Fatalf("Indexer::handleIndexerResume Unable to set IndexerState In Local"+
			"Meta Storage. Err %v", errMsg)
		common.CrashOnError(errMsg)
	}

}

func (idx *indexer) checkAnyStreamRequestPending() bool {

	idx.stateLock.RLock()
	defer idx.stateLock.RUnlock()

	for s, bs := range idx.streamBucketStatus {

		for b, _ := range bs {
			if idx.checkStreamRequestPending(s, b) {
				logging.Debugf("Indexer::checkStreamRequestPending %v %v", s, b)
				return true
			}
		}
	}

	return false

}

func (idx *indexer) checkRecoveryInProgress() bool {

	idx.stateLock.RLock()
	defer idx.stateLock.RUnlock()

	for s, bs := range idx.streamBucketStatus {

		for b, status := range bs {
			if status == STREAM_PREPARE_RECOVERY ||
				status == STREAM_RECOVERY {
				logging.Debugf("Indexer::checkRecoveryInProgress %v %v", s, b)
				return true
			}
		}
	}

	return false

}

//memoryUsed returns the memory usage reported by
//golang runtime + memory allocated by cgo
//components(e.g. fdb buffercache)
func (idx *indexer) memoryUsed(forceRefresh bool) (uint64, uint64, uint64) {

	var ms runtime.MemStats

	if forceRefresh {
		idx.updateMemstats()
		gMemstatCacheLastUpdated = time.Now()
	}

	gMemstatLock.RLock()
	ms = gMemstatCache
	gMemstatLock.RUnlock()

	timeout := time.Millisecond * time.Duration(idx.config["memstats_cache_timeout"].Uint64())
	if time.Since(gMemstatCacheLastUpdated) > timeout {
		go idx.updateMemstats()
		gMemstatCacheLastUpdated = time.Now()
	}

	mem_used := ms.HeapInuse + ms.HeapIdle - ms.HeapReleased + ms.GCSys + forestdb.BufferCacheUsed()
	mem_storage := uint64(0)
	mode := common.GetStorageMode()
	if mode == common.MOI || mode == common.PLASMA {
		mem_storage += mm.Size()
	}
	mem_used += mem_storage

	idle := ms.HeapIdle - ms.HeapReleased

	return mem_used, idle, mem_storage
}

func (idx *indexer) updateMemstats() {

	var ms runtime.MemStats

	start := time.Now()
	runtime.ReadMemStats(&ms)
	elapsed := time.Since(start)

	gMemstatLock.Lock()
	gMemstatCache = ms
	gMemstatLock.Unlock()

	logging.Infof("Indexer::ReadMemstats Time Taken %v", elapsed)

}

func (idx *indexer) needsGCMoi() bool {

	var memUsed uint64
	memQuota := idx.config["settings.memory_quota"].Uint64()

	if idx.getIndexerState() == common.INDEXER_PAUSED {
		memUsed, _, _ = idx.memoryUsed(true)
	} else {
		memUsed, _, _ = idx.memoryUsed(false)
	}

	if memUsed >= memQuota {
		return true
	}

	forceGcFrac := idx.config["force_gc_mem_frac"].Float64()
	memQuotaFree := memQuota - memUsed

	if float64(memQuotaFree) < forceGcFrac*float64(memQuota) {
		return true
	}

	return false

}

func (idx *indexer) needsGCFdb() bool {

	var memUsed uint64
	memQuota := idx.config["settings.memory_quota"].Uint64()

	//ignore till 1GB
	ignoreThreshold := idx.config["min_oom_memory"].Uint64() * 4

	memUsed, _, _ = idx.memoryUsed(true)

	if memUsed < ignoreThreshold {
		return false
	}

	if memUsed >= memQuota {
		return true
	}

	forceGcFrac := idx.config["force_gc_mem_frac"].Float64()
	memQuotaFree := memQuota - memUsed

	if float64(memQuotaFree) < forceGcFrac*float64(memQuota) {
		return true
	}

	return false

}
func (idx *indexer) logMemstats() {

	var ms runtime.MemStats
	var oldNumGC uint32
	var PauseNs [256]uint64

	for {

		oldNumGC = ms.NumGC

		gMemstatLock.RLock()
		ms = gMemstatCache
		gMemstatLock.RUnlock()

		common.PrintMemstats(&ms, PauseNs[:], oldNumGC)

		time.Sleep(time.Second * time.Duration(idx.config["memstatTick"].Int()))
	}

}

func (idx *indexer) checkAnyValidIndex() bool {

	for _, inst := range idx.indexInstMap {

		if inst.State == common.INDEX_STATE_ACTIVE ||
			inst.State == common.INDEX_STATE_INITIAL ||
			inst.State == common.INDEX_STATE_CATCHUP ||
			inst.State == common.INDEX_STATE_CREATED {
			return true
		}
	}

	return false
}

func (idx *indexer) canSetStorageMode(sm string) bool {

	for _, inst := range idx.indexInstMap {

		if common.IndexTypeToStorageMode(inst.Defn.Using).String() != sm &&
			(inst.State != common.INDEX_STATE_DELETED || inst.State != common.INDEX_STATE_ERROR) {
			logging.Warnf("Indexer::canSetStorageMode Cannot Set Storage Mode %v. Found Index %v", sm, inst)
			return false
		}
	}

	return true
}

//
// This function returns the storage mode of the local node.
// 1) If the node has indexes, return storage mode of indexes
// 2) If node does not have indexes, return global storage mode (from ns-server / settings)
// 3) If indexes have mixed storage modes, then return NOT_SET
// 4) Storage mode is promoted to plasma if it is forestdb
//
func (idx *indexer) getLocalStorageMode(config common.Config) common.StorageMode {

	// Find out the storage mode from indexes
	storageMode := idx.getIndexStorageMode()

	// If there is no index, then use the global storage mode
	if storageMode == common.NOT_SET {
		storageMode = idx.promoteStorageModeIfNecessary(common.GetClusterStorageMode(), config)
	}

	// If there is mixed storage mode
	if storageMode == common.MIXED {
		storageMode = common.NOT_SET
	}

	if storageMode != common.GetClusterStorageMode() {
		logging.Warnf("Indexer::getLocalStorageMode(): local storage mode %v is different from cluster storage mode %v",
			storageMode, common.GetClusterStorageMode())
	}

	return storageMode
}

//
// This function returns the storage mode based on indexes on local node.
//
func (idx *indexer) getIndexStorageMode() common.StorageMode {

	storageMode := common.StorageMode(common.NOT_SET)
	for _, inst := range idx.indexInstMap {

		if inst.State != common.INDEX_STATE_DELETED || inst.State != common.INDEX_STATE_ERROR {

			indexStorageMode := common.IndexTypeToStorageMode(inst.Defn.Using)

			// If index has no storage mode, then this index will be skipped.   If index has no storage
			// mode, this index will have no storage (nil slice in partnMap).
			if indexStorageMode == common.NOT_SET {
				logging.Warnf("Indexer::getIndexStorageMode(): Index '%v' storage mode is set to invalid value %v",
					inst.Defn.Name, inst.Defn.Using)
				continue
			}

			// If it is the first index, the initialize storage mode
			if storageMode == common.NOT_SET {
				storageMode = indexStorageMode
				continue
			}

			// If index has different storage mode, promote storage mode if necessary.
			if storageMode != indexStorageMode {
				return common.MIXED
			}
		}
	}

	return storageMode
}

func (idx *indexer) promoteStorageModeIfNecessary(mode common.StorageMode, config common.Config) common.StorageMode {

	//
	// Check for upgrade
	//
	disable := config["settings.storage_mode.disable_upgrade"].Bool()
	if disable {
		logging.Warnf("Indexer::promoteStorageModeIfNecessary(): storage mode upgrade is disabled.")
	}

	//
	// Check for storage mode override
	//
	override := idx.getStorageModeOverride(config)

	return idx.promoteStorageModeIfNecessaryInternal(mode, disable, override)
}

func (idx *indexer) promoteStorageModeIfNecessaryInternal(mode common.StorageMode, disable bool, override common.StorageMode) common.StorageMode {

	if common.GetBuildMode() != common.ENTERPRISE {
		return mode
	}

	if !disable && mode == common.FORESTDB {
		mode = common.PLASMA
	}

	if override != common.NOT_SET {
		logging.Warnf("Indexer::promoteStorageModeIfNecessary(): override storage mode %v.", override)
		mode = override
	}

	return mode
}

func (idx *indexer) getStorageModeOverride(config common.Config) common.StorageMode {

	nodeUUID := config["nodeuuid"].String()
	override, err := mc.GetIndexerStorageModeOverride(nodeUUID)

	if err == nil && common.IsValidIndexType(override) {
		logging.Infof("Indexer::getStorageModeOverride(): override storage mode %v.", override)
		return common.IndexTypeToStorageMode(common.IndexType(override))
	}

	if err != nil {
		logging.Errorf("Error when fetching storage mode override.  Error=%s", err)
	}

	return common.NOT_SET
}

func (idx *indexer) getBootstrapStorageMode(config common.Config) common.StorageMode {

	nodeUUID := config["nodeuuid"].String()
	s, err := mc.GetIndexerLocalStorageMode(nodeUUID)
	if s == common.NOT_SET || err != nil {
		logging.Infof("Unable to fetch storage mode from metakv during bootrap.  Use storage mode setting for bootstrap")

		confStorageMode := strings.ToLower(config["settings.storage_mode"].String())
		s = common.IndexTypeToStorageMode(common.IndexType(confStorageMode))
	}

	return idx.promoteStorageModeIfNecessary(s, config)
}

func (idx *indexer) postIndexStorageModeForBootstrap(config common.Config, storageMode common.StorageMode) error {

	nodeUUID := config["nodeuuid"].String()
	err := mc.PostIndexerLocalStorageMode(nodeUUID, storageMode)
	if err != nil {
		logging.Errorf("Error when post storage mode to metakv during bootrap.  Error=%s", err)
		return err
	}

	return nil
}

func (idx *indexer) getInstIdFromDefnId(defnId common.IndexDefnId) common.IndexInstId {

	for instId, inst := range idx.indexInstMap {
		if inst.Defn.DefnId == defnId {
			return instId
		}
	}
	return 0
}
