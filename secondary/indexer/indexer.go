// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
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

	"github.com/couchbase/cbauth/service"

	"github.com/couchbase/indexing/secondary/audit"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/common/collections"
	forestdb "github.com/couchbase/indexing/secondary/fdb"
	"github.com/couchbase/indexing/secondary/iowrap"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/logging/systemevent"
	mc "github.com/couchbase/indexing/secondary/manager/common"
	"github.com/couchbase/indexing/secondary/memdb"
	"github.com/couchbase/indexing/secondary/memdb/nodetable"
	projClient "github.com/couchbase/indexing/secondary/projector/client"
	"github.com/couchbase/indexing/secondary/security"
	"github.com/couchbase/indexing/secondary/stubs/nitro/mm"
	"github.com/couchbase/indexing/secondary/stubs/nitro/plasma"
)

type Indexer interface {
	Shutdown() Message
}

var StreamAddrMap StreamAddressMap
var StreamTopicName map[common.StreamId]string
var ServiceAddrMap map[string]string

// httpMux holds the HTTP(S) REST server used by Indexer and its children. It can be retrieved
// via function GetHTTPMux.
var httpMux *http.ServeMux

type KeyspaceIdIndexCountMap map[string]int
type KeyspaceIdFlushInProgressMap map[string]bool
type KeyspaceIdObserveFlushDoneMap map[string]MsgChannel
type KeyspaceIdCurrRequest map[string]*currRequest
type KeyspaceIdRollbackTs map[string]*common.TsVbuuid
type KeyspaceIdRetryTs map[string]*common.TsVbuuid
type KeyspaceIdMinMergeTs map[string]*common.TsVbuuid

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
	ErrBucketEphemeral          = errors.New("Ephemeral Buckets Must Use MOI or PLASMA Storage")
	ErrBucketEphemeralStd       = errors.New("Standard GSI Index on ephemeral bucket requires fully upgraded cluster")
)

// Backup corrupt index data files
const (
	CORRUPT_DATA_SUBDIR = ".corruptData"
)

// indexer is the central GSI class that runs the main message loop.
// It also implements the Indexer interface.
type indexer struct {
	id       string
	state    common.IndexerState
	nodeInfo *service.NodeInfo // never changes; info about the local node

	indexInstMap  common.IndexInstMap //map of indexInstId to IndexInst
	indexPartnMap IndexPartnMap       //map of indexInstId to PartitionInst

	streamKeyspaceIdStatus map[common.StreamId]KeyspaceIdStatus

	streamKeyspaceIdFlushInProgress  map[common.StreamId]KeyspaceIdFlushInProgressMap
	streamKeyspaceIdObserveFlushDone map[common.StreamId]KeyspaceIdObserveFlushDoneMap

	streamKeyspaceIdCurrRequest  map[common.StreamId]KeyspaceIdCurrRequest
	streamKeyspaceIdRollbackTs   map[common.StreamId]KeyspaceIdRollbackTs
	streamKeyspaceIdRetryTs      map[common.StreamId]KeyspaceIdRetryTs
	streamKeyspaceIdRequestQueue map[common.StreamId]map[string]chan *kvRequest
	streamKeyspaceIdRequestLock  map[common.StreamId]map[string]chan *sync.Mutex
	streamKeyspaceIdSessionId    map[common.StreamId]map[string]uint64
	streamKeyspaceIdCollectionId map[common.StreamId]map[string]string
	streamKeyspaceIdOSOException map[common.StreamId]map[string]bool
	streamKeyspaceIdMinMergeTs   map[common.StreamId]KeyspaceIdMinMergeTs

	streamKeyspaceIdPendBuildDone      map[common.StreamId]map[string]*buildDoneSpec
	streamKeyspaceIdPendStart          map[common.StreamId]map[string]bool
	streamKeyspaceIdPendCollectionDrop map[common.StreamId]map[string][]common.IndexInstId

	keyspaceIdRollbackTimes map[string]int64

	keyspaceIdBuildTs map[string]Timestamp
	buildTsLock       map[common.StreamId]map[string]*sync.Mutex

	activeKVNodes map[string]bool // Key -> KV node UUID

	//TODO Remove this once cbq bridge support goes away
	keyspaceIdCreateClientChMap map[string]MsgChannel

	wrkrRecvCh MsgChannel //channel to receive messages from workers
	//   (SettingsManager, MutationManager, KVSender, Timekeeper)
	wrkrPrioRecvCh MsgChannel // buffered channel for high-priority worker requests
	internalRecvCh MsgChannel //buffered channel to queue worker requests (most MsgXyz messages)

	adminRecvCh         MsgChannel //channel to receive admin messages (ClustMgrAgent)
	internalAdminRecvCh MsgChannel //internal channel to receive admin messages (DDL; unbuffered channel)
	internalAdminRespCh MsgChannel //internal channel to respond admin messages (DDL; unbuffered channel)

	shutdownInitCh     MsgChannel //internal shutdown channel for indexer
	shutdownCompleteCh MsgChannel //indicate shutdown completion

	mutMgrCmdCh          MsgChannel //channel to send commands to mutation manager
	storageMgrCmdCh      MsgChannel //channel to send commands to storage manager
	tkCmdCh              MsgChannel //channel to send commands to timekeeper
	rebalMgrCmdCh        MsgChannel //channel to send commands to rebalance manager
	ddlSrvMgrCmdCh       MsgChannel //channel to send commands to ddl service manager
	schedIdxCreatorCmdCh MsgChannel // channel to send commands to sheduled index creator
	compactMgrCmdCh      MsgChannel //channel to send commands to compaction manager
	clustMgrAgentCmdCh   MsgChannel //channel to send messages to index coordinator
	kvSenderCmdCh        MsgChannel //channel to send messages to kv sender
	settingsMgrCmdCh     MsgChannel
	statsMgrCmdCh        MsgChannel
	scanCoordCmdCh       MsgChannel //chhannel to send messages to scan coordinator
	meteringMgrCmdCh     MsgChannel // channel to send messages to metering manager

	mutMgrExitCh MsgChannel //channel to indicate mutation manager exited

	tk              Timekeeper             //handle to timekeeper
	storageMgr      StorageManager         //handle to storage manager
	compactMgr      CompactionManager      //handle to compaction manager
	mutMgr          MutationManager        //handle to mutation manager
	ddlSrvMgr       *DDLServiceMgr         //handle to ddl service manager
	schedIdxCreator *schedIndexCreator     //handle to scheduled index creator
	clustMgrAgent   ClustMgrAgent          //handle to ClustMgrAgent
	kvSender        KVSender               //handle to KVSender
	settingsMgr     *settingsManager       //handle to settings manager
	statsMgr        *statsManager          //handle to statistics manager
	scanCoord       ScanCoordinator        //handle to ScanCoordinator
	cpuThrottle     *CpuThrottle           //handle to CPU throttler (for Autofailover)
	meteringMgr     *MeteringThrottlingMgr //handle to metering throttling service

	// masterMgr holds AutofailoverServiceManager and RebalanceServiceManager singletons as
	// ns_server only supports registering a single object for RPC calls.
	masterMgr *MasterServiceManager

	config common.Config // map of current indexer config settings with "indexer." prefix stripped

	kvlock       sync.Mutex   //fine-grain lock for KVSender
	clustMgrLock sync.Mutex   // lock to protect concurrent reads and writes from clustMgrAgentCmdCh
	stateLock    sync.RWMutex //lock to protect the keyspaceIdStatus map

	stats *IndexerStats

	enableManager bool // forced to true in cmd/indexer/main.go, overriding default false in config.go
	cpuProfFd     *os.File

	rebalanceRunning bool
	rebalanceToken   *RebalanceToken

	mergePartitionList []mergeSpec
	prunePartitionList []pruneSpec
	merged             map[common.IndexInstId]common.IndexInst
	pruned             map[common.IndexInstId]common.IndexInst
	lastStreamUpdate   int64

	bootstrapStorageMode common.StorageMode

	httpsSrvLock sync.Mutex
	httpsSrv     *http.Server
	tlsListener  net.Listener

	httpSrvLock sync.Mutex
	httpSrv     *http.Server
	tcpListener net.Listener

	enableSecurityChange chan bool

	cinfoProvider     common.ClusterInfoProvider
	cinfoProviderLock sync.RWMutex

	testServRunning bool

	keyspaceIdResetList                map[string]resetList
	keyspaceIdObserveFlushDoneForReset map[string]MsgChannel

	pendingReset map[common.IndexInstId]bool

	bsRunParams *runParams // bootstrap values of DDL running and inProgressIndexNames
}

type kvRequest struct {
	lock       *sync.Mutex
	keyspaceId string
	streamId   common.StreamId
	grantCh    chan bool
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

type currRequest struct {
	request   Message
	reqCh     StopChannel
	sessionId uint64
}

type buildDoneSpec struct {
	sessionId uint64
	flushTs   *common.TsVbuuid
}

type resetList []common.IndexInstId

// NewIndexer is the constructor for the Indexer interface implemented by the indexer class.
// config is a few hard-coded defaults but does NOT contain most indexer config values as
// passed in. Instead NewSettingsManager below merges common/config.go values into this and
// saves the result in idx.config, so only try to access config values after that point.
func NewIndexer(config common.Config) (Indexer, Message) {

	idx := &indexer{
		wrkrRecvCh:     make(MsgChannel, WORKER_RECV_QUEUE_LEN),
		wrkrPrioRecvCh: make(MsgChannel, WORKER_RECV_QUEUE_LEN),
		internalRecvCh: make(MsgChannel, WORKER_MSG_QUEUE_LEN),

		adminRecvCh:         make(MsgChannel, WORKER_MSG_QUEUE_LEN),
		internalAdminRecvCh: make(MsgChannel),
		internalAdminRespCh: make(MsgChannel),

		shutdownInitCh:     make(MsgChannel),
		shutdownCompleteCh: make(MsgChannel),

		mutMgrCmdCh:          make(MsgChannel),
		storageMgrCmdCh:      make(MsgChannel),
		tkCmdCh:              make(MsgChannel),
		rebalMgrCmdCh:        make(MsgChannel),
		ddlSrvMgrCmdCh:       make(MsgChannel),
		schedIdxCreatorCmdCh: make(MsgChannel),
		compactMgrCmdCh:      make(MsgChannel),
		clustMgrAgentCmdCh:   make(MsgChannel),
		kvSenderCmdCh:        make(MsgChannel),
		settingsMgrCmdCh:     make(MsgChannel),
		statsMgrCmdCh:        make(MsgChannel),
		scanCoordCmdCh:       make(MsgChannel),
		meteringMgrCmdCh:     make(MsgChannel),

		mutMgrExitCh: make(MsgChannel),

		indexInstMap:  make(common.IndexInstMap),
		indexPartnMap: make(IndexPartnMap),

		merged: make(map[common.IndexInstId]common.IndexInst),
		pruned: make(map[common.IndexInstId]common.IndexInst),

		streamKeyspaceIdStatus:             make(map[common.StreamId]KeyspaceIdStatus),
		streamKeyspaceIdFlushInProgress:    make(map[common.StreamId]KeyspaceIdFlushInProgressMap),
		streamKeyspaceIdObserveFlushDone:   make(map[common.StreamId]KeyspaceIdObserveFlushDoneMap),
		streamKeyspaceIdCurrRequest:        make(map[common.StreamId]KeyspaceIdCurrRequest),
		streamKeyspaceIdRollbackTs:         make(map[common.StreamId]KeyspaceIdRollbackTs),
		streamKeyspaceIdRetryTs:            make(map[common.StreamId]KeyspaceIdRetryTs),
		streamKeyspaceIdMinMergeTs:         make(map[common.StreamId]KeyspaceIdMinMergeTs),
		streamKeyspaceIdRequestQueue:       make(map[common.StreamId]map[string]chan *kvRequest),
		streamKeyspaceIdRequestLock:        make(map[common.StreamId]map[string]chan *sync.Mutex),
		streamKeyspaceIdSessionId:          make(map[common.StreamId]map[string]uint64),
		streamKeyspaceIdCollectionId:       make(map[common.StreamId]map[string]string),
		streamKeyspaceIdOSOException:       make(map[common.StreamId]map[string]bool),
		streamKeyspaceIdPendBuildDone:      make(map[common.StreamId]map[string]*buildDoneSpec),
		streamKeyspaceIdPendStart:          make(map[common.StreamId]map[string]bool),
		streamKeyspaceIdPendCollectionDrop: make(map[common.StreamId]map[string][]common.IndexInstId),
		keyspaceIdBuildTs:                  make(map[string]Timestamp),
		buildTsLock:                        make(map[common.StreamId]map[string]*sync.Mutex),
		keyspaceIdRollbackTimes:            make(map[string]int64),
		keyspaceIdCreateClientChMap:        make(map[string]MsgChannel),

		activeKVNodes: make(map[string]bool),

		enableSecurityChange: make(chan bool),

		keyspaceIdResetList:                make(map[string]resetList),
		keyspaceIdObserveFlushDoneForReset: make(map[string]MsgChannel),

		pendingReset: make(map[common.IndexInstId]bool),
		bsRunParams:  &runParams{},
	}

	logging.Infof("Indexer::NewIndexer Status Warmup")

	var res Message

	// Setting manager must be the first component to initialized.  In particular, setting manager will
	// read the indexer settings from metakv, including cluster-level storage mode.  Since metakv is
	// eventually consistent, if setting cannot read the latest settings from metakv during this step,
	// those new settings will be delievered to the indexer through a callback.
	idx.settingsMgr, idx.config, res = NewSettingsManager(idx.settingsMgrCmdCh, idx.wrkrRecvCh, config)
	if res.GetMsgType() != MSG_SUCCESS {
		logging.Fatalf("Indexer::NewIndexer settingsMgr Init Error %+v", res)
		return nil, res
	}
	clusterAddr := idx.config["clusterAddr"].String() // "127.0.0.1:<cluster_admin_port>" (eg 8091)
	host, _, _ := net.SplitHostPort(clusterAddr)
	port := idx.config["httpPort"].String()
	httpAddr := net.JoinHostPort(host, port) // "127.0.0.1:<indexer_http_port"> (eg 9102, 9108, ...)

	// CPU throttling is disabled until CpuThrottle.SetCpuThrottling(true) is called
	idx.cpuThrottle = NewCpuThrottle(idx.config["cpu.throttle.target"].Float64())
	autofailoverMgr := NewAutofailoverServiceManager(httpAddr, idx.cpuThrottle)

	// Initialize auditing
	err := audit.InitAuditService(clusterAddr)
	if err != nil {
		common.CrashOnError(err)
	}

	// Initialize SystemEventLogger
	err = systemevent.InitSystemEventLogger(clusterAddr)
	if err != nil {
		common.CrashOnError(err)
	}

	//Initialize security context
	encryptLocalHost := config["security.encryption.encryptLocalhost"].Bool()
	err = idx.initSecurityContext(encryptLocalHost)
	if err != nil {
		idxErr := Error{
			code:     ERROR_INDEXER_INTERNAL_ERROR,
			severity: FATAL,
			cause:    err,
			category: INDEXER,
		}
		return nil, &MsgError{err: idxErr}
	}

	idx.stats = NewIndexerStats()
	idx.initFromConfig()

	logging.Infof("Indexer::NewIndexer Starting with Vbuckets %v", idx.config["numVbuckets"].Int())

	useCInfoLite := idx.config["use_cinfo_lite"].Bool()
	idx.cinfoProvider, err = common.NewClusterInfoProvider(useCInfoLite, clusterAddr,
		DEFAULT_POOL, "indexer", idx.config)
	if err != nil {
		logging.Errorf("Indexer::NewIndexer Unable to get new ClusterInfoProvider err: %v use_cinfo_lite: %v", err, useCInfoLite)
		common.CrashOnError(err)
	}

	go common.PollForDeletedBucketsV2(clusterAddr, DEFAULT_POOL, idx.config)

	// WatchClusterVersionChanges is used for queryport server auth enforcement
	// as well as for choice between bucket/collection seqnos for session
	// consistent scans.
	go common.WatchClusterVersionChanges(idx.config["clusterAddr"].String(), int64(common.INDEXER_71_VERSION))

	//Start Mutation Manager
	idx.mutMgr, res = NewMutationManager(idx.mutMgrCmdCh, idx.wrkrRecvCh, idx.config,
		idx.cpuThrottle)
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
	idx.tk, res = NewTimekeeper(idx.tkCmdCh, idx.wrkrRecvCh, idx.config, idx.cinfoProvider, &idx.cinfoProviderLock)
	if res.GetMsgType() != MSG_SUCCESS {
		logging.Fatalf("Indexer::NewIndexer Timekeeper Init Error %+v", res)
		return nil, res
	}

	snapListners := idx.getSnapshotListeners()
	snapshotNotifych := make([]chan IndexSnapshot, snapListners)
	for i := 0; i < snapListners; i++ {
		snapshotNotifych[i] = make(chan IndexSnapshot, 5000)
	}

	snapReqWorkers := idx.getSnapshotReqWorkers()
	snapshotReqCh := make([]MsgChannel, snapReqWorkers)
	for i := 0; i < snapReqWorkers; i++ {
		snapshotReqCh[i] = make(MsgChannel, 5000)
	}

	//Start Scan Coordinator
	idx.scanCoord, res = NewScanCoordinator(idx.scanCoordCmdCh, idx.wrkrRecvCh,
		idx.config, snapshotNotifych, snapshotReqCh, idx.stats.Clone(), idx.cpuThrottle)
	if res.GetMsgType() != MSG_SUCCESS {
		logging.Fatalf("Indexer::NewIndexer Scan Coordinator Init Error %+v", res)
		return nil, res
	}

	// Start compaction manager
	idx.compactMgr, res = NewCompactionManager(idx.compactMgrCmdCh, idx.wrkrRecvCh, idx.config)
	if res.GetMsgType() != MSG_SUCCESS {
		logging.Fatalf("Indexer::NewIndexer NewCompactionManager Init Error %+v", res)
		return nil, res
	}

	// Find out if there is a bootstrapStorageMode for this node.   Bootstrap storage mode is
	// set during storage upgrade to instruct the indexer to use this storage for bootstraping
	// indexer components.   During storage upgrade, indexer may need to restart so it
	// can boostrap with the bootstrap storage mode.
	idx.bootstrapStorageMode = idx.getBootstrapStorageMode(idx.config)
	logging.Infof("Indexer::NewIndexer bootstrap storage mode %v", idx.bootstrapStorageMode)
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

	if err := idx.initHTTP(); err != nil {
		common.CrashOnError(err)
	}

	// Start internal version monitor only after starting http server.
	go common.MonitorInternalVersion(int64(common.INDEXER_71_VERSION), common.MIN_VER_SRV_AUTH,
		idx.config["clusterAddr"].String())

	// indexer is now ready to take security change
	close(idx.enableSecurityChange)

	//bootstrap phase 1
	idx.bootstrap1(snapshotNotifych, snapshotReqCh)

	// we need to initialize metering manager after the bootstrap1 as we need to get indexerId.
	if common.GetBuildMode() == common.ENTERPRISE && common.GetDeploymentModel() == common.SERVERLESS_DEPLOYMENT {
		idx.meteringMgr, res = NewMeteringManager(idx.id, idx.config, idx.meteringMgrCmdCh)
		if res.GetMsgType() != MSG_SUCCESS {
			logging.Fatalf("Indexer::NewIndexer NewMeteringManager Init Error %+v", res)
			return nil, res
		}
		idx.tk.SetMeteringMgr(idx.meteringMgr)
		idx.scanCoord.SetMeteringMgr(idx.meteringMgr)
		idx.meteringMgr.RegisterRestEndpoints()
	}

	//Start DDL Service Manager
	//Initialize DDL Service Manager before rebalance manager so DDL service manager is ready
	//when Rebalancing manager receives ns_server rebalancing callback.
	idx.ddlSrvMgr, res = NewDDLServiceMgr(common.IndexerId(idx.id), idx.ddlSrvMgrCmdCh, idx.wrkrRecvCh, idx.config)
	if res.GetMsgType() != MSG_SUCCESS {
		logging.Fatalf("Indexer::NewIndexer DDL Service Manager Init Error %+v", res)
		return nil, res
	}

	idx.schedIdxCreator, res = NewSchedIndexCreator(common.IndexerId(idx.id), idx.schedIdxCreatorCmdCh, idx.wrkrRecvCh, idx.config)
	if res.GetMsgType() != MSG_SUCCESS {
		logging.Fatalf("Indexer::NewIndexer Scheduled Index Creator Init Error %+v", res)
		return nil, res
	}

	// Construct nodeInfo, which never changes in the future
	idx.nodeInfo = &service.NodeInfo{
		NodeID:   service.NodeID(idx.config["nodeuuid"].String()),
		Priority: service.Priority(common.INDEXER_CUR_VERSION),
	}

	// Start Rebalance Manager
	rebalMgr := NewRebalanceServiceManager(idx.rebalMgrCmdCh, idx.wrkrRecvCh, idx.wrkrPrioRecvCh,
		idx.config, idx.nodeInfo, idx.rebalanceRunning, idx.rebalanceToken, idx.statsMgr)

	// Start Pause-Resume Manager
	pauseMgr := NewPauseServiceManager(httpAddr)

	// Start Generic Service Manager, which also delegates to Pause and Rebalance Managers
	genericMgr := NewGenericServiceManager(idx.nodeInfo, pauseMgr, rebalMgr)

	// Register service managers with ns_server for RCP callbacks. This returns a single
	// MasterServiceManager object that implements all the interfaces we want callbacks for via
	// delegation to the API-specific objects passed to it.
	idx.masterMgr = NewMasterServiceManager(autofailoverMgr, genericMgr, pauseMgr, rebalMgr)

	go idx.monitorKVNodes()

	// enable inMemoryCompression feature on 7.1 cluster upgrade
	go idx.enablePlasmaInMemCompression()

	//start the main indexer loop
	idx.run()

	return idx, &MsgSuccess{}
}

func (idx *indexer) initSecurityContext(encryptLocalHost bool) error {

	certFile := idx.config["certFile"].String()
	keyFile := idx.config["keyFile"].String()
	caFile := idx.config["caFile"].String()
	clusterAddr := idx.config["clusterAddr"].String()
	logger := func(err error) { common.Console(clusterAddr, err.Error()) }
	if err := security.InitSecurityContext(logger, clusterAddr, certFile, keyFile, caFile, encryptLocalHost); err != nil {
		return err
	}

	fn := func(refreshCert bool, refreshEncrypt bool) error {
		select {
		case <-idx.enableSecurityChange:
		default:
			logging.Infof("Receive security change during indexer bootstrap.  Restarting indexer ...")
			os.Exit(1)
		}

		msg := &MsgSecurityChange{
			refreshCert:    refreshCert,
			refreshEncrypt: refreshEncrypt,
		}
		idx.internalRecvCh <- msg

		return nil
	}

	security.RegisterCallback("indexer", fn)

	if err := idx.refreshSecurityContextOnTopology(clusterAddr); err != nil {
		return err
	}

	return nil
}

func (idx *indexer) refreshSecurityContextOnTopology(clusterAddr string) error {

	fn := func(r int, e error) error {
		var cinfo *common.ClusterInfoCache
		url, err := common.ClusterAuthUrl(clusterAddr)
		if err != nil {
			return err
		}

		cinfo, err = common.NewClusterInfoCache(url, DEFAULT_POOL)
		if err != nil {
			return err
		}
		cinfo.SetUserAgent("Indexer::refreshSecurityContextOnTopology")
		cinfo.Lock()
		defer cinfo.Unlock()

		if err := cinfo.Fetch(); err != nil {
			return err
		}

		// When adding the node during the init time. nodesvs will not have httpPort in it.
		// Add Port mapping from command line if the mapping does not have port.
		mapping := cinfo.EncryptPortMapping()
		httpPort := idx.config["httpPort"].String()
		if _, ok := mapping[httpPort]; !ok {
			httpsPort := idx.config["httpsPort"].String()
			mapping[httpPort] = httpsPort
		}
		security.SetEncryptPortMapping(mapping)

		return nil
	}

	helper := common.NewRetryHelper(10, time.Second, 1, fn)
	return helper.Run()
}

func (idx *indexer) handleSecurityChange(msg Message) {

	exitFn := func(msg string) {
		logging.Infof(msg)
		os.Exit(1)
	}

	refreshEncrypt := msg.(*MsgSecurityChange).RefreshEncrypt()

	if refreshEncrypt {
		logging.Infof("handleSecurityChange: refresh security context")
		clusterAddr := idx.config["clusterAddr"].String()
		if err := idx.refreshSecurityContextOnTopology(clusterAddr); err != nil {
			exitFn(fmt.Sprintf("Fail to refresh security context on security change. Error %v", err))
		}
	}

	// stop HTTPS server
	idx.httpsSrvLock.Lock()
	if idx.httpsSrv != nil {
		// This does not close connections.  Use idx.httpSrv.Close() on 1.11
		idx.tlsListener.Close()
		idx.httpsSrv = nil
		idx.tlsListener = nil
	}
	idx.httpsSrvLock.Unlock()

	idx.httpSrvLock.Lock()
	if idx.httpSrv != nil {
		idx.tcpListener.Close()
		idx.httpSrv = nil
		idx.tcpListener = nil
	}
	idx.httpSrvLock.Unlock()

	if refreshEncrypt {
		// restart lifecyclemgr
		logging.Infof("handleSecurityChange: restarting index manager")

		if err := idx.sendMsgToClustMgrAndProcessResponse(msg); err != nil {
			exitFn(fmt.Sprintf("Fail to restart lifecycle mgr on security change. Error %v", err))
		}

		//restart mutation manager
		logging.Infof("handleSecurityChange: restarting mutation manager")
		if err := idx.sendMsgToWorker(msg, idx.mutMgrCmdCh); err != nil {
			exitFn(fmt.Sprintf("Fail to restart mutation mgr on security change. Error %v", err))
		}

		//restart scan coordinator
		logging.Infof("handleSecurityChange: restarting scan coordinator")
		if err := idx.sendMsgToWorker(msg, idx.scanCoordCmdCh); err != nil {
			exitFn(fmt.Sprintf("Fail to restart scan coordinator on security change. Error %v", err))
		}
	}

	// start HTTP server
	initHttp := func(r int, e error) error {
		logging.Infof("handleSecurityChange: restarting http server")
		return idx.initHttpServer()
	}
	rh := common.NewRetryHelper(10, time.Second, 1, initHttp)
	if err := rh.Run(); err != nil {
		exitFn(fmt.Sprintf("Fail to restart http server on security change. Error %v", err))
	}

	// start HTTPS server
	fn := func(r int, e error) error {
		logging.Infof("handleSecurityChange: restarting https server")
		return idx.initHttpsServer()
	}
	helper := common.NewRetryHelper(10, time.Second, 1, fn)
	if err := helper.Run(); err != nil {
		exitFn(fmt.Sprintf("Fail to restart https server on security change. Error %v", err))
	}

	if refreshEncrypt {
		// reset memcached connection
		logging.Infof("handleSecurityChange: restarting keyspace sequence cache")
		common.ResetBucketSeqnos()
	}

	logging.Infof("handleSecurityChange: done")
}

func (idx *indexer) initFromConfig() {

	// Read memquota setting
	memQuota := int64(idx.config.GetIndexerMemoryQuota())
	idx.stats.memoryQuota.Set(memQuota)
	plasma.SetMemoryQuota(int64(float64(memQuota) * PLASMA_MEMQUOTA_FRAC))
	memdb.Debug(idx.config["settings.moi.debug"].Bool())
	updateMOIWriters(idx.config["settings.moi.persistence_threads"].Int())
	reclaimBlockSize := int64(idx.config["plasma.LSSReclaimBlockSize"].Int())
	plasma.SetLogReclaimBlockSize(reclaimBlockSize)

	idx.initStreamAddressMap()
	idx.initStreamFlushMap()
	idx.initServiceAddressMap()
	idx.initStreamSessionIdMap()
	idx.initStreamPendBuildDone()
	idx.initStreamCollectionIdMap()

	idx.enableManager = idx.config["enableManager"].Bool()

	isEnterprise := idx.config["isEnterprise"].Bool()
	if isEnterprise {
		common.SetBuildMode(common.ENTERPRISE)
	} else {
		common.SetBuildMode(common.COMMUNITY)
	}
	logging.Infof("Indexer::NewIndexer Build Mode Set %v", common.GetBuildMode())

	// Check if cluster storage mode is set
	storageMode := idx.config["settings.storage_mode"].String()
	if storageMode != "" {
		if common.SetClusterStorageModeStr(storageMode) {
			logging.Infof("Indexer::Cluster Storage Mode Set %v", common.GetClusterStorageMode())
		} else {
			logging.Fatalf("Indexer::Cluster Invalid Storage Mode %v", storageMode)
		}
	}

	if mcdTimeout, ok := idx.config["memcachedTimeout"]; ok {
		common.SetDcpMemcachedTimeout(uint32(mcdTimeout.Int()))
		logging.Infof("memcachedTimeout set to %v\n", uint32(mcdTimeout.Int()))
	}
}

func GetHTTPMux() *http.ServeMux {
	if httpMux == nil {
		panic("httpMux is not initialized.")
	}

	return httpMux
}

func (idx *indexer) initHTTP() error {
	idx.initHTTPMux()
	idx.initPeriodicProfile()
	if err := idx.initHttpServer(); err != nil {
		return err
	}
	return idx.initHttpsServer()
}

func (idx *indexer) initHTTPMux() {

	httpMux = http.NewServeMux()

	overrideHttpDebugHandlers := func() {
		httpMux.HandleFunc("/debug/pprof/", common.PProfHandler)
		httpMux.HandleFunc("/debug/pprof/goroutine", common.GrHandler)
		httpMux.HandleFunc("/debug/pprof/block", common.BlockHandler)
		httpMux.HandleFunc("/debug/pprof/heap", common.HeapHandler)
		httpMux.HandleFunc("/debug/pprof/threadcreate", common.TCHandler)
		httpMux.HandleFunc("/debug/pprof/profile", common.ProfileHandler)
		httpMux.HandleFunc("/debug/pprof/cmdline", common.CmdlineHandler)
		httpMux.HandleFunc("/debug/pprof/symbol", common.SymbolHandler)
		httpMux.HandleFunc("/debug/pprof/trace", common.TraceHandler)
		httpMux.HandleFunc("/debug/vars", common.ExpvarHandler)
	}

	overrideHttpDebugHandlers()
	idx.settingsMgr.RegisterRestEndpoints()
	idx.statsMgr.RegisterRestEndpoints()
	idx.clustMgrAgent.RegisterRestEndpoints()
}

func (idx *indexer) initPeriodicProfile() {
	addr := net.JoinHostPort("", idx.config["httpPort"].String())
	logging.PeriodicProfile(logging.Debug, addr, "goroutine")
}

func (idx *indexer) initHttpServer() error {

	// Setup http server
	var addr string
	if !security.DisableNonSSLPort() {
		addr = net.JoinHostPort("", idx.config["httpPort"].String())
	} else {
		addr = net.JoinHostPort(security.GetLocalHost(), idx.config["httpPort"].String())
	}

	logging.Infof("indexer:: Staring http server : %v", addr)

	srv := &http.Server{
		ReadTimeout:       time.Duration(idx.config["http.readTimeout"].Int()) * time.Second,
		WriteTimeout:      time.Duration(idx.config["http.writeTimeout"].Int()) * time.Second,
		ReadHeaderTimeout: time.Duration(idx.config["http.readHeaderTimeout"].Int()) * time.Second,
		Addr:              addr,
		Handler:           GetHTTPMux(),
	}

	lsnr, err := security.MakeProtocolAwareTCPListener(addr)
	if err != nil {
		return fmt.Errorf("Error in creating TCP Listener: %v", err)
	}

	idx.httpSrvLock.Lock()
	idx.httpSrv = srv
	idx.tcpListener = lsnr
	idx.httpSrvLock.Unlock()

	go func() {
		// replace below with ListenAndServe on moving to go1.8
		if err := srv.Serve(lsnr); err != nil {
			logging.Errorf("indexer:: Error from Http Server: %v", err)

			idx.httpSrvLock.Lock()
			if idx.httpSrv != nil && idx.httpSrv == srv {
				// This does not close connections.  Use idx.httpSrv.Close() on 1.11
				lsnr.Close()

				// reset before releasing the lock
				idx.httpSrv = nil
				idx.tcpListener = nil

				// self restart
				time.Sleep(time.Duration(10) * time.Second)
				go idx.initHttpServer()
			}
			idx.httpSrvLock.Unlock()
		}
	}()

	return nil
}

func (idx *indexer) initHttpsServer() error {

	sslPort := idx.config["httpsPort"].String()
	if security.EncryptionEnabled() || len(sslPort) != 0 {

		sslAddr := net.JoinHostPort("", sslPort)

		// allow only strong ssl as this is an internal API and interop is not a concern
		sslsrv := &http.Server{
			Addr:    sslAddr,
			Handler: GetHTTPMux(),
		}
		if err := security.SecureHTTPServer(sslsrv); err != nil {
			return fmt.Errorf("Error in securing HTTPS server: %v", err)
		}

		// replace below with ListenAndServeTLS on moving to go1.8
		lsnr, err := security.MakeAndSecureTCPListener(sslAddr)
		if err != nil {
			return fmt.Errorf("Error in creating SSL Listener: %v", err)
		}

		logging.Infof("indexer:: SSL server started: %v", sslAddr)

		idx.httpsSrvLock.Lock()
		idx.httpsSrv = sslsrv
		idx.tlsListener = lsnr
		idx.httpsSrvLock.Unlock()

		go func() {
			if err := sslsrv.Serve(lsnr); err != nil {
				logging.Errorf("HTTPS Server terminates on error: %v", err)

				idx.httpsSrvLock.Lock()
				if idx.httpsSrv != nil && idx.httpsSrv == sslsrv {
					// This does not close connections.  Use idx.httpSrv.Close() on 1.11
					idx.tlsListener.Close()

					// reset before releasing the lock
					idx.httpsSrv = nil
					idx.tlsListener = nil

					// self restart
					go idx.initHttpsServer()
				}
				idx.httpsSrvLock.Unlock()
			}
		}()
	}

	return nil
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

func (idx *indexer) acquireStreamRequestLock(keyspaceId string, streamId common.StreamId) *kvRequest {

	// queue request
	request := &kvRequest{grantCh: make(chan bool, 1), lock: nil, keyspaceId: keyspaceId, streamId: streamId}

	idx.kvlock.Lock()
	defer idx.kvlock.Unlock()

	// allocate the request queue
	rq, ok := idx.streamKeyspaceIdRequestQueue[streamId][keyspaceId]
	if !ok {
		rq = make(chan *kvRequest, 5000)

		if _, ok = idx.streamKeyspaceIdRequestQueue[streamId]; !ok {
			idx.streamKeyspaceIdRequestQueue[streamId] = make(map[string]chan *kvRequest)
		}
		idx.streamKeyspaceIdRequestQueue[streamId][keyspaceId] = rq
	}

	// allocate the lock
	lq, ok := idx.streamKeyspaceIdRequestLock[streamId][keyspaceId]
	if !ok {
		lq = make(chan *sync.Mutex, 1) // hold one lock

		if _, ok = idx.streamKeyspaceIdRequestLock[streamId]; !ok {
			idx.streamKeyspaceIdRequestLock[streamId] = make(map[string]chan *sync.Mutex)
		}
		idx.streamKeyspaceIdRequestLock[streamId][keyspaceId] = lq

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
	keyspaceId := req.keyspaceId

	rq, ok := idx.streamKeyspaceIdRequestQueue[streamId][keyspaceId]
	if ok && len(rq) != 0 {
		next := <-rq
		next.lock = req.lock
		next.grantCh <- true
	} else {
		if lq, ok := idx.streamKeyspaceIdRequestLock[streamId][keyspaceId]; ok {
			lq <- req.lock
		} else {
			common.CrashOnError(errors.New("releaseStreamRequestLock: streamKeyspaceIdRequestLock is not initialized"))
		}
	}
}

const MSG_LOOP_MARKER string = "msg_loop"             // to grep for all the following logXyz messages
const MSG_PROCESSING_SLOW time.Duration = time.Minute // threshold for warning of slow message processing

// logProcessingTime logs the time it took to process a message. To avoid log flooding, this logs at levels
//   Warn  -- if time taken is long, regardless of forceLog flag
//   Info  -- if time taken is short but forceLog flag is true (admin messages = DDL)
//   Debug -- if time taken is short and forceLog flag is false
// classMethod is logging prefix of form "class::method".
func logProcessingTime(classMethod string, msg Message, channel string, timeTaken time.Duration, forceLog bool) {

	if timeTaken > MSG_PROCESSING_SLOW {
		logging.Warnf("%v:%v: %v message from %v channel processing took %v > %v",
			classMethod, MSG_LOOP_MARKER, msg.GetMsgType().String(), channel, timeTaken, MSG_PROCESSING_SLOW)
		return
	}

	var loggingFunc func(format string, v ...interface{})
	if forceLog {
		loggingFunc = logging.Infof
	} else if logging.IsEnabled(logging.Debug) {
		loggingFunc = logging.Debugf
	}
	if loggingFunc != nil {
		loggingFunc("%v:%v: %v message from %v channel processing took %v",
			classMethod, MSG_LOOP_MARKER, msg.GetMsgType().String(), channel, timeTaken)
	}
}

// logStreamRequestLockTime logs the time consumed waiting for stream request locks in drop processing.
// If this is slow it logs at Warn level, else Info.
// classMethod is logging prefix of form "class::method".
func logStreamRequestLockTime(classMethod string, msg Message, channel string, timeTaken time.Duration) {
	if timeTaken <= MSG_PROCESSING_SLOW {
		logging.Infof("%v:%v: %v message from %v channel stream request lock waits took %v",
			classMethod, MSG_LOOP_MARKER, msg.GetMsgType().String(), channel, timeTaken)
	} else {
		logging.Warnf("%v:%v: %v message from %v channel stream request lock waits took %v > %v",
			classMethod, MSG_LOOP_MARKER, msg.GetMsgType().String(), channel, timeTaken, MSG_PROCESSING_SLOW)
	}
}

// logShutdownStart logs a shutdown starting message for a non-trivial message processing loop shutdown.
// classMethod is logging prefix of form "class::method".
func logShutdownStart(classMethod string) {
	logging.Infof("%v:%v: shutdown starting",
		classMethod, MSG_LOOP_MARKER)
}

// logShutdownComplete logs a standard shutdown complete message for a message procesing loop.
// classMethod is logging prefix of form "class::method".
func logShutdownComplete(classMethod string) {
	logging.Infof("%v:%v: shutdown complete",
		classMethod, MSG_LOOP_MARKER)
}

//run starts the main loop for the indexer
func (idx *indexer) run() {
	const classMethod string = "Indexer::run" // for logging

	go idx.listenWorkerMsgs() // wrkrRecvCh
	go idx.listenAdminMsgs()  // adminRecvCh

	for {
		var msg Message
		var ok bool               // needed as "msg, ok := <-" shadows msg when creating ok
		var receiveTime time.Time // time msg was received
		var channel string        // name of channel msg came from
		var forceLog bool         // admin messages force logging of processing time

		// Process everything in high-priority channel first
		for len(idx.wrkrPrioRecvCh) > 0 {
			select {

			case msg, ok = <-idx.wrkrPrioRecvCh:
				if ok {
					receiveTime = time.Now()
					idx.handleWorkerMsgs(msg)
					logProcessingTime(classMethod, msg, "wrkrPrioRecvCh", time.Since(receiveTime), true)
				}

			case <-idx.shutdownInitCh:
				logShutdownStart(classMethod)
				idx.shutdownWorkers()
				close(idx.shutdownCompleteCh)
				logShutdownComplete(classMethod)
				return
			}
		}

		// Process next message to arrive in any channel
		select {

		case msg, ok = <-idx.wrkrPrioRecvCh:
			if ok {
				receiveTime = time.Now()
				channel = "wrkrPrioRecvCh"
				forceLog = true
				idx.handleWorkerMsgs(msg)
			}

		case msg, ok = <-idx.internalRecvCh:
			if ok {
				receiveTime = time.Now()
				channel = "internalRecvCh"
				forceLog = false
				idx.handleWorkerMsgs(msg)
			}

		case msg, ok = <-idx.internalAdminRecvCh:
			if ok {
				receiveTime = time.Now()
				channel = "internalAdminRecvCh"
				forceLog = true
				resp := idx.handleAdminMsgs(msg)
				idx.internalAdminRespCh <- resp
			}

		case <-idx.shutdownInitCh:
			logShutdownStart(classMethod)

			//send shutdown to all workers
			idx.shutdownWorkers()
			//close the shutdown complete channel to indicate
			//all workers are shutdown
			close(idx.shutdownCompleteCh)

			logShutdownComplete(classMethod)
			return
		}
		logProcessingTime(classMethod, msg, channel, time.Since(receiveTime), forceLog)
	}
}

// listenAdminMsgs is the message processing loop for the adminRecvCh channel.
func (idx *indexer) listenAdminMsgs() {
	const classMethod string = "Indexer::listenAdminMsgs" // for logging

	for {
		select {
		case msg, ok := <-idx.adminRecvCh:
			if ok {
				receiveTime := time.Now()

				// internalAdminRecvCh size is 1.   So it will blocked if the previous msg is being
				// processed.
				idx.internalAdminRecvCh <- msg
				resp := <-idx.internalAdminRespCh

				// now that indexer has processed the message.  Let's make sure that
				// the stream request is finished before processing the next admin
				// msg.  This is done by acquiring a lock on the stream request for each
				// bucket (on both streams).   The lock is FIFO, so if this function
				// can get a lock, it will mean that previous stream request would have
				// been cleared.

				//create and build don't need to be checked
				//create doesn't take stream lock. build only works
				//on a fresh stream.
				if msg.GetMsgType() == CLUST_MGR_DROP_INDEX_DDL ||
					msg.GetMsgType() == CLUST_MGR_PRUNE_PARTITION {

					if resp.GetMsgType() == MSG_SUCCESS_DROP {
						streamId := resp.(*MsgSuccessDrop).GetStreamId()
						keyspaceId := resp.(*MsgSuccessDrop).GetKeyspaceId()

						f := func(streamId common.StreamId, keyspaceId string) {
							lock := idx.acquireStreamRequestLock(keyspaceId, streamId)
							defer idx.releaseStreamRequestLock(lock)
							idx.waitStreamRequestLock(lock)
						}

						startStreamRequestLockTime := time.Now()
						f(streamId, keyspaceId)
						logStreamRequestLockTime(classMethod, msg, "adminRecvCh", time.Since(startStreamRequestLockTime))
					}
				}
				logProcessingTime(classMethod, msg, "adminRecvCh", time.Since(receiveTime), true)
			}
		case <-idx.shutdownInitCh:
			logShutdownComplete(classMethod)
			return
		}
	}
}

func (idx *indexer) getKeyspaceIdForAdminMsg(msg Message) []string {
	switch msg.GetMsgType() {

	case CLUST_MGR_CREATE_INDEX_DDL, CBQ_CREATE_INDEX_DDL:
		createMsg := msg.(*MsgCreateIndex)
		return []string{createMsg.GetIndexInst().Defn.Bucket}

	case CLUST_MGR_BUILD_INDEX_DDL:
		buildMsg := msg.(*MsgBuildIndex)
		return buildMsg.GetBucketList()

	case CLUST_MGR_DROP_INDEX_DDL, CBQ_DROP_INDEX_DDL:
		dropMsg := msg.(*MsgDropIndex)
		return []string{dropMsg.GetKeyspaceId()}

	default:
		return nil
	}
}

// listenWorkerMsgs is the message processing loop for the wrkrRecvCh channel.
func (idx *indexer) listenWorkerMsgs() {
	const classMethod string = "Indexer::listenWorkerMsgs" // for logging

	//listen to worker messages
	for {
		select {

		case msg, ok := <-idx.wrkrRecvCh:
			if ok {
				receiveTime := time.Now()
				if msg.GetMsgType() == MSG_ERROR {
					err := msg.(*MsgError).GetError()
					if err.code == ERROR_MUT_MGR_PANIC {
						close(idx.mutMgrExitCh)
					}
				}
				idx.internalRecvCh <- msg
				logProcessingTime(classMethod, msg, "wrkrRecvCh", time.Since(receiveTime), false)
			}

		case <-idx.shutdownInitCh:
			//exit the loop
			logShutdownComplete(classMethod)
			return
		}
	}
}

// handleWorkerMsgs handles worker messages (wrkrPrioRecvCh, internalRecvCh).
func (idx *indexer) handleWorkerMsgs(msg Message) {

	switch msg.GetMsgType() {

	case STREAM_READER_HWT,
		STREAM_READER_STREAM_BEGIN,
		STREAM_READER_STREAM_END,
		STREAM_READER_SNAPSHOT_MARKER,
		STREAM_READER_CONN_ERROR,
		STREAM_READER_SYSTEM_EVENT,
		STREAM_READER_OSO_SNAPSHOT_MARKER:

		if msg.GetMsgType() == STREAM_READER_SYSTEM_EVENT {
			idx.handleDcpSystemEvent(msg)
		}

		//fwd the message to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

	case STREAM_READER_STREAM_DROP_DATA:

		logging.Warnf("Indexer::handleWorkerMsgs Received Drop Data "+
			"From Mutation Mgr %v. Ignored.", msg)

	case TK_STABILITY_TIMESTAMP:
		//send TS to Mutation Manager
		ts := msg.(*MsgTKStabilityTS).GetTimestamp()
		keyspaceId := msg.(*MsgTKStabilityTS).GetKeyspaceId()
		streamId := msg.(*MsgTKStabilityTS).GetStreamId()
		changeVec := msg.(*MsgTKStabilityTS).GetChangeVector()
		countVec := msg.(*MsgTKStabilityTS).GetCountVector()
		hasAllSB := msg.(*MsgTKStabilityTS).HasAllSB()

		if idx.getStreamKeyspaceIdState(streamId, keyspaceId) == STREAM_INACTIVE {
			logging.Warnf("Indexer: Skipped PersistTs for %v %v. "+
				"STREAM_INACTIVE", streamId, keyspaceId)
			return
		}

		idx.streamKeyspaceIdFlushInProgress[streamId][keyspaceId] = true

		if ts.GetSnapType() == common.FORCE_COMMIT {
			idx.storageMgrCmdCh <- &MsgMutMgrFlushDone{mType: MUT_MGR_FLUSH_DONE,
				streamId:   streamId,
				keyspaceId: keyspaceId,
				ts:         ts,
				hasAllSB:   hasAllSB}
			<-idx.storageMgrCmdCh
		} else {
			idx.mutMgrCmdCh <- &MsgMutMgrFlushMutationQueue{
				mType:      MUT_MGR_PERSIST_MUTATION_QUEUE,
				keyspaceId: keyspaceId,
				ts:         ts,
				streamId:   streamId,
				changeVec:  changeVec,
				countVec:   countVec,
				hasAllSB:   hasAllSB}

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

		keyspaceId := msg.(*MsgMutMgrFlushDone).GetKeyspaceId()
		streamId := msg.(*MsgMutMgrFlushDone).GetStreamId()

		// consolidate partitions now
		idx.mergePartitions(keyspaceId, streamId)
		idx.mergePartitionForIdleKeyspaceIds()
		idx.prunePartitions(keyspaceId, streamId)
		idx.prunePartitionForIdleKeyspaceIds()

		//process any pending collection drop
		if instIdList, ok := idx.streamKeyspaceIdPendCollectionDrop[streamId][keyspaceId]; ok &&
			len(instIdList) != 0 {
			logging.Infof("Indexer::StorageSnapDone Cleaning up index data for stream: %v, keyspaceId: %v, instIdList: %v",
				streamId, keyspaceId, instIdList)
			idx.cleanupIndexDataForCollectionDrop(streamId, keyspaceId, instIdList)
			delete(idx.streamKeyspaceIdPendCollectionDrop[streamId], keyspaceId)
		}

		idx.streamKeyspaceIdFlushInProgress[streamId][keyspaceId] = false

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

	case TK_ADD_INSTANCE_FAIL:
		idx.handleAddInstanceFail(msg)

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

	case INDEX_STATS_DONE, INDEX_STATS_BROADCAST,
		INDEX_BOOTSTRAP_STATS_UPDATE:
		idx.sendMsgToClustMgr(msg)

	case INDEXER_KEYSPACE_NOT_FOUND:
		idx.handleKeyspaceNotFound(msg)

	case INDEXER_MTR_FAIL:
		idx.handleMTRFail(msg)

	case INDEXER_STATS:
		idx.handleStats(msg)

	case MSG_ERROR:
		//crash for all errors by default
		logging.Fatalf("Indexer::handleWorkerMsgs Fatal Error On Worker Channel %+v", msg)
		respCh := make(chan bool)
		idx.statsMgrCmdCh <- &MsgStatsPersister{
			mType:  STATS_LOG_AT_EXIT,
			respCh: respCh}
		<-respCh // Wait for response
		err := msg.(*MsgError).GetError()
		common.CrashOnError(err.cause)

	case STREAM_READER_ERROR:
		//crash for all errors by default
		logging.Fatalf("Indexer::handleWorkerMsgs Fatal Error On Worker Channel %+v", msg)
		respCh := make(chan bool)
		idx.statsMgrCmdCh <- &MsgStatsPersister{
			mType:  STATS_LOG_AT_EXIT,
			respCh: respCh}
		<-respCh // Wait for response
		err := msg.(*MsgStreamError).GetError()
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

	case INDEXER_STORAGE_WARMUP_DONE:
		idx.handleStorageWarmupDone(msg)

	case STATS_READ_PERSISTED_STATS:
		idx.handleReadPersistedStats(msg)

	case UPDATE_MAP_WORKER:
		idx.handleUpdateMapToWorker(msg)

	case ADD_INDEX_INSTANCE:
		idx.handleAddIndexInstanceAtWorker(msg)

	case STORAGE_UPDATE_SNAP_MAP:
		idx.storageMgrCmdCh <- msg
		<-idx.storageMgrCmdCh

	case INDEXER_SECURITY_CHANGE:
		idx.handleSecurityChange(msg)

	case STORAGE_ROLLBACK_DONE:
		idx.handleStorageRollbackDone(msg)

	case INDEXER_UPDATE_BUILD_TS:
		idx.handleUpdateBuildTs(msg)

	case POOL_CHANGE:
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

	case TK_INIT_BUILD_DONE_NO_CATCHUP_ACK:
		idx.handleBuildDoneNoCatchupAck(msg)

	case INDEXER_RESET_INDEX_DONE:
		idx.handleResetIndexDone(msg)

	case RESET_STREAM:
		idx.handleResetStream(msg)

	case INDEXER_DROP_COLLECTION:
		idx.handleDropCollection(msg)

	default:
		logging.Fatalf("Indexer::handleWorkerMsgs Unknown Message %+v", msg)
		common.CrashOnError(errors.New("Unknown Msg On Worker Channel"))
	}

}

func (idx *indexer) handleDropCollection(msg Message) {
	streamId := msg.(*MsgIndexerDropCollection).GetStreamId()
	keyspaceId := msg.(*MsgIndexerDropCollection).GetKeyspaceId()
	scopeId := msg.(*MsgIndexerDropCollection).GetScopeId()
	collectionId := msg.(*MsgIndexerDropCollection).GetCollectionId()
	idx.processCollectionDrop(streamId, keyspaceId, scopeId, collectionId)
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
					//restart is only required for ForestDB storage engine
					//to initialize the buffer cache correctly
					if confStorageMode == common.ForestDB {
						logging.Infof("Indexer::updateStorageMode Storage Mode Set %v. Restarting indexer", common.GetStorageMode())
						idx.stats.needsRestart.Set(true)
						os.Exit(0)
					} else {
						logging.Infof("Indexer::updateStorageMode Storage Mode Set %v. ", common.GetStorageMode())
						if idx.getIndexerState() == common.INDEXER_ACTIVE &&
							common.GetStorageMode() == common.PLASMA {
							RecoveryDone()
						}
					}
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
					logging.Infof("Indexer::updateStorageMode Storage Mode Set %v. Restarting indexer", common.GetStorageMode())
					idx.stats.needsRestart.Set(true)
					os.Exit(0)
				} else {
					logging.Infof("Indexer::updateStorageMode Invalid Storage Mode %v", confStorageMode)
				}
			}
		}
	}
}

// handleConfigUpdate updates Indexer config settings and propagates them to children / workers.
func (idx *indexer) handleConfigUpdate(msg Message) {

	cfgUpdate := msg.(*MsgConfigUpdate)
	oldConfig := idx.config
	newConfig := cfgUpdate.GetConfig()

	idx.updateStorageMode(newConfig)

	if newConfig["settings.memory_quota"].Uint64() !=
		oldConfig["settings.memory_quota"].Uint64() {

		memQuota := int64(newConfig.GetIndexerMemoryQuota())
		idx.stats.memoryQuota.Set(memQuota)
		plasma.SetMemoryQuota(int64(float64(memQuota) * PLASMA_MEMQUOTA_FRAC))

		if common.GetStorageMode() == common.FORESTDB ||
			common.GetStorageMode() == common.NOT_SET {
			logging.Infof("Indexer::handleConfigUpdate restart indexer due to memory_quota")
			idx.stats.needsRestart.Set(true)
			os.Exit(0)
		}
	}

	if common.GetStorageMode() == common.MOI {
		if moiPersisters := newConfig["settings.moi.persistence_threads"].Int(); moiPersisters != oldConfig["settings.moi.persistence_threads"].Int() {
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

	if newConfig["settings.compaction.plasma.manual"].Bool() !=
		oldConfig["settings.compaction.plasma.manual"].Bool() {
		logging.Infof("Indexer::handleConfigUpdate restart indexer due to compaction.plasma.manual")
		idx.stats.needsRestart.Set(true)
		os.Exit(0)
	}

	if percent, ok := newConfig["settings.gc_percent"]; ok && percent.Int() > 0 {
		logging.Infof("Indexer: Setting GC percent to %v", percent.Int())
		debug.SetGCPercent(percent.Int())
	}

	if newConfig["api.enableTestServer"].Bool() && !idx.testServRunning {
		// Start indexer endpoints for CRUD operations.
		// Initialize the QE REST server on config change.
		certFile := newConfig["certFile"].String()
		keyFile := newConfig["keyFile"].String()
		NewTestServer(newConfig["clusterAddr"].String(), certFile, keyFile)
		idx.testServRunning = true
	}

	if mcdTimeout, ok := newConfig["memcachedTimeout"]; ok {
		if mcdTimeout.Int() != oldConfig["memcachedTimeout"].Int() {
			common.SetDcpMemcachedTimeout(uint32(mcdTimeout.Int()))
			logging.Infof("memcachedTimeout set to %v\n", uint32(mcdTimeout.Int()))
		}
	}

	if workersPerReader, ok := newConfig["vbseqnos.workers_per_reader"]; ok {
		if newConfig["vbseqnos.workers_per_reader"].Int() !=
			oldConfig["vbseqnos.workers_per_reader"].Int() {
			common.UpdateVbSeqnosWorkersPerReader(int32(workersPerReader.Int()))
			common.ResetBucketSeqnos()
		}
	}

	memdb.Debug(newConfig["settings.moi.debug"].Bool())
	idx.setProfilerOptions(newConfig)
	idx.cpuThrottle.SetCpuTarget(newConfig["cpu.throttle.target"].Float64())

	idx.config = newConfig

	idx.compactMgrCmdCh <- msg
	<-idx.compactMgrCmdCh
	idx.tkCmdCh <- msg
	<-idx.tkCmdCh
	idx.scanCoordCmdCh <- msg
	<-idx.scanCoordCmdCh

	// sendMsgToKVSender lock protects writes and reads to
	// kvSenderCmdCh so that message crossover is prevented
	idx.sendMsgToKVSender(msg)

	idx.mutMgrCmdCh <- msg
	<-idx.mutMgrCmdCh
	idx.statsMgrCmdCh <- msg
	<-idx.statsMgrCmdCh
	idx.rebalMgrCmdCh <- msg
	<-idx.rebalMgrCmdCh
	idx.ddlSrvMgrCmdCh <- msg
	<-idx.ddlSrvMgrCmdCh
	idx.schedIdxCreatorCmdCh <- msg
	<-idx.schedIdxCreatorCmdCh
	if common.GetBuildMode() == common.ENTERPRISE && common.GetDeploymentModel() == common.SERVERLESS_DEPLOYMENT {
		idx.meteringMgrCmdCh <- msg
		<-idx.meteringMgrCmdCh
	}

	idx.sendMsgToClustMgr(msg)

	idx.storageMgrCmdCh <- msg
	<-idx.storageMgrCmdCh
	idx.updateSliceWithConfig(newConfig)

	newUseCInfoLite := newConfig["use_cinfo_lite"].Bool()
	oldUseCInfoLite := oldConfig["use_cinfo_lite"].Bool()
	if oldUseCInfoLite != newUseCInfoLite {
		logging.Infof("Indexer::handleConfigUpdate: Updating ClusterInfoProvider in Indexer")

		cip, err := common.NewClusterInfoProvider(newUseCInfoLite,
			newConfig["clusterAddr"].String(), DEFAULT_POOL, "indexer", newConfig)
		if err != nil {
			logging.Errorf("Indexer::handleConfigUpdate Unable to update ClusterInfoProvider in Indexer err: %v, use_cinfo_lite: old %v new %v",
				err, oldUseCInfoLite, newUseCInfoLite)
			common.CrashOnError(err)
		}

		idx.cinfoProviderLock.Lock()
		oldPtr := idx.cinfoProvider
		idx.cinfoProvider = cip
		idx.cinfoProviderLock.Unlock()

		logging.Infof("Indexer::handleConfigUpdate Updated ClusterInfoProvider in Indexer use_cinfo_lite: old %v new %v",
			oldUseCInfoLite, newUseCInfoLite)

		oldPtr.Close()
	}
}

// handleAdminMsgs handles admin (DDL) messages (internalAdminRecvCh).
func (idx *indexer) handleAdminMsgs(msg Message) (resp Message) {

	switch msg.GetMsgType() {

	case CLUST_MGR_CREATE_INDEX_DDL,
		CBQ_CREATE_INDEX_DDL:

		idx.handleCreateIndex(msg)
		resp = &MsgSuccess{}

	case CLUST_MGR_BUILD_INDEX_DDL:
		idx.handleBuildIndex(msg)
		resp = &MsgSuccess{}

	case CLUST_MGR_DROP_INDEX_DDL,
		CBQ_DROP_INDEX_DDL:
		resp = idx.handleDropIndex(msg)

	case CLUST_MGR_PRUNE_PARTITION:
		resp = idx.handlePrunePartition(msg)

	case MSG_ERROR:

		logging.Fatalf("Indexer::handleAdminMsgs Fatal Error On Admin Channel %+v", msg)
		err := msg.(*MsgError).GetError()
		respCh := make(chan bool)
		idx.statsMgrCmdCh <- &MsgStatsPersister{
			mType:  STATS_LOG_AT_EXIT,
			respCh: respCh}
		<-respCh // Wait for response
		common.CrashOnError(err.cause)

	default:
		logging.Errorf("Indexer::handleAdminMsgs Unknown Message %+v", msg)
		common.CrashOnError(errors.New("Unknown Msg On Admin Channel"))

	}

	return

}

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

	var ephemeral, valid bool
	var err error

	func() {
		idx.cinfoProviderLock.RLock()
		defer idx.cinfoProviderLock.RUnlock()

		valid = idx.cinfoProvider.ValidateBucket(indexInst.Defn.Bucket, []string{indexInst.Defn.BucketUUID})
		if valid {
			ephemeral, err = idx.cinfoProvider.IsEphemeral(indexInst.Defn.Bucket)
		}
	}()

	if !valid {
		logging.Errorf("Indexer::handleCreateIndex Bucket %v Not Found")

		if clientCh != nil {
			clientCh <- &MsgError{
				err: Error{code: ERROR_INDEXER_UNKNOWN_BUCKET,
					severity: FATAL,
					cause:    ErrUnknownBucket,
					category: INDEXER}}

		}
		return
	}

	if err != nil {
		errStr := fmt.Sprintf("Cannot Query Bucket Type of %v", indexInst.Defn.Bucket)
		logging.Errorf("Indexer::handleCreateIndex %v", errStr)
		if clientCh != nil {
			clientCh <- &MsgError{
				err: Error{severity: FATAL,
					cause:    errors.New(errStr),
					category: INDEXER}}

		}
		return
	}

	if ephemeral {
		allowed, reason, err := idx.isAllowedEphemeral(indexInst.Defn.Bucket)
		if err != nil {
			errStr := fmt.Sprintf("Cannot check if index creation is allowed on ephemeral bucket %v. Error %v",
				indexInst.Defn.Bucket, err)

			logging.Errorf("Indexer::handleCreateIndex %v", errStr)

			if clientCh != nil {
				clientCh <- &MsgError{
					err: Error{severity: FATAL,
						cause:    errors.New(errStr),
						category: INDEXER,
					},
				}
			}

			return
		}

		if !allowed {
			logging.Errorf("Indexer::handleCreateIndex %v", reason)

			if clientCh != nil {
				clientCh <- &MsgError{
					err: Error{code: ERROR_BUCKET_EPHEMERAL_STD,
						severity: FATAL,
						cause:    ErrBucketEphemeralStd,
						category: INDEXER,
					},
				}
			}

			return
		}
	}

	if idx.rebalanceRunning || idx.rebalanceToken != nil {
		reqCtx := msg.(*MsgCreateIndex).GetRequestCtx()
		if reqCtx.ReqSource == common.DDLRequestSourceUser {
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
		idx.stats.AddPartitionStats(indexInst, partnDefn.GetPartitionId())
	}

	//allocate partition/slice
	partnInstMap, _, err := idx.initPartnInstance(indexInst, clientCh, false)
	if err != nil {
		for _, partnDefn := range partitions {
			idx.stats.RemovePartitionStats(indexInst.InstId, partnDefn.GetPartitionId())
		}
		idx.stats.RemoveIndexStats(indexInst)
		return
	}

	// update rollback time for the bucket
	if _, ok := idx.keyspaceIdRollbackTimes[indexInst.Defn.Bucket]; !ok {
		idx.keyspaceIdRollbackTimes[indexInst.Defn.Bucket] = time.Now().UnixNano()
	}

	//update index maps with this index
	idx.indexInstMap[indexInst.InstId] = indexInst
	idx.indexPartnMap[indexInst.InstId] = partnInstMap

	msgUpdateIndexInstMap := idx.newIndexInstMsg(idx.indexInstMap)
	msgUpdateIndexInstMap.AppendUpdatedInsts(common.IndexInstList{indexInst})

	msgUpdateIndexPartnMap := &MsgUpdatePartnMap{indexPartnMap: idx.indexPartnMap}
	msgUpdateIndexPartnMap.SetUpdatedPartnMap(partnInstMap)

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

func (idx *indexer) isAllowedEphemeral(bucket string) (bool, string, error) {
	if common.GetStorageMode() == common.MOI {
		return true, "", nil
	}

	idx.cinfoProviderLock.RLock()
	defer idx.cinfoProviderLock.RUnlock()

	cVersion := idx.cinfoProvider.ClusterVersion()
	if cVersion < common.INDEXER_70_VERSION {
		retMsg := fmt.Sprintf("Bucket %v is Ephemeral. Standard GSI index on Ephemeral buckets"+
			" is supported only on fully upgraded cluster.", bucket)
		return false, retMsg, nil
	}

	if cVersion >= common.INDEXER_71_VERSION {
		return true, "", nil
	}

	ninfo, err := idx.cinfoProvider.GetNodesInfoProvider()
	if ninfo == nil || err != nil {
		return false, "", fmt.Errorf("Cluster info cache is nil.")
	}

	ver, err := common.GetInternalIndexerVersion(ninfo, false)
	if err != nil {
		return false, "", err
	}

	logging.Infof("indexer::isAllowedEphemeral While creating index on ephemeral bucket %v, internal indexer version is (%v)", bucket, ver)

	if ver.LessThan(common.InternalVersion(common.MIN_VER_STD_GSI_EPHEMERAL)) {
		retMsg := fmt.Sprintf("Bucket %v is Ephemeral. Standard GSI index on Ephemeral buckets"+
			" is supported only on fully upgraded cluster.", bucket)
		return false, retMsg, nil
	}

	return true, "", nil
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

	logging.Infof("Indexer::handleMergePartition Source %v Target %v", srcInstId, tgtInstId)

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

	idx.mergePartitionForIdleKeyspaceIds()
}

// When a inst needs to be merged, one of the following can happens:
// 1) index RState is set to ACTIVE
// 2) index is merged
// 2) merge is postponed because flush is in progress.
// 3) merge is postponed because the source is not active.
// 4) merge is postponed because of other reasons (indxer pause, recovery).
//
// For those merge that is postponed, indexer needs to retry when the bucket flush is idle.
//
func (idx *indexer) mergePartitionForIdleKeyspaceIds() {

	if len(idx.mergePartitionList) > 0 {
		buckets := make(map[string]map[common.StreamId]bool)
		for _, spec := range idx.mergePartitionList {
			sourceId := spec.srcInstId
			if source, ok := idx.indexInstMap[sourceId]; ok {
				if _, ok := buckets[source.Defn.Bucket]; !ok {
					buckets[source.Defn.Bucket] = make(map[common.StreamId]bool)
				}
				buckets[source.Defn.Bucket][source.Stream] = true
			}
		}

		for bucket, streams := range buckets {
			for streamId, _ := range streams {
				if !idx.streamKeyspaceIdFlushInProgress[streamId][bucket] {
					idx.mergePartitions(bucket, streamId)
				}
			}
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
// Merge partiton can only be performed when:
// 1) After a snapshot
// 2) there is no flush (idle bucket)
// 3) There is no recovery for bucket stream
// 4) Indexer is active
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
// Merge partition updates the indexer's state in 4 phases:
// 1) update indexer internal data structure
// 2) move partitions in index snapshot in storage manager
// 3) update index metadata.  Once metadata is updated, the
//    merge operation is considered committed.
// 4) remove the merged inst from bucket stream
//
// For step (4), stream update is queued and done in batches.
// If the corresponding stream is closed, all queued stream
// update will be removed.   All queued stream will also be
// cleared when the stream restarted.
//
// If recovery starts,
// 1) bucket stream will be closed during prepare phase.   Any queued
//    stream update will be dropped
// 2) For any in-flight stream update that has already started, it can succeed
//    or fail.  If fail, stream update will abort due to recovery.
//    Recovery can only start after all in-flight are done (due to stream lock).
// 3) When bucket stream re-starts for recovery, the bucket stream
//    will use the latest state of each index inst.  So those merged inst
//    will not be included in the new bucket stream.
// 4) New merge operation will be deferred until recovery is done.   So
//    no new stream update will be queued while there is recovery.
// 5) After recvovery is done, merge operation will be processed as normal.
//
// For deferred index, partitions can be merged during recovery.   There is
// no stream update for deferred index.
//
//
func (idx *indexer) mergePartitions(keyspaceId string, streamId common.StreamId) {

	// Do not merge when indexer is not active
	if is := idx.getIndexerState(); is != common.INDEXER_ACTIVE {
		return
	}

	// nothing to merge
	if len(idx.mergePartitionList) == 0 {
		return
	}

	// Do not merge during recovery
	if streamId != common.NIL_STREAM && idx.getStreamKeyspaceIdState(streamId, keyspaceId) != STREAM_ACTIVE {
		logging.Debugf("MergePartition.  Indexer in recovery.  Cannot merge instance.")
		return
	}

	logging.Infof("MergePartitions: keyspaceId %v streamId %v", keyspaceId, streamId)

	var remaining []mergeSpec
	if len(idx.mergePartitionList) != 0 {
		remaining = make([]mergeSpec, 0, len(idx.mergePartitionList))
	}

	for _, spec := range idx.mergePartitionList {

		sourceId := spec.srcInstId
		targetId := spec.tgtInstId
		respch := spec.respch

		if !idx.mergePartition(keyspaceId, streamId, sourceId, targetId, idx.merged, respch) {
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
					if streamId != common.NIL_STREAM && idx.getStreamKeyspaceIdState(streamId, bucket) == STREAM_ACTIVE {
						idx.removeIndexesFromStream(list4, bucket, bucketUUID, streamId, state, nil)
					}
				}
			}
		}
	}
}

func (idx *indexer) removePendingStreamUpdate(indexes map[common.IndexInstId]common.IndexInst, streamId common.StreamId,
	bucket string) map[common.IndexInstId]common.IndexInst {

	remaining := make(map[common.IndexInstId]common.IndexInst)

	sorted := make(map[string]map[common.StreamId][]common.IndexInst)
	for instId, _ := range indexes {
		inst, ok := idx.indexInstMap[instId]
		if !ok {
			logging.Warnf("removePendingStreamUpdate:  inst %v does not exist in indexInstMap. Skip stream update.", instId)
			continue
		}

		if _, ok := sorted[inst.Defn.Bucket]; !ok {
			sorted[inst.Defn.Bucket] = make(map[common.StreamId][]common.IndexInst)
		}

		sorted[inst.Defn.Bucket][inst.Stream] = append(sorted[inst.Defn.Bucket][inst.Stream], inst)
	}

	for bucket1, list1 := range sorted {
		for streamId1, list2 := range list1 {
			if bucket1 != bucket || streamId1 != streamId {
				for _, inst := range list2 {
					remaining[inst.InstId] = inst
				}
			}
		}
	}

	return remaining
}

func (idx *indexer) mergePartition(bucket string, streamId common.StreamId, sourceId common.IndexInstId, targetId common.IndexInstId,
	merged map[common.IndexInstId]common.IndexInst, respch chan error) bool {

	if source, ok := idx.indexInstMap[sourceId]; ok {

		logging.Infof("MergePartition: Merge instance %v to instance %v", source.InstId, targetId)

		// Only merge partition from the given bucket
		if source.Defn.Bucket != bucket {
			logging.Warnf("MergePartition: Source Index Instance %v is not in bucket %v.  Do not merge now.",
				source.InstId, bucket)
			return false
		}

		// Only merge partition from the given stream
		if source.Stream != streamId {
			logging.Warnf("MergePartition: Source Index Instance %v is not in stream %v.  Do not merge now.",
				source.InstId, streamId)
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
		if source.Stream != common.NIL_STREAM && idx.getStreamKeyspaceIdState(source.Stream, source.Defn.Bucket) != STREAM_ACTIVE {
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
				logging.Warnf("MergePartition: Source Index Instance stream %v and target index instance stream %v are not on the same. "+
					"Do not merge now.", target.Stream, source.Stream)
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
			if err := idx.sendMsgToClustMgrAndProcessResponse(msg); err != nil {
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
// cleanupIndexAfterMerge cleans up the index instance w/o removing the data.
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
	idx.stats.RemoveIndexStats(inst)

	// Update index maps with this index
	msgUpdateIndexInstMap := idx.newIndexInstMsg(idx.indexInstMap)
	msgUpdateIndexInstMap.AppendDeletedInstIds([]common.IndexInstId{inst.InstId})

	msgUpdateIndexPartnMap := &MsgUpdatePartnMap{indexPartnMap: idx.indexPartnMap}
	msgUpdateIndexPartnMap.AppendDeletedInstIds([]common.IndexInstId{inst.InstId})
	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, msgUpdateIndexPartnMap); err != nil {
		common.CrashOnError(err)
	}
}

//
// Prune Partition.
//
func (idx *indexer) handlePrunePartition(msg Message) (resp Message) {

	instId := msg.(*MsgClustMgrPrunePartition).GetInstId()
	partitions := msg.(*MsgClustMgrPrunePartition).GetPartitions()
	respch := msg.(*MsgClustMgrPrunePartition).GetRespCh()

	if inst, ok := idx.indexInstMap[instId]; ok {

		spec := pruneSpec{
			instId:     instId,
			partitions: partitions,
		}

		idx.prunePartitionList = append(idx.prunePartitionList, spec)

		if ok, _ := idx.streamKeyspaceIdFlushInProgress[inst.Stream][inst.Defn.Bucket]; !ok {
			idx.prunePartitions(inst.Defn.Bucket, inst.Stream)
		}
		resp = &MsgSuccessDrop{
			streamId:   inst.Stream,
			keyspaceId: inst.Defn.Bucket,
		}
	} else {
		logging.Warnf("PrunePartition.  Index instance %v not found. Skip", instId)
		resp = &MsgError{}
	}

	idx.prunePartitionForIdleKeyspaceIds()

	respch <- &MsgSuccess{}

	return
}

//
// Prune partition is for updating indexer's state after a partition is
// removed from an index instance.    When indexer handles this request,
// the index inst metadata is already updated with the partitioned removed.
// Therefore, indexer must handle this request to ensure the indexer's
// state in sync with metadata.
//
// Prune partiton can only be performed when:
// 1) After a snapshot
// 2) there is no flush (idle bucket)
// 3) There is no recovery for bucket stream
// 4) Indexer is active
//
// Prune partition updates the indexer's state in 4 phases:
// 1) update indexer internal data structure
// 2) remove partitions from index snapshot in storage manager
// 3) remove partition's data file
// 4) update bucket stream to remove partitions from index
//
// For step (4), stream update is queued and done in batches.
// If the corresponding stream is closed, all queued stream
// update will be removed.   All queued stream will also be
// cleared when the stream restarted.
//
// If recovery starts,
// 1) bucket stream will be closed during prepare phase.   Any queued
//    stream update will be dropped
// 2) For any in-flight stream update that has already started, it can succeed
//    or fail.  If fail, stream update will abort due to recovery.
//    Recovery can only start after all in-flight are done (due to stream lock).
// 3) When bucket stream re-starts for recovery, the bucket stream
//    will use the latest state of the index inst. So pruned partitions
//    will not be included in the new bucket stream.
// 4) New prune partition will be deferred until recovery is done.   So
//    no new stream update will be queued while there is recovery.
// 5) After recvovery is done, prune partition will be processed as normal.
//
// For deferred index, partitions can be pruned during recovery.   There is
// no stream update for deferred index.
//
func (idx *indexer) prunePartitions(keyspaceId string, streamId common.StreamId) {

	// nothing to prune
	if len(idx.prunePartitionList) == 0 {
		return
	}

	// Do not prune when indexer is not active
	if is := idx.getIndexerState(); is != common.INDEXER_ACTIVE {
		logging.Warnf("PrunePartition.  Indexer inactive.  Cannot prune instance.")
		return
	}

	// Do not prune during recovery
	if streamId != common.NIL_STREAM && idx.getStreamKeyspaceIdState(streamId, keyspaceId) != STREAM_ACTIVE {
		logging.Debugf("PrunePartition.  Indexer in recovery.  Cannot prune instance.")
		return
	}

	logging.Infof("PrunePartitions: keyspaceId %v stream %v", keyspaceId, streamId)

	var remaining []pruneSpec
	if len(idx.prunePartitionList) != 0 {
		remaining = make([]pruneSpec, 0, len(idx.prunePartitionList))
	}

	for _, spec := range idx.prunePartitionList {

		instId := spec.instId
		partitions := spec.partitions

		if !idx.prunePartition(keyspaceId, streamId, instId, partitions, idx.pruned) {
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
				if streamId != common.NIL_STREAM && idx.getStreamKeyspaceIdState(streamId, bucket) == STREAM_ACTIVE {
					idx.sendStreamUpdateForIndex(list3, bucket, bucketUUID, streamId)
				}
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
func (idx *indexer) prunePartition(bucket string, streamId common.StreamId, instId common.IndexInstId, partitions []common.PartitionId,
	prunedInst map[common.IndexInstId]common.IndexInst) bool {

	if inst, ok := idx.indexInstMap[instId]; ok {

		logging.Infof("PrunePartition: Prune instance %v partitions %v", instId, partitions)

		// Only prune partition from the given bucket
		if inst.Defn.Bucket != bucket {
			logging.Warnf("PurnePartition: Index Instance %v is not in bucket %v.  Do not prune now.",
				inst.InstId, bucket)
			return false
		}

		// Only prune partition from the given streamId
		if inst.Stream != streamId {
			logging.Warnf("PurnePartition: Index Instance %v is not in stream %v.  Do not prune now.",
				inst.InstId, streamId)
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
		if inst.Stream != common.NIL_STREAM && idx.getStreamKeyspaceIdState(inst.Stream, inst.Defn.Bucket) != STREAM_ACTIVE {
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
			msgUpdateIndexInstMap.AppendUpdatedInsts(common.IndexInstList{inst})
			msgUpdateIndexPartnMap := &MsgUpdatePartnMap{indexPartnMap: idx.indexPartnMap}
			msgUpdateIndexPartnMap.SetUpdatedPartnMap(idx.indexPartnMap[inst.InstId])

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

			// If it is not a deferred index (NIL_STREAM), then remove the old partitions from projector.
			// For a single invocation of prunePartitions(), an inst can be pruned multiple times.
			// This map will store the last copy of the inst after all the pruning.  The indexer
			// will then send the final copy to the projector.    Note that the prunePartitions()
			// can be invoked many times, and each invocation can be pruning the same instance.
			// In this case, indexer will make multiple calls to the projector.
			if inst.Stream != common.NIL_STREAM {
				prunedInst[inst.InstId] = inst
			}
		}

	} else {
		logging.Warnf("PrunePartition.  Index instance %v not found. Skip", instId)
	}

	return true
}

// When a inst needs to be pruned, one of the following can happens:
// 1) index is pruned
// 2) prune is postponed because flush is in progress.
// 3) prune is postponed because of other reasons (indxer pause, recovery).
//
// For those prune that is postponed, indexer needs to retry when the bucket flush is idle.
//
func (idx *indexer) prunePartitionForIdleKeyspaceIds() {

	if len(idx.prunePartitionList) > 0 {
		buckets := make(map[string]map[common.StreamId]bool)
		for _, spec := range idx.prunePartitionList {
			if inst, ok := idx.indexInstMap[spec.instId]; ok {
				if _, ok := buckets[inst.Defn.Bucket]; !ok {
					buckets[inst.Defn.Bucket] = make(map[common.StreamId]bool)
				}
				buckets[inst.Defn.Bucket][inst.Stream] = true
			}
		}

		for bucket, streams := range buckets {
			for streamId, _ := range streams {
				if !idx.streamKeyspaceIdFlushInProgress[streamId][bucket] {
					idx.prunePartitions(bucket, streamId)
				}
			}
		}
	}
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

// handleBuildIndex performs index builds for potentially multiple indexes in one call. These can be due to normal
// create index or rebalance. It constructs an error map (errMap) from instId to error and sends it back to the caller.
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
		if reqCtx.ReqSource == common.DDLRequestSourceUser {
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

	keyspaceIdIndexList := idx.groupIndexListByKeyspaceId(instIdList)
	errMap := make(map[common.IndexInstId]error) // build errors by instId

	for keyspaceId, instIdList := range keyspaceIdIndexList {
		instIdList, ok := idx.checkValidIndexInst(keyspaceId, instIdList, clientCh, errMap)
		if !ok {
			logging.Errorf("Indexer::handleBuildIndex Invalid Index List "+
				"KeyspaceId %v. Index in error %v.", keyspaceId, errMap)
			if idx.enableManager {
				if len(instIdList) == 0 {
					delete(keyspaceIdIndexList, keyspaceId)
					continue
				}
			} else {
				return
			}
		}

		// Check if Initial Build is already running for this index's keyspace. Indexer does not support multiple
		// builds on the same keyspace because the keyspaceId is used as a key to stream maps.
		if ok := idx.checkDuplicateInitialBuildRequest(keyspaceId, instIdList, clientCh, errMap); !ok {
			logging.Errorf("Indexer::handleBuildIndex Build Already In"+
				" Progress. KeyspaceId %v. Index in error %v", keyspaceId, errMap)
			if idx.enableManager {
				delete(keyspaceIdIndexList, keyspaceId)
				continue
			} else {
				return
			}
		}

		// Limit the number of concurrent build streams.
		if ok := idx.checkParallelCollectionBuilds(keyspaceId, instIdList, clientCh, errMap); !ok {
			maxParallelCollectionBuilds := idx.config["max_parallel_collection_builds"].Int()
			logging.Errorf("Indexer::handleBuildIndex Build is already in progress for %v collections."+
				" KeyspaceID: %v. Instances in error: %v", maxParallelCollectionBuilds, instIdList, keyspaceId)
			if idx.enableManager {
				delete(keyspaceIdIndexList, keyspaceId)
				continue
			} else {
				return
			}
		}

		inst := idx.indexInstMap[instIdList[0]]
		collectionId := inst.Defn.CollectionId

		cluster := idx.config["clusterAddr"].String()
		numVbuckets := idx.config["numVbuckets"].Int()

		//all indexes get built using INIT_STREAM
		var buildStream common.StreamId = common.INIT_STREAM

		idx.cinfoProviderLock.RLock()
		clusterVer := idx.cinfoProvider.ClusterVersion()
		idx.cinfoProviderLock.RUnlock()

		reqcid := idx.makeCollectionIdForStreamRequest(buildStream, keyspaceId, collectionId, clusterVer)

		buildTs, err := GetCurrentKVTs(cluster, "default", keyspaceId, reqcid, numVbuckets)
		if err != nil {
			errStr := fmt.Sprintf("Error Connecting KV %v Err %v",
				idx.config["clusterAddr"].String(), err)
			logging.Errorf("Indexer::handleBuildIndex %v", errStr)
			if idx.enableManager {
				idx.bulkUpdateError(instIdList, errStr)
				for _, instId := range instIdList {
					errMap[instId] = &common.IndexerError{Reason: errStr, Code: common.TransientError}
				}
				delete(keyspaceIdIndexList, keyspaceId)
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
			keyspaceIdIndexList[keyspaceId] = instIdList
		} else {
			delete(keyspaceIdIndexList, keyspaceId)
			continue
		}

		idx.bulkUpdateStream(instIdList, buildStream)

		//always set state to Initial, once stream request/build is done,
		//this will get changed to active
		var buildState common.IndexState = common.INDEX_STATE_INITIAL

		idx.bulkUpdateState(instIdList, buildState)
		idx.bulkUpdateRState(instIdList, msg.(*MsgBuildIndex).GetRequestCtx())

		logging.Infof("Indexer::handleBuildIndex Added Index: %v to Stream: %v State: %v",
			instIdList, buildStream, buildState)

		msgUpdateIndexInstMap := idx.newIndexInstMsg(idx.indexInstMap)
		updatedIndexes := idx.getInsts(instIdList)
		msgUpdateIndexInstMap.AppendUpdatedInsts(updatedIndexes)

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
		idx.sendStreamUpdateForBuildIndex(instIdList, buildStream, keyspaceId,
			reqcid, clusterVer, buildTs, clientCh)

		idx.setStreamKeyspaceIdState(buildStream, keyspaceId, STREAM_ACTIVE)

		//store updated state and streamId in meta store
		if idx.enableManager {
			if err := idx.updateMetaInfoForIndexList(instIdList, true,
				true, false, true, true, false, false, false, nil); err != nil {
				common.CrashOnError(err)
			}
		} else {
			idx.keyspaceIdCreateClientChMap[keyspaceId] = clientCh
			return
		}
	}

	if idx.enableManager {
		clientCh <- &MsgBuildIndexResponse{errMap: errMap} // return instId-specific build errors to caller
	} else {
		clientCh <- &MsgSuccess{}
	}
}

func (idx *indexer) handleDropIndex(msg Message) (resp Message) {
	indexInstId := msg.(*MsgDropIndex).GetIndexInstId()
	clientCh := msg.(*MsgDropIndex).GetResponseChannel()
	logging.Infof("Indexer::handleDropIndex - IndexInstId %v", indexInstId)

	//actual error is not required for admin msg handler
	resp = &MsgError{}

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
		if reqCtx.ReqSource == common.DDLRequestSourceUser {
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

	idx.stats.RemoveIndexStats(indexInst)

	//if the index state is Created/Ready/Deleted, only data cleanup is
	//required. No stream updates are required.
	if indexInst.State == common.INDEX_STATE_CREATED ||
		indexInst.State == common.INDEX_STATE_READY ||
		indexInst.State == common.INDEX_STATE_DELETED {

		idx.cleanupIndexData([]common.IndexInst{indexInst}, clientCh)
		logging.Infof("Indexer::handleDropIndex Cleanup Successful for "+
			"Index Data %v", indexInst)
		resp = &MsgSuccess{}
		clientCh <- resp
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
	msgUpdateIndexInstMap.AppendUpdatedInsts(common.IndexInstList{indexInst})

	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, nil); err != nil {
		clientCh <- &MsgError{
			err: Error{code: ERROR_INDEXER_INTERNAL_ERROR,
				severity: FATAL,
				cause:    err,
				category: INDEXER}}
		common.CrashOnError(err)
	}

	//check if there is already a drop request waiting on this bucket
	if ok := idx.checkDuplicateDropRequest(indexInst, clientCh); ok {
		return
	}

	//if there is a flush in progress for this index's bucket and stream
	//wait for the flush to finish before drop
	streamId := indexInst.Stream
	keyspaceId := indexInst.Defn.KeyspaceId(streamId)

	if ok, _ := idx.streamKeyspaceIdFlushInProgress[streamId][keyspaceId]; ok {
		notifyCh := make(MsgChannel)
		idx.streamKeyspaceIdObserveFlushDone[streamId][keyspaceId] = notifyCh
		go idx.processDropAfterFlushDone(indexInst, notifyCh, clientCh)
	} else {
		idx.cleanupIndex(indexInst, clientCh)
	}

	resp = &MsgSuccessDrop{
		streamId:   streamId,
		keyspaceId: keyspaceId,
	}
	return
}

func (idx *indexer) handlePrepareRecovery(msg Message) {

	streamId := msg.(*MsgRecovery).GetStreamId()
	keyspaceId := msg.(*MsgRecovery).GetKeyspaceId()
	sessionId := msg.(*MsgRecovery).GetSessionId()

	if ok, currSid := idx.validateSessionId(streamId, keyspaceId, sessionId, true); !ok {
		logging.Infof("Indexer::handlePrepareRecovery StreamId %v KeyspaceId %v SessionId %v. "+
			"Skipped. Current SessionId %v.", streamId, keyspaceId, sessionId, currSid)
		return
	}

	logging.Infof("Indexer::handlePrepareRecovery StreamId %v KeyspaceId %v",
		streamId, keyspaceId)

	idx.stopKeyspaceIdStream(streamId, keyspaceId, false)

}

func (idx *indexer) handleInitPrepRecovery(msg Message) {

	keyspaceId := msg.(*MsgRecovery).GetKeyspaceId()
	streamId := msg.(*MsgRecovery).GetStreamId()
	rollbackTs := msg.(*MsgRecovery).GetRestartTs()
	retryTs := msg.(*MsgRecovery).GetRetryTs()
	requestCh := msg.(*MsgRecovery).GetRequestCh()
	sessionId := msg.(*MsgRecovery).GetSessionId()

	if ok, currSid := idx.validateSessionId(streamId, keyspaceId, sessionId, true); !ok {
		logging.Infof("Indexer::handleInitPrepRecovery StreamId %v KeyspaceId %v SessionId %v. "+
			"Skipped. Current SessionId %v.", streamId, keyspaceId, sessionId, currSid)
		return
	}

	//if the stream is inactive(e.g. all indexes get dropped)
	if idx.getStreamKeyspaceIdState(streamId, keyspaceId) == STREAM_INACTIVE {
		logging.Infof("Indexer::handleInitPrepRecovery StreamId %v KeyspaceId %v "+
			"State %v. Skipping INIT_PREPARE and Cleaning up.",
			streamId, keyspaceId, idx.getStreamKeyspaceIdState(streamId, keyspaceId))
		idx.cleanupStreamKeyspaceIdState(streamId, keyspaceId)
	} else {

		allowRequest := true

		// If this is a new recovery request
		if requestCh == nil {
			// Do not allow recovery if there is an MTR or recovery going on.
			if idx.streamKeyspaceIdCurrRequest[streamId][keyspaceId] != nil ||
				idx.getStreamKeyspaceIdState(streamId, keyspaceId) != STREAM_ACTIVE {
				allowRequest = false
			}
		} else {
			// If this is a recursive request, make sure it has the same requestStopCh.
			req := idx.streamKeyspaceIdCurrRequest[streamId][keyspaceId]
			if req.reqCh != requestCh {
				allowRequest = false
			}
		}

		if !allowRequest {
			logging.Infof("Indexer::handleInitPrepRecovery StreamId %v KeyspaceId %v "+
				"SessionId %v. Cannot initiate another recovery while previous "+
				"recovery in progress.", streamId, keyspaceId, sessionId)
			return
		}

		if rollbackTs != nil {
			logging.Infof("Indexer::handleInitPrepRecovery StreamId %v KeyspaceId %v SessionId %v "+
				"RollbackTs %v", streamId, keyspaceId, sessionId, rollbackTs)
			idx.setRollbackTs(streamId, keyspaceId, rollbackTs)
		}

		if retryTs != nil {
			idx.setRetryTsForRecovery(streamId, keyspaceId, retryTs)
		}

		//if there is any pending build done for this stream/keyspace,
		//that needs to be cleared. As part of recovery stream will be restarted
		//and build done will be evaluated again.
		if _, ok := idx.streamKeyspaceIdPendBuildDone[streamId][keyspaceId]; ok {
			logging.Infof("Indexer::handleInitPrepRecovery StreamId %v KeyspaceId %v SessionId %v. "+
				"Clear pendBuildDone due to recovery.", streamId, keyspaceId, sessionId)
			delete(idx.streamKeyspaceIdPendBuildDone[streamId], keyspaceId)
		}

		idx.setStreamKeyspaceIdState(streamId, keyspaceId, STREAM_PREPARE_RECOVERY)

		logging.Infof("Indexer::handleInitPrepRecovery StreamId %v KeyspaceId %v State %v "+
			"SessionId %v", streamId, keyspaceId, STREAM_PREPARE_RECOVERY, sessionId)

		//fwd the msg to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

	}
}

func (idx *indexer) handleResetStream(msg Message) {

	keyspaceId := msg.(*MsgStreamUpdate).GetKeyspaceId()
	streamId := msg.(*MsgStreamUpdate).GetStreamId()
	sessionId := msg.(*MsgStreamUpdate).GetSessionId()
	ignoreOSOException := msg.(*MsgStreamUpdate).IgnoreOSOException()
	reqCh := msg.(*MsgStreamUpdate).GetStopChannel()

	exception := idx.streamKeyspaceIdOSOException[streamId][keyspaceId]

	logging.Infof("Indexer::handleResetStream %v %v %v %v %v", streamId,
		keyspaceId, sessionId, ignoreOSOException, exception)

	if ok, currSid := idx.validateSessionId(streamId, keyspaceId, sessionId, true); !ok {
		logging.Infof("Indexer::handleResetStream StreamId %v KeyspaceId %v SessionId %v. "+
			"Skipped. Current SessionId %v.", streamId, keyspaceId, sessionId, currSid)
		return
	}

	//if the stream is inactive(e.g. all indexes get dropped)
	if idx.getStreamKeyspaceIdState(streamId, keyspaceId) == STREAM_INACTIVE {
		logging.Infof("Indexer::handleResetStream StreamId %v KeyspaceId %v "+
			"State %v. Skipping Reset Stream and Cleaning up.",
			streamId, keyspaceId, idx.getStreamKeyspaceIdState(streamId, keyspaceId))
		idx.cleanupStreamKeyspaceIdState(streamId, keyspaceId)
	} else {

		//if OSO Exception has already been recorded and the flag to ignore exception
		//has not been set, ignore the message
		if idx.streamKeyspaceIdOSOException[streamId][keyspaceId] && !ignoreOSOException {
			logging.Infof("Indexer::handleResetStream StreamId %v KeyspaceId %v "+
				"OSOException Already Seen. Skipping Reset Stream.",
				streamId, keyspaceId)
		} else {

			//check if a recovery is already in progress. This case should not happen.
			//this is just for safety and debug information.
			if idx.getStreamKeyspaceIdState(streamId, keyspaceId) != STREAM_ACTIVE {
				logging.Warnf("Indexer::handleResetStream StreamId %v KeyspaceId %v "+
					"ResetStream received during recovery.", streamId, keyspaceId)
			}

			idx.streamKeyspaceIdOSOException[streamId][keyspaceId] = true
			idx.initRecoveryForOSO(streamId, keyspaceId, sessionId, reqCh)
		}
	}
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

	keyspaceId := msg.(*MsgRecovery).GetKeyspaceId()
	streamId := msg.(*MsgRecovery).GetStreamId()
	sessionId := msg.(*MsgRecovery).GetSessionId()
	reqCh := msg.(*MsgRecovery).GetRequestCh()

	if ok, currSid := idx.validateSessionId(streamId, keyspaceId, sessionId, true); !ok {
		logging.Infof("Indexer::handlePrepareDone StreamId %v KeyspaceId %v SessionId %v. "+
			"Skipped. Current SessionId %v.", streamId, keyspaceId, sessionId, currSid)
		return
	}

	logging.Infof("Indexer::handlePrepareDone StreamId %v KeyspaceId %v",
		streamId, keyspaceId)

	//if the stream is inactive(e.g. all indexes get dropped)
	if idx.getStreamKeyspaceIdState(streamId, keyspaceId) == STREAM_INACTIVE {
		logging.Infof("Indexer::handlePrepareDone Skip PREPARE_DONE for Inactive "+
			"KeyspaceId. StreamId %v KeyspaceId %v.", streamId, keyspaceId)
		idx.cleanupStreamKeyspaceIdState(streamId, keyspaceId)
		return
	} else {
		idx.deleteStreamKeyspaceIdCurrRequest(streamId, keyspaceId, msg, reqCh, sessionId)
	}

	//fwd the msg to timekeeper
	idx.tkCmdCh <- msg
	<-idx.tkCmdCh

}

func (idx *indexer) handleInitRecovery(msg Message) {

	streamId := msg.(*MsgRecovery).GetStreamId()
	keyspaceId := msg.(*MsgRecovery).GetKeyspaceId()
	restartTs := msg.(*MsgRecovery).GetRestartTs()
	retryTs := msg.(*MsgRecovery).GetRetryTs()
	sessionId := msg.(*MsgRecovery).GetSessionId()

	if ok, currSid := idx.validateSessionId(streamId, keyspaceId, sessionId, true); !ok {
		logging.Infof("Indexer::handleInitRecovery StreamId %v KeyspaceId %v SessionId %v. "+
			"Skipped. Current SessionId %v.", streamId, keyspaceId, sessionId, currSid)
		return
	}

	//if a recovery is in progress and all indexes get dropped, recovery needs to be
	//aborted in timekeeper
	if idx.getStreamKeyspaceIdState(streamId, keyspaceId) == STREAM_INACTIVE {
		logging.Infof("Indexer::handleInitRecovery Aborting Recovery for "+
			"Stream: %v KeyspaceId: %v. KeyspaceId Inactive", streamId, keyspaceId)

		idx.tkCmdCh <- &MsgRecovery{mType: INDEXER_ABORT_RECOVERY,
			streamId:   streamId,
			keyspaceId: keyspaceId}
		<-idx.tkCmdCh
		idx.cleanupStreamKeyspaceIdState(streamId, keyspaceId)
		return
	}

	idx.setStreamKeyspaceIdState(streamId, keyspaceId, STREAM_RECOVERY)

	logging.Infof("Indexer::handleInitRecovery StreamId %v KeyspaceId %v %v",
		streamId, keyspaceId, STREAM_RECOVERY)

	sessionId = idx.genNextSessionId(streamId, keyspaceId)

	//if there is a rollbackTs, process rollback
	if ts, ok := idx.streamKeyspaceIdRollbackTs[streamId][keyspaceId]; ok && ts != nil {
		//if there is a retryTs, use that first
		if rts, ok := idx.streamKeyspaceIdRetryTs[streamId][keyspaceId]; ok && rts != nil {
			logging.Infof("Indexer::handleInitRecovery StreamId %v KeyspaceId %v Using RetryTs %v",
				streamId, keyspaceId, rts)
			idx.streamKeyspaceIdRetryTs[streamId][keyspaceId] = nil
			idx.startKeyspaceIdStream(streamId, keyspaceId, rts, nil, nil, false, false, sessionId)
		} else {
			idx.processRollback(streamId, keyspaceId, ts, sessionId)
		}
	} else {
		idx.startKeyspaceIdStream(streamId, keyspaceId, restartTs, retryTs, nil, false, false, sessionId)
	}

}

func (idx *indexer) handleStorageRollbackDone(msg Message) {

	keyspaceId := msg.(*MsgRollbackDone).GetKeyspaceId()
	streamId := msg.(*MsgRollbackDone).GetStreamId()
	restartTs := msg.(*MsgRollbackDone).GetRestartTs()
	err := msg.(*MsgRollbackDone).GetError()
	sessionId := msg.(*MsgRollbackDone).GetSessionId()

	if ok, currSid := idx.validateSessionId(streamId, keyspaceId, sessionId, false); !ok {
		logging.Infof("Indexer::handleStoragRollbackDone StreamId %v KeyspaceId %v SessionId %v. "+
			"Skipped. Current SessionId %v.", streamId, keyspaceId, sessionId, currSid)
		return
	}

	//if a recovery is in progress and all indexes get dropped, recovery needs to be
	//aborted in timekeeper
	if idx.getStreamKeyspaceIdState(streamId, keyspaceId) == STREAM_INACTIVE {
		logging.Infof("Indexer::handleStorageRollbackDone Aborting Recovery for "+
			"Stream: %v KeyspaceId: %v. KeyspaceId Inactive", streamId, keyspaceId)

		idx.tkCmdCh <- &MsgRecovery{mType: INDEXER_ABORT_RECOVERY,
			streamId:   streamId,
			keyspaceId: keyspaceId}
		<-idx.tkCmdCh
		//any pending build done needs to be processed
		idx.processPendingBuildDone(streamId, keyspaceId, sessionId)
		return
	}

	//notify storage rollback done
	if streamId == common.MAINT_STREAM {
		idx.scanCoordCmdCh <- &MsgRollback{
			streamId:     streamId,
			keyspaceId:   keyspaceId,
			rollbackTime: 0,
		}
		<-idx.scanCoordCmdCh
	}

	if err != nil {
		logging.Fatalf("Indexer::handleStorageRollbackDone Error during Rollback %v", err)
		common.CrashOnError(err)
	}

	//NOTE - Reset Index functionality allows to support collection flush easily.
	//As collection flush is not supported in 7.0, rollback to 0 means
	//to clear out the data of all indexes in MAINT_STREAM and rebuild.
	//For 7.0, it is okay to follow the pre-7.0 model of letting the
	//indexes use the MAINT_STREAM to rebuild from 0. This keeps index
	//stream management simple. The only other behavior change would be
	//that indexes remain in Active state after rollback to 0(dcp rollback
	//or bucket flush) which is same as pre 7.0 behavior.

	//if index in MAINT_STREAM rollback to 0, reset state to created and
	//schedule the build again
	resetOnRollback := idx.config["recovery.reset_index_on_rollback"].Bool()
	if resetOnRollback && (restartTs == nil && streamId == common.MAINT_STREAM) {
		idx.resetIndexesOnRollback(streamId, keyspaceId, sessionId)
		return
	}

	idx.startKeyspaceIdStream(streamId, keyspaceId, restartTs, nil, nil, false, false, sessionId)
	go idx.collectProgressStats(true)

}

func (idx *indexer) resetIndexesOnRollback(streamId common.StreamId,
	keyspaceId string, sessionId uint64) {

	logging.Infof("Indexer::resetIndexesOnRollback %v %v %v", streamId, keyspaceId, sessionId)

	if streamId != common.MAINT_STREAM {
		logging.Warnf("Indexer::resetIndexesOnRollback Invalid call %v", streamId)
		return
	}

	//get the list of instances to be reset(all indexes in the keyspace in MAINT_STREAM)
	var rList resetList
	for instId, index := range idx.indexInstMap {

		if index.Stream == streamId &&
			index.Defn.KeyspaceId(index.Stream) == keyspaceId &&
			index.State != common.INDEX_STATE_DELETED {
			logging.Infof("Indexer::resetIndexesOnRollback %v %v Adding %v to reset list",
				streamId, keyspaceId, instId)
			rList = append(rList, instId)
		}
	}

	//for any partitioned index, if there is any instance
	//in INIT_STREAM, it needs to be reset as well
	var pList resetList
	for _, instId := range rList {
		index := idx.indexInstMap[instId]
		if common.IsPartitioned(index.Defn.PartitionScheme) {
			for pinstId, pindex := range idx.indexInstMap {
				if pindex.RealInstId == index.InstId &&
					pindex.Stream == common.INIT_STREAM {
					logging.Infof("Indexer::resetIndexesOnRollback %v %v Adding %v to "+
						"reset list. Found partitioned index in state %v.",
						pindex.Stream, keyspaceId, pinstId, pindex.State)
					rList = append(rList, pinstId)
					pList = append(pList, pinstId)
				}
			}
		}
	}

	if len(pList) != 0 {
		//if a flush is in progress
		if ok, _ := idx.streamKeyspaceIdFlushInProgress[common.INIT_STREAM][keyspaceId]; ok {
			notifyCh := make(MsgChannel)
			idx.keyspaceIdObserveFlushDoneForReset[keyspaceId] = notifyCh
			idx.keyspaceIdResetList[keyspaceId] = rList
			go idx.processResetAfterFlushDone(keyspaceId, sessionId, notifyCh)
			return
		}
	}

	if rList != nil {
		idx.doResetIndexesOnRollback(keyspaceId, sessionId, rList)
	} else {

		//if no indexes need to be reset, set the stream to INACTIVE
		//stream is already stopped as reset happens during rollback
		logging.Infof("Indexer::resetIndexesOnRollback %v %v. No Index Found For Reset. Setting state %v",
			streamId, keyspaceId, STREAM_INACTIVE)
		idx.setStreamKeyspaceIdState(streamId, keyspaceId, STREAM_INACTIVE)
		idx.cleanupAllStreamKeyspaceIdState(streamId, keyspaceId)

		//process any pending build done.
		idx.processPendingBuildDone(streamId, keyspaceId, sessionId)

	}

}

func (idx *indexer) processResetAfterFlushDone(keyspaceId string, sessionId uint64, notifyCh MsgChannel) {

	logging.Infof("Indexer::processResetAfterFlushDone %v %v %v", common.MAINT_STREAM,
		keyspaceId, sessionId)

	select {
	case <-notifyCh:
		idx.doResetIndexesOnRollback(keyspaceId, sessionId, idx.keyspaceIdResetList[keyspaceId])
		idx.keyspaceIdResetList[keyspaceId] = nil
	}

	idx.keyspaceIdObserveFlushDoneForReset[keyspaceId] = nil

	//indicate done
	close(notifyCh)
}

func (idx *indexer) doResetIndexesOnRollback(keyspaceId string,
	sessionId uint64, resetList resetList) {

	logging.Infof("Indexer:doResetIndexesOnRollback %v %v %v ResetList %v",
		common.MAINT_STREAM, keyspaceId, sessionId, resetList)

	if len(resetList) == 0 {
		logging.Infof("Indexer::doResetIndexesOnRollback %v %v %v Empty Reset List.",
			common.MAINT_STREAM, keyspaceId, sessionId)
		return
	}

	var wg sync.WaitGroup
	updatedInstances := make([]common.IndexInst, 0)
	for _, instId := range resetList {

		if index, ok := idx.indexInstMap[instId]; ok {

			if index.State != common.INDEX_STATE_DELETED {
				idx.resetSingleIndexOnRollback(&index, &wg)
				idx.indexInstMap[instId] = index
				updatedInstances = append(updatedInstances, index)
			} else {
				logging.Infof("Indexer::doResetIndexesOnRollback Index %v in %v state. Skipping.", instId, index.State)
			}
		} else {
			logging.Infof("Indexer::doResetIndexesOnRollback Index %v not found. Possibly deleted. Skipping.", instId)
		}
	}

	//send updated maps to all workers
	msgUpdateIndexInstMap := idx.newIndexInstMsg(idx.indexInstMap)
	msgUpdateIndexInstMap.AppendUpdatedInsts(updatedInstances)

	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, nil); err != nil {
		common.CrashOnError(err)
	}

	idx.addPendingReset(updatedInstances)

	go idx.waitForIndexReset(keyspaceId, sessionId, &wg)
}

func (idx *indexer) resetSingleIndexOnRollback(inst *common.IndexInst,
	wg *sync.WaitGroup) {

	logging.Infof("Indexer::resetSingleIndexOnRollback Reset index %v %v %v %v %v",
		inst.Defn.Bucket, inst.Defn.Scope, inst.Defn.Collection, inst.Defn.Name, inst.InstId)

	// update metadata
	respch := make(chan error)
	msg := &MsgClustMgrResetIndexOnRollback{
		inst:   *inst,
		respch: respch,
	}

	if err := idx.sendMsgToClustMgrAndProcessResponse(msg); err != nil {
		common.CrashOnError(err)
	}

	// update index instance
	inst.State = common.INDEX_STATE_CREATED
	inst.Stream = common.NIL_STREAM
	inst.Error = ""

	if wg == nil {
		// The metadata commit is done asynchronously to avoid deadlock.
		// If metadata update fails, indexer will restart so it can restore
		//to a consistent state.
		//TODO avoid a crash/restart loop
		err := <-respch
		if err != nil {
			common.CrashOnError(err)
		}
		return
	} else {

		wg.Add(1)
		go func() {

			defer wg.Done()
			// The metadata commit is done asynchronously to avoid deadlock.
			// If metadata update fails, indexer will restart so it can restore
			//to a consistent state.
			//TODO avoid a crash/restart loop
			err := <-respch
			if err != nil {
				common.CrashOnError(err)
			}

			logging.Infof("Indexer::resetSingleIndexOnRollback Reset done %v %v %v %v",
				inst.Defn.Bucket, inst.Defn.Scope, inst.Defn.Collection, inst.Defn.Name)

		}()
	}
}

func (idx *indexer) waitForIndexReset(keyspaceId string, sessionId uint64, wg *sync.WaitGroup) {

	//wait for all the metadata to be updated
	wg.Wait()

	idx.internalRecvCh <- &MsgResetIndexDone{
		streamId:   common.MAINT_STREAM,
		keyspaceId: keyspaceId,
		sessionId:  sessionId,
	}
}

func (idx *indexer) handleResetIndexDone(msg Message) {

	streamId := msg.(*MsgResetIndexDone).GetStreamId()
	keyspaceId := msg.(*MsgResetIndexDone).GetKeyspaceId()
	sessionId := msg.(*MsgResetIndexDone).GetSessionId()

	if ok, currSid := idx.validateSessionId(streamId, keyspaceId, sessionId, true); !ok {
		logging.Warnf("Indexer::handleResetIndexDone StreamId %v KeyspaceId %v SessionId %v. "+
			"Current SessionId %v.", streamId, keyspaceId, sessionId, currSid)
	}

	logging.Infof("Indexer::handleResetIndexDone %v %v %v",
		streamId, keyspaceId, sessionId)

	//sanity check, at this point there should be no active index in the stream
	if idx.checkKeyspaceIdExistsInStream(keyspaceId, streamId, false) {
		logging.Fatalf("Indexer::handleResetIndexDone %v %v Unexpected active index(es).",
			streamId, keyspaceId)
		common.CrashOnError(ErrInconsistentState)
	}

	//stream is already stopped as reset happens during rollback
	idx.setStreamKeyspaceIdState(streamId, keyspaceId, STREAM_INACTIVE)
	idx.cleanupAllStreamKeyspaceIdState(streamId, keyspaceId)

	//TODO check if it is possible for deleted state indexes to
	//be in the stream, if so handle the case

	//process any pending build done.
	idx.processPendingBuildDone(streamId, keyspaceId, sessionId)

}

func (idx *indexer) handleRecoveryDone(msg Message) {

	keyspaceId := msg.(*MsgRecovery).GetKeyspaceId()
	streamId := msg.(*MsgRecovery).GetStreamId()
	sessionId := msg.(*MsgRecovery).GetSessionId()
	reqCh := msg.(*MsgRecovery).GetRequestCh()
	mergeTs := msg.(*MsgRecovery).GetRestartTs()

	if ok, currSid := idx.validateSessionId(streamId, keyspaceId, sessionId, true); !ok {
		logging.Infof("Indexer::handleRecoveryDone StreamId %v KeyspaceId %v SessionId %v. "+
			"Skipped. Current SessionId %v.", streamId, keyspaceId, sessionId, currSid)
		return
	}

	logging.Infof("Indexer::handleRecoveryDone StreamId %v KeyspaceId %v SessionId %v",
		streamId, keyspaceId, sessionId)

	idx.deleteStreamKeyspaceIdCurrRequest(streamId, keyspaceId, msg, reqCh, sessionId)
	idx.cleanupStreamKeyspaceIdRecoveryState(streamId, keyspaceId)

	//during recovery, if all indexes of a keyspace gets dropped,
	//further processing is not required. Stream cleanup is done by
	//whoever is changing stream status to INACTIVE
	if idx.getStreamKeyspaceIdState(streamId, keyspaceId) == STREAM_INACTIVE {
		logging.Infof("Indexer::handleRecoveryDone Skip Recovery for Stream %v. "+
			"Inactive KeyspaceId %v", streamId, keyspaceId)
		idx.processPendingBuildDone(streamId, keyspaceId, sessionId)
		return
	}

	//send the msg to timekeeper
	idx.tkCmdCh <- msg
	<-idx.tkCmdCh

	//during recovery, if all indexes of a keyspace gets dropped,
	//the stream needs to be stopped for that keyspace.
	if !idx.checkKeyspaceIdExistsInStream(keyspaceId, streamId, true) {

		//for MAINT_STREAM, keep the stream running if catchup state index
		//exist for INIT_STREAM
		catchupPending := false
		if streamId == common.MAINT_STREAM {
			catchupPending = idx.checkCatchupPendingForStream(streamId, keyspaceId)
		}

		if !catchupPending {
			if idx.getStreamKeyspaceIdState(streamId, keyspaceId) != STREAM_INACTIVE {
				logging.Infof("Indexer::handleRecoveryDone StreamId %v KeyspaceId %v "+
					"State %v. No Index Found. Cleaning up.", streamId, keyspaceId,
					idx.getStreamKeyspaceIdState(streamId, keyspaceId))
				idx.stopKeyspaceIdStream(streamId, keyspaceId, true)

				idx.setStreamKeyspaceIdState(streamId, keyspaceId, STREAM_INACTIVE)
				idx.cleanupAllStreamKeyspaceIdState(streamId, keyspaceId)
				idx.processPendingBuildDone(streamId, keyspaceId, sessionId)
			}
		} else {
			//store the mergeTs for use in recovery of INIT_STREAM
			idx.setMinMergeTsForCatchup(streamId, keyspaceId, mergeTs)
		}
	} else {
		//change status to Active
		idx.setStreamKeyspaceIdState(streamId, keyspaceId, STREAM_ACTIVE)

		if streamId == common.MAINT_STREAM {
			//for MAINT_STREAM, check if there is any pendingBuildDone for an
			//INIT_STREAM of the same bucket
			idx.processPendingBuildDone(streamId, keyspaceId, sessionId)

			//store the mergeTs for use in recovery of INIT_STREAM
			idx.setMinMergeTsForCatchup(streamId, keyspaceId, mergeTs)
		} else {
			//for INIT_STREAM, if MAINT_STREAM is not running
			//i. all indexes dropped
			//ii. all indexes get reset due to rollback to 0
			//it needs to be started here

			maintStreamId := common.MAINT_STREAM
			bucket := GetBucketFromKeyspaceId(keyspaceId)
			state := idx.getStreamKeyspaceIdState(maintStreamId, bucket)
			catchupPending := idx.checkCatchupPendingForStream(maintStreamId, bucket)
			if state == STREAM_INACTIVE && catchupPending {
				if idx.streamKeyspaceIdPendStart[maintStreamId][bucket] {
					logging.Infof("Indexer::handleRecoveryDone Skip Start Inactive Stream %v %v %v"+
						" due to pending start.", maintStreamId, bucket, state)
				} else {
					logging.Infof("Indexer::handleRecoveryDone Start Inactive Stream %v %v %v",
						maintStreamId, bucket, state)

					restartTs := msg.(*MsgRecovery).GetRestartTs()
					idx.restartMaintStreamForCatchup(bucket, restartTs)
				}
			}

			//If any OSO exception has been recorded, initiate recovery when the stream request
			//finishes. Any earlier recovery attempts would have been skipped as this stream
			//request was in progress. If a recovery is already in progress, this call to
			//initiate recovery will get skipped.
			if idx.streamKeyspaceIdOSOException[streamId][keyspaceId] {
				idx.initRecoveryForOSO(streamId, keyspaceId, sessionId, nil)
			}
		}
	}

	logging.Infof("Indexer::handleRecoveryDone StreamId %v KeyspaceId %v SessionId %v %v",
		streamId, keyspaceId, sessionId, idx.getStreamKeyspaceIdState(streamId, keyspaceId))

}

func (idx *indexer) handleKVStreamRepair(msg Message) {

	keyspaceId := msg.(*MsgKVStreamRepair).GetKeyspaceId()
	streamId := msg.(*MsgKVStreamRepair).GetStreamId()
	restartTs := msg.(*MsgKVStreamRepair).GetRestartTs()
	async := msg.(*MsgKVStreamRepair).GetAsync()
	sessionId := msg.(*MsgKVStreamRepair).GetSessionId()

	if ok, currSid := idx.validateSessionId(streamId, keyspaceId, sessionId, false); !ok {
		logging.Infof("Indexer::handleKVStreamRepair StreamId %v KeyspaceId %v SessionId %v. "+
			"Skipped. Current SessionId %v.", streamId, keyspaceId, sessionId, currSid)
		return
	}

	is := idx.getIndexerState()
	if is == common.INDEXER_PREPARE_UNPAUSE {
		logging.Warnf("Indexer::handleKVStreamRepair Skipped Repair "+
			"In %v state", is)
		return
	}

	//repair is not required for inactive/prepare recovery keyspace streams
	state := idx.getStreamKeyspaceIdState(streamId, keyspaceId)
	if state == STREAM_INACTIVE ||
		state == STREAM_PREPARE_RECOVERY {
		logging.Infof("Indexer::handleKVStreamRepair Skip KVStreamRepair %v %v %v",
			streamId, keyspaceId, state)
		return
	}

	//if there is already a repair in progress for this keyspace stream
	//ignore the request
	if idx.checkStreamRequestPending(streamId, keyspaceId) == false {
		logging.Infof("Indexer::handleKVStreamRepair Initiate Stream Repair %v KeyspaceId %v "+
			"StreamId %v", streamId, keyspaceId, sessionId)
		idx.startKeyspaceIdStream(streamId, keyspaceId, restartTs, nil, nil, true, async, sessionId)
	} else {
		logging.Infof("Indexer::handleKVStreamRepair Ignore Stream Repair Request for Stream "+
			"%v KeyspaceId %v. Request In Progress.", streamId, keyspaceId)
	}

}

func (idx *indexer) handleInitBuildDoneAck(msg Message) {

	streamId := msg.(*MsgTKInitBuildDone).GetStreamId()
	keyspaceId := msg.(*MsgTKInitBuildDone).GetKeyspaceId()
	sessionId := msg.(*MsgTKInitBuildDone).GetSessionId()
	mergeTs := msg.(*MsgTKInitBuildDone).GetMergeTs()

	logging.Infof("Indexer::handleInitBuildDoneAck StreamId %v KeyspaceId %v SessionId %v",
		streamId, keyspaceId, sessionId)

	//skip processing initial build done ack for inactive or recovery streams.
	//the streams would be restarted and then build done would get recomputed.
	state := idx.getStreamKeyspaceIdState(streamId, keyspaceId)

	//if INIT_STREAM has started recovery, set the minMergeTs from the successful
	//AddInstance. minMergeTs is still valid as CATCHUP state indexes were
	//added to MAINT_STREAM. If INIT_STREAM is active, minMergeTs needs to be
	//set here as well for use in case of recovery of INIT_STREAM.
	if state == STREAM_ACTIVE ||
		state == STREAM_PREPARE_RECOVERY ||
		state == STREAM_RECOVERY {
		idx.setMinMergeTsForCatchup(streamId, keyspaceId, mergeTs)
	}

	if state == STREAM_INACTIVE ||
		state == STREAM_PREPARE_RECOVERY ||
		state == STREAM_RECOVERY {
		logging.Infof("Indexer::handleInitBuildDoneAck Skip InitBuildDoneAck %v %v %v",
			streamId, keyspaceId, state)
		return
	}

	bucket := GetBucketFromKeyspaceId(keyspaceId)
	//if MAINT_STREAM is not running
	//i. all indexes dropped
	//ii. all indexes get reset due to rollback to 0
	//it needs to be started here
	if idx.getStreamKeyspaceIdState(common.MAINT_STREAM, bucket) == STREAM_INACTIVE {
		logging.Infof("Indexer::handleInitBuildDoneAck StreamId %v KeyspaceId %v SessionId %v."+
			" Starting MAINT_STREAM.", streamId, keyspaceId, sessionId)
		idx.prepareStreamKeyspaceIdForFreshStart(common.MAINT_STREAM, bucket)
		sid := idx.genNextSessionId(common.MAINT_STREAM, bucket)

		idx.setStreamKeyspaceIdState(common.MAINT_STREAM, bucket, STREAM_ACTIVE)
		idx.startKeyspaceIdStream(common.MAINT_STREAM, bucket, mergeTs, nil, nil, false, false, sid)
	}

	if ok, currSid := idx.validateSessionId(streamId, keyspaceId, sessionId, true); !ok {
		logging.Infof("Indexer::handleInitBuildDoneAck StreamId %v KeyspaceId %v SessionId %v. "+
			"Skipped. Current SessionId %v.", streamId, keyspaceId, sessionId, currSid)
		return
	}

	switch streamId {

	case common.INIT_STREAM:

		//send the ack to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

		bucket := GetBucketFromKeyspaceId(keyspaceId)
		idx.processPendingBuildDone(common.MAINT_STREAM, bucket, sessionId)

	default:
		logging.Fatalf("Indexer::handleInitBuildDoneAck Unexpected Initial Build Ack Done "+
			"Received for Stream %v KeyspaceId %v", streamId, keyspaceId)
		common.CrashOnError(errors.New("Unexpected Initial Build Ack Done"))
	}

}

func (idx *indexer) handleAddInstanceFail(msg Message) {

	streamId := msg.(*MsgTKInitBuildDone).GetStreamId()
	keyspaceId := msg.(*MsgTKInitBuildDone).GetKeyspaceId()
	sessionId := msg.(*MsgTKInitBuildDone).GetSessionId()

	logging.Infof("Indexer::handleAddInstanceFail StreamId %v KeyspaceId %v SessionId %v",
		streamId, keyspaceId, sessionId)

	switch streamId {

	case common.INIT_STREAM:

		//reset the minMergeTs. It will be updated by RecoveryDone message
		//after recovery of MAINT_STREAM finishes.
		idx.setMinMergeTsForCatchup(streamId, keyspaceId, nil)

		//notify failure to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

		mergeStreamId := common.MAINT_STREAM

		//initiate recovery if not already in progress
		//it is ok to skip ADD_FAIL if recovery is already in progess
		//as at this point the index state change has already been
		//picked up by MAINT_STREAM recovery
		bucket := GetBucketFromKeyspaceId(keyspaceId)
		state := idx.getStreamKeyspaceIdState(mergeStreamId, bucket)

		if state == STREAM_PREPARE_RECOVERY ||
			state == STREAM_RECOVERY {
			logging.Infof("Indexer::handleAddInstanceFail Skip Recovery %v %v %v",
				mergeStreamId, bucket, state)
			return
		} else if state == STREAM_INACTIVE {

			//if MAINT_STREAM is not running
			//i. all indexes dropped
			//ii. all indexes get reset due to rollback to 0
			//it needs to be started here

			if idx.streamKeyspaceIdPendStart[common.MAINT_STREAM][bucket] {
				logging.Infof("Indexer::handleAddInstanceFail Skip Start Inactive Stream %v %v %v due to pending start.",
					mergeStreamId, bucket, state)
				return
			}

			logging.Infof("Indexer::handleAddInstanceFail Start Inactive Stream %v %v %v",
				mergeStreamId, bucket, state)

			flushTs := msg.(*MsgTKInitBuildDone).GetFlushTs()
			idx.restartMaintStreamForCatchup(bucket, flushTs)

		} else {
			//use the current sessionId for MAINT_STREAM
			maintSessionId := idx.getCurrentSessionId(mergeStreamId, bucket)
			idx.handleInitPrepRecovery(&MsgRecovery{mType: INDEXER_INIT_PREP_RECOVERY,
				streamId:   mergeStreamId,
				keyspaceId: bucket,
				sessionId:  maintSessionId})
		}

	default:
		logging.Fatalf("Indexer::handleAddInstanceFail Unexpected Add Instance Fail "+
			"Received for Stream %v KeyspaceId %v", streamId, keyspaceId)
		common.CrashOnError(errors.New("Unexpected Add Instance Fail"))
	}
}

func (idx *indexer) handleMergeStreamAck(msg Message) {

	streamId := msg.(*MsgTKMergeStream).GetStreamId()
	keyspaceId := msg.(*MsgTKMergeStream).GetKeyspaceId()
	mergeTs := msg.(*MsgTKMergeStream).GetMergeTS()
	sessionId := msg.(*MsgTKMergeStream).GetSessionId()
	reqCh := msg.(*MsgTKMergeStream).GetRequestCh()

	logging.Infof("Indexer::handleMergeStreamAck StreamId %v KeyspaceId %v SessionId %v",
		streamId, keyspaceId, sessionId)

	if ok, currSid := idx.validateSessionId(streamId, keyspaceId, sessionId, false); !ok {
		logging.Infof("Indexer::handleMergeStreamAck StreamId %v KeyspaceId %v SessionId %v. "+
			"Skipped. Current SessionId %v.", streamId, keyspaceId, sessionId, currSid)
		return
	}

	switch streamId {

	case common.INIT_STREAM:
		idx.deleteStreamKeyspaceIdCurrRequest(streamId, keyspaceId, msg, reqCh, sessionId)

		//if MAINT_STREAM is not running
		//i. all indexes dropped
		//ii. all indexes get reset due to rollback to 0
		//it needs to be started here

		mergeStreamId := common.MAINT_STREAM
		bucket := GetBucketFromKeyspaceId(keyspaceId)
		mstate := idx.getStreamKeyspaceIdState(mergeStreamId, bucket)
		if mstate == STREAM_INACTIVE {
			if idx.streamKeyspaceIdPendStart[mergeStreamId][bucket] {
				logging.Infof("Indexer::handleMergeStreamAck Skip Start Inactive Stream %v %v %v due to pending start.",
					mergeStreamId, bucket, mstate)
			} else {
				logging.Infof("Indexer::handleMergeStreamAck Start Inactive Stream %v %v %v",
					mergeStreamId, bucket, mstate)

				idx.restartMaintStreamForCatchup(bucket, mergeTs)
			}
		}

		//send the ack to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

	default:
		logging.Fatalf("Indexer::handleMergeStreamAck Unexpected Merge Stream Ack "+
			"Received for Stream %v KeyspaceId %v", streamId, keyspaceId)
		common.CrashOnError(errors.New("Unexpected Merge Stream Ack"))
	}

}

func (idx *indexer) handleStreamRequestDone(msg Message) {

	streamId := msg.(*MsgStreamInfo).GetStreamId()
	keyspaceId := msg.(*MsgStreamInfo).GetKeyspaceId()
	sessionId := msg.(*MsgStreamInfo).GetSessionId()
	reqCh := msg.(*MsgStreamInfo).GetRequestCh()

	if ok, currSid := idx.validateSessionId(streamId, keyspaceId, sessionId, true); !ok {
		logging.Infof("Indexer::handleStreamRequestDone StreamId %v KeyspaceId %v SessionId %v. "+
			"Skipped. Current SessionId %v.", streamId, keyspaceId, sessionId, currSid)
		return
	}

	logging.Infof("Indexer::handleStreamRequestDone StreamId %v KeyspaceId %v",
		streamId, keyspaceId)

	//send the ack to timekeeper
	idx.tkCmdCh <- msg
	<-idx.tkCmdCh

	idx.deleteStreamKeyspaceIdCurrRequest(streamId, keyspaceId, msg, reqCh, sessionId)

	//If any OSO exception has been recorded, initiate recovery when the stream request
	//finishes. Any earlier recovery attempts would have been skipped as this stream
	//request was in progress. If a recovery is already in progress, this call to
	//initiate recovery will get skipped.
	if idx.streamKeyspaceIdOSOException[streamId][keyspaceId] {
		idx.initRecoveryForOSO(streamId, keyspaceId, sessionId, nil)
	}
}

func (idx *indexer) initRecoveryForOSO(streamId common.StreamId,
	keyspaceId string, sessionId uint64, requestCh StopChannel) {

	logging.Infof("Indexer::initRecoveryForOSO StreamId %v KeyspaceId %v SessionId %v. "+
		"Initiate Recovery.", streamId, keyspaceId, sessionId)

	//create zero ts for rollback to 0
	numVbuckets := idx.config["numVbuckets"].Int()
	restartTs := common.NewTsVbuuid(GetBucketFromKeyspaceId(keyspaceId), numVbuckets)

	idx.handleInitPrepRecovery(&MsgRecovery{mType: INDEXER_INIT_PREP_RECOVERY,
		streamId:   streamId,
		keyspaceId: keyspaceId,
		sessionId:  sessionId,
		restartTs:  restartTs,
		requestCh:  requestCh})

}

func (idx *indexer) handleMTRFail(msg Message) {

	streamId := msg.(*MsgRecovery).GetStreamId()
	keyspaceId := msg.(*MsgRecovery).GetKeyspaceId()
	sessionId := msg.(*MsgRecovery).GetSessionId()
	reqCh := msg.(*MsgRecovery).GetRequestCh()

	if ok, currSid := idx.validateSessionId(streamId, keyspaceId, sessionId, true); !ok {
		logging.Infof("Indexer::handleMTRFail StreamId %v KeyspaceId %v SessionId %v. "+
			"Skipped. Current SessionId %v.", streamId, keyspaceId, sessionId, currSid)
		return
	}

	logging.Infof("Indexer::handleMTRFail StreamId %v KeyspaceId %v SessionId %v",
		streamId, keyspaceId, sessionId)

	//Cleanup any lock that indexer may be holding.
	idx.deleteStreamKeyspaceIdCurrRequest(streamId, keyspaceId, msg, reqCh, sessionId)

	//check for any pending build done
	if idx.getStreamKeyspaceIdState(streamId, keyspaceId) == STREAM_INACTIVE {
		idx.processPendingBuildDone(streamId, keyspaceId, sessionId)
		return
	}

}

// handleKeyspaceNotFound deletes the metadata for built indexes in the missing keyspace, or
// if recovery already in progress recovery will do the cleanup so this will skip it. It will
// also distribute updated maps to all the workers and delete the associated slices. Similar
// to cleanupIndexDataForCollectionDrop for collections. (Cleanup of deferred indexes from a
// dropped keyspace uses a different codepath: IndexManager.monitorKeyspace (manager/manager.go)).
func (idx *indexer) handleKeyspaceNotFound(msg Message) {

	streamId := msg.(*MsgRecovery).GetStreamId()
	keyspaceId := msg.(*MsgRecovery).GetKeyspaceId()
	inMTR := msg.(*MsgRecovery).InMTR()
	sessionId := msg.(*MsgRecovery).GetSessionId()

	if ok, currSid := idx.validateSessionId(streamId, keyspaceId, sessionId, true); !ok {
		logging.Infof("Indexer::handleKeyspaceNotFound StreamId %v KeyspaceId %v SessionId %v. "+
			"Skipped. Current SessionId %v.", streamId, keyspaceId, sessionId, currSid)
		return
	}

	logging.Infof("Indexer::handleKeyspaceNotFound StreamId %v KeyspaceId %v SessionId %v",
		streamId, keyspaceId, sessionId)

	// if in MTR, MTR must have stopped when recieving this message.
	// Cleanup any lock that indexer may be holding.
	if inMTR {
		idx.cleanupStreamKeyspaceIdState(streamId, keyspaceId)
	}

	is := idx.getIndexerState()
	if is == common.INDEXER_PREPARE_UNPAUSE {
		logging.Warnf("Indexer::handleKeyspaceNotFound Skipped KeyspaceId Cleanup "+
			"In %v state", is)
		return
	}

	//If stream is inactive, no cleanup is required.
	//If stream is prepare_recovery, recovery will take care of
	//validating the keyspaceId and taking corrective action.
	state := idx.getStreamKeyspaceIdState(streamId, keyspaceId)

	if state == STREAM_INACTIVE || state == STREAM_PREPARE_RECOVERY {
		// Recovery already in progress so it will do the cleanup
		logging.Infof("Indexer::handleKeyspaceNotFound Skip %v %v %v",
			streamId, keyspaceId, state)
		return
	}

	// delete index inst on the bucket from metadata repository and
	// return the list of deleted inst
	bucket, scope, collection := SplitKeyspaceId(keyspaceId)
	deletedInstIds := idx.deleteIndexInstOnDeletedKeyspace(bucket, scope, collection, streamId)

	if len(deletedInstIds) == 0 {
		logging.Infof("Indexer::handleKeyspaceNotFound Empty IndexList. Stopping the keyspaceId stream %v %v",
			streamId, keyspaceId)

		idx.stopKeyspaceIdStream(streamId, keyspaceId, true)
		idx.setStreamKeyspaceIdState(streamId, keyspaceId, STREAM_INACTIVE)

		return
	}

	idx.bulkUpdateState(deletedInstIds, common.INDEX_STATE_DELETED)
	logging.Infof("Indexer::handleKeyspaceNotFound Updated Index State to DELETED %v",
		deletedInstIds)

	msgUpdateIndexInstMap := idx.newIndexInstMsg(idx.indexInstMap)
	deletedInsts := idx.getInsts(deletedInstIds)
	msgUpdateIndexInstMap.AppendUpdatedInsts(deletedInsts)
	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, nil); err != nil {
		common.CrashOnError(err)
	}

	// If there is a pending collection drop at this point, it means
	// flush is in progress. Skip clean-up here as the index data will
	// be cleaned-up once flush is done
	if val, ok := idx.streamKeyspaceIdFlushInProgress[streamId][keyspaceId]; !ok || val == false {
		idx.stopKeyspaceIdStream(streamId, keyspaceId, true)
		idx.cleanupIndexData(deletedInsts, nil)
		idx.setStreamKeyspaceIdState(streamId, keyspaceId, STREAM_INACTIVE)

		logging.Infof("Indexer::handleKeyspaceNotFound %v %v %v",
			streamId, keyspaceId, STREAM_INACTIVE)
	} else {
		idx.streamKeyspaceIdPendCollectionDrop[streamId][keyspaceId] =
			append(idx.streamKeyspaceIdPendCollectionDrop[streamId][keyspaceId], deletedInstIds...)
		logging.Infof("Indexer::handleKeyspaceNotFound Skip clean-up of index data as "+
			"flush is in progress %v %v", streamId, keyspaceId)
	}
}

func (idx *indexer) handleDcpSystemEvent(cmd Message) {

	eventType := cmd.(*MsgStream).GetEventType()
	streamId := cmd.(*MsgStream).GetStreamId()
	meta := cmd.(*MsgStream).GetMutationMeta()
	scopeId := cmd.(*MsgStream).GetScopeId()
	collectionId := cmd.(*MsgStream).GetCollectionId()

	if eventType == common.CollectionDrop {
		idx.processCollectionDrop(streamId,
			meta.keyspaceId, scopeId, collectionId)
	}

	return

}

func (idx *indexer) processCollectionDrop(streamId common.StreamId,
	keyspaceId, scopeId, collectionId string) {

	logging.Verbosef("Indexer::processCollectionDrop %v %v %v %v", streamId,
		keyspaceId, scopeId, collectionId)

	bucket := GetBucketFromKeyspaceId(keyspaceId)
	idx.cinfoProviderLock.RLock()
	bucketUUID, _ := idx.cinfoProvider.GetBucketUUID(bucket)
	idx.cinfoProviderLock.RUnlock()

	//get the collection name from index inst map(this may already be gone from manifest)
	var collection, scope string
	for _, index := range idx.indexInstMap {
		if index.Defn.CollectionId == collectionId &&
			index.Stream == streamId &&
			index.State != common.INDEX_STATE_DELETED &&
			index.Defn.Bucket == bucket &&
			(bucketUUID == common.BUCKET_UUID_NIL || bucketUUID == index.Defn.BucketUUID) {

			collection = index.Defn.Collection
			scope = index.Defn.Scope
			bucketUUID = index.Defn.BucketUUID
			break
		}
	}

	if collection == "" {
		logging.Infof("Indexer::processCollectionDrop No Index Found for %v %v %v.",
			streamId, keyspaceId, collectionId)
		return
	}

	// delete index inst on the keyspace from metadata repository and
	// return the list of deleted inst
	instIdList := idx.deleteIndexInstOnDeletedKeyspace(bucket, scope, collection, streamId)

	if len(instIdList) == 0 {
		logging.Infof("Indexer::processCollectionDrop Empty IndexList %v %v %v %v.",
			streamId, keyspaceId, collectionId, collection)
		return
	}

	idx.bulkUpdateState(instIdList, common.INDEX_STATE_DELETED)
	logging.Infof("Indexer::processCollectionDrop Updated Index State to DELETED %v %v %v %v %v",
		streamId, keyspaceId, collectionId, collection, instIdList)

	msgUpdateIndexInstMap := idx.newIndexInstMsg(idx.indexInstMap)
	updatedInstances := idx.getInsts(instIdList)
	msgUpdateIndexInstMap.AppendUpdatedInsts(updatedInstances)

	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, nil); err != nil {
		common.CrashOnError(err)
	}

	if !idx.streamKeyspaceIdFlushInProgress[streamId][keyspaceId] {
		idx.cleanupIndexDataForCollectionDrop(streamId, keyspaceId, instIdList)
	} else {
		logging.Infof("Indexer::processCollectionDrop Flush in Progress Adding instances to pending list %v %v %v %v",
			streamId, keyspaceId, collectionId, collection)
		currList := idx.streamKeyspaceIdPendCollectionDrop[streamId][keyspaceId]
		currList = append(currList, instIdList...)
		idx.streamKeyspaceIdPendCollectionDrop[streamId][keyspaceId] = currList
	}
}

// cleanupIndexDataForCollectionDrop deletes the metadata for built indexes in the dropped
// collection, distributes updated maps to all the workers, and deletes the associated slices.
// Caller must guarantee instIdList is non-empty. Similar to handleKeyspaceNotFound for buckets.
func (idx *indexer) cleanupIndexDataForCollectionDrop(streamId common.StreamId,
	keyspaceId string, deletedInstIds []common.IndexInstId) {

	logging.Infof("Indexer::cleanupIndexDataForCollectionDrop %v %v", streamId, keyspaceId)

	bucketUUID := idx.indexInstMap[deletedInstIds[0]].Defn.BucketUUID // to-be-deleted info needed below
	deletedInsts := idx.getInsts(deletedInstIds)
	idx.cleanupIndexData(deletedInsts, nil)

	// Skip instances with NIL_STREAM
	indexesWithStream := make([]common.IndexInst, 0)
	for _, index := range deletedInsts {
		if index.Stream == streamId {
			indexesWithStream = append(indexesWithStream, index)
		}
	}

	idx.removeIndexesFromStream(indexesWithStream, keyspaceId,
		bucketUUID, streamId, common.INDEX_STATE_ACTIVE, nil)
}

func (idx *indexer) newIndexInstMsg(m common.IndexInstMap) *MsgUpdateInstMap {
	return &MsgUpdateInstMap{indexInstMap: m, stats: idx.stats.Clone(),
		rollbackTimes: idx.keyspaceIdRollbackTimes}
}

func (idx *indexer) newKeyspaceStatsMsg() *MsgUpdateKeyspaceStatsMap {
	return &MsgUpdateKeyspaceStatsMap{keyspaceStatsMap: idx.stats.GetKeyspaceStatsMap().Clone()}
}

// cleanupIndexData updates and distributes index metadata to workers reflecting
// the deletion of a set of instances and deletes the associated slices.
func (idx *indexer) cleanupIndexData(indexInsts []common.IndexInst,
	clientCh MsgChannel) {

	// Delete all instances from internal maps
	var indexInstIds []common.IndexInstId
	idxPartnInfoMap := make(map[common.IndexInstId]PartitionInstMap, len(indexInsts))
	for _, indexInst := range indexInsts {
		indexInstId := indexInst.InstId
		indexInstIds = append(indexInstIds, indexInstId)
		idxPartnInfoMap[indexInstId] = idx.indexPartnMap[indexInstId] // to-be-deleted metadata needed below

		delete(idx.indexInstMap, indexInstId)
		delete(idx.indexPartnMap, indexInstId)
		deleteFreeWriters(indexInstId)
		idx.deletePendingReset(indexInstId)
	}

	// Send the updated maps to all workers
	msgUpdateIndexInstMap := idx.newIndexInstMsg(idx.indexInstMap)
	msgUpdateIndexInstMap.AppendDeletedInstIds(indexInstIds)
	msgUpdateIndexPartnMap := &MsgUpdatePartnMap{indexPartnMap: idx.indexPartnMap}
	msgUpdateIndexPartnMap.AppendDeletedInstIds(indexInstIds)
	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap,
		msgUpdateIndexPartnMap); err != nil {
		if clientCh != nil {
			clientCh <- &MsgError{
				err: Error{code: ERROR_INDEXER_INTERNAL_ERROR,
					severity: FATAL,
					cause:    err,
					category: INDEXER}}
		}
		common.CrashOnError(err)
	}

	// Delete the slices associated with the deleted instances
	for _, indexInst := range indexInsts {
		//for all partitions managed by this indexer
		if indexInst.RState != common.REBAL_MERGED {
			for _, partnInst := range idxPartnInfoMap[indexInst.InstId] {
				sc := partnInst.Sc
				pid := partnInst.Defn.GetPartitionId()
				//close all the slices
				for _, slice := range sc.GetAllSlices() {
					go func() {
						slice.Close()
						logging.Infof("Indexer::cleanupIndexData IndexInst %v Partition %v Close Done",
							slice.IndexInstId(), pid)
						//wipe the physical files
						slice.Destroy()
						logging.Infof("Indexer::cleanupIndexData IndexInst %v Partition %v Destroy Done",
							slice.IndexInstId(), pid)
					}()
				}
			}
		}
	}
}

func (idx *indexer) cleanupIndex(indexInst common.IndexInst,
	clientCh MsgChannel) {

	idx.cleanupIndexData([]common.IndexInst{indexInst}, clientCh)

	//send Stream update to workers
	if ok := idx.sendStreamUpdateForDropIndex(indexInst, clientCh); !ok {
		return
	}

	// If a proxy is deleted before merge has happened, we have to make sure
	// that the real instance is updated. If a proxy is already merged to the
	// real index inst, this function will not be called (since the proxy
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

	// When dropping an index in INIT_STREAM, check if the MAINT_STREAM
	// is no longer required. This happens if MAINT_STREAM was started by
	// catchup phase and there is no index on this bucket
	if indexInst.Stream == common.INIT_STREAM {
		idx.cleanupEmptyMaintStream(indexInst.Defn.Bucket)
	}

	if clientCh != nil {
		clientCh <- &MsgSuccess{}
	}
}

func (idx *indexer) sendStreamUpdateForIndex(indexInstList []common.IndexInst,
	keyspaceId string, bucketUUID string, streamId common.StreamId) {

	sessionId := idx.getCurrentSessionId(streamId, keyspaceId)

	respCh := make(MsgChannel)
	stopCh := make(StopChannel)

	cmd := &MsgStreamUpdate{
		mType:      ADD_INDEX_LIST_TO_STREAM,
		streamId:   streamId,
		keyspaceId: keyspaceId,
		indexList:  indexInstList,
		respCh:     respCh,
		stopCh:     stopCh,
		sessionId:  sessionId}

	retryCount := 0

	reqLock := idx.acquireStreamRequestLock(keyspaceId, streamId)
	go func(reqLock *kvRequest) {
		defer idx.releaseStreamRequestLock(reqLock)
		idx.waitStreamRequestLock(reqLock)
	retryloop:
		for {
			if !idx.ValidateKeyspace(streamId, keyspaceId, []string{bucketUUID}) {
				logging.Errorf("Indexer::sendStreamUpdateForIndex Keyspace Not Found "+
					"For Stream %v KeyspaceId %v", streamId, keyspaceId)
				break retryloop
			}
			idx.sendMsgToKVSender(cmd)

			if resp, ok := <-respCh; ok {

				switch resp.GetMsgType() {

				case MSG_SUCCESS:
					logging.Infof("Indexer::sendStreamUpdateForIndex Success Stream %v KeyspaceId %v "+
						"SessionId %v.", streamId, keyspaceId, sessionId)
					break retryloop

				default:
					if idx.getStreamKeyspaceIdState(streamId, keyspaceId) != STREAM_ACTIVE {
						logging.Warnf("Indexer::sendStreamUpdateForIndex Stream %v KeyspaceId %v "+
							"SessionId %v. KeyspaceId stream not active. Aborting.", streamId,
							keyspaceId, sessionId)
						break retryloop
					}

					//log and retry for all other responses
					respErr := resp.(*MsgError).GetError()

					//If projector returns TopicMissing/GenServerClosed, AddInstance
					//cannot succeed. This needs to be aborted so that stream lock is
					//released and MTR can proceed to repair the Topic.
					if respErr.cause.Error() == common.ErrorClosed.Error() ||
						respErr.cause.Error() == projClient.ErrorTopicMissing.Error() {
						logging.Warnf("Indexer::sendStreamUpdateForIndex Stream %v KeyspaceId %v "+
							"SessionId %v. Error from Projector %v. Aborting.", streamId, keyspaceId,
							sessionId, respErr.cause)
						break retryloop

					} else if retryCount < 10 {
						logging.Errorf("Indexer::sendStreamUpdateForIndex Stream %v KeyspaceId %v "+
							"SessionId %v. Error from Projector %v. Retrying.", streamId, keyspaceId,
							sessionId, respErr.cause)
						retryCount++
						time.Sleep(KV_RETRY_INTERVAL * time.Millisecond)
					} else {
						logging.Errorf("Indexer::sendStreamUpdateForIndex Stream %v keyspaceId %v "+
							"SessionId %v. Error from Projector %v. Reach max retry count. Stop retrying.",
							streamId, keyspaceId, sessionId, respErr.cause)
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

	if idx.enableManager {
		//shutdown cluster manager
		idx.sendMsgToClustMgr(&MsgGeneral{mType: CLUST_MGR_AGENT_SHUTDOWN})
	}

	// shutdown kv sender
	// sendMsgToKVSender lock protects writes and reads to
	// kvSenderCmdCh so that message crossover is prevented
	idx.sendMsgToKVSender(&MsgGeneral{mType: KV_SENDER_SHUTDOWN})

	// shutdown ddl manager
	idx.ddlSrvMgrCmdCh <- &MsgGeneral{mType: ADMIN_MGR_SHUTDOWN}
	<-idx.ddlSrvMgrCmdCh

	// shutdown scheduled index creator
	idx.schedIdxCreatorCmdCh <- &MsgGeneral{mType: ADMIN_MGR_SHUTDOWN}
	<-idx.schedIdxCreatorCmdCh
}

func (idx *indexer) Shutdown() Message {

	logging.Infof("Indexer::Shutdown -  Shutting Down")
	//close the internal shutdown channel
	close(idx.shutdownInitCh)
	<-idx.shutdownCompleteCh
	logging.Infof("Indexer:Shutdown - Shutdown Complete")
	return nil
}

// sendStreamUpdateForBuildIndex starts the logical stream for a given keyspaceId
// in the particular case of an index build. (startKeyspaceIdStream does this for
// all other cases.)
func (idx *indexer) sendStreamUpdateForBuildIndex(instIdList []common.IndexInstId,
	buildStream common.StreamId, keyspaceId string, cid string,
	clusterVer uint64, buildTs Timestamp, clientCh MsgChannel) {

	var cmd Message
	var indexList []common.IndexInst
	var bucketUUIDList []string
	for _, instId := range instIdList {
		indexInst := idx.indexInstMap[instId]
		indexList = append(indexList, indexInst)
		bucketUUIDList = append(bucketUUIDList, indexInst.Defn.BucketUUID)
	}

	respCh := make(MsgChannel)

	clustAddr := idx.config["clusterAddr"].String()
	numVb := idx.config["numVbuckets"].Int()
	enableAsync := idx.config["enableAsyncOpenStream"].Bool()
	enableOSO := idx.config["build.enableOSO"].Bool()

	if enableOSO &&
		clusterVer >= common.INDEXER_71_VERSION &&
		buildStream == common.INIT_STREAM {
		enableOSO = true
	} else {
		enableOSO = false
	}

	idx.prepareStreamKeyspaceIdForFreshStart(buildStream, keyspaceId)

	sessionId := idx.genNextSessionId(buildStream, keyspaceId)

	async := enableAsync && clusterVer >= common.INDEXER_65_VERSION

	idx.streamKeyspaceIdCollectionId[buildStream][keyspaceId] = cid

	//Set collectionAware to true unconditionally. DCP allows to enable
	//collections on upgraded nodes in mixed mode.
	collectionAware := true

	cmd = &MsgStreamUpdate{mType: OPEN_STREAM,
		streamId:           buildStream,
		keyspaceId:         keyspaceId,
		indexList:          indexList,
		buildTs:            buildTs,
		respCh:             respCh,
		restartTs:          nil,
		allowMarkFirstSnap: true,
		rollbackTime:       idx.keyspaceIdRollbackTimes[keyspaceId],
		async:              async,
		sessionId:          sessionId,
		collectionId:       cid,
		collectionAware:    collectionAware,
		enableOSO:          enableOSO}

	// Create the corresponding KeyspaceStats object before starting the stream
	idx.stats.AddKeyspaceStats(buildStream, keyspaceId)
	idx.distributeKeyspaceStatsMapsToWorkers()

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

	idx.initBuildTsLock(buildStream, keyspaceId)

	stopCh := make(StopChannel)

	idx.setStreamKeyspaceIdCurrRequest(buildStream, keyspaceId, cmd, stopCh, sessionId)

	reqLock := idx.acquireStreamRequestLock(keyspaceId, buildStream)
	go func(reqLock *kvRequest) {
		defer idx.releaseStreamRequestLock(reqLock)
		idx.waitStreamRequestLock(reqLock)
		count := 0

	retryloop:
		for {
			if !idx.ValidateKeyspace(buildStream, keyspaceId, bucketUUIDList) {
				logging.Errorf("Indexer::sendStreamUpdateForBuildIndex Keyspace Not Found "+
					"For Stream %v KeyspaceId %v SessionId %v", buildStream, keyspaceId, sessionId)
				idx.internalRecvCh <- &MsgRecovery{mType: INDEXER_KEYSPACE_NOT_FOUND,
					streamId:   buildStream,
					keyspaceId: keyspaceId,
					inMTR:      true,
					sessionId:  sessionId}
				break retryloop // recovery will delete the index metadata
			}
			idx.sendMsgToKVSender(cmd)

			if resp, ok := <-respCh; ok {

				switch resp.GetMsgType() {

				case MSG_SUCCESS_OPEN_STREAM:
					idx.injectRandomDelay(10) // no-op unless enabled
					logging.Infof("Indexer::sendStreamUpdateForBuildIndex Stream Request Success For "+
						"Stream %v KeyspaceId %v Cid %v SessionId %v", buildStream, keyspaceId, cid, sessionId)

					//once stream request is successful re-calculate the KV timestamp.
					//This makes sure indexer doesn't use a timestamp which can never
					//be caught up to (due to kv rollback).
					//if there is a failover after this, it will be observed as a rollback

					// Asyncronously compute the KV timestamp
					go idx.computeKeyspaceBuildTsAsync(clustAddr, keyspaceId, cid, numVb, buildStream)

					idx.internalRecvCh <- &MsgStreamInfo{mType: STREAM_REQUEST_DONE,
						streamId:   buildStream,
						keyspaceId: keyspaceId,
						activeTs:   resp.(*MsgSuccessOpenStream).GetActiveTs(),
						pendingTs:  resp.(*MsgSuccessOpenStream).GetPendingTs(),
						sessionId:  sessionId,
						reqCh:      stopCh,
					}
					break retryloop // success

				case INDEXER_ROLLBACK:
					//an initial build request should never receive rollback message
					logging.Errorf("Indexer::sendStreamUpdateForBuildIndex Unexpected Rollback from "+
						"Projector during Initial Stream Request %v", resp)
					common.CrashOnError(ErrKVRollbackForInitRequest)

				default:
					// Locally retriable cases will be retried by next retryloop iteration.
					// The rest will be retried by recovery (by sending MsgRecovery below),
					// which will restart the streams and repair any issues.
					respErr := resp.(*MsgError).GetError()
					count++

					state := idx.getStreamKeyspaceIdState(buildStream, keyspaceId)

					if state == STREAM_PREPARE_RECOVERY || state == STREAM_INACTIVE {
						logging.Errorf("Indexer::sendStreamUpdateForBuildIndex Stream %v KeyspaceId %v "+
							"SessionId %v. Error from Projector %v. Not Retrying. State %v", buildStream,
							keyspaceId, sessionId, respErr.cause, state)

						idx.internalRecvCh <- &MsgRecovery{mType: INDEXER_MTR_FAIL,
							streamId:   buildStream,
							keyspaceId: keyspaceId,
							inMTR:      true,
							requestCh:  stopCh,
							sessionId:  sessionId}

						break retryloop // recovery will retry

					} else if count > MAX_PROJ_RETRY {
						// Start recovery if max retries has reached. If the projector
						// state is not correct, this ensures projector state will get cleaned up.
						logging.Errorf("Indexer::sendStreamUpdateForBuildIndex Stream %v KeyspaceId %v "+
							"SessionId %v. Error from Projector %v. Start recovery after %v retries.",
							buildStream, sessionId, keyspaceId, respErr.cause, MAX_PROJ_RETRY)

						//Reset Stream if OSO
						if enableOSO {
							idx.internalRecvCh <- &MsgStreamUpdate{
								mType:      RESET_STREAM,
								streamId:   buildStream,
								keyspaceId: keyspaceId,
								sessionId:  sessionId,
								stopCh:     stopCh,
							}
						} else {
							idx.internalRecvCh <- &MsgRecovery{
								mType:      INDEXER_INIT_PREP_RECOVERY,
								streamId:   buildStream,
								keyspaceId: keyspaceId,
								requestCh:  stopCh,
								sessionId:  sessionId,
							}
						}
						break retryloop // recovery will retry
					} else {
						logging.Errorf("Indexer::sendStreamUpdateForBuildIndex Stream %v KeyspaceId %v "+
							"SessionId %v. Error from Projector %v. Retrying %v.", buildStream, keyspaceId,
							sessionId, respErr.cause, count)

						time.Sleep(KV_RETRY_INTERVAL * time.Millisecond)
					}
				}
			}
		}
	}(reqLock)
}

func (idx *indexer) makeCollectionIdForStreamRequest(streamId common.StreamId,
	keyspaceId string, collectionId string, clusterVer uint64) string {

	if streamId == common.MAINT_STREAM {
		//MAINT_STREAM always works at bucket level and
		//collectionID is not required in stream request
		return ""
	} else if streamId == common.INIT_STREAM {
		//INIT_STREAM works on collection level after
		//cluster has fully upgraded to 7.0 or later
		if clusterVer >= common.INDEXER_70_VERSION {
			return collectionId
		} else {
			return ""
		}
	}

	return ""

}

func (idx *indexer) sendMsgToKVSender(cmd Message) {
	idx.kvlock.Lock()
	defer idx.kvlock.Unlock()

	//send stream update to kv sender
	idx.kvSenderCmdCh <- cmd
	<-idx.kvSenderCmdCh
}

func (idx *indexer) sendMsgToClustMgr(cmd Message) (Message, bool) {
	idx.clustMgrLock.Lock()
	defer idx.clustMgrLock.Unlock()

	//send stream update to kv sender
	idx.clustMgrAgentCmdCh <- cmd
	resp, ok := <-idx.clustMgrAgentCmdCh
	return resp, ok
}

// sendStreamUpdateToWorker synchronously sends a message to a worker and awaits the
// reply or channel death, logging and returning an error unless it receives a success message.
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

	return idx.removeIndexesFromStream(indexList, indexInst.Defn.KeyspaceId(indexInst.Stream),
		indexInst.Defn.BucketUUID, indexInst.Stream, indexInst.State, clientCh)
}

// removeIndexesFromStream is called for a list of indexes that are all in the same keyspace and
// are all assumed to be in DELETED state.
// o If any active indexes still exist in that keyspace, it removes only the listed indexes from
//   the stream. Since these are in DELETED state, the active indexes must be other ones, so the
//   keyspace is kept alive.
// o Else the request for DCP records for the entire keyspace is removed from the logical stream.
func (idx *indexer) removeIndexesFromStream(indexList []common.IndexInst,
	keyspaceId string,
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

		if idx.getStreamKeyspaceIdState(streamId, keyspaceId) == STREAM_INACTIVE {
			logging.Warnf("Indexer::removeIndexesFromStream Stream %v KeyspaceId %v "+
				"KeyspaceId stream not active. Skipping.", streamId, keyspaceId)
			continue
		}

		sessionId := idx.getCurrentSessionId(streamId, keyspaceId)
		isRemoveKeyspace := false
		if idx.checkKeyspaceIdExistsInStream(keyspaceId, streamId, false) {

			cmd = &MsgStreamUpdate{mType: REMOVE_INDEX_LIST_FROM_STREAM,
				streamId:  streamId,
				indexList: indexList,
				respCh:    respCh,
				sessionId: sessionId,
			}
		} else {
			cmd = &MsgStreamUpdate{mType: REMOVE_KEYSPACE_FROM_STREAM,
				streamId:      streamId,
				keyspaceId:    keyspaceId,
				respCh:        respCh,
				sessionId:     sessionId,
				abortRecovery: true,
			}
			idx.setStreamKeyspaceIdState(streamId, keyspaceId, STREAM_INACTIVE)
			idx.cleanupAllStreamKeyspaceIdState(streamId, keyspaceId)
			isRemoveKeyspace = true
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

		// If removing entire keyspace, also remove the stats for it
		if isRemoveKeyspace {
			idx.stats.RemoveKeyspaceStats(streamId, keyspaceId)
			idx.distributeKeyspaceStatsMapsToWorkers()
		}

		reqLock := idx.acquireStreamRequestLock(keyspaceId, streamId)
		go func(reqLock *kvRequest) {
			defer idx.releaseStreamRequestLock(reqLock)
			idx.waitStreamRequestLock(reqLock)
		retryloop:
			for {

				if !idx.ValidateKeyspace(streamId, keyspaceId, []string{bucketUUID}) {
					logging.Errorf("Indexer::removeIndexesFromStream Keyspace Not Found "+
						"For Stream %v KeyspaceId %v", streamId, keyspaceId)
					idx.internalRecvCh <- &MsgRecovery{mType: INDEXER_KEYSPACE_NOT_FOUND,
						streamId:   streamId,
						keyspaceId: keyspaceId,
						sessionId:  sessionId}
					break retryloop
				}

				idx.sendMsgToKVSender(cmd)

				if resp, ok := <-respCh; ok {

					switch resp.GetMsgType() {

					case MSG_SUCCESS:
						logging.Infof("Indexer::removeIndexesFromStream Success Stream %v "+
							"KeyspaceId %v SessionId %v", streamId, keyspaceId, sessionId)
						break retryloop

					default:
						if idx.getStreamKeyspaceIdState(streamId, keyspaceId) != STREAM_ACTIVE {
							logging.Warnf("Indexer::removeIndexesFromStream Stream %v KeyspaceId %v "+
								"SessionId %v KeyspaceId stream not active. Aborting.", streamId,
								keyspaceId, sessionId)
							break retryloop
						}

						//log and retry for all other responses
						respErr := resp.(*MsgError).GetError()
						logging.Errorf("Indexer::removeIndexesFromStream - Stream %v KeyspaceId %v"+
							"SessionId %v. Error from Projector %v. Retrying.", streamId, keyspaceId,
							sessionId, respErr.cause)
						time.Sleep(KV_RETRY_INTERVAL * time.Millisecond)

					}
				}
			}
		}(reqLock)
	}

	return true

}

func (idx *indexer) initPartnInstance(indexInst common.IndexInst,
	respCh MsgChannel, bootstrapPhase bool) (PartitionInstMap, PartitionInstMap, error) {

	//initialize partitionInstMap for this index
	partnInstMap := make(PartitionInstMap)
	var failedPartnInstances PartitionInstMap

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
		var slice Slice
		idx.cinfoProviderLock.RLock()
		ephemeral, err := idx.cinfoProvider.IsEphemeral(indexInst.Defn.Bucket)
		idx.cinfoProviderLock.RUnlock()
		if err != nil {
			logging.Errorf("Indexer::initPartnInstance Failed to check bucket type ephemeral: %v\n", err)
		} else {
			slice, err = NewSlice(SliceId(0), &indexInst, &partnInst, idx.config, idx.stats, ephemeral, !bootstrapPhase, idx.meteringMgr)
		}

		if err == nil {
			partnInst.Sc.AddSlice(0, slice)
			logging.Infof("Indexer::initPartnInstance Initialized Slice: \n\t Index: %v Slice: %v",
				indexInst.InstId, slice)

			partnInstMap[partnDefn.GetPartitionId()] = partnInst
		} else {
			if bootstrapPhase && err == errStorageCorrupted {
				errStr := fmt.Sprintf("storage corruption for indexInst %v partnDefn %v", indexInst, partnDefn)
				logging.Errorf("Indexer:: initPartnInstance %v", errStr)
				failedPartnInstances = failedPartnInstances.Add(partnDefn.GetPartitionId(), partnInst)
				continue
			}

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
			return nil, nil, err1
		}
	}

	return partnInstMap, failedPartnInstances, nil
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

	if err := idx.sendUpdatedIndexMapToWorker(msgUpdateIndexInstMap, nil, idx.compactMgrCmdCh,
		"compactionManager"); err != nil {
		return err
	}

	return nil
}

func (idx *indexer) sendUpdatedIndexMapToWorker(msgUpdateIndexInstMap Message,
	msgUpdateIndexPartnMap Message, workerCmdCh chan Message, workerStr string) error {

	if err := idx.sendMessageToWorker(msgUpdateIndexInstMap, workerCmdCh, workerStr); err != nil {
		return err
	}
	if err := idx.sendMessageToWorker(msgUpdateIndexPartnMap, workerCmdCh, workerStr); err != nil {
		return err
	}

	return nil
}

// distributeKeyspaceStatsMapsToWorkers sends a clone of current KeyspaceStatsMap to local consumers that need it.
func (idx *indexer) distributeKeyspaceStatsMapsToWorkers() error {
	msg := idx.newKeyspaceStatsMsg()

	// Mutation Manager will pass this on to Stream Reader children it manages
	if err := idx.sendUpdatedKeyspaceStatsMapToWorker(msg, idx.mutMgrCmdCh, "MutationMgr"); err != nil {
		return err
	}

	if err := idx.sendUpdatedKeyspaceStatsMapToWorker(msg, idx.storageMgrCmdCh, "StorageMgr"); err != nil {
		return err
	}

	if err := idx.sendUpdatedKeyspaceStatsMapToWorker(msg, idx.tkCmdCh, "Timekeeper"); err != nil {
		return err
	}

	return nil
}

// sendUpdatedKeyspaceStatsMapToWorker synchronously sends updated keyspace stats pointer.
func (idx *indexer) sendUpdatedKeyspaceStatsMapToWorker(msgUpdateKeyspaceStatsMap Message,
	workerCmdCh chan Message, workerStr string) error {

	return idx.sendMessageToWorker(msgUpdateKeyspaceStatsMap, workerCmdCh, workerStr)
}

// sendMessageToWorker synchronously sends a message a worker and logs errors in detail.
func (idx *indexer) sendMessageToWorker(msg Message, workerCmdCh chan Message, workerStr string) error {

	if msg != nil {
		var resp Message
		var ok bool
		if workerCmdCh == idx.clustMgrAgentCmdCh && workerStr == "clusterMgrAgent" {
			resp, ok = idx.sendMsgToClustMgr(msg)
		} else {
			workerCmdCh <- msg
			resp, ok = <-workerCmdCh
		}

		if ok {
			if resp.GetMsgType() == MSG_ERROR {
				logging.Errorf("Indexer::sendMessageToWorker - Error received from %v processing "+
					"Msg %v Err %v. Aborted.", workerStr, msg, resp)
				respErr := resp.(*MsgError).GetError()
				return respErr.cause
			}
		} else {
			logging.Errorf("Indexer::sendMessageToWorker - Error communicating with %v "+
				"processing Msg %v. Aborted.", workerStr, msg)
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
	ServiceAddrMap[common.INDEX_HTTPS_SERVICE] = idx.config["httpsPort"].String()

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
				index.Defn.Scope == indexInst.Defn.Scope &&
				index.Defn.Collection == indexInst.Defn.Collection &&
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

//checkDuplicateInitialBuildRequest check if any other index on the given collection
//is already building
func (idx *indexer) checkDuplicateInitialBuildRequest(keyspaceId string,
	instIdList []common.IndexInstId, respCh MsgChannel, errMap map[common.IndexInstId]error) bool {

	//if initial build is already running for some other index on this collection,
	//cannot start another one
	for _, index := range idx.indexInstMap {

		if (index.State == common.INDEX_STATE_INITIAL ||
			index.State == common.INDEX_STATE_CATCHUP) &&
			index.Defn.KeyspaceId(common.INIT_STREAM) == keyspaceId {

			errStr := fmt.Sprintf("Build Already In Progress. Keyspace %v", keyspaceId)
			logging.Errorf("Indexer::checkDuplicateInitialBuildRequest %v, %v", index, keyspaceId)
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

// checkParallelCollectionBuilds returns false if the current number of keyspaces
// on which index builds are happening is greater than the configuration value
// "max_parallel_collection_builds".

// The caller should schedule building the indexes in background when this method
// returns false
func (idx *indexer) checkParallelCollectionBuilds(keyspaceId string,
	instIdList []common.IndexInstId, respCh MsgChannel, errMap map[common.IndexInstId]error) bool {

	maxParallelCollectionBuilds := idx.config["max_parallel_collection_builds"].Int()

	parallelCollectionBuildMap := make(map[string]bool)
	// Find all the keyspaces on which initial build is in progress
	for _, index := range idx.indexInstMap {

		if index.State == common.INDEX_STATE_INITIAL ||
			index.State == common.INDEX_STATE_CATCHUP {
			keyspaceId := index.Defn.KeyspaceId(common.INIT_STREAM)
			if _, ok := parallelCollectionBuildMap[keyspaceId]; !ok {
				parallelCollectionBuildMap[keyspaceId] = true
			}
		}
	}
	currParallelCollectionBuilds := len(parallelCollectionBuildMap)

	if currParallelCollectionBuilds >= maxParallelCollectionBuilds {
		// These instances can not be built now as the limit on maxParallelCollectionBuilds
		// has been reached. Add all the instances to errMap for scheduling their build
		// in the background
		errStr := fmt.Sprintf("Build Already In Progress for %v collections.", currParallelCollectionBuilds)
		logging.Errorf("Indexer::checkParallelCollectionBuilds %v, %v. "+
			"Current collection build map: %v", instIdList, keyspaceId, parallelCollectionBuildMap)
		if idx.enableManager {
			idx.bulkUpdateError(instIdList, errStr)
			for _, instId := range instIdList {
				errMap[instId] = &common.IndexerError{Reason: errStr, Code: common.MaxParallelCollectionBuilds}
			}
			return false
		} else {
			if respCh != nil {
				respCh <- &MsgError{
					err: Error{code: ERROR_MAX_PARALLEL_COLLECTION_BUILDS,
						severity: NORMAL,
						cause:    errors.New(errStr),
						category: INDEXER}}
			}
			return false
		}
	}
	return true
}

// handleCheckDDLInProgress handles MsgCheckDDLInProgress
// (MsgType INDEXER_CHECK_DDL_IN_PROGRESS).
func (idx *indexer) handleCheckDDLInProgress(msg Message) {

	var ddlInProgress bool
	var inProgressIndexNames []string

	ddlMsg := msg.(*MsgCheckDDLInProgress)
	respCh := ddlMsg.GetRespCh()
	if idx.getIndexerState() == common.INDEXER_BOOTSTRAP {
		ddlInProgress, inProgressIndexNames = idx.bsRunParams.ddlRunning, idx.bsRunParams.ddlRunningIndexNames
	} else {
		ddlInProgress, inProgressIndexNames = idx.checkDDLInProgress()
	}
	respCh <- &MsgDDLInProgressResponse{
		ddlInProgress:        ddlInProgress,
		inProgressIndexNames: inProgressIndexNames}

	return
}

// checkDDLInProgress returns true and a slice of index names currently in DDL
// processing if DDL is currently running, else false and an empty slice.
func (idx *indexer) checkDDLInProgress() (bool, []string) {

	ddlInProgress := false
	inProgressIndexNames := make([]string, 0, len(idx.indexInstMap))
	for _, index := range idx.indexInstMap {

		if index.State == common.INDEX_STATE_INITIAL ||
			index.State == common.INDEX_STATE_CATCHUP {
			ddlInProgress = true
			inProgressIndexNames = append(inProgressIndexNames, index.Defn.Bucket+":"+index.Defn.Name)
		}
	}
	return ddlInProgress, inProgressIndexNames
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

func (idx *indexer) handleInitialBuildDone(msg Message) {

	keyspaceId := msg.(*MsgTKInitBuildDone).GetKeyspaceId()
	streamId := msg.(*MsgTKInitBuildDone).GetStreamId()
	sessionId := msg.(*MsgTKInitBuildDone).GetSessionId()
	flushTs := msg.(*MsgTKInitBuildDone).GetFlushTs()

	if ok, currSid := idx.validateSessionId(streamId, keyspaceId, sessionId, true); !ok {
		logging.Infof("Indexer::handleInitialBuildDone StreamId %v KeyspaceId %v SessionId %v. "+
			"Skipped. Current SessionId %v.", streamId, keyspaceId, sessionId, currSid)
		return
	}

	//skip processing initial build done for inactive or recovery streams.
	//the streams would be restarted and then build done would get recomputed.
	state := idx.getStreamKeyspaceIdState(streamId, keyspaceId)

	if state == STREAM_INACTIVE ||
		state == STREAM_PREPARE_RECOVERY ||
		state == STREAM_RECOVERY {
		logging.Infof("Indexer::handleInitialBuildDone Skip InitBuildDone %v %v %v",
			streamId, keyspaceId, state)
		return
	}

	logging.Infof("Indexer::handleInitialBuildDone KeyspaceId: %v Stream: %v SessionId: %v",
		keyspaceId, streamId, sessionId)

	bucket, _, _ := SplitKeyspaceId(keyspaceId)
	mState := idx.getStreamKeyspaceIdState(common.MAINT_STREAM, bucket)

	maintPendStart := idx.streamKeyspaceIdPendStart[common.MAINT_STREAM][bucket]

	//if MAINT_STREAM stream is in recovery or pending start, it cannot be determined if
	//the stream will be active again or not (e.g. all indexes get dropped).
	//add the state to pending build done and process it later
	if mState == STREAM_PREPARE_RECOVERY ||
		mState == STREAM_RECOVERY ||
		maintPendStart {
		idx.processBuildDoneInRecovery(streamId, keyspaceId, sessionId, flushTs)
	} else if mState == STREAM_INACTIVE {
		//if MAINT_STREAM is not running, it needs to be started
		idx.streamKeyspaceIdPendStart[common.MAINT_STREAM][bucket] = true
		idx.processBuildDoneNoCatchup(streamId, keyspaceId, sessionId, flushTs)
	} else {
		idx.processBuildDoneCatchup(streamId, keyspaceId, sessionId, flushTs)
	}

}

func (idx *indexer) processBuildDoneInRecovery(streamId common.StreamId,
	keyspaceId string, sessionId uint64, flushTs *common.TsVbuuid) {

	logging.Infof("Indexer::processBuildDoneInRecovery %v %v %v", streamId, keyspaceId,
		sessionId)

	//save the buildDone information for processing once the recovery finishes
	spec := &buildDoneSpec{
		sessionId: sessionId,
		flushTs:   flushTs,
	}
	idx.streamKeyspaceIdPendBuildDone[streamId][keyspaceId] = spec

}

func (idx *indexer) processPendingBuildDone(streamId common.StreamId,
	bucket string, sessionId uint64) {

	//pending build check is only to be done for MAINT_STREAM
	if streamId != common.MAINT_STREAM {
		return
	}

	logging.Infof("Indexer::processPendingBuildDone %v %v %v", streamId, bucket, sessionId)

	pendStreamId := common.INIT_STREAM

	//consider all keyspaceIds for the bucket
	for keyspaceId, spec := range idx.streamKeyspaceIdPendBuildDone[pendStreamId] {

		if spec != nil &&
			GetBucketFromKeyspaceId(keyspaceId) == bucket {

			logging.Infof("Indexer::processPendingBuildDone Processing %v %v ", pendStreamId, keyspaceId)

			mState := idx.getStreamKeyspaceIdState(streamId, bucket)

			maintPendStart := idx.streamKeyspaceIdPendStart[streamId][bucket]
			if mState == STREAM_PREPARE_RECOVERY ||
				mState == STREAM_RECOVERY ||
				maintPendStart {
				//TODO Collections is this case possible as this function gets called on recovery done?
				logging.Infof("Indexer::processPendingBuildDone %v %v. Maint Stream In %v state."+
					" PendStart %v. Wait for next recovery done to trigger pending build done.",
					pendStreamId, keyspaceId, mState, maintPendStart)
			} else if mState == STREAM_INACTIVE {
				//if MAINT_STREAM is not running, it needs to be started
				idx.streamKeyspaceIdPendStart[streamId][bucket] = true
				idx.processBuildDoneNoCatchup(pendStreamId, keyspaceId, spec.sessionId, spec.flushTs)
				delete(idx.streamKeyspaceIdPendBuildDone[pendStreamId], keyspaceId)
			} else {
				idx.processBuildDoneCatchup(pendStreamId, keyspaceId, spec.sessionId, spec.flushTs)
				delete(idx.streamKeyspaceIdPendBuildDone[pendStreamId], keyspaceId)
			}
			//process one pending build at a time
			break
		}
	}
}

func (idx *indexer) processBuildDoneCatchup(streamId common.StreamId,
	keyspaceId string, sessionId uint64, flushTs *common.TsVbuuid) {

	logging.Infof("Indexer::processBuildDoneCatchup %v %v %v", streamId, keyspaceId,
		sessionId)

	//get the list of indexes for this keyspaceId and stream in INITIAL state
	var indexList []common.IndexInst
	var instIdList []common.IndexInstId
	var bucketUUIDList []string
	for _, index := range idx.indexInstMap {
		if index.Defn.KeyspaceId(index.Stream) == keyspaceId && index.Stream == streamId &&
			index.State == common.INDEX_STATE_INITIAL {
			index.State = common.INDEX_STATE_CATCHUP

			indexList = append(indexList, index)
			instIdList = append(instIdList, index.InstId)
			bucketUUIDList = append(bucketUUIDList, index.Defn.BucketUUID)
		}
	}

	if len(instIdList) == 0 {
		logging.Infof("Indexer::processBuildDoneCatchup Empty IndexList %v %v. Nothing to do.",
			streamId, keyspaceId)
		return
	}

	//update the IndexInstMap
	for _, index := range indexList {
		idx.indexInstMap[index.InstId] = index
	}

	//send updated maps to all workers
	msgUpdateIndexInstMap := idx.newIndexInstMsg(idx.indexInstMap)
	msgUpdateIndexInstMap.AppendUpdatedInsts(indexList)

	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, nil); err != nil {
		common.CrashOnError(err)
	}

	//update index state in metadata
	if err := idx.updateMetaInfoForIndexList(instIdList, true, true, false, false, true, false, false, false, nil); err != nil {
		common.CrashOnError(err)
	}

	// collect progress stats after initial is done or transition to catchup phase. This must be done
	// after updating metaInfo.
	idx.sendProgressStats()

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
				logging.Errorf("Indexer::processBuildDoneCatchup %v", err)
				common.CrashOnError(err)
			}
		}
	}

	//Set the minMergeTs as nil. It will be set by TK_INIT_BUILD_DONE_ACK.
	//The index state change from INITIAL->CATCHUP is done by this function.
	//If any minMergeTs gets set due to prior recovery of MAINT_STREAM, it
	//needs to be reset here. Any minMergeTs set after this point is valid as
	//the index is guaranteed to be picked up by recovery of MAINT_STREAM or
	//AddInstance as part of this function.
	idx.setMinMergeTsForCatchup(streamId, keyspaceId, nil)

	idx.tkCmdCh <- &MsgTKToggleFlush{mType: TK_ENABLE_FLUSH,
		streamId:   streamId,
		keyspaceId: keyspaceId}
	<-idx.tkCmdCh

	respCh := make(MsgChannel)
	stopCh := make(StopChannel)

	//Add index to MAINT_STREAM in Catchup State,
	//so mutations for this index are already in queue to
	//allow convergence with INIT_STREAM.

	//use bucket as keyspaceId for MAINT_STREAM
	bucket := GetBucketFromKeyspaceId(keyspaceId)
	cmd := &MsgStreamUpdate{mType: ADD_INDEX_LIST_TO_STREAM,
		streamId:   common.MAINT_STREAM,
		keyspaceId: bucket,
		indexList:  indexList,
		respCh:     respCh,
		stopCh:     stopCh,
		sessionId:  sessionId}

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

	reqLock := idx.acquireStreamRequestLock(keyspaceId, streamId)
	go func(reqLock *kvRequest) {
		defer idx.releaseStreamRequestLock(reqLock)
		idx.waitStreamRequestLock(reqLock)
		count := 0
	retryloop:
		for {
			if !idx.ValidateKeyspace(common.MAINT_STREAM, bucket, bucketUUIDList) {
				logging.Errorf("Indexer::processBuildDoneCatchup Keyspace Not Found "+
					"For Stream %v KeyspaceId %v SessionId %v", streamId, keyspaceId, sessionId)
				//TODO need to send bucket not found?
				break retryloop
			}
			idx.sendMsgToKVSender(cmd)

			if resp, ok := <-respCh; ok {

				switch resp.GetMsgType() {

				case MSG_SUCCESS:

					idx.injectRandomDelay(10)

					logging.Infof("Indexer::processBuildDoneCatchup Success Stream %v KeyspaceId %v "+
						"SessionId %v", streamId, keyspaceId, sessionId)

					mergeTs := resp.(*MsgStreamUpdate).GetRestartTs()

					idx.internalRecvCh <- &MsgTKInitBuildDone{
						mType:      TK_INIT_BUILD_DONE_ACK,
						streamId:   streamId,
						keyspaceId: keyspaceId,
						mergeTs:    mergeTs,
						sessionId:  sessionId}
					break retryloop

				default:
					//log and retry for all other responses
					respErr := resp.(*MsgError).GetError()
					count++

					if count > MAX_PROJ_RETRY {
						//if the max retry count has been exceeded, check the status of MAINT_STREAM.
						//if MAINT_STREAM is in recovery, retry for more time before recovery finishes.
						//At this point, the index state has already been changed in TK and Indexer maps.
						//But it is unknown if MAINT_STREAM recovery has picked up that state change or not
						//(depending on when it started). If the stream state is active right now, it is okay
						//to generate ADD_FAIL message. If MAINT_STREAM recovery starts after that, it will
						//pick up the state change.
						bucket := GetBucketFromKeyspaceId(keyspaceId)
						mstate := idx.getStreamKeyspaceIdState(common.MAINT_STREAM, bucket)
						if mstate == STREAM_PREPARE_RECOVERY || mstate == STREAM_RECOVERY {
							logging.Infof("Indexer::processBuildDoneCatchup Stream %v KeyspaceId %v SessionId %v"+
								"Detected MAINT_STREAM in %v state. Reset retry count.", streamId,
								keyspaceId, sessionId, mstate)
							count = 0
						} else {
							// Send message to main loop to start recovery if cannot add instances over threshold.
							//If the projector state is not correct, this ensures projector state will get cleaned up.
							logging.Errorf("Indexer::processBuildDoneCatchup Stream %v KeyspaceId %v SessionId %v"+
								"Error from Projector %v. Start recovery after %v retries.", streamId,
								keyspaceId, respErr.cause, sessionId, MAX_PROJ_RETRY)

							idx.internalRecvCh <- &MsgTKInitBuildDone{
								mType:      TK_ADD_INSTANCE_FAIL,
								streamId:   streamId,
								keyspaceId: keyspaceId,
								sessionId:  sessionId,
								flushTs:    flushTs,
							}
							break retryloop
						}
					} else {
						logging.Errorf("Indexer::processBuildDoneCatchup Stream %v KeyspaceId %v SessionId %v."+
							"Error from Projector %v. Retrying %v.", streamId, keyspaceId, sessionId, respErr.cause, count)
						time.Sleep(KV_RETRY_INTERVAL * time.Millisecond)
					}
				}
			}
		}
	}(reqLock)

}

func (idx *indexer) processBuildDoneNoCatchup(streamId common.StreamId,
	keyspaceId string, sessionId uint64, flushTs *common.TsVbuuid) {

	logging.Infof("Indexer::processBuildDoneNoCatchup %v %v %v", streamId, keyspaceId,
		sessionId)

	//get the list of indexes for this keyspaceId and stream in INITIAL state
	var indexList []common.IndexInst
	var instIdList []common.IndexInstId
	var bucketUUIDList []string
	for _, index := range idx.indexInstMap {
		if index.Defn.KeyspaceId(index.Stream) == keyspaceId && index.Stream == streamId &&
			index.State == common.INDEX_STATE_INITIAL {

			index.State = common.INDEX_STATE_ACTIVE
			index.Stream = common.MAINT_STREAM

			indexList = append(indexList, index)
			instIdList = append(instIdList, index.InstId)
			bucketUUIDList = append(bucketUUIDList, index.Defn.BucketUUID)

		}
	}

	if len(instIdList) == 0 {
		logging.Infof("Indexer::processBuildDoneNoCatchup Empty IndexList %v %v. Nothing to do.",
			streamId, keyspaceId)
		return
	}

	//update the IndexInstMap
	for _, index := range indexList {
		idx.indexInstMap[index.InstId] = index
	}

	idx.updateRStateForPendingReset(indexList)

	//send updated maps to all workers
	msgUpdateIndexInstMap := idx.newIndexInstMsg(idx.indexInstMap)
	msgUpdateIndexInstMap.AppendUpdatedInsts(indexList)

	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, nil); err != nil {
		common.CrashOnError(err)
	}

	//update index state in metadata
	if err := idx.updateMetaInfoForIndexList(instIdList, true, true, false, false, true, false, false, false, nil); err != nil {
		common.CrashOnError(err)
	}

	// collect progress stats after initial is done or transition to catchup phase. This must be done
	// after updating metaInfo.
	idx.sendProgressStats()

	respCh := make(MsgChannel)
	stopCh := make(StopChannel)

	cmd := &MsgStreamUpdate{mType: REMOVE_KEYSPACE_FROM_STREAM,
		streamId:      streamId,
		keyspaceId:    keyspaceId,
		respCh:        respCh,
		stopCh:        stopCh,
		abortRecovery: true,
		sessionId:     sessionId,
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
	idx.setStreamKeyspaceIdState(streamId, keyspaceId, STREAM_INACTIVE)
	idx.cleanupStreamKeyspaceIdState(streamId, keyspaceId)
	idx.stats.RemoveKeyspaceStats(streamId, keyspaceId)
	idx.distributeKeyspaceStatsMapsToWorkers()

	reqLock := idx.acquireStreamRequestLock(keyspaceId, streamId)
	go func(reqLock *kvRequest) {
		defer idx.releaseStreamRequestLock(reqLock)
		idx.waitStreamRequestLock(reqLock)
		count := 0
	retryloop:
		for {
			if !idx.ValidateKeyspace(streamId, keyspaceId, bucketUUIDList) {
				logging.Errorf("Indexer::processBuildDoneNoCatchup Keyspace Not Found "+
					"For Stream %v KeyspaceId %v SessionId %v", streamId, keyspaceId, sessionId)
				break retryloop
			}
			idx.sendMsgToKVSender(cmd)

			if resp, ok := <-respCh; ok {

				switch resp.GetMsgType() {

				case MSG_SUCCESS:

					idx.injectRandomDelay(10)

					logging.Infof("Indexer::processBuildDoneNoCatchup Success Stream %v KeyspaceId %v "+
						"SessionId %v", streamId, keyspaceId, sessionId)

					idx.internalRecvCh <- &MsgTKInitBuildDone{
						mType:      TK_INIT_BUILD_DONE_NO_CATCHUP_ACK,
						streamId:   streamId,
						keyspaceId: keyspaceId,
						flushTs:    flushTs,
						sessionId:  sessionId}
					break retryloop

				default:
					//log and retry for all other responses
					respErr := resp.(*MsgError).GetError()
					count++

					logging.Errorf("Indexer::processBuildDoneNoCatchup Stream %v KeyspaceId %v SessionId %v."+
						"Error from Projector %v. Retrying %v.", streamId, keyspaceId, sessionId, respErr.cause, count)
					time.Sleep(KV_RETRY_INTERVAL * time.Millisecond)
				}
			}
		}
	}(reqLock)

}

func (idx *indexer) handleBuildDoneNoCatchupAck(msg Message) {

	streamId := msg.(*MsgTKInitBuildDone).GetStreamId()
	keyspaceId := msg.(*MsgTKInitBuildDone).GetKeyspaceId()
	flushTs := msg.(*MsgTKInitBuildDone).GetFlushTs()

	logging.Infof("Indexer::handleBuildDoneNoCatchupAck %v %v", streamId, keyspaceId)

	newStream := common.MAINT_STREAM
	//MAINT_STREAM runs at bucket level
	bucket, _, _ := SplitKeyspaceId(keyspaceId)
	idx.prepareStreamKeyspaceIdForFreshStart(newStream, bucket)
	sessionId := idx.genNextSessionId(newStream, bucket)

	idx.setStreamKeyspaceIdState(newStream, bucket, STREAM_ACTIVE)
	idx.startKeyspaceIdStream(newStream, bucket, flushTs, nil, nil, false, false, sessionId)
}

func (idx *indexer) handleMergeStream(msg Message) {

	keyspaceId := msg.(*MsgTKMergeStream).GetKeyspaceId()
	streamId := msg.(*MsgTKMergeStream).GetStreamId()
	sessionId := msg.(*MsgTKMergeStream).GetSessionId()

	enableMaintStreamFlush := func(keyspaceId string) {
		bucket := GetBucketFromKeyspaceId(keyspaceId)
		state := idx.getStreamKeyspaceIdState(common.MAINT_STREAM, bucket)

		if state == STREAM_ACTIVE {
			//enable flush for this keyspaceId in MAINT_STREAM
			idx.tkCmdCh <- &MsgTKToggleFlush{mType: TK_ENABLE_FLUSH,
				streamId:          common.MAINT_STREAM,
				keyspaceId:        bucket,
				resetPendingMerge: true}
			<-idx.tkCmdCh
		}
	}

	if ok, currSid := idx.validateSessionId(streamId, keyspaceId, sessionId, true); !ok {
		logging.Infof("Indexer::handleMergeStream StreamId %v KeyspaceId %v SessionId %v. "+
			"Skipped. Current SessionId %v.", streamId, keyspaceId, sessionId, currSid)
		//Timekeeper disables MAINT_STREAM flush before sending the MsgTKMergeStream.
		//It needs to be enabled if this message is being skipped.
		enableMaintStreamFlush(keyspaceId)
		return
	}

	//skip processing stream merge for inactive or recovery streams.
	state := idx.getStreamKeyspaceIdState(streamId, keyspaceId)

	if state == STREAM_INACTIVE ||
		state == STREAM_PREPARE_RECOVERY ||
		state == STREAM_RECOVERY {
		logging.Infof("Indexer::handleMergeStream Skip MergeStream %v %v %v",
			streamId, keyspaceId, state)
		//Timekeeper disables MAINT_STREAM flush before sending the MsgTKMergeStream.
		//It needs to be enabled if this message is being skipped.
		enableMaintStreamFlush(keyspaceId)
		return
	}

	switch streamId {

	case common.INIT_STREAM:
		idx.handleMergeInitStream(msg)

	default:
		logging.Fatalf("Indexer::handleMergeStream Only INIT_STREAM can be merged "+
			"to MAINT_STREAM. Found Stream: %v.", streamId)
		common.CrashOnError(ErrInvalidStream)
	}
}

// handleMergeInitStream switches index maintenance for a keyspaceId from
// one stream to another (e.g. INIT_STREAM or CATCHUP_STREAM to MAINT_STREAM).
func (idx *indexer) handleMergeInitStream(msg Message) {

	keyspaceId := msg.(*MsgTKMergeStream).GetKeyspaceId()
	streamId := msg.(*MsgTKMergeStream).GetStreamId()
	mergeTs := msg.(*MsgTKMergeStream).GetMergeTS()

	sessionId := idx.getCurrentSessionId(streamId, keyspaceId)

	/*//Send FORCE_COMMIT_MERGE message to storage manager to create snapshot.
	//This snapshot allows stale=false scans to proceed.
	if mergeTs != nil && mergeTs.GetSnapType() == common.FORCE_COMMIT_MERGE {
		logging.Infof("Indexer::handleMergeInitStream Forcing snapshot commit for keyspaceId: %v "+
			"Stream: %v SessionId: %v", keyspaceId, streamId, sessionId)

		idx.storageMgrCmdCh <- &MsgMutMgrFlushDone{mType: MUT_MGR_FLUSH_DONE,
			streamId:   streamId,
			keyspaceId: keyspaceId,
			ts:         mergeTs}
		<-idx.storageMgrCmdCh
	}*/

	logging.Infof("Indexer::handleMergeInitStream keyspaceId: %v Stream: %v SessionId: %v",
		keyspaceId, streamId, sessionId)

	//get the list of indexes for this keyspaceId in CATCHUP state
	var indexList []common.IndexInst
	var bucketUUIDList []string
	for _, index := range idx.indexInstMap {
		if index.Defn.KeyspaceId(index.Stream) == keyspaceId && index.Stream == streamId &&
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

	idx.updateRStateForPendingReset(indexList)

	//send updated maps to all workers
	msgUpdateIndexInstMap := idx.newIndexInstMsg(idx.indexInstMap)
	msgUpdateIndexInstMap.AppendUpdatedInsts(indexList)

	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, nil); err != nil {
		common.CrashOnError(err)
	}

	respCh := make(MsgChannel)
	stopCh := make(StopChannel)

	//remove keyspaceId from INIT_STREAM
	var cmd Message
	cmd = &MsgStreamUpdate{mType: REMOVE_KEYSPACE_FROM_STREAM,
		streamId:      streamId,
		keyspaceId:    keyspaceId,
		respCh:        respCh,
		stopCh:        stopCh,
		abortRecovery: true,
		sessionId:     sessionId,
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
	idx.setStreamKeyspaceIdState(streamId, keyspaceId, STREAM_INACTIVE)
	idx.cleanupStreamKeyspaceIdState(streamId, keyspaceId)
	idx.stats.RemoveKeyspaceStats(streamId, keyspaceId)
	idx.distributeKeyspaceStatsMapsToWorkers()

	//enable flush for this keyspaceId in MAINT_STREAM
	bucket := GetBucketFromKeyspaceId(keyspaceId)
	idx.tkCmdCh <- &MsgTKToggleFlush{mType: TK_ENABLE_FLUSH,
		streamId:          common.MAINT_STREAM,
		keyspaceId:        bucket,
		resetPendingMerge: true}
	<-idx.tkCmdCh

	//for cbq bridge, return response after merge is done and
	//index is ready to query
	if !idx.enableManager {
		if clientCh, ok := idx.keyspaceIdCreateClientChMap[keyspaceId]; ok {
			if clientCh != nil {
				clientCh <- &MsgSuccess{}
			}
			delete(idx.keyspaceIdCreateClientChMap, keyspaceId)
		}
	} else {
		var instIdList []common.IndexInstId
		for _, inst := range indexList {
			instIdList = append(instIdList, inst.InstId)
		}

		if err := idx.updateMetaInfoForIndexList(instIdList, true, true,
			false, false, true, false, false, false, nil); err != nil {
			common.CrashOnError(err)
		}
	}

	idx.setStreamKeyspaceIdCurrRequest(streamId, keyspaceId, cmd, stopCh, sessionId)

	reqLock := idx.acquireStreamRequestLock(keyspaceId, streamId)
	go func(reqLock *kvRequest) {
		defer idx.releaseStreamRequestLock(reqLock)
		idx.waitStreamRequestLock(reqLock)
	retryloop:
		for {

			idx.sendMsgToKVSender(cmd)

			if resp, ok := <-respCh; ok {

				switch resp.GetMsgType() {

				case MSG_SUCCESS:

					idx.injectRandomDelay(10)

					logging.Infof("Indexer::handleMergeInitStream Success Stream %v KeyspaceId %v "+
						"SessionId %v", streamId, keyspaceId, sessionId)
					idx.internalRecvCh <- &MsgTKMergeStream{
						mType:      TK_MERGE_STREAM_ACK,
						streamId:   streamId,
						keyspaceId: keyspaceId,
						mergeTs:    mergeTs,
						mergeList:  indexList,
						sessionId:  sessionId,
						reqCh:      stopCh}
					break retryloop

				default:
					//log and retry for all other responses
					respErr := resp.(*MsgError).GetError()
					logging.Errorf("Indexer::handleMergeInitStream Stream %v KeyspaceId %v SessionId %v"+
						"Error from Projector %v. Retrying.", streamId, keyspaceId, sessionId, respErr.cause)
					time.Sleep(KV_RETRY_INTERVAL * time.Millisecond)
				}
			}
		}
	}(reqLock)

	logging.Infof("Indexer::handleMergeInitStream Merge Done KeyspaceId: %v Stream: %v SessionId %v",
		keyspaceId, streamId, sessionId)

}

// cleanupMaintStream cleanes up all the instances in MAINT_STREAM which are in state
// INDEX_STATE_DELETED when INIT_STREAM is in state STREAM_INACTIVE
func (idx *indexer) cleanupMaintStream(keyspaceId string) {
	if idx.getStreamKeyspaceIdState(common.INIT_STREAM, keyspaceId) != STREAM_INACTIVE {
		return
	}

	bucket := GetBucketFromKeyspaceId(keyspaceId)
	maintStreamInstList := idx.getIndexListForKeyspaceIdAndStream(common.MAINT_STREAM, bucket)
	var cleanupInstList []common.IndexInst

	for _, maintStreamInst := range maintStreamInstList {
		if maintStreamInst.State == common.INDEX_STATE_DELETED {
			cleanupInstList = append(cleanupInstList, maintStreamInst)
		}
	}

	for _, cleanupInst := range cleanupInstList {
		logging.Infof("Indexer::cleanupMaintStream Cleaning up instance: %v", cleanupInst.InstId)
		idx.cleanupIndex(cleanupInst, nil)
	}
}

// cleanupEmptyMaintStream cleanes up MAINT_STREAM for a given keyspaceId(bucket)
// if there are no indexes in INIT_STREAM for the bucket
func (idx *indexer) cleanupEmptyMaintStream(bucket string) {

	streamId := common.MAINT_STREAM

	//if MAINT_STREAM is not active, nothing to do
	//recovery will detect that stream doesn't need to be started if there
	//are no eligible indexes
	if idx.getStreamKeyspaceIdState(streamId, bucket) != STREAM_ACTIVE {
		return
	}

	//if there no indexes left on this bucket
	found := false
	for _, indexInst := range idx.indexInstMap {
		if indexInst.Defn.KeyspaceId(streamId) == bucket {
			found = true
			break
		}
	}

	if !found {
		logging.Infof("Indexer::cleanupEmptyMaintStream %v %v. Stop empty stream.", streamId, bucket)
		idx.stopKeyspaceIdStream(streamId, bucket, true)
		idx.setStreamKeyspaceIdState(streamId, bucket, STREAM_INACTIVE)
		idx.cleanupAllStreamKeyspaceIdState(streamId, bucket)
	}

}

// checkKeyspaceIdExistsInStream determines whether the stream has any index we need DCP records for.
func (idx *indexer) checkKeyspaceIdExistsInStream(keyspaceId string, streamId common.StreamId, checkDelete bool) bool {

	//check if any index of the given keyspaceId is in the Stream
	for _, index := range idx.indexInstMap {

		// use checkDelete to verify index in DELETED status.   If an index is dropped while
		// there is concurrent build, the stream will not be cleaned up.
		if index.Defn.KeyspaceId(index.Stream) == keyspaceId && index.Stream == streamId &&
			(index.State == common.INDEX_STATE_ACTIVE ||
				index.State == common.INDEX_STATE_CATCHUP ||
				index.State == common.INDEX_STATE_INITIAL ||
				(index.State == common.INDEX_STATE_DELETED && checkDelete)) {
			return true
		}
	}

	return false

}

//checkLastKeyspaceIdInStream returns true if the given keyspaceId is the only keyspaceId
//active in the given stream, else false
func (idx *indexer) checkLastKeyspaceIdInStream(keyspaceId string, streamId common.StreamId) bool {

	for _, index := range idx.indexInstMap {

		if index.Defn.KeyspaceId(index.Stream) != keyspaceId && index.Stream == streamId &&
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

func (idx *indexer) getIndexListForKeyspaceIdAndStream(streamId common.StreamId,
	keyspaceId string) []common.IndexInst {

	indexList := make([]common.IndexInst, 0)
	for _, idx := range idx.indexInstMap {

		if idx.Stream == streamId && idx.Defn.KeyspaceId(idx.Stream) == keyspaceId {

			indexList = append(indexList, idx)

		}
	}

	return indexList

}

//checkCatchupPendingForStream return true if there is any index in INIT_STREAM and catchup
//state for the input keyspaceId in MAINT_STREAM
func (idx *indexer) checkCatchupPendingForStream(streamId common.StreamId,
	keyspaceId string) bool {

	//catch is only possible for MAINT_STREAM
	if streamId != common.MAINT_STREAM {
		return false
	}

	//check if any index of the given keyspaceId is in the Stream
	for _, index := range idx.indexInstMap {

		if index.Defn.KeyspaceId(index.Stream) == keyspaceId && index.Stream == common.INIT_STREAM &&
			index.State == common.INDEX_STATE_CATCHUP {
			return true
		}
	}

	return false
}

//checkStreamKeyspaceIdInCatchupPhase returns true if the input streamId/keyspaceId
//is in Catchup phase i.e. index in INDEX_STATE_CATCHUP
func (idx *indexer) checkStreamKeyspaceIdInCatchupPhase(streamId common.StreamId,
	keyspaceId string) bool {

	//catch is only possible for INIT_STREAM
	if streamId != common.INIT_STREAM {
		return false
	}

	//check if any index of the given keyspaceId is in the Stream
	for _, index := range idx.indexInstMap {

		if index.Defn.KeyspaceId(index.Stream) == keyspaceId && index.Stream == streamId &&
			index.State == common.INDEX_STATE_CATCHUP {
			return true
		}
	}

	return false
}

// stopKeyspaceIdStream removes the request to receive DCP records of given keyspaceId
// from a given streamId. Used during recovery so does not call cleanupStreamKeyspaceIdState.
func (idx *indexer) stopKeyspaceIdStream(streamId common.StreamId, keyspaceId string, resetKeyspaceStats bool) {

	sessionId := idx.getCurrentSessionId(streamId, keyspaceId)

	logging.Infof("Indexer::stopKeyspaceIdStream Stream: %v KeyspaceId %v SessionId %v",
		streamId, keyspaceId, sessionId)

	idx.merged = idx.removePendingStreamUpdate(idx.merged, streamId, keyspaceId)
	idx.pruned = idx.removePendingStreamUpdate(idx.pruned, streamId, keyspaceId)

	//if the stream is inactive(e.g. all indexes get dropped)
	if idx.getStreamKeyspaceIdState(streamId, keyspaceId) == STREAM_INACTIVE {
		logging.Infof("Indexer::stopKeyspaceIdStream StreamId %v KeyspaceId %v State %v. "+
			"Skip StopKeyspaceIdStream.", streamId, keyspaceId, idx.getStreamKeyspaceIdState(streamId, keyspaceId))
		idx.cleanupStreamKeyspaceIdState(streamId, keyspaceId)
		return
	}

	respCh := make(MsgChannel)
	stopCh := make(StopChannel)

	var cmd Message
	cmd = &MsgStreamUpdate{mType: REMOVE_KEYSPACE_FROM_STREAM,
		streamId:   streamId,
		keyspaceId: keyspaceId,
		respCh:     respCh,
		stopCh:     stopCh,
		sessionId:  sessionId}

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

	if resetKeyspaceStats {
		idx.stats.RemoveKeyspaceStats(streamId, keyspaceId)
		idx.distributeKeyspaceStatsMapsToWorkers()
	}

	idx.setStreamKeyspaceIdCurrRequest(streamId, keyspaceId, cmd, stopCh, sessionId)

	reqLock := idx.acquireStreamRequestLock(keyspaceId, streamId)
	go func(reqLock *kvRequest) {
		defer idx.releaseStreamRequestLock(reqLock)
		idx.waitStreamRequestLock(reqLock)
	retryloop:
		for {

			idx.sendMsgToKVSender(cmd)

			if resp, ok := <-respCh; ok {

				switch resp.GetMsgType() {

				case MSG_SUCCESS:
					idx.injectRandomDelay(10)
					logging.Infof("Indexer::stopKeyspaceIdStream Success Stream %v KeyspaceId %v "+
						"SessionId %v", streamId, keyspaceId, sessionId)
					idx.internalRecvCh <- &MsgRecovery{mType: INDEXER_PREPARE_DONE,
						streamId:   streamId,
						keyspaceId: keyspaceId,
						sessionId:  sessionId,
						requestCh:  stopCh}
					break retryloop

				default:
					//log and retry for all other responses
					respErr := resp.(*MsgError).GetError()
					logging.Errorf("Indexer::stopKeyspaceIdStream Stream %v KeyspaceId %v "+
						"SessionId %v. Error from Projector %v. Retrying.", streamId,
						keyspaceId, sessionId, respErr.cause)
					time.Sleep(KV_RETRY_INTERVAL * time.Millisecond)

				}
			}
		}
	}(reqLock)
}

// startKeyspaceIdStream starts the logical stream for a given keyspaceId (except for the
// case of an index build, where this is done by sendStreamUpdateForBuildIndex instead).
// Used during recovery.
func (idx *indexer) startKeyspaceIdStream(streamId common.StreamId, keyspaceId string,
	restartTs *common.TsVbuuid, retryTs *common.TsVbuuid, allNilSnapsOnWarmup map[string]bool,
	inRepair bool, async bool, sessionId uint64) {

	logging.Infof("Indexer::startKeyspaceIdStream Stream: %v KeyspaceId: %v SessionId %v RestartTS %v",
		streamId, keyspaceId, sessionId, restartTs)

	idx.merged = idx.removePendingStreamUpdate(idx.merged, streamId, keyspaceId)
	idx.pruned = idx.removePendingStreamUpdate(idx.pruned, streamId, keyspaceId)

	var indexList []common.IndexInst
	var bucketUUIDList []string

	switch streamId {

	case common.MAINT_STREAM:

		for _, indexInst := range idx.indexInstMap {

			if indexInst.Defn.KeyspaceId(streamId) == keyspaceId {
				switch indexInst.State {
				case common.INDEX_STATE_ACTIVE:
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
			if indexInst.Defn.KeyspaceId(indexInst.Stream) == keyspaceId &&
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
		logging.Fatalf("Indexer::startKeyspaceIdStream Unsupported StreamId %v", streamId)
		common.CrashOnError(ErrInvalidStream)

	}

	if len(indexList) == 0 {
		logging.Infof("Indexer::startKeyspaceIdStream Nothing to Start. Stream: %v KeyspaceId: %v",
			streamId, keyspaceId)
		idx.setStreamKeyspaceIdState(streamId, keyspaceId, STREAM_INACTIVE)

		//process any pending build done
		idx.processPendingBuildDone(streamId, keyspaceId, sessionId)
		return
	}

	respCh := make(MsgChannel)
	stopCh := make(StopChannel)

	//allow first snap optimization when restarting from 0.
	//this cannot be done when warming up if some indexes have nil snapshots
	//in a keyspaceId while others don't
	allowMarkFirstSnap := false
	if restartTs == nil {
		if allNilSnapsOnWarmup == nil || (allNilSnapsOnWarmup != nil && allNilSnapsOnWarmup[keyspaceId] == true) {
			allowMarkFirstSnap = true
		}
	}

	keyspaceInRecovery := false
	if idx.getStreamKeyspaceIdState(streamId, keyspaceId) == STREAM_RECOVERY {
		keyspaceInRecovery = true
	}

	clustAddr := idx.config["clusterAddr"].String()
	numVb := idx.config["numVbuckets"].Int()
	enableAsync := idx.config["enableAsyncOpenStream"].Bool()

	idx.cinfoProviderLock.RLock()
	clusterVer := idx.cinfoProvider.ClusterVersion()
	idx.cinfoProviderLock.RUnlock()
	if !inRepair {
		async = enableAsync && clusterVer >= common.INDEXER_65_VERSION
	}

	//on warmup, OSO can only be allowed if all snapshots are nil
	allowOSO := false
	if allNilSnapsOnWarmup == nil || (allNilSnapsOnWarmup != nil && allNilSnapsOnWarmup[keyspaceId] == true) {
		allowOSO = true
	}

	enableOSO := idx.config["build.enableOSO"].Bool()
	if enableOSO &&
		allowOSO &&
		clusterVer >= common.INDEXER_71_VERSION &&
		streamId == common.INIT_STREAM {
		enableOSO = true
	} else {
		enableOSO = false
	}

	//if OSO exception has been recorded, disable OSO and use regular mode
	if idx.streamKeyspaceIdOSOException[streamId][keyspaceId] {
		logging.Infof("Indexer::startKeyspaceIdStream %v %v. Disable OSO due to "+
			"exception.", streamId, keyspaceId)
		enableOSO = false

		//clear the OSO exception for regular mode
		logging.Infof("Indexer::startKeyspaceIdStream %v %v. "+
			"Clear OSOException due to stream restart.", streamId, keyspaceId)
		delete(idx.streamKeyspaceIdOSOException[streamId], keyspaceId)
	}

	var cid string
	var ok bool
	if cid, ok = idx.streamKeyspaceIdCollectionId[streamId][keyspaceId]; !ok {
		//if the cid has not been set e.g. in warmup, set it from the first index
		//get cid of any index and determine if it needs to be used
		cid = indexList[0].Defn.CollectionId
		cid = idx.makeCollectionIdForStreamRequest(streamId, keyspaceId, cid, clusterVer)
		idx.streamKeyspaceIdCollectionId[streamId][keyspaceId] = cid
	}

	//Set collectionAware to true unconditionally. DCP allows to enable
	//collections on upgraded nodes in mixed mode.
	collectionAware := true

	cmd := &MsgStreamUpdate{mType: OPEN_STREAM,
		streamId:           streamId,
		keyspaceId:         keyspaceId,
		indexList:          indexList,
		restartTs:          restartTs,
		respCh:             respCh,
		stopCh:             stopCh,
		allowMarkFirstSnap: allowMarkFirstSnap,
		rollbackTime:       idx.keyspaceIdRollbackTimes[keyspaceId],
		keyspaceInRecovery: keyspaceInRecovery,
		async:              async,
		sessionId:          sessionId,
		collectionId:       cid,
		collectionAware:    collectionAware,
		enableOSO:          enableOSO}

	//For recovery of INIT_STREAM, send the stored mergeTs to timekeeper.
	//If indexes are in Catchup state, it will be used for merging to MAINT_STREAM.
	if streamId == common.INIT_STREAM {
		cmd.mergeTs = idx.getMinMergeTsForCatchup(streamId, keyspaceId)
	}

	// Create the corresponding KeyspaceStats object before starting the stream
	idx.stats.AddKeyspaceStats(streamId, keyspaceId)
	idx.distributeKeyspaceStatsMapsToWorkers()

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

	idx.initBuildTsLock(streamId, keyspaceId)
	idx.setStreamKeyspaceIdCurrRequest(streamId, keyspaceId, cmd, stopCh, sessionId)

	reqLock := idx.acquireStreamRequestLock(keyspaceId, streamId)
	go func(reqLock *kvRequest) {
		defer idx.releaseStreamRequestLock(reqLock)
		idx.waitStreamRequestLock(reqLock)
		count := 0
	retryloop:
		for {
			//validate keyspace before every try
			if !idx.ValidateKeyspace(streamId, keyspaceId, bucketUUIDList) {
				logging.Errorf("Indexer::startKeyspaceIdStream Keyspace Not Found "+
					"For Stream %v KeyspaceId %v SessionId %v", streamId, keyspaceId, sessionId)
				idx.internalRecvCh <- &MsgRecovery{mType: INDEXER_KEYSPACE_NOT_FOUND,
					streamId:   streamId,
					keyspaceId: keyspaceId,
					inMTR:      true,
					sessionId:  sessionId}
				break retryloop
			}

			// simplified version of idxDefn.keyspaceId with only the bits necessary here,
			// should not be used generically
			// introduced this code as idxDefn.keyspaceId() would return bucket keyspace without the scope:collection
			// for pre CC indexes where as we always need the _default/_default scope and collection
			// since bucket exists and is validated already
			getKeyspaceId := func(idx *common.IndexDefn) string {
				//index created pre CC will have empty scope/collection, set it to _default
				if idx.Scope == "" && idx.Collection == "" {
					return strings.Join([]string{idx.Bucket, common.DEFAULT_SCOPE, common.DEFAULT_COLLECTION}, ":")
				}
				return strings.Join([]string{idx.Bucket, idx.Scope, idx.Collection}, ":")
			}

			if clusterVer >= common.INDEXER_70_VERSION && streamId == common.MAINT_STREAM {
				validKeyspaceIdMap := make(map[string]bool)
				invalidKeyspaceIdMap := make(map[string]bool)

				for _, indexinst := range indexList {
					keyspaceIdTemp := getKeyspaceId(&indexinst.Defn)
					if _, ok := validKeyspaceIdMap[keyspaceIdTemp]; ok {
						// keyspace is already validated
						continue
					}

					collectionId := indexinst.Defn.CollectionId
					scopeId := indexinst.Defn.ScopeId
					b, s, c := SplitKeyspaceId(keyspaceIdTemp)

					if _, ok := invalidKeyspaceIdMap[keyspaceIdTemp]; !ok {
						idx.cinfoProviderLock.RLock()
						valid := idx.cinfoProvider.ValidateCollectionID(b, s, c, collectionId, false)
						idx.cinfoProviderLock.RUnlock()
						if !valid {
							invalidKeyspaceIdMap[keyspaceIdTemp] = true
							idx.internalRecvCh <- &MsgIndexerDropCollection{
								streamId:     streamId,
								keyspaceId:   keyspaceId,
								scopeId:      scopeId,
								collectionId: collectionId}
						} else {
							validKeyspaceIdMap[keyspaceIdTemp] = true
						}
					}
				}
			}

			idx.sendMsgToKVSender(cmd)

			if resp, ok := <-respCh; ok {

				switch resp.GetMsgType() {

				case MSG_SUCCESS_OPEN_STREAM:

					idx.injectRandomDelay(10)

					logging.Infof("Indexer::startKeyspaceIdStream Success "+
						"Stream %v KeyspaceId %v Cid %v SessionId %v", streamId, keyspaceId, cid, sessionId)

					//once stream request is successful re-calculate the KV timestamp.
					//This makes sure indexer doesn't use a timestamp which can never
					//be caught up to (due to kv rollback).
					//if there is a failover after this, it will be observed as a rollback

					if streamId == common.INIT_STREAM {
						// Asyncronously compute the KV timestamp
						go idx.computeKeyspaceBuildTsAsync(clustAddr, keyspaceId, cid, numVb, streamId)
					}

					idx.internalRecvCh <- &MsgRecovery{mType: INDEXER_RECOVERY_DONE,
						streamId:   streamId,
						keyspaceId: keyspaceId,
						restartTs:  restartTs,
						activeTs:   resp.(*MsgSuccessOpenStream).GetActiveTs(),
						pendingTs:  resp.(*MsgSuccessOpenStream).GetPendingTs(),
						sessionId:  sessionId,
						requestCh:  stopCh,
					}
					break retryloop

				case INDEXER_ROLLBACK:
					logging.Infof("Indexer::startKeyspaceIdStream Rollback from "+
						"Projector For Stream %v KeyspaceId %v SessionId %v", streamId,
						keyspaceId, sessionId)

					//Reset Stream if OSO
					if enableOSO {
						idx.internalRecvCh <- &MsgStreamUpdate{
							mType:      RESET_STREAM,
							streamId:   streamId,
							keyspaceId: keyspaceId,
							sessionId:  sessionId,
							stopCh:     stopCh,
						}

					} else {
						rollbackTs := resp.(*MsgRollback).GetRollbackTs()
						idx.internalRecvCh <- &MsgRecovery{mType: INDEXER_INIT_PREP_RECOVERY,
							streamId:   streamId,
							keyspaceId: keyspaceId,
							restartTs:  rollbackTs,
							retryTs:    retryTs,
							requestCh:  stopCh,
							sessionId:  sessionId}
					}
					break retryloop

				default:
					//log and retry for all other responses
					respErr := resp.(*MsgError).GetError()
					count++

					state := idx.getStreamKeyspaceIdState(streamId, keyspaceId)

					if state == STREAM_PREPARE_RECOVERY || state == STREAM_INACTIVE {
						logging.Errorf("Indexer::startKeyspaceIdStream Stream %v KeyspaceId %v SessionId %v "+
							"Error from Projector %v. Not Retrying. State %v", streamId, keyspaceId,
							sessionId, respErr.cause, state)

						idx.internalRecvCh <- &MsgRecovery{mType: INDEXER_MTR_FAIL,
							streamId:   streamId,
							keyspaceId: keyspaceId,
							inMTR:      true,
							requestCh:  stopCh,
							sessionId:  sessionId}

						break retryloop

					} else if count > MAX_PROJ_RETRY {
						// Start recovery if max retries has reached..  If the projector
						// state is not correct, this ensures projector state will get cleaned up.
						logging.Errorf("Indexer::startKeyspaceIdStream Stream %v KeyspaceId %v SessionId %v. "+
							"Error from Projector %v. Start recovery after %v retries.", streamId,
							keyspaceId, sessionId, respErr.cause, MAX_PROJ_RETRY)

						//Reset Stream if OSO
						if enableOSO {
							idx.internalRecvCh <- &MsgStreamUpdate{
								mType:      RESET_STREAM,
								streamId:   streamId,
								keyspaceId: keyspaceId,
								sessionId:  sessionId,
								stopCh:     stopCh,
							}
						} else {
							idx.internalRecvCh <- &MsgRecovery{
								mType:      INDEXER_INIT_PREP_RECOVERY,
								streamId:   streamId,
								keyspaceId: keyspaceId,
								requestCh:  stopCh,
								sessionId:  sessionId,
							}
						}
						break retryloop
					} else {
						logging.Errorf("Indexer::startKeyspaceIdStream Stream %v KeyspaceId %v "+
							"SessionId %v. Error from Projector %v. Retrying %v.", streamId, keyspaceId,
							sessionId, respErr.cause, count)
						time.Sleep(KV_RETRY_INTERVAL * time.Millisecond)
					}
				}
			}
		}
	}(reqLock)
}

func (idx *indexer) processRollback(streamId common.StreamId,
	keyspaceId string, rollbackTs *common.TsVbuuid, sessionId uint64) {

	if streamId == common.MAINT_STREAM {
		idx.keyspaceIdRollbackTimes[keyspaceId] = time.Now().UnixNano()
	}

	//send to storage manager to rollback
	msg := &MsgRollback{streamId: streamId,
		keyspaceId:   keyspaceId,
		rollbackTs:   rollbackTs,
		rollbackTime: idx.keyspaceIdRollbackTimes[keyspaceId],
		sessionId:    sessionId}

	if streamId == common.MAINT_STREAM {
		idx.scanCoordCmdCh <- msg
		<-idx.scanCoordCmdCh
	}

	idx.storageMgrCmdCh <- msg
	<-idx.storageMgrCmdCh

}

//helper function to init streamFlush map for all streams
func (idx *indexer) initStreamFlushMap() {

	for i := 0; i < int(common.ALL_STREAMS); i++ {
		idx.streamKeyspaceIdFlushInProgress[common.StreamId(i)] = make(KeyspaceIdFlushInProgressMap)
		idx.streamKeyspaceIdObserveFlushDone[common.StreamId(i)] = make(KeyspaceIdObserveFlushDoneMap)
	}
}

func (idx *indexer) initStreamSessionIdMap() {

	for i := 0; i < int(common.ALL_STREAMS); i++ {
		idx.streamKeyspaceIdSessionId[common.StreamId(i)] = make(map[string]uint64)
	}
}

func (idx *indexer) initStreamPendBuildDone() {

	for i := 0; i < int(common.ALL_STREAMS); i++ {
		idx.streamKeyspaceIdPendBuildDone[common.StreamId(i)] = make(map[string]*buildDoneSpec)
		idx.streamKeyspaceIdPendStart[common.StreamId(i)] = make(map[string]bool)
		idx.streamKeyspaceIdPendCollectionDrop[common.StreamId(i)] = make(map[string][]common.IndexInstId)
	}
}

func (idx *indexer) initStreamCollectionIdMap() {

	for i := 0; i < int(common.ALL_STREAMS); i++ {
		idx.streamKeyspaceIdCollectionId[common.StreamId(i)] = make(map[string]string)
		idx.streamKeyspaceIdOSOException[common.StreamId(i)] = make(map[string]bool)
	}
}

func (idx *indexer) notifyFlushObserver(msg Message) {

	//if there is any observer for flush, notify
	keyspaceId := msg.(*MsgMutMgrFlushDone).GetKeyspaceId()
	streamId := msg.(*MsgMutMgrFlushDone).GetStreamId()

	if notifyCh, ok := idx.streamKeyspaceIdObserveFlushDone[streamId][keyspaceId]; ok {
		if notifyCh != nil {
			notifyCh <- msg
			//wait for a sync response that cleanup is done.
			//notification is sent one by one as there is no lock
			<-notifyCh
		}
	}

	if streamId == common.INIT_STREAM {
		if notifyCh, ok := idx.keyspaceIdObserveFlushDoneForReset[keyspaceId]; ok {
			if notifyCh != nil {
				notifyCh <- msg
				//wait for a sync response that reset is done.
				<-notifyCh
			}
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
	keyspaceId := indexInst.Defn.KeyspaceId(streamId)
	idx.streamKeyspaceIdObserveFlushDone[streamId][keyspaceId] = nil

	//indicate done
	close(notifyCh)
}

func (idx *indexer) checkDuplicateDropRequest(indexInst common.IndexInst,
	respCh MsgChannel) bool {

	//if there is any observer for flush done for this stream/keyspaceId,
	//drop is already in progress
	stream := indexInst.Stream
	keyspaceId := indexInst.Defn.KeyspaceId(stream)
	if obs, ok := idx.streamKeyspaceIdObserveFlushDone[stream][keyspaceId]; ok && obs != nil {

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

func (idx *indexer) bootstrap1(snapshotNotifych []chan IndexSnapshot, snapshotReqCh []MsgChannel) error {

	logging.Infof("Indexer::indexer version %v", common.INDEXER_CUR_VERSION)
	idx.genIndexerId()

	idx.recoverRebalanceState()

	start := time.Now()
	err := idx.recoverIndexInstMap()
	if err != nil {
		logging.Fatalf("Indexer::initFromPersistedState Error Recovering IndexInstMap %v", err)
	}
	logging.Infof("Indexer::initFromPersistedState Recovered IndexInstMap %v, elapsed: %v", idx.indexInstMap, time.Since(start))

	idx.bsRunParams.ddlRunning, idx.bsRunParams.ddlRunningIndexNames = idx.checkDDLInProgress()

	go func() {
		//set topic names based on indexer id
		idx.initStreamTopicName()

		//close any old streams with projector
		idx.closeAllStreams()

		idx.validateIndexInstMap()

		// Cleanup orphan indexes, if any.
		idx.cleanupOrphanIndexes()

		// Upgrade storage depending on the storage mode of the indexes residing on this node.
		// This step does not depend on the cluster storage mode (from metakv).   The indexer
		// may need to restart if the bootstrap storage mode is different than the storage mode of
		// the upgraded indexes.
		needsRestart := idx.upgradeStorage()

		//Start Storage Manager
		var res Message
		stats := idx.stats.Clone()
		idx.storageMgr, res = NewStorageManager(idx.storageMgrCmdCh, idx.wrkrRecvCh,
			idx.indexPartnMap, idx.config, snapshotNotifych, snapshotReqCh, stats)
		if res.GetMsgType() == MSG_ERROR {
			err := res.(*MsgError).GetError()
			logging.Fatalf("Indexer::NewIndexer Storage Manager Init Error %v", err)
			idx.internalRecvCh <- &MsgStorageWarmupDone{err: err.cause, needsRestart: needsRestart}
			return
		}

		//Recover indexes from local metadata.
		err = idx.initFromPersistedState()
		if err != nil {
			//sending error will cause indexer to restart
			idx.internalRecvCh <- &MsgStorageWarmupDone{err: err, needsRestart: needsRestart}
			return
		}

		idx.internalRecvCh <- &MsgStorageWarmupDone{err: err, needsRestart: needsRestart}
	}()

	return nil

}

//if any index in MAINT_STREAM has nil snapshot, it needs
//to be reset. Either:
//1. The index was able to clear its snapshot on rollback
//but couldn't reset the metadata before crash.
//2. The index never created a disk snapshot as the disk
//snapshot happens only at 10mins interval.
func (idx *indexer) findAndResetEmptySnapshotIndex() common.IndexInstList {
	updatedInsts := make(common.IndexInstList, 0)

	for instId, index := range idx.indexInstMap {

		if index.Stream == common.MAINT_STREAM &&
			index.State != common.INDEX_STATE_DELETED {

			//ignore proxy instances as rebalancer will clean those up
			if common.IsPartitioned(index.Defn.PartitionScheme) && index.RealInstId != 0 {
				continue
			}

			anyPartnNonNil := false
			anyPartnNil := false
			if partnMap, ok := idx.indexPartnMap[instId]; ok {

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
					if latestSnapInfo != nil {
						anyPartnNonNil = true
					} else {
						anyPartnNil = true
					}
				}
			}

			//if all partns are nil
			if anyPartnNil && !anyPartnNonNil {
				idx.resetSingleIndexOnRollback(&index, nil)
				idx.indexInstMap[instId] = index
				updatedInsts = append(updatedInsts, index)
			}
		}
	}
	return updatedInsts
}

func (idx *indexer) createRealInstIdMap() common.IndexInstMap {
	realInstIdMap := make(common.IndexInstMap)
	for _, inst := range idx.indexInstMap {
		if inst.IsProxy() {
			newInst := inst
			newInst.Pc = inst.Pc.Clone()
			realInstIdMap[inst.RealInstId] = newInst
		}
	}
	return realInstIdMap
}

func (idx *indexer) cleanupOrphanIndexes() {
	storageDir := idx.config["storage_dir"].String()

	mode := idx.getLocalStorageMode(idx.config)
	flist, err := ListSlices(mode, storageDir)
	if err != nil {
		logging.Warnf("Error %v during cleaning up the orphan indexes.", err)
		return
	}

	instExists := func(instId common.IndexInstId,
		partnId common.PartitionId, m common.IndexInstMap) bool {

		if inst, ok := m[instId]; !ok {
			// Orphan Index Instance
			return false
		} else {
			if exists := inst.Pc.CheckPartitionExists(partnId); !exists {
				// Orphan Partition Instance
				return false
			}
		}
		return true
	}

	realInstIdMap := idx.createRealInstIdMap()

	stDirPathLen := len(storageDir) + len(string(os.PathSeparator))
	orphanIndexList := make([]string, 0, len(flist))
	for _, f := range flist {
		instId, partnId, err := GetInstIdPartnIdFromPath(f[stDirPathLen:])
		if err != nil {
			logging.Warnf("Error %v during GetInstIdPartnIdFromPath for %v.", err, f)
			continue
		}

		// Check if instId, partnId exists
		if instExists(instId, partnId, idx.indexInstMap) {
			continue
		}

		// Check if realInstId, partnId exists
		if instExists(instId, partnId, realInstIdMap) {
			continue
		}

		logging.Infof("Found orphan index slice %v. Scheduling it for cleanup.", f)
		orphanIndexList = append(orphanIndexList, f)
	}

	for _, f := range orphanIndexList {
		if err := DestroySlice(mode, storageDir, f); err != nil {
			logging.Warnf("Error %v while removing orphan index data for %v.", err, f)
		} else {
			logging.Infof("Cleaned up the orphan index slice %v.", f)
		}
	}
}

func (idx *indexer) handleStorageWarmupDone(msg Message) {

	err := msg.(*MsgStorageWarmupDone).GetError()
	needsRestart := msg.(*MsgStorageWarmupDone).NeedsRestart()

	if err != nil {
		logging.Fatalf("Indexer::Unable to Bootstrap Indexer from Persisted Metadata %v", err)
		common.CrashOnError(err)
	}

	if needsRestart {
		logging.Infof("Restarting indexer after storage upgrade")
		idx.stats.needsRestart.Set(true)
		os.Exit(0)
	}

	msgUpdateIndexInstMap := idx.newIndexInstMsg(idx.indexInstMap)
	msgUpdateIndexPartnMap := &MsgUpdatePartnMap{indexPartnMap: idx.indexPartnMap}

	resetOnRollback := idx.config["recovery.reset_index_on_rollback"].Bool()

	if resetOnRollback {

		//any index with nil snapshot should be moved to INIT_STREAM
		//TODO optimize for case where keyspace has 0 documents
		updatedInsts := idx.findAndResetEmptySnapshotIndex()

		//send updated maps
		msgUpdateIndexInstMap = idx.newIndexInstMsg(idx.indexInstMap)
		msgUpdateIndexInstMap.AppendUpdatedInsts(updatedInsts)
		msgUpdateIndexPartnMap = &MsgUpdatePartnMap{indexPartnMap: idx.indexPartnMap}
	}

	// Distribute current stats object and index information
	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, msgUpdateIndexPartnMap); err != nil {
		common.CrashOnError(err)
	}

	err = idx.bootstrap2()
	if err != nil {
		common.CrashOnError(err)
	}

	if idx.getIndexerState() == common.INDEXER_BOOTSTRAP {
		idx.setIndexerState(common.INDEXER_ACTIVE)
		idx.stats.indexerState.Set(int64(common.INDEXER_ACTIVE))

		// notify storage manager that indexer has become active
		idx.storageMgrCmdCh <- &MsgIndexerState{mType: INDEXER_ACTIVE}
		<-idx.storageMgrCmdCh
	}

	idx.scanCoordCmdCh <- &MsgIndexerState{mType: INDEXER_RESUME, rollbackTimes: idx.keyspaceIdRollbackTimes}
	<-idx.scanCoordCmdCh

	// Persist node uuid in Metadata store
	clustMgrMsg := &MsgClustMgrLocal{
		mType: CLUST_MGR_SET_LOCAL,
		key:   INDEXER_NODE_UUID,
		value: idx.config["nodeuuid"].String(),
	}

	respMsg, _ := idx.sendMsgToClustMgr(clustMgrMsg)
	resp := respMsg.(*MsgClustMgrLocal)

	errMsg := resp.GetError()
	if errMsg != nil {
		logging.Fatalf("Indexer::NewIndexer Unable to set INDEXER_NODE_UUID In Local"+
			"Meta Storage. Err %v", errMsg)
		common.CrashOnError(errMsg)
	}

	logging.Infof("Indexer::NewIndexer Status %v", idx.getIndexerState())

	// Initialize the public REST API server after indexer bootstrap is completed
	NewRestServer(idx.config["clusterAddr"].String(), idx.statsMgr)

	go idx.monitorMemUsage()
	go idx.logMemstats()
	go idx.collectProgressStats(true)

	idx.statsMgrCmdCh <- &MsgStatsPersister{
		mType: STATS_PERSISTER_START,
	}
	<-idx.statsMgrCmdCh
}

func (idx *indexer) handleReadPersistedStats(msg Message) {
	respCh := msg.(*MsgStatsPersister).GetResponseChannel()
	idx.statsMgrCmdCh <- msg
	<-idx.statsMgrCmdCh
	respCh <- true
}

func (idx *indexer) bootstrap2() error {

	if common.GetStorageMode() == common.MOI {
		clustMgrMsg := &MsgClustMgrLocal{
			mType: CLUST_MGR_GET_LOCAL,
			key:   INDEXER_STATE_KEY,
		}

		respMsg, _ := idx.sendMsgToClustMgr(clustMgrMsg)
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
		memory_quota := idx.config.GetIndexerMemoryQuota()
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
	if err := idx.sendMsgToClustMgrAndProcessResponse(msg); err != nil {
		return err
	}

	//send Ready to Settings Manager
	if resp := idx.sendStreamUpdateToWorker(msg, idx.settingsMgrCmdCh,
		"SettingsMgr"); resp.GetMsgType() != MSG_SUCCESS {
		return resp.(*MsgError).GetError().cause
	}

	//send Ready to Rebalance Manager
	if resp := idx.sendStreamUpdateToWorker(msg, idx.rebalMgrCmdCh,
		"RebalanceMgr"); resp.GetMsgType() != MSG_SUCCESS {
		return resp.(*MsgError).GetError().cause
	}

	//if there are no indexes, return from here
	if len(idx.indexInstMap) == 0 {
		return nil
	}

	if ok := idx.startStreams(); !ok {
		return errors.New("Unable To Start DCP Streams")
	}

	return nil
}

func (idx *indexer) recoverRebalanceState() {

	clustMgrMsg := &MsgClustMgrLocal{
		mType: CLUST_MGR_GET_LOCAL,
		key:   RebalanceRunning,
	}

	respMsg, _ := idx.sendMsgToClustMgr(clustMgrMsg)
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

	clustMgrMsg = &MsgClustMgrLocal{
		mType: CLUST_MGR_GET_LOCAL,
		key:   RebalanceTokenTag,
	}

	respMsg, _ = idx.sendMsgToClustMgr(clustMgrMsg)
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

func (idx *indexer) handleAddIndexInstanceAtWorker(msg Message) {
	req := msg.(*MsgAddIndexInst)
	workerCh := req.GetWorkerCh()
	workerStr := req.GetWorkerStr()
	respCh := req.GetRespCh()

	err := idx.sendMessageToWorker(msg, workerCh, workerStr)
	respCh <- err
}

func (idx *indexer) handleUpdateMapToWorker(msg Message) {
	req := msg.(*MsgUpdateWorker)
	workerCh := req.GetWorkerCh()
	workerStr := req.GetWorkerStr()
	instMap := req.GetIndexInstMap()
	partnMap := req.GetIndexPartnMap()
	respCh := req.GetRespCh()

	//send updated maps
	msgUpdateIndexInstMap := idx.newIndexInstMsg(instMap)
	msgUpdateIndexPartnMap := &MsgUpdatePartnMap{indexPartnMap: partnMap}

	err := idx.sendUpdatedIndexMapToWorker(msgUpdateIndexInstMap, msgUpdateIndexPartnMap, workerCh, workerStr)
	respCh <- err
}

func (idx *indexer) handleUpdateBuildTs(msg Message) {
	keyspaceId := msg.(*MsgStreamUpdate).GetKeyspaceId()
	buildTs := msg.(*MsgStreamUpdate).GetTimestamp()
	streamId := msg.(*MsgStreamUpdate).GetStreamId()

	if buildTs != nil {
		idx.keyspaceIdBuildTs[keyspaceId] = buildTs

		streamState := idx.getStreamKeyspaceIdState(streamId, keyspaceId)
		if streamState != STREAM_INACTIVE {
			// Update timekeeper with buildTs
			idx.tkCmdCh <- msg
			<-idx.tkCmdCh
		} else {
			logging.Infof("Indexer::handleUpdateBuildTs Skipping updateBuildTs message to "+
				"timekeeper as stream: %v is in state: %v for keyspaceId: %v", streamId, streamState, keyspaceId)
		}
	}
}

func (idx *indexer) genIndexerId() {

	if idx.enableManager {

		//try to fetch IndexerId from manager
		clustMgrMsg := &MsgClustMgrLocal{
			mType: CLUST_MGR_GET_LOCAL,
			key:   INDEXER_ID_KEY,
		}

		respMsg, _ := idx.sendMsgToClustMgr(clustMgrMsg)
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
			clustMgrMsg := &MsgClustMgrLocal{
				mType: CLUST_MGR_SET_LOCAL,
				key:   INDEXER_ID_KEY,
				value: idx.id,
			}

			respMsg, _ := idx.sendMsgToClustMgr(clustMgrMsg)
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

func (idx *indexer) initFromPersistedState() error {

	// Set the storage mode specific to this indexer node
	common.SetStorageMode(idx.getLocalStorageMode(idx.config))
	initBufPools(idx.config)
	logging.Infof("Indexer::local storage mode %v", common.GetStorageMode().String())

	bootstrapStats := NewIndexerStats()

	// Initialize stats objects and update stats from persistence
	for _, inst := range idx.indexInstMap {
		if inst.State != common.INDEX_STATE_DELETED {
			for _, partnDefn := range inst.Pc.GetAllPartitions() {
				idx.stats.AddPartitionStats(inst, partnDefn.GetPartitionId())
			}

			// Initialise keyspace stats for this stream
			stream := inst.Stream
			keyspaceId := inst.Defn.KeyspaceId(stream)
			if stream == common.INIT_STREAM || stream == common.MAINT_STREAM {
				idx.stats.AddKeyspaceStats(stream, keyspaceId)
			}
		}
	}

	// Stats that are initialized in previous loop
	// need to be populated with values from persistence store
	idx.updateStatsFromPersistence()

	localIndexInstMap := make(common.IndexInstMap)
	localIndexPartnMap := make(IndexPartnMap)

	for _, inst := range idx.indexInstMap {

		for _, partnDefn := range inst.Pc.GetAllPartitions() {
			// Since bootstrapStats does not have index stats yet, initialize index and partition stats
			bootstrapStats.AddPartitionStats(inst, partnDefn.GetPartitionId())
		}

		//allocate partition/slice
		var partnInstMap PartitionInstMap
		var failedPartnInstances PartitionInstMap
		var err error
		if partnInstMap, failedPartnInstances, err = idx.initPartnInstance(inst, nil, true); err != nil {
			return err
		}

		// Cleanup all partition instances for which, initPartnInstance has failed due to storage corruption
		for failedPartnId, failedPartnInstance := range failedPartnInstances {
			logMsg := "Detected storage corruption for index %v, partition id %v. Starting cleanup."
			common.Console(idx.config["clusterAddr"].String(), logMsg, inst.Defn.Name, failedPartnId)

			logging.Infof("Indexer::initFromPersistedState Starting cleanup for %v", failedPartnInstance)
			// Can this return an error?
			idx.forceCleanupIndexPartition(&inst, failedPartnId, failedPartnInstance)
			logging.Infof("Indexer::initFromPersistedState Done cleanup for %v", failedPartnInstance)

			logMsg = "Cleanup done for index %v, partition id %v."
			common.Console(idx.config["clusterAddr"].String(), logMsg, inst.Defn.Name, failedPartnId)
		}

		// If there are no partitions left, don't add this index instance to the indexInstMap
		if len(failedPartnInstances) != 0 && len(inst.Pc.GetAllPartitions()) == 0 {
			logging.Infof("Indexer::initFromPersistedState Skipping index instance %v", inst.InstId)
			idx.stats.RemoveIndexStats(inst)
			delete(idx.indexInstMap, inst.InstId)
			delete(idx.indexPartnMap, inst.InstId)
			continue
		}

		idx.indexInstMap[inst.InstId] = inst
		idx.indexPartnMap[inst.InstId] = partnInstMap

		localIndexInstMap[inst.InstId] = inst
		localIndexPartnMap[inst.InstId] = partnInstMap

		// update index maps in storage manager
		// Note: Unlike scan coordinator, storage manager can not incrementally update
		// indexInstMap and indexPartnMap. This is because stale=ok scans might request
		// for a snapshot while inst update is in progress. This can lead to concurrent
		// map access violation. Hence, storage manager has to clone the entire map, update
		// the instance in the clone and update the original instance maps. It works for
		// scan coordinator as the instance updates and reads are mutex protected
		err = idx.sendInstMapToWorker(idx.storageMgrCmdCh, "StorageMgr", localIndexInstMap, localIndexPartnMap)
		if err != nil { // continue in case of error
			continue
		}

		if common.GetStorageMode() == common.MOI {
			respCh := make(chan bool)

			idx.internalRecvCh <- &MsgUpdateSnapMap{
				idxInstId:  inst.InstId,
				idxInst:    inst,
				partnMap:   partnInstMap,
				streamId:   common.ALL_STREAMS,
				keyspaceId: "",
				respch:     respCh,
			}
			<-respCh

		} else {
			idx.internalRecvCh <- &MsgUpdateSnapMap{
				idxInstId: inst.InstId,
				idxInst:   inst,
				partnMap:  partnInstMap,
				streamId:  common.ALL_STREAMS,
				//TODO Collections verify this will work
				keyspaceId: "",
			}
		}

		idx.initializeBootstrapStats(bootstrapStats, inst.InstId)
		//update index maps in scan coordinator
		err = idx.addInstAtWorker(idx.scanCoordCmdCh, "ScanCoordinator", inst, partnInstMap, bootstrapStats)
		if err != nil { // continue in case of error
			continue
		}

		idx.updateBootstrapStats(bootstrapStats, inst.InstId)
	}

	return nil
}

// Send a message to stats manager to retrieve stats from
// persisted state and wait for it to complete
func (idx *indexer) updateStatsFromPersistence() {
	respCh := make(chan bool)
	idx.internalRecvCh <- &MsgStatsPersister{
		mType:  STATS_READ_PERSISTED_STATS,
		stats:  idx.stats,
		respCh: respCh}
	<-respCh

}

func (idx *indexer) sendInstMapToWorker(wCh MsgChannel, wStr string,
	instMap common.IndexInstMap, partnMap IndexPartnMap) error {

	respCh := make(chan error)
	idx.internalRecvCh <- &MsgUpdateWorker{
		workerCh:      wCh,
		workerStr:     wStr,
		indexInstMap:  instMap,
		indexPartnMap: partnMap,
		respCh:        respCh,
	}
	err := <-respCh
	return err
}

func (idx *indexer) addInstAtWorker(wCh MsgChannel, wStr string,
	indexInst common.IndexInst, instPartns PartitionInstMap, stats *IndexerStats) error {

	instClone := indexInst
	instClone.Pc = indexInst.Pc.Clone()

	instPartnsClone := make(PartitionInstMap)
	for k, v := range instPartns {
		instPartnsClone[k] = v
	}

	idxStats := stats.indexes[indexInst.InstId]
	if idxStats != nil {
		idxStats = idxStats.clone()
	}

	respCh := make(chan error)
	idx.internalRecvCh <- &MsgAddIndexInst{
		workerCh:   wCh,
		workerStr:  wStr,
		indexInst:  instClone,
		instPartns: instPartnsClone,
		stats:      idxStats,
		respCh:     respCh,
	}
	err := <-respCh
	return err
}

// initialize bootstrap stats
func (idx *indexer) initializeBootstrapStats(stats *IndexerStats,
	id common.IndexInstId) {

	idxStats := stats.indexes[id]

	state := idx.indexInstMap[id].State
	idxStats.indexState.Set((uint64)(state))

	idxStats.numDocsPending.Set(math.MaxInt64)
	idxStats.numDocsQueued.Set(math.MaxInt64)
	idxStats.lastRollbackTime.Set(time.Now().UnixNano())
	idxStats.progressStatTime.Set(time.Now().UnixNano())
	stats.indexes[id] = idxStats
}

// broadcast stats to clients
func (idx *indexer) updateBootstrapStats(stats *IndexerStats,
	id common.IndexInstId) {

	idxStats := stats.indexes[id]
	statsForIndex := make(common.Statistics)
	statsForIndex[fmt.Sprintf("%v", id)] = idxStats
	// Marshall stats to byte slice

	idx.internalRecvCh <- &MsgStatsRequest{
		mType: INDEX_BOOTSTRAP_STATS_UPDATE,
		stats: statsForIndex,
	}
}

// broadcast stats to clients
func (idx *indexer) broadcastBootstrapStats(stats *IndexerStats,
	id common.IndexInstId) {

	idxStats := stats.indexes[id]

	state := idx.indexInstMap[id].State
	idxStats.indexState.Set((uint64)(state))

	idxStats.numDocsPending.Set(math.MaxInt64)
	idxStats.numDocsQueued.Set(math.MaxInt64)
	idxStats.lastRollbackTime.Set(time.Now().UnixNano())
	idxStats.progressStatTime.Set(time.Now().UnixNano())
	// Marshall stats to byte slice
	spec := NewStatsSpec(false, false, false, false, false, nil)
	spec.OverrideFilter("gsiClient")
	notifyStats := stats.GetStats(spec)
	if val, ok := notifyStats.(map[string]interface{}); ok {
		idx.internalRecvCh <- &MsgStatsRequest{
			mType: INDEX_STATS_BROADCAST,
			stats: val,
		}
	} else {
		logging.Fatalf("Indexer::broadcastBootstrapStats, Invalid stats type for spec: %v", spec)
	}
}

// "move" the data files from original location to backup location.
// return true if any error has occured during backup and cleanup is needed.
// return false if "move" is successful and no need to cleanup data.
func (idx *indexer) backupCorruptIndexDataFiles(indexInst *common.IndexInst,
	partnId common.PartitionId, sliceId SliceId) (needsDataCleanup bool) {
	logging.Infof("Indexer::backupCorruptIndexDataFiles %v %v take backup of corrupt data files",
		indexInst.InstId, partnId)

	if idx.config["settings.corrupt_index_num_backups"].Int() < 1 {
		logging.Infof("Indexer::backupCorruptIndexDataFiles %v %v no need to backup as num backups is < 1",
			indexInst.InstId, partnId)
		needsDataCleanup = true
		return
	}

	storageDir := idx.config["storage_dir"].String()
	corruptDataDir := filepath.Join(storageDir, CORRUPT_DATA_SUBDIR)
	if err := iowrap.Os_MkdirAll(corruptDataDir, 0755); err != nil {
		logging.Errorf("Indexer::backupCorruptIndexDataFiles %v %v error %v while taking backup:MkdirAll %v",
			indexInst.InstId, partnId, err, corruptDataDir)
		needsDataCleanup = true
		return
	}

	err := MoveSlice(common.IndexTypeToStorageMode(indexInst.Defn.Using), indexInst, partnId, sliceId,
		storageDir, storageDir, corruptDataDir)
	if err != nil {
		needsDataCleanup = true
		return
	}

	needsDataCleanup = false
	return
}

// Force cleanup on index partition.
// This needs to be called only during bootstrap.
func (idx *indexer) forceCleanupIndexPartition(indexInst *common.IndexInst,
	partnId common.PartitionId, partnInst PartitionInst) {

	// mark metadata
	logging.Infof("Indexer::forceCleanupIndexPartition %v %v mark metadata as deleted", indexInst.InstId, partnId)

	msg := &MsgClustMgrCleanupPartition{
		defn:             indexInst.Defn,
		instId:           indexInst.InstId,
		replicaId:        indexInst.ReplicaId,
		partnId:          partnInst.Defn.GetPartitionId(),
		updateStatusOnly: true,
	}

	if err := idx.sendMsgToClustMgrAndProcessResponse(msg); err != nil {
		logging.Errorf("Indexer::forceCleanupIndexPartition %v %v Got error %v in marking metadata as deleted",
			indexInst.InstId, partnId, err)
		common.CrashOnError(err)
	}

	//cleanup the disk directory
	logging.Infof("Indexer::forceCleanupIndexPartition Cleaning up data files for %v %v",
		indexInst.InstId, partnId)

	// backup the corrupt index data files, if enabled
	needsDataCleanup := true
	if idx.config["settings.enable_corrupt_index_backup"].Bool() {
		needsDataCleanup = idx.backupCorruptIndexDataFiles(indexInst, partnId, SliceId(0))
	}

	if needsDataCleanup {
		if err := idx.forceCleanupPartitionData(indexInst, partnId, SliceId(0)); err != nil {
			logging.Infof("Indexer::forceCleanupIndexPartition Error (%v) in cleaning up data files for %v %v",
				err, indexInst.InstId, partnId)
		}
	}

	// cleanup partition internal data structure
	logging.Infof("Indexer::forceCleanupIndexPartition %v %v Cleanup partition in-memory data structure",
		indexInst.InstId, partnId)

	indexInst.Pc.RemovePartition(partnId)
	idx.stats.RemovePartitionStats(indexInst.InstId, partnId)

	// delete metadata
	logging.Infof("Indexer::forceCleanupIndexPartition %v %v actually delete metadata", indexInst.InstId, partnId)

	msg = &MsgClustMgrCleanupPartition{
		defn:      indexInst.Defn,
		instId:    indexInst.InstId,
		replicaId: indexInst.ReplicaId,
		partnId:   partnInst.Defn.GetPartitionId(),
	}

	if err := idx.sendMsgToClustMgrAndProcessResponse(msg); err != nil {
		logging.Errorf("Indexer::forceCleanupIndexPartition %v %v Got error %v in deleting metadata. "+
			"Metadata will be deleted on next indexer restart.", indexInst.InstId, partnId, err)
	}
}

func (idx *indexer) recoverIndexInstMap() error {

	if idx.enableManager {
		return idx.recoverInstMapFromManager()
	} else {
		return idx.recoverInstMapFromFile()
	}

}

func (idx *indexer) recoverInstMapFromManager() error {

	clustMgrMsg := &MsgClustMgrTopology{}

	resp, _ := idx.sendMsgToClustMgr(clustMgrMsg)

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
		if err := DestroySlice(common.IndexTypeToStorageMode(inst.Defn.Using), storage_dir, path); err != nil {
			common.CrashOnError(err)
		}
	}

	// update metadata
	msg := &MsgClustMgrResetIndexOnUpgrade{
		inst: *inst,
	}
	idx.sendMsgToClustMgrAndProcessResponse(msg)
}

func (idx *indexer) validateIndexInstMap() {

	bucketUUIDMap := make(map[string]bool)
	bucketValid := make(map[string]bool)

	keyspaceMap := make(map[string]bool)
	keyspaceValid := make(map[string]bool)

	idx.cinfoProviderLock.RLock()
	defer idx.cinfoProviderLock.RUnlock()

	clusterVer := idx.cinfoProvider.ClusterVersion()

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
			bucketUUIDValid := idx.cinfoProvider.ValidateBucket(bucket, []string{index.Defn.BucketUUID})
			bucketUUIDMap[bucketUUID] = bucketUUIDValid

			if _, ok := bucketValid[bucket]; ok {
				bucketValid[bucket] = bucketValid[bucket] && bucketUUIDValid
			} else {
				bucketValid[bucket] = bucketUUIDValid
			}
		}

		if clusterVer >= common.INDEXER_70_VERSION {

			keyspace := strings.Join([]string{index.Defn.Bucket,
				index.Defn.Scope, index.Defn.Collection}, ":")
			if _, ok := keyspaceMap[keyspace]; !ok {

				cidValid := idx.cinfoProvider.ValidateCollectionID(index.Defn.Bucket,
					index.Defn.Scope, index.Defn.Collection, index.Defn.CollectionId, true)

				if _, ok := keyspaceValid[keyspace]; ok {
					keyspaceValid[keyspace] = keyspaceValid[keyspace] && cidValid
				} else {
					keyspaceValid[keyspace] = cidValid
				}

			}
		}
	}

	// handle bucket that fails validation
	for bucket, valid := range bucketValid {
		if !valid {
			instList := idx.deleteIndexInstOnDeletedKeyspace(bucket, "", "", common.NIL_STREAM)
			for _, instId := range instList {
				index := idx.indexInstMap[instId]
				logging.Warnf("Indexer::validateIndexInstMap Bucket %v Not Found."+
					"Not Recovering Index %v", bucket, index)
				delete(idx.indexInstMap, instId)
			}
		}
	}

	// handle collection that fails validation
	for keyspace, valid := range keyspaceValid {
		if !valid {
			bucket, scope, collection := SplitKeyspaceId(keyspace)
			instList := idx.deleteIndexInstOnDeletedKeyspace(bucket, scope, collection, common.NIL_STREAM)
			for _, instId := range instList {
				index := idx.indexInstMap[instId]
				logging.Warnf("Indexer::validateIndexInstMap Keyspace %v Not Found."+
					"Not Recovering Index %v", keyspace, index)
				delete(idx.indexInstMap, instId)
			}
		}
	}
	idx.checkMaintStreamIndexBuild()

}

// Given an index instance as input, getConfProxies returns a list of
// index instances, which share same real instance id with input instance.
// Because they share same instance ids, these instances can potentially
// *conflict* depending on the state and partition Ids. This function
// ignores the instances which do not have a valid recovery state as
// those instances won't be recovered, and conflict shouldn't matter.
func (idx *indexer) getConflictProxies(inst *common.IndexInst) []*common.IndexInst {
	if inst == nil || inst.RealInstId == common.IndexInstId(0) {
		return nil
	}

	insts := make([]*common.IndexInst, 0)

	for _, index := range idx.indexInstMap {
		// Skip the input instance (self).
		if index.InstId == inst.InstId {
			continue
		}

		if index.RealInstId == common.IndexInstId(0) {
			continue
		}

		if index.RealInstId != inst.RealInstId {
			continue
		}

		// Ignore the invalid recovery states as the
		// conflicting instance won't affect them.
		if !isValidRecoveryState(index.State) {
			continue
		}

		insts = append(insts, &index)
	}

	return insts
}

//force cleanup of index data should only be used when storage manager has not yet
//been initialized
func (idx *indexer) forceCleanupIndexData(inst *common.IndexInst, sliceId SliceId) error {

	if inst.RState != common.REBAL_MERGED {
		conflictInsts := idx.getConflictProxies(inst)
		partnDefnList := inst.Pc.GetAllPartitions()
		for _, partnDefn := range partnDefnList {
			if inst.IsProxy() && inst.RState == common.REBAL_PENDING_DELETE {
				// For RState is REBAL_PENDING_DELETE(i.e. tombstone) skip the cleanup if
				// (1) partition is a valid partition for RealInstId see MB-42108 for details OR
				// (2) partition is a valid partition for another instance with same RealInstId.
				if realInst, ok := idx.indexInstMap[inst.RealInstId]; ok {
					if exists := realInst.Pc.CheckPartitionExists(partnDefn.GetPartitionId()); exists {
						partitionIDs, _ := realInst.Pc.GetAllPartitionIds()
						logging.Infof("Skip cleanup for proxy InstId %v Partition %v. Valid partition"+
							" found for Real InstId %v Partitions %v", inst.InstId, partnDefn.GetPartitionId(),
							inst.RealInstId, partitionIDs)
						continue
					}
				}

				skipCleanup := false
				for _, cinst := range conflictInsts {
					if exists := cinst.Pc.CheckPartitionExists(partnDefn.GetPartitionId()); exists {
						partitionIDs, _ := cinst.Pc.GetAllPartitionIds()
						logging.Infof("Skip cleanup for proxy InstId %v Partition %v. Conflicting partition"+
							" found for InstId %v Partitions %v", inst.InstId, partnDefn.GetPartitionId(),
							cinst.InstId, partitionIDs)
						skipCleanup = true
						break
					}
				}

				if skipCleanup {
					continue
				}
			}

			logging.Infof("Indexer::forceCleanupIndexData Cleaning Up partition %v, "+
				"IndexInstId %v, IndexDefnId %v ", partnDefn.GetPartitionId(), inst.InstId, inst.Defn.DefnId)

			//cleanup the disk directory
			if err := idx.forceCleanupPartitionData(inst, partnDefn.GetPartitionId(), sliceId); err != nil {
				logging.Errorf("Indexer::forceCleanupIndexData Error Cleaning Up partition %v, "+
					"IndexInstId %v, IndexDefnId %v. Error %v", partnDefn.GetPartitionId(), inst.InstId, inst.Defn.DefnId, err)
				return err
			}
		}
	}

	return nil
}

//force cleanup of index partition data should only be used when storage manager has not yet
//been initialized
func (idx *indexer) forceCleanupPartitionData(inst *common.IndexInst, partitionId common.PartitionId, sliceId SliceId) error {

	storage_dir := idx.config["storage_dir"].String()
	path := filepath.Join(storage_dir, IndexPath(inst, partitionId, sliceId))
	return DestroySlice(common.IndexTypeToStorageMode(inst.Defn.Using), storage_dir, path)
}

//On warmup, if an index is found in MAINT_STREAM and state INITIAL
//it needs to be moved to INIT_STREAM. Post 6.5, initial build of
//an index never happens using MAINT_STREAM. During upgrade, it is
//possible for such an index to exist.
func (idx *indexer) checkMaintStreamIndexBuild() {

	var updatedList []common.IndexInstId
	for _, index := range idx.indexInstMap {
		if index.Stream == common.MAINT_STREAM &&
			index.State == common.INDEX_STATE_INITIAL {
			index.Stream = common.INIT_STREAM
			idx.indexInstMap[index.InstId] = index
			updatedList = append(updatedList, index.InstId)
		}
	}

	if idx.enableManager {
		if err := idx.updateMetaInfoForIndexList(updatedList,
			true, true, false, false, true, false, false, false, nil); err != nil {
			common.CrashOnError(err)
		}
	}

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
	restartTs, allNilSnaps := idx.makeRestartTs(common.MAINT_STREAM)

	idx.initStreamKeyspaceIdState(common.MAINT_STREAM)

	for keyspaceId, ts := range restartTs {
		idx.keyspaceIdRollbackTimes[keyspaceId] = time.Now().UnixNano()
		sessionId := idx.genNextSessionId(common.MAINT_STREAM, keyspaceId)
		idx.startKeyspaceIdStream(common.MAINT_STREAM, keyspaceId, ts, nil, allNilSnaps,
			false, false, sessionId)
		idx.setStreamKeyspaceIdState(common.MAINT_STREAM, keyspaceId, STREAM_ACTIVE)
	}

	//Start INIT_STREAM
	restartTs, allNilSnaps = idx.makeRestartTs(common.INIT_STREAM)

	idx.initStreamKeyspaceIdState(common.INIT_STREAM)

	for keyspaceId, ts := range restartTs {
		sessionId := idx.genNextSessionId(common.INIT_STREAM, keyspaceId)
		idx.startKeyspaceIdStream(common.INIT_STREAM, keyspaceId, ts, nil, allNilSnaps,
			false, false, sessionId)
		idx.setStreamKeyspaceIdState(common.INIT_STREAM, keyspaceId, STREAM_ACTIVE)
	}

	return true

}

func (idx *indexer) makeRestartTs(streamId common.StreamId) (map[string]*common.TsVbuuid, map[string]bool) {

	restartTs := make(map[string]*common.TsVbuuid)
	allNilSnaps := make(map[string]bool)

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

				keyspaceId := idxInst.Defn.KeyspaceId(idxInst.Stream)
				if _, ok := allNilSnaps[keyspaceId]; !ok {
					allNilSnaps[keyspaceId] = true
				}

				//There may not be a valid snapshot info if no flush
				//happened for this index
				if latestSnapInfo != nil {
					allNilSnaps[keyspaceId] = false
					ts := latestSnapInfo.Timestamp()
					if oldTs, ok := restartTs[keyspaceId]; ok {
						if oldTs == nil {
							continue
						}
						if !ts.AsRecentTs(oldTs) {
							restartTs[keyspaceId] = ts
						}
					} else {
						restartTs[keyspaceId] = ts
					}
				} else {
					//set restartTs to nil for this keyspace
					restartTs[keyspaceId] = nil
				}
			}
		}
	}

	//for pre 7.0 indexes, the manifestUID needs to be set to epoch
	for _, ts := range restartTs {
		if ts != nil {
			ts.SetEpochManifestUIDIfEmpty()
		}
	}
	return restartTs, allNilSnaps
}

// closeAllStreams sequentially sends CLOSE_STREAM commands to KVSender for MAINT_STREAM
// and INIT_STREAM and waits forever for a response for each. It is only called during
// indexer bootstrap to close any outstanding streams in projectors.
func (idx *indexer) closeAllStreams() {

	respCh := make(MsgChannel)

	for streamId := common.NIL_STREAM; streamId < common.ALL_STREAMS; streamId++ {

		//skip for nil and catchup stream
		if streamId == common.NIL_STREAM ||
			streamId == common.CATCHUP_STREAM {
			continue
		}

		cmd := &MsgStreamUpdate{mType: CLOSE_STREAM,
			streamId: streamId,
			respCh:   respCh,
		}

		count := 0
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
					count++
					if count > MAX_PROJ_RETRY {
						logging.Fatalf("Indexer::closeAllStreams Stream %v "+
							"Projector health check needed, indexer can not proceed, Error received %v. Retrying (%v).",
							streamId, respErr.cause, count)
					} else {
						logging.Warnf("Indexer::closeAllStreams Stream %v "+
							"Projector health check needed, indexer can not proceed, Error received %v. Retrying (%v).",
							streamId, respErr.cause, count)
					}

					time.Sleep(KV_RETRY_INTERVAL * time.Millisecond)
				}
			}
		}
		// Do not need to update and distribute keyspace stats here as this function is bootstrap-only, so they are
		// empty, and recipients are not all started yet so attempted forward will wait forever for response.
	}
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

	return idx.sendMsgToClustMgrAndProcessResponse(msg)

}

func (idx *indexer) updateMetaInfoForDeleteKeyspace(bucket,
	scope, collection string, streamId common.StreamId) error {

	msg := &MsgClustMgrUpdate{
		mType:      CLUST_MGR_DEL_KEYSPACE,
		bucket:     bucket,
		scope:      scope,
		collection: collection,
		streamId:   streamId}

	return idx.sendMsgToClustMgrAndProcessResponse(msg)
}

func (idx *indexer) cleanupIndexMetadata(indexInst common.IndexInst) error {

	temp := indexInst
	temp.Pc = nil
	msg := &MsgClustMgrUpdate{mType: CLUST_MGR_CLEANUP_INDEX, indexList: []common.IndexInst{temp}}
	return idx.sendMsgToClustMgrAndProcessResponse(msg)
}

func (idx *indexer) sendMsgToClustMgrAndProcessResponse(msg Message) error {

	res, ok := idx.sendMsgToClustMgr(msg)

	if ok {

		switch res.GetMsgType() {

		case MSG_SUCCESS:
			return nil

		case MSG_ERROR:
			logging.Errorf("Indexer::sendMsgToClustMgrAndProcessResponse Error "+
				"from Cluster Manager %v", res)
			err := res.(*MsgError).GetError()
			return err.cause

		default:
			logging.Fatalf("Indexer::sendMsgToClustMgrAndProcessResponse Unknown Response "+
				"from Cluster Manager %v", res)
			common.CrashOnError(errors.New("Unknown Response"))

		}

	} else {

		logging.Fatalf("clustMgrAgent::sendMsgToClustMgrAndProcessResponse Unexpected Channel Close " +
			"from Cluster Manager")
		common.CrashOnError(errors.New("Unknown Response"))

	}

	return nil
}

func (idx *indexer) sendMsgToWorker(msg Message, cmdCh MsgChannel) error {

	cmdCh <- msg

	if res, ok := <-cmdCh; ok {

		switch res.GetMsgType() {

		case MSG_SUCCESS:
			return nil

		case MSG_ERROR:
			err := res.(*MsgError).GetError()
			return err.cause

		default:
			logging.Errorf("Indexer::sendMsgToWorker Unknown Response %v", res)
			return fmt.Errorf("Fail to send message to worker: Unknown Response")
		}
	} else {
		logging.Errorf("clustMgrAgent::sendMsgToWorker Channel Close ")
		return fmt.Errorf("Fail to send message to worker: Closed Channel")
	}

	return nil
}

func (idx *indexer) handleSetLocalMeta(msg Message) {

	key := msg.(*MsgClustMgrLocal).GetKey()
	value := msg.(*MsgClustMgrLocal).GetValue()

	respch := msg.(*MsgClustMgrLocal).GetRespCh()
	checkDDL := msg.(*MsgClustMgrLocal).GetCheckDDL()

	if key == RebalanceRunning && checkDDL {
		if inProgress, indexList := idx.checkDDLInProgress(); inProgress {
			respch <- &MsgClustMgrLocal{
				mType:             CLUST_MGR_SET_LOCAL,
				key:               key,
				value:             value,
				err:               ErrDDLRunning,
				inProgressIndexes: indexList,
			}
			return
		}
	}

	respMsg, _ := idx.sendMsgToClustMgr(msg)

	err := respMsg.(*MsgClustMgrLocal).GetError()
	if err == nil {
		if key == RebalanceRunning {
			idx.rebalanceRunning = true

			msg := &MsgClustMgrUpdate{mType: CLUST_MGR_REBALANCE_RUNNING}
			idx.sendMsgToClustMgrAndProcessResponse(msg)

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

	respMsg, _ := idx.sendMsgToClustMgr(msg)
	respch := msg.(*MsgClustMgrLocal).GetRespCh()
	respch <- respMsg

}

func (idx *indexer) handleDelLocalMeta(msg Message) {

	respMsg, _ := idx.sendMsgToClustMgr(msg)

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

// getInsts returns the IndexInst objects identified by the IDs in instIdList.
func (idx *indexer) getInsts(instIdList []common.IndexInstId) []common.IndexInst {

	updInsts := make([]common.IndexInst, 0)
	for _, instId := range instIdList {
		idxInst := idx.indexInstMap[instId]
		updInsts = append(updInsts, idxInst)
	}
	return updInsts
}

// bulkUpdateState sets the states of a set of index instances specified
// by instIdList to the value of the state arg.
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
			logging.Infof("bulkUpdateRState: Index instance %v rstate moved to PENDING", instId)
		} else {
			idxInst.RState = common.REBAL_ACTIVE
			logging.Infof("bulkUpdateRState: Index instance %v rstate moved to ACTIVE", instId)
		}
		idx.indexInstMap[instId] = idxInst
	}
}

// bulkUpdateStream sets the streams of a set of index instances specified
// by instIdList to the value of the stream arg.
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
			buildTs[i] = ts
		}
		idxInst.BuildTs = buildTs
		idx.indexInstMap[instId] = idxInst
	}
}

func (idx *indexer) checkKeyspaceIdInRecovery(keyspaceId string,
	instIdList []common.IndexInstId, clientCh MsgChannel, errMap map[common.IndexInstId]error) bool {

	initState := idx.getStreamKeyspaceIdState(common.INIT_STREAM, keyspaceId)

	bucket := GetBucketFromKeyspaceId(keyspaceId)
	maintState := idx.getStreamKeyspaceIdState(common.MAINT_STREAM, bucket)

	if initState == STREAM_RECOVERY ||
		initState == STREAM_PREPARE_RECOVERY ||
		maintState == STREAM_RECOVERY ||
		maintState == STREAM_PREPARE_RECOVERY {

		if idx.enableManager {
			errStr := fmt.Sprintf("KeyspaceId %v In Recovery", keyspaceId)
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

func (idx *indexer) checkValidIndexInst(keyspaceId string, instIdList []common.IndexInstId,
	clientCh MsgChannel, errMap map[common.IndexInstId]error) ([]common.IndexInstId, bool) {

	if len(instIdList) == 0 {
		return instIdList, true
	}

	newList := make([]common.IndexInstId, 0, len(instIdList))
	count := 0
	skipCount := 0

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
				newList = append(newList, instId)
				count++
			} else if index.State == common.INDEX_STATE_INITIAL ||
				index.State == common.INDEX_STATE_CATCHUP {
				logging.Infof("Index build is already in progress for inst %v. State %v.", instId, index.State)
				skipCount++
			} else if index.State == common.INDEX_STATE_ACTIVE {
				logging.Infof("Index build is already finished for inst %v. State %v.", instId, index.State)
				skipCount++
			} else {
				errStr := fmt.Sprintf("Invalid Index State %v for %v In Build Request", index.State, instId)
				idx.updateError(instId, errStr)
				errMap[instId] = &common.IndexerError{Reason: errStr, Code: common.IndexInvalidState}
			}
		}
	}

	newList = newList[0:count]
	return newList, len(newList) == len(instIdList)-skipCount
}

func (idx *indexer) groupIndexListByKeyspaceId(instIdList []common.IndexInstId) map[string][]common.IndexInstId {

	keyspaceIdInstList := make(map[string][]common.IndexInstId)
	for _, instId := range instIdList {
		indexInst := idx.indexInstMap[instId]
		keyspaceId := indexInst.Defn.KeyspaceId(common.INIT_STREAM)
		if instList, ok := keyspaceIdInstList[keyspaceId]; ok {
			instList = append(instList, indexInst.InstId)
			keyspaceIdInstList[keyspaceId] = instList
		} else {
			var newInstList []common.IndexInstId
			newInstList = append(newInstList, indexInst.InstId)
			keyspaceIdInstList[keyspaceId] = newInstList
		}
	}
	return keyspaceIdInstList

}

func (idx *indexer) checkBucketExists(bucket string,
	instIdList []common.IndexInstId, clientCh MsgChannel, errMap map[common.IndexInstId]error) ([]common.IndexInstId, bool) {

	if len(instIdList) == 0 {
		return instIdList, false
	}

	newList := make([]common.IndexInstId, len(instIdList))
	count := 0

	idx.cinfoProviderLock.RLock()
	currUUID, err := idx.cinfoProvider.GetBucketUUID(bucket)
	idx.cinfoProviderLock.RUnlock()
	if err != nil {
		logging.Fatalf("Indexer::checkBucketExists Error Fetching Bucket Info: %v for bucket: %v, currUUID: %v", err, bucket, currUUID)
	}

	for _, instId := range instIdList {
		indexInst := idx.indexInstMap[instId]
		if indexInst.Defn.Bucket != bucket || indexInst.Defn.BucketUUID != currUUID || err != nil {
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

	idx.updateStatsFromMemStats()

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
	mem_used := int64(forestdb.BufferCacheUsed()) + memdb.MemoryInUse() + plasma.MemoryInUse() + nodetable.MemoryInUse()
	return mem_used
}

func NewSlice(id SliceId, indInst *common.IndexInst, partnInst *PartitionInst,
	conf common.Config, stats *IndexerStats, ephemeral, isNew bool, meteringMgr *MeteringThrottlingMgr) (slice Slice, err error) {
	// Default storage is forestdb
	storage_dir := conf["storage_dir"].String()
	iowrap.Os_Mkdir(storage_dir, 0755)
	if _, e := iowrap.Os_Stat(storage_dir); e != nil {
		common.CrashOnError(e)
	}
	path := filepath.Join(storage_dir, IndexPath(indInst, partnInst.Defn.GetPartitionId(), id))

	partitionId := partnInst.Defn.GetPartitionId()
	numPartitions := indInst.Pc.GetNumPartitions()
	instId := GetRealIndexInstId(indInst)

	log_dir := conf["log_dir"].String()

	switch indInst.Defn.Using {
	case common.MemDB, common.MemoryOptimized:
		slice, err = NewMemDBSlice(path, id, indInst.Defn, instId, partitionId, indInst.Defn.IsPrimary, !ephemeral, numPartitions, conf,
			stats.GetPartitionStats(indInst.InstId, partitionId))
	case common.ForestDB:
		slice, err = NewForestDBSlice(path, id, indInst.Defn, instId, partitionId, indInst.Defn.IsPrimary, numPartitions, conf,
			stats.GetPartitionStats(indInst.InstId, partitionId))
	case common.PlasmaDB:
		slice, err = NewPlasmaSlice(storage_dir, log_dir, path, id, indInst.Defn, instId, partitionId, indInst.Defn.IsPrimary, numPartitions, conf,
			stats.GetPartitionStats(indInst.InstId, partitionId), stats, isNew, meteringMgr)
	}

	return
}

func DestroySlice(mode common.StorageMode, storageDir string, path string) error {

	switch mode {
	case common.MOI, common.FORESTDB, common.NOT_SET:
		return iowrap.Os_RemoveAll(path)
	case common.PLASMA:
		return DestroyPlasmaSlice(storageDir, path)
	}

	return fmt.Errorf("unable to delete instance %v : unrecognized storage type %v", path, mode)
}

func ListSlices(mode common.StorageMode, storageDir string) ([]string, error) {

	listFiles := func() ([]string, error) {
		pattern := GetIndexPathPattern()
		return filepath.Glob(filepath.Join(storageDir, pattern))
	}

	switch mode {
	case common.MOI, common.FORESTDB, common.NOT_SET:
		return listFiles()
	case common.PLASMA:
		return listFiles()
	}
	return nil, fmt.Errorf("unable to list instance : unrecognized storage type %v", mode)
}

func MoveSlice(mode common.StorageMode, indexInst *common.IndexInst, partnId common.PartitionId, sliceId SliceId,
	storageDir string, sourceDir string, targetDir string) error {

	// Given any path, rename() will add a timestamp to the first sub-directory
	// after sourceDir.  The renamed sub-directory will be added to the targetDir
	// to form a full path (as the backup dir).  This function expects that the
	// first sub-directory is distinctive enough to differentiate among different
	// index files. This function call needs to be stable since it can be called
	// multiple times for the same path during each bootstrap.  So it uses
	// indexer uptime as timestamp.
	rename := func(path string) (string, error) {

		if !strings.HasPrefix(path, sourceDir) {
			return "", fmt.Errorf("path (%v) does not contain source directory (%v)", path, sourceDir)
		}

		sourceDirLen := len(sourceDir)
		if path[sourceDirLen] == filepath.Separator {
			sourceDirLen++
		}

		if len(path) <= sourceDirLen {
			return "", fmt.Errorf("path (%v) does not contain source directory (%v)", path, sourceDir)
		}

		indexPath := path[sourceDirLen:]

		strTime := fmt.Sprintf("%d-%02d-%02dT%02d-%02d-%02d-%03d", uptime.Year(), uptime.Month(),
			uptime.Day(), uptime.Hour(), uptime.Minute(), uptime.Second(), uptime.Nanosecond()/1000/1000)

		destIndexPath := strTime + "_" + indexPath
		return filepath.Join(targetDir, destIndexPath), nil
	}

	clean := func(srcPath string) {
		deleteOldBackups(targetDir, sourceDir, srcPath)
	}

	switch mode {
	case common.MOI, common.FORESTDB, common.NOT_SET:
		return moveIndexFile(indexInst, partnId, sliceId, sourceDir, targetDir)
	case common.PLASMA:
		indexPath := IndexPath(indexInst, partnId, sliceId)
		srcPath := filepath.Join(sourceDir, indexPath)
		return BackupCorruptedPlasmaSlice(storageDir, srcPath, rename, clean)
	}
	return fmt.Errorf("unable to move instance : unrecognized storage type %v", mode)
}

func moveIndexFile(indexInst *common.IndexInst, partnId common.PartitionId, sliceId SliceId, sourceDir string, targetDir string) error {
	indexPath := IndexPath(indexInst, partnId, sliceId)
	srcPath := filepath.Join(sourceDir, indexPath)

	if err := deleteOldBackups(targetDir, sourceDir, srcPath); err != nil {
		logging.Errorf("Indexer::moveIndexFile %v %v error %v while taking backup %v",
			indexInst.InstId, partnId, err, targetDir)
		return err
	}

	t := time.Now()
	strTime := fmt.Sprintf("%d-%02d-%02dT%02d-%02d-%02d-%03d", t.Year(), t.Month(),
		t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond()/1000/1000)
	destIndexPath := strTime + "_" + indexPath
	destPath := filepath.Join(targetDir, destIndexPath)

	if err := iowrap.Os_Rename(srcPath, destPath); err != nil {
		logging.Errorf("Indexer::moveIndexFile %v %v error %v while taking backup:Rename(%v, %v)",
			indexInst.InstId, partnId, err, srcPath, destPath)
		return err
	}

	logging.Infof("Indexer::moveIndexFile %v %v is moved to %v",
		indexInst.InstId, partnId, destPath)

	return nil
}

// This function deletes old backups if any.  This function works in a pair with rename function in MoveSlice().
// 1) Given any path, it finds the first sub-directory after the sourceDir.
// 2) It will then iterate through the sub-directories under targetDir.
// 3) If the target sub-directory name matches the sub-directory in (1), then it creates a new path using the target sub-directory name.
// 4) The new path in (4) is removed.
func deleteOldBackups(targetDir string, sourceDir string, srcPath string) error {

	strip := func(prefix string, paths []string) []string {
		dirs := strings.Split(prefix, string(filepath.Separator))
		return paths[len(dirs):]
	}

	join := func(prefix string, paths []string, startPos int) string {
		if len(paths) <= startPos {
			return filepath.Join(targetDir, prefix)
		}

		paths = paths[startPos:]
		result := filepath.Join(targetDir, prefix)
		for _, path := range paths {
			result = filepath.Join(result, path)
		}
		return result
	}

	if !strings.HasPrefix(srcPath, sourceDir) {
		return fmt.Errorf("path (%v) does not contain source directory (%v)", srcPath, sourceDir)
	}

	indexPaths := strings.Split(srcPath, string(filepath.Separator))
	indexPaths = strip(sourceDir, indexPaths)

	files, err := iowrap.Ioutil_ReadDir(targetDir)
	if err != nil {
		logging.Errorf("Indexer::deleteOldBackups encounter error %v while taking backup:ReadDir %v",
			err, targetDir)
		return err
	}

	for _, f := range files {
		if strings.HasSuffix(f.Name(), indexPaths[0]) {
			fpath := join(f.Name(), indexPaths, 1)
			logging.Infof("Indexer::deleteOldBackups deleting path %v", fpath)
			if err = iowrap.Os_RemoveAll(fpath); err != nil {
				logging.Errorf("Indexer::deleteOldBackups error %v while removing old backup %v",
					err, fpath)
				return err
			}
		}
	}
	return nil
}

func (idx *indexer) setProfilerOptions(config common.Config) {
	// CPU-profiling
	cpuProfile, ok := config["settings.cpuProfile"]
	if ok && cpuProfile.Bool() && idx.cpuProfFd == nil {
		cpuProfDir, ok := config["settings.cpuProfDir"]
		fname := "indexer_cpu.prof"
		if ok && cpuProfDir.String() != "" {
			fname = filepath.Join(cpuProfDir.String(), fname)
		}
		logging.Infof("Indexer:: cpu profiling => %q\n", fname)
		idx.cpuProfFd = startCPUProfile(fname)
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
		memProfDir, ok := config["settings.memProfDir"]
		fname := "indexer_mem.pprof"
		if ok && memProfDir.String() != "" {
			fname = filepath.Join(memProfDir.String(), fname)
		}
		if dumpMemProfile(fname) {
			logging.Infof("Indexer:: mem profile => %q\n", fname)
		}
	}
}

func (idx *indexer) getIndexInstForKeyspaceId(keyspaceId string) ([]common.IndexInstId, error) {

	resp, _ := idx.sendMsgToClustMgr(&MsgClustMgrTopology{})

	var result []common.IndexInstId = nil

	switch resp.GetMsgType() {
	case CLUST_MGR_GET_GLOBAL_TOPOLOGY:
		instMap := resp.(*MsgClustMgrTopology).GetInstMap()
		for id, inst := range instMap {
			if inst.Defn.KeyspaceId(inst.Stream) == keyspaceId {
				result = append(result, id)
			}
		}
	default:
		return nil, errors.New("Fail to read Metadata")
	}

	return result, nil
}

func (idx *indexer) deleteIndexInstOnDeletedKeyspace(bucket,
	scope, collection string, streamId common.StreamId) []common.IndexInstId {

	var instIdList []common.IndexInstId = nil

	if idx.enableManager {
		if err := idx.updateMetaInfoForDeleteKeyspace(bucket,
			scope, collection, streamId); err != nil {
			common.CrashOnError(err)
		}
	}

	if collection == "" {
		// Only mark index inst as DELETED if it is actually got deleted in metadata.
		for _, index := range idx.indexInstMap {
			if index.Defn.Bucket == bucket &&
				(streamId == common.NIL_STREAM || (index.Stream == streamId ||
					index.Stream == common.NIL_STREAM)) {

				instIdList = append(instIdList, index.InstId)

				idx.stats.RemoveIndexStats(index)
			}
		}

	} else {
		// Only mark index inst as DELETED if it is actually got deleted in metadata.
		for _, index := range idx.indexInstMap {
			if index.Defn.Bucket == bucket &&
				index.Defn.Scope == scope &&
				index.Defn.Collection == collection &&
				(streamId == common.NIL_STREAM || (index.Stream == streamId ||
					index.Stream == common.NIL_STREAM)) {

				instIdList = append(instIdList, index.InstId)

				idx.stats.RemoveIndexStats(index)
			}
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
	fd, err := iowrap.Os_Create(filename)
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
	fd, err := iowrap.Os_Create(filename)
	if err != nil {
		logging.Errorf("Indexer:: unable to create %q: %v\n", filename, err)
		return false
	}
	pprof.WriteHeapProfile(fd)
	defer iowrap.File_Close(fd)
	return true
}

func (idx *indexer) computeKeyspaceBuildTsAsync(clusterAddr string,
	keyspaceId string, cid string, numVb int, streamId common.StreamId) {

	// Acquire the buildTsLock
	// The buildTsLock is per bucket per stream lock. It serves two purposes:
	// (i) It serializes the update of buildTs to indexer and timekeeper so that
	//     the buildTs computed earlier by one go-routine can not overwrite the
	//     buildTs computed later by another go-routine
	// (ii) Incase of any issues with KV, it prevents multiple go-routines to
	//      flood the logs with error messages while fetching the KVT's
	mutex := idx.buildTsLock[streamId][keyspaceId]
	mutex.Lock()
	defer mutex.Unlock()

	buildTs, err := computeKeyspaceBuildTs(clusterAddr, keyspaceId, cid, numVb)
	if err != nil {
		logging.Errorf("Indexer::computeBucketBuildTsAsync, stream: %v, keyspace: %v, "+
			"cid: %v err: %v", streamId, keyspaceId, cid, err)
	} else {
		msgBuildTs := &MsgStreamUpdate{
			mType:      INDEXER_UPDATE_BUILD_TS,
			streamId:   streamId,
			keyspaceId: keyspaceId,
			buildTs:    buildTs,
		}
		// Send a message to indexer to update the buildTs.
		// Indexer would forward this message to timekeeper
		idx.internalRecvCh <- msgBuildTs
	}
}

//calculates buildTs for keyspace. This is a blocking call
//which will keep trying till success as indexer cannot work
//without a buildts.
func computeKeyspaceBuildTs(clustAddr string, keyspaceId string,
	cid string, numVb int) (buildTs Timestamp, err error) {

kvtsloop:
	for {
		buildTs, err = GetCurrentKVTs(clustAddr, "default", keyspaceId, cid, numVb)
		//TODO Collections - handle the case when collection doesn't exist
		if err != nil {
			logging.Errorf("Indexer::computeKeyspaceBuildTs Error Fetching BuildTs %v", err)
			var uuid string
			bucket, _, _ := SplitKeyspaceId(keyspaceId)
			uuid, err = common.GetBucketUUID(clustAddr, bucket)
			if err == nil && uuid == common.BUCKET_UUID_NIL {
				// BUCKET_UUID_NIL is returned in non-error case
				// if bucket does not exist. Do not infinitely retry if bucket does not exist.
				err = errors.New(fmt.Sprintf("Bucket %v does not exist anymore.", bucket))
				logging.Errorf("Indexer::computeBucketBuildTs Error: %v", err)
				return
			}

			//validate collection
			if cid != "" {
				collId := ""
				bucket, scope, coll := SplitKeyspaceId(keyspaceId)
				collId, err = common.GetCollectionID(clustAddr, bucket, scope, coll)
				if err == nil && collId == collections.COLLECTION_ID_NIL {
					err = errors.New(fmt.Sprintf("Keyspace %v does not exist anymore.", keyspaceId))
					logging.Errorf("Indexer::computeBucketBuildTs Error: %v", err)
					return

				}

			}
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

func (idx *indexer) initStreamKeyspaceIdState(streamId common.StreamId) {

	idx.stateLock.Lock()
	defer idx.stateLock.Unlock()

	if _, ok := idx.streamKeyspaceIdStatus[streamId]; !ok {
		idx.streamKeyspaceIdStatus[streamId] = make(KeyspaceIdStatus)
	}

}

func (idx *indexer) getStreamKeyspaceIdState(streamId common.StreamId, keyspaceId string) StreamStatus {

	idx.stateLock.RLock()
	defer idx.stateLock.RUnlock()
	return idx.streamKeyspaceIdStatus[streamId][keyspaceId]

}

func (idx *indexer) setStreamKeyspaceIdState(streamId common.StreamId, keyspaceId string, status StreamStatus) {

	idx.stateLock.Lock()
	defer idx.stateLock.Unlock()
	idx.streamKeyspaceIdStatus[streamId][keyspaceId] = status

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

			memory_quota := idx.config.GetIndexerMemoryQuota()
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
	clustMgrMsg := &MsgClustMgrLocal{
		mType: CLUST_MGR_SET_LOCAL,
		key:   INDEXER_STATE_KEY,
		value: fmt.Sprintf("%s", common.INDEXER_PAUSED),
	}

	respMsg, _ := idx.sendMsgToClustMgr(clustMgrMsg)
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
	clustMgrMsg := &MsgClustMgrLocal{
		mType: CLUST_MGR_SET_LOCAL,
		key:   INDEXER_STATE_KEY,
		value: fmt.Sprintf("%s", common.INDEXER_ACTIVE),
	}

	respMsg, _ := idx.sendMsgToClustMgr(clustMgrMsg)
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

	for s, bs := range idx.streamKeyspaceIdStatus {

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

	for s, bs := range idx.streamKeyspaceIdStatus {

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

func (idx *indexer) updateStatsFromMemStats() {
	gMemstatLock.RLock()
	idx.stats.pauseTotalNs.Set(gMemstatCache.PauseTotalNs)
	gMemstatLock.RUnlock()
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
	memQuota := idx.config.GetIndexerMemoryQuota()

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
	memQuota := idx.config.GetIndexerMemoryQuota()

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

func (idx *indexer) setRetryTsForRecovery(streamId common.StreamId, keyspaceId string,
	retryTs *common.TsVbuuid) {

	if _, ok := idx.streamKeyspaceIdRetryTs[streamId]; ok {
		idx.streamKeyspaceIdRetryTs[streamId][keyspaceId] = retryTs
	} else {
		keyspaceIdRetryTs := make(KeyspaceIdRetryTs)
		keyspaceIdRetryTs[keyspaceId] = retryTs
		idx.streamKeyspaceIdRetryTs[streamId] = keyspaceIdRetryTs
	}
}

func (idx *indexer) setRollbackTs(streamId common.StreamId, keyspaceId string,
	rollbackTs *common.TsVbuuid) {

	if _, ok := idx.streamKeyspaceIdRollbackTs[streamId]; ok {
		idx.streamKeyspaceIdRollbackTs[streamId][keyspaceId] = rollbackTs
	} else {
		keyspaceIdRollbackTs := make(KeyspaceIdRollbackTs)
		keyspaceIdRollbackTs[keyspaceId] = rollbackTs
		idx.streamKeyspaceIdRollbackTs[streamId] = keyspaceIdRollbackTs
	}
}

func (idx *indexer) setMinMergeTsForCatchup(streamId common.StreamId, keyspaceId string,
	mergeTs *common.TsVbuuid) {

	setMergeTs := func(streamId common.StreamId, keyspaceId string, mergeTs *common.TsVbuuid) {
		logging.Infof("Indexer::setMinMergeTsForCatchup %v %v set minMergeTs", streamId, keyspaceId)
		if _, ok := idx.streamKeyspaceIdMinMergeTs[streamId]; ok {
			idx.streamKeyspaceIdMinMergeTs[streamId][keyspaceId] = mergeTs.Copy()
		} else {
			keyspaceIdMinMergeTs := make(KeyspaceIdMinMergeTs)
			keyspaceIdMinMergeTs[keyspaceId] = mergeTs.Copy()
			idx.streamKeyspaceIdMinMergeTs[streamId] = keyspaceIdMinMergeTs
		}
	}

	if streamId == common.INIT_STREAM {
		setMergeTs(streamId, keyspaceId, mergeTs)

	} else if streamId == common.MAINT_STREAM {
		//consider all keyspaceIds in INIT_STREAM for this bucket
		for kspId, _ := range idx.streamKeyspaceIdMinMergeTs[common.INIT_STREAM] {
			if GetBucketFromKeyspaceId(kspId) == keyspaceId &&
				idx.checkStreamKeyspaceIdInCatchupPhase(common.INIT_STREAM, kspId) {
				setMergeTs(common.INIT_STREAM, kspId, mergeTs)
			}
		}
	}

}

func (idx *indexer) getMinMergeTsForCatchup(streamId common.StreamId,
	keyspaceId string) *common.TsVbuuid {

	if _, ok := idx.streamKeyspaceIdMinMergeTs[streamId]; ok {
		return idx.streamKeyspaceIdMinMergeTs[streamId][keyspaceId]
	}
	return nil
}

func (idx *indexer) initBuildTsLock(streamId common.StreamId, keyspaceId string) {
	if _, ok := idx.buildTsLock[streamId]; !ok {
		idx.buildTsLock[streamId] = make(map[string]*sync.Mutex)
	}
	if _, ok := idx.buildTsLock[streamId][keyspaceId]; !ok {
		idx.buildTsLock[streamId][keyspaceId] = &sync.Mutex{}
	}
}

//sessionId helper functions. these functions can only be called from the genserver
//as no sync mechanism is being used.
func (idx *indexer) genNextSessionId(
	streamId common.StreamId,
	keyspaceId string) uint64 {

	var sid uint64
	var ok bool

	if sid, ok = idx.streamKeyspaceIdSessionId[streamId][keyspaceId]; ok {
		sid++
	} else {
		sid = uint64(rand.Int63n(100000) + int64(1)) //start with a random number between 1 and 100000
	}
	idx.streamKeyspaceIdSessionId[streamId][keyspaceId] = sid
	return sid
}

func (idx *indexer) getCurrentSessionId(
	streamId common.StreamId,
	keyspaceId string) uint64 {

	if sid, ok := idx.streamKeyspaceIdSessionId[streamId][keyspaceId]; ok {
		return sid
	} else {
		return 0
	}
}

func (idx *indexer) validateSessionId(
	streamId common.StreamId,
	keyspaceId string,
	sessionId uint64,
	assert bool) (bool, uint64) {

	valid := false
	curr := uint64(0)
	if cid, ok := idx.streamKeyspaceIdSessionId[streamId][keyspaceId]; ok {
		if sessionId == cid {
			valid = true
		}
		curr = cid
	}

	if !valid && assert && idx.config["debug.assertOnError"].Bool() {
		common.CrashOnError(errors.New(fmt.Sprintf("sessionId validation "+
			"failed. curr %v. have %v", curr, sessionId)))
	}

	return valid, curr

}

// injectRandomDelay injects random delay up to max seconds if config
// randomDelayInjection flag is set (for debug), else it is a no-op.
func (idx *indexer) injectRandomDelay(max int) {
	if idx.config["debug.randomDelayInjection"].Bool() {
		time.Sleep(time.Duration(rand.Intn(max)) * time.Second)
	}
}

//streamkeyspaceIdCurrRequest helper functions
func (idx *indexer) setStreamKeyspaceIdCurrRequest(
	streamId common.StreamId,
	keyspaceId string,
	cmd Message,
	reqCh StopChannel,
	sessionId uint64) {

	if _, ok := idx.streamKeyspaceIdCurrRequest[streamId]; !ok {
		idx.streamKeyspaceIdCurrRequest[streamId] = make(KeyspaceIdCurrRequest)
	}

	idx.streamKeyspaceIdCurrRequest[streamId][keyspaceId] = &currRequest{
		request:   cmd,
		reqCh:     reqCh,
		sessionId: sessionId,
	}

}

//clear the currRequest
func (idx *indexer) deleteStreamKeyspaceIdCurrRequest(
	streamId common.StreamId,
	keyspaceId string,
	cmd Message,
	reqCh StopChannel,
	sessionId uint64) {

	var req *currRequest
	if bCurrRequest, ok := idx.streamKeyspaceIdCurrRequest[streamId]; ok {
		req, ok = bCurrRequest[keyspaceId]
		if !ok {
			logging.Infof("Indexer::deleteStreamKeyspaceIdCurrRequest CurrRequest for keyspaceId Not Present "+
				"SessionId %v StreamId %v KeyspaceId %v cmd %v ", sessionId, streamId, keyspaceId, cmd)
			return
		}
	} else {
		logging.Infof("Indexer::deleteStreamKeyspaceIdCurrRequest CurrRequest Not Present for stream"+
			"SessionId %v StreamId %v KeyspaceId %v cmd %v ", sessionId, streamId, keyspaceId, cmd)
		return
	}

	//allow the caller to reset state if stopCh matches
	if req.reqCh == reqCh {
		delete(idx.streamKeyspaceIdCurrRequest[streamId], keyspaceId)
	} else {
		logging.Infof("Indexer::deleteStreamKeyspaceIdCurrRequest Not clearing "+
			"Curr Request %v SessionId %v. Requested %v SessionId %v. %v %v.", req.request,
			req.sessionId, cmd, sessionId, streamId, keyspaceId)
	}
}

func (idx *indexer) cleanupStreamKeyspaceIdCurrRequest(
	streamId common.StreamId,
	keyspaceId string,
) {
	delete(idx.streamKeyspaceIdCurrRequest[streamId], keyspaceId)
}

func (idx *indexer) checkStreamRequestPending(
	streamId common.StreamId,
	keyspaceId string) bool {

	if bCurrReq, ok := idx.streamKeyspaceIdCurrRequest[streamId]; ok {
		if req, ok := bCurrReq[keyspaceId]; ok {
			if req != nil {
				logging.Errorf("Indexer::checkStreamRequestPending %v %v %+v",
					keyspaceId, streamId, req)
				return true
			}
		}
	}

	return false
}

// cleanupStreamKeyspaceIdState
// Recovery flows do not call this; streams are stopped and started
// during recovery without state cleanup.
func (idx *indexer) cleanupStreamKeyspaceIdState(
	streamId common.StreamId,
	keyspaceId string) {

	// Remove keyspace stats for the stream as they are not removed
	// during recovery
	idx.stats.RemoveKeyspaceStats(streamId, keyspaceId)
	idx.distributeKeyspaceStatsMapsToWorkers()

	idx.cleanupStreamKeyspaceIdRecoveryState(streamId, keyspaceId)
	idx.cleanupStreamKeyspaceIdCurrRequest(streamId, keyspaceId)
}

func (idx *indexer) cleanupStreamKeyspaceIdRecoveryState(
	streamId common.StreamId,
	keyspaceId string) {

	delete(idx.streamKeyspaceIdRollbackTs[streamId], keyspaceId)
	delete(idx.streamKeyspaceIdRetryTs[streamId], keyspaceId)
}

// cleanupAllStreamKeyspaceIdState
// Recovery flows do not call this; streams are stopped and started
// during recovery without state cleanup.
func (idx *indexer) cleanupAllStreamKeyspaceIdState(
	streamId common.StreamId,
	keyspaceId string) {

	idx.cleanupStreamKeyspaceIdState(streamId, keyspaceId)
	delete(idx.streamKeyspaceIdFlushInProgress[streamId], keyspaceId)
	delete(idx.streamKeyspaceIdObserveFlushDone[streamId], keyspaceId)
	delete(idx.streamKeyspaceIdPendBuildDone[streamId], keyspaceId)
	delete(idx.streamKeyspaceIdPendStart[streamId], keyspaceId)
	delete(idx.streamKeyspaceIdCollectionId[streamId], keyspaceId)
	delete(idx.streamKeyspaceIdOSOException[streamId], keyspaceId)
	delete(idx.streamKeyspaceIdPendCollectionDrop[streamId], keyspaceId)
	delete(idx.streamKeyspaceIdMinMergeTs[streamId], keyspaceId)
}

func (idx *indexer) prepareStreamKeyspaceIdForFreshStart(
	streamId common.StreamId,
	keyspaceId string) {

	logging.Infof("Indexer::prepareStreamKeyspaceIdForFreshStart %v %v", streamId, keyspaceId)

	//clear all state before fresh start
	idx.cleanupAllStreamKeyspaceIdState(streamId, keyspaceId)

	idx.initStreamKeyspaceIdState(streamId)

	//clear worker state
	cmd := &MsgStreamUpdate{mType: REMOVE_KEYSPACE_FROM_STREAM,
		streamId:      streamId,
		keyspaceId:    keyspaceId,
		sessionId:     idx.getCurrentSessionId(streamId, keyspaceId),
		abortRecovery: true,
	}

	idx.tkCmdCh <- cmd
	<-idx.tkCmdCh

	idx.mutMgrCmdCh <- cmd
	<-idx.mutMgrCmdCh

	idx.stats.RemoveKeyspaceStats(streamId, keyspaceId)
	idx.distributeKeyspaceStatsMapsToWorkers()
}

func (idx *indexer) monitorKVNodes() {

	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("Indexer::monitorKVNodes crashed: %v\n", r)
			go idx.monitorKVNodes()
		}
	}()

	selfRestart := func() {
		time.Sleep(5000 * time.Millisecond)
		go idx.monitorKVNodes()
	}

	clusterAddr := idx.config["clusterAddr"].String()
	url, err := common.ClusterAuthUrl(clusterAddr)
	if err != nil {
		logging.Errorf("Indexer::monitorKVNodes, error observed while retrieving ClusterAuthUrl, err : %v", err)
		selfRestart()
		return
	}

	scn, err := common.NewServicesChangeNotifier(url, DEFAULT_POOL, "MonitorKvNodes")
	if err != nil {
		logging.Errorf("Indexer::monitorKVNodes, error observed while initializing ServicesChangeNotifier, err: %v", err)
		selfRestart()
		return
	}
	defer scn.Close()

	cinfo, err := common.NewClusterInfoCache(url, DEFAULT_POOL)
	if err != nil {
		logging.Errorf("Indexer::monitorKVNodes, error observed during the initilization of clusterInfoCache, err : %v", err)
		selfRestart()
		return
	}
	cinfo.SetUserAgent("MonitorKVNodes")

	getActiveKVNodes := func() map[string]bool {
		// Get all active KV nodes
		activeKVNodes := cinfo.GetActiveKVNodes()

		// Retrive kv node UUID's of active nodes
		kvNodeUUIDs := make(map[string]bool)
		for _, node := range activeKVNodes {
			kvNodeUUIDs[node.NodeUUID] = true
		}
		return kvNodeUUIDs
	}

	changeInKVNodes := func(prev map[string]bool, curr map[string]bool) bool {
		if len(prev) != len(curr) {
			return true
		} else {
			for nodeuuid, _ := range curr {
				if _, ok := prev[nodeuuid]; !ok {
					return true
				}
			}
		}
		return false
	}

	sendKVNodes := func(kvNodeUUIDs map[string]bool) {
		idx.stateLock.RLock()
		for streamId, keyspaceIdStatus := range idx.streamKeyspaceIdStatus {
			for keyspaceId, _ := range keyspaceIdStatus {

				poolChangeMsg := &MsgPoolChange{
					mType:      POOL_CHANGE,
					nodes:      kvNodeUUIDs,
					streamId:   streamId,
					keyspaceId: keyspaceId,
				}
				idx.internalRecvCh <- poolChangeMsg
			}
		}
		idx.stateLock.RUnlock()
	}

	updateNodeToHostMap := func() {
		allKVNodes := cinfo.GetAllKVNodes()

		currNodeToHostMap := idx.stats.nodeToHostMap.Get()

		// Check if there is any change between currNodeToHostMap, allKVNodes
		updateRequired := false
		if len(currNodeToHostMap) != len(allKVNodes) {
			updateRequired = true
		} else {
			for _, node := range allKVNodes {
				if hostname, ok := currNodeToHostMap[node.NodeUUID]; !ok {
					updateRequired = true
					break
				} else if node.Hostname != hostname {
					logging.Infof("Indexer::monitorKVNodes Hostname for node: %v changed from %v to %v",
						node.NodeUUID, hostname, node.Hostname)
					updateRequired = true
					break
				}
			}
		}

		if updateRequired {
			newNodeToHostMap := make(map[string]string)
			for _, node := range allKVNodes {
				newNodeToHostMap[node.NodeUUID] = node.Hostname
			}

			idx.stats.nodeToHostMap.Set(newNodeToHostMap)
		}
	}

	// Force update the nodeToHostMap for the first time
	if err := cinfo.FetchWithLock(); err != nil {
		logging.Errorf("Indexer::monitorKVNodes, error observed while Fetching cluster info cache, err: %v", err)
		selfRestart()
		return
	}
	updateNodeToHostMap()

	// Incase a pool change notification is missed, periodically sending active
	// list of nodes to timekeeper ensures that timekeeper's book-keeping is
	// always updated with active KV nodes
	ticker := time.NewTicker(10 * time.Minute)

	ch := scn.GetNotifyCh()
	for {
		select {
		case notif, ok := <-ch:
			if !ok {
				selfRestart()
				return
			}

			// Process only PoolChangeNotification as any change to
			// ClusterMembership is reflected only in PoolChangeNotification
			if notif.Type != common.PoolChangeNotification {
				continue
			}

			if err := cinfo.FetchNodesAndSvsInfoWithLock(); err != nil {
				logging.Errorf("Indexer::monitorKVNodes, error observed while Updating cluster info cache, err: %v", err)
				selfRestart()
				return
			}

			currActiveKVNodes := getActiveKVNodes()
			if currActiveKVNodes == nil {
				selfRestart()
				return
			}

			if changeInKVNodes(idx.activeKVNodes, currActiveKVNodes) {
				idx.activeKVNodes = currActiveKVNodes
				sendKVNodes(currActiveKVNodes)
			}

			updateNodeToHostMap()

		case <-ticker.C:
			if err := cinfo.FetchNodesAndSvsInfoWithLock(); err != nil {
				logging.Errorf("Indexer::monitorKVNodes, error observed while Fetching cluster info cache due to timer, err: %v", err)
				selfRestart()
				return
			}

			currActiveKVNodes := getActiveKVNodes()
			if currActiveKVNodes == nil {
				selfRestart()
				return
			}

			idx.activeKVNodes = currActiveKVNodes
			if len(currActiveKVNodes) > 0 {
				sendKVNodes(currActiveKVNodes)
			}
			updateNodeToHostMap()

		case <-idx.shutdownInitCh:
			return
		}
	}
}

func (idx *indexer) ValidateKeyspace(streamId common.StreamId, keyspaceId string,
	bucketUUIDs []string) bool {

	collectionId := idx.streamKeyspaceIdCollectionId[streamId][keyspaceId]

	bucket, scope, collection := SplitKeyspaceId(keyspaceId)

	//if the stream is using a cid, validate collection.
	//otherwise only validate the bucket
	idx.cinfoProviderLock.RLock()
	defer idx.cinfoProviderLock.RUnlock()

	if collectionId == "" {
		if !idx.cinfoProvider.ValidateBucket(bucket, bucketUUIDs) {
			return false
		}
	} else {

		if scope == "" && collection == "" {
			scope = common.DEFAULT_SCOPE
			collection = common.DEFAULT_COLLECTION
		}

		if !idx.cinfoProvider.ValidateCollectionID(bucket,
			scope, collection, collectionId, true) {
			return false
		}
	}
	return true

}

func (idx *indexer) getSnapshotListeners() int {
	snapListners := idx.config["settings.snapshotListeners"].Int()
	if snapListners <= 0 {
		return 1
	}
	return snapListners
}

func (idx *indexer) getSnapshotReqWorkers() int {
	snapReqWorkers := idx.config["settings.snapshotRequestWorkers"].Int()
	if snapReqWorkers <= 0 {
		return 1
	}
	return snapReqWorkers
}

func (idx *indexer) addPendingReset(instances []common.IndexInst) {

	for _, inst := range instances {
		idx.pendingReset[inst.InstId] = true
	}
}

func (idx *indexer) updateRStateForPendingReset(instances []common.IndexInst) {

	for _, inst := range instances {
		if _, ok := idx.pendingReset[inst.InstId]; ok {
			if !inst.IsProxy() &&
				inst.RState != common.REBAL_ACTIVE {
				logging.Infof("Indexer::updateRStateForPendingReset Index Instance %v "+
					"rstate moved to ACTIVE", inst.InstId)
				inst.RState = common.REBAL_ACTIVE
				idx.indexInstMap[inst.InstId] = inst
			}
			delete(idx.pendingReset, inst.InstId)
		}
	}
}

func (idx *indexer) deletePendingReset(instId common.IndexInstId) {
	delete(idx.pendingReset, instId)
}

//restartMaintStreamForCatchup starts the MAINT_STREAM for the given bucket using the
//input restarTs and sets the stream state to STREAM_ACTIVE. It is currently used to
//restart an inactive MAINT_STREAM if a Catchup state index exists in INIT_STREAM to
//facilitate stream merge.
func (idx *indexer) restartMaintStreamForCatchup(bucket string, restartTs *common.TsVbuuid) {

	streamId := common.MAINT_STREAM
	logging.Infof("Indexer::restartMaintStreamForCatchup %v %v %v", streamId, bucket, STREAM_ACTIVE)
	idx.prepareStreamKeyspaceIdForFreshStart(streamId, bucket)
	idx.setStreamKeyspaceIdState(streamId, bucket, STREAM_ACTIVE)
	maintSessionId := idx.genNextSessionId(streamId, bucket)
	idx.startKeyspaceIdStream(streamId, bucket, restartTs, nil, nil, false, false, maintSessionId)
}

// If user enables the feature before upgrade to 7.1 we will not override it.
// But if user disables the feature before upgrade to 7.1 we will still enable it.
// By default feature is disabled, so not handling the case of override in case of disabling a already disabled feature.
func (idx *indexer) enablePlasmaInMemCompression() {

	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("Indexer::enablePlasmaInMemCompression crashed: %v\n", r)
			go idx.enablePlasmaInMemCompression()
		}
	}()

	selfRestart := func() {
		time.Sleep(5000 * time.Millisecond)
		go idx.enablePlasmaInMemCompression()
	}

	checkInMemCompressionEnabled := func() bool {
		inMemCompressionMainIdx := idx.config["plasma.mainIndex.enableInMemoryCompression"].Bool()
		inMemCompressionBackIdx := idx.config["plasma.backIndex.enableInMemoryCompression"].Bool()
		// even if one of the entry is enabled, someone has already enabled/disabled the featuer and hence
		// we dont need to set the feature enable flag
		logging.Debugf("Indexer::enablePlasmaInMemCompression setting value for mainStore:%v, backStore:%v for indexer.",
			inMemCompressionMainIdx, inMemCompressionBackIdx)
		if inMemCompressionMainIdx || inMemCompressionBackIdx {
			return true
		}
		return false
	}

	// send settings request to local http url
	postRequestFn := func() error {
		url := "/settings"
		addr := getLocalHttpAddr(idx.config)
		logging.Debugf("Indexer::enablePlasmaInMemCompression addr : %v .", addr)

		enablePlasmaInMemCompresison := struct {
			Settings1 bool `json:"indexer.plasma.mainIndex.enableInMemoryCompression"`
			Settings2 bool `json:"indexer.plasma.backIndex.enableInMemoryCompression"`
		}{true, true}

		body, err := json.Marshal(enablePlasmaInMemCompresison)
		if err != nil {
			logging.Errorf("Indexer::enablePlasmaInMemCompression error in marshalling settings request, err: %v", err)
			return err
		}

		bodybuf := bytes.NewBuffer(body)

		resp, err := postWithAuth(addr+url, "application/json", bodybuf)
		if err != nil {
			logging.Errorf("Indexer::enablePlasmaInMemCompression error in posting http request for settings change on %v, err: %v",
				addr+url, err)
			return err
		}
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		return nil
	}

	indexerId := common.IndexerId(idx.id)
	enabled := checkInMemCompressionEnabled()
	if enabled {
		logging.Debugf("Indexer::enablePlasmaInMemCompression setting is already enabled for indexer %v.", indexerId)
		return
	}

	time.Sleep(time.Second * 60) // wait 60 seconds on indexer restart to get everything up

	for { // check every 10 secs for cluster version upgrade
		clusterVersion := common.GetClusterVersion()
		if clusterVersion >= common.INDEXER_71_VERSION {
			break
		}
		time.Sleep(time.Second * 10)
	}

	// re check if feature is already enabled while we were looping
	enabled = checkInMemCompressionEnabled()
	if enabled {
		logging.Debugf("Indexer::enablePlasmaInMemCompression setting already enabled for indexer %v.", indexerId)
		return
	}

	// get the metakv value
	exists, err := mc.EnablePlasmaInMemoryCompressionTokenExist()
	if err != nil {
		logging.Errorf("Indexer::enablePlasmaInMemCompression error in getting feature value from metakv for indexer %v, err: %v ",
			indexerId, err)
		selfRestart()
		return
	}
	if exists {
		logging.Debugf("Indexer::enablePlasmaInMemCompression feature is already enabled for indexer %v", indexerId)
		return
	}
	logging.Infof("Indexer::enablePlasmaInMemCompression trying to enable feature for indexer %v", indexerId)

	err = postRequestFn()
	if err != nil {
		selfRestart()
		return
	}
	// update metakv that we are done
	if err := mc.PostEnablePlasmaInMemoryCompressionToken(); err != nil {
		logging.Errorf("Indexer::enablePlasmaInMemCompression error in setting feature value in metakv for indexer %v, err: %v",
			indexerId, err)
		selfRestart()
		return
	}
	logging.Infof("Indexer::enablePlasmaInMemCompression done enabling feature for indexer %v", indexerId)
}
