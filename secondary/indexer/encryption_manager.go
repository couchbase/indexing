// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/audit"

	"github.com/couchbase/gocbcrypto"

	couchbase "github.com/couchbase/indexing/secondary/dcp"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

/*
Method flow.

Cbauth calls EncryptionMgr methods:
1. GetInUseKeysCallback:
	Get keys being used by indexer components
2. RefreshKeysCallback:
	Notify EncryptionMgr about new keys. EncryptionMgr to get keys
3. DropKeysCallback:
	Notify EncryptionMgr to start re-encrypting data being encrypted with the dropKey.

EncryptionMgr calls cbauth methods:
1. RegisterEncryptionKeysCallbacks:
	Register above 3 callbacks to cbauth.
2. GetEncryptionKeysBlocking:
	Get encryption key data from cbauth.
3. KeysDropComplete:
	As DropKeysCallback continues async at indexer, EncryptionMgr notifies cbauth when deletion is complete.

Slice/StatsMgr/ClusterMgrAgent call EncryptionMgr methods:
1. getActiveKeyIdCipher:
	Get active key data from EncryptionMgr for keydatatype.
2. getKeyCipherById:
	Get key data by keyId from EncryptionMgr for keydatatype.
3. SetInUseKeys:
	When a component starts using an encryption key, mark it to return in GetInUseKeysCallback.


	Slice/StatsMgr/ClusterMgrAgent								Cbauth

    getActiveKeyIdCipher ---                                 --- GetInUseKeysCallback
                            \       +---------------+       /
    getKeyCipherById     ------->   | EncryptionMgr |   <------  RefreshKeysCallback
                            /       +---------------+       \
    SetInUseKeys         ---                                 --- DropKeysCallback

Mutex Lock

EncryptionMgr.mu	: Updating/getting cluster keys available at indexer.
EncryptionMgr.muid	: Updating in-use encryption keys by indexer components. Multiple keydatatypes can be updated concurrently.
EncryptionMgr.cbMu	: Avoid key drop/update operation race by mutually exclusive book-keeping.

*/

type EncryptionCallbacks struct {
	GetInUseKeysCallback        func(kdt KeyDataType) ([]string, error)
	RefreshKeysCallback         func(kdt KeyDataType) error
	DropKeysCallback            func(kdt KeyDataType, keyids []string)
	SynchronizeKeyFilesCallback func(kdt KeyDataType) error
}

var errDropWaitCanceled = errors.New("encryption drop wait canceled")

const kdtTypeServiceBucket = "service_bucket"

// logKeyIDs formats key IDs for logging. Empty string IDs are shown as "<unencrypted>".
func logKeyIDs(keyIDs ...string) string {
	if len(keyIDs) == 0 {
		return "<unencrypted>"
	}
	result := make([]string, len(keyIDs))
	for i, id := range keyIDs {
		if id == "" {
			result[i] = "<unencrypted>"
		} else {
			result[i] = id
		}
	}
	return fmt.Sprintf("%v", result)
}

// logKDT formats a KeyDataType for logging.
func logKDT(kdts ...KeyDataType) string {
	result := make([]string, len(kdts))
	for i, kdt := range kdts {
		if kdt.TypeName == kdtTypeServiceBucket {
			result[i] = fmt.Sprintf("{t: %v, bID: %v}", kdt.TypeName, kdt.BucketUUID)
		} else {
			result[i] = fmt.Sprintf("{t: %v}", kdt.TypeName)
		}
	}
	return fmt.Sprintf("%v", result)
}

type EncryptionMgr struct {
	supvCmdch MsgChannel //supervisor sends commands on this channel
	supvMsgch MsgChannel //channel to send any message to supervisor
	config    common.Config

	dataTypeKeyInfoMap map[KeyDataType]*EncrKeysInfo     // Map holds key data received from ns-server
	kdtKeyidKeyMap     map[KeyDataType]map[string]EaRKey // Keeps only available keys
	keyidKdtMap        map[string]KeyDataType            // Map helps searching keydatatype by keyid
	mu                 sync.Mutex

	// Map holds keys being used by indexer sub components
	// As GetInUseKeysCallback expects indexer to return in use keys quickly, only key no longer used can be removed.
	indexerUsedKeyIds map[KeyDataType][]string
	muid              sync.Mutex

	cinfoProvider     common.ClusterInfoProvider
	cinfoProviderLock sync.RWMutex

	testRunning bool
	enableTest  atomic.Bool

	wrkrQueue     MsgChannel // as multiple concurrent encryption messages can be received, they will be queued and processed async after following signals.
	bootstrapDone chan bool  // closed channel means indexer bootstrap is done.
	recoveryDone  chan bool  // closed channel means indexerUsedKeyIds update is completed after inputs from indexer components.
	cachingDone   chan bool  // closed channel means encryptionMgr updated keys for all the buckets available in clusterInfoCache

	isRecoveryDone atomic.Bool // if false, return error for callbacks getInUseKeysCallback

	// This map holds Encryption Messages for KeyDataType when there is already Encryption Message in progress
	// Once previous operation completes, Message from this map is picked up and enqueued to wrkrQueue.
	pendingMap map[KeyDataType][]Message

	dropOrUpdateInProgress map[KeyDataType]Message // Message entry in map means present in wrkrQueue or in progress sent to indexer. Skip enqueuing similar Message.
	cbMu                   sync.Mutex              //Mutex used for read/write of common book-keeping update/drop for callbacks
}

func NewEncryptionMgr(supvCmdch MsgChannel, supvMsgch MsgChannel, config common.Config) (*EncryptionMgr, Message) {

	encryptionMgr := &EncryptionMgr{
		supvCmdch:              supvCmdch,
		supvMsgch:              supvMsgch,
		config:                 config,
		dataTypeKeyInfoMap:     make(map[KeyDataType]*EncrKeysInfo, 0),
		kdtKeyidKeyMap:         make(map[KeyDataType]map[string]EaRKey, 0),
		keyidKdtMap:            make(map[string]KeyDataType),
		indexerUsedKeyIds:      make(map[KeyDataType][]string, 0),
		testRunning:            false,
		wrkrQueue:              make(MsgChannel, WRKR_QUEUE_LEN),
		bootstrapDone:          make(chan bool),
		recoveryDone:           make(chan bool),
		cachingDone:            make(chan bool),
		pendingMap:             make(map[KeyDataType][]Message),
		dropOrUpdateInProgress: make(map[KeyDataType]Message),
	}

	clusterAddr := config["clusterAddr"].String()
	useCinfolite := config["use_cinfo_lite"].Bool()

	cip, err := common.NewClusterInfoProvider(useCinfolite, clusterAddr, common.DEFAULT_POOL, "EncryptionMgr", config)
	if err != nil {
		logging.Errorf("NewEncryptionMgr Unable to get new ClusterInfoProvider in NewEncryptionMgr err: %v use_cinfo_lite: %v",
			err, useCinfolite)

		errMsg := &MsgError{err: Error{code: ERROR_ENCRYPTION_MGR,
			severity: FATAL,
			category: ENCRYPTION_MGR,
			cause:    err,
		},
		}
		return nil, errMsg
	}

	func() {
		encryptionMgr.cinfoProviderLock.Lock()
		defer encryptionMgr.cinfoProviderLock.Unlock()

		encryptionMgr.cinfoProvider = cip
	}()

	encryptionMgr.enableTest.Store(false)
	encryptionMgr.setEnableTest()

	encryptionMgr.isRecoveryDone.Store(false)

	//ENCRYPT_TODO: Remove persisted test keys when test-framework not required
	//keyPersistPath = config["storage_dir"].String()

	go encryptionMgr.cacheKeysForBootstrap()
	go encryptionMgr.recoverInUseKeys()
	go encryptionMgr.run()
	go encryptionMgr.runQueue()

	//ENCRYPT_TODO: Address comment #174 later
	err = RegisterCallbacks(encryptionMgr)
	if err != nil {
		errMsg := &MsgError{err: Error{code: ERROR_ENCRYPTION_MGR,
			severity: FATAL,
			category: ENCRYPTION_MGR,
			cause:    err,
		}}
		return nil, errMsg
	}
	return encryptionMgr, &MsgSuccess{}
}

// Errors
var (
	ErrEncrMgrNotReady = errors.New("EncryptionMgr not ready to process requests")
)

func RegisterCallbacks(e *EncryptionMgr) error {

	cbs := &EncryptionCallbacks{
		GetInUseKeysCallback: func(kdt KeyDataType) ([]string, error) {
			return e.getInUseKeysCallback(kdt)
		},
		RefreshKeysCallback: func(kdt KeyDataType) error {
			return e.refreshKeysCallback(kdt)
		},
		DropKeysCallback: func(kdt KeyDataType, keyids []string) {
			e.dropKeysCallback(kdt, keyids)
		},
		SynchronizeKeyFilesCallback: func(kdt KeyDataType) error {
			return e.synchronizeKeyFilesCallback(kdt)
		},
	}

	err := cbauth.RegisterEncryptionKeysCallbacks(cbs.RefreshKeysCallback, cbs.GetInUseKeysCallback, cbs.DropKeysCallback, cbs.SynchronizeKeyFilesCallback)
	if err == nil {
		return nil
	} else {
		logging.Warnf("EncryptionMgr:RegisterCallbacks err:%v", err)
		return err
	}
	//ENCRYPT_TODO: Remove persisted test keys when test-framework not required
	//cbsTest = cbs
}

func (e *EncryptionMgr) setEnableTest() {

	enableTestVal, ok := e.config["encryption.enable_test"]
	if ok {
		if enableTestVal.Bool() {
			e.enableTest.Store(true)
		} else {
			e.enableTest.Store(false)
		}
		logging.Infof("EncryptionMgr:setEnableTest set to %v", e.enableTest.Load())
	}
}

func (e *EncryptionMgr) run() {
	logging.Infof("EncryptionMgr:run started")
loop:
	for {
		select {
		case cmd, ok := <-e.supvCmdch:
			if ok {
				e.handleSupervisorCommands(cmd)
			} else {
				//supervisor channel closed. exit
				break loop
			}
		}
	}
	logging.Infof("EncryptionMgr:run exited...")
}

func (e *EncryptionMgr) runQueue() {
	logging.Infof("EncryptionMgr:runQueue")
	<-e.cachingDone
	logging.Infof("EncryptionMgr:runQueue caching done...")
	<-e.bootstrapDone
	logging.Infof("EncryptionMgr:runQueue bootstrap done...")
	<-e.recoveryDone
	logging.Infof("EncryptionMgr:runQueue key recovery done...")

	for {
		select {
		case msg, ok := <-e.wrkrQueue:
			if ok {
				e.handleQueueMsg(msg)
			}
		}
	}
}

// Send msg to indexer and start tracking go-routine
func (e *EncryptionMgr) handleQueueMsg(msg Message) {

	e.supvMsgch <- msg

	switch msg.GetMsgType() {
	case ENCRYPTION_UPDATE_KEY:
		//Update will continue in background and will not block the queue
		go e.trackUpdateKeys(msg)

	case ENCRYPTION_DROP_KEY:
		//Drop will continue in background and drop received for same keyIds will be skipped due to e.dropInProgress
		//as it can take hours in some cases
		go e.trackDropKeys(msg)

	default:
		logging.Warnf("EncryptionMgr:handleQueueMsg unexpected msg: %v", msg.GetMsgType().String())
	}
}

func (e *EncryptionMgr) trackUpdateKeys(msg Message) {
	kdt := msg.(*MsgEncryptionUpdateKey).GetKeyDataType()
	respCh := msg.(*MsgEncryptionUpdateKey).GetRespCh()
	keyid := msg.(*MsgEncryptionUpdateKey).GetEarKey().Id

	err := <-respCh
	if err != nil {
		logging.Warnf(
			"EncryptionMgr:trackUpdateKeys err:%v for keydatatype:%v keyid:%v",
			err, logKDT(kdt), logKeyIDs(keyid),
		)
	} else {
		logging.Infof(
			"EncryptionMgr:trackUpdateKeys complete for keydatatype:%v keyid:%v",
			logKDT(kdt), logKeyIDs(keyid),
		)
	}

	e.cbMu.Lock()
	defer e.cbMu.Unlock()

	e.resetDropOrUpdateProgress(kdt)
	nextPendingMsg := e.getNextMessagePending(kdt)
	if nextPendingMsg != nil {
		logging.Infof("EncryptionMgr:trackUpdateKeys complete, enqueuing pending msg for "+
			"keydatatype:%v msg:%v", logKDT(kdt), nextPendingMsg.GetMsgType().String())
		e.enqueue(nextPendingMsg)
		e.setDropOrUpdateInProgress(kdt, nextPendingMsg)
	}
}

func (e *EncryptionMgr) trackDropKeys(msg Message) {
	kdt := msg.(*MsgEncryptionDropKey).GetKeyDataType()
	dropKeyids := msg.(*MsgEncryptionDropKey).GetDropKeyIds()
	respCh := msg.(*MsgEncryptionDropKey).GetRespCh()

	err := <-respCh
	if err != nil {
		logging.Warnf(
			"EncryptionMgr:trackDropKeys err:%v for keydatatype:%v drop keyids:%v",
			err, logKDT(kdt), logKeyIDs(dropKeyids...),
		)
	} else {
		logging.Infof(
			"EncryptionMgr:trackDropKeys complete for keydatatype:%v drop keyids:%v",
			logKDT(kdt), logKeyIDs(dropKeyids...),
		)
		// Mark keys as not in-use
		e.DropInUseKeys(kdt, dropKeyids)
	}

	e.cbMu.Lock()
	defer e.cbMu.Unlock()

	e.resetDropOrUpdateProgress(kdt)

	err2 := cbauth.KeysDropComplete(kdt, err)
	if err2 != nil {
		logging.Warnf(
			"EncryptionMgr:trackDropKeys error during notifying KeysDropComplete kdt:%v err:%v",
			logKDT(kdt), err2,
		)
	}

	nextPendingMsg := e.getNextMessagePending(kdt)
	if nextPendingMsg != nil {
		logging.Infof("EncryptionMgr:trackDropKeys complete, enqueuing"+
			" pending msg for keydatatype:%v msg:%v",
			logKDT(kdt), nextPendingMsg.GetMsgType().String())
		e.enqueue(nextPendingMsg)
		e.setDropOrUpdateInProgress(kdt, nextPendingMsg)
	}
}

func (e *EncryptionMgr) enqueue(cmd Message) {
	e.wrkrQueue <- cmd
}

// Protected by e.cbMu
func (e *EncryptionMgr) setDropOrUpdateInProgress(kdt KeyDataType, msg Message) {
	e.dropOrUpdateInProgress[kdt] = msg
}

// Protected by e.cbMu
func (e *EncryptionMgr) resetDropOrUpdateProgress(kdt KeyDataType) {
	delete(e.dropOrUpdateInProgress, kdt)
}

// Protected by e.cbMu
func (e *EncryptionMgr) getDropOrUpdateInProgress(kdt KeyDataType) Message {
	if msg, ok := e.dropOrUpdateInProgress[kdt]; ok {
		return msg
	}
	return nil
}

// For a keydatatype, this function return first entry in slice holding Messages to be processed
// Protected by e.cbMu
func (e *EncryptionMgr) getNextMessagePending(kdt KeyDataType) Message {

	_, ok := e.pendingMap[kdt]
	if !ok {
		return nil
	} else {
		if len(e.pendingMap[kdt]) == 0 {
			return nil
		}
		//Get first message of the slice and return for further processing
		//Remove the message from slice
		message := e.pendingMap[kdt][0]
		e.pendingMap[kdt] = e.pendingMap[kdt][1:]

		return message
	}
	return nil
}

// For a keydatatype, this function adds message entry in slice holding Messages to be processed
// This will be done when there is already message in progress for the same keydatatype
// Protected by e.cbMu
func (e *EncryptionMgr) addMessagePending(kdt KeyDataType, msg Message) {

	//If e.pendingMap[kdt] slice is empty, result will contain only msg
	e.pendingMap[kdt] = append(e.pendingMap[kdt], msg)
}

// For a keydatatype, this function return first entry in slice holding Messages to be processed
// Protected by e.cbMu
func (e *EncryptionMgr) isDuplicatePending(kdt KeyDataType, msg Message) bool {

	for _, pendingMsg := range e.pendingMap[kdt] {
		if e.isDuplicateMsg(msg, pendingMsg) {
			return true
		}
	}
	return false
}

func (e *EncryptionMgr) isDuplicateMsg(msg, msgActive Message) bool {

	//Drop and Update are not duplicate
	msgType := msg.GetMsgType()
	if msgType != msgActive.GetMsgType() {
		return false
	}

	switch msgType {
	case ENCRYPTION_UPDATE_KEY:
		keyid1 := msg.(*MsgEncryptionUpdateKey).GetEarKey().Id
		keyid2 := msgActive.(*MsgEncryptionUpdateKey).GetEarKey().Id
		if keyid1 != keyid2 {
			return false
		}

	case ENCRYPTION_DROP_KEY:
		dropKeyids := msg.(*MsgEncryptionDropKey).GetDropKeyIds()
		dropKeyidsActive := msgActive.(*MsgEncryptionDropKey).GetDropKeyIds()

		// If there is at least one different entry in dropKeyids, enqueue it.
		activeMap := make(map[string]bool)
		for _, key := range dropKeyidsActive {
			activeMap[key] = true
		}
		for _, key := range dropKeyids {
			if _, ok := activeMap[key]; !ok {
				return false
			}
		}
	}

	return true
}

func (e *EncryptionMgr) handleSupervisorCommands(cmd Message) {
	switch cmd.GetMsgType() {
	case CONFIG_SETTINGS_UPDATE:
		e.handleConfigUpdate(cmd)
	case CLUST_MGR_INDEXER_READY:
		e.handleIndexerReady(cmd)
	}
}

func (e *EncryptionMgr) handleConfigUpdate(cmd Message) {
	cfgUpdate := cmd.(*MsgConfigUpdate)
	newConfig := cfgUpdate.GetConfig()

	newUseCInfoLite := newConfig["use_cinfo_lite"].Bool()
	oldUseCInfoLite := e.config["use_cinfo_lite"].Bool()
	clusterAddr := newConfig["clusterAddr"].String()

	if oldUseCInfoLite != newUseCInfoLite {
		logging.Infof("EncryptionMgr:handleConfigUpdate Updating ClusterInfoProvider")

		cip, err := common.NewClusterInfoProvider(newUseCInfoLite, clusterAddr, common.DEFAULT_POOL, "EncryptionMgr", newConfig)
		if err != nil {
			logging.Errorf("EncryptionMgr:handleConfigUpdate Unable to update ClusterInfoProvider in EncryptionMgr err: %v, use_cinfo_lite: old %v new %v",
				err, oldUseCInfoLite, newUseCInfoLite)
			common.CrashOnError(err)
		}

		oldPtr := e.cinfoProvider

		func() {
			e.cinfoProviderLock.Lock()
			defer e.cinfoProviderLock.Unlock()

			e.cinfoProvider = cip
		}()

		logging.Infof("LifecycleMgr:handleConfigUpdate Updated ClusterInfoProvider in LifecycleMgr use_cinfo_lite: old %v new %v",
			oldUseCInfoLite, newUseCInfoLite)
		oldPtr.Close()
	}
	e.config = cfgUpdate.GetConfig()

	e.setEnableTest()
	//ENCRYPT_TODO: Remove persisted test keys when test-framework not required
	//e.RegisterRestEndpoints()

	e.supvCmdch <- &MsgSuccess{}
}

func (e *EncryptionMgr) handleIndexerReady(cmd Message) {
	logging.Infof("EncryptionMgr:handleIndexerReady...")
	close(e.bootstrapDone)
	e.supvCmdch <- &MsgSuccess{}
}

func (e *EncryptionMgr) cacheKeysForBootstrap() {

	//ENCRYPT_TODO: Remove persisted test keys when test-framework not required
	//recoverPersistedKeys()

	// Cache keys for buckets
	var buckets []couchbase.BucketName
	func() {
		e.cinfoProviderLock.RLock()
		defer e.cinfoProviderLock.RUnlock()

		buckets = e.cinfoProvider.GetBucketNames()
	}()
	logging.Infof("EncryptionMgr:caching keys for buckets %v", buckets)

	for _, bucket := range buckets {
		if bucket.UUID == common.BUCKET_UUID_NIL {
			continue
		}
		kdt := KeyDataType{TypeName: kdtTypeServiceBucket, BucketUUID: bucket.UUID}
		ctx := context.Background()
		encrKeysInfo, err := cbauth.GetEncryptionKeysBlocking(ctx, kdt)
		if err != nil {
			logging.Fatalf("EncryptionMgr:caching keys for bucket:%v err:%v", bucket.Name, err)
			panic(err)
		}
		e.SetClusterEncrKeysInfo(kdt, encrKeysInfo)
		logging.Infof("EncryptionMgr:cached keys for bucket %v", bucket)
	}

	// ENCRYPT_TODO: Cache keys for log, config, audit
	//kdt := KeyDataType{TypeName: "log", BucketUUID: ""}
	//encrKeysInfo := cbmockGetEncryptionKeysBlocking(kdt)
	//e.SetClusterEncrKeysInfo(kdt, encrKeysInfo)

	ctx := context.Background()
	encrKeysInfo, err := cbauth.GetEncryptionKeysBlocking(ctx, MetadataKDT)
	if err != nil {
		logging.Fatalf("EncryptionMgr:caching keys for config err:%v", err)
		panic(err)
	}
	e.SetClusterEncrKeysInfo(MetadataKDT, encrKeysInfo)
	logging.Infof("EncryptionMgr:cached keys for %v", logKDT(MetadataKDT))

	//
	//kdt = KeyDataType{TypeName: "audit", BucketUUID: ""}
	//encrKeysInfo = cbmockGetEncryptionKeysBlocking(kdt)
	//e.SetClusterEncrKeysInfo(kdt, encrKeysInfo)

	close(e.cachingDone)
}

// mergeMap removes duplicate key entries in slice for the datatypes
func mergeMap(map1, map2 map[KeyDataType][]string) map[KeyDataType][]string {

	kdtKeyBoolMap := make(map[KeyDataType]map[string]bool)

	for kdt, slice := range map1 {
		keyBoolMap, ok := kdtKeyBoolMap[kdt]
		if !ok {
			keyBoolMap = make(map[string]bool)
			kdtKeyBoolMap[kdt] = keyBoolMap
		}
		for _, s := range slice {
			keyBoolMap[s] = true
		}
	}

	for kdt, slice := range map2 {
		keyBoolMap, ok := kdtKeyBoolMap[kdt]
		if !ok {
			keyBoolMap = make(map[string]bool)
			kdtKeyBoolMap[kdt] = keyBoolMap
		}
		for _, s := range slice {
			keyBoolMap[s] = true
		}
	}

	respMap := make(map[KeyDataType][]string)
	for kdt, keyBoolMap := range kdtKeyBoolMap {
		s := make([]string, 0)
		for key, _ := range keyBoolMap {
			s = append(s, key)
		}
		respMap[kdt] = s
	}
	return respMap
}

func (e *EncryptionMgr) recoverInUseKeys() {
	// ENCRYPT_TODO: revisit & remove if all components do setInUseKeys during warmup
	logging.Infof("EncryptionMgr:recoverInUseKeys...")
	<-e.bootstrapDone

	// This map will store in-use keys by union/merge of keys from all the components.
	allKdtKeys := make(map[KeyDataType][]string)
	logging.Infof("EncryptionMgr:recoverInUseKeys sending msg to components...")

	// This message is not for specific bucket thus bucketUUID is empty. This will get keys in use for all buckets.
	respMapCh := make(chan map[KeyDataType][]string)
	e.supvMsgch <- &MsgEncryptionGetInuseKeys{
		keyDataType: KeyDataType{TypeName: kdtTypeServiceBucket, BucketUUID: ""},
		respMapCh:   respMapCh,
	}
	kdtKeysMap := <-respMapCh
	allKdtKeys = mergeMap(kdtKeysMap, allKdtKeys)

	// ENCRYPT_TODO: Add other key data types later
	//e.supvMsgch <- &MsgEncryptionGetInuseKeys{keyDataType: KeyDataType{TypeName: "log", BucketUUID: ""}, respMapCh: respMapCh}
	//kdtKeysMap = <-respMapCh
	//allKdtKeys = mergeMap(kdtKeysMap, allKdtKeys)

	configKeysCh := make(chan *common.Optional[[]string])
	e.supvMsgch <- &MsgClustMgrGetInuseKeys{respCh: configKeysCh}
	configKeys, ok := (<-configKeysCh).Get()
	if !ok {
		logging.Warnf("EncryptionMgr:recoveryInUseKeys failed to get metadata store keys")
	} else {
		kdtKeysMap[MetadataKDT] = configKeys
	}
	allKdtKeys = mergeMap(kdtKeysMap, allKdtKeys)

	//e.supvMsgch <- &MsgEncryptionGetInuseKeys{keyDataType: KeyDataType{TypeName: "audit", BucketUUID: ""}, respMapCh: respMapCh}
	//kdtKeysMap = <-respMapCh
	//allKdtKeys = mergeMap(kdtKeysMap, allKdtKeys)

	for kdt, keys := range allKdtKeys {
		for _, key := range keys {
			e.SetInUseKeys(kdt, key)
		}
	}
	logging.Infof("EncryptionMgr:recoverInUseKeys done...")
	e.isRecoveryDone.Store(true)
	close(e.recoveryDone)
}

func (e *EncryptionMgr) RegisterRestEndpoints() {

	mux := GetHTTPMux()
	mux.HandleFunc("/encryption/GetInUseKeys", e.getInUseKeysHandler)
	if e.testRunning == false && e.enableTest.Load() {
		//mux.HandleFunc("/test/RefreshKeysCallback", addEncryptionKey)
		//mux.HandleFunc("/test/DropKeysCallback", dropEncryptionKey)
		//mux.HandleFunc("/test/DisableEncryption", disableEncryption)
		//mux.HandleFunc("/test/GetInUseKeys", getInUseKeys)
		//mux.HandleFunc("/test/GetToBeUsedKeys", getToBeUsedKeys)

		// ENCRYPT_TODO: Remove endpoints when test-framework not required
		e.testRunning = true
	}
}

// Get keyids which are being used by indexer components to encrypt data
func (e *EncryptionMgr) getInUseKeysHandler(w http.ResponseWriter, r *http.Request) {

	// ENCRYPT_TODO: Add params for specific keydatatype later.
	valid := validateAuth(w, r)
	if !valid {
		return
	}

	kdtKeysMap, err := e.GetInUseKeysAll()
	if err != nil {
		err2 := fmt.Errorf("Error getting in use keys err:%v", err.Error())
		http.Error(w, err2.Error(), http.StatusServiceUnavailable)
		return
	}

	// key part of json is string
	kdtstrKeysMap := make(map[string][]string)
	for k, v := range kdtKeysMap {
		newKey := fmt.Sprintf("%v", k)
		kdtstrKeysMap[newKey] = v
	}
	data, err := json.Marshal(kdtstrKeysMap)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", fmt.Sprintf("%v", len(data)))
	w.WriteHeader(http.StatusOK)
	w.Write(data)
	return
}

func (e *EncryptionMgr) GetInUseKeys(kdt KeyDataType) ([]string, error) {

	if !e.isRecoveryDone.Load() {
		return []string{}, ErrEncrMgrNotReady
	}
	logging.Infof("EncryptionMgr:GetInUseKeys %v", logKDT(kdt))
	e.muid.Lock()
	defer e.muid.Unlock()

	keyids, ok := e.indexerUsedKeyIds[kdt]
	if !ok {
		logging.Warnf("EncryptionMgr:GetInUseKeys no in-use keys for type:%v uuid:%v", kdt.TypeName, kdt.BucketUUID)
		return []string{}, nil
	}

	logging.Infof("EncryptionMgn:GetInUseKeys %v - %v", logKDT(kdt), logKeyIDs(keyids...))

	return keyids, nil
}

func (e *EncryptionMgr) GetInUseKeysAll() (map[KeyDataType][]string, error) {

	kdtKeysMap := make(map[KeyDataType][]string)
	if !e.isRecoveryDone.Load() {
		logging.Warnf("EncryptionMgr:GetInUseKeysAll err:%v", ErrEncrMgrNotReady)
		return kdtKeysMap, ErrEncrMgrNotReady
	}
	logging.Infof("EncryptionMgr:GetInUseKeysAll")
	e.muid.Lock()
	defer e.muid.Unlock()

	for key, value := range e.indexerUsedKeyIds {
		sliceCopy := make([]string, len(value))
		copy(sliceCopy, value)
		kdtKeysMap[key] = sliceCopy
	}

	return kdtKeysMap, nil
}

// When any of the component starts to use key for encryption, use this to update book-keeping
func (e *EncryptionMgr) SetInUseKeys(kdt KeyDataType, keyId string) {
	e.muid.Lock()
	defer e.muid.Unlock()

	keyids, ok := e.indexerUsedKeyIds[kdt]
	if !ok || keyids == nil {
		keys := make([]string, 0)
		keys = append(keys, keyId)
		e.indexerUsedKeyIds[kdt] = keys
		logging.Infof("EncryptionMgr:SetInUseKeys new KeyDataType:%v KeyId:%v",
			logKDT(kdt), logKeyIDs(keyId))
	} else {
		for _, k := range keyids {
			if keyId == k {
				// Info logging is done only when if added to map or dropped from map
				logging.Verbosef(
					"EncryptionMgr:SetInUseKeys key already in use keys map for keydatatype:%v keyid:%v",
					logKDT(kdt), logKeyIDs(keyId),
				)
				return
			}
		}
		e.indexerUsedKeyIds[kdt] = append(e.indexerUsedKeyIds[kdt], keyId)
		logging.Infof("EncryptionMgr:SetInUseKeys KeyDataType:%v KeyId:%v",
			logKDT(kdt), logKeyIDs(keyId))
	}
}

func (e *EncryptionMgr) DropInUseKeys(kdt KeyDataType, dropkeys []string) {
	e.muid.Lock()
	defer e.muid.Unlock()

	logging.Infof("EncryptionMgr:DropInUseKeys %v %v", logKDT(kdt), logKeyIDs(dropkeys...))
	newKeyids := make([]string, 0)
	keyids, ok := e.indexerUsedKeyIds[kdt]

	dropMap := make(map[string]bool)
	for _, k := range dropkeys {
		dropMap[k] = true
	}

	if ok && keyids != nil {
		for _, key := range keyids {
			if _, ok := dropMap[key]; !ok {
				newKeyids = append(newKeyids, key)
			} else {
				logging.Infof(
					"EncryptionMgr:DropInUseKeys %v deleting in-use key:%v",
					logKDT(kdt), logKeyIDs(key),
				)
			}
		}
		e.indexerUsedKeyIds[kdt] = newKeyids
	}
}

func (e *EncryptionMgr) GetClusterEncrKeysInfo(kdt KeyDataType) (EncrKeysInfo, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	encrKeysInfo, ok := e.dataTypeKeyInfoMap[kdt]
	if ok && encrKeysInfo != nil {
		return *encrKeysInfo, nil
	}
	return EncrKeysInfo{}, fmt.Errorf("EncryptionMgr:GetClusterEncrKeysInfo unable to find keys for type:%v uuid:%v", kdt.TypeName, kdt.BucketUUID)
}

func (e *EncryptionMgr) SetClusterEncrKeysInfo(kdt KeyDataType, info *EncrKeysInfo) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.SetClusterEncrKeysInfoNoLock(kdt, info)
}

func (e *EncryptionMgr) SetClusterEncrKeysInfoNoLock(kdt KeyDataType, info *EncrKeysInfo) {
	var dkinfo EncrKeysInfo
	dkinfo = *info

	encrKeysInfo, ok := e.dataTypeKeyInfoMap[kdt]
	if !ok || encrKeysInfo == nil {
		//New entry
		e.dataTypeKeyInfoMap[kdt] = &dkinfo
	} else {
		//Override current entry
		e.dataTypeKeyInfoMap[kdt] = &dkinfo
	}
	e.setKeyCacheByKeyid(kdt, &dkinfo)
}

// Check if caller function holds EncryptionMgr.mu mutex.
func (e *EncryptionMgr) setKeyCacheByKeyid(kdt KeyDataType, info *EncrKeysInfo) {
	// Caller updates entire DeksInfo in dataTypeKeyInfoMap thus new map is created
	e.kdtKeyidKeyMap[kdt] = make(map[string]EaRKey)
	for _, k := range info.Keys {
		e.kdtKeyidKeyMap[kdt][k.Id] = k
		e.keyidKdtMap[k.Id] = kdt
	}
}

// Check if caller function holds EncryptionMgr.mu mutex.
func (e *EncryptionMgr) getKeyCacheByKeyid(kdt KeyDataType, keyid string) (EaRKey, error) {

	if keyidKeymap, ok := e.kdtKeyidKeyMap[kdt]; !ok {
		return EaRKey{}, fmt.Errorf("EncryptionMgr:getKeyCacheByKeyid invalid key for keydatatype")
	} else {
		if earkey, ok := keyidKeymap[keyid]; !ok {
			return EaRKey{}, fmt.Errorf("EncryptionMgr:getKeyCacheByKeyid invalid key for keyid")
		} else {
			return earkey, nil
		}
	}
}

// This call supports key search across all the key datatypes by keyId.
// If keyId is not present in EncryptionMgr, attempt is made to get from cbauth
func (e *EncryptionMgr) getKeyCipherById(keyId string) ([]byte, string) {

	if keyId == "" {
		return []byte{}, gocbcrypto.CipherNameNone
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	kdt, ok := e.keyidKdtMap[keyId]
	if !ok {
		// kdt for the keyid is not cached thus try getting kdt for all the possible types.

		// KeyDatatype: service_bucket, try for all bucket_UUIDs
		var buckets []couchbase.BucketName
		func() {
			e.cinfoProviderLock.RLock()
			defer e.cinfoProviderLock.RUnlock()

			buckets = e.cinfoProvider.GetBucketNames()
		}()
		kdtFound := false
		for _, bucket := range buckets {
			if bucket.UUID == common.BUCKET_UUID_NIL {
				continue
			}
			currKdt := KeyDataType{TypeName: kdtTypeServiceBucket, BucketUUID: bucket.UUID}
			ctx := context.Background()
			encrKeysInfo, err := cbauth.GetEncryptionKeysBlocking(ctx, currKdt)
			if err != nil {
				logging.Fatalf(
					"EncryptionMgr:getKeyCipherById GetEncryptionKeysBlocking for keyid:%v bucket:%v err:%v",
					logKeyIDs(keyId), bucket.Name, err,
				)
				panic(err)
			}
			for _, earkey := range encrKeysInfo.Keys {
				if earkey.Id == keyId {
					kdtFound = true
					// Update encryptionMgr cache as keyids are unique and currKdt has key with keyId
					e.SetClusterEncrKeysInfoNoLock(currKdt, encrKeysInfo)
					return earkey.Key, earkey.Cipher
				}
			}
		}

		// ENCRYPT_TODO: Try getting keys for keydatatypes log, config, audit when being used to match with keyId.

		// No keydata is available for keyId for all possible keydatatypes
		if !kdtFound {
			return []byte{}, gocbcrypto.CipherNameNone
		}

	} else {
		earkey, err := e.getKeyCacheByKeyid(kdt, keyId)
		if err != nil {
			logging.Warnf("EncryptionMgr:getKeyCipherById for key:%v err:%v",
				logKeyIDs(keyId), err.Error())
		} else {
			return earkey.Key, earkey.Cipher
		}
	}

	ctx := context.Background()
	encrKeysInfo, err := cbauth.GetEncryptionKeysBlocking(ctx, kdt)
	if err != nil {
		logging.Fatalf("EncryptionMgr:getKeyCipherById retry for key:%v err:%v",
			logKeyIDs(keyId), err.Error())
		panic(err)
	}
	// Update encryption related info
	e.SetClusterEncrKeysInfoNoLock(kdt, encrKeysInfo)

	// Try getting key for keyId as keyinfo is updated
	kdt, ok = e.keyidKdtMap[keyId]
	if !ok {
		logging.Warnf("EncryptionMgr:getKeyCipherById retry could not find keydatatype for key:%v",
			logKeyIDs(keyId))
	} else {
		earkey, err := e.getKeyCacheByKeyid(kdt, keyId)
		if err != nil {
			logging.Warnf("EncryptionMgr:getKeyCipherById retry for key:%v err:%v",
				logKeyIDs(keyId), err.Error())
		} else {
			return earkey.Key, earkey.Cipher
		}
	}

	return []byte{}, gocbcrypto.CipherNameNone
}

// Get active key for keydatatype
// If key data is not present in EncryptionMgr, attempt is made to get from cbauth
func (e *EncryptionMgr) getActiveKeyIdCipher(typename, bucketUUID string) ([]byte, string, string) {

	kdt := KeyDataType{TypeName: typename, BucketUUID: bucketUUID}
	encrKeysInfo, err := e.GetClusterEncrKeysInfo(kdt)
	if err != nil {
		logging.Warnf(
			"EncryptionMgr:getActiveKeyIdCipher cache no key data for keydatatype: %v",
			logKDT(kdt),
		)
	} else {
		key, keyid, cipher := getActiveKeyIdCipherFromEncrKeysInfo(encrKeysInfo)
		if len(key) == 0 && keyid == "" && cipher == "" {
			//Encryption disabled for keydatatype thus keydata can be empty
			return key, keyid, gocbcrypto.CipherNameNone
		} else if len(key) == 0 || keyid == "" || cipher == "" {
			//Try getting latest info from cbauth
			logging.Warnf(
				"EncryptionMgr:getActiveKeyIdCipher cache no key data, keylength:%d keyid:%v cipher:%v",
				len(key), logKeyIDs(keyid), cipher,
			)
		} else {
			return key, keyid, cipher
		}
	}

	// GetEncryptionKeysBlocking normally return error only if ctx is getting cancelled otherwise errors are hard failures.
	// Return error is those cases.
	ctx := context.Background()
	encrKeysInfoPtr, err := cbauth.GetEncryptionKeysBlocking(ctx, kdt)
	if err != nil {
		logging.Fatalf("EncryptionMgr:getActiveKeyIdCipher cbauth for keydatatype: %v err:%v",
			logKDT(kdt), err.Error())
		panic(err)
	}
	// Update encryption related info
	e.SetClusterEncrKeysInfo(kdt, encrKeysInfoPtr)

	// Try getting active key as info is updated
	encrKeysInfo, err = e.GetClusterEncrKeysInfo(KeyDataType{TypeName: typename, BucketUUID: bucketUUID})
	if err != nil {
		//It is safe to assume Key data is present in encrKeysInfo from GetEncryptionKeysBlocking, still log warning if key data has some fields missing
		logging.Warnf("EncryptionMgr:getActiveKeyIdCipher cbauth no key data for keydatatype: %v",
			logKDT(kdt))
	} else {
		key, keyid, cipher := getActiveKeyIdCipherFromEncrKeysInfo(encrKeysInfo)
		if len(key) == 0 && keyid == "" && cipher == "" {
			//Encryption disabled for keydatatype thus keydata can be empty
			return key, keyid, gocbcrypto.CipherNameNone
		} else if len(key) == 0 || keyid == "" || cipher == "" {
			//It is safe to assume activeKey is present in encrKeys from GetEncryptionKeysBlocking, still log warning if key data has some fields missing
			logging.Warnf(
				"EncryptionMgr:getActiveKeyIdCipher cbauth no key data keylength:%d keyid:%v cipher:%v",
				len(key), logKeyIDs(keyid), cipher,
			)
			return []byte{}, "", gocbcrypto.CipherNameNone
		} else {
			return key, keyid, cipher
		}
	}

	// Encryption can be disabled i.e. empty active key.
	return []byte{}, "", gocbcrypto.CipherNameNone
}

func getActiveKeyIdCipherTest(typename, bucketUUID string) ([]byte, string, string) {
	// empty for storage test
	return []byte{}, "", gocbcrypto.CipherNameNone
}

func getKeyCipherByIdTest(keyId string) ([]byte, string) {
	// empty for storage test
	return []byte{}, gocbcrypto.CipherNameNone
}

func setInUseKeysTest(kdt KeyDataType, keyId string) {
	// no-op for storage test
}

// For storage test
var EncrCbsTest = SliceEncryptionCallbacks{
	getActiveKeyIdCipher: getActiveKeyIdCipherTest,
	getKeyCipherById:     getKeyCipherByIdTest,
	setInUseKeys:         setInUseKeysTest,
}

// ns-server expects service to cache in-use keys and return callback quickly
func (e *EncryptionMgr) getInUseKeysCallback(kdt KeyDataType) ([]string, error) {
	logging.Infof("EncryptionMgr:getInUseKeysCallback %v", logKDT(kdt))
	return e.GetInUseKeys(kdt)
}

// This can be called concurrently by many callers.
func (e *EncryptionMgr) refreshKeysCallback(kdt KeyDataType) error {

	logging.Infof("EncryptionMgr:refreshKeysCallback %v", logKDT(kdt))

	// ENCRYPT_TODO: Enable refreshKeysCallback for other datatypes later when being used.

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	encrKeysInfo, err := cbauth.GetEncryptionKeysBlocking(ctx, kdt)
	if err != nil {
		logging.Fatalf("EncryptionMgr:RefreshKeysCallback GetEncryptionKeysBlocking err:%v", err)
		if err == context.DeadlineExceeded {
			return err
		}
		// If the keys are not available when you call GetEncryptionKeys() from the refreshKeysCallback callback, it is a hard error
		panic(err)
	}
	e.SetClusterEncrKeysInfo(kdt, encrKeysInfo)

	earkey := getActiveEarKeyFromEncrKeysInfo(*encrKeysInfo)

	respCh := make(chan error, 1)
	msg := &MsgEncryptionUpdateKey{kdt, earkey, respCh}

	e.cbMu.Lock()
	defer e.cbMu.Unlock()

	//Skip sending update for active key if update is already getting processed
	activeMsg := e.getDropOrUpdateInProgress(kdt)

	// As the there is ongoing key drop/update, skip msg adding to wrkrQueue and check if this can be added to pending map.
	if activeMsg != nil {
		logging.Warnf(
			"EncryptionMgr:RefreshKeysCallback: drop/update in progress for keydatatype:%v keyid:%v.",
			logKDT(kdt), logKeyIDs(earkey.Id),
		)

		// As the current msg is duplicate of ongoing update, this msg can be skipped from enqueuing.
		skipMsg := e.isDuplicateMsg(msg, activeMsg)
		if skipMsg {
			logging.Warnf(
				"EncryptionMgr:RefreshKeysCallback:duplicate to active msg for keydatatype:%v keyid:%v.",
				logKDT(kdt), logKeyIDs(earkey.Id),
			)
			return fmt.Errorf("Duplicate to ongoing request")
		}

		skipAddPending := e.isDuplicatePending(kdt, msg)
		if !skipAddPending {
			// As the current msg is not present in pendingMap and update/drop key is happening for same keydatatype, this msg can be added to pending map.
			logging.Warnf(
				"EncryptionMgr:RefreshKeysCallback added to pending msg for keydatatype:%v keyid:%v.",
				logKDT(kdt), logKeyIDs(earkey.Id),
			)
			e.addMessagePending(kdt, msg)
			return nil
		}

		// As the current msg is present in pendingMap and update/drop key is happening for same keydatatype, this msg can be skipped from adding to pending map.
		logging.Warnf(
			"EncryptionMgr:RefreshKeysCallback:duplicate to pending msg for keydatatype:%v keyid:%v.",
			logKDT(kdt), logKeyIDs(earkey.Id),
		)
		return fmt.Errorf("Duplicate to pending request ")
	}

	// As the there is no ongoing key drop/update, adding msg to wrkrQueue
	logging.Infof(
		"EncryptionMgr:RefreshKeysCallback:Starting for keydatatype:%v keyid:%v",
		logKDT(kdt), logKeyIDs(earkey.Id),
	)
	e.enqueue(msg)
	e.setDropOrUpdateInProgress(kdt, msg)
	return nil
}

// This method is not expected to return error to caller.
// If keyids contain "", it is expected to encrypt the unencrypted data
func (e *EncryptionMgr) dropKeysCallback(kdt KeyDataType, keyids []string) {

	logging.Infof("EncryptionMgr:DropKeysCallback:Received for keydatatype:%v dropkeyids:%v",
		logKDT(kdt), logKeyIDs(keyids...))

	// ENCRYPT_TODO: Enable dropKeysCallback for other datatypes later when being used.

	// Use latest key data from cbauth for dropKey of keydatatype
	// If keys are not present, send error to cbauth using KeysDropComplete, otherwise it is expected to treat it as hard error.
	encrKeysInfo, err := cbauth.GetEncryptionKeys(kdt)
	if err != nil {
		if err == cbauth.ErrKeysNotAvailable {
			logging.Warnf("EncryptionMgr:DropKeysCallback error during GetEncryptionKeys kdt:%v err:%v",
				logKDT(kdt), err)
			err2 := cbauth.KeysDropComplete(kdt, err)
			if err2 != nil {
				logging.Warnf(
					"EncryptionMgr:DropKeysCallback error during notifying KeysDropComplete kdt:%v err:%v",
					logKDT(kdt), err2)
			}
			return
		}
		logging.Fatalf("EncryptionMgr:DropKeysCallback could not get key data kdt:%v err:%v", logKDT(kdt), err)
		panic(err)
	}

	// This updates keys cache
	e.SetClusterEncrKeysInfo(kdt, encrKeysInfo)

	earkey := getActiveEarKeyFromEncrKeysInfo(*encrKeysInfo)

	for _, dropKey := range keyids {
		// DropKeys should not include active keyid
		if dropKey == earkey.Id {
			err2 := cbauth.KeysDropComplete(kdt, fmt.Errorf("DropKey received for active keyid"))
			if err2 != nil {
				logging.Warnf(
					"EncryptionMgr:DropKeysCallback error activeKeyId:%v in dropkeyids kdt:%v,"+
						" error during notifying KeysDropComplete err:%v",
					logKeyIDs(dropKey), logKDT(kdt), err2,
				)
			} else {
				logging.Warnf(
					"EncryptionMgr:DropKeysCallback error activeKeyId:%v in dropkeyids kdt:%v",
					logKeyIDs(earkey.Id), logKDT(kdt),
				)
			}
			return
		}
	}

	respCh := make(chan error, 1)
	msg := &MsgEncryptionDropKey{kdt, keyids, earkey, respCh}

	e.cbMu.Lock()
	defer e.cbMu.Unlock()

	//Skip sending drop for keys if drop is already getting processed
	activeMsg := e.getDropOrUpdateInProgress(kdt)
	// As the there is ongoing key drop/update, skip msg adding to wrkrQueue
	// If msg is not duplicate of ongoing update or also not present in pendingMap, add it to pendingMap.
	if activeMsg != nil {
		logging.Warnf(
			"EncryptionMgr:DropKeysCallback: drop/update in progress for keydatatype:%v activekeyid:%v.",
			logKDT(kdt), logKeyIDs(earkey.Id),
		)

		// As the current msg is duplicate of ongoing drop, this msg can be skipped from enqueuing.
		skipMsg := e.isDuplicateMsg(msg, activeMsg)
		if skipMsg {
			logging.Warnf(
				"EncryptionMgr:DropKeysCallback:duplicate to active msg for keydatatype:%v keyids:%v",
				logKDT(kdt), logKeyIDs(keyids...),
			)
			return
		}

		// As the current msg is also present in pendingMap, this msg can be skipped from adding to pending map.
		skipAddPending := e.isDuplicatePending(kdt, msg)
		if !skipAddPending {
			logging.Warnf(
				"EncryptionMgr:DropKeysCallback added to pending msg for keydatatype:%v keyid:%v.",
				logKDT(kdt), logKeyIDs(earkey.Id),
			)
			e.addMessagePending(kdt, msg)
			return
		}

		logging.Warnf(
			"EncryptionMgr:DropKeysCallback:duplicate to pending msg for keydatatype:%v keyid:%v.",
			logKDT(kdt), logKeyIDs(earkey.Id),
		)
		return
	}

	logging.Infof(
		"EncryptionMgr:DropKeysCallback:Starting for keydatatype:%v using active keyid:%v",
		logKDT(kdt), logKeyIDs(earkey.Id),
	)
	e.enqueue(msg)
	e.setDropOrUpdateInProgress(kdt, msg)
}

func (e *EncryptionMgr) synchronizeKeyFilesCallback(kdt KeyDataType) error {
	// ENCRYPT_TODO: Add synchronizeKeyFilesCallback for rebalance
	logging.Infof("EncryptionMgr:synchronizeKeyFilesCallback...")
	return nil
}

var MetadataKDT = KeyDataType{TypeName: "other"}

func (e *EncryptionMgr) getActiveKeyCipherMetadataCb() (*EaRKey, error) {
	key, id, cipher := e.getActiveKeyIdCipher(
		MetadataKDT.TypeName, MetadataKDT.BucketUUID,
	)
	return &EaRKey{
		Id:     id,
		Key:    key,
		Cipher: cipher,
	}, nil
}

func (e *EncryptionMgr) getKeyCipherByIDMetadataCb(keyID common.KeyID) (*EaRKey, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	key, err := e.getKeyCacheByKeyid(MetadataKDT, keyID)
	return &key, err
}

func (e *EncryptionMgr) getEncryptionKeysMetadataCb() (*EncrKeysInfo, error) {
	keys, err := e.GetClusterEncrKeysInfo(MetadataKDT)
	return &keys, err
}

func (e *EncryptionMgr) setInuseKeysMetadataCb(keyID common.KeyID) error {
	e.SetInUseKeys(MetadataKDT, keyID)
	return nil
}

func getActiveKeyIdCipherFromEncrKeysInfo(encrKeysInfo EncrKeysInfo) ([]byte, string, string) {
	activeKey := encrKeysInfo.ActiveKeyId
	for _, earkey := range encrKeysInfo.Keys {
		if activeKey == earkey.Id {
			return earkey.Key, earkey.Id, earkey.Cipher
		}
	}
	return []byte{}, "", ""
}

func getActiveEarKeyFromEncrKeysInfo(encrKeysInfo EncrKeysInfo) EaRKey {
	activeKey := encrKeysInfo.ActiveKeyId
	for _, earkey := range encrKeysInfo.Keys {
		if activeKey == earkey.Id {
			return earkey
		}
	}

	earkey := EaRKey{}

	// activeKeyId is empty string "" means encryption might not be enabled and cipher field can be empty string "".
	// gocbcrypto is used for encryption of data
	// gocbcrypto uses cipher mode "None" for un-encrypted data
	if encrKeysInfo.ActiveKeyId == "" {
		earkey.Cipher = gocbcrypto.CipherNameNone
	}

	return earkey
}

func validateAuth(w http.ResponseWriter, r *http.Request) bool {
	creds, valid, err := common.IsAuthValid(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error() + "\n"))
		return false
	} else if valid == false {
		audit.Audit(common.AUDIT_UNAUTHORIZED, r, "EncryptionMgr::validateAuth", "")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write(common.HTTP_STATUS_UNAUTHORIZED)
		return false
	}

	if creds != nil {
		allowed, err := creds.IsAllowed("cluster.admin.internal.index!read")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return false
		} else if !allowed {
			logging.Verbosef("EncryptionMgr::validateAuth not enough permissions")
			w.WriteHeader(http.StatusForbidden)
			w.Write(common.HTTP_STATUS_FORBIDDEN)
			return false
		}
	}
	return true
}
