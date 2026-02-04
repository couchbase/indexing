// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"fmt"
	"sync"
	"sync/atomic"

	couchbase "github.com/couchbase/indexing/secondary/dcp"

	//"github.com/couchbase/cbauth"
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
	GetInUseKeysCallback func(kdt KeyDataType) ([]string, error)
	RefreshKeysCallback  func(kdt KeyDataType) error
	DropKeysCallback     func(kdt KeyDataType, keyids []string)
}

type EncryptionMgr struct {
	supvCmdch MsgChannel //supervisor sends commands on this channel
	supvMsgch MsgChannel //channel to send any message to supervisor
	config    common.Config

	dataTypeKeyInfoMap map[KeyDataType]*DeksInfo         // Map holds key data received from ns-server
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
		dataTypeKeyInfoMap:     make(map[KeyDataType]*DeksInfo, 0),
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

	go encryptionMgr.cacheKeysForBootstrap()
	go encryptionMgr.recoverInUseKeys()
	go encryptionMgr.run()
	go encryptionMgr.runQueue()
	RegisterCallbacks(encryptionMgr)
	return encryptionMgr, &MsgSuccess{}
}

func RegisterCallbacks(e *EncryptionMgr) {

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
	}

	//ENCRYPT_TODO: Register callbacks when cbauth changes are merged. Until that test RESTApis will be used.
	cbsTest = cbs
	//err := cbauth.RegisterEncryptionKeysCallbacks(cbs.RefreshKeysCallback, cbs.GetInUseKeysCallback, cbs.DropKeysCallback)
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
	logging.Infof("EncryptionMgr:trackUpdateKeys KeyUpdateComplete for keydatatype:%v keyid:%v err:%v", kdt, keyid, err)

	e.cbMu.Lock()
	defer e.cbMu.Unlock()

	e.resetDropOrUpdateProgress(kdt)
	nextPendingMsg := e.getNextMessagePending(kdt)
	if nextPendingMsg != nil {
		logging.Infof("EncryptionMgr:trackUpdateKeys KeyUpdateComplete, enqueuing pending msg for keydatatype:%v msg:%v", kdt, nextPendingMsg.GetMsgType().String())
		e.enqueue(nextPendingMsg)
		e.setDropOrUpdateInProgress(kdt, msg)
	}
}

func (e *EncryptionMgr) trackDropKeys(msg Message) {
	kdt := msg.(*MsgEncryptionDropKey).GetKeyDataType()
	dropKeyids := msg.(*MsgEncryptionDropKey).GetDropKeyIds()
	respCh := msg.(*MsgEncryptionDropKey).GetRespCh()

	err := <-respCh
	logging.Infof("EncryptionMgr:trackDropKeys KeyDropComplete for keydatatype:%v drop keyids:%v err:%v", kdt, dropKeyids, err)

	e.cbMu.Lock()
	defer e.cbMu.Unlock()

	e.resetDropOrUpdateProgress(kdt)
	//ENCRYPT_TODO: call method to signal key drop when cbauth changes merged.
	//Update e.indexerUsedKeyIds thus keyids no longer get reflected in e.GetInUseKeys()
	//call cbauth.KeyDropComplete(kdt,err)
	nextPendingMsg := e.getNextMessagePending(kdt)
	if nextPendingMsg != nil {
		logging.Infof("EncryptionMgr:trackDropKeys KeyDropComplete, enqueuing pending msg for keydatatype:%v msg:%v", kdt, nextPendingMsg.GetMsgType().String())
		e.enqueue(nextPendingMsg)
		e.setDropOrUpdateInProgress(kdt, msg)
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
	e.RegisterRestEndpoints()

	e.supvCmdch <- &MsgSuccess{}
}

func (e *EncryptionMgr) handleIndexerReady(cmd Message) {
	logging.Infof("EncryptionMgr:handleIndexerReady...")
	close(e.bootstrapDone)
	e.supvCmdch <- &MsgSuccess{}
}

func (e *EncryptionMgr) cacheKeysForBootstrap() {

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
		kdt := KeyDataType{TypeName: "bucket", BucketUUID: bucket.UUID}
		//ENCRYPT_TODO: Use below method when cbauth changes are merged.
		//deksInfo, err := cbauth.GetEncryptionKeysBlocking(kdt)
		//if err != nil {
		//	logging.Warnf("EncryptionMgr:caching keys for bucket:%v err:%v", bucket.Name, err)
		//	continue
		//}
		deksInfo := cbmockGetEncryptionKeysBlocking(kdt)
		if deksInfo == nil {
			continue
		}
		e.SetClusterDeksInfo(kdt, deksInfo)
	}

	// ENCRYPT_TODO: Cache keys for log, config, audit
	//kdt := KeyDataType{TypeName: "log", BucketUUID: ""}
	//deksInfo := cbmockGetEncryptionKeysBlocking(kdt)
	//e.SetClusterDeksInfo(kdt, deksInfo)
	//
	//kdt = KeyDataType{TypeName: "config", BucketUUID: ""}
	//deksInfo = cbmockGetEncryptionKeysBlocking(kdt)
	//e.SetClusterDeksInfo(kdt, deksInfo)
	//
	//kdt = KeyDataType{TypeName: "audit", BucketUUID: ""}
	//deksInfo = cbmockGetEncryptionKeysBlocking(kdt)
	//e.SetClusterDeksInfo(kdt, deksInfo)

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
	logging.Infof("EncryptionMgr:recoverInUseKeys...")
	<-e.bootstrapDone

	// This map will store in-use keys by union/merge of keys from all the components.
	allKdtKeys := make(map[KeyDataType][]string)
	logging.Infof("EncryptionMgr:recoverInUseKeys sending msg to components...")

	// This message is not for specific bucket thus bucketUUID is empty. This will get keys in use for all buckets.
	respMapCh := make(chan map[KeyDataType][]string)
	e.supvMsgch <- &MsgEncryptionGetInuseKeys{keyDataType: KeyDataType{TypeName: "bucket", BucketUUID: ""}, respMapCh: respMapCh}
	kdtKeysMap := <-respMapCh
	allKdtKeys = mergeMap(kdtKeysMap, allKdtKeys)

	// ENCRYPT_TODO: Add other key data types later
	//e.supvMsgch <- &MsgEncryptionGetInuseKeys{keyDataType: KeyDataType{TypeName: "log", BucketUUID: ""}, respMapCh: respMapCh}
	//kdtKeysMap = <-respMapCh
	//allKdtKeys = mergeMap(kdtKeysMap, allKdtKeys)
	//
	//e.supvMsgch <- &MsgEncryptionGetInuseKeys{keyDataType: KeyDataType{TypeName: "config", BucketUUID: ""}, respMapCh: respMapCh}
	//kdtKeysMap = <-respMapCh
	//allKdtKeys = mergeMap(kdtKeysMap, allKdtKeys)
	//
	//e.supvMsgch <- &MsgEncryptionGetInuseKeys{keyDataType: KeyDataType{TypeName: "audit", BucketUUID: ""}, respMapCh: respMapCh}
	//kdtKeysMap = <-respMapCh
	//allKdtKeys = mergeMap(kdtKeysMap, allKdtKeys)

	for kdt, keys := range allKdtKeys {
		for _, key := range keys {
			err := e.SetInUseKeys(kdt, key)
			if err != nil {
				logging.Warnf("EncryptionMgr:recoverInUseKeys SetInUseKeys keydatatype:%v key:%v err:%v", kdt, key, err)
			}
		}
	}
	close(e.recoveryDone)
}

func (e *EncryptionMgr) RegisterRestEndpoints() {

	if e.testRunning == false && e.enableTest.Load() {
		mux := GetHTTPMux()
		mux.HandleFunc("/test/RefreshKeysCallback", addEncryptionKey)
		mux.HandleFunc("/test/DropKeysCallback", dropEncryptionKey)
		mux.HandleFunc("/test/DisableEncryption", disableEncryption)
		mux.HandleFunc("/test/GetInUseKeys", getInUseKeys)
		mux.HandleFunc("/test/GetToBeUsedKeys", getToBeUsedKeys)

		e.testRunning = true
	}
}

func (e *EncryptionMgr) GetInUseKeys(kdt KeyDataType) ([]string, error) {

	<-e.recoveryDone
	logging.Infof("EncryptionMgr:GetInUseKeys %v", kdt)
	e.muid.Lock()
	defer e.muid.Unlock()

	keyids, ok := e.indexerUsedKeyIds[kdt]
	if !ok {
		return nil, fmt.Errorf("EncryptionMgr:GetInUseKeys unable to find keys map for type:%v uuid:%v", kdt.TypeName, kdt.BucketUUID)
	} else {
		return keyids, nil
	}
}

// When any of the component starts to use key for encryption, use this to update book-keeping
func (e *EncryptionMgr) SetInUseKeys(kdt KeyDataType, key string) error {
	e.muid.Lock()
	defer e.muid.Unlock()

	logging.Infof("EncryptionMgr:SetInUseKeys %v %v", kdt, key)
	keyids, ok := e.indexerUsedKeyIds[kdt]
	if !ok || keyids == nil {
		keys := make([]string, 0)
		keys = append(keys, key)
		e.indexerUsedKeyIds[kdt] = keys
	} else {
		for _, k := range keyids {
			if key == k {
				return fmt.Errorf("EncryptionMgr:SetInUseKeys key already in use keys map for type:%v uuid:%v", kdt.TypeName, kdt.BucketUUID)
			}
		}
		e.indexerUsedKeyIds[kdt] = append(e.indexerUsedKeyIds[kdt], key)
	}
	return nil
}

func (e *EncryptionMgr) GetClusterDeksInfo(kdt KeyDataType) (DeksInfo, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	deksinfo, ok := e.dataTypeKeyInfoMap[kdt]
	if ok && deksinfo != nil {
		return *deksinfo, nil
	}
	return DeksInfo{}, fmt.Errorf("EncryptionMgr:GetClusterDeksInfo unable to find keys for type:%v uuid:%v", kdt.TypeName, kdt.BucketUUID)
}

func (e *EncryptionMgr) SetClusterDeksInfo(kdt KeyDataType, info *DeksInfo) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.SetClusterDeksInfoNoLock(kdt, info)
}

func (e *EncryptionMgr) SetClusterDeksInfoNoLock(kdt KeyDataType, info *DeksInfo) {
	var dkinfo DeksInfo
	dkinfo = *info

	deksinfo, ok := e.dataTypeKeyInfoMap[kdt]
	if !ok || deksinfo == nil {
		//New entry
		e.dataTypeKeyInfoMap[kdt] = &dkinfo
	} else {
		//Override current entry
		e.dataTypeKeyInfoMap[kdt] = &dkinfo
	}
	e.setKeyCacheByKeyid(kdt, &dkinfo)
}

// Check if caller function holds EncryptionMgr.mu mutex.
func (e *EncryptionMgr) setKeyCacheByKeyid(kdt KeyDataType, info *DeksInfo) {
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
func (e *EncryptionMgr) getKeyCipherById(keyId string) ([]byte, string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	kdt, ok := e.keyidKdtMap[keyId]
	if !ok {
		logging.Warnf("EncryptionMgr:getKeyCipherById could not find keydatatype for key:%v", keyId)
	} else {
		earkey, err := e.getKeyCacheByKeyid(kdt, keyId)
		if err != nil {
			logging.Warnf("EncryptionMgr:getKeyCipherById for key:%v err:%v", keyId, err.Error())
		} else {
			return earkey.Data, earkey.Cipher, nil
		}
	}

	//ENCRYPT_TODO: Use below method when cbauth changes are merged.
	//deksInfo, err := cbauth.GetEncryptionKeysBlocking(kdt)
	//if err != nil {
	//	logging.Warnf("EncryptionMgr:getKeyCipherById retry for key:%v err:%v", keyId, err.Error())
	//	return []byte{}, "", fmt.Errorf("EncryptionMgr:getKeyCipherById could get keys from cbauth for key:%v", keyId)
	//}
	// Update encryption related info
	deksInfo := cbmockGetEncryptionKeysBlocking(kdt)
	e.SetClusterDeksInfoNoLock(kdt, deksInfo)

	// Try getting key for keyId as keyinfo is updated
	kdt, ok = e.keyidKdtMap[keyId]
	if !ok {
		logging.Warnf("EncryptionMgr:getKeyCipherById retry could not find keydatatype for key:%v", keyId)
	} else {
		earkey, err := e.getKeyCacheByKeyid(kdt, keyId)
		if err != nil {
			logging.Warnf("EncryptionMgr:getKeyCipherById retry for key:%v err:%v", keyId, err.Error())
		} else {
			return earkey.Data, earkey.Cipher, nil
		}
	}

	return []byte{}, "", fmt.Errorf("EncryptionMgr:getKeyCipherById for key:%v failed", keyId)
}

// Get active key for keydatatype
// If key data is not present in EncryptionMgr, attempt is made to get from cbauth
func (e *EncryptionMgr) getActiveKeyIdCipher(typename, bucketUUID string) ([]byte, string, string, error) {

	kdt := KeyDataType{TypeName: typename, BucketUUID: bucketUUID}
	deksinfo, err := e.GetClusterDeksInfo(kdt)
	if err != nil {
		logging.Warnf("EncryptionMgr:getActiveKeyIdCipher no key data for keydatatype: %v", kdt)
	} else {
		key, keyid, cipher := getActiveKeyIdCipherFromDeksInfo(deksinfo)
		if len(key) == 0 || keyid == "" || cipher == "" {
			logging.Warnf("EncryptionMgr:getActiveKeyIdCipher no key data keylength:%d keyid:%v cipher:%v", len(key), keyid, cipher)
		} else {
			return key, keyid, cipher, nil
		}
	}

	//ENCRYPT_TODO: Use below method when cbauth changes are merged.
	//deksInfo, err := cbauth.GetEncryptionKeysBlocking(kdt)
	//if err != nil {
	//	logging.Warnf("EncryptionMgr:getKeyCipherById retry for key:%v err:%v", keyId, err.Error())
	//	return []byte{}, "", fmt.Errorf("EncryptionMgr:getActiveKeyIdCipher could get keys from cbauth for keydatatype: %v", kdt)
	//}
	// Update encryption related info
	deksInfo := cbmockGetEncryptionKeysBlocking(kdt)
	e.SetClusterDeksInfo(kdt, deksInfo)

	// Try getting active key as info is updated
	deksinfo, err = e.GetClusterDeksInfo(KeyDataType{TypeName: typename, BucketUUID: bucketUUID})
	if err != nil {
		logging.Warnf("EncryptionMgr:getActiveKeyIdCipher retry no key data for keydatatype: %v", kdt)
	} else {
		key, keyid, cipher := getActiveKeyIdCipherFromDeksInfo(deksinfo)
		if len(key) == 0 || keyid == "" || cipher == "" {
			logging.Warnf("EncryptionMgr:getActiveKeyIdCipher retry no key data keylength:%d keyid:%v cipher:%v", len(key), keyid, cipher)
		} else {
			return key, keyid, cipher, nil
		}
	}
	return []byte{}, "", "", fmt.Errorf("EncryptionMgr:getActiveKeyIdCipher failed")
}

// ns-server expects service to cache in-use keys and return callback quickly
func (e *EncryptionMgr) getInUseKeysCallback(kdt KeyDataType) ([]string, error) {
	logging.Infof("EncryptionMgr:getInUseKeysCallback %v", kdt)
	return e.GetInUseKeys(kdt)
}

// This can be called concurrently by many callers.
func (e *EncryptionMgr) refreshKeysCallback(kdt KeyDataType) error {

	logging.Infof("EncryptionMgr:refreshKeysCallback %v", kdt)
	//ENCRYPT_TODO: Use below method when cbauth changes are merged.
	//deksInfo, err := cbauth.GetEncryptionKeysBlocking(kdt)
	//if err != nil {
	//	return err
	//}
	deksInfo := cbmockGetEncryptionKeysBlocking(kdt)

	e.SetClusterDeksInfo(kdt, deksInfo)

	earkey, err := getActiveEarKeyFromDeksInfo(*deksInfo)
	if err != nil {
		logging.Warnf("EncryptionMgr:RefreshKeysCallback:could not get active key data.")
		return err
	}

	respCh := make(chan error)
	msg := &MsgEncryptionUpdateKey{kdt, earkey, respCh}

	e.cbMu.Lock()
	defer e.cbMu.Unlock()

	//Skip sending update for active key if update is already getting processed
	activeMsg := e.getDropOrUpdateInProgress(kdt)

	// As the there is ongoing key drop/update, skip msg adding to wrkrQueue
	// If msg is not duplicate of ongoing update or also not present in pendingMap, add it to pendingMap.
	if activeMsg != nil {
		logging.Warnf("EncryptionMgr:RefreshKeysCallback: drop/update in progress for keydatatype:%v keyid:%v.", kdt, earkey.Id)

		// As the current msg is duplicate of ongoing update, this msg can be skipped from enqueuing.
		skipMsg := e.isDuplicateMsg(msg, activeMsg)
		if skipMsg {
			logging.Warnf("EncryptionMgr:RefreshKeysCallback:duplicate to active msg for keydatatype:%v keyid:%v.", kdt, earkey.Id)
			return nil
		}

		// As the current msg is also present in pendingMap, this msg can be skipped from adding to pending map.
		skipAddPending := e.isDuplicatePending(kdt, msg)
		if !skipAddPending {
			logging.Warnf("EncryptionMgr:RefreshKeysCallback added to pending msg for keydatatype:%v keyid:%v.", kdt, earkey.Id)
			e.addMessagePending(kdt, msg)
			return nil
		}

		logging.Warnf("EncryptionMgr:RefreshKeysCallback:duplicate to pending msg for keydatatype:%v keyid:%v.", kdt, earkey.Id)
		return nil
	}

	logging.Infof("EncryptionMgr:RefreshKeysCallback:Starting for keydatatype:%v keyid:%v", kdt, earkey.Id)
	e.enqueue(msg)
	e.setDropOrUpdateInProgress(kdt, msg)
	return nil
}

// This method is not expected to return error to caller.
// If keyids contain "", it is expected to encrypt the unencrypted data
func (e *EncryptionMgr) dropKeysCallback(kdt KeyDataType, keyids []string) {

	logging.Infof("EncryptionMgr:DropKeysCallback:Starting for keydatatype:%v keyids:%v", kdt, keyids)
	if _, err := e.GetClusterDeksInfo(kdt); err != nil {
		//ENCRYPT_TODO: call method to signal key drop when cbauth changes merged.
		//call cbauth.KeyDropComplete(kdt,err)
		logging.Warnf("EncryptionMgr:DropKeysCallback:could not get key data.")
	} else {
		// ENCRYPT_TODO: Use below method when cbauth changes are merged.
		// deksInfo, err := cbauth.GetEncryptionKeysBlocking(kdt)
		// This updates unavailable keys
		deksInfo := cbmockGetEncryptionKeysBlocking(kdt)
		e.SetClusterDeksInfo(kdt, deksInfo)

		earkey, err := getActiveEarKeyFromDeksInfo(*deksInfo)
		if err != nil {
			logging.Warnf("EncryptionMgr:DropKeysCallback:could not get active key data.")
			return
		}

		respCh := make(chan error)
		msg := &MsgEncryptionDropKey{kdt, keyids, earkey, respCh}

		e.cbMu.Lock()
		defer e.cbMu.Unlock()

		//Skip sending drop for keys if drop is already getting processed
		activeMsg := e.getDropOrUpdateInProgress(kdt)
		// As the there is ongoing key drop/update, skip msg adding to wrkrQueue
		// If msg is not duplicate of ongoing update or also not present in pendingMap, add it to pendingMap.
		if activeMsg != nil {
			logging.Warnf("EncryptionMgr:DropKeysCallback: drop/update in progress for keydatatype:%v activekeyid:%v.", kdt, earkey.Id)

			// As the current msg is duplicate of ongoing drop, this msg can be skipped from enqueuing.
			skipMsg := e.isDuplicateMsg(msg, activeMsg)
			if skipMsg {
				logging.Warnf("EncryptionMgr:DropKeysCallback:duplicate to active msg for keydatatype:%v keyids:%v", kdt, keyids)
				return
			}

			// As the current msg is also present in pendingMap, this msg can be skipped from adding to pending map.
			skipAddPending := e.isDuplicatePending(kdt, msg)
			if !skipAddPending {
				logging.Warnf("EncryptionMgr:DropKeysCallback added to pending msg for keydatatype:%v keyid:%v.", kdt, earkey.Id)
				e.addMessagePending(kdt, msg)
				return
			}

			logging.Warnf("EncryptionMgr:DropKeysCallback:duplicate to pending msg for keydatatype:%v keyid:%v.", kdt, earkey.Id)
			return
		}

		logging.Infof("EncryptionMgr:DropKeysCallback:Starting for keydatatype:%v keyid:%v", kdt, earkey.Id)
		e.enqueue(msg)
		e.setDropOrUpdateInProgress(kdt, msg)
	}
}

func getActiveKeyIdCipherFromDeksInfo(deksinfo DeksInfo) ([]byte, string, string) {
	activeKey := deksinfo.ActiveKey
	for _, earkey := range deksinfo.Keys {
		if activeKey == earkey.Id {
			return earkey.Data, earkey.Id, earkey.Cipher
		}
	}
	return []byte{}, "", ""
}

func getActiveEarKeyFromDeksInfo(deksinfo DeksInfo) (EaRKey, error) {
	activeKey := deksinfo.ActiveKey
	for _, earkey := range deksinfo.Keys {
		if activeKey == earkey.Id {
			return earkey, nil
		}
	}
	return EaRKey{}, fmt.Errorf("Earkey with activeKeyId %v not found", activeKey)
}
