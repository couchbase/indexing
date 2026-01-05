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

	//"github.com/couchbase/cbauth"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

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

	testRunning bool
	enableTest  atomic.Bool

	wrkrQueue     MsgChannel // as multiple concurrent encryption messages can be received, they will be queued and processed async after following signals.
	bootstrapDone chan bool  // closed channel means indexer bootstrap is done.
	recoveryDone  chan bool  // closed channel means indexerUsedKeyIds update is completed after inputs from indexer components.
}

func NewEncryptionMgr(supvCmdch MsgChannel, supvMsgch MsgChannel, config common.Config) (*EncryptionMgr, Message) {

	encryptionMgr := &EncryptionMgr{
		supvCmdch:          supvCmdch,
		supvMsgch:          supvMsgch,
		config:             config,
		dataTypeKeyInfoMap: make(map[KeyDataType]*DeksInfo, 0),
		kdtKeyidKeyMap:     make(map[KeyDataType]map[string]EaRKey, 0),
		keyidKdtMap:        make(map[string]KeyDataType),
		indexerUsedKeyIds:  make(map[KeyDataType][]string, 0),
		testRunning:        false,
		wrkrQueue:          make(MsgChannel, WRKR_QUEUE_LEN),
		bootstrapDone:      make(chan bool),
		recoveryDone:       make(chan bool),
	}

	encryptionMgr.enableTest.Store(false)
	encryptionMgr.setEnableTest()

	go encryptionMgr.run()
	go encryptionMgr.runQueue()
	RegisterCallbacks(encryptionMgr)
	return encryptionMgr, nil
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

	//TODO: Register callbacks when cbauth changes are merged. Until that test RESTApis will be used.
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
	<-e.bootstrapDone
	<-e.recoveryDone
	logging.Infof("EncryptionMgr:runQueue bootstrap and key recovery done...")

	//TODO: Process enqueued encryption requests.
	//for {
	//	select {
	//		msg,ok:=<-e.wrkrQueue
	//		if ok {
	//
	//		}
	//	}
	//}
}

func (e *EncryptionMgr) enqueue(cmd Message) {
	e.wrkrQueue <- cmd
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

func (e *EncryptionMgr) recoverInUseKeys() {
	//TODO: Send &MsgEncryptionGetInuseKeys{} to statsMgr/storageMgr/clusterMgr and wait for response
	//TODO: Add handlers to MsgEncryptionGetInuseKeys in concerned components.
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
	//TODO: Add logic for wait race with bootstrap
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

	keyids, ok := e.indexerUsedKeyIds[kdt]
	if !ok || keyids == nil {
		return fmt.Errorf("EncryptionMgr:SetInUseKeys unable to find keys map for type:%v uuid:%v", kdt.TypeName, kdt.BucketUUID)
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
func (e *EncryptionMgr) getKeyCipherById(keyId string) ([]byte, string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	kdt, ok := e.keyidKdtMap[keyId]
	if !ok {
		return []byte{}, "", fmt.Errorf("EncryptionMgr:getKeyCipherById could not find keydatatype for key:%v", keyId)
	} else {
		earkey, err := e.getKeyCacheByKeyid(kdt, keyId)
		if err != nil {
			return []byte{}, "", fmt.Errorf("EncryptionMgr:getKeyCipherById for key:%v err:%v", keyId, err.Error())
		} else {
			return earkey.Data, earkey.Cipher, nil
		}
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

// Get active key for keydatatype
func (e *EncryptionMgr) getActiveKeyIdCipher(typename, bucketUUID string) ([]byte, string, string, error) {

	deksinfo, err := e.GetClusterDeksInfo(KeyDataType{TypeName: typename, BucketUUID: bucketUUID})
	if err != nil {
		return []byte{}, "", "", err
	} else {
		key, keyid, cipher := getActiveKeyIdCipherFromDeksInfo(deksinfo)
		if len(key) == 0 || keyid == "" || cipher == "" {
			return []byte{}, "", "", fmt.Errorf("getActiveBucketKeyIdCipher keylength:%d keyid:%v cipher:%v", len(key), keyid, cipher)
		} else {
			return key, keyid, cipher, nil
		}
	}
}

// ns-server expects service to cache in-use keys and return callback quickly
func (e *EncryptionMgr) getInUseKeysCallback(kdt KeyDataType) ([]string, error) {
	logging.Infof("EncryptionMgr:getInUseKeysCallback %v", kdt)
	return e.GetInUseKeys(kdt)
}

// This can be called concurrently by many callers.
func (e *EncryptionMgr) refreshKeysCallback(kdt KeyDataType) error {

	logging.Infof("EncryptionMgr:refreshKeysCallback %v", kdt)
	//TODO: Use below method when cbauth changes are merged.
	//deksInfo, err := cbauth.GetEncryptionKeysBlocking(kdt)
	//if err != nil {
	//	return err
	//}
	deksInfo := cbmockGetEncryptionKeysBlocking(kdt)

	e.SetClusterDeksInfo(kdt, deksInfo)

	//TODO: handle message at concerned components
	//activeKey := deksInfo.ActiveKey
	//e.enqueue(&MsgEncryptionUpdate{kdt, activeKey})

	return nil
}

// This method is not expected to return error to caller.
// If keyids contain "", it is expected to encrypt the unencrypted data
func (e *EncryptionMgr) dropKeysCallback(kdt KeyDataType, keyids []string) {

	logging.Infof("EncryptionMgr:refreshKeysCallback %v %v", kdt, keyids)
	if _, err := e.GetClusterDeksInfo(kdt); err != nil {
		//TODO: call method to signal key drop when ns-server changes merged.
		//call cbauth.KeyDropComplete(kdt,err)
	} else {
		// TODO: Use below method when cbauth changes are merged.
		// deksInfo, err := cbauth.GetEncryptionKeysBlocking(kdt)
		// This updates unavailable keys
		deksInfo := cbmockGetEncryptionKeysBlocking(kdt)
		e.SetClusterDeksInfo(kdt, deksInfo)

		//TODO: handle message at concerned components
		//activeKey := deksInfo.ActiveKey
		//e.enqueue(&MsgEncryptionDropKeys{kdt, activeKey, keyids})
	}
}
