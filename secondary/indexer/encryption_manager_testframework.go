// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

//Test framework to mock ns-server/cbauth providing indexer keys
//Use of these apis do not require enabling cluster level encryption

package indexer

import (
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/couchbase/indexing/secondary/logging"
)

var mockDataTypeKeyInfoMap = make(map[KeyDataType]*EncrKeysInfo)
var mu sync.Mutex
var cbsTest *EncryptionCallbacks
var keyPersistPath string
var keyPersistFile = "testKeysPersistFile"

type Entry struct {
	Datatype KeyDataType `json:"datatype"`
	Earkey   EaRKey      `json:"earkey"`
}

type TestEntry struct {
	Entries []Entry `json:"entries"`
}

type dropKey struct {
	Datatype KeyDataType `json:"datatype"`
	Keyids   []string    `json:"keyids"`
}

func cbmockGetEncryptionKeysBlocking(dtype KeyDataType) *EncrKeysInfo {

	var deksInfo *EncrKeysInfo
	var ok bool
	func() {
		mu.Lock()
		defer mu.Unlock()
		deksInfo, ok = mockDataTypeKeyInfoMap[dtype]

		if !ok {
			deksInfo = &EncrKeysInfo{ActiveKeyId: "", Keys: make([]EaRKey, 0), UnavailableKeyIds: make([]string, 0)}
			mockDataTypeKeyInfoMap[dtype] = deksInfo
		}
	}()

	return deksInfo
}

func getPersistPath() string {
	return keyPersistPath + "/" + keyPersistFile
}

func persistKeys(data map[KeyDataType]*EncrKeysInfo) error {

	logging.Infof("EncryptionMgrTest:updated %v ", getPersistPath())
	file, err := os.Create(getPersistPath())
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	return encoder.Encode(data)
}

func recoverPersistedKeys() {
	file, err := os.Open(getPersistPath())
	if err != nil {
		logging.Warnf("EncryptionMgrTest:Recover keys failed err:%v", err)
		return
	}

	defer file.Close()

	var decodedMap map[KeyDataType]*EncrKeysInfo
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&decodedMap)
	if err != nil {
		logging.Warnf("EncryptionMgrTest:Recover keys failed err:%v", err)
		return
	}

	mu.Lock()
	defer mu.Unlock()

	mockDataTypeKeyInfoMap = decodedMap
	logging.Infof("EncryptionMgrTest:Recovered keys from %v", getPersistPath())
}

// For /test/RefreshKeysCallback
func addEncryptionKey(w http.ResponseWriter, r *http.Request) {

	valid := validateAuth(w, r)
	if !valid {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var e TestEntry
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&e); err != nil {
		http.Error(w, "Invalid Json input", http.StatusBadRequest)
		return
	}
	for _, entry := range e.Entries {
		dtype := entry.Datatype
		earkey := entry.Earkey

		encoded := base64.StdEncoding.EncodeToString(earkey.Key)
		earkey.Key = []byte(encoded)

		func() {
			mu.Lock()
			defer mu.Unlock()

			deksInfo, ok := mockDataTypeKeyInfoMap[dtype]
			if !ok {
				mockDataTypeKeyInfoMap[dtype] = &EncrKeysInfo{ActiveKeyId: earkey.Id, Keys: []EaRKey{earkey}, UnavailableKeyIds: []string{}}
			} else {
				if deksInfo.ActiveKeyId == earkey.Id {
					errmsg := fmt.Sprintf("addEncryptionKey Keyid:%v can't be active key. Add new active key.", earkey.Id)
					http.Error(w, errmsg, http.StatusBadRequest)
					return
				}
				deksInfo.ActiveKeyId = earkey.Id
				deksInfo.Keys = append(deksInfo.Keys, earkey)
			}
			//persistKeys(mockDataTypeKeyInfoMap)
		}()

		go cbsTest.RefreshKeysCallback(dtype)
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Ok"))
}

//	For /test/DropKeysCallback
//
// Before drop, ensure that key is not being used from /test/GetInUseKeys
// It is for specific datatype
func dropEncryptionKey(w http.ResponseWriter, r *http.Request) {

	valid := validateAuth(w, r)
	if !valid {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var dropkey dropKey
	if err := json.NewDecoder(r.Body).Decode(&dropkey); err != nil {
		http.Error(w, "Invalid Json input", http.StatusBadRequest)
		return
	}

	dtype := dropkey.Datatype
	keyids := dropkey.Keyids

	func() {
		mu.Lock()
		defer mu.Unlock()
		deksInfo, ok := mockDataTypeKeyInfoMap[dtype]
		if !ok {
			http.Error(w, "KeyDataType not present", http.StatusBadRequest)
			return
		} else {
			for _, keyid := range keyids {
				if deksInfo.ActiveKeyId == keyid {
					errmsg := fmt.Sprintf("dropEncryptionKey Keyid:%v can't be active key. Add new active key before dropping.", keyid)
					http.Error(w, errmsg, http.StatusBadRequest)
					return
				}
				earkeys := mockDataTypeKeyInfoMap[dtype].Keys
				newearkeys := make([]EaRKey, 0)
				for _, k := range earkeys {
					if k.Id != keyid {
						newearkeys = append(newearkeys, k)
					}
				}
				mockDataTypeKeyInfoMap[dtype].Keys = newearkeys
			}
			for _, keyid := range keyids {
				add := true
				for _, uk := range mockDataTypeKeyInfoMap[dtype].UnavailableKeyIds {
					if uk == keyid {
						add = false
						break
					}
				}
				if add {
					mockDataTypeKeyInfoMap[dtype].UnavailableKeyIds = append(mockDataTypeKeyInfoMap[dtype].UnavailableKeyIds, keyid)
				}
			}

			//persistKeys(mockDataTypeKeyInfoMap)
			go cbsTest.DropKeysCallback(dtype, keyids)
		}
	}()
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Ok"))
}

//	For /test/DisableEncryption
//
// This operation is applied over all the datatypes
func disableEncryption(w http.ResponseWriter, r *http.Request) {

	valid := validateAuth(w, r)
	if !valid {
		return
	}

	func() {
		mu.Lock()
		defer mu.Unlock()

		for kdt, info := range mockDataTypeKeyInfoMap {
			if info == nil {
				err := fmt.Errorf("Error while disabling encryption for keydatatype:%v buckeetuuid:%v", kdt.TypeName, kdt.BucketUUID)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			//encryption already disabled in mockDataType
			if info.ActiveKeyId == "" {
				continue
			}

			unavailKeys := make([]string, 0)
			for _, uk := range mockDataTypeKeyInfoMap[kdt].UnavailableKeyIds {
				unavailKeys = append(unavailKeys, uk)
			}

			activeKey := mockDataTypeKeyInfoMap[kdt].ActiveKeyId
			mockDataTypeKeyInfoMap[kdt].ActiveKeyId = ""

			//All non deleted keys also should not be available
			for _, k := range info.Keys {
				found := false
				for _, uk := range unavailKeys {
					if k.Id == uk {
						found = true
						break
					}
				}
				if !found {
					mockDataTypeKeyInfoMap[kdt].UnavailableKeyIds = append(mockDataTypeKeyInfoMap[kdt].UnavailableKeyIds, k.Id)
				}
			}

			//persistKeys(mockDataTypeKeyInfoMap)
			go cbsTest.DropKeysCallback(kdt, []string{activeKey})
		}
	}()

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Ok"))
}

//	For /test/GetInUseKeys
//
// Get keyids which are being used by indexer components to encrypt data
func getInUseKeys(w http.ResponseWriter, r *http.Request) {

	valid := validateAuth(w, r)
	if !valid {
		return
	}

	var kdt KeyDataType
	if err := json.NewDecoder(r.Body).Decode(&kdt); err != nil {
		http.Error(w, "Invalid Json input", http.StatusBadRequest)
		return
	}

	keys, err := cbsTest.GetInUseKeysCallback(kdt)
	if err != nil {
		err := fmt.Errorf("Error getting in use keys err:%v", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	kbytes := []byte("[\n" + strings.Join(keys, "\n") + "\n]")

	w.Header().Set("Content-Length", fmt.Sprintf("%v", len(kbytes)))
	w.WriteHeader(http.StatusOK)
	w.Write(kbytes)
	return
}

//	For	/test/GetToBeUsedKeys
//
// Get keys which indexer components are expected to use and encrypt data
func getToBeUsedKeys(w http.ResponseWriter, r *http.Request) {
	valid := validateAuth(w, r)
	if !valid {
		return
	}

	var kdt KeyDataType
	if err := json.NewDecoder(r.Body).Decode(&kdt); err != nil {
		http.Error(w, "Invalid Json input", http.StatusBadRequest)
		return
	}

	func() {
		mu.Lock()
		defer mu.Unlock()

		deksInfo, ok := mockDataTypeKeyInfoMap[kdt]
		if !ok {
			err := fmt.Errorf("Mock keys not set for keydatatype:%v buckeetuuid:%v", kdt.TypeName, kdt.BucketUUID)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		} else {
			w.WriteHeader(http.StatusOK)
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(deksInfo); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}()
}
