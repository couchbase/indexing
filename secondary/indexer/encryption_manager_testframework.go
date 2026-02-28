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
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/couchbase/indexing/secondary/audit"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

//TODO: Later update to import actual cbauth structs
//KeyDataType to cbauth.KeyDataType
//DeksInfo to cbauth.DeksInfo
//EaRKey to cbauthimpl.EaRKey

type KeyDataType struct {
	TypeName   string `json:"type_name"`
	BucketUUID string `json:"bucket_uuid"` // UUID is used only when Type is "bucket"
}

type DeksInfo struct {
	ActiveKey       string   `json:"active_key_id"`
	Keys            []EaRKey `json:"keys"`
	UnavailableKeys []string `json:"unavailable_keys"`
}

type EaRKey struct {
	Id     string `json:"id"`
	Cipher string `json:"cipher"`
	Data   []byte `json:"data"`
}

var mockDataTypeKeyInfoMap = make(map[KeyDataType]*DeksInfo)
var mu sync.Mutex
var cbsTest *EncryptionCallbacks

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

func cbmockGetEncryptionKeysBlocking(dtype KeyDataType) *DeksInfo {

	var deksInfo *DeksInfo
	var ok bool
	func() {
		mu.Lock()
		defer mu.Unlock()
		deksInfo, ok = mockDataTypeKeyInfoMap[dtype]
	}()

	if !ok {
		return nil
	} else {
		return deksInfo
	}
}

func validateAuth(w http.ResponseWriter, r *http.Request) bool {
	creds, valid, err := common.IsAuthValid(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error() + "\n"))
		return false
	} else if valid == false {
		audit.Audit(common.AUDIT_UNAUTHORIZED, r, "EncryptionMgrTest::validateAuth", "")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write(common.HTTP_STATUS_UNAUTHORIZED)
		return false
	}

	if creds != nil {
		allowed, err := creds.IsAllowed("cluster.admin.internal.index!write")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return false
		} else if !allowed {
			logging.Verbosef("EncryptionMgrTest::validateAuth not enough permissions")
			w.WriteHeader(http.StatusForbidden)
			w.Write(common.HTTP_STATUS_FORBIDDEN)
			return false
		}
	}
	return true
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

		encoded := base64.StdEncoding.EncodeToString(earkey.Data)
		earkey.Data = []byte(encoded)

		func() {
			mu.Lock()
			defer mu.Unlock()

			deksInfo, ok := mockDataTypeKeyInfoMap[dtype]
			if !ok {
				mockDataTypeKeyInfoMap[dtype] = &DeksInfo{ActiveKey: earkey.Id, Keys: []EaRKey{earkey}, UnavailableKeys: []string{}}
			} else {
				if deksInfo.ActiveKey == earkey.Id {
					errmsg := fmt.Sprintf("addEncryptionKey Keyid:%v can't be active key. Add new active key.", earkey.Id)
					http.Error(w, errmsg, http.StatusBadRequest)
					return
				}
				deksInfo.ActiveKey = earkey.Id
				deksInfo.Keys = append(deksInfo.Keys, earkey)
			}
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
				if deksInfo.ActiveKey == keyid {
					errmsg := fmt.Sprintf("dropEncryptionKey Keyid:%v can't be active key. Add new active key before dropping.", keyid)
					http.Error(w, errmsg, http.StatusBadRequest)
					return
				}
			}
			for _, keyid := range keyids {
				deksInfo.UnavailableKeys = append(deksInfo.UnavailableKeys, keyid)
			}

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
			if info.ActiveKey == "" {
				continue
			}

			unavailKeys := make([]string, 0)
			for _, uk := range mockDataTypeKeyInfoMap[kdt].UnavailableKeys {
				unavailKeys = append(unavailKeys, uk)
			}

			activeKey := mockDataTypeKeyInfoMap[kdt].ActiveKey
			mockDataTypeKeyInfoMap[kdt].ActiveKey = ""

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
					mockDataTypeKeyInfoMap[kdt].UnavailableKeys = append(mockDataTypeKeyInfoMap[kdt].UnavailableKeys, k.Id)
				}
			}

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
	kbytes := []byte(strings.Join(keys, "\n"))

	w.Header().Set("Content-Length", fmt.Sprintf("%v", len(keys)))
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
