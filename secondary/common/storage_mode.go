//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package common

import (
	"strings"
	"sync"
)

type StorageMode byte

const (
	NOT_SET = iota
	MOI
	PLASMA
	FORESTDB
)

func (s StorageMode) String() string {
	switch s {
	case NOT_SET:
		return "not_set"
	case MOI:
		return "memory_optimized"
	case FORESTDB:
		return "forestdb"
	case PLASMA:
		return "plasma"
	default:
		return "invalid"
	}
}

//NOTE: This map needs to be in sync with IndexType in
//common/index.go
var smStrMap = map[string]StorageMode{
	"memdb":            MOI,
	"memory_optimized": MOI,
	"forestdb":         FORESTDB,
	"plasma":           PLASMA,
}

//Global Storage Mode
var gStorageMode StorageMode
var smLock sync.RWMutex //lock to protect gStorageMode

func GetStorageMode() StorageMode {

	smLock.RLock()
	defer smLock.RUnlock()
	return gStorageMode

}

func SetStorageMode(mode StorageMode) {

	smLock.Lock()
	defer smLock.Unlock()
	gStorageMode = mode

}

func SetStorageModeStr(mode string) bool {

	smLock.Lock()
	defer smLock.Unlock()
	if s, ok := smStrMap[strings.ToLower(mode)]; ok {
		gStorageMode = s
		return true
	} else {
		gStorageMode = NOT_SET
		return false
	}

}

func IndexTypeToStorageMode(t IndexType) StorageMode {

	switch strings.ToLower(string(t)) {
	case MemDB, MemoryOptimized:
		return MOI
	case ForestDB:
		return FORESTDB
	case PlasmaDB:
		return PLASMA
	default:
		return NOT_SET
	}
}
