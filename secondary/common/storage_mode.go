//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package common

import (
	"strings"
	"sync"

	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/stubs"
)

type StorageMode byte

const (
	NOT_SET = iota
	MOI
	PLASMA
	FORESTDB
	MIXED
)

func (s StorageMode) String() string {
	switch s {
	case NOT_SET:
		return "not_set"
	case MOI:
		return MemoryOptimized
	case FORESTDB:
		return ForestDB
	case PLASMA:
		return PlasmaDB
	default:
		return "invalid"
	}
}

//NOTE: This map needs to be in sync with IndexType in
//common/index.go
var smStrMap = map[string]StorageMode{
	MemDB:           MOI,
	MemoryOptimized: MOI,
	ForestDB:        FORESTDB,
	PlasmaDB:        PLASMA,
}

//Storage Mode
var gStorageMode StorageMode
var gClusterStorageMode StorageMode
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
	if gStorageMode == PLASMA && !stubs.UsePlasma() {
		logging.Warnf("Plasma is available only in EE but this is CE. Using ForestDB")
		gStorageMode = FORESTDB
	}
}

func SetStorageModeStr(mode string) bool {

	smLock.Lock()
	defer smLock.Unlock()
	if s, ok := smStrMap[strings.ToLower(mode)]; ok {
		gStorageMode = s
		if gStorageMode == PLASMA && !stubs.UsePlasma() {
			logging.Warnf("Plasma is available only in EE but this is CE. Using ForestDB")
			gStorageMode = FORESTDB
		}
		return true
	} else {
		gStorageMode = NOT_SET
		return false
	}

}

func GetClusterStorageMode() StorageMode {

	smLock.RLock()
	defer smLock.RUnlock()
	return gClusterStorageMode

}

func SetClusterStorageMode(mode StorageMode) {

	smLock.Lock()
	defer smLock.Unlock()
	gClusterStorageMode = mode

}

func SetClusterStorageModeStr(mode string) bool {

	smLock.Lock()
	defer smLock.Unlock()
	if s, ok := smStrMap[strings.ToLower(mode)]; ok {
		gClusterStorageMode = s
		return true
	} else {
		gClusterStorageMode = NOT_SET
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

func StorageModeToIndexType(m StorageMode) IndexType {
	switch m {
	case MOI:
		return MemoryOptimized
	case FORESTDB:
		return ForestDB
	case PLASMA:
		return PlasmaDB
	default:
		return ""
	}
}
