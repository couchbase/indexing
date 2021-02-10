package indexer

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/couchbase/indexing/secondary/common"
)

type SnapshotWaitersContainer struct {
	sync.Mutex
	waiters []*snapshotWaiter
}

type SnapshotWaitersMap map[common.IndexInstId]*SnapshotWaitersContainer

type SnapshotWaitersMapHolder struct {
	ptr *unsafe.Pointer
}

func (swmh *SnapshotWaitersMapHolder) Init() {
	swmh.ptr = new(unsafe.Pointer)
}

func (swmh *SnapshotWaitersMapHolder) Set(waiterMap SnapshotWaitersMap) {
	atomic.StorePointer(swmh.ptr, unsafe.Pointer(&waiterMap))
}

func (swmh *SnapshotWaitersMapHolder) Get() SnapshotWaitersMap {
	if ptr := atomic.LoadPointer(swmh.ptr); ptr != nil {
		return *(*SnapshotWaitersMap)(ptr)
	} else {
		return make(SnapshotWaitersMap)
	}
}

func (swmh *SnapshotWaitersMapHolder) Clone() SnapshotWaitersMap {
	if ptr := atomic.LoadPointer(swmh.ptr); ptr != nil {
		currMap := *(*SnapshotWaitersMap)(ptr)
		return copySnapshotWaiterMap(currMap)
	} else {
		return make(SnapshotWaitersMap)
	}
}

func copySnapshotWaiterMap(waiterMap SnapshotWaitersMap) SnapshotWaitersMap {
	cloneMap := make(SnapshotWaitersMap)
	for instId, waiterContainer := range waiterMap {
		cloneMap[instId] = waiterContainer
	}
	return cloneMap
}
