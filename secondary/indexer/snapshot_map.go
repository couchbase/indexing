package indexer

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/couchbase/indexing/secondary/common"
)

type IndexSnapMap map[common.IndexInstId]*IndexSnapshotContainer

type IndexSnapshotContainer struct {
	sync.Mutex
	snap    IndexSnapshot
	deleted bool

	// TODO: Added for debugging MB-50006. Not supposed to go to production
	creationTime uint64
}

type IndexSnapMapHolder struct {
	ptr *unsafe.Pointer
}

func (ism *IndexSnapMapHolder) Init() {
	ism.ptr = new(unsafe.Pointer)
}

func (ism *IndexSnapMapHolder) Set(indexSnapMap IndexSnapMap) {
	atomic.StorePointer(ism.ptr, unsafe.Pointer(&indexSnapMap))
}

func (ism *IndexSnapMapHolder) Get() IndexSnapMap {
	if ptr := atomic.LoadPointer(ism.ptr); ptr != nil {
		return *(*IndexSnapMap)(ptr)
	} else {
		return make(IndexSnapMap)
	}
}

func (ipm *IndexSnapMapHolder) Clone() IndexSnapMap {
	if ptr := atomic.LoadPointer(ipm.ptr); ptr != nil {
		currMap := *(*IndexSnapMap)(ptr)
		return copyIndexSnapMap(currMap)
	} else {
		return make(IndexSnapMap)
	}
}
