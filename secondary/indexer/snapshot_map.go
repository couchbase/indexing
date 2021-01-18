package indexer

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/couchbase/indexing/secondary/common"
)

// TODO: Re-name this to IndexSnapMap after doing storage_manager changes
type IndexSnapMap2 map[common.IndexInstId]*IndexSnapshotContainer

type IndexSnapshotContainer struct {
	sync.Mutex
	snap IndexSnapshot
}

// TODO: Rename this to indexSnapMap holder
type IndexSnapMap2Holder struct {
	ptr *unsafe.Pointer
}

func (ism *IndexSnapMap2Holder) Init() {
	ism.ptr = new(unsafe.Pointer)
}

func (ism *IndexSnapMap2Holder) Set(indexSnapMap IndexSnapMap2) {
	atomic.StorePointer(ism.ptr, unsafe.Pointer(&indexSnapMap))
}

func (ism *IndexSnapMap2Holder) Get() IndexSnapMap2 {
	if ptr := atomic.LoadPointer(ism.ptr); ptr != nil {
		return *(*IndexSnapMap2)(ptr)
	} else {
		return make(IndexSnapMap2)
	}
}

func (ipm *IndexSnapMap2Holder) Clone() IndexSnapMap2 {
	if ptr := atomic.LoadPointer(ipm.ptr); ptr != nil {
		currMap := *(*IndexSnapMap2)(ptr)
		return copyIndexSnapMap2(currMap)
	} else {
		return make(IndexSnapMap2)
	}
}
