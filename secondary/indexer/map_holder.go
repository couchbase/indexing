package indexer

import (
	"sync/atomic"
	"unsafe"

	"github.com/couchbase/indexing/secondary/common"
)

// Holder for IndexInstMap
type IndexInstMapHolder struct {
	ptr *unsafe.Pointer
}

func (iim *IndexInstMapHolder) Init() {
	iim.ptr = new(unsafe.Pointer)
}

func (iim *IndexInstMapHolder) Set(indexInstMap common.IndexInstMap) {
	atomic.StorePointer(iim.ptr, unsafe.Pointer(&indexInstMap))
}

func (iim *IndexInstMapHolder) Get() common.IndexInstMap {
	if ptr := atomic.LoadPointer(iim.ptr); ptr != nil {
		return *(*common.IndexInstMap)(ptr)
	} else {
		return make(common.IndexInstMap)
	}
}

func (iim *IndexInstMapHolder) Clone() common.IndexInstMap {
	if ptr := atomic.LoadPointer(iim.ptr); ptr != nil {
		currMap := *(*common.IndexInstMap)(ptr)
		return common.CopyIndexInstMap2(currMap)
	} else {
		return make(common.IndexInstMap)
	}
}

// Holder for IndexPartnMap
type IndexPartnMapHolder struct {
	ptr *unsafe.Pointer
}

func (ipm *IndexPartnMapHolder) Init() {
	ipm.ptr = new(unsafe.Pointer)
}

func (ipm *IndexPartnMapHolder) Set(indexPartnMap IndexPartnMap) {
	atomic.StorePointer(ipm.ptr, unsafe.Pointer(&indexPartnMap))
}

func (ipm *IndexPartnMapHolder) Get() IndexPartnMap {
	if ptr := atomic.LoadPointer(ipm.ptr); ptr != nil {
		return *(*IndexPartnMap)(ptr)
	} else {
		return make(IndexPartnMap)
	}
}

func (ipm *IndexPartnMapHolder) Clone() IndexPartnMap {
	if ptr := atomic.LoadPointer(ipm.ptr); ptr != nil {
		currMap := *(*IndexPartnMap)(ptr)
		return CopyIndexPartnMap(currMap)
	} else {
		return make(IndexPartnMap)
	}
}

// Holder for StreamKeyspaceIdInstList
type StreamKeyspaceIdInstListHolder struct {
	ptr *unsafe.Pointer
}

func (s *StreamKeyspaceIdInstListHolder) Init() {
	s.ptr = new(unsafe.Pointer)
}

func (s *StreamKeyspaceIdInstListHolder) Set(streamKeyspaceIdInstList StreamKeyspaceIdInstList) {
	atomic.StorePointer(s.ptr, unsafe.Pointer(&streamKeyspaceIdInstList))
}

func (s *StreamKeyspaceIdInstListHolder) Get() StreamKeyspaceIdInstList {
	if ptr := atomic.LoadPointer(s.ptr); ptr != nil {
		return *(*StreamKeyspaceIdInstList)(ptr)
	} else {
		return make(StreamKeyspaceIdInstList)
	}
}

func (s *StreamKeyspaceIdInstListHolder) Clone() StreamKeyspaceIdInstList {
	if ptr := atomic.LoadPointer(s.ptr); ptr != nil {
		currList := *(*StreamKeyspaceIdInstList)(ptr)
		return CopyStreamKeyspaceIdInstList(currList)
	} else {
		return make(StreamKeyspaceIdInstList)
	}
}

func CopyStreamKeyspaceIdInstList(inList StreamKeyspaceIdInstList) StreamKeyspaceIdInstList {

	outList := make(StreamKeyspaceIdInstList)
	for streamId, keyspaceIdInstList := range inList {

		cloneKeyspaceIdInstList := make(KeyspaceIdInstList)
		for keyspaceId, instList := range keyspaceIdInstList {
			for _, instId := range instList {
				cloneKeyspaceIdInstList[keyspaceId] = append(cloneKeyspaceIdInstList[keyspaceId], instId)
			}
		}

		outList[streamId] = cloneKeyspaceIdInstList
	}
	return outList
}
