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

// Holder for StreamKeyspaceIdInstList
type StreamKeyspaceIdInstsPerWorkerHolder struct {
	ptr *unsafe.Pointer
}

func (s *StreamKeyspaceIdInstsPerWorkerHolder) Init() {
	s.ptr = new(unsafe.Pointer)
}

func (s *StreamKeyspaceIdInstsPerWorkerHolder) Set(streamKeyspaceIdInstsPerWorker StreamKeyspaceIdInstsPerWorker) {
	atomic.StorePointer(s.ptr, unsafe.Pointer(&streamKeyspaceIdInstsPerWorker))
}

func (s *StreamKeyspaceIdInstsPerWorkerHolder) Get() StreamKeyspaceIdInstsPerWorker {
	if ptr := atomic.LoadPointer(s.ptr); ptr != nil {
		return *(*StreamKeyspaceIdInstsPerWorker)(ptr)
	} else {
		return make(StreamKeyspaceIdInstsPerWorker)
	}
}

func (s *StreamKeyspaceIdInstsPerWorkerHolder) Clone() StreamKeyspaceIdInstsPerWorker {
	if ptr := atomic.LoadPointer(s.ptr); ptr != nil {
		currList := *(*StreamKeyspaceIdInstsPerWorker)(ptr)
		return CopyStreamKeyspaceIdInstsPerWorker(currList)
	} else {
		return make(StreamKeyspaceIdInstsPerWorker)
	}
}

func CopyStreamKeyspaceIdInstsPerWorker(inList StreamKeyspaceIdInstsPerWorker) StreamKeyspaceIdInstsPerWorker {

	outList := make(StreamKeyspaceIdInstsPerWorker)
	for streamId, keyspaceIdInstsPerWorker := range inList {

		cloneKeyspaceIdInstsPerWorker := make(KeyspaceIdInstsPerWorker)
		for keyspaceId, instsPerWorker := range keyspaceIdInstsPerWorker {
			cloneKeyspaceIdInstsPerWorker[keyspaceId] = make([][]common.IndexInstId, len(instsPerWorker))
			for workerId, instList := range instsPerWorker {
				cloneKeyspaceIdInstsPerWorker[keyspaceId][workerId] = make([]common.IndexInstId, len(instList))
				copy(cloneKeyspaceIdInstsPerWorker[keyspaceId][workerId], instList)
			}
		}

		outList[streamId] = cloneKeyspaceIdInstsPerWorker
	}
	return outList
}

type BucketNameNumVBucketsMapHolder struct {
	ptr *unsafe.Pointer
}

func (h *BucketNameNumVBucketsMapHolder) Init() {
	h.ptr = new(unsafe.Pointer)
}

func (h *BucketNameNumVBucketsMapHolder) Set(bucketNameNumVBucketsMap map[string]int) {
	atomic.StorePointer(h.ptr, unsafe.Pointer(&bucketNameNumVBucketsMap))
}

func (h *BucketNameNumVBucketsMapHolder) Get() map[string]int {
	if ptr := atomic.LoadPointer(h.ptr); ptr != nil {
		return *(*map[string]int)(ptr)
	} else {
		return make(map[string]int)
	}
}

func (h *BucketNameNumVBucketsMapHolder) Clone() map[string]int {
	if ptr := atomic.LoadPointer(h.ptr); ptr != nil {
		currMap := *(*map[string]int)(ptr)
		newMap := make(map[string]int)
		for b, nvb := range currMap {
			newMap[b] = nvb
		}
		return newMap
	} else {
		return make(map[string]int)
	}
}
