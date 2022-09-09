package common

import (
	"sync/atomic"
	"unsafe"
)

type nodesInfoHolder struct {
	ptr *unsafe.Pointer
}

func (nh *nodesInfoHolder) Init() {
	nh.ptr = new(unsafe.Pointer)
}

func (nh *nodesInfoHolder) Set(nodes *NodesInfo) {
	atomic.StorePointer(nh.ptr, unsafe.Pointer(nodes))
}

func (nh *nodesInfoHolder) Get() *NodesInfo {
	ptr := atomic.LoadPointer(nh.ptr)
	return (*NodesInfo)(ptr)
}

type collectionInfoHolder struct {
	ptr *unsafe.Pointer
}

func (ch *collectionInfoHolder) Init() {
	ch.ptr = new(unsafe.Pointer)
}

func (ch *collectionInfoHolder) Set(ptr *collectionInfo) {
	atomic.StorePointer(ch.ptr, unsafe.Pointer(ptr))
}

func (ch *collectionInfoHolder) Get() *collectionInfo {
	ptr := atomic.LoadPointer(ch.ptr)
	return (*collectionInfo)(ptr)
}

type bucketInfoHolder struct {
	ptr *unsafe.Pointer
}

func (bh *bucketInfoHolder) Init() {
	bh.ptr = new(unsafe.Pointer)
}

func (bh *bucketInfoHolder) Set(bucketInfoPtr *bucketInfo) {
	atomic.StorePointer(bh.ptr, unsafe.Pointer(bucketInfoPtr))
}

func (bh *bucketInfoHolder) Get() *bucketInfo {
	bucketInfoPtr := atomic.LoadPointer(bh.ptr)
	return (*bucketInfo)(bucketInfoPtr)
}

type poolInfoHolder struct {
	ptr *unsafe.Pointer
}

func (nh *poolInfoHolder) Init() {
	nh.ptr = new(unsafe.Pointer)
}

func (nh *poolInfoHolder) Set(nodes *PoolInfo) {
	atomic.StorePointer(nh.ptr, unsafe.Pointer(nodes))
}

func (nh *poolInfoHolder) Get() *PoolInfo {
	ptr := atomic.LoadPointer(nh.ptr)
	return (*PoolInfo)(ptr)
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
