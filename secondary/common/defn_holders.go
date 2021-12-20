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
