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

func (nh *nodesInfoHolder) Set(nodes *nodesInfo) {
	atomic.StorePointer(nh.ptr, unsafe.Pointer(nodes))
}

func (nh *nodesInfoHolder) Get() *nodesInfo {
	ptr := atomic.LoadPointer(nh.ptr)
	return (*nodesInfo)(ptr)
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
