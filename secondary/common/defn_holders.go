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
