//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package indexer

import (
	slab "github.com/couchbase/go-slab"
	"sync"
)

//Slab Manager provides a wrapper around go-slab library to allow concurrent access and
//memory accounting(e.g. limit the max memory of Arena etc).
//It also has a release buffer to queue up release requests to reduce contention
//between concurrent threads waiting to return back memory.

type SlabManager interface {
	//AllocBuf allocates a buffer of given size. If returned buffer is nil,
	//Message will have error message
	AllocBuf(bufSize int) ([]byte, Message)

	//ReleaseBuf releases the buffer back to free pool
	ReleaseBuf(buf []byte) bool

	//SetMaxMemoryLimit sets the maximum memory that can be allocated
	SetMaxMemoryLimit(maxMemAlloc uint64) bool

	//GetMaxMemoryLimit returns the maximum memory that can be allocated
	GetMaxMemoryLimit() uint64
}

type slabManager struct {
	arena *slab.Arena
	lock  sync.Mutex //concurrent access to slab library needs to be protected

	startChunkSize int    //initial chunk size for the slab library
	slabSize       int    //size of each slab
	maxMemAlloc    uint64 //max memory that can be allocated

	currUserAllocatedMemory   uint64 //curr memory user has allocated
	currActualAllocatedMemory uint64 //curr slab memory allocated to fulfill the request

	releaseChan chan []byte //buffered chan to queue release requests

	internalErr bool        //this flag is set if underlying library goes to a bad state
	stopch      StopChannel //panic handler closes this channel to signal stop
}

const DEFAULT_GROWTH_FACTOR float64 = 2.0
const DEFAULT_RELEASE_BUFFER int = 10000

//NewSlabManager returns a slabManager struct instance with an initialized Arena
func NewSlabManager(startChunkSize int, slabSize int, maxMemAlloc uint64) (SlabManager, Message) {

	sm := &slabManager{
		arena: slab.NewArena(startChunkSize,
			slabSize,
			DEFAULT_GROWTH_FACTOR,
			nil), //default make([]byte) for slab memory

		startChunkSize: startChunkSize,
		slabSize:       slabSize,
		maxMemAlloc:    maxMemAlloc,

		releaseChan: make(chan []byte, DEFAULT_RELEASE_BUFFER),
	}

	//init error from slab library
	if sm.arena == nil {
		return nil, &MsgError{
			err: Error{code: ERROR_SLAB_INIT,
				severity: FATAL,
				category: SLAB_MANAGER}}
	}

	//start the release handler
	go sm.releaseHandler()

	return sm, nil
}

//AllocBuf is a thread-safe wrapper over go-slab's Alloc.
//It also limits the memory allocation to the specified limit.
func (sm *slabManager) AllocBuf(bufSize int) ([]byte, Message) {

	//bad alloc request
	if bufSize <= 0 {
		return nil, &MsgError{
			err: Error{code: ERROR_SLAB_BAD_ALLOC_REQUEST,
				severity: NORMAL,
				category: SLAB_MANAGER}}
	}

	//panic handler
	defer sm.panicHandler()

	//if the underlying library is in bad shape, return internal error
	//caller has to start a new slab manager
	if sm.internalErr {
		return nil, &MsgError{
			err: Error{code: ERROR_SLAB_INTERNAL_ERROR,
				severity: FATAL,
				category: SLAB_MANAGER}}
	}

	//acquire lock and allocate memory
	sm.lock.Lock()
	defer sm.lock.Unlock()

	//return error if caller has exhausted quota
	if sm.currUserAllocatedMemory >= sm.maxMemAlloc {
		return nil, &MsgError{
			err: Error{code: ERROR_SLAB_MEM_LIMIT_EXCEED,
				severity: NORMAL,
				category: SLAB_MANAGER}}
	}

	//allocate memory
	buf := sm.arena.Alloc(bufSize)

	if buf == nil {
		return nil, &MsgError{
			err: Error{code: ERROR_SLAB_INTERNAL_ALLOC_ERROR,
				severity: FATAL,
				category: SLAB_MANAGER}}
	}

	return buf, nil
}

//incrementStats increments the stats for given buf size
func (sm *slabManager) incrementStats(bufSize int) {

	sm.currUserAllocatedMemory += uint64(bufSize)
	if bufSize <= sm.startChunkSize {
		sm.currActualAllocatedMemory += uint64(sm.startChunkSize)
	} else { //get the right chunk size
		sm.currActualAllocatedMemory += uint64(((bufSize - 1) / sm.startChunkSize) *
			int(DEFAULT_GROWTH_FACTOR) * sm.startChunkSize)
	}
}

//ReleaseBuf queues the released buf on a buffered channel and returns.
//This helps to avoid lock contention between concurrent threads
//waiting to return back memory. A background goroutine will
//read from the channel and actually return the memory.
func (sm *slabManager) ReleaseBuf(buf []byte) bool {

	//if internalErr has happened, underlying library is unusable
	if sm.internalErr {
		return false
	}

	sm.releaseChan <- buf
	return true
}

//releaseHandler keeps polling the releaseChan and returns any buffer release back to the slab.
//It is a thread-safe wrapper over go-slab's DecRef.
func (sm *slabManager) releaseHandler() {

	//panic handler
	defer sm.panicHandler()

	ok := true
	var buf []byte
	for ok {
		select {
		case buf, ok = <-sm.releaseChan:
			func() {
				//lock the arena and release back memory
				sm.lock.Lock()
				defer sm.lock.Unlock()
				sm.arena.DecRef(buf)
				sm.decrementStats(len(buf))
			}()
		case <-sm.stopch:
			return
		}
	}

}

//decrementStats decrements the stats for given buf size
func (sm *slabManager) decrementStats(bufSize int) {

	sm.currUserAllocatedMemory -= uint64(bufSize)
	if bufSize <= sm.startChunkSize {
		sm.currActualAllocatedMemory -= uint64(sm.startChunkSize)
	} else { //get the right chunk size
		sm.currActualAllocatedMemory -= uint64(((bufSize - 1) / sm.startChunkSize) *
			int(DEFAULT_GROWTH_FACTOR) * sm.startChunkSize)
	}
}

//panicHandler handles the panic from underlying slab allocation library
func (sm *slabManager) panicHandler() {

	//panic recovery
	if r := recover(); r != nil {
		sm.internalErr = true //panic from underlying, mark the whole thing unusable
		close(sm.stopch)
	}
}

//SetMaxMemoryLimit sets the maximum memory that can be allocated
func (sm *slabManager) SetMaxMemoryLimit(maxMemAlloc uint64) bool {

	sm.lock.Lock()
	defer sm.lock.Unlock()
	sm.maxMemAlloc = maxMemAlloc
	return true
}

//GetMaxMemoryLimit returns the maximum memory that can be allocated
func (sm *slabManager) GetMaxMemoryLimit() uint64 {

	sm.lock.Lock()
	defer sm.lock.Unlock()
	return sm.maxMemAlloc
}
