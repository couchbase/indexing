//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package indexer

import (
	"errors"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

// MutationQueue interface specifies methods which a mutation queue for indexer
// needs to implement
type MutationQueue interface {

	//enqueue a mutation reference based on vbucket. This is a blocking call which
	//will wait in case there is no free slot available for allocation.
	//caller can close the appch to force this call to return.
	Enqueue(mutation *MutationKeys, vbucket Vbucket, appch StopChannel) error

	//dequeue a vbucket's mutation and keep sending on a channel until stop signal
	Dequeue(vbucket Vbucket) (<-chan *MutationKeys, chan<- bool, error)
	//dequeue a vbucket's mutation upto seqno(wait if not available)
	DequeueUptoSeqno(vbucket Vbucket, seqno uint64) (<-chan *MutationKeys, chan bool, error)
	//dequeue single element for a vbucket and return
	DequeueSingleElement(vbucket Vbucket) (*MutationKeys, bool)
	//dequeue N elements for a vbucket and return
	DequeueN(vbucket Vbucket, count uint64) (<-chan *MutationKeys, chan bool, error)

	//return reference to a vbucket's mutation at Tail of queue without dequeue
	PeekTail(vbucket Vbucket) *MutationKeys
	//return reference to a vbucket's mutation at Head of queue without dequeue
	PeekHead(vbucket Vbucket) *MutationKeys

	//return size of queue per vbucket
	GetSize(vbucket Vbucket) int64

	//returns the numbers of vbuckets for the queue
	GetNumVbuckets() uint16

	//set the minVbQueueLength config
	SetMinVbQueueLength(minLength uint64)

	//destroy the resources
	Destroy()
}

//AtomicMutationQueue is a lock-free multi-queue with internal queue per
//vbucket for storing mutation references. This is loosely based on
//http://www.drdobbs.com/parallel/writing-lock-free-code-a-corrected-queue/210604448?pgno=1
//with the main difference being that free nodes are being reused here to reduce GC.
//
//It doesn't copy the mutation and its caller's responsiblity
//to allocate/deallocate KeyVersions struct. A mutation which is currently in queue
//shouldn't be freed.
//
//This implementation uses Go "atomic" pkg to provide safe concurrent access
//for a single reader and writer per vbucket queue without using mutex locks.
//
//It provides safe concurrent read/write access across vbucket queues.

type atomicMutationQueue struct {
	head      []unsafe.Pointer //head pointer per vbucket queue
	tail      []unsafe.Pointer //tail pointer per vbucket queue
	size      []int64          //size of queue per vbucket
	memUsed   *int64           //memory used by queue
	maxMemory *int64           //max memory to be used

	allocPollInterval   uint64 //poll interval for new allocs, if queue is full
	dequeuePollInterval uint64 //poll interval for dequeue, if waiting for mutations
	resultChanSize      uint64 //size of buffered result channel
	minQueueLen         uint64

	free        []*node //free pointer per vbucket queue
	stopch      []StopChannel
	numVbuckets uint16 //num vbuckets for the queue
	isDestroyed atomic.Bool

	keyspaceId string
}

// NewAtomicMutationQueue allocates a new Atomic Mutation Queue and initializes it
func NewAtomicMutationQueue(keyspaceId string, numVbuckets uint16, maxMemory *int64,
	memUsed *int64, config common.Config) *atomicMutationQueue {

	q := &atomicMutationQueue{head: make([]unsafe.Pointer, numVbuckets),
		tail:                make([]unsafe.Pointer, numVbuckets),
		free:                make([]*node, numVbuckets),
		size:                make([]int64, numVbuckets),
		numVbuckets:         numVbuckets,
		maxMemory:           maxMemory,
		memUsed:             memUsed,
		stopch:              make([]StopChannel, numVbuckets),
		allocPollInterval:   getAllocPollInterval(config),
		dequeuePollInterval: config["mutation_queue.dequeuePollInterval"].Uint64(),
		resultChanSize:      config["mutation_queue.resultChanSize"].Uint64(),
		minQueueLen:         config["settings.minVbQueueLength"].Uint64(),
		keyspaceId:          keyspaceId,
	}

	var x uint16
	for x = 0; x < numVbuckets; x++ {
		node := &node{} //sentinel node for the queue
		q.head[x] = unsafe.Pointer(node)
		q.tail[x] = unsafe.Pointer(node)
		q.free[x] = node
		q.stopch[x] = make(StopChannel)
		q.size[x] = 0
	}

	return q

}

// Node represents a single element in the queue
type node struct {
	mutation *MutationKeys
	next     *node
}

// Enqueue will enqueue the mutation reference for given vbucket.
// Caller should not free the mutation till it is dequeued.
// Mutation will not be copied internally by the queue.
// caller can call appch to force this call to return. Otherwise
// this is a blocking call till there is a slot available for enqueue.
func (q *atomicMutationQueue) Enqueue(mutation *MutationKeys,
	vbucket Vbucket, appch StopChannel) error {

	if vbucket < 0 || vbucket > Vbucket(q.numVbuckets)-1 {
		return errors.New("vbucket out of range")
	}

	//no more requests are taken once queue
	//is marked as destroyed
	if q.isDestroyed.Load() {
		return nil
	}

	//create a new node
	n := q.allocNode(vbucket, appch)
	if n == nil {
		return nil
	}

	n.mutation = mutation
	n.next = nil

	atomic.AddInt64(q.memUsed, n.mutation.Size())

	//point tail's next to new node
	tail := (*node)(atomic.LoadPointer(&q.tail[vbucket]))
	tail.next = n
	//update tail to new node
	atomic.StorePointer(&q.tail[vbucket], unsafe.Pointer(tail.next))

	atomic.AddInt64(&q.size[vbucket], 1)

	return nil

}

// DequeueUptoSeqno returns a channel on which it will return mutation reference
// for specified vbucket upto the sequence number specified.
// This function will keep polling till mutations upto seqno are available
// to be sent. It terminates when it finds a mutation with seqno higher than
// the one specified as argument. This allow for multiple mutations with same
// seqno (e.g. in case of multiple indexes)
// It closes the mutation channel to indicate its done.
func (q *atomicMutationQueue) DequeueUptoSeqno(vbucket Vbucket, seqno uint64) (
	<-chan *MutationKeys, chan bool, error) {

	datach := make(chan *MutationKeys, q.resultChanSize)
	errch := make(chan bool)

	go q.dequeueUptoSeqno(vbucket, seqno, datach, errch)

	return datach, errch, nil

}

func (q *atomicMutationQueue) dequeueUptoSeqno(vbucket Vbucket, seqno uint64,
	datach chan *MutationKeys, errch chan bool) {

	var dequeueSeq uint64
	var totalWait int

	for {
		memReleased := int64(0)
		totalWait += int(q.dequeuePollInterval)
		if totalWait > 30000 {
			if totalWait%5000 == 0 {
				logging.Warnf("Indexer::MutationQueue Dequeue Waiting For "+
					"Seqno %v KeyspaceId %v Vbucket %v for %v ms. Last Dequeue %v.", seqno,
					q.keyspaceId, vbucket, totalWait, dequeueSeq)
			}
		}
		for atomic.LoadPointer(&q.head[vbucket]) !=
			atomic.LoadPointer(&q.tail[vbucket]) { //if queue is nonempty

			head := (*node)(atomic.LoadPointer(&q.head[vbucket]))
			//copy the mutation pointer
			m := head.next.mutation
			if m != nil && seqno >= m.meta.seqno {
				//free mutation pointer
				head.next.mutation = nil
				//move head to next
				atomic.StorePointer(&q.head[vbucket], unsafe.Pointer(head.next))
				atomic.AddInt64(&q.size[vbucket], -1)
				memReleased += m.size
				//send mutation to caller
				dequeueSeq = m.meta.seqno
				datach <- m
			} else {

				if m == nil {
					if q.isDestroyed.Load() {
						logging.Infof("Indexer::MutationQueue Dequeue Aborted as "+
							"mutation is nil and queue is destroyed for keyspaceId: %v. "+
							"vbucket: %v Last Dequeue %v Head Seqno %v.",
							q.keyspaceId, vbucket, dequeueSeq, seqno)
						atomic.AddInt64(q.memUsed, -memReleased)
						close(datach) // close datach and return as if normal flush is done
						return
					} else {
						logging.Warnf("Indexer::MutationQueue Dequeue Aborted as "+
							"mutation is nil. Seqno: %v, KeyspaceId: %v, vbucket: %v, Last Dequeue %v",
							seqno, q.keyspaceId, vbucket, dequeueSeq)
					}
				} else {
					logging.Warnf("Indexer::MutationQueue Dequeue Aborted For "+
						"Seqno %v KeyspaceId %v Vbucket %v. Last Dequeue %v Head Seqno %v.", seqno,
						q.keyspaceId, vbucket, dequeueSeq, m.meta.seqno)
				}
				atomic.AddInt64(q.memUsed, -memReleased)
				close(errch)
				return
			}

			//once the seqno is reached, close the channel
			if seqno <= dequeueSeq {
				atomic.AddInt64(q.memUsed, -memReleased)
				close(datach)
				return
			}
		}
		atomic.AddInt64(q.memUsed, -memReleased)
		time.Sleep(time.Millisecond * time.Duration(q.dequeuePollInterval))
	}
}

// Dequeue returns a channel on which it will return mutation reference for specified vbucket.
// This function will keep polling and send mutations as those become available.
// It returns a stop channel on which caller can signal it to stop.
func (q *atomicMutationQueue) Dequeue(vbucket Vbucket) (<-chan *MutationKeys,
	chan<- bool, error) {

	datach := make(chan *MutationKeys)
	stopch := make(chan bool)

	//every dequeuePollInterval milliseconds, check for new mutations
	ticker := time.NewTicker(time.Millisecond * time.Duration(q.dequeuePollInterval))

	go func() {
		for {
			select {
			case <-ticker.C:
				q.dequeue(vbucket, datach)
			case <-stopch:
				ticker.Stop()
				close(datach)
				return
			}
		}
	}()

	return datach, stopch, nil

}

func (q *atomicMutationQueue) dequeue(vbucket Vbucket, datach chan *MutationKeys) {

	//keep dequeuing till list is empty
	for {
		m, queueEmpty := q.DequeueSingleElement(vbucket)
		if queueEmpty {
			return
		}
		if m != nil {
			datach <- m //send mutation to caller
		}
	}

}

// DequeueSingleElement dequeues a single element and returns.
// Returns nil in case of empty queue.
func (q *atomicMutationQueue) DequeueSingleElement(vbucket Vbucket) (*MutationKeys, bool) {

	if atomic.LoadPointer(&q.head[vbucket]) !=
		atomic.LoadPointer(&q.tail[vbucket]) { //if queue is nonempty

		head := (*node)(atomic.LoadPointer(&q.head[vbucket]))
		//copy the mutation pointer
		m := head.next.mutation
		//free mutation pointer
		head.next.mutation = nil
		//move head to next
		atomic.StorePointer(&q.head[vbucket], unsafe.Pointer(head.next))
		atomic.AddInt64(&q.size[vbucket], -1)
		if m != nil {
			atomic.AddInt64(q.memUsed, -m.Size())
		}
		return m, false
	}
	return nil, true
}

// DequeueN returns a channel on which it will return mutation reference
// for specified vbucket upto the count of mutations specified.
// This function will keep polling till mutations upto count are available
// to be sent. It terminates when it has sent the number of mutations
// specified as argument.
// It closes the mutation channel to indicate its done.
func (q *atomicMutationQueue) DequeueN(vbucket Vbucket, count uint64) (
	<-chan *MutationKeys, chan bool, error) {

	datach := make(chan *MutationKeys, q.resultChanSize)
	errch := make(chan bool)

	go q.dequeueN(vbucket, count, datach, errch)

	return datach, errch, nil

}

func (q *atomicMutationQueue) dequeueN(vbucket Vbucket, count uint64,
	datach chan *MutationKeys, errch chan bool) {

	var dequeueSeq uint64
	var totalWait int
	var currCount uint64

	for {
		memReleased := int64(0)
		totalWait += int(q.dequeuePollInterval)
		if totalWait > 30000 {
			if totalWait%5000 == 0 {
				logging.Warnf("Indexer::MutationQueue Dequeue Waiting For "+
					"Count %v KeyspaceId %v Vbucket %v for %v ms. Curr Count %v Seq %v. ",
					count, q.keyspaceId, vbucket, totalWait, currCount, dequeueSeq)
			}
		}
		for atomic.LoadPointer(&q.head[vbucket]) !=
			atomic.LoadPointer(&q.tail[vbucket]) { //if queue is nonempty

			head := (*node)(atomic.LoadPointer(&q.head[vbucket]))
			//copy the mutation pointer
			m := head.next.mutation
			if m == nil { // Early exit if mutation queue is destroyed while DequeueN is in progress
				atomic.AddInt64(q.memUsed, -memReleased)
				close(datach)
				return
			}

			if currCount < count {
				//free mutation pointer
				head.next.mutation = nil
				//move head to next
				atomic.StorePointer(&q.head[vbucket], unsafe.Pointer(head.next))
				atomic.AddInt64(&q.size[vbucket], -1)
				memReleased += m.Size()
				//send mutation to caller
				dequeueSeq = m.meta.seqno
				currCount++
				datach <- m
			}

			//once count is reached, close the channel
			if currCount >= count {
				atomic.AddInt64(q.memUsed, -memReleased)
				close(datach)
				return
			}
		}
		atomic.AddInt64(q.memUsed, -memReleased)
		time.Sleep(time.Millisecond * time.Duration(q.dequeuePollInterval))
	}
}

// PeekTail returns reference to a vbucket's mutation at tail of queue without dequeue
func (q *atomicMutationQueue) PeekTail(vbucket Vbucket) *MutationKeys {
	if atomic.LoadPointer(&q.head[vbucket]) !=
		atomic.LoadPointer(&q.tail[vbucket]) { //if queue is nonempty
		tail := (*node)(atomic.LoadPointer(&q.tail[vbucket]))
		return tail.mutation
	}
	return nil
}

// PeekHead returns reference to a vbucket's mutation at head of queue without dequeue
func (q *atomicMutationQueue) PeekHead(vbucket Vbucket) *MutationKeys {
	if atomic.LoadPointer(&q.head[vbucket]) !=
		atomic.LoadPointer(&q.tail[vbucket]) { //if queue is nonempty
		head := (*node)(atomic.LoadPointer(&q.head[vbucket]))
		return head.mutation
	}
	return nil
}

// GetSize returns the size of the vbucket queue
func (q *atomicMutationQueue) GetSize(vbucket Vbucket) int64 {
	return atomic.LoadInt64(&q.size[vbucket])
}

// GetNumVbuckets returns the numbers of vbuckets for the queue
func (q *atomicMutationQueue) GetNumVbuckets() uint16 {
	return q.numVbuckets
}

// allocNode tries to get node from freelist, otherwise allocates a new node and returns
func (q *atomicMutationQueue) allocNode(vbucket Vbucket, appch StopChannel) *node {

	n := q.checkMemAndAlloc(vbucket)
	if n != nil {
		return n
	}

	//every allocPollInterval milliseconds, check for memory usage
	ticker := time.NewTicker(time.Millisecond * time.Duration(q.allocPollInterval))
	defer ticker.Stop()

	var totalWait uint64
	for {
		select {
		case <-ticker.C:
			totalWait += q.allocPollInterval
			n := q.checkMemAndAlloc(vbucket)
			if n != nil {
				return n
			}
			if totalWait > 300000 { // 5mins
				logging.Warnf("Indexer::MutationQueue Max Wait Period for Node "+
					"Alloc Expired %v. Forcing Alloc. KeyspaceId %v Vbucket %v", totalWait, q.keyspaceId, vbucket)
				return &node{}
			} else if totalWait > 5000 {
				if totalWait%3000 == 0 {
					logging.Warnf("Indexer::MutationQueue Waiting for Node "+
						"Alloc for %v Milliseconds KeyspaceId %v Vbucket %v", totalWait, q.keyspaceId, vbucket)
				}
			}

		case <-q.stopch[vbucket]:
			return nil

		case <-appch:
			//caller no longer wants to wait
			//allocate new node and return
			return &node{}

		}
	}

	return nil

}

func (q *atomicMutationQueue) checkMemAndAlloc(vbucket Vbucket) *node {

	currMem := atomic.LoadInt64(q.memUsed)
	maxMem := atomic.LoadInt64(q.maxMemory)
	currLen := atomic.LoadInt64(&q.size[vbucket])
	minQueueLen := atomic.LoadUint64(&q.minQueueLen)

	if currMem < maxMem || currLen < int64(minQueueLen) {
		//get node from freelist
		n := q.popFreeList(vbucket)
		if n != nil {
			return n
		} else {
			//allocate new node and return
			return &node{}
		}
	} else {
		return nil
	}

}

// popFreeList removes a node from freelist and returns to caller.
// if freelist is empty, it returns nil.
func (q *atomicMutationQueue) popFreeList(vbucket Vbucket) *node {

	if q.free[vbucket] != (*node)(atomic.LoadPointer(&q.head[vbucket])) {
		n := q.free[vbucket]
		q.free[vbucket] = q.free[vbucket].next
		n.mutation = nil
		n.next = nil
		return n
	} else {
		return nil
	}

}

// Destroy will free up all resources of the queue.
// Importantly it will free up pending mutations as well.
// Once destroy have been called, further enqueue operations
// will be no-op.
func (q *atomicMutationQueue) Destroy() {

	//set the flag so no more Enqueue requests
	//are taken on this queue
	q.isDestroyed.Store(true)

	//ensure all pending allocs get stopped
	var i uint16
	for i = 0; i < q.numVbuckets; i++ {
		close(q.stopch[i])
	}

	//dequeue all the items in the queue and free
	for i = 0; i < q.numVbuckets; i++ {
		mutch := make(chan *MutationKeys)
		go func() {
			for mutk := range mutch {
				mutk.Free()
			}
		}()
		q.dequeue(Vbucket(i), mutch)
		close(mutch)
	}

}

func (q *atomicMutationQueue) SetMinVbQueueLength(minLength uint64) {
	atomic.StoreUint64(&q.minQueueLen, minLength)
}

func getAllocPollInterval(config common.Config) uint64 {

	if common.GetStorageMode() == common.FORESTDB {
		return config["mutation_queue.fdb.allocPollInterval"].Uint64()
	} else {
		return config["mutation_queue.moi.allocPollInterval"].Uint64()
	}

}
