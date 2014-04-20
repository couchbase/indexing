//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package indexer

import (
	"errors"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/indexing/secondary/common"
)

//MutationQueue interface specifies methods which a mutation queue for indexer
//needs to implement
type MutationQueue interface {

	//enqueue a mutation reference based on vbucket
	Enqueue(mutation *common.Mutation, vbucket uint16) error

	//dequeue a vbucket's mutation and keep sending on a channel until stop signal
	Dequeue(vbucket uint16) (<-chan *common.Mutation, chan<- bool, error)
	//dequeue a vbucket's mutation upto seqno(wait if not available)
	DequeueUptoSeqno(vbucket uint16, seqno uint64) (<-chan *common.Mutation, error)
	//dequeue single element for a vbucket and return
	DequeueSingleElement(vbucket uint16) *common.Mutation

	//return reference to a vbucket's mutation at Tail of queue without dequeue
	PeekTail(vbucket uint16) *common.Mutation
	//return reference to a vbucket's mutation at Head of queue without dequeue
	PeekHead(vbucket uint16) *common.Mutation

	//return size of queue per vbucket
	GetSize(vbucket uint16) int64

	//returns the numbers of vbuckets for the queue
	GetNumVbuckets() uint16
}

//AtomicMutationQueue is a lock-free multi-queue with internal queue per
//vbucket for storing mutation references. This is loosely based on
//http://www.drdobbs.com/parallel/writing-lock-free-code-a-corrected-queue/210604448?pgno=1
//with the main difference being that free nodes are being reused here to reduce GC.
//
//It doesn't copy the mutation and its caller's responsiblity
//to allocate/deallocate Mutation struct. A mutation which is currently in queue
//shouldn't be freed.
//
//This implementation uses Go "atomic" pkg to provide safe concurrent access
//for a single reader and writer per vbucket queue without using mutex locks.
//
//It provides safe concurrent read/write access across vbucket queues.

type atomicMutationQueue struct {
	head        []unsafe.Pointer //head pointer per vbucket queue
	tail        []unsafe.Pointer //tail pointer per vbucket queue
	free        []*node          //free pointer per vbucket queue
	numVbuckets uint16           //num vbuckets for the queue
	size        []int64          //size of queue per vbucket
}

//NewAtomicMutationQueue allocates a new Atomic Mutation Queue and initializes it
func NewAtomicMutationQueue(numVbuckets uint16) *atomicMutationQueue {

	q := &atomicMutationQueue{head: make([]unsafe.Pointer, numVbuckets),
		tail:        make([]unsafe.Pointer, numVbuckets),
		free:        make([]*node, numVbuckets),
		size:        make([]int64, numVbuckets),
		numVbuckets: numVbuckets,
	}

	var x uint16
	for x = 0; x < numVbuckets; x++ {
		node := &node{} //sentinel node for the queue
		q.head[x] = unsafe.Pointer(node)
		q.tail[x] = unsafe.Pointer(node)
		q.free[x] = node
	}

	return q

}

//Node represents a single element in the queue
type node struct {
	mutation *common.Mutation
	next     *node
}

//Poll Interval for dequeue thread
const DEQUEUE_POLL_INTERVAL = 5

//Enqueue will enqueue the mutation reference for given vbucket.
//Caller should not free the mutation till it is dequeued.
//Mutation will not be copied internally by the queue.
func (q *atomicMutationQueue) Enqueue(mutation *common.Mutation, vbucket uint16) error {

	if vbucket < 0 || vbucket > q.numVbuckets-1 {
		return errors.New("vbucket out of range")
	}

	//create a new node
	n := q.allocNode(vbucket)
	n.mutation = mutation
	n.next = nil

	//point tail's next to new node
	tail := (*node)(atomic.LoadPointer(&q.tail[vbucket]))
	tail.next = n
	//update tail to new node
	atomic.StorePointer(&q.tail[vbucket], unsafe.Pointer(tail.next))

	atomic.AddInt64(&q.size[vbucket], 1)

	return nil

}

//DequeueUptoSeqno returns a channel on which it will return mutation reference
//for specified vbucket upto the sequence number specified.
//This function will keep polling till mutations upto seqno are available
//to be sent. It terminates when it finds a mutation with seqno higher than
//the one specified as argument. This allow for multiple mutations with same
//seqno (e.g. in case of multiple indexes)
//It closes the mutation channel to indicate its done.
func (q *atomicMutationQueue) DequeueUptoSeqno(vbucket uint16, seqno uint64) (
	<-chan *common.Mutation, error) {

	datach := make(chan *common.Mutation)

	go q.dequeueUptoSeqno(vbucket, seqno, datach)

	return datach, nil

}

func (q *atomicMutationQueue) dequeueUptoSeqno(vbucket uint16, seqno uint64,
	datach chan *common.Mutation) {

	//every DEQUEUE_POLL_INTERVAL milliseconds, check for new mutations
	ticker := time.NewTicker(time.Millisecond * DEQUEUE_POLL_INTERVAL)

	for _ = range ticker.C {
		for atomic.LoadPointer(&q.head[vbucket]) !=
			atomic.LoadPointer(&q.tail[vbucket]) { //if queue is nonempty

			head := (*node)(atomic.LoadPointer(&q.head[vbucket]))
			//copy the mutation pointer
			m := head.next.mutation
			if seqno >= m.Seqno {
				//move head to next
				atomic.StorePointer(&q.head[vbucket], unsafe.Pointer(head.next))
				atomic.AddInt64(&q.size[vbucket], -1)
				//send mutation to caller
				datach <- m
			} else {
				ticker.Stop()
				close(datach)
				return
			}
		}
	}
}

//Dequeue returns a channel on which it will return mutation reference for specified vbucket.
//This function will keep polling and send mutations as those become available.
//It returns a stop channel on which caller can signal it to stop.
func (q *atomicMutationQueue) Dequeue(vbucket uint16) (<-chan *common.Mutation,
	chan<- bool, error) {

	datach := make(chan *common.Mutation)
	stopch := make(chan bool)

	//every DEQUEUE_POLL_INTERVAL milliseconds, check for new mutations
	ticker := time.NewTicker(time.Millisecond * DEQUEUE_POLL_INTERVAL)

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

func (q *atomicMutationQueue) dequeue(vbucket uint16, datach chan *common.Mutation) {

	//keep dequeuing till list is empty
	for {
		m := q.DequeueSingleElement(vbucket)
		if m == nil {
			return
		}
		//send mutation to caller
		datach <- m
	}

}

//DequeueSingleElement dequeues a single element and returns.
//Returns nil in case of empty queue.
func (q *atomicMutationQueue) DequeueSingleElement(vbucket uint16) *common.Mutation {

	if atomic.LoadPointer(&q.head[vbucket]) !=
		atomic.LoadPointer(&q.tail[vbucket]) { //if queue is nonempty

		head := (*node)(atomic.LoadPointer(&q.head[vbucket]))
		//copy the mutation pointer
		m := head.next.mutation
		//move head to next
		atomic.StorePointer(&q.head[vbucket], unsafe.Pointer(head.next))
		atomic.AddInt64(&q.size[vbucket], -1)
		return m
	}
	return nil
}

//PeekTail returns reference to a vbucket's mutation at tail of queue without dequeue
func (q *atomicMutationQueue) PeekTail(vbucket uint16) *common.Mutation {
	if atomic.LoadPointer(&q.head[vbucket]) !=
		atomic.LoadPointer(&q.tail[vbucket]) { //if queue is nonempty
		tail := (*node)(atomic.LoadPointer(&q.tail[vbucket]))
		return tail.mutation
	}
	return nil
}

//PeekHead returns reference to a vbucket's mutation at head of queue without dequeue
func (q *atomicMutationQueue) PeekHead(vbucket uint16) *common.Mutation {
	if atomic.LoadPointer(&q.head[vbucket]) !=
		atomic.LoadPointer(&q.tail[vbucket]) { //if queue is nonempty
		head := (*node)(atomic.LoadPointer(&q.head[vbucket]))
		return head.mutation
	}
	return nil
}

//GetSize returns the size of the vbucket queue
func (q *atomicMutationQueue) GetSize(vbucket uint16) int64 {
	return atomic.LoadInt64(&q.size[vbucket])
}

//GetNumVbuckets returns the numbers of vbuckets for the queue
func (q *atomicMutationQueue) GetNumVbuckets() uint16 {
	return q.numVbuckets
}

//allocNode tries to get node from freelist, otherwise allocates a new node and returns
func (q *atomicMutationQueue) allocNode(vbucket uint16) *node {

	//get node from freelist
	n, err := q.popFreeList(vbucket)
	if err == nil {
		return n
	} else {
		//allocate new node and return
		return &node{}
	}

}

//popFreeList removes a node from freelist and returns to caller.
//if freelist is empty, it returns nil.
func (q *atomicMutationQueue) popFreeList(vbucket uint16) (*node, error) {

	if q.free[vbucket] != (*node)(atomic.LoadPointer(&q.head[vbucket])) {
		n := q.free[vbucket]
		q.free[vbucket] = q.free[vbucket].next
		n.mutation = nil
		n.next = nil
		return n, nil
	} else {
		return nil, errors.New("Free List Empty")
	}

}
