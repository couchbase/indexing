package memcached

import (
	"sync/atomic"
	"unsafe"

	"github.com/couchbase/indexing/secondary/dcp/transport"
)

type node struct {
	pkt   *transport.MCRequest
	bytes int
	next  *node
}

// Implementation of a concurrent queue (Single writer & single reader queue)
type AtomicMutationQueue struct {
	head unsafe.Pointer
	tail unsafe.Pointer

	activeMutCh chan bool

	size  int64 // Represents the current size of the queue (in bytes)
	items int64 // Represents the count of mutations in the queue
}

func NewAtomicMutationQueue() *AtomicMutationQueue {
	q := &AtomicMutationQueue{
		activeMutCh: make(chan bool),
	}
	node := &node{} // sentinal node
	q.head = unsafe.Pointer(node)
	q.tail = unsafe.Pointer(node)

	return q
}

// Enqueue writes a mutation to the queue. It inserts a mutation at the
// "tail" part of the queue. This queue is designed for single writer
// and single reader. Hence, StorePointer is sufficient to enqueue an
// item in the queue (instead of a CAS)
func (q *AtomicMutationQueue) Enqueue(pkt *transport.MCRequest, bytes int) {

	newNode := &node{
		pkt:   pkt,
		bytes: bytes,
		next:  nil,
	}

	tail := (*node)(atomic.LoadPointer((&q.tail)))
	tail.next = newNode

	atomic.StorePointer(&q.tail, unsafe.Pointer(newNode))

	atomic.AddInt64(&q.size, int64(bytes))
	atomic.AddInt64(&q.items, 1)

	q.updateMutCh() // Notify waiting consumer that mutations are available
}

// Dequeue will return a mutation if the mutation is ready. Otherwise, it
// will block until the mutation arrives. This atomic mutation queue is designed
// for single reader and single writer - So, LoadPointer & StorePointer are sufficient
// instead of CAS
func (q *AtomicMutationQueue) Dequeue(abortCh chan bool) (*transport.MCRequest, int) {

	for {
		headPtr := (atomic.LoadPointer(&q.head))
		tailPtr := (atomic.LoadPointer(&q.tail))

		if headPtr != tailPtr {
			head := (*node)(headPtr)
			pkt := head.next.pkt
			bytes := head.next.bytes
			head.next.pkt = nil

			atomic.StorePointer(&q.head, unsafe.Pointer(head.next))
			atomic.AddInt64(&q.size, 0-int64(bytes)) // Decrement the size
			atomic.AddInt64(&q.items, -1)

			return pkt, bytes
		} else {
			select {
			case <-q.activeMutCh: // Wait for mutations
			case <-abortCh:
				return nil, 0
			}
		}
	}

}

func (q *AtomicMutationQueue) GetSize() int64 {
	return atomic.LoadInt64(&q.size)
}

func (q *AtomicMutationQueue) GetItems() int64 {
	return atomic.LoadInt64(&q.items)
}

// Writes to activeMutCh if there are any mutations.
// Skip if the channel is already full
func (q *AtomicMutationQueue) updateMutCh() {
	select {
	case q.activeMutCh <- true:
		return
	default:
		return
	}
}
