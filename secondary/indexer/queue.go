// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.
package indexer

import (
	"sync/atomic"
)

//-----------------------------
// Queue with a rotating buffer
//-----------------------------

type Row struct {
	key  []byte
	len  int
	last bool
}

type Queue struct {
	size    int64
	count   int64
	limit   int64
	isClose int32

	buf  []Row
	head int64 // pos of head of buf
	free int64 // pos of head of free list

	notifych chan bool
	enqch    chan bool
	deqch    chan bool
	donech   chan bool
}

//
// Constructor
//
func NewQueue(size int64, limit int64, notifych chan bool) *Queue {

	rbuf := &Queue{}
	rbuf.size = size
	rbuf.limit = limit
	rbuf.buf = make([]Row, size)
	rbuf.notifych = notifych
	rbuf.enqch = make(chan bool, 1)
	rbuf.deqch = make(chan bool, 1)
	rbuf.donech = make(chan bool)

	return rbuf
}

//
// This funciton notifies a new row added to buffer.
//
func (b *Queue) notifyEnq() {

	select {
	case b.enqch <- true:
	default:
	}

	if b.notifych != nil {
		select {
		case b.notifych <- true:
		default:
		}
	}
}

//
// This funciton notifies a new row removed from buffer
//
func (b *Queue) notifyDeq() {

	select {
	case b.deqch <- true:
	default:
	}
}

//
// Add a new row to the rotating buffer.  If the buffer is full,
// this function will be blocked.
//
func (b *Queue) Enqueue(key *Row) {

	for {
		count := atomic.LoadInt64(&b.count)

		if count < b.size {
			next := b.free + 1
			if next >= b.size {
				next = 0
			}

			b.buf[b.free] = *key
			b.free = next
			if atomic.AddInt64(&b.count, 1) == b.limit || key.last {
				b.notifyEnq()
			}
			return
		}

		select {
		case <-b.deqch:
		case <-b.donech:
			return
		}
	}
}

//
// Remove the first row from the rotating buffer.  If the buffer is empty,
// this function will be blocked.
//
func (b *Queue) Dequeue(row *Row) bool {

	for {
		count := atomic.LoadInt64(&b.count)

		if count > 0 {
			next := b.head + 1
			if next >= b.size {
				next = 0
			}

			*row = b.buf[b.head]
			b.head = next
			if atomic.AddInt64(&b.count, -1) == (b.size - 1) {
				b.notifyDeq()
			}
			return true
		}

		select {
		case <-b.enqch:
		case <-b.donech:
			return false
		}
	}

	return false
}

//
// Get a copy of the first row in the rotating buffer.  This
// function is not blocked and will not return a copy.
//
func (b *Queue) Peek(row *Row) bool {

	count := atomic.LoadInt64(&b.count)

	if count > 0 {
		*row = b.buf[b.head]
		return true
	}

	return false
}

func (b *Queue) Len() int64 {

	return atomic.LoadInt64(&b.count)
}

func (b *Queue) Cap() int64 {

	return b.size
}

// Unblock all Enqueue and Dequeue calls
func (b *Queue) Close() {

	if atomic.SwapInt32(&b.isClose, 1) == 0 {
		close(b.donech)
	}
}
