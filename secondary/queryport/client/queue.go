// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package client

import (
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/query/value"
	"sync/atomic"
)

//-----------------------------
// Queue with a rotating buffer
//-----------------------------

type Row struct {
	pkey  []byte
	value []value.Value
	skey  common.ScanResultKey
	last  bool
}

type Queue struct {
	size    int64
	count   int64
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
func NewQueue(size int64, notifych chan bool) *Queue {

	rbuf := &Queue{}
	rbuf.size = size
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
func (b *Queue) NotifyEnq() {

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
func (b *Queue) NotifyDeq() {

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
			atomic.AddInt64(&b.count, 1)
			if key.last {
				b.NotifyEnq()
			}
			return
		}

		b.NotifyEnq()
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
				b.NotifyDeq()
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
