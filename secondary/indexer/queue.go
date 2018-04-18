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
	"github.com/couchbase/indexing/secondary/common"
	"sync/atomic"
)

//-----------------------------
// Buffer pool
//-----------------------------

var queueBufPool *common.BytesBufPool

func init() {
	queueBufPool = common.NewByteBufferPool(DEFAULT_MAX_SEC_KEY_LEN + MAX_DOCID_LEN + 2)
}

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

	enqCount int64
	deqCount int64
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

	initRows(rbuf.buf)
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

			tmp := b.buf[b.free].copyKey(key.key)
			b.buf[b.free] = *key
			b.buf[b.free].key = tmp
			b.free = next
			if atomic.AddInt64(&b.count, 1) == b.limit || key.last {
				b.notifyEnq()
			}

			atomic.AddInt64(&b.enqCount, 1)
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

			tmp := row.copyKey(b.buf[b.head].key)
			*row = b.buf[b.head]
			row.key = tmp
			b.head = next
			if atomic.AddInt64(&b.count, -1) == (b.size - 1) {
				b.notifyDeq()
			}

			atomic.AddInt64(&b.deqCount, 1)
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
		tmp := row.copyKey(b.buf[b.head].key)
		*row = b.buf[b.head]
		row.key = tmp
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

func (b *Queue) EnqueueCount() int64 {

	return b.enqCount
}

func (b *Queue) DequeueCount() int64 {

	return b.deqCount
}

// Unblock all Enqueue and Dequeue calls
func (b *Queue) Close() {

	if atomic.SwapInt32(&b.isClose, 1) == 0 {
		close(b.donech)
		freeRows(b.buf)
	}
}

//-----------------------------
// Row
//-----------------------------

func (r *Row) copyKey(key []byte) []byte {
	if len(key) > cap(r.key) {
		r.freeKeyBuf()
		r.key = make([]byte, 0, len(key))
	}

	r.key = r.key[:0]
	r.key = append(r.key, key...)

	return r.key
}

func (r *Row) initKeyBuf() {
	bufPtr := queueBufPool.Get()
	r.key = (*bufPtr)[:0]
}

func (r *Row) freeKeyBuf() {
	if r.key != nil {
		queueBufPool.Put(&r.key)
		r.key = nil
	}
}

func initRows(rows []Row) {
	for _, row := range rows {
		row.initKeyBuf()
	}
}

func freeRows(rows []Row) {
	for _, row := range rows {
		row.freeKeyBuf()
	}
}
