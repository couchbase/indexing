// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.
package client

import (
	"errors"
	"fmt"
	"github.com/couchbase/indexing/secondary/collatejson"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/query/value"
	"runtime"
	"sync/atomic"
)

var SkipPartitionError = errors.New("Skip Partition")

//--------------------------
// request broker
//--------------------------

type RequestBroker struct {
	scan    ScanRequestHandler
	count   CountRequestHandler
	factory ResponseHandlerFactory
	sender  ResponseSender

	size     int64
	sorted   bool
	queues   []*Queue
	notifych chan bool
	closed   int32
	killch   chan bool

	sendCount    int64
	receiveCount int64
	numIndexers  int64

	requestId string
}

type doneStatus struct {
	err     error
	partial bool
}

//
// New Request Broker
//
func NewRequestBroker(requestId string, size int64) *RequestBroker {

	return &RequestBroker{requestId: requestId, size: size, sorted: true}
}

//
// Set ResponseHandlerFactory
//
func (b *RequestBroker) SetResponseHandlerFactory(factory ResponseHandlerFactory) {

	b.factory = factory
}

//
// Set ScanRequestHandler
//
func (b *RequestBroker) SetScanRequestHandler(handler ScanRequestHandler) {

	b.scan = handler
}

//
// Set CountRequestHandler
//
func (b *RequestBroker) SetCountRequestHandler(handler CountRequestHandler) {

	b.count = handler
}

//
// Set ResponseSender
//
func (b *RequestBroker) SetResponseSender(sender ResponseSender) {

	b.sender = sender
}

//
// Close the request broker
//
func (b *RequestBroker) Close() {

	atomic.StoreInt32(&b.closed, 1)
	close(b.killch)

	for _, queue := range b.queues {
		queue.Close()
	}
}

func (b *RequestBroker) IncrementReceiveCount(count int) {

	atomic.AddInt64(&b.receiveCount, int64(count))
}

func (b *RequestBroker) IncrementSendCount() {

	atomic.AddInt64(&b.sendCount, 1)
}

func (b *RequestBroker) SetNumIndexers(num int) {

	atomic.StoreInt64(&b.numIndexers, int64(num))
}

func (b *RequestBroker) NumIndexers() int64 {

	return atomic.LoadInt64(&b.numIndexers)
}

//
// Is the request broker closed
//
func (b *RequestBroker) isClose() bool {

	return atomic.LoadInt32(&b.closed) == 1
}

//
// Scatter requests over multiple connections
//
func (c *RequestBroker) scatter(client []*GsiScanClient, index *common.IndexDefn, rollback []int64,
	partition [][]common.PartitionId, numPartition uint32, settings *ClientSettings) (count int64, err error, partial bool) {

	c.SetNumIndexers(len(partition))

	if c.scan != nil {
		err, partial = c.scatterScan(client, index, rollback, partition, numPartition, settings)
		return 0, err, partial
	} else if c.count != nil {
		return c.scatterCount(client, index, rollback, partition, numPartition)
	}

	return 0, fmt.Errorf("Intenral error: Fail to process request for index %v:%v.  Unknown request handler.", index.Bucket, index.Name), false
}

//
// Scatter scan requests over multiple connections
//
func (c *RequestBroker) scatterScan(client []*GsiScanClient, index *common.IndexDefn, rollback []int64, partition [][]common.PartitionId,
	numPartition uint32, settings *ClientSettings) (err error, partial bool) {

	c.notifych = make(chan bool, 1)
	c.killch = make(chan bool, 1)
	donech_gather := make(chan bool, 1)

	if len(partition) > 1 {
		c.queues = make([]*Queue, len(client))
		size, limit := queueSize(int(c.size), len(client), c.sorted, settings)
		for i := 0; i < len(client); i++ {
			c.queues[i] = NewQueue(int64(size), int64(limit), c.notifych)
		}

		if c.sorted {
			go c.gather(donech_gather)
		} else {
			go c.forward(donech_gather)
		}
	}

	donech_scatter := make([]chan *doneStatus, len(client))
	for i, _ := range client {
		donech_scatter[i] = make(chan *doneStatus, 1)
		go c.scanSingleNode(ResponseHandlerId(i), client[i], index, rollback[i], partition[i], numPartition, donech_scatter[i])
	}

	for i, _ := range client {
		status := <-donech_scatter[i]
		partial = partial && status.partial
		if status.err != nil {
			err = errors.New(fmt.Sprintf("%v %v", err, status.err))
		}
	}

	if len(partition) > 1 && err == nil {
		<-donech_gather
	}

	return
}

//
// Scatter count requests over multiple connections
//
func (c *RequestBroker) scatterCount(client []*GsiScanClient, index *common.IndexDefn, rollback []int64,
	partition [][]common.PartitionId, numPartition uint32) (count int64, err error, partial bool) {

	donech := make([]chan *doneStatus, len(client))
	for i, _ := range client {
		donech[i] = make(chan *doneStatus, 1)
		go c.countSingleNode(ResponseHandlerId(i), client[i], index, rollback[i], partition[i], numPartition, donech[i], &count)
	}

	for i, _ := range client {
		status := <-donech[i]
		partial = partial && status.partial
		if status.err != nil {
			err = errors.New(fmt.Sprintf("%v %v", err, status.err))
		}
	}

	return
}

//
// Gather results from multiple connections
//
func (c *RequestBroker) gather(donech chan bool) {

	size := len(c.queues)
	rows := make([]Row, size)

	defer close(donech)

	for {
		var candidate *Row
		var id int
		var count int

		if c.isClose() {
			return
		}

		for i := 0; i < size; i++ {
			if c.queues[i].Peek(&rows[i]) {
				count++

				if rows[i].last {
					continue
				}

				if candidate == nil || c.compareKey(candidate.value, rows[i].value) > 0 {
					candidate = &rows[i]
					id = i
				}
			}
		}

		if count == size {
			if candidate != nil {
				if c.queues[id].Dequeue(&rows[id]) {
					if !c.sender(rows[id].pkey, rows[id].value, rows[id].skey) {
						return
					}
				}
			} else {
				break
			}
		} else {
			select {
			case <-c.notifych:
				continue
			case <-c.killch:
				return
			}
		}
	}
}

//
// Gather results from multiple connections
//
func (c *RequestBroker) forward(donech chan bool) {

	size := len(c.queues)
	rows := make([]Row, size)

	defer close(donech)

	for {
		if c.isClose() {
			return
		}

		count := 0
		found := false
		for i := 0; i < size; i++ {
			if c.queues[i].Peek(&rows[i]) {

				if rows[i].last {
					count++
					continue
				}

				found = true

				if c.queues[i].Dequeue(&rows[i]) {
					if !c.sender(rows[i].pkey, rows[i].value, rows[i].skey) {
						return
					}
				}
			}
		}

		if count == size {
			return
		}

		if !found {
			select {
			case <-c.notifych:
				continue
			case <-c.killch:
				return
			}
		}
	}
}

// This function compares two set of secondart key values.
// Returns –int, 0 or +int depending on if the receiver this
// sorts less than, equal to, or greater than the input
// argument Value to the method.
func (c *RequestBroker) compareKey(key1, key2 []value.Value) int {

	ln := len(key1)
	if len(key2) < ln {
		ln = len(key2)
	}

	for i := 0; i < ln; i++ {
		if r := key1[i].Collate(key2[i]); r != 0 {
			return r
		}
	}

	return len(key1) - len(key2)
}

//
// This function makes a scan request through a single connection.
//
func (c *RequestBroker) scanSingleNode(id ResponseHandlerId, client *GsiScanClient, index *common.IndexDefn, rollback int64,
	partition []common.PartitionId, numPartition uint32, donech chan *doneStatus) {

	err, partial := c.scan(client, index, rollback, partition, numPartition, c.factory(id))
	if err == SkipPartitionError {
		if len(c.queues) > 0 {
			var r Row
			r.last = true
			c.queues[int(id)].Enqueue(&r)
		}
		err = nil
	}

	donech <- &doneStatus{err: err, partial: partial}
}

//
// This function makes a count request through a single connection.
//
func (c *RequestBroker) countSingleNode(id ResponseHandlerId, client *GsiScanClient, index *common.IndexDefn, rollback int64,
	partition []common.PartitionId, numPartition uint32, donech chan *doneStatus, count *int64) {

	cnt, err, partial := c.count(client, index, rollback, partition, numPartition)
	atomic.AddInt64(count, cnt)
	donech <- &doneStatus{err: err, partial: partial}
}

//
// When a response is received from a connection, the response will first be passed to the caller so the caller
// has a chance to handle the rows first (e.g. backfill).    The caller will then forward the rows back to the
// broker for additional processing (e.g. gather).
//
func (c *RequestBroker) SendEntries(id ResponseHandlerId, pkeys [][]byte, skeys []common.SecondaryKey) bool {

	if len(pkeys) == 0 && len(skeys) == 0 {
		if len(c.queues) > 0 {
			var r Row
			r.last = true
			c.queues[int(id)].Enqueue(&r)
		}
		return false
	}

	for i, skey := range skeys {

		vals := make([]value.Value, len(skey))
		for j := 0; j < len(skey); j++ {
			if s, ok := skey[j].(string); ok && collatejson.MissingLiteral.Equal(s) {
				vals[j] = value.NewMissingValue()
			} else {
				vals[j] = value.NewValue(skey[j])
			}
		}

		if len(c.queues) > 0 {
			var r Row
			r.pkey = pkeys[i]
			r.value = vals
			r.skey = skey
			c.queues[int(id)].Enqueue(&r)
		} else {
			if !c.sender(pkeys[i], vals, skey) || c.isClose() {
				return false
			}
		}
	}

	return !c.isClose()
}

func (c *RequestBroker) Len(id ResponseHandlerId) int64 {

	if len(c.queues) > 0 {
		return c.queues[int(id)].Len()
	}
	return -1
}

func (c *RequestBroker) Cap(id ResponseHandlerId) int64 {

	if len(c.queues) > 0 {
		return c.queues[int(id)].Cap()
	}
	return -1
}

func (c *RequestBroker) ReceiveCount() int64 {
	return atomic.LoadInt64(&c.receiveCount)
}

func (c *RequestBroker) SendCount() int64 {
	return atomic.LoadInt64(&c.sendCount)
}

//--------------------------
// default request broker
//--------------------------

type bypassResponseReader struct {
	pkey []byte
	skey common.SecondaryKey
}

func (d *bypassResponseReader) GetEntries() ([]common.SecondaryKey, [][]byte, error) {
	return []common.SecondaryKey{d.skey}, [][]byte{d.pkey}, nil
}

func (d *bypassResponseReader) Error() error {
	return nil
}

func makeDefaultRequestBroker(cb ResponseHandler) *RequestBroker {

	broker := NewRequestBroker("", 4096)

	factory := func(id ResponseHandlerId) ResponseHandler {
		return makeDefaultResponseHandler(id, broker)
	}

	sender := func(pkey []byte, mskey []value.Value, uskey common.SecondaryKey) bool {
		if cb != nil {
			var reader bypassResponseReader
			reader.pkey = pkey
			reader.skey = uskey
			return cb(&reader)
		}
		broker.IncrementSendCount()
		return true
	}

	broker.SetResponseHandlerFactory(factory)
	broker.SetResponseSender(sender)

	return broker
}

func makeDefaultResponseHandler(id ResponseHandlerId, broker *RequestBroker) ResponseHandler {

	handler := func(resp ResponseReader) bool {
		err := resp.Error()
		if err != nil {
			logging.Errorf("defaultResponseHandler: %v", err)
			broker.Close()
			return false
		}
		skeys, pkeys, err := resp.GetEntries()
		if err != nil {
			logging.Errorf("defaultResponseHandler: %v", err)
			broker.Close()
			return false
		}
		if len(pkeys) != 0 || len(skeys) != 0 {
			if len(pkeys) != 0 {
				broker.IncrementReceiveCount(len(pkeys))
			} else {
				broker.IncrementReceiveCount(len(skeys))
			}
		}
		return broker.SendEntries(id, pkeys, skeys)
	}

	return handler
}

func queueSize(size int, partition int, sorted bool, settings *ClientSettings) (int, int) {

	queueSize := int(settings.ScanQueueSize())
	limit := int(settings.ScanNotifyCount())

	if queueSize == 0 {
		queueSize = size
	}

	numCpu := runtime.NumCPU()

	if numCpu >= partition || !sorted {
		return queueSize, limit
	}

	ratio := partition / numCpu
	return ratio * queueSize, limit
}
