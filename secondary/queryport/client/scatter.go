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
	"bytes"
	"errors"
	"fmt"
	"github.com/couchbase/indexing/secondary/collatejson"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/query/value"
	"math"
	"reflect"
	"strings"
	//"runtime"
	"encoding/json"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/parser"
	qvalue "github.com/couchbase/query/value"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

//--------------------------
// constant
//--------------------------

var (
	NotMyPartition = "Not my partition"
)

//--------------------------
// request broker
//--------------------------

type RequestBroker struct {
	// callback
	scan    ScanRequestHandler
	count   CountRequestHandler
	factory ResponseHandlerFactory
	sender  ResponseSender
	timer   ResponseTimer

	// initialization
	requestId string
	size      int64

	// scatter/gather
	queues   []*Queue
	notifych chan bool
	closed   int32
	killch   chan bool
	bGather  bool
	errMap   map[common.PartitionId]map[uint64]error
	partial  int32
	mutex    sync.Mutex

	// scan
	defn           *common.IndexDefn
	limit          int64
	offset         int64
	sorted         bool
	pushdownLimit  int64
	pushdownOffset int64
	pushdownSorted bool
	scans          Scans
	grpAggr        *GroupAggr
	projections    *IndexProjection
	indexOrder     *IndexKeyOrder
	projDesc       []bool
	distinct       bool

	// stats
	sendCount    int64
	receiveCount int64
	numIndexers  int64
}

type doneStatus struct {
	err     error
	partial bool
}

const (
	NoPick = -1
	Done   = -2
)

const (
	MetaIdPos = -1
)

//
// New Request Broker
//
func NewRequestBroker(requestId string, size int64) *RequestBroker {

	return &RequestBroker{
		requestId:      requestId,
		size:           size,
		sorted:         true,
		pushdownSorted: true,
		limit:          math.MaxInt64,
		pushdownLimit:  math.MaxInt64,
		errMap:         make(map[common.PartitionId]map[uint64]error),
	}
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
// Set ResponseTimer
//
func (b *RequestBroker) SetResponseTimer(timer ResponseTimer) {

	b.timer = timer
}

//
// Set Limit
//
func (b *RequestBroker) SetLimit(limit int64) {

	b.limit = limit
	b.pushdownLimit = limit
}

//
// Set offset
//
func (b *RequestBroker) SetOffset(offset int64) {

	b.offset = offset
	b.pushdownOffset = offset
}

//
// Set Distinct
//
func (b *RequestBroker) SetDistinct(distinct bool) {

	b.distinct = distinct
}

//
// Set sorted
//
func (b *RequestBroker) SetSorted(sorted bool) {

	b.sorted = sorted
	b.pushdownSorted = sorted
}

//
// Get Limit
//
func (b *RequestBroker) GetLimit() int64 {

	return b.pushdownLimit
}

//
// Get offset
//
func (b *RequestBroker) GetOffset() int64 {

	return b.pushdownOffset
}

//
// Get Sort
//
func (b *RequestBroker) GetSorted() bool {

	return b.pushdownSorted
}

//
// Set Scans
//
func (b *RequestBroker) SetScans(scans Scans) {

	b.scans = scans
}

//
// Set GroupAggr
//
func (b *RequestBroker) SetGroupAggr(grpAggr *GroupAggr) {

	b.grpAggr = grpAggr
}

//
// Set Projection
//
func (b *RequestBroker) SetProjection(projection *IndexProjection) {

	b.projections = projection
}

//
// Set Index Order
//
func (b *RequestBroker) SetIndexOrder(indexOrder *IndexKeyOrder) {

	b.indexOrder = indexOrder
}

//
// Close the broker on error
//
func (b *RequestBroker) Error(err error, instId uint64, partitions []common.PartitionId) {

	b.mutex.Lock()
	defer b.mutex.Unlock()

	skip := partitions
	if strings.HasPrefix(err.Error(), NotMyPartition) {
		if offset := strings.Index(err.Error(), ":"); offset != -1 {
			content := err.Error()[offset+1:]
			missing := make(map[common.IndexInstId][]common.PartitionId)
			if err1 := json.Unmarshal([]byte(content), &missing); err1 == nil {
				if _, ok := missing[common.IndexInstId(instId)]; ok {
					skip = missing[common.IndexInstId(instId)]
					logging.Warnf("scan err : NotMyPartition instId %v partition %v", instId, skip)
				}
			} else {
				logging.Errorf("fail to unmarshall NotMyPartition error.  Err:%v", err)
			}
		}
	}

	for _, partition := range skip {
		if _, ok := b.errMap[partition]; !ok {
			b.errMap[partition] = make(map[uint64]error)
		}
		logging.Debugf("request broker: scan error instId %v partition %v error %v", instId, partition, err)
		b.errMap[partition][instId] = err
	}

	b.close()
}

func (b *RequestBroker) Partial(partial bool) {
	partial = b.IsPartial() || partial
	if partial {
		atomic.StoreInt32(&b.partial, 1)
	} else {
		atomic.StoreInt32(&b.partial, 0)
	}
}

//
// Close the broker when scan is done
//
func (b *RequestBroker) done() {
	b.close()
}

//
// Close the request broker
//
func (b *RequestBroker) close() {

	if atomic.SwapInt32(&b.closed, 1) == 0 {
		close(b.killch)

		for _, queue := range b.queues {
			queue.Close()
		}
	}
}

//
// Is the request broker closed
//
func (b *RequestBroker) isClose() bool {

	return atomic.LoadInt32(&b.closed) == 1
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

func (c *RequestBroker) Len(id ResponseHandlerId) int64 {

	if c.useGather() {
		return c.queues[int(id)].Len()
	}
	return -1
}

func (c *RequestBroker) Cap(id ResponseHandlerId) int64 {

	if c.useGather() {
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

func (c *RequestBroker) GetError() map[common.PartitionId]map[uint64]error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if len(c.errMap) == 0 {
		return nil
	}

	result := make(map[common.PartitionId]map[uint64]error)
	for partnId, instErrMap := range c.errMap {
		result[partnId] = make(map[uint64]error)
		for instId, err := range instErrMap {
			result[partnId][instId] = err
		}
	}

	return result
}

func (c *RequestBroker) IsPartial() bool {
	if atomic.LoadInt32(&c.partial) == 1 {
		return true
	}
	return false
}

func (c *RequestBroker) useGather() bool {

	//return len(c.queues) > 0
	return c.bGather
}

func (b *RequestBroker) reset() {

	// scatter/gather
	b.queues = nil
	b.notifych = nil
	b.closed = 0
	b.killch = nil
	b.bGather = false
	b.errMap = make(map[common.PartitionId]map[uint64]error)
	b.partial = 0 // false

	// stats
	b.sendCount = 0
	b.receiveCount = 0
	b.numIndexers = 0

	// scans
	b.defn = nil
	b.pushdownLimit = b.limit
	b.pushdownOffset = b.offset
	b.pushdownSorted = b.sorted
	b.projDesc = nil
}

//--------------------------
// scatter/gather
//--------------------------

//
// Scatter requests over multiple connections
//
func (c *RequestBroker) scatter(client []*GsiScanClient, index *common.IndexDefn, targetInstId []uint64, rollback []int64,
	partition [][]common.PartitionId, numPartition uint32, settings *ClientSettings) (count int64, err map[common.PartitionId]map[uint64]error, partial bool) {

	defer func() {
		logging.Debugf("scatter: requestId %v items recieved %v items processed %v", c.requestId, c.ReceiveCount(), c.SendCount())
	}()

	c.reset()
	c.SetNumIndexers(len(partition))
	c.defn = index

	partition = c.filterPartitions(index, partition, numPartition)
	client, rollback, partition = filterClients(client, rollback, partition)
	c.analyzeGroupBy(partition, numPartition, index)
	c.analyzeOrderBy(partition, numPartition, index)
	c.analyzeProjection(partition, numPartition, index)
	c.changePushdownParams(partition, numPartition, index)

	if len(partition) == len(client) {
		for i, partitions := range partition {
			logging.Debugf("scatter: requestId %v queryport %v partition %v", c.requestId, client[i].queryport, partitions)
		}
	}

	logging.Debugf("scatter: requestId %v limit %v offset %v sorted %v pushdown limit %v pushdown offset %v pushdown sorted %v",
		c.requestId, c.limit, c.offset, c.sorted, c.pushdownLimit, c.pushdownOffset, c.pushdownSorted)

	if c.projections != nil {
		logging.Debugf("scatter: requestId %v projection %v Desc %v", c.requestId, c.projections.EntryKeys, c.projDesc)
	}

	if c.scan != nil {
		err, partial = c.scatterScan2(client, index, targetInstId, rollback, partition, numPartition, settings)
		return 0, err, partial
	} else if c.count != nil {
		return c.scatterCount(client, index, targetInstId, rollback, partition, numPartition)
	}

	e := fmt.Errorf("Intenral error: Fail to process request for index %v:%v.  Unknown request handler.", index.Bucket, index.Name)
	return 0, c.makeErrorMap(targetInstId, partition, e), false
}

func (c *RequestBroker) makeErrorMap(targetInstIds []uint64, partitions [][]common.PartitionId, err error) map[common.PartitionId]map[uint64]error {

	errMap := make(map[common.PartitionId]map[uint64]error)

	for i, partnList := range partitions {
		for _, partition := range partnList {

			if _, ok := errMap[partition]; !ok {
				errMap[partition] = make(map[uint64]error)
			}
			errMap[partition][targetInstIds[i]] = err
		}
	}

	return errMap
}

//
// Scatter scan requests over multiple connections
//
func (c *RequestBroker) scatterScan(client []*GsiScanClient, index *common.IndexDefn, targetInstId []uint64, rollback []int64, partition [][]common.PartitionId,
	numPartition uint32, settings *ClientSettings) (err error, partial bool) {

	c.notifych = make(chan bool, 1)
	c.killch = make(chan bool, 1)
	donech_gather := make(chan bool, 1)

	if len(partition) > 1 {
		c.bGather = true
	}

	if c.useGather() {
		c.queues = make([]*Queue, len(client))
		size := queueSize(int(c.size), len(client), c.sorted, settings)
		for i := 0; i < len(client); i++ {
			c.queues[i] = NewQueue(int64(size), c.notifych)
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
		go c.scanSingleNode(ResponseHandlerId(i), client[i], index, targetInstId[i], rollback[i], partition[i], numPartition, donech_scatter[i])
	}

	for i, _ := range client {
		status := <-donech_scatter[i]
		partial = partial || status.partial
		if status.err != nil {
			err = errors.New(fmt.Sprintf("%v %v", err, status.err))
		}
	}

	if c.useGather() && err == nil {
		<-donech_gather
	}

	return
}

func (c *RequestBroker) scatterScan2(client []*GsiScanClient, index *common.IndexDefn, targetInstId []uint64, rollback []int64,
	partition [][]common.PartitionId, numPartition uint32, settings *ClientSettings) (errMap map[common.PartitionId]map[uint64]error, partial bool) {

	c.notifych = make(chan bool, 1)
	c.killch = make(chan bool, 1)
	donech_gather := make(chan bool, 1)

	if len(partition) > 1 {
		c.bGather = true
	}

	if c.useGather() {
		c.queues = make([]*Queue, len(client))
		size := queueSize(int(c.size), len(client), c.sorted, settings)
		for i := 0; i < len(client); i++ {
			c.queues[i] = NewQueue(int64(size), c.notifych)
		}
	}

	donech_scatter := make([]chan *doneStatus, len(client))
	for i, _ := range client {
		donech_scatter[i] = make(chan *doneStatus, 1)
		go c.scanSingleNode(ResponseHandlerId(i), client[i], index, targetInstId[i], rollback[i], partition[i], numPartition, donech_scatter[i])
	}

	if c.useGather() {
		// Gather is done either
		// 1) scan is finished (done() method is called)
		// 2) there is an error (Error() method is called)
		// The gather routine could exit before all scatter routines have exited
		if c.sorted {
			c.gather(donech_gather)
		} else {
			c.forward(donech_gather)
		}

		errMap = c.GetError()
	}

	// Wait for all scatter routine is done
	// If we are using gatherer, then do not wait unless there is an error.
	if !c.useGather() || len(errMap) != 0 {
		for i, _ := range client {
			<-donech_scatter[i]
		}
	}

	errMap = c.GetError()
	partial = c.IsPartial()

	return
}

//
// Scatter count requests over multiple connections
//
func (c *RequestBroker) scatterCount(client []*GsiScanClient, index *common.IndexDefn, targetInstId []uint64, rollback []int64,
	partition [][]common.PartitionId, numPartition uint32) (count int64, err map[common.PartitionId]map[uint64]error, partial bool) {

	donech := make([]chan *doneStatus, len(client))
	for i, _ := range client {
		donech[i] = make(chan *doneStatus, 1)
		go c.countSingleNode(ResponseHandlerId(i), client[i], index, targetInstId[i], rollback[i], partition[i], numPartition, donech[i], &count)
	}

	for i, _ := range client {
		status := <-donech[i]
		partial = partial || status.partial
	}

	err = c.GetError()
	return
}

func (c *RequestBroker) sort(rows []Row, sorted []int) bool {

	size := len(c.queues)

	for i := 0; i < size; i++ {
		sorted[i] = i
	}

	for i := 0; i < size-1; i++ {
		if !c.queues[sorted[i]].Peek(&rows[sorted[i]]) {
			return false
		}

		for j := i + 1; j < size; j++ {
			if !c.queues[sorted[j]].Peek(&rows[sorted[j]]) {
				return false
			}

			if !c.defn.IsPrimary {

				if rows[sorted[i]].last && !rows[sorted[j]].last ||
					(!rows[sorted[i]].last && !rows[sorted[j]].last &&
						c.compareKey(rows[sorted[i]].value, rows[sorted[j]].value) > 0) {
					tmp := sorted[i]
					sorted[i] = sorted[j]
					sorted[j] = tmp
				}

			} else {

				if rows[sorted[i]].last && !rows[sorted[j]].last ||
					(!rows[sorted[i]].last && !rows[sorted[j]].last &&
						c.comparePrimaryKey(rows[sorted[i]].pkey, rows[sorted[j]].pkey) > 0) {
					tmp := sorted[i]
					sorted[i] = sorted[j]
					sorted[j] = tmp
				}
			}
		}
	}

	return true
}

//
// Gather results from multiple connections
// rows - buffer of rows from each scatter gorountine
// sorted - sorted order of the rows
//
func (c *RequestBroker) pick(rows []Row, sorted []int) int {

	size := len(c.queues)

	if !c.queues[sorted[0]].Peek(&rows[sorted[0]]) {
		return NoPick
	}

	pos := 0
	for i := 1; i < size; i++ {
		if !c.queues[sorted[i]].Peek(&rows[sorted[i]]) {
			return NoPick
		}

		if !c.defn.IsPrimary {

			// last value always sorted last
			if rows[sorted[pos]].last && !rows[sorted[i]].last ||
				(!rows[sorted[pos]].last && !rows[sorted[i]].last &&
					c.compareKey(rows[sorted[pos]].value, rows[sorted[i]].value) > 0) {

				tmp := sorted[pos]
				sorted[pos] = sorted[i]
				sorted[i] = tmp
				pos = i

			} else {
				break
			}

		} else {

			// last value always sorted last
			if rows[sorted[pos]].last && !rows[sorted[i]].last ||
				(!rows[sorted[pos]].last && !rows[sorted[i]].last &&
					c.comparePrimaryKey(rows[sorted[pos]].pkey, rows[sorted[i]].pkey) > 0) {

				tmp := sorted[pos]
				sorted[pos] = sorted[i]
				sorted[i] = tmp
				pos = i

			} else {
				break
			}
		}
	}

	for i := 0; i < size; i++ {
		if !rows[sorted[i]].last {
			return sorted[0]
		}
	}

	return Done
}

//
// Gather results from multiple connections with sorting.
// The gather routine must follow CBQ processing order:
// 1) filter (where)
// 2) group-by
// 3) aggregate
// 4) projection
// 5) order-by
// 6) offset
// 7) limit
// The last 3 steps are performed by the gathering.
//
// This routine is called only if cbq requires sorting:
// 1) It is not an aggregate query
// 2) Order-by is provided
//
// If sorting is required, then the sort key will be in the projection
// list, since cbq will put all keys referenced in query into the
// projection list for non-aggregate query.    For aggregate query,
// only aggregate and group keys will be in projection.
//
func (c *RequestBroker) gather(donech chan bool) {

	size := len(c.queues)
	rows := make([]Row, size)
	sorted := make([]int, size)

	defer close(donech)

	// initial sort
	isSorted := false
	for !isSorted {
		if c.isClose() {
			return
		}

		isSorted = c.sort(rows, sorted)

		if !isSorted {
			select {
			case <-c.notifych:
				continue
			case <-c.killch:
				return
			}
		}
	}

	var curOffset int64 = 0
	var curLimit int64 = 0

	for {
		var id int

		if c.isClose() {
			return
		}

		id = c.pick(rows, sorted)
		if id == Done {
			return
		}

		if id == NoPick {
			select {
			case <-c.notifych:
				continue
			case <-c.killch:
				return
			}
		}

		if c.queues[id].Dequeue(&rows[id]) {

			// skip offset
			if curOffset < c.offset {
				curOffset++
				continue
			}

			curLimit++
			c.Partial(true)
			if !c.sender(rows[id].pkey, rows[id].value, rows[id].skey) {
				c.done()
				return
			}

			// reaching limit
			if curLimit >= c.limit {
				c.done()
				return
			}
		}
	}
}

//
// Gather results from multiple connections without sorting
//
func (c *RequestBroker) forward(donech chan bool) {

	size := len(c.queues)
	rows := make([]Row, size)
	sorted := make([]int, size)

	defer close(donech)

	// initial sort (only the first row from each indexer)
	// This is just to make sure that we have recieve at
	// least one row before streaming response back.
	isSorted := false
	for !isSorted {
		if c.isClose() {
			return
		}

		isSorted = c.sort(rows, sorted)

		if !isSorted {
			select {
			case <-c.notifych:
				continue
			case <-c.killch:
				return
			}
		}
	}

	var curOffset int64 = 0
	var curLimit int64 = 0

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

					// skip offset
					if curOffset < c.offset {
						curOffset++
						continue
					}

					curLimit++
					c.Partial(true)
					if !c.sender(rows[i].pkey, rows[i].value, rows[i].skey) {
						c.done()
						return
					}

					// reaching limit
					if curLimit >= c.limit {
						c.done()
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
// Returns –int, 0 or +int depending on if key1
// sorts less than, equal to, or greater than key2.
//
// Each sec key is a array of Value, ordered based on
// how they are defined in the index.  In addition, if
// the key is in order-by, cbq will put it in the
// projection list (even if the user does not ask for it).
//
func (c *RequestBroker) compareKey(key1, key2 []value.Value) int {

	ln := len(key1)
	if len(key2) < ln {
		ln = len(key2)
	}

	for i := 0; i < ln; i++ {

		if r := key1[i].Collate(key2[i]); r != 0 {

			// default: ascending
			if i >= len(c.projDesc) {
				return r
			}

			// asecending
			if !c.projDesc[i] {
				return r
			}

			// descending
			return 0 - r
		}
	}

	return len(key1) - len(key2)
}

// This function compares the primary key.
// Returns –int, 0 or +int depending on if key1
// sorts less than, equal to, or greater than key2.
func (c *RequestBroker) comparePrimaryKey(k1 []byte, k2 []byte) int {

	return bytes.Compare(k1, k2)
}

//
// This function makes a scan request through a single connection.
//
func (c *RequestBroker) scanSingleNode(id ResponseHandlerId, client *GsiScanClient, index *common.IndexDefn, instId uint64,
	rollback int64, partition []common.PartitionId, numPartition uint32, donech chan *doneStatus) {

	if len(partition) == 0 {
		if c.useGather() {
			var r Row
			r.last = true
			c.queues[int(id)].Enqueue(&r)
		}
		donech <- &doneStatus{err: nil, partial: false}
		return
	}

	begin := time.Now()
	err, partial := c.scan(client, index, rollback, partition, c.factory(id, instId, partition))
	if err != nil {
		// If there is any error, then stop the broker.
		// This will force other go-routine to terminate.

		c.Partial(partial)
		c.Error(err, instId, partition)

	} else {
		// update timer if there is no error
		elapsed := float64(time.Since(begin))
		if c.timer != nil {
			for _, partitionId := range partition {
				c.timer(instId, partitionId, elapsed)
			}
		}
	}

	donech <- &doneStatus{err: err, partial: partial}
}

//
// This function makes a count request through a single connection.
//
func (c *RequestBroker) countSingleNode(id ResponseHandlerId, client *GsiScanClient, index *common.IndexDefn, instId uint64, rollback int64,
	partition []common.PartitionId, numPartition uint32, donech chan *doneStatus, count *int64) {

	if len(partition) == 0 {
		donech <- &doneStatus{err: nil, partial: false}
		return
	}

	cnt, err, partial := c.count(client, index, rollback, partition)
	if err != nil {
		// If there is any error, then stop the broker.
		// This will force other go-routine to terminate.
		c.Partial(partial)
		c.Error(err, instId, partition)
	}

	if err == nil && !partial {
		atomic.AddInt64(count, cnt)
	}

	donech <- &doneStatus{err: err, partial: partial}
}

//
// When a response is received from a connection, the response will first be passed to the caller so the caller
// has a chance to handle the rows first (e.g. backfill).    The caller will then forward the rows back to the
// broker for additional processing (e.g. gather).
//
func (c *RequestBroker) SendEntries(id ResponseHandlerId, pkeys [][]byte, skeys []common.SecondaryKey) bool {

	// StreamEndResponse has nil pkeys and nil skeys
	if len(pkeys) == 0 && len(skeys) == 0 {
		if c.useGather() {
			var r Row
			r.last = true
			c.queues[int(id)].Enqueue(&r)
		}
		return false
	}

	if c.isClose() {
		return false
	}

	for i, skey := range skeys {

		if c.useGather() {
			var vals []value.Value
			if c.sorted {
				vals = make([]value.Value, len(skey))
				for j := 0; j < len(skey); j++ {
					if s, ok := skey[j].(string); ok && collatejson.MissingLiteral.Equal(s) {
						vals[j] = value.NewMissingValue()
					} else {
						vals[j] = value.NewValue(skey[j])
					}
				}
			}

			var r Row
			r.pkey = pkeys[i]
			r.value = vals
			r.skey = skey
			c.queues[int(id)].Enqueue(&r)
		} else {

			c.Partial(true)
			if !c.sender(pkeys[i], nil, skey) {
				c.done()
				return false
			}
		}
	}

	if c.useGather() {
		c.queues[int(id)].NotifyEnq()
	}

	return !c.isClose()
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

	broker := NewRequestBroker("", 256)

	factory := func(id ResponseHandlerId, instId uint64, partitions []common.PartitionId) ResponseHandler {
		return makeDefaultResponseHandler(id, broker, instId, partitions)
	}

	sender := func(pkey []byte, mskey []value.Value, uskey common.SecondaryKey) bool {
		broker.IncrementSendCount()
		if cb != nil {
			var reader bypassResponseReader
			reader.pkey = pkey
			reader.skey = uskey
			return cb(&reader)
		}
		return true
	}

	broker.SetResponseHandlerFactory(factory)
	broker.SetResponseSender(sender)

	return broker
}

func makeDefaultResponseHandler(id ResponseHandlerId, broker *RequestBroker, instId uint64, partitions []common.PartitionId) ResponseHandler {

	handler := func(resp ResponseReader) bool {
		err := resp.Error()
		if err != nil {
			logging.Errorf("defaultResponseHandler: %v", err)
			broker.Error(err, instId, partitions)
			return false
		}
		skeys, pkeys, err := resp.GetEntries()
		if err != nil {
			logging.Errorf("defaultResponseHandler: %v", err)
			broker.Error(err, instId, partitions)
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

//--------------------------
// Partition Elimination
//--------------------------

//
// Filter partitions based on index partiton key
//
func (c *RequestBroker) filterPartitions(index *common.IndexDefn, partitions [][]common.PartitionId, numPartition uint32) [][]common.PartitionId {

	if numPartition == 1 || (len(partitions) == 1 && len(partitions[0]) == 1) {
		return partitions
	}

	partitionKeyPos := partitionKeyPos(index)
	if len(partitionKeyPos) == 0 {
		return partitions
	}

	partitionKeyValues := partitionKeyValues(c.requestId, partitionKeyPos, c.scans)
	if len(partitionKeyValues) == 0 {
		return partitions
	}

	filter := partitionKeyHash(partitionKeyValues, c.scans, numPartition)
	if len(filter) == 0 {
		return partitions
	}

	return filterPartitionIds(partitions, filter)
}

//
// Find out the position of parition key in the index key list
//
func partitionKeyPos(defn *common.IndexDefn) []int {

	if defn.PartitionScheme == common.SINGLE {
		return nil
	}

	var pos []int
	secExprs := make(expression.Expressions, 0, len(defn.SecExprs))
	for _, key := range defn.SecExprs {
		expr, err := parser.Parse(key)
		if err != nil {
			logging.Errorf("Fail to parse secondary key", logging.TagUD(key))
			return nil
		}
		secExprs = append(secExprs, expr)
	}

	partnExprs := make(expression.Expressions, 0, len(defn.PartitionKeys))
	for _, key := range defn.PartitionKeys {
		expr, err := parser.Parse(key)
		if err != nil {
			logging.Errorf("Fail to parse partition key", logging.TagUD(key))
			return nil
		}
		partnExprs = append(partnExprs, expr)
	}

	if !defn.IsPrimary {
		for _, partnExpr := range partnExprs {
			for i, secExpr := range secExprs {

				if partnExpr.EquivalentTo(secExpr) {
					pos = append(pos, i)
					break
				}
			}
		}
	} else {
		id := expression.NewField(expression.NewMeta(), expression.NewFieldName("id", false))
		idself := expression.NewField(expression.NewMeta(expression.NewIdentifier("self")), expression.NewFieldName("id", false))

		// for primary index, it can be partitioned on an expression based on metaId.  For scan, n1ql only push down if
		// span is metaId (not expr on metaId).   So if partition key is not metaId, then there is no partition elimination.
		if len(partnExprs) == 1 && (partnExprs[0].EquivalentTo(id) || partnExprs[0].EquivalentTo(idself)) {
			pos = append(pos, MetaIdPos)
		}
	}

	if len(pos) != len(defn.PartitionKeys) {
		return nil
	}

	return pos
}

//
// Extract the partition key value from each scan (AND-predicate from where clause)
// Scans is a OR-list of AND-predicate
// Each scan (AND-predicate) should have all the partition keys
// If any scan does not have all the partition keys, then the request needs to be scatter-gather
//
func partitionKeyValues(requestId string, partnKeyPos []int, scans Scans) [][]interface{} {

	partnKeyValues := make([][]interface{}, len(scans))

	for scanPos, scan := range scans {
		if scan != nil {
			if len(scan.Filter) > 0 {

				for i, filter := range scan.Filter {
					logging.Debugf("scatter: requestId %v filter[%v] low %v high %v", requestId, i, filter.Low, filter.High)
				}

				for _, pos := range partnKeyPos {

					if pos != MetaIdPos && pos >= len(scan.Filter) {
						partnKeyValues[scanPos] = nil
						break
					}

					if pos != MetaIdPos && reflect.DeepEqual(scan.Filter[pos].Low, scan.Filter[pos].High) {
						partnKeyValues[scanPos] = append(partnKeyValues[scanPos], qvalue.NewValue(scan.Filter[pos].Low))

					} else if pos == MetaIdPos && len(scan.Filter) == 1 {
						// n1ql only push down span on primary key for metaId()
						// it will not push down on expr on metaId()
						partnKeyValues[scanPos] = append(partnKeyValues[scanPos], qvalue.NewValue(scan.Filter[0].Low))

					} else {
						partnKeyValues[scanPos] = nil
						break
					}
				}

			} else if len(scan.Seek) > 0 {

				for i, seek := range scan.Seek {
					logging.Debugf("scatter: requestId %v seek[%v] value %v", requestId, i, seek)
				}

				for _, pos := range partnKeyPos {

					if pos != MetaIdPos && pos >= len(scan.Seek) {
						partnKeyValues[scanPos] = nil
						break
					}

					if pos != MetaIdPos {
						partnKeyValues[scanPos] = append(partnKeyValues[scanPos], qvalue.NewValue(scan.Seek[pos]))

					} else if pos == MetaIdPos && len(scan.Filter) == 1 {
						// n1ql only push down span on primary key for metaId()
						// it will not push down on expr on metaId()
						partnKeyValues[scanPos] = append(partnKeyValues[scanPos], qvalue.NewValue(scan.Seek[0]))

					} else {
						partnKeyValues[scanPos] = nil
						break
					}
				}
			}
		}
	}

	return partnKeyValues
}

//
// Generate a list of partitonId from the partition key values of each scan
//
func partitionKeyHash(partnKeyValues [][]interface{}, scans Scans, numPartition uint32) map[common.PartitionId]bool {

	if len(partnKeyValues) != len(scans) {
		return nil
	}

	for i, scan := range scans {
		if scan != nil && len(partnKeyValues[i]) == 0 {
			return nil
		}
	}

	result := make(map[common.PartitionId]bool)
	for _, values := range partnKeyValues {

		v, e := qvalue.NewValue(values).MarshalJSON()
		if e != nil {
			return nil
		}

		partnId := common.HashKeyPartition(v, int(numPartition))
		result[partnId] = true
	}

	return result
}

//
// Given the indexer-partitionId map, filter out the partitionId that are not used in the scans
//
func filterPartitionIds(allPartitions [][]common.PartitionId, filter map[common.PartitionId]bool) [][]common.PartitionId {

	result := make([][]common.PartitionId, len(allPartitions))
	for i, partitions := range allPartitions {
		for _, partition := range partitions {
			if filter[partition] {
				result[i] = append(result[i], partition)
			}
		}
	}

	return result
}

//
// Filter out any Scan client that are not used in scans
//
func filterClients(clients []*GsiScanClient, timestamps []int64, allPartitions [][]common.PartitionId) ([]*GsiScanClient, []int64, [][]common.PartitionId) {

	var newClient []*GsiScanClient
	var newTS []int64
	var newPartition [][]common.PartitionId

	for i, partitions := range allPartitions {
		if len(partitions) != 0 {
			newClient = append(newClient, clients[i])
			newTS = append(newTS, timestamps[i])
			newPartition = append(newPartition, partitions)
		}
	}

	return newClient, newTS, newPartition
}

//--------------------------
// API2 Push Down
//--------------------------

func (c *RequestBroker) changePushdownParams(partitions [][]common.PartitionId, numPartition uint32, index *common.IndexDefn) {
	c.changeLimit(partitions, numPartition, index)
	c.changeOffset(partitions, numPartition, index)
	c.changeSorted(partitions, numPartition, index)
}

func (c *RequestBroker) changeLimit(partitions [][]common.PartitionId, numPartition uint32, index *common.IndexDefn) {

	c.pushdownLimit = c.limit

	// non-partition index
	if index.PartitionScheme == common.SINGLE {
		return
	}

	// there is no limit
	if c.limit == math.MaxInt64 {
		return
	}

	// there is only a single indexer involved in the scan
	if numPartition == 1 || len(partitions) == 1 {
		return
	}

	// We have multiple partitions and there is a limit clause.  We need to change the limit only if
	// there is also offset. If there is no offset, then limit does not have to change.
	if c.offset == 0 {
		return
	}

	/*
		// We do not have to change the limit there is exactly one partition for each scan (AND-predicate).
		partitionKeyPos := partitionKeyPos(index)
		if len(partitionKeyPos) != 0 {
			pushDownLimitAsIs := true
			partitionKeyValues := partitionKeyValues(c.requestId, partitionKeyPos, c.scans)
			if len(c.scans) == len(partitionKeyValues) {
				for i, scan := range c.scans {
					if scan != nil && len(partitionKeyValues[i]) == 0 {
						pushDownLimitAsIs = false
						break
					}
				}

				// At this point, each scan is going to be mapped to one partition.
				// 1) the qualifying rows for each scan will only be coming a single indexer
				// 2) for each scan, the qualifying rows will be sorted
				// In this case, it is possible to push down the limit to each indexer,
				// without worrying about missing qualitying rows.
				if pushDownLimitAsIs {
					return
				}
			}
		}
	*/

	// if there are multiple partitions AND there is offset, limit is offset + limit.
	// offset will be set back to 0 in changeOffset()
	c.pushdownLimit = c.offset + c.limit

}

func (c *RequestBroker) changeOffset(partitions [][]common.PartitionId, numPartition uint32, index *common.IndexDefn) {

	c.pushdownOffset = c.offset

	// non-partition index
	if index.PartitionScheme == common.SINGLE {
		return
	}

	// there is no offset
	if c.offset == 0 {
		return
	}

	// there is only a single indexer involved in the scan
	if numPartition == 1 || len(partitions) == 1 {
		return
	}

	/*
		// We do not have to change the offset there is exactly one partition for each scan (AND-predicate).
		partitionKeyPos := partitionKeyPos(index)
		if len(partitionKeyPos) != 0 {
			pushDownOffsetAsIs := true
			partitionKeyValues := partitionKeyValues(c.requestId, partitionKeyPos, c.scans)
			if len(c.scans) == len(partitionKeyValues) {
				for i, scan := range c.scans {
					if scan != nil && len(partitionKeyValues[i]) == 0 {
						pushDownOffsetAsIs = false
						break
					}
				}

				// At this point, each scan is going to be mapped to one partition.
				// 1) the qualifying rows for each scan will only be coming a single indexer
				// 2) for each scan, the qualifying rows will be sorted
				// In this case, it is possible to push down the offset to each indexer,
				// without worrying about missing qualitying rows.
				if pushDownOffsetAsIs {
					return
				}
			}
		}
	*/

	// if there is multiple partition, change offset to 0
	c.pushdownOffset = 0
}

func (c *RequestBroker) changeSorted(partitions [][]common.PartitionId, numPartition uint32, index *common.IndexDefn) {

	c.pushdownSorted = c.sorted

	if c.distinct {
		c.pushdownSorted = true
	}

	// If it is an aggregate query, need to sort in the indexer
	if c.grpAggr != nil && (len(c.grpAggr.Group) != 0 || len(c.grpAggr.Aggrs) != 0) {
		c.pushdownSorted = true
	}
}

//--------------------------
// API3 push down
//--------------------------

//
// For aggregate query, n1ql will expect full aggregate results for partitioned index if group-by keys matches the partition keys.
//
func (c *RequestBroker) analyzeGroupBy(partitions [][]common.PartitionId, numPartition uint32, index *common.IndexDefn) {

	// non-partition index
	if index.PartitionScheme == common.SINGLE {
		return
	}

	// there is only a single indexer involved in the scan
	if numPartition == 1 || len(partitions) == 1 {
		return
	}

	// aggreate query
	if c.grpAggr != nil && len(c.grpAggr.Group) != 0 {

		// check if partition keys are leading index keys
		positions := partitionKeyPos(index)
		for i, pos := range positions {
			if i != pos {
				// if partition keys do not follow index key order
				c.sorted = false
				return
			}
		}

		// check if group keys are leading index keys
		for i, group := range c.grpAggr.Group {
			if int32(i) != group.KeyPos {
				// if group keys do not follow index key order
				c.sorted = false
				return
			}
		}

		// both paritition keys and group keys are in index order.
		// check if group key and partition key are the same length
		if len(index.PartitionKeys) != len(c.grpAggr.Group) {
			c.sorted = false
			return
		}
	}
}

//
// If sorted, then analyze the index order to see if we need to add index to projection list.
//
func (c *RequestBroker) analyzeOrderBy(partitions [][]common.PartitionId, numPartition uint32, index *common.IndexDefn) {

	// non-partition index
	if index.PartitionScheme == common.SINGLE {
		return
	}

	// there is only a single indexer involved in the scan
	if numPartition == 1 || len(partitions) == 1 {
		return
	}

	// no need to sort
	if !c.sorted {
		return
	}

	// skip aggregate query (order-by is after aggregate)
	if c.grpAggr != nil && (len(c.grpAggr.Group) != 0 || len(c.grpAggr.Aggrs) != 0) {
		return
	}

	if c.indexOrder != nil {

		projection := make(map[int64]bool)
		if c.projections != nil && len(c.projections.EntryKeys) != 0 {
			for _, position := range c.projections.EntryKeys {
				projection[position] = true
			}
		}

		// If the order-by key is not in the projection, then add it.
		for _, order := range c.indexOrder.KeyPos {
			if !projection[int64(order)] {
				c.projections.EntryKeys = append(c.projections.EntryKeys, int64(order))
				projection[int64(order)] = true
			}
		}
	}
}

//
// If sorted, then analyze the projection to find out the sort order (asc/desc).
//
func (c *RequestBroker) analyzeProjection(partitions [][]common.PartitionId, numPartition uint32, index *common.IndexDefn) {

	// non-partition index
	if index.PartitionScheme == common.SINGLE {
		return
	}

	// there is only a single indexer involved in the scan
	if numPartition == 1 || len(partitions) == 1 {
		return
	}

	// no need to sort
	if !c.sorted {
		return
	}

	if c.projections != nil && len(c.projections.EntryKeys) != 0 {

		pos := make([]int, len(c.projections.EntryKeys))
		for i, position := range c.projections.EntryKeys {
			pos[i] = int(position)
		}
		sort.Ints(pos)

		c.projDesc = make([]bool, len(c.projections.EntryKeys))
		for i, position := range pos {
			if position >= 0 && position < len(index.Desc) {
				c.projDesc[i] = index.Desc[position]
			}
		}
	}
}

//--------------------------
// utilities
//--------------------------

func queueSize(size int, partition int, sorted bool, settings *ClientSettings) int {

	queueSize := int(settings.ScanQueueSize())

	if queueSize == 0 {
		queueSize = size
	}

	return queueSize

	//FIXME
	/*
		numCpu := runtime.NumCPU()

		if numCpu >= partition || !sorted {
			return queueSize
		}

		ratio := partition / numCpu
		return ratio * queueSize
	*/
}
