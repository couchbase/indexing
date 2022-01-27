// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package client

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/query/value"

	//"runtime"
	"encoding/json"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/parser"
	qvalue "github.com/couchbase/query/value"
)

//--------------------------
// constant
//--------------------------

var (
	NotMyPartition = "Not my partition"
)

var SyncPools []*common.BytesBufPool
var NUM_SYNC_POOLS int
var syncPoolsCtr uint32

func InitializeSyncPools() []*common.BytesBufPool {
	NUM_SYNC_POOLS = runtime.GOMAXPROCS(0) * 2
	pools := make([]*common.BytesBufPool, NUM_SYNC_POOLS)
	for i := 0; i < NUM_SYNC_POOLS; i++ {
		pools[i] = common.NewByteBufferPool(common.TEMP_BUF_SIZE)
	}
	return pools
}

func GetFromPools() (*[]byte, uint32) {
	// No need to worry about the integer overflow.
	idx := atomic.AddUint32(&syncPoolsCtr, 1)
	poolIdx := idx % uint32(NUM_SYNC_POOLS)
	buf := SyncPools[int(poolIdx)].Get()
	return buf, poolIdx
}

func PutInPools(buf *[]byte, poolIdx uint32) {
	SyncPools[int(poolIdx)].Put(buf)
}

func init() {
	SyncPools = InitializeSyncPools()
}

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
	waiter  BackfillWaiter

	// initialization
	requestId   string
	size        int64
	concurrency int
	retry       bool

	// scatter/gather
	queues   []*Queue
	notifych chan bool
	closed   int32
	killch   chan bool
	bGather  bool
	errMap   map[common.PartitionId]map[uint64]error
	partial  int32
	mutex    sync.Mutex

	//backfill
	backfills []*os.File

	// scan
	defn           *common.IndexDefn
	limit          int64
	offset         int64
	sorted         bool // return rows in sorted order?
	pushdownLimit  int64
	pushdownOffset int64
	pushdownSorted bool
	scans          Scans
	grpAggr        *GroupAggr
	projections    *IndexProjection
	indexOrder     *IndexKeyOrder // ordering of index key parts
	projDesc       []bool         // which returned fields (in projection order) are indexed descending
	distinct       bool

	// Additional key positions (not in projection list) added due to
	// IndexKeyOrder for sorting purpose. These additions keys need to be
	// pruned from index entry row before sending to N1QL
	indexOrderPosPruneMap map[int64]bool

	// stats
	sendCount    int64
	receiveCount int64
	numIndexers  int64
	dataEncFmt   uint32 // common.DataEncodingFormat

	// Temporary bufferes needed for DecodeN1QLValues.
	tmpbufs        []*[]byte
	tmpbufsPoolIdx []uint32
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
func NewRequestBroker(requestId string, size int64, concurrency int) *RequestBroker {

	return &RequestBroker{
		requestId:      requestId,
		size:           size,
		concurrency:    concurrency,
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
// Set BackfillWaiter
//
func (b *RequestBroker) SetBackfillWaiter(waiter BackfillWaiter) {

	b.waiter = waiter
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
// Also reset indexOrderPosPruneMap. There should be analyzeOrderBy invoked
// after SetProjection is called to ensure indexOrderPosPruneMap is
// correctly populated
//
func (b *RequestBroker) SetProjection(projection *IndexProjection) {

	b.projections = projection
	b.indexOrderPosPruneMap = nil
}

//
// Set Index Order
//
func (b *RequestBroker) SetIndexOrder(indexOrder *IndexKeyOrder) {

	b.indexOrder = indexOrder
}

//
// Retry
//
func (b *RequestBroker) SetRetry(retry bool) {
	b.retry = retry
}

func (b *RequestBroker) DoRetry() bool {
	return b.retry
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
func (b *RequestBroker) IsClose() bool {

	return atomic.LoadInt32(&b.closed) == 1
}

func (b *RequestBroker) IncrementReceiveCount(count int) {

	atomic.AddInt64(&b.receiveCount, int64(count))
}

func (b *RequestBroker) IncrementSendCount() {

	atomic.AddInt64(&b.sendCount, 1)
}

// Prune the additional keys added for Order By processing
func (b *RequestBroker) pruneOrderByProjections(vals value.Values) value.Values {

	if len(b.indexOrderPosPruneMap) == 0 {
		// Nothing to prune, return
		return vals
	}

	if len(vals) == 0 { // if vals is nil or empty
		return vals
	}

	inclPos := 0
	for i, val := range vals {
		if _, ok := b.indexOrderPosPruneMap[int64(i)]; !ok {
			// i is not in the map, which means this pos was not additionally
			// added by analyzeOrderBy. Include this in final result
			vals[inclPos] = val
			inclPos++
		}
	}

	return vals[:inclPos]
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

func (c *RequestBroker) AddBackfill(backfill *os.File) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.backfills = append(c.backfills, backfill)
}

func (c *RequestBroker) GetBackfills() []*os.File {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.backfills
}

func (b *RequestBroker) reset() {

	// scatter/gather
	b.queues = nil
	b.notifych = nil
	b.closed = 0
	b.killch = make(chan bool, 1)
	b.bGather = false
	b.errMap = make(map[common.PartitionId]map[uint64]error)
	b.partial = 0 // false

	// backfill
	// do not reset backfills
	//b.backfills = nil

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
func (c *RequestBroker) scatter(clientMaker scanClientMaker, index *common.IndexDefn,
	scanports []string, targetInstId []uint64, rollback []int64, partition [][]common.PartitionId,
	numPartition uint32, settings *ClientSettings) (count int64,
	err map[common.PartitionId]map[uint64]error, partial bool, refresh bool) {

	defer func() {
		logging.Verbosef("scatter: requestId %v items recieved %v items processed %v", c.requestId, c.ReceiveCount(), c.SendCount())
	}()

	c.reset()
	c.SetNumIndexers(len(partition))
	c.defn = index

	concurrency := int(settings.MaxConcurrency())
	if concurrency == 0 {
		concurrency = int(numPartition)
	}

	var ok bool
	var client []*GsiScanClient
	partition = c.filterPartitions(index, partition, numPartition)
	ok, client, targetInstId, rollback, partition = c.makeClients(clientMaker, concurrency, index, numPartition, scanports, targetInstId, rollback, partition)
	if !ok {
		return 0, nil, false, true
	}

	c.analyzeOrderBy(partition, numPartition, index)
	c.analyzeProjection(partition, numPartition, index)
	c.changePushdownParams(partition, numPartition, index)

	if len(partition) == len(client) {
		for i, partitions := range partition {
			logging.Verbosef("scatter: requestId %v queryport %v partition %v", c.requestId, client[i].queryport, partitions)
		}
	}

	logging.Verbosef("scatter: requestId %v limit %v offset %v sorted %v pushdown limit %v pushdown offset %v pushdown sorted %v isAggr %v",
		c.requestId, c.limit, c.offset, c.sorted, c.pushdownLimit, c.pushdownOffset, c.pushdownSorted, c.grpAggr != nil)

	if c.projections != nil {
		logging.Debugf("scatter: requestId %v projection %v Desc %v", c.requestId, c.projections.EntryKeys, c.projDesc)
	}

	if c.scan != nil {
		err, partial = c.scatterScan2(client, index, targetInstId, rollback, partition, numPartition, settings)
		return 0, err, partial, false
	} else if c.count != nil {
		count, err, partial := c.scatterCount(client, index, targetInstId, rollback, partition, numPartition)
		return count, err, partial, false
	}

	e := fmt.Errorf("Intenral error: Fail to process request for index %v:%v:%v:%v.  Unknown request handler.", index.Bucket,
		index.Scope, index.Collection, index.Name)
	return 0, c.makeErrorMap(targetInstId, partition, e), false, false
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
// Make Scan client that are used in scans
//
func (c *RequestBroker) makeClients(maker scanClientMaker, max_concurrency int, index *common.IndexDefn, numPartition uint32,
	scanports []string, targetInstIds []uint64, timestamps []int64, allPartitions [][]common.PartitionId) (bool, []*GsiScanClient,
	[]uint64, []int64, [][]common.PartitionId) {

	var newClient []*GsiScanClient
	var newInstId []uint64
	var newTS []int64
	var newPartition [][]common.PartitionId
	var newScanport []string

	for i, partitions := range allPartitions {
		if len(partitions) != 0 {
			client := maker(scanports[i])
			if client == nil {
				return false, nil, nil, nil, nil
			}

			newClient = append(newClient, client)
			newScanport = append(newScanport, scanports[i])
			newInstId = append(newInstId, targetInstIds[i])
			newTS = append(newTS, timestamps[i])
			newPartition = append(newPartition, partitions)
		}
	}

	if c.isPartitionedAggregate(index) {
		newClient, newInstId, newTS, newPartition = c.splitClients(maker, max_concurrency, newScanport, newClient, newInstId, newTS, newPartition)
	}

	return true, newClient, newInstId, newTS, newPartition
}

//
// split the clients until it reaches the maximum parallelism
//
func (c *RequestBroker) splitClients(maker scanClientMaker, max_concurrency int, scanports []string, clients []*GsiScanClient, instIds []uint64,
	timestamps []int64, allPartitions [][]common.PartitionId) ([]*GsiScanClient, []uint64, []int64, [][]common.PartitionId) {

	if len(clients) >= max_concurrency {
		return clients, instIds, timestamps, allPartitions
	}

	max := 0
	pos := -1
	for i, partitions := range allPartitions {
		if len(partitions) > max {
			max = len(partitions)
			pos = i
		}
	}

	// split the clients with the most partitions
	if pos != -1 && len(allPartitions[pos]) > 1 {

		len := len(allPartitions[pos]) / 2

		client := maker(scanports[pos])
		clients = append(clients, client)
		scanports = append(scanports, scanports[pos])
		instIds = append(instIds, instIds[pos])
		timestamps = append(timestamps, timestamps[pos])
		allPartitions = append(allPartitions, allPartitions[pos][len:])

		allPartitions[pos] = allPartitions[pos][0:len]

		return c.splitClients(maker, max_concurrency, scanports, clients, instIds, timestamps, allPartitions)
	}

	return clients, instIds, timestamps, allPartitions
}

//
// Scatter scan requests over multiple connections
//
func (c *RequestBroker) scatterScan(client []*GsiScanClient, index *common.IndexDefn, targetInstId []uint64, rollback []int64, partition [][]common.PartitionId,
	numPartition uint32, settings *ClientSettings) (err error, partial bool) {

	c.notifych = make(chan bool, 1)
	donech_gather := make(chan bool, 1)

	if len(partition) > 1 {
		c.bGather = true
	}

	var tmpbuf *[]byte
	var tmpbufPoolIdx uint32
	c.tmpbufs = make([]*[]byte, len(client))
	c.tmpbufsPoolIdx = make([]uint32, len(client))
	for i := 0; i < len(client); i++ {
		tmpbuf, tmpbufPoolIdx = GetFromPools()
		c.tmpbufs[i] = tmpbuf
		c.tmpbufsPoolIdx[i] = tmpbufPoolIdx
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

	for i, _ := range client {
		PutInPools(c.tmpbufs[i], c.tmpbufsPoolIdx[i])
	}

	if c.useGather() && err == nil {
		<-donech_gather
	}

	return
}

func (c *RequestBroker) scatterScan2(client []*GsiScanClient, index *common.IndexDefn, targetInstId []uint64, rollback []int64,
	partition [][]common.PartitionId, numPartition uint32, settings *ClientSettings) (errMap map[common.PartitionId]map[uint64]error, partial bool) {

	c.notifych = make(chan bool, 1)
	donech_gather := make(chan bool, 1)

	if len(partition) > 1 {
		c.bGather = true
	}

	var tmpbuf *[]byte
	var tmpbufPoolIdx uint32
	c.tmpbufs = make([]*[]byte, len(client))
	c.tmpbufsPoolIdx = make([]uint32, len(client))
	for i := 0; i < len(client); i++ {
		tmpbuf, tmpbufPoolIdx = GetFromPools()
		c.tmpbufs[i] = tmpbuf
		c.tmpbufsPoolIdx[i] = tmpbufPoolIdx
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
	for i, _ := range client {
		<-donech_scatter[i]
	}
	logging.Debugf("RequestBroker.scatterScan2: requestId %v scatter done", c.requestId)

	// Wait for gather to finish (all rows are sent to cbq)
	if c.useGather() {
		<-donech_gather
	}
	logging.Debugf("RequestBroker.scatterScan2: requestId %v gather done", c.requestId)

	// if there is an error, wait for backfill done before retrying.
	// This is to make sure the backfill goroutine will not interfere
	// with retry execution.
	if c.waiter != nil {
		c.waiter()
		logging.Debugf("RequestBroker.scatterScan2: requestId %v done reading from temp file", c.requestId)
	}

	for i, _ := range client {
		PutInPools(c.tmpbufs[i], c.tmpbufsPoolIdx[i])
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

// sort bubble sorts the heads of all queues into the rows
// argument. If all queues have entries, it returns true and
// the rows values will be fully sorted; otherwise it returns
// false and the rows values will not be fully sorted.
//
// This function is only called to initialize the ordering.
// Subsequently the closely related pick function is called to
// consume rows one by one and incrementally update the sort.
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
		if c.IsClose() {
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

	var cont bool
	var retBuf *[]byte

	tmpbuf, tmpbufPoolIdx := GetFromPools()
	defer func() {
		PutInPools(tmpbuf, tmpbufPoolIdx)
	}()

	for {
		var id int

		if c.IsClose() {
			return
		}

		id = c.pick(rows, sorted)
		if id == Done {
			// just to be safe.  Call c.done()
			c.done()
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

			// Remove additional keys in result row added due to Order By
			// processing. This is needed only for partitioned index
			// with Order By. So pruning is needed only in the gather() method
			prunedRow := c.pruneOrderByProjections(rows[id].value)
			cont, retBuf = c.sender(rows[id].pkey, prunedRow, rows[id].skey, tmpbuf)
			if retBuf != nil {
				tmpbuf = retBuf
			}
			if !cont {
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
	// This is just to make sure that we have received at
	// least one row from each before streaming response back.
	isSorted := false
	for !isSorted {
		if c.IsClose() {
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

	var cont bool
	var retBuf *[]byte

	tmpbuf, tmpbufPoolIdx := GetFromPools()
	defer func() {
		PutInPools(tmpbuf, tmpbufPoolIdx)
	}()

	for {
		if c.IsClose() {
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
					cont, retBuf = c.sender(rows[i].pkey, rows[i].value, rows[i].skey, tmpbuf)
					if retBuf != nil {
						tmpbuf = retBuf
					}
					if !cont {
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
			// just to be safe.  Call c.done()
			c.done()
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

// This function compares two sets of secondary key values.
// Returns –int, 0 or +int depending on if key1
// sorts less than, equal to, or greater than key2.
//
// Each sec key is a array of Value, ordered based on
// how they are defined in the index.  In addition, if
// the key is in order-by, cbq will put it in the
// projection list (even if the user does not ask for it).
//
func (c *RequestBroker) compareKey(key1, key2 []value.Value) int {

	// Find shorter key len ln. Save key lens ln1, ln2 to avoid recomputes.
	ln1, ln2 := len(key1), len(key2)
	ln := ln1
	if ln2 < ln {
		ln = ln2
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

	return ln1 - ln2
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

		//c.Partial(partial)
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
func (c *RequestBroker) SendEntries(id ResponseHandlerId, pkeys [][]byte,
	skeys *common.ScanResultEntries) (bool, error) {

	if c.IsClose() {
		return false, nil
	}

	// StreamEndResponse has nil pkeys and nil skeys
	if len(pkeys) == 0 && skeys.GetLength() == 0 {
		if c.useGather() {
			var r Row
			r.last = true
			c.queues[int(id)].Enqueue(&r)
		}
		return false, nil
	}

	var err error
	var rb *[]byte
	var cont bool
	var skey common.ScanResultKey

	tmpbuf := c.tmpbufs[int(id)]
	defer func() {
		c.tmpbufs[int(id)] = tmpbuf
	}()

	for i := 0; i < skeys.GetLength(); i++ {
		if c.useGather() {
			var vals []value.Value
			if c.sorted {
				vals, err, rb = skeys.Getkth(tmpbuf, i)
				if err != nil {
					logging.Errorf("Error %v in RequestBroker::SendEntries Getkth", err)
					return false, err
				}

				if rb != nil {
					tmpbuf = rb
				}
			}

			if c.sorted && len(vals) == 0 {
				vals = make(value.Values, 0)
			}

			var r Row
			r.pkey = pkeys[i]
			r.value = vals
			r.skey, err = skeys.GetkthKey(i)
			if err != nil {
				logging.Errorf("Error %v in RequestBroker::SendEntries GetkthKey", err)
				return false, err
			}
			c.queues[int(id)].Enqueue(&r)
		} else {

			c.Partial(true)
			skey, err = skeys.GetkthKey(i)
			if err != nil {
				logging.Errorf("Error %v in RequestBroker::SendEntries GetkthKey", err)
				return false, err
			}
			cont, rb = c.sender(pkeys[i], nil, skey, tmpbuf)
			if rb != nil {
				tmpbuf = rb
			}
			if !cont {
				c.done()
				return false, nil
			}
		}
	}

	if c.useGather() {
		c.queues[int(id)].NotifyEnq()
	}

	return !c.IsClose(), nil
}

//--------------------------
// default request broker
//--------------------------

type bypassResponseReader struct {
	pkey []byte
	skey common.ScanResultKey
}

func (d *bypassResponseReader) GetEntries(dataEncFmt common.DataEncodingFormat) (*common.ScanResultEntries, [][]byte, error) {
	entries := common.NewScanResultEntries(dataEncFmt)
	entries.Make(1)
	entry, err := d.skey.GetRaw()
	if err != nil {
		return nil, nil, err
	}

	entries, err = entries.Append(entry)
	if err != nil {
		return nil, nil, err
	}

	return entries, [][]byte{d.pkey}, nil
}

func (d *bypassResponseReader) Error() error {
	return nil
}

func makeDefaultRequestBroker(cb ResponseHandler,
	dataEncFmt common.DataEncodingFormat) *RequestBroker {

	broker := NewRequestBroker("", 256, -1)
	broker.SetDataEncodingFormat(dataEncFmt)

	factory := func(id ResponseHandlerId, instId uint64, partitions []common.PartitionId) ResponseHandler {
		return makeDefaultResponseHandler(id, broker, instId, partitions)
	}

	sender := func(pkey []byte, mskey []value.Value, uskey common.ScanResultKey, tmpbuf *[]byte) (bool, *[]byte) {
		broker.IncrementSendCount()
		if cb != nil {
			var reader bypassResponseReader
			reader.pkey = pkey
			reader.skey = uskey
			return cb(&reader), nil
		}
		return true, nil
	}

	broker.SetResponseHandlerFactory(factory)
	broker.SetResponseSender(sender)

	return broker
}

func makeDefaultResponseHandler(id ResponseHandlerId, broker *RequestBroker, instId uint64, partitions []common.PartitionId) ResponseHandler {

	handler := func(resp ResponseReader) bool {
		var cont bool
		err := resp.Error()
		if err != nil {
			logging.Errorf("defaultResponseHandler: %v", err)
			broker.Error(err, instId, partitions)
			return false
		}
		skeys, pkeys, err := resp.GetEntries(broker.GetDataEncodingFormat())
		if err != nil {
			logging.Errorf("defaultResponseHandler: %v", err)
			broker.Error(err, instId, partitions)
			return false
		}
		if len(pkeys) != 0 || skeys.GetLength() != 0 {
			if len(pkeys) != 0 {
				broker.IncrementReceiveCount(len(pkeys))
			} else {
				broker.IncrementReceiveCount(skeys.GetLength())
			}
		}
		cont, err = broker.SendEntries(id, pkeys, skeys)
		if err != nil {
			logging.Errorf("defaultResponseHandler: SendEntries %v", err)
			broker.Error(err, instId, partitions)
			return false
		}
		return cont
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

	filter := partitionKeyHash(partitionKeyValues, c.scans, numPartition, index.HashScheme)
	if len(filter) == 0 {
		return partitions
	}

	return filterPartitionIds(partitions, filter)
}

//
// Find out the position of parition key in the index key list
// If there is any partition key cannot be found in the index key list,
// this function returns nil
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
// Each scan (AND-predicate) has a list of filters, with each filter being a operator on a index key
// The filters in the scan is sorted based on index order
// For partition elimination to happen, the filters must contain all the partiton keys
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

					} else if pos == MetaIdPos &&
						len(scan.Filter) == 1 &&
						reflect.DeepEqual(scan.Filter[0].Low, scan.Filter[0].High) {
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

					} else if pos == MetaIdPos &&
						len(scan.Filter) == 1 &&
						reflect.DeepEqual(scan.Filter[0].Low, scan.Filter[0].High) {
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
func partitionKeyHash(partnKeyValues [][]interface{}, scans Scans, numPartition uint32, hashScheme common.HashScheme) map[common.PartitionId]bool {

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

		partnId := common.HashKeyPartition(v, int(numPartition), hashScheme)
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

//--------------------------
// API2 Push Down
//--------------------------

func (c *RequestBroker) changePushdownParams(partitions [][]common.PartitionId, numPartition uint32, index *common.IndexDefn) {
	c.changeLimit(partitions, numPartition, index)
	c.changeOffset(partitions, numPartition, index)
	c.changeSorted(partitions, numPartition, index)
}

//
// For aggregate query, cbq-engine will push down limit only if full aggregate results are needed.  Like range query, the limit
// parameter will be rewritten before pushing down to indexer.   The limit will be applied in gathered when results are returning
// from all indexers.
//
// For pre-aggregate result, cbq-engine will not push down limit, since limit can only apply after aggregation.
//
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

//
// For aggregate query, cbq-engine will push down offset only if full aggregate results are needed.  Like range query, the offset
// parameter will be rewritten before pushing down to indexer.   The offset will be applied in gathered when results are returning
// from all indexers.
//
// For pre-aggregate result, cbq-engine will not push down offset, since limit can only apply after aggregation.
//
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

//
// Determine if the results from each indexer needs to be sorted based on index key order.
//
func (c *RequestBroker) changeSorted(partitions [][]common.PartitionId, numPartition uint32, index *common.IndexDefn) {

	c.pushdownSorted = c.sorted

	if c.distinct {
		c.pushdownSorted = true
	}

	// If it is an aggregate query, need to sort using index order
	// The indexer depends on results are sorted across partitions co-located on the same node
	if c.grpAggr != nil && (len(c.grpAggr.Group) != 0 || len(c.grpAggr.Aggrs) != 0) {
		c.pushdownSorted = true
	}

}

//--------------------------
// API3 push down
//--------------------------

func (c *RequestBroker) isPartitionedAggregate(index *common.IndexDefn) bool {

	// non-partition index
	if index.PartitionScheme == common.SINGLE {
		return false
	}

	// aggreate query
	if c.grpAggr != nil {
		return true
	}

	return false
}

//
// For aggregate query, n1ql will expect full aggregate results for partitioned index if
// 1) group-by keys matches the partition keys (order is not important)
// 2) group keys are leading index keys
// 3) partition keys are leading index keys
//
func (c *RequestBroker) isPartialAggregate(partitions [][]common.PartitionId, numPartition uint32, index *common.IndexDefn) bool {

	// non-partition index
	if index.PartitionScheme == common.SINGLE {
		return false
	}

	// aggreate query
	if c.grpAggr != nil && len(c.grpAggr.Group) != 0 {

		// check if partition keys are leading index keys
		positions := partitionKeyPos(index)
		if len(positions) != len(index.PartitionKeys) {
			return true
		}
		for i, pos := range positions {
			if i != pos {
				return true
			}
		}

		// check if group keys are leading index keys
		for i, group := range c.grpAggr.Group {
			if int32(i) != group.KeyPos {
				// if group keys do not follow index key order
				return true
			}
		}

		// both paritition keys and group keys are in index order.
		// check if group key and partition key are the same length (order is not important)
		if len(index.PartitionKeys) != len(c.grpAggr.Group) {
			return true
		}
	}

	return false
}

//
// We cannot sort if it is pre-aggregate result, so set sorted to false.  Otherwise, the result
// is sorted if there is an order-by clause.
//
// Note that cbq-engine will not push down order-by unless order by matches leading index keys.
// If it is a partitioned index, order-by will not push down unless it is full aggregate result.
//
// THIS FUNCTION IS NOT CALLED BECAUSE CBQ IS ALREADY HANDLING IT
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

	if c.isPartialAggregate(partitions, numPartition, index) {
		c.sorted = false
		return
	}
}

//
// If sorted, then analyze the index order to see if we need to add index to projection list.
// If it is a non-covering index, cbq-engine will not add order-by keys to the projection list. So Gsi client
// will have to add the keys for sorting the scattered results.
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

	if c.indexOrder != nil {

		projection := make(map[int64]bool)
		if c.projections != nil && len(c.projections.EntryKeys) != 0 {
			// Entry key can be an index key, group key expression, or aggregate expression.
			for _, position := range c.projections.EntryKeys {
				projection[position] = true
			}
		}

		// If the order-by key is not in the projection, then add it.
		// Cbq-engine pushes order-by only if the order-by keys match the leading index keys.
		// So order-by key must be an index key.
		if c.projections == nil {
			// Everything should be projected. Else this can cause order by to
			// prune positions which need to be present
			return
		}

		if c.indexOrderPosPruneMap == nil {
			c.indexOrderPosPruneMap = make(map[int64]bool)
		}
		for _, order := range c.indexOrder.KeyPos {
			if !projection[int64(order)] {
				c.projections.EntryKeys = append(c.projections.EntryKeys, int64(order))
				if _, ok := c.indexOrderPosPruneMap[int64(order)]; !ok {
					// If not present the map, add
					c.indexOrderPosPruneMap[int64(order)] = true
				}
				projection[int64(order)] = true
			}
		}
	}
}

//
// If sorted, then analyze the projection to find out the sort order (asc/desc).
//
// Cbq-engine pushes order-by only if the order-by keys match the leading index keys.  If there is an expression in the order-by clause,
// cbq will not push down order-by even if the expression is based on the index key.
//
// For aggregate query, the order-by keys need to match the group-key as well.  So if the group-key is based on an expression on an index key,
// this means the order-by key also needs to have the same expression.  In this case, cbq will not push down order-by (violates rule above).
// Cbq also will not allow if there is more order-by keys than group-keys.
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

	// order-by is pushed down only if order-by keys match leading index keys
	//
	// If there is a projection list, cbq will have added any order-by keys
	// to it (handled by "if" block). "Else if" block handles the special
	// case where there is an order-by but no projection list because primary
	// key and all index keys are selected.
	if c.projections != nil && len(c.projections.EntryKeys) != 0 {

		pos := make([]int, len(c.projections.EntryKeys))
		for i, position := range c.projections.EntryKeys {
			pos[i] = int(position)
		}
		sort.Ints(pos)

		c.projDesc = make([]bool, len(c.projections.EntryKeys))
		for i, position := range pos {
			// EntryKey could be index key, group key expression, aggregate expression.
			// For expression,  entryKey would be an integer greater than len(index.SecKeys)
			if position >= 0 && position < len(index.Desc) {
				// The result coming from indexer are in index order.  If
				// an index key is not in the projection list, it will be
				// skipped in the returned result.
				c.projDesc[i] = index.Desc[position]
			}
		}
	} else {
		c.projDesc = index.Desc
	}
}

func (c *RequestBroker) SetDataEncodingFormat(val common.DataEncodingFormat) {
	atomic.StoreUint32(&c.dataEncFmt, uint32(val))
}

func (c *RequestBroker) GetDataEncodingFormat() common.DataEncodingFormat {
	return common.DataEncodingFormat(atomic.LoadUint32(&c.dataEncFmt))
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
		numCpu := runtime.GOMAXPROCS(0)

		if numCpu >= partition || !sorted {
			return queueSize
		}

		ratio := partition / numCpu
		return ratio * queueSize
	*/
}
