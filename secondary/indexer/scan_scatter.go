// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package indexer

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/pipeline"
)

var ErrFinishCallback error = errors.New("Callback done due to error")

const (
	NoPick = -1
	Done   = -2
)

//--------------------------
// scatter range scan
//--------------------------

func getPartitionId(request *ScanRequest, pos int) common.PartitionId {

	// For nil-snapshot, it has empty list of snapshot.  So return 0, since there won't be any stats.
	// For non-partitioned index, len(request.PartitionIds) is either 1 or 0.   If it is 1, then
	// request.PartitionIds[0] returns partitionId 0.
	//
	if len(request.PartitionIds) > pos {
		return request.PartitionIds[pos]
	}

	return common.PartitionId(0)
}

func scatter(request *ScanRequest, scan Scan, snapshots []SliceSnapshot, cb EntryCallback, config common.Config) (err error) {

	if len(snapshots) == 0 {
		return
	}

	if len(snapshots) == 1 {
		partitionId := getPartitionId(request, 0)
		return scanOne(request, scan, snapshots, partitionId, cb)
	}

	return scanMultiple(request, scan, snapshots, cb, config)
}

func scanMultiple(request *ScanRequest, scan Scan, snapshots []SliceSnapshot, cb EntryCallback, config common.Config) (err error) {

	var wg sync.WaitGroup

	notifych := make(chan bool, 1)
	killch := make(chan bool, 1)
	donech := make(chan bool, 1)
	errch := make(chan error, len(snapshots)+100)

	sorted := request.Sorted

	queues := make([]*Queue, len(snapshots))
	size, limit := queueSize(len(snapshots), sorted, config)
	for i := 0; i < len(snapshots); i++ {
		partitionId := getPartitionId(request, i)

		var m *allocator
		if v := request.connCtx.Get(fmt.Sprintf("%v%v", ScanQueue, partitionId)); v == nil {
			bufPool := request.connCtx.GetBufPool(partitionId)
			m = newAllocator(int64(size), bufPool)
		} else {
			m = v.(*allocator)
		}

		queues[i] = NewQueue(int64(size), int64(limit), notifych, m)
	}
	defer func() {
		for i, queue := range queues {
			queue.Close()
			queue.Free()

			partitionId := getPartitionId(request, i)
			if m := queue.GetAllocator(); m != nil {
				//logging.Debugf("Free allocator %p partition id %v count %v malloc %v", m, partitionId, m.count, m.numMalloc)
				request.connCtx.Put(fmt.Sprintf("%v%v", ScanQueue, partitionId), m)
			}
		}
	}()

	// run gather
	if sorted {
		go gather(request, queues, donech, notifych, killch, errch, cb)
	} else {
		go forward(request, queues, donech, notifych, killch, errch, cb)
	}

	// run scatter
	for i, snap := range snapshots {
		wg.Add(1)
		partitionId := getPartitionId(request, i)
		go scanSingleSlice(request, scan, request.Ctxs[i], snap, partitionId, queues[i], &wg, errch, nil)
	}

	// wait for scatter to be done
	wg.Wait()

	errcnt := len(errch)
	for i := 0; i < errcnt; i++ {
		err = <-errch
		if err == pipeline.ErrSupervisorKill || err == ErrLimitReached {
			break
		}
	}

	if err != nil {
		// stop gather go-routine
		close(killch)
		errch <- err
	}

	// wait for gather to be done
	<-donech

	enqCount := int64(0)
	deqCount := int64(0)
	for _, queue := range queues {
		enqCount += queue.EnqueueCount()
		deqCount += queue.DequeueCount()
	}
	logging.Debugf("scan_scatter.scanMultiple: scan done.  enqueue count %v dequeue count %v", enqCount, deqCount)

	return
}

func scanOne(request *ScanRequest, scan Scan, snapshots []SliceSnapshot, partitionId common.PartitionId, cb EntryCallback) (err error) {

	errch := make(chan error, 1)
	count := scanSingleSlice(request, scan, request.Ctxs[0], snapshots[0], partitionId, nil, nil, errch, cb)

	logging.Debugf("scan_scatter:scanOnce: scan done. Count %v", count)

	errcnt := len(errch)
	for i := 0; i < errcnt; i++ {
		err = <-errch
		if err == pipeline.ErrSupervisorKill || err == ErrLimitReached {
			break
		}
	}

	return
}

func scanSingleSlice(request *ScanRequest, scan Scan, ctx IndexReaderContext, snap SliceSnapshot, partitionId common.PartitionId,
	queue *Queue, wg *sync.WaitGroup, errch chan error, cb EntryCallback) (count int) {

	defer func() {
		if wg != nil {
			wg.Done()
		}

		request.Stats.updatePartitionStats(partitionId, func(ps *IndexStats) {
			ps.numRowsScanned.Add(int64(count))
		})
	}()

	handler := func(entry, value []byte) error {
		// Do not call enqueue when there is error.
		if len(errch) != 0 {
			return ErrFinishCallback
		}

		count++

		if queue != nil {

			var r Row
			if !request.isPrimary {
				entry1 := secondaryIndexEntry(entry)
				r.len = entry1.lenKey()
			}
			r.key = entry
			r.value = value

			queue.Enqueue(&r)
			return nil
		} else {
			return cb(entry, value)
		}
	}

	var err error
	if scan.ScanType == AllReq {
		err = snap.Snapshot().All(ctx, handler, nil)
	} else if scan.ScanType == LookupReq {
		err = snap.Snapshot().Range(ctx, scan.Equals, scan.Equals, Both, handler, nil)
	} else if scan.ScanType == RangeReq || scan.ScanType == FilterRangeReq {
		err = snap.Snapshot().Range(ctx, scan.Low, scan.High, scan.Incl, handler, nil)
	}

	if err != nil {
		if err != ErrFinishCallback {
			errch <- err
		}
		if queue != nil {
			queue.Close()
		}
	} else if queue != nil {
		// If there is no error, tells gather routine that I am done.
		// Do not call enqueue when there is error.
		var r Row
		r.last = true
		queue.Enqueue(&r)
	}

	return
}

//--------------------------
// scatter count
//--------------------------

func scatterCount(request *ScanRequest, snapshots []SliceSnapshot, stop StopChannel) (count uint64, err error) {

	if len(snapshots) == 0 {
		return
	}

	var wg sync.WaitGroup

	errch := make(chan error, len(snapshots))

	// run scatter
	for i, snap := range snapshots {
		wg.Add(1)
		go countSingleSlice(request, request.Ctxs[i], snap, &wg, errch, stop, &count)
	}

	// wait for scatter to be done
	wg.Wait()

	if len(errch) > 0 {
		err = <-errch
	}

	return
}

func countSingleSlice(request *ScanRequest, ctx IndexReaderContext, snap SliceSnapshot, wg *sync.WaitGroup, errch chan error, stopch StopChannel, count *uint64) {

	defer func() {
		wg.Done()
	}()

	var err error
	var cnt uint64

	if len(request.Keys) > 0 {
		cnt, err = snap.Snapshot().CountLookup(ctx, request.Keys, stopch)
	} else if request.Low.Bytes() == nil && request.High.Bytes() == nil {
		cnt, err = snap.Snapshot().CountTotal(ctx, stopch)
	} else {
		cnt, err = snap.Snapshot().CountRange(ctx, request.Low, request.High, request.Incl, stopch)
	}

	if err != nil {
		errch <- err
	} else {
		atomic.AddUint64(count, cnt)
	}
}

//--------------------------
// scatter multi-count
//--------------------------

func scatterMultiCount(request *ScanRequest, scan Scan, snapshots []SliceSnapshot, previousRows [][]byte, stop StopChannel) (count uint64, err error) {

	if len(snapshots) == 0 {
		return
	}

	var wg sync.WaitGroup

	errch := make(chan error, len(snapshots))

	// run scatter
	for i, snap := range snapshots {
		wg.Add(1)
		go multiCountSingleSlice(request, scan, request.Ctxs[i], snap, previousRows[i], &wg, errch, stop, &count)
	}

	// wait for scatter to be done
	wg.Wait()

	if len(errch) > 0 {
		err = <-errch
	}

	return
}

func multiCountSingleSlice(request *ScanRequest, scan Scan, ctx IndexReaderContext, snap SliceSnapshot, previousRow []byte, wg *sync.WaitGroup,
	errch chan error, stopch StopChannel, count *uint64) {

	defer func() {
		wg.Done()
	}()

	var err error
	var cnt uint64

	if scan.ScanType == AllReq {
		cnt, err = snap.Snapshot().MultiScanCount(ctx, MinIndexKey, MaxIndexKey, Both, scan, request.Distinct, stopch)
	} else if scan.ScanType == LookupReq || scan.ScanType == RangeReq || scan.ScanType == FilterRangeReq {
		cnt, err = snap.Snapshot().MultiScanCount(ctx, scan.Low, scan.High, scan.Incl, scan, request.Distinct, stopch)
	}

	if err != nil {
		errch <- err
	} else {
		atomic.AddUint64(count, cnt)
	}
}

//--------------------------
// scatter stats
//--------------------------

func scatterStats(request *ScanRequest, snapshots []SliceSnapshot, stop StopChannel) (count uint64, err error) {

	if len(snapshots) == 0 {
		return
	}

	var wg sync.WaitGroup

	errch := make(chan error, len(snapshots))

	// run scatter
	for i, snap := range snapshots {
		wg.Add(1)
		go statsSingleSlice(request, request.Ctxs[i], snap, &wg, errch, stop, &count)
	}

	// wait for scatter to be done
	wg.Wait()

	if len(errch) > 0 {
		err = <-errch
	}

	return
}

func statsSingleSlice(request *ScanRequest, ctx IndexReaderContext, snap SliceSnapshot, wg *sync.WaitGroup,
	errch chan error, stopch StopChannel, count *uint64) {

	defer func() {
		wg.Done()
	}()

	var err error
	var cnt uint64

	if request.Low.Bytes() == nil && request.High.Bytes() == nil {
		cnt, err = snap.Snapshot().StatCountTotal()
	} else {
		cnt, err = snap.Snapshot().CountRange(ctx, request.Low, request.High, request.Incl, stopch)
	}

	if err != nil {
		errch <- err
	} else {
		atomic.AddUint64(count, cnt)
	}
}

//--------------------------
// scatter fast count
//--------------------------

func scatterFastCount(request *ScanRequest, scan Scan, snapshots []SliceSnapshot, stop StopChannel) (count uint64, err error) {

	if len(snapshots) == 0 {
		return
	}

	var wg sync.WaitGroup

	errch := make(chan error, len(snapshots))

	// run scatter
	for i, snap := range snapshots {
		wg.Add(1)
		go fastCountSingleSlice(request, scan, request.Ctxs[i], snap, &wg, errch, stop, &count)
	}

	// wait for scatter to be done
	wg.Wait()

	if len(errch) > 0 {
		err = <-errch
	}

	return
}

func fastCountSingleSlice(request *ScanRequest, scan Scan, ctx IndexReaderContext, snap SliceSnapshot, wg *sync.WaitGroup,
	errch chan error, stopch StopChannel, count *uint64) {

	defer func() {
		wg.Done()
	}()

	var err error
	var cnt uint64
	var nullCnt uint64

	desc := false
	if request.IndexInst.Defn.HasDescending() {
		//fast count only works for first leading key
		if request.IndexInst.Defn.Desc[0] {
			desc = true
		}
	}

	if !desc {
		if scan.Incl == Low || scan.Incl == Both {
			cnt, err = snap.Snapshot().CountTotal(ctx, stopch)
		} else if scan.Incl == Neither {
			nullCnt, err = snap.Snapshot().CountRange(ctx, scan.Low, scan.Low, Both, stopch)
			if err == nil {
				cnt, err = snap.Snapshot().CountTotal(ctx, stopch)
			}
		}
	} else {
		//for desc, inclusion gets flipped to High
		if scan.Incl == High || scan.Incl == Both {
			cnt, err = snap.Snapshot().CountTotal(ctx, stopch)
		} else if scan.Incl == Neither {
			//for desc, nulls collate on the higher end
			nullCnt, err = snap.Snapshot().CountRange(ctx, scan.High, scan.High, Both, stopch)
			if err == nil {
				cnt, err = snap.Snapshot().CountTotal(ctx, stopch)
			}
		}
	}

	if err != nil {
		errch <- err
	} else {
		cnt = cnt - nullCnt
		atomic.AddUint64(count, cnt)
	}
}

//--------------------------
// gather range scan
//--------------------------

func scan_sort(request *ScanRequest, queues []*Queue, rows []Row, sorted []int) bool {

	size := len(queues)

	for i := 0; i < size; i++ {
		sorted[i] = i
	}

	for i := 0; i < size-1; i++ {
		if !queues[sorted[i]].Peek(&rows[sorted[i]]) {
			return false
		}

		for j := i + 1; j < size; j++ {
			if !queues[sorted[j]].Peek(&rows[sorted[j]]) {
				return false
			}

			if rows[sorted[i]].last && !rows[sorted[j]].last ||
				(!rows[sorted[i]].last && !rows[sorted[j]].last &&
					compareKey(request, &rows[sorted[i]], &rows[sorted[j]]) > 0) {
				tmp := sorted[i]
				sorted[i] = sorted[j]
				sorted[j] = tmp
			}
		}
	}

	return true
}

// Gather results from multiple connections
// rows - buffer of rows from each scatter gorountine
// sorted - sorted order of the rows
func scan_pick(request *ScanRequest, queues []*Queue, rows []Row, sorted []int) int {

	size := len(queues)

	if !queues[sorted[0]].Peek(&rows[sorted[0]]) {
		return NoPick
	}

	pos := 0
	for i := 1; i < size; i++ {
		if !queues[sorted[i]].Peek(&rows[sorted[i]]) {
			return NoPick
		}

		// last value always sorted last
		if rows[sorted[pos]].last && !rows[sorted[i]].last ||
			(!rows[sorted[pos]].last && !rows[sorted[i]].last &&
				compareKey(request, &rows[sorted[pos]], &rows[sorted[i]]) > 0) {

			tmp := sorted[pos]
			sorted[pos] = sorted[i]
			sorted[i] = tmp
			pos = i

		} else {
			break
		}
	}

	for i := 0; i < size; i++ {
		if !rows[sorted[i]].last {
			return sorted[0]
		}
	}

	return Done
}

func gather(request *ScanRequest, queues []*Queue, donech chan bool, notifych chan bool, killch chan bool,
	errch chan error, cb EntryCallback) {

	defer close(donech)

	ensembleSize := len(queues)
	sorted := make([]int, ensembleSize)

	rows := make([]Row, ensembleSize)
	for i := 0; i < ensembleSize; i++ {
		rows[i].init(newAllocator(0, queues[i].GetBytesBuf()))
	}
	defer func() {
		for i := 0; i < ensembleSize; i++ {
			rows[i].freeBuffers()
		}
	}()

	// initial sort
	isSorted := false
	for !isSorted {
		if len(errch) != 0 {
			return
		}

		isSorted = scan_sort(request, queues, rows, sorted)

		if !isSorted {
			select {
			case <-notifych:
				continue
			case <-killch:
				return
			}
		}
	}

	for {
		var id int

		if len(errch) != 0 {
			return
		}

		id = scan_pick(request, queues, rows, sorted)
		if id == Done {
			break
		}

		if id == NoPick {
			select {
			case <-notifych:
				continue
			case <-killch:
				return
			}
		}

		if queues[id].Dequeue(&rows[id]) {
			if err := cb(rows[id].key, rows[id].value); err != nil {
				errch <- err

				// unblock all producers
				for _, queue := range queues {
					queue.Close()
				}
				return
			}
		}
	}
}

func forward(request *ScanRequest, queues []*Queue, donech chan bool, notifych chan bool, killch chan bool,
	errch chan error, cb EntryCallback) {

	defer close(donech)

	ensembleSize := len(queues)

	rows := make([]Row, ensembleSize)
	for i := 0; i < ensembleSize; i++ {
		rows[i].init(newAllocator(0, queues[i].GetBytesBuf()))
	}
	defer func() {
		for i := 0; i < ensembleSize; i++ {
			rows[i].freeBuffers()
		}
	}()

	for {
		if len(errch) != 0 {
			return
		}

		count := 0
		found := false
		for i := 0; i < ensembleSize; i++ {
			if queues[i].Peek(&rows[i]) {

				if rows[i].last {
					count++
					continue
				}

				found = true

				if queues[i].Dequeue(&rows[i]) {
					if err := cb(rows[i].key, rows[i].value); err != nil {
						errch <- err

						// unblock all producers
						for _, queue := range queues {
							queue.Close()
						}
						return
					}
				}
			}
		}

		if count == ensembleSize {
			return
		}

		if !found {
			select {
			case <-notifych:
				continue
			case <-killch:
				return
			}
		}
	}
}

func compareKey(request *ScanRequest, k1 *Row, k2 *Row) int {

	if request.isPrimary {
		return comparePrimaryKey(k1, k2)
	}

	return compareSecKey(k1, k2)
}

func comparePrimaryKey(k1 *Row, k2 *Row) int {

	return bytes.Compare(k1.key, k2.key)
}

func compareSecKey(k1 *Row, k2 *Row) int {

	return bytes.Compare(k1.key[:k1.len], k2.key[:k2.len])
}

func queueSize(partition int, sorted bool, cfg common.Config) (int, int) {

	size := cfg["scan.queue_size"].Int()
	limit := cfg["scan.notify_count"].Int()

	numCpu := runtime.GOMAXPROCS(0)

	if numCpu >= partition || !sorted {
		return size, limit
	}

	ratio := partition / numCpu
	return ratio * size, limit
}
