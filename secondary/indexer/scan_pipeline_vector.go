package indexer

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/collatejson"
	"github.com/couchbase/indexing/secondary/common"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	l "github.com/couchbase/indexing/secondary/logging"
	p "github.com/couchbase/indexing/secondary/pipeline"
	"github.com/couchbase/indexing/secondary/vector/codebook"
	n1qlval "github.com/couchbase/query/value"
)

var ErrScanWorkerStopped error = errors.New("scan workerd stopped")

type ScanJob struct {
	batch    int
	pid      common.PartitionId
	cid      int64
	scan     Scan
	snap     SliceSnapshot
	ctx      IndexReaderContext
	codebook codebook.Codebook
	doneCh   chan<- struct{}

	bytesRead    uint64
	rowsScanned  uint64
	rowsReturned uint64

	decodeDur int64
	decodeCnt int64

	distCmpDur int64
	distCmpCnt int64

	logPrefix string
	startTime time.Time
}

func NewScanJob(r *ScanRequest, batch int, pid common.PartitionId, cid int64, scan Scan, snap SliceSnapshot,
	ctx IndexReaderContext, cb codebook.Codebook) (job *ScanJob) {
	j := &ScanJob{
		batch:    batch,
		pid:      pid,
		cid:      cid,
		scan:     scan,
		snap:     snap,
		ctx:      ctx,
		codebook: cb,
	}
	j.logPrefix = fmt.Sprintf("%v[%v]ScanJob Batch(%v) Partn(%v) Centroid(%v) Scan[%s-%s]", r.LogPrefix, r.RequestId,
		j.batch, j.pid, j.cid, logging.TagUD(j.scan.Low), logging.TagUD(j.scan.High))
	j.doneCh = make(chan<- struct{})
	return j
}

func (j *ScanJob) SetStartTime() {
	if logging.IsEnabled(logging.Timing) {
		j.startTime = time.Now()
	}
}

func (j *ScanJob) PrintStats() {
	getDebugStr := func() string {
		s := fmt.Sprintf("%v stats rowsScanned: %v rowsReturned: %v", j.logPrefix, j.rowsScanned, j.rowsReturned)
		if logging.IsEnabled(logging.Timing) {
			s += fmt.Sprintf(" timeTaken: %v", time.Since(j.startTime))
		}
		return s
	}
	logging.LazyVerbosef("%v", getDebugStr)
}

// -----------
// Scan Worker
// -----------

// ScanWorker will scan a range, calculate distances, add distance
// and send data down the line. It gets jobs from the User on the
// workCh in Scanner goroutine. Scanner upon getting a ScanJob will
// launch Sender to process and send data downstream. Scanner will
// then continue to scan the data from the storage and send the same
// to sender using senderCh, Sender will do all the processing for the
// data it gets and send it downstream using outCh and if Sender get
// any error while processing it will send back to Scanner on senderErrCh.
// Scanner can then forward the same to user using errCh. Channel stopCh
// will be used by user to halt the ScanWorker and jobs its running.
type ScanWorker struct {
	id int
	r  *ScanRequest

	workCh <-chan *ScanJob // To get Job from outside
	stopCh <-chan struct{} // To get stop signal from outside

	// User of ScanWorker can stop listening to errCh after closing
	// the stopCh. So if stopCh is closed ignore sending errors.
	errCh chan<- error // To report error back
	// User of ScanWorker should wait till Sender routine for current
	// ScanJob halts before closing the outCh. Use can either use
	// doneCh in the ScanJob or jobsWg
	outCh chan<- *Row // To send output back

	// Sender vars
	currJob     *ScanJob
	senderCh    chan *Row
	senderErrCh chan error

	senderChSize    int // Channel size from storage scanner to sender
	senderBatchSize int // Batch size to process data in batches in sender

	// Job related vars
	jobsWg *sync.WaitGroup

	// Buffers
	buf    *[]byte
	mem    *allocator
	itrRow Row

	// VECTOR_TODO: handle rollback
	bytesRead    uint64
	rowsScanned  uint64
	rowsReturned uint64

	logPrefix string
	startTime time.Time
}

func NewScanWorker(id int, r *ScanRequest, workCh <-chan *ScanJob, outCh chan<- *Row,
	stopCh chan struct{}, errCh chan error, wg *sync.WaitGroup, senderChSize,
	senderBatchSize int) *ScanWorker {

	w := &ScanWorker{
		id:              id,
		r:               r,
		workCh:          workCh,
		outCh:           outCh,
		stopCh:          stopCh,
		jobsWg:          wg,
		errCh:           errCh,
		senderChSize:    senderChSize,
		senderBatchSize: senderBatchSize,
	}

	w.logPrefix = fmt.Sprintf("%v[%v]ScanWorker[%v]", r.LogPrefix, r.RequestId, id)
	w.SetStartTime()
	w.buf = r.getFromSecKeyBufPool() //Composite element filtering

	var m *allocator
	if v := w.r.connCtx.Get(fmt.Sprintf("%v%v", VectorScanWorker, id)); v == nil {
		bufPool := w.r.connCtx.GetVectorBufPool(id)
		m = newAllocator(int64(senderBatchSize), bufPool)
	} else {
		m = v.(*allocator)
	}
	w.mem = m

	go w.Scanner()

	return w
}

func (w *ScanWorker) Close() {
	w.r.connCtx.Put(fmt.Sprintf("%v%v", VectorScanWorker, w.id), w.mem)
	w.mem = nil
}

func (w *ScanWorker) SetStartTime() {
	if logging.IsEnabled(logging.Timing) {
		w.startTime = time.Now()
	}
}

func (w *ScanWorker) PrintStats() {
	getDebugStr := func() string {
		s := fmt.Sprintf("%v Stats rowsScanned: %v rowsReturned: %v",
			w.logPrefix, w.rowsScanned, w.rowsReturned)
		if logging.IsEnabled(logging.Timing) {
			s += fmt.Sprintf(" timeTaken: %v", time.Since(w.startTime))
		}
		return s
	}
	logging.LazyVerbosef("%v", getDebugStr)
}

func (w *ScanWorker) Scanner() {
	defer w.PrintStats()
	for {
		var job *ScanJob
		var ok bool

		select {
		case <-w.stopCh:
			return
		case job, ok = <-w.workCh:
			if !ok {
				return
			}
			w.currJob = job
			w.currJob.SetStartTime()
			logging.Tracef("%v received job: %+v", w.logPrefix, job)
			defer logging.Tracef("%v done with job: %+v", w.logPrefix, job)
		}

		w.senderCh = make(chan *Row, w.senderChSize)
		w.senderErrCh = make(chan error)
		go w.Sender()

		scan := job.scan
		snap := job.snap.Snapshot()
		ctx := job.ctx
		handler := w.scanIteratorCallback
		if w.r.isBhiveScan {
			handler = w.bhiveIteratorCallback
		}

		// VECTOR_TODO: Check if we can move this logic to another goroutine so that
		// this main goroutine is free for error handling and check if use of row.last
		// can be avoided
		var err error
		if scan.ScanType == AllReq {
			err = snap.All(ctx, handler)
		} else if scan.ScanType == RangeReq || scan.ScanType == FilterRangeReq {
			err = snap.Range(ctx, scan.Low, scan.High, scan.Incl, handler)
		} else {
			err = snap.Lookup(ctx, scan.Low, handler)
		}

		// Got error from storage layer or iterator callback
		if err != nil {
			// Send error out if we did not stop the worker
			if err != ErrScanWorkerStopped {
				select {
				case <-w.stopCh:
				case w.errCh <- err:
				}
			}
			close(w.senderCh) // Stop sender
			<-w.senderErrCh   // Wait for sender to finish
			w.unblockJobSender(job.doneCh)
			return
		}

		// Send last row and wait for sender
		var r Row
		r.last = true

		// If senderCh is blocked keep checking for error from sender
		var senderErr error
		select {
		case w.senderCh <- &r:
			// If last row is sent keep waiting for sender to get closed
			senderErr = <-w.senderErrCh
		case senderErr = <-w.senderErrCh:
		}

		if senderErr != nil {
			select {
			case <-w.stopCh:
			case w.errCh <- senderErr:
			}
			w.unblockJobSender(job.doneCh)
			close(w.senderCh)
			return
		}

		w.unblockJobSender(job.doneCh)
		close(w.senderCh)
	}
}

func (w *ScanWorker) unblockJobSender(doneCh chan<- struct{}) {
	if doneCh != nil {
		close(doneCh) // Mark the job done
	}
	if w.jobsWg != nil {
		w.jobsWg.Done()
	}
	w.currJob.PrintStats()
	w.currJob = nil
}

func (w *ScanWorker) Sender() {
	defer close(w.senderErrCh)
	defer func() {
		if r := recover(); r != nil {
			l.Fatalf("%v - panic(%v) detected while processing %s", w.logPrefix, r, w.r)
			l.Fatalf("%s", l.StackTraceAll())
			w.senderErrCh <- fmt.Errorf("%v panic %v", w.logPrefix, r)
		}
	}()

	batchSize := w.senderBatchSize
	rows := make([]*Row, batchSize)
	logging.Verbosef("%v ChannelSize: %v BatchSize: %v", w.logPrefix, cap(w.senderCh), batchSize)

	vectorDim := w.r.getVectorDim()

	codes := make([]byte, 0, batchSize*w.r.getVectorCodeSize())
	fvecs := make([]float32, batchSize*vectorDim)
	dists := make([]float32, batchSize)

	var err error
	var ok bool

	for {
		vecCount := 0
		lastRowReceived := false
		for ; vecCount < batchSize; vecCount++ {
			rows[vecCount], ok = <-w.senderCh
			if !ok {
				return
			}

			if rows[vecCount].last {
				lastRowReceived = true
				break // Finish the data gathered till ensemble
			}
		}

		// If we did not even get one valid Row before last
		if vecCount == 0 {
			return
		}

		// Make list of vectors to calculate distance
		for i := 0; i < vecCount; i++ {
			codei := rows[i].value
			codes = append(codes, codei...)
		}

		// Decode vectors
		t0 := time.Now()
		err = w.currJob.codebook.DecodeVectors(vecCount, codes, fvecs[:vecCount*vectorDim])
		if err != nil {
			logging.Verbosef("%v Sender got error: %v from DecodeVectors", w.logPrefix, err)
			w.senderErrCh <- err
			return
		}
		atomic.AddInt64(&w.currJob.decodeDur, int64(time.Now().Sub(t0)))
		atomic.AddInt64(&w.currJob.decodeCnt, int64(vecCount))

		// Compute distance from query vector using codebook
		t0 = time.Now()
		qvec := w.r.queryVector
		dists = dists[:vecCount]
		err = w.currJob.codebook.ComputeDistance(qvec, fvecs[:vecCount*vectorDim], dists)
		if err != nil {
			logging.Verbosef("%v Sender got error: %v from ComputeDistance", w.logPrefix, err)
			w.senderErrCh <- err
			return
		}
		atomic.AddInt64(&w.currJob.distCmpDur, int64(time.Now().Sub(t0)))
		atomic.AddInt64(&w.currJob.distCmpCnt, int64(vecCount))

		// Substitue distance in place centroidId and send to outCh
		for i := 0; i < vecCount; i++ {
			rows[i].dist = dists[i] // Add distance for sorting in heap

			select {
			case <-w.stopCh:
			case w.outCh <- rows[i]:
				w.rowsReturned++
				w.currJob.rowsReturned++
			}

			rows[i] = nil
		}

		if lastRowReceived {
			return
		}
		//re-init for the next batch
		codes = codes[:0]
		fvecs = fvecs[:0]
		dists = dists[:0]

	}
}

func (w *ScanWorker) scanIteratorCallback(entry, value []byte) error {

	var err error
	var skipRow bool

	w.rowsScanned++
	w.currJob.rowsScanned++
	w.bytesRead += uint64(len(entry))
	w.currJob.bytesRead += uint64(len(entry))
	if value != nil {
		w.bytesRead += uint64(len(value))
		w.currJob.bytesRead += uint64(len(value))
	}

	entry, err = w.reverseCollate(entry)
	if err != nil {
		logging.Verbosef("%v Sender got error: %v from reverseCollate", w.logPrefix, err)
		return err
	}

	skipRow, err = w.skipRow(entry)
	if err != nil {
		logging.Verbosef("%v Sender got error: %v from skipRow", w.logPrefix, err)
		return err
	}

	if skipRow {
		return nil
	}

	entry1 := secondaryIndexEntry(entry)

	w.itrRow.len = entry1.lenKey()
	w.itrRow.key = entry
	w.itrRow.value = value

	// VECTOR_TODO:
	// 1. Check if adding a Queue/CirularBuffer of Rows here in place of senderCh will help
	// 2. Check if having a sync.Pool of Row objects per connCtx will help
	var newRow Row
	newRow.init(w.mem)
	newRow.copy(&w.itrRow)

	select {
	case <-w.stopCh:
		return ErrScanWorkerStopped
	case err := <-w.senderErrCh:
		logging.Tracef("%v scanIteratorCallback got error: %v from Sender", w.logPrefix, err)
		return err
	case w.senderCh <- &newRow:
	}

	w.itrRow.len = 0
	w.itrRow.key = nil
	w.itrRow.value = nil

	return nil
}

func (w *ScanWorker) bhiveIteratorCallback(entry, value []byte) error {

	recordId, meta := w.currJob.snap.Snapshot().DecodeMeta(value)
	// Replace value with meta for now. Once include column support is added,
	// meta() has to be split into include column fields and quantized codes
	value = meta

	var r Row
	r.key = entry
	r.value = value
	r.recordId = recordId

	select {
	case <-w.stopCh:
		return ErrScanWorkerStopped
	case err := <-w.senderErrCh:
		logging.Tracef("%v scanIteratorCallback got error: %v from Sender", w.logPrefix, err)
		return err
	case w.senderCh <- &r:
	}

	w.rowsScanned++
	w.currJob.rowsScanned++
	w.bytesRead += uint64(len(entry) + 8) // 8 bytes for centroidId that are read and filtered out
	w.currJob.bytesRead += uint64(len(entry) + 8)
	if value != nil {
		w.bytesRead += uint64(len(value))
		w.currJob.bytesRead += uint64(len(value))
	}

	return nil
}

func (w *ScanWorker) reverseCollate(entry []byte) ([]byte, error) {
	//get the key in original format
	if !w.r.hasDesc() {
		return entry, nil
	}

	// VECTOR_TODO: Check if we need buffer pool here
	revbuf := make([]byte, 0)
	revbuf = append(revbuf, entry...)
	_, err := jsonEncoder.ReverseCollate(revbuf, w.r.IndexInst.Defn.Desc)
	if err != nil {
		return nil, err
	}

	return revbuf, nil
}

func (w *ScanWorker) skipRow(entry []byte) (skipRow bool, err error) {
	skipRow = false
	if w.currJob.scan.ScanType == FilterRangeReq {
		if len(entry) > cap(*w.buf) {
			*w.buf = make([]byte, 0, len(entry)+1024)
		}
		// VECTOR_TODO: Update this to filterScanRow2
		skipRow, _, err = filterScanRow(entry, w.currJob.scan, (*w.buf)[:0])
		if err != nil {
			return true, err
		}
	}
	if skipRow {
		return true, nil
	}
	return false, nil
}

// -----------
// Worker Pool
// -----------

// Pool of ScanWokers
type WorkerPool struct {
	jobs       chan *ScanJob  // Channel to send jobs to workers
	jobsWg     sync.WaitGroup // Wait group to wait for submitted jobs to finish
	errCh      chan error     // Channel to receive errros from workers
	numWorkers int            // Number of workers
	stopCh     chan struct{}  // Channel to halt workerpool and its workers
	stopOnce   sync.Once      // To execute stop only once upon external trigger or internal error
	workers    []*ScanWorker  // Array of workers
	sendCh     chan *Row      // Channel to send data out for downstream processing
	logPrefix  string
}

// NewWorkerPool creates a new WorkerPool
func NewWorkerPool(numWorkers int) *WorkerPool {
	wp := &WorkerPool{
		jobs:       make(chan *ScanJob, 0),
		jobsWg:     sync.WaitGroup{},
		errCh:      make(chan error),
		numWorkers: numWorkers,
		stopCh:     make(chan struct{}),
		workers:    make([]*ScanWorker, numWorkers),
	}
	return wp
}

func (wp *WorkerPool) Close() {
	for _, w := range wp.workers {
		w.Close()
	}
}

// Init starts the worker pool
func (wp *WorkerPool) Init(r *ScanRequest, scanWorkerSenderChSize, scanWorkerBatchSize int) {
	wp.sendCh = make(chan *Row, 20*wp.numWorkers)
	wp.logPrefix = fmt.Sprintf("%v[%v]WorkerPool", r.LogPrefix, r.RequestId)

	for i := 0; i < wp.numWorkers; i++ {
		wp.workers[i] = NewScanWorker(i, r, wp.jobs, wp.sendCh,
			wp.stopCh, wp.errCh, &wp.jobsWg, scanWorkerSenderChSize,
			scanWorkerBatchSize)
	}
}

// Submit adds a job to the pool
// Note: Submit and Wait cannot run concurrently but Stop can be run asynchronously
func (wp *WorkerPool) Submit(job *ScanJob) error {
	wp.jobsWg.Add(1) // If we are adding in case w.jobs <- job and if job is done before jobsWg.Add we will have negative counter
	select {
	case <-wp.stopCh:
		wp.jobsWg.Add(-1) // Job not submitted so revert the added delta. This is same as jobsWd.Done()
	case wp.jobs <- job:
	case err := <-wp.errCh:
		wp.jobsWg.Add(-1) // Job not submitted so revert the added delta
		logging.Verbosef("%v Submit: got error: %v", wp.logPrefix, err)
		wp.Stop()
		return err
	}
	return nil
}

// Wait waits for submitted jobs to finish or an error to occur
func (wp *WorkerPool) Wait() error {
	doneCh := make(chan struct{})
	go func() {
		wp.jobsWg.Wait()
		close(doneCh)
	}()
	select {
	case err := <-wp.errCh: // If any worker errors out
		if err != nil {
			logging.Verbosef("%v Wait: got error: %v", wp.logPrefix, err)
			wp.stopOnce.Do(func() {
				close(wp.stopCh)
			})
			<-doneCh         // Wait for running jobs to stop
			wp.sendLastRow() // Indicate downstream of early closure
			close(wp.sendCh) // Close downstream
			return err
		}
	case <-doneCh: // If current submitted jobs are done
		return nil
	case <-wp.stopCh: // If User stopped the worker pool
		<-doneCh
		return nil
	}
	return nil
}

// sendLastRow to indicate early closure to downstream i.e. to
// differentiate between closure on error and end
func (wp *WorkerPool) sendLastRow() {
	row := &Row{last: true}
	wp.sendCh <- row
}

// Stop signals all workers to stop and waits for workers to halt and closes
// downstream. Use Wait to wait till submitted jobs are done. Stop will halt
// the workers
func (wp *WorkerPool) Stop() {
	wp.stopOnce.Do(func() {
		close(wp.stopCh)
		wp.jobsWg.Wait()
		close(wp.sendCh)
	})
}

func (wp *WorkerPool) GetOutCh() <-chan *Row {
	return wp.sendCh
}

// -------------
// MergeOperator
// -------------

// Function to write data downstream from MergeOperator
type WriteItem func(...[]byte) error

// MergeOperator will read from recvQ, based on distances and either stores
// in heap or forwards row to down stream
type MergeOperator struct {
	req *ScanRequest

	recvCh <-chan *Row
	heap   *TopKRowHeap

	errCh    chan error
	mergerWg sync.WaitGroup

	writeItem WriteItem // WriteItem is expected to copy data downstream

	buf   *[]byte
	cktmp [][]byte

	rowsReceived    uint64
	rowsOffsetCount uint64

	logPrefix string
	startTime time.Time
}

func NewMergeOperator(recvCh <-chan *Row, r *ScanRequest, writeItem WriteItem) (
	fio *MergeOperator, err error) {

	fio = &MergeOperator{
		recvCh:    recvCh,
		writeItem: writeItem,
		req:       r,
		errCh:     make(chan error),
	}

	fio.logPrefix = fmt.Sprintf("%v[%v]MergeOperator", r.LogPrefix, r.RequestId)

	if r.useHeapForVectorIndex() {
		heapSize := r.Limit
		if r.Offset != 0 {
			heapSize += r.Offset
		}

		fio.heap, err = NewTopKRowHeap(int(heapSize), false)
		if err != nil {
			return nil, err
		}
	}

	fio.buf = r.getFromSecKeyBufPool()
	fio.cktmp = make([][]byte, len(r.IndexInst.Defn.SecExprs))

	fio.mergerWg.Add(1)
	go fio.Collector()

	return fio, nil
}

func (fio *MergeOperator) SetStartTime() {
	if logging.IsEnabled(logging.Timing) {
		fio.startTime = time.Now()
	}
}

func (fio *MergeOperator) PrintStats() {
	getDebugStr := func() string {
		s := fmt.Sprintf("%v stats rowsReceived: %v rowsOffsetCount: %v", fio.logPrefix,
			fio.rowsReceived, fio.rowsOffsetCount)
		if logging.IsEnabled(logging.Timing) {
			s += fmt.Sprintf(" timeTaken: %v", time.Since(fio.startTime))
		}
		return s
	}
	logging.LazyVerbosef("%v", getDebugStr)
}

func (fio *MergeOperator) substituteCentroidID(row *Row) error {

	vectorKeyPos := fio.req.getVectorKeyPos()

	// Substitute distance in place of centroid ID
	// 1. when projection is not pushed down (w.r.Indexprojection == nil )
	// 2. when we are projecting all keys (projectVectorDist)
	// 3. when vector field is being projected (projectVectorDist)
	if fio.req.isBhiveScan == false {
		distVal := n1qlval.NewValue(float64(row.dist))
		encodeBuf := make([]byte, 0, distVal.Size()*3)
		codec1 := collatejson.NewCodec(16)
		distCode, err := codec1.EncodeN1QLValue(distVal, encodeBuf)
		if err != nil && err.Error() == collatejson.ErrorOutputLen.Error() {
			distCode, err = encodeN1qlVal(distVal)
		}
		if err != nil {
			logging.Verbosef("%v Sender got error: %v from EncodeN1qlvalue", fio.logPrefix, err)
			return err
		}

		// VECTOR_TODO: Use buffer pools for these buffer allocations
		// VECTOR_TODO: Try to use fixed length encoding for float64 distance replacement with centroidID
		// ReplaceEncodedFieldInArray encodes distCode and replaces centroidId in key so add more buffer
		// for encoding of distCode incase it needs more space than centroidId => adding 3 * distCode size
		newBuf := make([]byte, 0, len(row.key)+(3*len(distCode)))
		codec2 := collatejson.NewCodec(16)
		newEntry, err := codec2.ReplaceEncodedFieldInArray(row.key, vectorKeyPos, distCode, newBuf)
		if err != nil {
			logging.Verbosef("%v Sender got error: %v from ReplaceEncodedFieldInArray key:%s pos:%v dist:%v",
				fio.logPrefix, err, logging.TagStrUD(row.key), vectorKeyPos, distCode)
			return err
		}
		row.key = newEntry // Replace old entry with centoidId to new entry with distance
	} else {

		// BHIVE scan requires special treatment as the row.key would be docid unlike a composite
		// vector index where row.key is CJSON encoded key + docID
		// Hence, we need to construct a new code and build a secondary entry

		docidLen := len(row.key) + 2 // 2 bytes extra for length encoding
		var distValArr []interface{}
		distVal := n1qlval.NewValue(float64(row.dist))
		distValArr = append(distValArr, distVal)

		encodeBuf := make([]byte, 0, distVal.Size()*3+uint64(docidLen))
		codec := collatejson.NewCodec(16)
		distCode, err := codec.EncodeN1QLValue(n1qlval.NewValue(distValArr), encodeBuf)
		if err != nil {
			distCode, err = encodeN1qlVal(n1qlval.NewValue(distValArr))
		}
		if err != nil {
			logging.Verbosef("%v Sender got error: %v from EncodeN1qlvalue", fio.logPrefix, err)
			return err
		}

		// VECTOR_TODO: This buffer is unnecessary and can be optimised out
		encodeBuf2 := make([]byte, 0, len(distCode)+docidLen)
		newEntry, err := NewSecondaryIndexEntry3(distCode, row.key, encodeBuf2)

		if err != nil {
			logging.Verbosef("%v Sender got error: %v from ReplaceEncodedFieldInArray key:%s pos:%v dist:%v",
				fio.logPrefix, err, logging.TagStrUD(row.key), vectorKeyPos, distCode)
			return err
		}
		row.key = newEntry
	}

	return nil
}

func (fio *MergeOperator) Collector() {
	defer fio.mergerWg.Done()
	fio.SetStartTime()
	defer fio.PrintStats()

	var row *Row
	var ok bool
	var err error

	projectDistance := fio.req.ProjectVectorDist()

	for {
		row, ok = <-fio.recvCh
		if !ok {
			break
		}

		// last row is send to indicate early closure
		if row.last {
			return
		}

		fio.rowsReceived++

		if fio.req.useHeapForVectorIndex() {
			fio.heap.Push(row)
			continue
		}

		// Substitue centroid ID
		if projectDistance {
			err := fio.substituteCentroidID(row)
			if err != nil {
				fio.errCh <- err
				return
			}
		}

		// If not get projected fields and send it down stream
		entry := row.key
		if fio.req.Indexprojection != nil && fio.req.Indexprojection.projectSecKeys {
			entry, err = projectKeys(nil, row.key, (*fio.buf)[:0], fio.req, fio.cktmp)
			if err != nil {
				logging.Verbosef("%v Collector got error: %v from projectKeys", fio.logPrefix, err)
				fio.errCh <- err
				return
			}
		}

		err = fio.writeItem(entry)
		if err != nil {
			logging.Verbosef("%v Collector got error: %v from writeItem", fio.logPrefix, err)
			fio.errCh <- err
			return
		}

		row.free() // Free the memory after sending data downstream writeItem is expected to make copy
	}

	if !fio.req.useHeapForVectorIndex() {
		return
	}

	if logging.IsEnabled(logging.Verbose) {
		fio.heap.PrintHeap()
	}

	// Read all elements from heap
	rowList := fio.heap.List()
	sortedRows := RowHeap{
		rows:  make([]*Row, 0),
		isMin: true,
	}
	sortedRows.rows = append(sortedRows.rows, rowList...)
	sort.Sort(sortedRows)

	// Read from Heap and get projected fields and send it down stream
	for _, row := range sortedRows.rows {

		// Substitue centroid ID
		if projectDistance {
			fio.substituteCentroidID(row)
			if err != nil {
				fio.heap.Destroy()
				fio.errCh <- err
				return
			}
		}

		entry := row.key
		if fio.req.Indexprojection != nil && fio.req.Indexprojection.projectSecKeys {
			entry, err = projectKeys(nil, row.key, (*fio.buf)[:0], fio.req, fio.cktmp)
			if err != nil {
				logging.Verbosef("%v Collector got error: %v from projectKeys from heap", fio.logPrefix, err)
				fio.heap.Destroy()
				fio.errCh <- err
				return
			}
		}

		if fio.req.Offset != 0 && fio.rowsOffsetCount < uint64(fio.req.Offset) {
			fio.rowsOffsetCount++
			continue
		}

		err = fio.writeItem(entry)
		if err != nil {
			logging.Verbosef("%v Collector got error: %v from writeItem from heap", fio.logPrefix, err)
			fio.heap.Destroy()
			fio.errCh <- err
			return
		}
	}

	fio.heap.Destroy()
}

func (fio *MergeOperator) Wait() error {
	doneCh := make(chan struct{})
	go func() {
		fio.mergerWg.Wait()
		close(doneCh)
	}()

	select {
	case err := <-fio.errCh:
		logging.Verbosef("%v got error: %v while waiting for MergeOperator to finish", fio.logPrefix, err)
		return err
	case <-doneCh:
		return nil
	}
}

// ----------------
// IndexScanSource2
// ----------------

type IndexScanSource2 struct {
	p.ItemWriter
	is IndexSnapshot
	p  *ScanPipeline

	parllelCIDScans int
}

func (s *IndexScanSource2) Routine() error {
	defer func() {
		if r := recover(); r != nil {
			logging.Fatalf("IndexScanSource2 - panic %v detected while processing %s", r, s.p.req)
			logging.Fatalf("%s", logging.StackTraceAll())
			panic(r)
		}
	}()

	var err error
	defer s.CloseWrite()

	// We can have multiple readers running on given snapshot i.e. snapshots can be
	// accessed concurrently so we need not duplicate snapshot for every reader.
	// As snap.Open() is done in NewSnapshotIterator ref counting is also taken care of
	snaps, err := GetSliceSnapshotsMap(s.is, s.p.req.PartitionIds)
	if err != nil {
		s.CloseWithError(err)
		return err
	}

	// How many readers per partition are configured and how may are needed
	readersPerPartition := s.p.req.parallelCentroidScans
	if s.p.req.nprobes < s.p.req.parallelCentroidScans {
		readersPerPartition = s.p.req.nprobes
	}

	// Make map of parition to reader contexts i.e. readers per parition
	ctxs := make(map[common.PartitionId][]IndexReaderContext)
	for i, pid := range s.p.req.PartitionIds {
		ctxs[pid] = make([]IndexReaderContext, 0)
		for j := 0; j < readersPerPartition; j++ {
			ctxs[pid] = append(ctxs[pid], s.p.req.Ctxs[i*readersPerPartition+j])
		}
	}

	// Scans and codebooks
	scans := s.p.req.vectorScans
	codebooks := s.p.req.codebookMap

	// Get values from config
	scanWorkerBatchSize := s.p.config["scan.vector.scanworker_batch_size"].Int()
	scanWorkerSenderChSize := s.p.config["scan.vector.scanworker_senderch_size"].Int()

	// Spawn Scan Workers
	wp := NewWorkerPool(readersPerPartition * len(s.p.req.PartitionIds))
	wp.Init(s.p.req, scanWorkerSenderChSize, scanWorkerBatchSize)
	defer wp.Close()

	wpOutCh := wp.GetOutCh() // Output of Workerpool is input of MergeOperator

	writeItemAddStat := func(itm ...[]byte) error {
		s.p.rowsReturned++
		return s.WriteItem(itm...)
	}

	// Spawn Merge Operator
	fanIn, err := NewMergeOperator(wpOutCh, s.p.req, writeItemAddStat)
	if err != nil { // Stop worker pool and return
		wp.Stop()
		s.CloseWithError(err)
		return err
	}

	// Make ScanJobs and schedule them on the WorkerPool
	// * readersPerPartition should be launched in parallel
	// * Run one scan on all partitions and then launch next one
	rem := s.p.req.nprobes % readersPerPartition
	numBatchesPerScan := s.p.req.nprobes / readersPerPartition
	if rem != 0 {
		numBatchesPerScan += 1
	}

	// Assigning BatchId to scan jobs in every paritition every batch will
	// have at max readersPerPartition * numPartitions number of jobs all the jobs
	// in the batch can be run in parallel. We have only readersPerPartition number
	// of readers for every partition so only that many readers can run in parallel
	// in one batch. We will be running the jobs batch wise one batch after other

	// VECTOR_TODO: Fix this scheduling algorithm. Here we must wait for slowest
	// scan in every batch to finish. If not all batches are equally sized this is
	// not optimal. We will to have schedule every job independently and hold semaphore
	// on number of readers per parition to ensure only a fixed number of scans per
	// partition are running in parallel at any point of time.
	jobMap := make(map[int][]*ScanJob, 0)
	maxBatchId := 0
	for _, pid := range s.p.req.PartitionIds {
		cidScans := scans[pid]
		c := 0
		for cid, scanList := range cidScans {
			for sid, scan := range scanList {
				batchId := sid*numBatchesPerScan + (c / readersPerPartition)
				if batchId > maxBatchId {
					maxBatchId = batchId
				}
				ctx := ctxs[pid][c%readersPerPartition]
				job := NewScanJob(s.p.req, batchId, pid, cid, scan, snaps[pid], ctx, codebooks[pid])
				_, ok := jobMap[batchId]
				if !ok {
					jobMap[batchId] = make([]*ScanJob, 0)
				}
				jobMap[batchId] = append(jobMap[batchId], job)
			}
			c++
		}
	}

	defer func() {
		for i := 0; i <= maxBatchId; i++ {
			for _, job := range jobMap[i] {
				logging.Verbosef("%v decode %v, dist %v, cnt %v, scanned %v", job.logPrefix,
					job.decodeDur, job.distCmpDur, job.decodeCnt, job.rowsScanned)
				s.p.rowsScanned += job.rowsScanned
				s.p.bytesRead += job.bytesRead
				s.p.decodeDur += job.decodeDur
				s.p.decodeCnt += job.decodeCnt
				s.p.distCmpDur += job.distCmpDur
				s.p.distCmpCnt += job.distCmpCnt
			}
		}
	}()

	var wpErr, fanInErr error
	go func() {
		defer wp.Stop()
		for i := 0; i <= maxBatchId; i++ {
			for _, job := range jobMap[i] {
				wpErr := wp.Submit(job)
				if wpErr != nil {
					return
				}
			}
			wpErr = wp.Wait()
			if wpErr != nil {
				return
			}
		}
	}()

	// Wait for Merge Operator to terminate it will happen either when we stopped worker pool
	fanInErr = fanIn.Wait()
	if fanInErr != nil { // If there is an error in FanIn stop worker pool
		wp.Stop()
		s.CloseWithError(fanInErr)
		return fanInErr
	}

	// If there was workerpool error causing early closure of Merger return error from worker pool
	if wpErr != nil {
		s.CloseWithError(wpErr)
		return wpErr
	}

	return nil
}

func NewScanPipeline2(req *ScanRequest, w ScanResponseWriter, is IndexSnapshot, cfg c.Config) *ScanPipeline {

	scanPipeline := new(ScanPipeline)
	scanPipeline.req = req
	scanPipeline.config = cfg

	src := &IndexScanSource2{is: is, p: scanPipeline}
	src.InitWriter()
	dec := &IndexScanDecoder{p: scanPipeline}
	dec.InitReadWriter()
	wr := &IndexScanWriter{w: w, p: scanPipeline}
	wr.InitReader()

	dec.SetSource(src)
	wr.SetSource(dec)

	scanPipeline.src = src
	scanPipeline.object.AddSource("source", src)
	scanPipeline.object.AddFilter("decoder", dec)
	scanPipeline.object.AddSink("writer", wr)

	return scanPipeline
}
