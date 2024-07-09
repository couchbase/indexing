package indexer

import (
	"errors"
	"fmt"
	"sync"

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

	// Job related vars
	jobsWg *sync.WaitGroup

	// Buffers
	buf *[]byte

	// VECTOR_TODO: handle stats and rollback
	bytesRead    uint64
	rowsScanned  uint64
	rowsReturned uint64
}

func NewScanWorker(id int, r *ScanRequest, workCh <-chan *ScanJob, outCh chan<- *Row,
	stopCh chan struct{}, errCh chan error, wg *sync.WaitGroup) *ScanWorker {
	w := &ScanWorker{
		id:     id,
		r:      r,
		workCh: workCh,
		outCh:  outCh,
		stopCh: stopCh,
		jobsWg: wg,
		errCh:  errCh,
	}

	w.senderCh = make(chan *Row, 20)

	w.buf = r.getFromSecKeyBufPool() //Composite element filtering

	go w.Scanner()

	return w
}

func (w *ScanWorker) Scanner() {
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
		}

		w.senderErrCh = make(chan error)
		go w.Sender()

		scan := job.scan
		snap := job.snap.Snapshot()
		ctx := job.ctx
		handler := w.scanIteratorCallback

		var err error
		if scan.ScanType == AllReq {
			err = snap.All(ctx, handler)
		} else if scan.ScanType == RangeReq || scan.ScanType == FilterRangeReq {
			err = snap.Range(ctx, scan.Low, scan.High, scan.Incl, handler)
		} else {
			err = fmt.Errorf("invalid scan type received at scan worker %v", scan.ScanType)
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
		w.senderCh <- &r
		senderErr := <-w.senderErrCh
		if senderErr != nil {
			select {
			case <-w.stopCh:
			case w.errCh <- senderErr:
			}
			w.unblockJobSender(job.doneCh)
			return
		}

		w.unblockJobSender(job.doneCh)
	}
}

func (w *ScanWorker) unblockJobSender(doneCh chan<- struct{}) {
	if doneCh != nil {
		close(doneCh) // Mark the job done
	}
	if w.jobsWg != nil {
		w.jobsWg.Done()
	}
}

func (w *ScanWorker) Sender() {
	defer close(w.senderErrCh)

	batchSize := 1
	rows := make([]*Row, batchSize)

	var err error
	var ok bool

	for {
		for i := 0; i < batchSize; {
			rows[i], ok = <-w.senderCh
			if !ok {
				return
			}

			if rows[i].last {
				break // Finish the data gathered till ensemble
			}

			rows[i].key, err = w.reverseCollate(rows[i].key)
			if err != nil {
				w.senderErrCh <- err
				return
			}

			skipRow, err := w.skipRow(rows[i].key)
			if err != nil {
				w.senderErrCh <- err
				return
			}
			if !skipRow {
				i++
			}
		}

		// If we did not even get one valid Row before last
		if rows[0].last {
			return
		}

		// Make list of vectors to calculate distance
		fvecs := make([]float32, 0)
		vecCount := 0
		for ; vecCount < batchSize && !rows[vecCount].last; vecCount++ {

			codei := rows[vecCount].value

			veci := make([]float32, w.r.getVectorDim())
			// VECTOR_TODO: Update to DecodeVectors
			err = w.currJob.codebook.DecodeVector(codei, veci)
			if err != nil {
				w.senderErrCh <- err
				return
			}

			fvecs = append(fvecs, veci...)
		}

		// Compute distance from query vector using codebook
		qvec := w.r.queryVector
		dists := make([]float32, vecCount)
		err = w.currJob.codebook.ComputeDistance(qvec, fvecs, dists)
		if err != nil {
			w.senderErrCh <- err
			return
		}

		// Substitue distance in place centroidId and send to outCh
		// VECTOR_TODO: Check if we can discard moving value to next stage
		codec := collatejson.NewCodec(16)
		vectorKeyPos := w.r.getVectorKeyPos()
		i := 0
		for ; i < batchSize && !rows[i].last; i++ {
			// Substitute distance in place of centroid ID.
			// VECTOR_TODO: Do this only when you need to project distance field
			distVal := n1qlval.NewValue(float64(dists[i]))
			encodeBuf := make([]byte, 0, distVal.Size()*3)
			distCode, err := codec.EncodeN1QLValue(distVal, encodeBuf)
			if err != nil && err.Error() == collatejson.ErrorOutputLen.Error() {
				distCode, err = encodeN1qlVal(distVal)
			}
			if err != nil {
				w.senderErrCh <- err
				return
			}

			// VECTOR_TODO: Use buffer pools for these buffer allocations
			// VECTOR_TODO: Try to use fixed length encoding for float64 distance replacement with centroidID
			newBuf := make([]byte, 0, len(rows[i].key)+len(distCode))
			newEntry, err := codec.ReplaceEncodedFieldInArray(rows[i].key, vectorKeyPos, distCode, newBuf)
			if err != nil {
				w.senderErrCh <- err
				return
			}

			rows[i].key = newEntry  // Replace old entry with centoidId to new entry with distance
			rows[i].dist = dists[i] // Add distance for sorting in heap

			select {
			case <-w.stopCh:
			case w.outCh <- rows[i]:
			}
		}

		if i > 0 && i < batchSize && rows[i].last {
			return
		}
	}
}

func (w *ScanWorker) scanIteratorCallback(entry, value []byte) error {
	select {
	case <-w.stopCh:
		return ErrScanWorkerStopped
	case err := <-w.senderErrCh:
		return err
	default:
	}

	w.rowsScanned++

	entry1 := secondaryIndexEntry(entry)

	var r Row
	r.key = entry
	r.value = value
	r.len = entry1.lenKey()

	// Scan and send data to sender
	w.senderCh <- &r

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

// Init starts the worker pool
func (wp *WorkerPool) Init(r *ScanRequest) {
	wp.sendCh = make(chan *Row, 20*wp.numWorkers)

	for i := 0; i < wp.numWorkers; i++ {
		wp.workers[i] = NewScanWorker(i, r, wp.jobs, wp.sendCh,
			wp.stopCh, wp.errCh, &wp.jobsWg)
	}
}

// Submit adds a job to the pool
// Note: Submit and Wait cannot run concurrently but Stop can be run asynchronously
func (wp *WorkerPool) Submit(job *ScanJob) error {
	select {
	case <-wp.stopCh:
	case wp.jobs <- job:
		wp.jobsWg.Add(1)
	case err := <-wp.errCh:
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

	writeItem WriteItem

	buf   *[]byte
	cktmp [][]byte

	// VECTOR_TODO: handle stats
	bytesRead    uint64
	rowsScanned  uint64
	rowsReturned uint64
}

func NewMergeOperator(recvCh <-chan *Row, r *ScanRequest, writeItem WriteItem) (
	fio *MergeOperator, err error) {

	fio = &MergeOperator{
		recvCh:    recvCh,
		writeItem: writeItem,
		req:       r,
	}

	if r.useHeapForVectorIndex() {
		fio.heap, err = NewTopKRowHeap(int(r.Limit), false)
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

func (fio *MergeOperator) Collector() {
	defer fio.mergerWg.Done()

	var row *Row
	var ok bool
	var err error

	for {
		row, ok = <-fio.recvCh
		if !ok {
			break
		}

		// last row is send to indicate early closure
		if row.last {
			return
		}

		// VECTOR_TODO: Add OFFSET too to the LIMIT, Check if LIMIT is adjusted at client side considering OFFSET
		// If Limit is pushed down push it to heap
		if fio.req.useHeapForVectorIndex() {
			fio.heap.Push(row)
			continue
		}

		// If not get projected fields and send it down stream
		entry := row.key
		if fio.req.Indexprojection != nil && fio.req.Indexprojection.projectSecKeys {
			entry, err = projectKeys(nil, row.key, (*fio.buf)[:0], fio.req, fio.cktmp)
			if err != nil {
				fio.errCh <- err
				return
			}
		}

		err = fio.writeItem(entry)
		if err != nil {
			fio.errCh <- err
			return
		}

	}

	if !fio.req.useHeapForVectorIndex() {
		return
	}

	if logging.IsEnabled(logging.Verbose) {
		fio.heap.PrintHeap()
	}

	// Read from Heap and get projected fields and send it down stream
	for row = fio.heap.Pop(); row != nil; row = fio.heap.Pop() {
		entry := row.key
		if fio.req.Indexprojection != nil && fio.req.Indexprojection.projectSecKeys {
			entry, err = projectKeys(nil, row.key, (*fio.buf)[:0], fio.req, fio.cktmp)
			if err != nil {
				fio.heap.Destroy()
				fio.errCh <- err
				return
			}
		}

		err = fio.writeItem(entry)
		if err != nil {
			fio.heap.Destroy()
			fio.errCh <- err
			return
		}
	}
}

func (fio *MergeOperator) Wait() error {
	doneCh := make(chan struct{})
	go func() {
		fio.mergerWg.Wait()
		close(doneCh)
	}()

	select {
	case err := <-fio.errCh:
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
			l.Fatalf("IndexScanSource2 - panic %v detected while processing %s", r, s.p.req)
			l.Fatalf("%s", l.StackTraceAll())
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

	// Spawn Scan Workers
	wp := NewWorkerPool(readersPerPartition * len(s.p.req.PartitionIds))
	wp.Init(s.p.req)
	wpOutCh := wp.GetOutCh() // Output of Workerpool is input of MergeOperator

	// Spawn Merge Operator
	fanIn, err := NewMergeOperator(wpOutCh, s.p.req, s.WriteItem)
	if err != nil { // Stop worker pool and return
		wp.Stop()
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
				job := &ScanJob{
					batch:    batchId,
					pid:      pid,
					cid:      cid,
					scan:     scan,
					snap:     snaps[pid],
					codebook: codebooks[pid],
					ctx:      ctxs[pid][c%readersPerPartition],
				}
				_, ok := jobMap[batchId]
				if !ok {
					jobMap[batchId] = make([]*ScanJob, 0)
				}
				jobMap[batchId] = append(jobMap[batchId], job)
			}
			c++
		}
	}

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
		return fanInErr
	}

	// If there was workerpool error causing early closure of Merger return error from worker pool
	if wpErr != nil {
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
