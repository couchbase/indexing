package indexer

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/indexing/secondary/collatejson"
	"github.com/couchbase/indexing/secondary/common"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/conversions"
	"github.com/couchbase/indexing/secondary/logging"
	l "github.com/couchbase/indexing/secondary/logging"
	p "github.com/couchbase/indexing/secondary/pipeline"
	"github.com/couchbase/indexing/secondary/vector/codebook"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"
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

	coarseSize int

	bytesRead    uint64
	rowsScanned  uint64
	rowsReturned uint64
	// when inline filter is pushed down, not all rows come to scan pipeline
	// rowsFiltered captures those rows that have been skipped by storage
	// from further processing. num_rows_scanned + num_rows_filtered will
	// give the total rows that are processed in the scan pipeline
	rowsFiltered uint64

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
	j.coarseSize = r.getVectorCoarseSize()
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
		s := fmt.Sprintf("%v stats rowsScanned: %v, rowsReturned: %v, rowsFiltered: %v",
			j.logPrefix, j.rowsScanned, j.rowsReturned, j.rowsFiltered)
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

	config c.Config

	// Sender vars
	currJob     *ScanJob
	senderCh    chan *Row
	senderErrCh chan error

	senderChSize    int // Channel size from storage scanner to sender
	senderBatchSize int // Batch size to process data in batches in sender

	// Job related vars
	jobsWg            *sync.WaitGroup
	sendLastRowPerJob bool

	// Buffers
	buf    *[]byte
	mem    *allocator
	itrRow Row

	bytesRead    uint64
	rowsScanned  uint64
	rowsReturned uint64

	// VECTOR_TODO: Add cache entry hits and misses
	cacheEntryHits uint64
	cacheEntryMiss uint64

	logPrefix string
	startTime time.Time

	vectorDim int
	heapSize  int
	codeSize  int

	//local heap for each worker
	heap *TopKRowHeap

	//reference to the current batch rows
	currBatchRows []*Row

	//temporary buffers to process each batch
	codes []byte
	fvecs []float32
	dists []float32

	cktmp [][]byte

	dtable []float32

	rowBuf *AtomicRowBuffer

	// temporary buffer to process each include column
	includeColumnBuf     []byte
	includeColumnExplode []bool
	includeColumnDecode  []bool
	includeColumncktemp  [][]byte
	includeColumndktemp  value.Values
	svPool               *value.StringValuePoolForIndex
	includeColumnLen     int
	explodeUpto          int

	incBuf   *[]byte
	inccktmp [][]byte

	//For caching values
	cv          *value.ScopeValue
	av          value.AnnotatedValue
	exprContext expression.Context
}

func NewScanWorker(id int, r *ScanRequest, workCh <-chan *ScanJob, outCh chan<- *Row,
	stopCh chan struct{}, errCh chan error, wg *sync.WaitGroup, config c.Config,
	sendLastRowPerJob bool) *ScanWorker {

	senderChSize := config["scan.vector.scanworker_senderch_size"].Int()

	w := &ScanWorker{
		id:                   id,
		r:                    r,
		workCh:               workCh,
		outCh:                outCh,
		stopCh:               stopCh,
		jobsWg:               wg,
		errCh:                errCh,
		senderChSize:         senderChSize,
		includeColumnExplode: make([]bool, len(r.IndexInst.Defn.Include)),
		includeColumnDecode:  make([]bool, len(r.IndexInst.Defn.Include)),
		includeColumncktemp:  make([][]byte, len(r.IndexInst.Defn.Include)),
		includeColumndktemp:  make(n1qlval.Values, len(r.IndexInst.Defn.Include)),
		sendLastRowPerJob:    sendLastRowPerJob,
		includeColumnLen:     len(r.IndexInst.Defn.Include),
		cktmp:                make([][]byte, len(r.IndexInst.Defn.SecExprs)),
		config:               config,
	}

	if w.includeColumnLen > 0 && r.indexOrder != nil && r.indexOrder.IsSortKeyNeeded() {
		w.incBuf = r.getFromSecKeyBufPool()
		w.inccktmp = make([][]byte, len(r.IndexInst.Defn.Include))
	}

	//init temp buffers
	w.vectorDim = r.getVectorDim()
	w.codeSize = r.getVectorCodeSize()

	bufferInitBatchSize := w.getBufferInitBatchSize()

	w.currBatchRows = make([]*Row, 0, bufferInitBatchSize)
	w.codes = make([]byte, 0, bufferInitBatchSize*r.getVectorCodeSize())
	w.fvecs = make([]float32, bufferInitBatchSize*r.getVectorDim())
	w.dists = make([]float32, bufferInitBatchSize)

	for i := len(r.IndexInst.Defn.SecExprs); i < len(r.IndexInst.Defn.SecExprs)+len(r.IndexInst.Defn.Include); i++ {
		index := i - len(r.IndexInst.Defn.SecExprs)
		if r.indexKeyNames != nil && r.indexKeyNames[i] != "" {
			w.includeColumnExplode[index] = true
			w.includeColumnDecode[index] = true
			w.explodeUpto = index
		}
	}

	//Use local heap for limit pushdown case. Each scan worker maintains
	//its own local heap as only limit(topK) number of rows are required to be
	//evaluated for final aggregation of results. Rest of the rows can be
	//discarded.
	if r.useHeapForVectorIndex() {
		//VECTOR_TODO add safety check to not init heap if limit is too high
		//VECTOR_TODO handle error
		w.heapSize = int(r.getLimit())
		if r.Offset != 0 {
			w.heapSize += int(r.Offset)
		}
		w.heap, _ = NewTopKRowHeap(w.heapSize, false, r.getRowCompare())

		//pre-allocate rows twice the size of buffer+heapSize
		w.rowBuf = NewAtomicRowBuffer((bufferInitBatchSize + w.heapSize) * 2)
	}

	w.logPrefix = fmt.Sprintf("%v[%v]ScanWorker[%v]", r.LogPrefix, r.RequestId, id)
	w.SetStartTime()
	w.buf = r.getFromSecKeyBufPool() //Composite element filtering

	if w.r.connCtx != nil {
		var m *allocator
		if v := w.r.connCtx.Get(fmt.Sprintf("%v%v", VectorScanWorker, id)); v == nil {
			bufPool := w.r.connCtx.GetVectorBufPool(id)
			m = newAllocator(int64(bufferInitBatchSize), bufPool)
		} else {
			m = v.(*allocator)
		}
		w.mem = m
	}

	w.cv = value.NewScopeValue(make(map[string]interface{}), nil)
	w.av = value.NewAnnotatedValue(w.cv)
	w.exprContext = expression.NewIndexContext()
	w.svPool = value.NewStringValuePoolForIndex(len(r.IndexInst.Defn.Include) + 1) // +1 is required for meta().id

	logging.Tracef("%v bufferInitBatchSize: %v", w.logPrefix, bufferInitBatchSize)

	go w.Scanner()

	return w
}

// This function calculates the batch size which should be used for initializing temp buffers
// based on the quantization and similarity metric.
func (w *ScanWorker) getBufferInitBatchSize() int {

	batchSize := w.config["scan.vector.scanworker_batch_size"].Int()
	largeBatchSize := w.config["scan.vector.scanworker_large_batch_size"].Int()

	qtype := w.r.IndexInst.Defn.VectorMeta.Quantizer.Type
	if qtype == c.SQ {
		return batchSize
	}
	similarity := w.r.IndexInst.Defn.VectorMeta.Similarity
	metric, _ := codebook.ConvertSimilarityToMetric(similarity)
	if metric != codebook.METRIC_L2 {
		return batchSize
	}

	//use a large batch size for PQ + L2
	if qtype == c.PQ && metric == codebook.METRIC_L2 {
		return largeBatchSize
	}
	return batchSize
}

func (w *ScanWorker) setSenderBatchSize() {

	batchSize := w.config["scan.vector.scanworker_batch_size"].Int()
	largeBatchSize := w.config["scan.vector.scanworker_large_batch_size"].Int()

	qtype := w.r.IndexInst.Defn.VectorMeta.Quantizer.Type
	if qtype == c.SQ {
		w.senderBatchSize = batchSize
		return
	}

	//use regular batch size for bhive, as it doesn't
	//benefit from larger batch size
	if w.r.isBhiveScan {
		w.senderBatchSize = batchSize
		return
	}

	similarity := w.r.IndexInst.Defn.VectorMeta.Similarity
	metric, _ := codebook.ConvertSimilarityToMetric(similarity)
	if metric != codebook.METRIC_L2 {
		w.senderBatchSize = batchSize
		return
	}

	//Use a large batch size for PQ + L2 for a single centroid scan.
	//Such scans reuse the distance table and large batch size reduces the
	//overheads associated with each batch processing as less number of
	//such calls need to be made to the faiss library.
	if !w.currJob.scan.MultiCentroid &&
		qtype == c.PQ &&
		metric == codebook.METRIC_L2 &&
		!w.r.IndexInst.Defn.VectorMeta.Quantizer.FastScan {
		w.senderBatchSize = largeBatchSize
	} else {
		w.senderBatchSize = batchSize
	}

}

func (w *ScanWorker) Close() {
	w.r.connCtx.Put(fmt.Sprintf("%v%v", VectorScanWorker, w.id), w.mem)
	w.mem = nil

	w.buf = nil

	w.currBatchRows = nil
	w.codes = nil
	w.fvecs = nil
	w.dists = nil
	w.cktmp = nil
	w.dtable = nil

	w.includeColumnBuf = nil
	w.includeColumnExplode = nil
	w.includeColumnDecode = nil
	w.includeColumncktemp = nil
	w.includeColumndktemp = nil

	w.heap = nil
	w.rowBuf = nil
	w.svPool = nil
	w.cv = nil
	w.av = nil
	w.exprContext = nil
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
	defer func() {
		if r := recover(); r != nil {
			l.Fatalf("%v - panic(%v) detected in scanner while processing %s", w.logPrefix, r, w.r)
			l.Fatalf("%s", l.StackTraceAll())

			select {
			case <-w.stopCh:
			case w.errCh <- fmt.Errorf("%v - panic(%v) detected in scanner while processing %s", w.logPrefix, r, w.r):
			}

			if w.currJob != nil {
				w.finishJob()
			}
		}
	}()

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
		w.heap, _ = NewTopKRowHeap(w.heapSize, false, w.r.getRowCompare())

		scan := job.scan
		snap := job.snap.Snapshot()
		ctx := job.ctx
		handler := w.scanIteratorCallback
		if w.r.isBhiveScan {
			handler = w.bhiveIteratorCallback
		}

		fincb := w.finishCallback

		//set batch size for each scan job
		w.setSenderBatchSize()
		logging.Tracef("%v senderBatchSize: %v", w.logPrefix, w.senderBatchSize)

		// VECTOR_TODO: Check if we can move this logic to another goroutine so that
		// this main goroutine is free for error handling and check if use of row.last
		// can be avoided
		var err error
		if scan.ScanType == AllReq {
			err = snap.All(ctx, handler, fincb)
		} else if scan.ScanType == RangeReq || scan.ScanType == FilterRangeReq {
			err = snap.Range(ctx, scan.Low, scan.High, scan.Incl, handler, fincb)
		} else if w.r.isBhiveScan {
			// Create copies of scan.Low and scan.High for Range2 calls
			lowBytes := bhiveCentroidId(append([]byte(nil), scan.Low.Bytes()...))
			highBytes := bhiveCentroidId(append([]byte(nil), scan.High.Bytes()...))

			if w.r.includeColumnFilters != nil {
				err = snap.Range2(ctx, lowBytes, highBytes, scan.Incl, w.r.Limit, w.r.topNScan, handler, fincb, w.inlineFilterCb2)
			} else if w.r.inlineFilterExpr != nil {
				err = snap.Range2(ctx, lowBytes, highBytes, scan.Incl, w.r.Limit, w.r.topNScan, handler, fincb, w.inlineFilterCb)
			} else {
				err = snap.Range2(ctx, lowBytes, highBytes, scan.Incl, w.r.Limit, w.r.topNScan, handler, fincb, nil)
			}
		} else {
			err = snap.Lookup(ctx, scan.Low, handler, fincb)
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
		}
		w.finishJob()
	}
}

// finishJob marks the job as finished. There are currently 2 mechanisms to
// track a job completion - a. job.doneCh b. jobsWg.
func (w *ScanWorker) finishJob() {
	if w.sendLastRowPerJob {
		select {
		case <-w.stopCh:
		case w.outCh <- &Row{last: true, workerId: w.id}:
		}
	}
	if w.currJob.doneCh != nil {
		close(w.currJob.doneCh) // Mark the job done
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
	logging.Verbosef("%v ChannelSize: %v BatchSize: %v", w.logPrefix, cap(w.senderCh), batchSize)

	var err error
	var ok bool

	for {
		vecCount := 0
		lastRowReceived := false
		for ; vecCount < batchSize; vecCount++ {
			w.currBatchRows[vecCount], ok = <-w.senderCh
			if !ok {
				return
			}

			if w.currBatchRows[vecCount].last {
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
			codei := w.currBatchRows[i].value
			w.codes = append(w.codes, codei...)
		}

		// Decode vectors
		t0 := time.Now()
		err = w.currJob.codebook.DecodeVectors(vecCount, w.codes, w.fvecs[:vecCount*w.vectorDim])
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
		w.dists = w.dists[:vecCount]
		err = w.currJob.codebook.ComputeDistance(qvec, w.fvecs[:vecCount*w.vectorDim], w.dists)
		if err != nil {
			logging.Verbosef("%v Sender got error: %v from ComputeDistance", w.logPrefix, err)
			w.senderErrCh <- err
			return
		}
		atomic.AddInt64(&w.currJob.distCmpDur, int64(time.Now().Sub(t0)))
		atomic.AddInt64(&w.currJob.distCmpCnt, int64(vecCount))

		// Substitue distance in place centroidId and send to outCh
		for i := 0; i < vecCount; i++ {
			w.currBatchRows[i].dist = w.dists[i] // Add distance for sorting in heap
			w.currBatchRows[i].workerId = w.id
			select {
			case <-w.stopCh:
			case w.outCh <- w.currBatchRows[i]:
				w.rowsReturned++
				w.currJob.rowsReturned++
			}

			w.currBatchRows[i] = nil
		}

		if lastRowReceived {
			return
		}

		//re-init for the next batch
		w.codes = w.codes[:0]
		w.fvecs = w.fvecs[:0]
		w.dists = w.dists[:0]

	}
}

// flushLocalHeap makes a copy of the rows in the local heap and
// sends it to the next stage of the scan pipeline.
func (w *ScanWorker) flushLocalHeap() {

	logging.Verbosef("%v flushLocalHeap %v %v", w.logPrefix, w.currJob.batch, w.currJob.pid)
	rowList := w.heap.List()

	for _, row := range rowList {

		//create a copy of row before sending it out
		var newRow Row

		newRow.init(w.mem)
		if w.r.isBhiveScan {
			newRow.copyForBhive(row)
		} else {
			newRow.copy(row)

			entry1 := secondaryIndexEntry(row.key)
			newRow.len = entry1.lenKey()
		}
		newRow.workerId = w.id

		select {
		case <-w.stopCh:
		case w.outCh <- &newRow:
			w.rowsReturned++
			w.currJob.rowsReturned++
		}
	}

}

// finishCallback is called by storage before iterator gets closed. After
// this point, any allocation related to iterator could get freed up.
// This callback allows the caller to do any final processing before iterator
// close. This is useful in case where scan pipeline is not making copy of
// the rows returned by storage iterator. During the callback, any rows
// which are required for downstream processing can be copied or any remaining
// rows in a batch can be processed.
func (w *ScanWorker) finishCallback() {

	logging.Verbosef("%v finishCallback %v %v", w.logPrefix, w.currJob.batch, w.currJob.pid)
	err := w.processCurrentBatch()
	if err != nil {
		select {
		case <-w.stopCh:
		case w.errCh <- err:
		}
	}
	//flush the local heap once done
	if w.r.useHeapForVectorIndex() {
		w.flushLocalHeap()
		w.heap.Destroy()
	}

}

// processCurrentBatch decodes current batch of rows, computes
// the distance from query vector and either stores in local heap(limit pushdown)
// or sends it to the next stage of scan pipeline.
func (w *ScanWorker) processCurrentBatch() (err error) {

	defer func() {
		if r := recover(); r != nil {
			l.Fatalf("%v - panic(%v) detected while processing %s", w.logPrefix, r, w.r)
			l.Fatalf("%s", l.StackTraceAll())
			err = fmt.Errorf("processCurrentBatch panic occurred: %v", r)
		}
	}()

	logging.Verbosef("%v processCurrentBatch %v %v %v", w.logPrefix, w.currJob.batch, w.currJob.pid, w.currJob.cid)

	if len(w.currBatchRows) == 0 {
		return
	}

	vecCount := len(w.currBatchRows)

	metric := w.currJob.codebook.MetricType()

	useDistEncoded := func() bool {

		if w.r.isBhiveScan {
			//keep in sync with bhive_slice.go useDistEncoded
			return w.r.IndexInst.Defn.VectorMeta.Quantizer.Type == common.SQ &&
				metric == codebook.METRIC_L2
		} else {
			return !w.currJob.scan.MultiCentroid &&
				metric == codebook.METRIC_L2 &&
				!w.r.IndexInst.Defn.VectorMeta.Quantizer.FastScan
		}
	}()

	//ComputeDistanceEncoded is only implemented for SQ currently. It can only
	//be used if all vectors in a batch belong to the same centroid. If cid < 0,
	//it implies that scan is spanning across centroids.
	if useDistEncoded {
		// Make list of vectors to calculate distance
		for i := 0; i < vecCount; i++ {
			codei := w.currBatchRows[i].value
			codei = codei[w.currJob.coarseSize:] //strip coarse code(i.e. centroidID)
			w.codes = append(w.codes, codei...)
		}
		t0 := time.Now()

		qvec := w.r.queryVector
		qtype := w.r.IndexInst.Defn.VectorMeta.Quantizer.Type
		if qtype == c.PQ {
			if w.dtable == nil {
				subQuantizers := w.r.IndexInst.Defn.VectorMeta.Quantizer.SubQuantizers
				nbits := w.r.IndexInst.Defn.VectorMeta.Quantizer.Nbits
				w.dtable = make([]float32, subQuantizers*(1<<nbits))
				err = w.currJob.codebook.ComputeDistanceTable(qvec, w.dtable)
				if err != nil {
					logging.Verbosef("%v Sender got error: %v from ComputeDistanceTable", w.logPrefix, err)
					return
				}
			}
		}

		w.dists = w.dists[:vecCount]
		err = w.currJob.codebook.ComputeDistanceEncoded(qvec, vecCount, w.codes,
			w.dists, w.dtable, w.currJob.cid)
		if err != nil {
			logging.Verbosef("%v Sender got error: %v from ComputeDistance", w.logPrefix, err)
			return
		}
		atomic.AddInt64(&w.currJob.distCmpDur, int64(time.Now().Sub(t0)))
		atomic.AddInt64(&w.currJob.distCmpCnt, int64(vecCount))
	} else {
		// Make list of vectors to calculate distance
		for i := 0; i < vecCount; i++ {
			codei := w.currBatchRows[i].value
			w.codes = append(w.codes, codei...)
		}
		// Decode vectors
		t0 := time.Now()
		err = w.currJob.codebook.DecodeVectors(vecCount, w.codes, w.fvecs[:vecCount*w.vectorDim])
		if err != nil {
			logging.Verbosef("%v Sender got error: %v from DecodeVectors", w.logPrefix, err)
			return
		}
		atomic.AddInt64(&w.currJob.decodeDur, int64(time.Now().Sub(t0)))
		atomic.AddInt64(&w.currJob.decodeCnt, int64(vecCount))

		// Compute distance from query vector using codebook
		t0 = time.Now()
		qvec := w.r.queryVector
		w.dists = w.dists[:vecCount]
		err = w.currJob.codebook.ComputeDistance(qvec, w.fvecs[:vecCount*w.vectorDim], w.dists)
		if err != nil {
			logging.Verbosef("%v Sender got error: %v from ComputeDistance", w.logPrefix, err)
			return
		}

		atomic.AddInt64(&w.currJob.distCmpDur, int64(time.Now().Sub(t0)))
		atomic.AddInt64(&w.currJob.distCmpCnt, int64(vecCount))

	}
	// Substitue distance in place centroidId and send to outCh or store in local heap
	for i := 0; i < vecCount; i++ {
		w.currBatchRows[i].dist = w.dists[i] // Add distance for sorting in heap

		var sortKey []byte
		if w.r.indexOrder != nil && w.r.indexOrder.IsSortKeyNeeded() {
			// VECTOR_TODO: Optimize buffer allocation
			sortKey = make([]byte, 0, len(w.currBatchRows[i].key)*3)
			// VECTOR_TODO: Avoid exploding composite and include keys multiple times
			sortKey, err = w.makeSortKeyForOrderBy(nil, nil, w.r.indexOrder,
				w.currBatchRows[i].key, w.currBatchRows[i].includeColumn, w.dists[i],
				w.r.getVectorKeyPos(), w.r.indexOrder.vectorDistDesc, sortKey)
			if err != nil {
				return err
			}
			w.currBatchRows[i].sortKey = sortKey
		}

		if w.r.useHeapForVectorIndex() {
			w.heap.Push(w.currBatchRows[i])
		} else {
			select {
			case <-w.stopCh:
			case w.outCh <- w.currBatchRows[i]:
				w.rowsReturned++
				w.currJob.rowsReturned++
			}
		}
		w.currBatchRows[i] = nil
	}

	//re-init for the next batch
	w.currBatchRows = w.currBatchRows[:0]
	w.codes = w.codes[:0]
	w.fvecs = w.fvecs[:0]
	w.dists = w.dists[:0]
	return
}

func (w *ScanWorker) scanIteratorCallback(entry, value []byte) error {

	if w.currJob.rowsScanned%SCAN_ROLLBACK_ERROR_BATCHSIZE == 0 && w.r.hasRollback != nil && w.r.hasRollback.Load() == true {
		return ErrIndexRollback
	}

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

	var newRow *Row
	if w.r.useHeapForVectorIndex() {
		newRow = w.rowBuf.Get()
		//Store reference to entry, value in the row struct.
		//processCurrentBatch will process and maintain a TopK local heap for these.
		//Before iterator close, these will be copied and sent down the pipeline.
		newRow.key = entry
		newRow.value = value
	} else {

		newRow = &Row{}
		//For non limit pushdown cases, all the entries need to be sent to query.
		//Copy the row as scan pipeline needs to access these after iterator close.
		entry1 := secondaryIndexEntry(entry)
		w.itrRow.len = entry1.lenKey()
		w.itrRow.key = entry
		w.itrRow.value = value

		// VECTOR_TODO:
		// 1. Check if adding a Queue/CirularBuffer of Rows here in place of senderCh will help
		// 2. Check if having a sync.Pool of Row objects per connCtx will help
		newRow.init(w.mem)
		newRow.copy(&w.itrRow)
	}
	newRow.workerId = w.id

	//store rows in the current batch buffer
	select {
	case <-w.stopCh:
		return ErrScanWorkerStopped
	default:
		w.currBatchRows = append(w.currBatchRows, newRow)
	}

	//once batch size number of rows are available, process the current batch
	if len(w.currBatchRows) == w.senderBatchSize {
		err := w.processCurrentBatch()
		if err != nil {
			logging.Tracef("%v scanIteratorCallback got error while processing batch: %v", w.logPrefix, err)
		}
		return err
	}

	//reset itrRow only if not using limit pushdown
	if !w.r.useHeapForVectorIndex() {
		w.itrRow.len = 0
		w.itrRow.key = nil
		w.itrRow.value = nil
		w.itrRow.includeColumn = nil
	}

	return nil
}

func resizeIncludeColumnBuf(buf []byte, capacity int) []byte {
	if cap(buf) < capacity {
		buf = make([]byte, 0, capacity+64) // Add 32 bytes additional padding
	}
	return buf
}

func (w *ScanWorker) setCoverForIncludeExprs(docid []byte, explodedIncludeValues n1qlval.Values) {
	cklen, metalen := w.r.cklen, w.r.allexprlen
	for i := cklen; i < metalen; i++ {
		index := i - cklen
		if w.r.indexKeyNames != nil && w.r.indexKeyNames[i] != "" {
			w.av.SetCover(w.r.indexKeyNames[i], explodedIncludeValues[index])
		}
	}

	if len(w.r.indexKeyNames) >= metalen && w.r.indexKeyNames[metalen] != "" {
		w.av.SetCover(w.r.indexKeyNames[metalen], w.svPool.Get(metalen-cklen, conversions.ByteSliceToString(docid)))
	}
}

// "meta" is the decoded version of the "rawMeta" given by iterator
// includeColumn fields will be extraced from the decoded meta
func (w *ScanWorker) inlineFilterCb(meta []byte, docid []byte) (bool, error) {

	if !w.r.isBhiveScan {
		return false, errors.New("Include column filtering is only supported with BHIVE indexes")
	}

	includeColumn := meta[w.codeSize:]

	w.includeColumnBuf = resizeIncludeColumnBuf(w.includeColumnBuf, len(includeColumn))

	_, explodedIncludeValues, err := jsonEncoder.ExplodeArray5(includeColumn, w.includeColumnBuf,
		w.includeColumncktemp, w.includeColumndktemp, w.includeColumnExplode, w.includeColumnDecode, w.explodeUpto, w.svPool)
	if err != nil {
		if err == collatejson.ErrorOutputLen {
			w.includeColumnBuf = make([]byte, 0, len(includeColumn)*3)
			_, explodedIncludeValues, err = jsonEncoder.ExplodeArray5(includeColumn, w.includeColumnBuf,
				w.includeColumncktemp, w.includeColumndktemp, w.includeColumnExplode, w.includeColumnDecode, w.explodeUpto, w.svPool)
		}
		if err != nil {
			return false, err
		}
	}

	w.setCoverForIncludeExprs(docid, explodedIncludeValues)

	evalValue, _, err := w.r.inlineFilterExpr.EvaluateForIndex(w.av, w.exprContext)
	switch evalValue.Actual().(type) {
	case bool:
		return evalValue.Actual().(bool), nil
	default:
		w.currJob.rowsFiltered++
		return false, nil
	}

	return false, nil
}

func (w *ScanWorker) inlineFilterCb2(meta []byte, docid []byte) (bool, error) {

	if !w.r.isBhiveScan {
		return false, errors.New("Include column filtering is only supported with BHIVE indexes")
	}

	includeColumn := meta[w.codeSize:]
	w.includeColumnBuf = resizeIncludeColumnBuf(w.includeColumnBuf, len(includeColumn))

	compositeKeys, _, err := jsonEncoder.ExplodeArray3(includeColumn, w.includeColumnBuf,
		w.includeColumncktemp, w.includeColumndktemp, w.includeColumnExplode, nil, w.explodeUpto)
	if err != nil {
		if err == collatejson.ErrorOutputLen {
			w.includeColumnBuf = make([]byte, 0, len(includeColumn)*3)
			compositeKeys, _, err = jsonEncoder.ExplodeArray3(includeColumn, w.includeColumnBuf,
				w.includeColumncktemp, w.includeColumndktemp, w.includeColumnExplode, nil, w.explodeUpto)
		}
		if err != nil {
			return false, err
		}
	}

	// Iterate over each scan for include columns.
	// Even if the row qualifies one scan, return "true"
	// Return false only if the row does not qualify all scans
	for _, inclScan := range w.r.includeColumnFilters {
		rowMatched := true
		compositeFilter := inclScan.CompositeFilters

		if len(compositeFilter) > w.includeColumnLen {
			// There cannot be more ranges than number of composite keys
			err = errors.New("There are more ranges than number of composite elements in the index")
			return false, err
		}
		rowMatched = applyFilter(compositeKeys, compositeFilter)
		if rowMatched {
			return true, nil
		}
	}

	w.currJob.rowsFiltered++

	// Row did not qualify any of the scans. Return false
	return false, nil
}

func (w *ScanWorker) bhiveIteratorCallback(entry, value []byte) error {

	if w.currJob.rowsScanned%SCAN_ROLLBACK_ERROR_BATCHSIZE == 0 && w.r.hasRollback != nil && w.r.hasRollback.Load() == true {
		return ErrIndexRollback
	}

	w.rowsScanned++
	w.currJob.rowsScanned++
	w.bytesRead += uint64(len(entry) + 8) // 8 bytes for centroidId that are read and filtered out
	w.currJob.bytesRead += uint64(len(entry) + 8)
	if value != nil {
		w.bytesRead += uint64(len(value))
		w.currJob.bytesRead += uint64(len(value))
	}

	codeSize := w.codeSize
	storeId, recordId, meta := w.currJob.snap.Snapshot().DecodeMeta(value)

	// The minimum length of decoded meta has to be "codeSize"
	// If it is less - Then there is a bug in decode meta logic
	if len(meta) == 0 || len(meta) < codeSize {
		logging.Fatalf("ScanWorker::bhiveIteratorCallback len(meta) is less than expected size. value: %v, len(value): %v, "+
			"meta: %v, len(meta): %v, codeSize: %v, docid: %v for indexInst: %v, partnId: %v, cid: %v, storeId: %v, recordId: %v",
			value, len(value), meta, len(meta), codeSize, entry, w.r.IndexInstId, w.currJob.pid, w.currJob.cid, storeId, recordId)
	}

	// The value field will contain both quantized codes and include column fields.
	// The first "codeSize" bytes contain quantized codes. Remaining bytes contain
	// the "includeColumn" value
	includeColumn := meta[codeSize:]
	value = meta[:codeSize]

	var newRow *Row
	if w.r.useHeapForVectorIndex() {
		//Store reference to entry, value in the row struct.
		//processCurrentBatch will process and maintain a TopK local heap for these.
		//Before iterator close, these will be copied and sent down the pipeline.
		newRow = w.rowBuf.Get()

		newRow.len = len(entry)
		newRow.key = entry
		newRow.value = value
		newRow.partnId = int(w.currJob.pid)
		newRow.recordId = recordId
		newRow.storeId = storeId
		newRow.cid = w.currJob.scan.Low.Bytes()

		// Copy the include column as is without explosion
		// Once all the rows are filtered out, we can then explode and project
		// only those keys that are required. Otherwise, we need memory allocation
		// at this point which does not go well with performance
		newRow.includeColumn = includeColumn

	} else {

		//For non limit pushdown cases, all the entries need to be sent to query.
		//Copy the row as scan pipeline needs to access these after iterator close.
		//
		// As this is a non-limit push down case, indexer need not re-rank on
		// the rows. Therefore, no need to populate other fields like partnId,
		// recordId etc.
		w.itrRow.len = len(entry)
		w.itrRow.key = entry
		w.itrRow.value = value
		w.itrRow.partnId = int(w.currJob.pid)
		w.itrRow.recordId = recordId
		w.itrRow.storeId = storeId
		w.itrRow.cid = w.currJob.scan.Low.Bytes()

		// VECTOR_TODO: It is not at all optimal to use the unexploded includeColumn here
		// as all rows are being push. Use exploded versions
		w.itrRow.includeColumn = includeColumn

		// VECTOR_TODO:
		// 1. Check if adding a Queue/CirularBuffer of Rows here in place of senderCh will help
		// 2. Check if having a sync.Pool of Row objects per connCtx will help
		newRow = &Row{}
		newRow.init(w.mem)
		newRow.copyForBhive(&w.itrRow)
	}

	select {
	case <-w.stopCh:
		return ErrScanWorkerStopped
	default:
		w.currBatchRows = append(w.currBatchRows, newRow)
	}

	//once batch size number of rows are available, process the current batch
	if len(w.currBatchRows) == w.senderBatchSize {
		err := w.processCurrentBatch()
		if err != nil {
			logging.Tracef("%v bhiveIteratorCallback got error while processing batch: %v", w.logPrefix, err)
		}
		return err
	}

	//reset itrRow only if not using limit pushdown
	if !w.r.useHeapForVectorIndex() {
		w.itrRow.len = 0
		w.itrRow.dist = 0
		w.itrRow.last = false
		w.itrRow.key = nil
		w.itrRow.value = nil
		w.itrRow.includeColumn = nil
		w.itrRow.partnId = 0
		w.itrRow.recordId = 0
		w.itrRow.storeId = 0
		w.itrRow.cid = nil
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

		// ck and dk returned should be used in group aggregates and projections
		skipRow, _, _, err = filterScanRow2(entry, w.currJob.scan, (*w.buf)[:0],
			w.cktmp, nil, w.r, nil)
		if err != nil {
			return true, err
		}
	}
	if skipRow {
		return true, nil
	}
	return false, nil
}

//Additional scan worker methods for debugging

func (w *ScanWorker) validateRow(row *Row, debugStr string, pos int) bool {

	if row.value == nil {
		logging.Fatalf("%v %v Detected null value at pos %v Row - %v", w.logPrefix, debugStr, pos, row)
		return false
	}

	if len(row.value) != w.r.getVectorCodeSize() {
		logging.Fatalf("%v %v Detected incorrect code size at pos %v Row - %v", w.logPrefix, debugStr, pos, row)
		return false
	}

	listNo := decodeListNo(row.value[:3])
	if !(listNo >= 0 && listNo < 100000) {
		logging.Fatalf("%v %v Detected listno %v out of range at pos %v Row - %v", w.logPrefix, debugStr, listNo, pos, row)
		return false
	}

	return true

}

// decodeListNo decodes the listno encoded
// as little-endian []byte to an int64
func decodeListNo(code []byte) int64 {
	var listNo int64
	nbit := 0

	for i := 0; i < len(code); i++ {
		listNo |= int64(code[i]) << nbit
		nbit += 8
	}

	return listNo
}

func (w *ScanWorker) printCurrentBatch() {

	for i, row := range w.currBatchRows {
		logging.Infof("%v Row %v Key %v Value %v", w.logPrefix, i, row.key, row.value)
	}

}

func (w *ScanWorker) printCurrentHeap() {
	rowList := w.heap.List()
	for i, row := range rowList {
		logging.Infof("%v Row %v Key %v Value %v", w.logPrefix, i, row.key, row.value)
	}
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
	recvChList []chan *Row
	mergeSort  bool
	mergeHeap  *TopKRowHeap
	r          *ScanRequest

	config c.Config
}

// NewWorkerPool creates a new WorkerPool
func NewWorkerPool(r *ScanRequest, numWorkers int, mergeSort bool, config c.Config) (*WorkerPool, error) {
	wp := &WorkerPool{
		r:          r,
		jobs:       make(chan *ScanJob, 0),
		jobsWg:     sync.WaitGroup{},
		errCh:      make(chan error),
		numWorkers: numWorkers,
		stopCh:     make(chan struct{}),
		workers:    make([]*ScanWorker, numWorkers),
		mergeSort:  mergeSort,
		config:     config,
	}
	if wp.mergeSort {
		wp.recvChList = make([]chan *Row, numWorkers)

		var err error
		rowCmp := r.getRowCompare()
		wp.mergeHeap, err = NewTopKRowHeap(numWorkers, true, rowCmp)
		if err != nil {
			return nil, err
		}
	}
	wp.logPrefix = fmt.Sprintf("%v[%v]WorkerPool", wp.r.LogPrefix, wp.r.RequestId)
	return wp, nil
}

func (wp *WorkerPool) Close() {
	for i, w := range wp.workers {
		w.Close()
		wp.workers[i] = nil
	}
	wp.mergeHeap = nil
	wp.workers = nil
	wp.recvChList = nil
}

func (wp *WorkerPool) Init() {
	wp.sendCh = make(chan *Row, 50*wp.numWorkers)

	var outCh chan<- *Row
	if !wp.mergeSort {
		outCh = wp.sendCh
	}

	for i := 0; i < wp.numWorkers; i++ {
		if wp.mergeSort {
			wp.recvChList[i] = make(chan *Row, 50)
			outCh = wp.recvChList[i]
		}

		wp.workers[i] = NewScanWorker(i, wp.r, wp.jobs, outCh, wp.stopCh, wp.errCh,
			&wp.jobsWg, wp.config, wp.mergeSort)
	}
}

// MergeSorter will sort data from recv channels and merge them and send to sendCh in sorted order
// User will submit a batch of jobs and start merge sorter. Wait while waiting for batch of jobs will
// wait for this merger sorter to finish too
func (wp *WorkerPool) MergeSorter() {
	if !wp.mergeSort {
		return
	}

	wp.jobsWg.Add(1)
	go wp.doMergeSort()
}

func (wp *WorkerPool) doMergeSort() {
	defer wp.jobsWg.Done()

	receivedLast := make([]bool, len(wp.recvChList))

	// Populate heap from the all channels
	activeChList := make(map[int]chan *Row, len(wp.recvChList))
	for _, recvCh := range wp.recvChList {
		var r *Row
		select {
		case <-wp.stopCh:
			return
		case r = <-recvCh:
		}
		wp.mergeHeap.Push(r)
		activeChList[r.workerId] = recvCh
	}

	for wp.mergeHeap.Len() > 0 {
		// Flush the min element into sendCh
		smallestRow := wp.mergeHeap.Pop()

		select {
		case <-wp.stopCh:
			return
		case wp.sendCh <- smallestRow:
		}

		if receivedLast[smallestRow.workerId] {
			continue
		}

		newRow, ok := <-activeChList[smallestRow.workerId]
		if ok {
			if newRow.last {
				receivedLast[newRow.workerId] = true
				continue
			}
			wp.mergeHeap.Push(newRow)
		}
	}
}

// Submit adds a job to the pool
// Note: Submit and Wait cannot run concurrently but Stop can be run asynchronously
// If err is seen while submitting, Submit will stop the workerpool and user is expected to
// Wait till previous submitted jobs are stopped
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
// If error is seen while waiting, Wait Stops the workerpool and waits for all the workers to
// exit. Sends last message to down stream to indicate early closure
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
			wp.Stop()
			<-doneCh         // Wait for running jobs to stop
			wp.sendLastRow() // Indicate downstream of early closure
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

func (wp *WorkerPool) GetOutCh() <-chan *Row {
	return wp.sendCh
}

// StopOutCh will stop the downstream send channel
func (wp *WorkerPool) StopOutCh() {
	close(wp.sendCh)
}

// sendLastRow to indicate early closure to downstream i.e. to
// differentiate between closure on error and end
func (wp *WorkerPool) sendLastRow() {
	row := &Row{last: true}
	wp.sendCh <- row
}

// Stop signals all workers to stop. Use Wait to wait till submitted jobs are done.
// Stop will halt the workers
func (wp *WorkerPool) Stop() {
	wp.stopOnce.Do(func() {
		close(wp.stopCh)
	})
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

	includebuf   *[]byte
	includecktmp [][]byte

	rowsReceived    uint64
	rowsOffsetCount uint64
	rowsReranked    uint64

	logPrefix string
	startTime time.Time

	numRerankWorkers int
	rerankCh         chan *Row
}

func NewMergeOperator(recvCh <-chan *Row, r *ScanRequest, writeItem WriteItem) (
	fio *MergeOperator, err error) {

	fio = &MergeOperator{
		recvCh:           recvCh,
		writeItem:        writeItem,
		req:              r,
		errCh:            make(chan error),
		numRerankWorkers: r.readersPerPartition,
		rerankCh:         make(chan *Row, r.readersPerPartition),
	}

	fio.logPrefix = fmt.Sprintf("%v[%v]MergeOperator", r.LogPrefix, r.RequestId)

	if r.useHeapForVectorIndex() {
		heapSize := r.getLimit()
		if r.Offset != 0 {
			heapSize += r.Offset
		}

		fio.heap, err = NewTopKRowHeap(int(heapSize), false, r.getRowCompare())
		if err != nil {
			return nil, err
		}
	}

	fio.buf = r.getFromSecKeyBufPool()
	fio.cktmp = make([][]byte, len(r.IndexInst.Defn.SecExprs))
	fio.includebuf = r.getFromSecKeyBufPool()
	fio.includecktmp = make([][]byte, len(r.IndexInst.Defn.Include))

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

	projectDistance := fio.req.projectVectorDist

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
		entry, err = fio.handleProjection(entry, row)
		if err != nil {
			fio.errCh <- err
			return
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

	if fio.req.enableReranking {
		fio.mergerWg.Add(1)
		go fio.rerankOnHeap()
		return // return from here as rerank logic will take care of substituing centroidID and writing items
	}

	// Read all elements from heap
	rowList := fio.heap.List()
	sortedRows := RowHeap{
		rows:  make([]*Row, 0),
		isMin: fio.req.indexOrder.IsOrderAscending(),
		less:  fio.req.getRowCompare(),
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

		if fio.req.Offset != 0 && fio.rowsOffsetCount < uint64(fio.req.Offset) {
			fio.rowsOffsetCount++
			continue
		}

		entry := row.key
		entry, err = fio.handleProjection(entry, row)
		if err != nil {
			fio.heap.Destroy()
			fio.errCh <- err
			return
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

func (fio *MergeOperator) rerankOnRow(row *Row, ctx IndexReaderContext, buf []byte) error {
	var err error

	partnId := row.partnId
	snap := fio.req.perPartnSnaps[c.PartitionId(partnId)]

	buf, err = snap.Snapshot().FetchValue(ctx, row.storeId, row.recordId, row.cid, buf)
	if err != nil {
		logging.Errorf("%v observed error while fetching value for storeId: %v recordId: %v, cid: %s, partnId: %v, err: %v",
			fio.logPrefix, row.storeId, row.recordId, row.cid, row.partnId, err)
		return err
	}

	// Convert the buffer to []float and compute distance
	value := unsafe.Slice((*float32)(unsafe.Pointer(&buf[0])), len(buf)/4)
	dists := make([]float32, 1)
	err = fio.req.codebookMap[c.PartitionId(partnId)].ComputeDistance(fio.req.queryVector, value, dists)
	if err != nil {
		logging.Errorf("%v observed error while computing distance for storeId: %v recordId: %v, cid: %s, partnId: %v, err: %v",
			fio.logPrefix, row.storeId, row.recordId, row.cid, row.partnId, err)
		return err
	}

	row.dist = dists[0]
	return nil
}

func (fio *MergeOperator) rerankWorker(wg *sync.WaitGroup, ctxCh map[c.PartitionId]chan IndexReaderContext, errCh chan error) {
	defer wg.Done()

	buf := make([]byte, 4*fio.req.IndexInst.Defn.VectorMeta.Dimension)
	for {
		select {
		case row, ok := <-fio.rerankCh:
			if !ok {
				return
			}

			ctx := <-ctxCh[c.PartitionId(row.partnId)]

			err := fio.rerankOnRow(row, ctx, buf)
			if err != nil {
				errCh <- err
				return
			}
			ctxCh[c.PartitionId(row.partnId)] <- ctx
		}
	}
}

func (fio *MergeOperator) rerankOnHeap() {
	defer fio.mergerWg.Done()

	allRows := fio.heap.List()
	var err error

	ctxCh := make(map[c.PartitionId]chan IndexReaderContext)
	for partnId, readerContexts := range fio.req.perPartnReaderCtx {
		if _, ok := ctxCh[partnId]; !ok {
			ctxCh[partnId] = make(chan IndexReaderContext, len(readerContexts))
		}
		for _, reader := range readerContexts {
			ctxCh[partnId] <- reader
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, fio.req.readersPerPartition)
	for i := 0; i < fio.req.readersPerPartition; i++ {
		wg.Add(1)
		go fio.rerankWorker(&wg, ctxCh, errCh)
	}

	for _, row := range allRows {
		select {
		case fio.rerankCh <- row:
		case err := <-errCh:
			if err != nil {
				close(fio.rerankCh)
				wg.Wait()        // Wait for all workers to exit
				fio.errCh <- err // Write the error to collector operator
				return
			}
		}
	}

	close(fio.rerankCh)
	wg.Wait()

	fio.rowsReranked += uint64(len(allRows))

	// sort the rows based on distance
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].dist < allRows[j].dist
	})

	projectDistance := fio.req.projectVectorDist
	// write the top-k items to next phase in the pipeline. "k" is
	// the limit on the number of rows that are requested in query
	for i := int64(0); i < min(fio.req.Limit, int64(len(allRows))); i++ {
		row := allRows[i]

		// Substitue centroid ID
		if projectDistance {
			fio.substituteCentroidID(row)
			if err != nil {
				fio.heap.Destroy()
				fio.errCh <- err
				return
			}
		}

		if fio.req.Offset != 0 && fio.rowsOffsetCount < uint64(fio.req.Offset) {
			fio.rowsOffsetCount++
			continue
		}

		entry := row.key
		entry, err = fio.handleProjection(entry, row)
		if err != nil {
			fio.heap.Destroy()
			fio.errCh <- err
			return
		}

		err = fio.writeItem(entry)
		if err != nil {
			logging.Verbosef("%v Collector got error: %v from writeItem from heap", fio.logPrefix, err)
			fio.heap.Destroy()
			fio.errCh <- err
			return
		}
	}

	// If everything went fine, then destroy the heap
	fio.heap.Destroy()

	return
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

func (fio *MergeOperator) handleProjection(entry []byte, row *Row) ([]byte, error) {
	var err error

	if fio.req.Indexprojection != nil {

		if fio.req.Indexprojection.projectSecKeys && !fio.req.Indexprojection.projectInclude {
			// Include fields are not required in projection. Project only secExprs
			entry, err = projectKeys(nil, row.key, (*fio.buf)[:0], fio.req, fio.cktmp)
			if err != nil {
				logging.Verbosef("%v Collector got error: %v from projectKeys", fio.logPrefix, err)
				return nil, err
			}
		} else if fio.req.Indexprojection.projectSecKeys || fio.req.Indexprojection.projectInclude {
			// Include fields are required in projection - Project both secExprs and include fields
			entry, err = projectSecKeysAndInclude(nil, row.key, row.includeColumn, (*fio.buf)[:0], (*fio.includebuf)[:0], fio.req, fio.cktmp, fio.includecktmp)
			if err != nil {
				logging.Verbosef("%v Collector got error: %v from projectSecKeysAndInclude", fio.logPrefix, err)
				return nil, err
			}
		} else {
			// no-op since projectSecKeys == false (which means project everything) and
			// projectInclude == false which means row.key can be used directly without any changes
		}
	} else if row.includeColumn != nil {
		// project both secExprs and includecolumn together
		entry, err = projectAllKeys(nil, row.key, row.includeColumn, (*fio.buf)[:0], fio.req)
		if err != nil {
			logging.Verbosef("%v Collector got error: %v from projectAllKeys", fio.logPrefix, err)
			return nil, err
		}
	} else {
		// no-op as req.IndexProjection == nil means project everything and row.includeColumn == nil
		// means row.key can be used directly. So, no need to change the key for projection
	}
	return entry, nil
}

func (fio *MergeOperator) Close() {

	fio.heap = nil
	fio.buf = nil
	fio.cktmp = nil
	fio.includebuf = nil
	fio.includecktmp = nil

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
	s.p.req.perPartnSnaps = snaps

	// For any_consistency scan during rollback we will get nil snapshot. As we will not
	// have any partitions in nil snapshot partition container this map will be empty.
	// For normal scans we dont error so following the same principle here.
	if len(snaps) == 0 {
		return nil
	}

	s.p.req.setReaderCtxMap()
	readersPerPartition := s.p.req.readersPerPartition
	ctxs := s.p.req.perPartnReaderCtx

	// Scans and codebooks
	scans := s.p.req.vectorScans
	codebooks := s.p.req.codebookMap

	// Merge sort of data across partitions is only needed when ordering and limit
	// are pushed down and we are scanning multiple partitions in this scan
	mergeSort := s.p.req.MultiPartnScan() && s.p.req.ScanRangeSequencing()

	// We can scan one range per paritition at a time when we have orderby and limit
	// pushed down and have barrier synchronization per range
	if mergeSort {
		readersPerPartition = 1
	}

	// Spawn Scan Workers
	wp, err := NewWorkerPool(s.p.req, readersPerPartition*len(s.p.req.PartitionIds), mergeSort, s.p.config)
	if err != nil {
		s.CloseWithError(err)
		return err
	}

	wp.Init()
	defer wp.Close()

	wpOutCh := wp.GetOutCh() // Output of Workerpool is input of MergeOperator

	writeItemAddStat := func(itm ...[]byte) error {
		s.p.rowsReturned++
		return s.WriteItem(itm...)
	}

	// Spawn Merge Operator
	fanIn, err := NewMergeOperator(wpOutCh, s.p.req, writeItemAddStat)
	if err != nil { // Stop worker pool and return
		wp.Stop() // Stop the workers
		s.CloseWithError(err)
		return err
	}
	defer fanIn.Close()

	// Make ScanJobs and schedule them on the WorkerPool
	// * readersPerPartition should be launched in parallel
	// * Run one scan on all partitions and then launch next one
	rem := s.p.req.perPartnScanParallelism % readersPerPartition
	numBatchesPerScan := s.p.req.perPartnScanParallelism / readersPerPartition
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
				s.p.rowsFiltered += job.rowsFiltered
				s.p.bytesRead += job.bytesRead
				s.p.decodeDur += job.decodeDur
				s.p.decodeCnt += job.decodeCnt
				s.p.distCmpDur += job.distCmpDur
				s.p.distCmpCnt += job.distCmpCnt
			}
		}
		s.p.rowsReranked += fanIn.rowsReranked
	}()

	doneCh := make(chan struct{})
	var wpErr, fanInErr error
	go func() {
		defer func() {
			wp.Stop()      // Stop the workers in non error cases, no-op on error
			wp.StopOutCh() // Close downstream channel, even in error case
			close(doneCh)  // Indicate finish of this goroutine
		}()
		stopped := false
		for i := 0; i <= maxBatchId; i++ {
			for _, job := range jobMap[i] {
				wpErr := wp.Submit(job)
				if wpErr != nil {
					stopped = true
					break
				}
			}
			if mergeSort {
				wp.MergeSorter()
			}
			wpErr = wp.Wait()
			if stopped || wpErr != nil {
				return
			}
		}
	}()

	// Wait for Merge Operator to terminate it will happen either when we stopped worker pool
	fanInErr = fanIn.Wait()
	if fanInErr != nil { // If there is an error in FanIn stop worker pool
		wp.Stop()
		<-doneCh
		s.CloseWithError(fanInErr)
		return fanInErr
	}

	// If there was workerpool error causing early closure of Merger return error from worker pool
	<-doneCh
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

func (w *ScanWorker) makeSortKeyForOrderBy(compositeKeys, explodedIncludekeys [][]byte, indexOrder *IndexKeyOrder, key []byte,
	includeKeys []byte, dist float32, vectorKeyPos int, vectorKeyPosDesc bool, buf []byte) ([]byte, error) {

	var keysToJoin [][]byte
	var err error

	desc := w.r.IndexInst.Defn.Desc
	isBhive := w.r.IndexInst.Defn.IsBhive()

	var numCompositeKeys int32
	var numIncludeKeys int32 = int32(len(w.r.IndexInst.Defn.Include))
	if compositeKeys == nil && !isBhive {
		compositeKeys, err = jsonEncoder.ExplodeArray4(key, buf)
		if err != nil {
			return nil, err
		}
		numCompositeKeys = int32(len(compositeKeys))
	} else if isBhive {
		// For bhive index we have only one composite key i.e. vector key
		numCompositeKeys = 1
	}

	if explodedIncludekeys == nil && includeKeys != nil {
		explodedIncludekeys, _, err = jsonEncoder.ExplodeArray3(includeKeys, (*w.incBuf)[:0], w.inccktmp, nil,
			w.r.Indexprojection.projectIncludeKeys, nil, len(w.r.IndexInst.Defn.Include))
		if err != nil {
			if err == collatejson.ErrorOutputLen {
				newBuf := make([]byte, 0, len(key)*3)
				explodedIncludekeys, _, err = jsonEncoder.ExplodeArray3(includeKeys, newBuf, w.inccktmp, nil,
					w.r.Indexprojection.projectIncludeKeys, nil, len(w.r.IndexInst.Defn.Include))
			}
			if err != nil {
				return nil, err
			}
		}
	}

	for i, keyPos := range indexOrder.keyPos {
		if keyPos == int32(vectorKeyPos) {
			distVal := n1qlval.NewValue(float64(dist))
			encodeBuf := make([]byte, 0, distVal.Size()*3)
			codec1 := collatejson.NewCodec(16)
			distCode, err := codec1.EncodeN1QLValue(distVal, encodeBuf)
			if err != nil && err.Error() == collatejson.ErrorOutputLen.Error() {
				distCode, err = encodeN1qlVal(distVal)
			}
			if err != nil {
				logging.Verbosef("makeSortKeyForOrderBy: Sender got error: %v from EncodeN1qlvalue", err)
				return nil, err
			}
			// Vector index has no order on distance as centroid is stored in index
			// So use order given in orderby pushdown
			if vectorKeyPosDesc {
				FlipBits(distCode)
			}
			keysToJoin = append(keysToJoin, distCode)
		} else if keyPos >= numCompositeKeys && keyPos < numCompositeKeys+numIncludeKeys {
			// For include keys there is no order in index so use order give in orderby pushdown
			revBuf := explodedIncludekeys[keyPos-numCompositeKeys]
			if indexOrder.desc[i] {
				revBuf = make([]byte, 0)
				revBuf = append(revBuf, explodedIncludekeys[keyPos-numCompositeKeys]...)
				FlipBits(revBuf)
			}
			keysToJoin = append(keysToJoin, revBuf)
		} else if keyPos < numCompositeKeys {
			// For composite keys flip bit when query orderBy order is different from index order
			revBuf := compositeKeys[keyPos]
			if desc != nil && desc[keyPos] != indexOrder.desc[i] {
				revBuf = make([]byte, 0)
				revBuf = append(revBuf, compositeKeys[keyPos]...)
				FlipBits(revBuf)
			}
			keysToJoin = append(keysToJoin, revBuf)
		} else {
			return nil, errors.New(fmt.Sprintf("keyPos %v is out of range indexOrder: %v", keyPos, indexOrder))
		}
	}

	buf = buf[:0]
	if buf, err = jsonEncoder.JoinArray(keysToJoin, buf); err != nil {
		l.Errorf("makeSortKeyForOrderBy: join array error %v", err)
		return nil, err
	}

	return buf, err
}

// AtomicRowBuffer is a thread-safe queue for Row pointers
type AtomicRowBuffer struct {
	queue []*Row
	head  int64
	tail  int64
	size  int64
	count int64
}

// NewRowBuffer initializes a new queue with a given size and pre-allocated Rows
func NewAtomicRowBuffer(size int) *AtomicRowBuffer {
	q := &AtomicRowBuffer{
		queue: make([]*Row, size),
		size:  int64(size),
		count: int64(size),
	}
	// Pre-allocate Row objects in the queue
	for i := 0; i < size; i++ {
		q.queue[i] = &Row{rowBuf: q}
	}
	q.tail = int64(size - 1)
	return q
}

// Put adds a new Row pointer to the queue and blocks if the queue is full
func (q *AtomicRowBuffer) Put(row *Row) {

	//set row references to nil
	row.key = nil
	row.value = nil
	row.includeColumn = nil
	row.cid = nil
	row.sortKey = nil

	for {
		// Check if the queue is full
		if atomic.LoadInt64(&q.count) == q.size {
			continue // Spin-wait if full
		}

		// Atomically increment count before proceeding
		if atomic.AddInt64(&q.count, 1) <= q.size {
			// Enqueue the row atomically
			tail := atomic.LoadInt64(&q.tail)
			tail = (tail + 1) % q.size
			q.queue[tail] = row
			// Move tail pointer in a circular fashion
			atomic.StoreInt64(&q.tail, tail)
			return
		} else {
			// Decrement count if the enqueue fails due to race
			atomic.AddInt64(&q.count, -1)
		}
	}
}

// Get removes a Row pointer from the queue and blocks if the queue is empty
func (q *AtomicRowBuffer) Get() *Row {
	for {
		// Check if the queue is empty
		if atomic.LoadInt64(&q.count) == 0 {
			continue // Spin-wait if empty
		}

		// Atomically decrement count before proceeding
		if atomic.AddInt64(&q.count, -1) >= 0 {
			// Dequeue the row atomically
			head := atomic.LoadInt64(&q.head)
			row := q.queue[head]
			// Move head pointer
			atomic.StoreInt64(&q.head, (head+1)%q.size)
			return row
		} else {
			// Increment count if the dequeue fails due to race
			atomic.AddInt64(&q.count, 1)
		}
	}
}
