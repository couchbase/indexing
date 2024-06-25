package indexer

import (
	"errors"
	"fmt"
	"sync"

	"github.com/couchbase/indexing/secondary/collatejson"
	"github.com/couchbase/indexing/secondary/common"
	c "github.com/couchbase/indexing/secondary/common"
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
			w.currJob = job
			if !ok {
				return
			}
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
	// VECTOR_TODO: Implement pipeline for vector scans
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
