/*
About OperationsMonitor:

Use OperationsMonitor to monitor the slow or hung operations.
Use NewOperation to initialize an operation to monitor.
Specify appropriate OperationTimeoutCallback, which will be executed
if the operation is not completed within specified timeout. After the
timeout, the operation is again considered for further monitoring.

GUIDELINES for using operations monitor

(1) Callback function should be lightweight block of code. Good examples
    of callback function can be information logging, updating some
    stats etc.

(2) Deciding correct value for the interval can be tricky. With large
    enough interval, and very high incoming operations rate, the monitor
    will never converge and it will start skipping the operations
    to be monitored. For example, if 1000 new operations will be added to
    the monitor every 10ms, with the interval >= 10ms, the monitor will
    start skipping the operations soon.
    Non zero batchSize introduces the another dimension to this problem.
    In above example, if batchSize is 500 and interval is 5ms, the monitor
    still won't converge and will start skipping operations.

(3) Use batchSize to prevent monitor from stealing too many contiguous
    CPU cycles, when there are too many operations being monitored.
*/

package common

import (
	"github.com/couchbase/indexing/secondary/logging"
	"sync"
	"sync/atomic"
	"time"
)

// OperationTimeoutCallback takes as input: the total time elapsed
// since the operation was created using NewOperation().
type OperationTimeoutCallback func(time.Duration)

type Operation struct {
	timeout time.Time
	callb   OperationTimeoutCallback
	doneCh  chan bool
	dur     time.Duration
	stTime  time.Time
}

func NewOperation(
	timeout time.Duration,
	doneCh chan bool,
	callb OperationTimeoutCallback) *Operation {

	now := time.Now()
	t := now.Add(timeout)
	return &Operation{
		doneCh:  doneCh,
		callb:   callb,
		timeout: t,
		dur:     timeout,
		stTime:  now,
	}
}

type OperationsMonitor struct {
	id           string
	operations   chan *Operation
	interval     time.Duration
	currOpsCount int64
	maxOps       int64
	closed       uint32
	batchSize    int64
	rwlock       sync.RWMutex
}

// NewOperationsMonitor
// Input:
// id        A unique identifier, to be used in all the log messages belonging
//           to the monitor
// maxOps    Maximum number of operations that can be monitored at a given time
// interval  Number of milliseconds in the monitor's interval
// batchSize Max number of operations to be processed in one iteration of the
//           monitor. This enables the user to control the burst on
//           cpu consumption. With batchSize <= 0, all operations will be
//           processed in one iteration.
func NewOperationsMonitor(id string, maxOps int, interval int,
	batchSize int) *OperationsMonitor {
	m := OperationsMonitor{
		id:         id,
		operations: make(chan *Operation, maxOps),
		interval:   time.Duration(interval) * time.Millisecond,
		maxOps:     int64(maxOps),
		batchSize:  int64(batchSize),
	}

	go m.monitor()
	return &m
}

func (m *OperationsMonitor) isClosed() bool {
	return atomic.LoadUint32(&m.closed) == 1
}

// Returns true if the operation is added to the monitor successfully.
// Returns false if
// (1) operations channel was full or
// (2) monitor is closed
func (m *OperationsMonitor) AddOperation(op *Operation) bool {
	// To protect against race between AddOperation and Close
	m.rwlock.RLock()
	defer m.rwlock.RUnlock()

	if m.isClosed() {
		return false
	}

	// Assumption: m.maxOps is small enough to avoid int64 overflow.
	if atomic.AddInt64(&m.currOpsCount, 1) > m.maxOps {
		atomic.AddInt64(&m.currOpsCount, -1)
		return false
	}

	m.operations <- op

	return true
}

func (m *OperationsMonitor) addOperationAgain(op *Operation) {
	// Check if monitor is closed before repopulating the operation
	m.rwlock.RLock()
	defer m.rwlock.RUnlock()

	if m.isClosed() {
		return
	}

	m.operations <- op
}

func (m *OperationsMonitor) handleOperation(op *Operation) {
	select {

	case val, ok := <-op.doneCh:
		atomic.AddInt64(&m.currOpsCount, -1)
		if !ok {
			return
		}

		if val {
			return
		}

		logging.Warnf("OperationsMonitor(%v) Unexpected value on doneCh", m.id)

	default:
		now := time.Now()
		if now.After(op.timeout) {
			// timeout has reached for this operation - re-initialize timeout
			op.callb(now.Sub(op.stTime))
			op.timeout = now.Add(op.dur)
		}

		m.addOperationAgain(op)
	}
}

func (m *OperationsMonitor) monitor() {
	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("OperationsMonitor(%v)::Monitor exiting with "+
				"error %v", m.id, r)
		}
	}()

	logging.Infof("OperationsMonitor(%v) starting monitor ...", m.id)
	for {
		if m.isClosed() {
			logging.Infof("OperationsMonitor(%v) exiting monitor ...", m.id)
			return
		}

		numOps := atomic.LoadInt64(&m.currOpsCount)
		if m.batchSize > 0 && m.batchSize < numOps {
			numOps = m.batchSize
		}

		for i := int64(0); i < numOps; i++ {
			op, ok := <-m.operations
			if !ok {
				logging.Infof("OperationsMonitor(%v) exiting monitor: op channel is closed", m.id)
				return
			}

			m.handleOperation(op)
		}

		time.Sleep(m.interval)
	}
}

func (m *OperationsMonitor) Close() {
	m.rwlock.Lock()
	defer m.rwlock.Unlock()

	if m.isClosed() {
		return
	}

	logging.Infof("OperationsMonitor(%v) stopping monitor ...", m.id)
	atomic.StoreUint32(&m.closed, 1)
	close(m.operations)
}
