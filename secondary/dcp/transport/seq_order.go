package transport

import "fmt"

//
// SeqOrderState provides an interface to store and check seqno ordering
// for a dcp stream for any vbucket.
// Usage guidelines:
// 1. Instantiate the SeqOrderState object separately for each vbucket stream.
// 2. Instantiate a new SeqOrderState object on stream end/start.
//
type SeqOrderState interface {

	// Process DCP snapshot event.
	ProcessSnapshot(sseq, eseq uint64)

	// Proccess any non-snapshot DCP event that has a seqno.
	ProcessSeqno(seq uint64) bool

	// Get information about the SeqOrderState in printable format.
	GetInfo() string

	// Get the number of times when the seq order was violated.
	GetErrCount() int
}

type seqOrderState struct {
	snapStart    uint64
	snapEnd      uint64
	snapStarted  bool
	prevSeq      uint64
	prevSeqValid bool
	errCount     int
}

func NewSeqOrderState() *seqOrderState {
	return &seqOrderState{}
}

func (s *seqOrderState) ProcessSnapshot(sseq, eseq uint64) {
	s.snapStart = sseq
	s.snapEnd = eseq
	s.snapStarted = true
	return
}

func (s *seqOrderState) ProcessSeqno(seq uint64) bool {
	if !s.snapStarted {
		s.errCount++
		return false
	}

	if s.prevSeqValid {
		if s.prevSeq >= seq {
			s.errCount++
			return false
		}
	}

	if seq > s.snapEnd || seq < s.snapStart {
		s.errCount++
		return false
	}

	s.prevSeq = seq
	s.prevSeqValid = true
	return true
}

func (s *seqOrderState) GetInfo() string {
	return fmt.Sprintf("{snapStart: %v, snapEnd %v, snapStarted %v, prevSeq: %v, prevSeqValid: %v, errCount: %v}",
		s.snapStart, s.snapEnd, s.snapStarted, s.prevSeq, s.prevSeqValid, s.errCount)
}

func (s *seqOrderState) GetErrCount() int {
	return s.errCount
}
