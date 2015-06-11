package client

import "sync"

import common "github.com/couchbase/indexing/secondary/common"

type RoundRobin struct {
	rw        sync.RWMutex
	replicas  Replicas                     // load replicas.
	deterants map[common.IndexDefnId]int64 // index -> deterant value
	pickOff   map[common.IndexDefnId]int
}

func NewRoundRobin() *RoundRobin {
	rr := &RoundRobin{
		deterants: make(map[common.IndexDefnId]int64),
		pickOff:   make(map[common.IndexDefnId]int),
	}
	return rr
}

// SetReplicas for the current set of indexes.
func (rr *RoundRobin) SetReplicas(replicas Replicas) {
	rr.rw.Lock()
	defer rr.rw.Unlock()

	rr.replicas = replicas
	// remove deterants for indexes that is been deleted / gone-offline.
	for defnId := range rr.deterants {
		if _, ok := rr.replicas[defnId]; !ok {
			delete(rr.deterants, defnId)
			delete(rr.pickOff, defnId)
		}
	}
}

func (rr *RoundRobin) Pick(defnID uint64, retry int) uint64 {
	rr.rw.Lock()
	defer rr.rw.Unlock()

	id := common.IndexDefnId(defnID)
	if _, ok := rr.pickOff[id]; !ok {
		rr.pickOff[id] = 0 // pickOff map gets initialized here
	}
	if replicas, ok := rr.replicas[id]; ok {
		l, count := len(replicas), len(replicas)
		// pick a replica starting from the next candidate.
		// if, a deterant was added to replica (happens due to failed scan-call)
		//     then decrement candidate's deterant, skip the candidate,
		//     and move next.
		// if, all candidates have a deterant, then all their deterants are
		//     decremented and defaults to input `defnID`.
		for i := rr.pickOff[id] % l; count > 0; i, count = (i+1)%l, count-1 {
			replicaID := replicas[common.IndexDefnId(i)]
			if det, ok := rr.deterants[replicaID]; ok && det > 0 {
				rr.deterants[replicaID]-- // deterant detected, decrement it.
				continue
			}
			// candidate available
			rr.deterants[replicaID] = 0  // deterants map get initialized here
			rr.pickOff[id] = (i + 1) % l // <-- round-robin happens here
			return uint64(replicaID)
		}
	}
	return defnID
}

// Timeit
func (rr *RoundRobin) Timeit(defnID uint64, value float64) {
	rr.rw.Lock()
	defer rr.rw.Unlock()
	id := common.IndexDefnId(defnID)
	rr.deterants[id] = 0
	if value < 0 {
		// a deterant is added so that this candidate will not be
		// picked for next 20 scan queries. This is to ensure a reasonable
		// balance between transient failures and permanant failures.
		// 1. if ns_server / gometa can adequately detect permanent failures
		//    then we can decrease the deterant value.
		// 2. if permanant failures are more often than transient failures
		//    we should increase the deterant value.
		// 3. if transient failures are more often than permanant failures
		//    we should decrease the deterant value.
		rr.deterants[id] = 20 // TODO: make this configurable ?
	}
}
