package protobuf

import (
	"sort"

	"github.com/couchbaselabs/goprotobuf/proto"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbaselabs/go-couchbase"
)

// NewError create a protobuf message `Error` and return its reference back to
// the caller.
func NewError(err error) *Error {
	if err != nil {
		return &Error{Error: proto.String(err.Error())}
	}
	return &Error{Error: proto.String("")}
}

// Error implement MessageMarshaller interface

func (req *Error) Name() string {
	return "Error"
}

func (req *Error) ContentType() string {
	return "application/protobuf"
}

func (req *Error) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *Error) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

// Append a vbucket detail to TsVb
func (ts *TsVb) Append(vbno uint16, seqno uint64) *TsVb {
	ts.Vbnos = append(ts.Vbnos, uint32(vbno))
	ts.Seqnos = append(ts.Seqnos, seqno)
	return ts
}

// Append a vbucket detail to TsVbFull
func (ts *TsVbFull) Append(seqno uint64) *TsVbFull {
	ts.Seqnos = append(ts.Seqnos, seqno)
	return ts
}

// TsVbuuid
func NewTsVbuuid(bucket string, maxVbuckets int) *TsVbuuid {
	return &TsVbuuid{
		Bucket:    proto.String(bucket),
		Vbnos:     make([]uint32, 0, maxVbuckets),
		Seqnos:    make([]uint64, 0, maxVbuckets),
		Vbuuids:   make([]uint64, 0, maxVbuckets),
		Snapshots: make([]*Snapshot, 0, maxVbuckets),
	}
}

// Append a vbucket detail to TsVbuuid
func (ts *TsVbuuid) Append(vbno uint16, seqno, vbuuid, start, end uint64) *TsVbuuid {
	snapshot := &Snapshot{
		Start: proto.Uint64(start),
		End:   proto.Uint64(end),
	}
	ts.Vbnos = append(ts.Vbnos, uint32(vbno))
	ts.Seqnos = append(ts.Seqnos, seqno)
	ts.Vbuuids = append(ts.Vbuuids, vbuuid)
	ts.Snapshots = append(ts.Snapshots, snapshot)
	return ts
}

// Union will return a union set of timestamps based on Vbuckets. Duplicate
// vbucket entries in `other` timestamp will be skipped.
func (ts *TsVbuuid) Union(other *TsVbuuid) *TsVbuuid {
	if ts == nil || other == nil {
		return ts
	}
	newts := NewTsVbuuid(ts.GetBucket(), c.MaxVbuckets)

	// copy from other
	newts.Vbnos = append(newts.Vbnos, other.Vbnos...)
	newts.Seqnos = append(newts.Seqnos, other.Seqnos...)
	newts.Vbuuids = append(newts.Vbuuids, other.Vbuuids...)
	newts.Snapshots = append(newts.Snapshots, other.Snapshots...)

	cache := [c.MaxVbuckets]byte{}
	for _, vbno := range other.Vbnos {
		cache[vbno] = 1
	}

	// deduplicate this
	for i, vbno := range ts.Vbnos {
		if cache[vbno] == 1 {
			continue
		}
		newts.Vbnos = append(newts.Vbnos, vbno)
		newts.Seqnos = append(newts.Seqnos, ts.Seqnos[i])
		newts.Vbuuids = append(newts.Vbuuids, ts.Vbuuids[i])
		newts.Snapshots = append(newts.Snapshots, ts.Snapshots[i])
	}
	sort.Sort(newts)
	return newts
}

// SelectByVbuckets will select vbuckets from `ts` for a subset of `vbuckets`,
// both `ts` and `vbuckets` are expected to be pre-sorted.
func (ts *TsVbuuid) SelectByVbuckets(vbuckets []uint16) *TsVbuuid {
	if ts == nil || vbuckets == nil {
		return ts
	}

	newts := NewTsVbuuid(ts.GetBucket(), c.MaxVbuckets)
	if len(ts.Vbnos) == 0 {
		return newts
	}

	cache := [c.MaxVbuckets]byte{} // TODO: optimize for GC
	for _, vbno := range vbuckets {
		cache[vbno] = 1
	}
	for i, vbno := range ts.Vbnos {
		if cache[vbno] == 1 {
			newts.Vbnos = append(newts.Vbnos, vbno)
			newts.Vbuuids = append(newts.Vbuuids, ts.Vbuuids[i])
			newts.Seqnos = append(newts.Seqnos, ts.Seqnos[i])
			newts.Snapshots = append(newts.Snapshots, ts.Snapshots[i])
		}
	}
	return newts
}

// FilterByVbuckets will exclude `vbuckets` from `ts`, both `ts` and `vbuckets`
// are expected to be pre-sorted.
func (ts *TsVbuuid) FilterByVbuckets(vbuckets []uint16) *TsVbuuid {
	if ts == nil || vbuckets == nil {
		return ts
	}

	newts := NewTsVbuuid(ts.GetBucket(), c.MaxVbuckets)
	if len(ts.Vbnos) == 0 {
		return newts
	}

	cache := [c.MaxVbuckets]byte{}
	for _, vbno := range vbuckets {
		cache[vbno] = 1
	}
	for i, vbno := range ts.Vbnos {
		if cache[vbno] == 1 {
			continue
		}
		newts.Vbnos = append(newts.Vbnos, vbno)
		newts.Seqnos = append(newts.Seqnos, ts.Seqnos[i])
		newts.Vbuuids = append(newts.Vbuuids, ts.Vbuuids[i])
		newts.Snapshots = append(newts.Snapshots, ts.Snapshots[i])
	}
	return newts
}

func (ts *TsVbuuid) ComputeFailoverTs(flogs couchbase.FailoverLog) *TsVbuuid {
	failoverTs := NewTsVbuuid(ts.GetBucket(), cap(ts.Vbnos))
	for vbno, flog := range flogs {
		x := flog[len(flog)-1]
		failoverTs.Append(vbno, x[1], x[0], 0, 0)
	}
	return failoverTs
}

// compute restart timestamp from high-water mark timestamp.
func (ts *TsVbuuid) ComputeRestartTs(flogs couchbase.FailoverLog) *TsVbuuid {
	restartTs := NewTsVbuuid(ts.GetBucket(), cap(ts.Vbnos))
	i := 0
	for vbno, flog := range flogs {
		x := flog[len(flog)-1]
		s := ts.Snapshots[i]
		start, end := s.GetStart(), s.GetEnd()
		restartTs.Append(vbno, start, x[0], start, end)
		i++
	}
	return restartTs
}

// InitialRestartTs for a subset of vbuckets.
func (ts *TsVbuuid) InitialRestartTs(vbnos []uint16) *TsVbuuid {
	for _, vbno := range vbnos {
		ts.Append(vbno, 0, 0, 0, 0)
	}
	return ts
}

// Sort TsVbuuid

func (ts *TsVbuuid) Len() int {
	return len(ts.Vbnos)
}

func (ts *TsVbuuid) Less(i, j int) bool {
	return ts.Vbnos[i] < ts.Vbnos[j]
}

func (ts *TsVbuuid) Swap(i, j int) {
	ts.Vbnos[i], ts.Vbnos[j] = ts.Vbnos[j], ts.Vbnos[i]
	ts.Seqnos[i], ts.Seqnos[j] = ts.Seqnos[j], ts.Seqnos[i]
	ts.Vbuuids[i], ts.Vbuuids[j] = ts.Vbuuids[j], ts.Vbuuids[i]
	ts.Snapshots[i], ts.Snapshots[j] = ts.Snapshots[j], ts.Snapshots[i]
}
