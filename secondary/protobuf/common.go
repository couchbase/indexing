package protobuf

import (
	"sort"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbaselabs/go-couchbase"
	"github.com/couchbaselabs/goprotobuf/proto"
)

// *****
// Error
// *****

// NewError create a protobuf message `Error` and return its
// reference back to the caller.
func NewError(err error) *Error {
	if err != nil {
		return &Error{Error: proto.String(err.Error())}
	}
	return &Error{Error: proto.String("")}
}

// Name implement MessageMarshaller{} interface
func (req *Error) Name() string {
	return "Error"
}

// ContentType implement MessageMarshaller{} interface
func (req *Error) ContentType() string {
	return "application/protobuf"
}

// Encode implement MessageMarshaller{} interface
func (req *Error) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

// Decode implement MessageMarshaller{} interface
func (req *Error) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

// ********
// Snapshot
// ********

func NewSnapshot(start, end uint64) *Snapshot {
	return &Snapshot{
		Start: proto.Uint64(start),
		End:   proto.Uint64(end),
	}
}

// ****
// TsVb
// ****

func NewTsVb(pool, bucket string) *TsVb {
	return &TsVb{
		Pool:   proto.String(pool),
		Bucket: proto.String(bucket),
		Vbnos:  make([]uint32, 0),
		Seqnos: make([]uint64, 0),
	}
}

// Append a vbucket detail to TsVb
func (ts *TsVb) Append(vbno uint16, seqno uint64) *TsVb {
	ts.Vbnos = append(ts.Vbnos, uint32(vbno))
	ts.Seqnos = append(ts.Seqnos, seqno)
	return ts
}

// ********
// TsVbFull
// ********

func NewTsVbFull(pool, bucket string, seqnos []uint64) *TsVbFull {
	return &TsVbFull{
		Pool:   proto.String(pool),
		Bucket: proto.String(bucket),
		Seqnos: seqnos,
	}
}

// ********
// TsVbuuid
// ********

func NewTsVbuuid(pool, bucket string, maxvb int) *TsVbuuid {
	return &TsVbuuid{
		Pool:      proto.String(pool),
		Bucket:    proto.String(bucket),
		Vbnos:     make([]uint32, 0, maxvb),
		Seqnos:    make([]uint64, 0, maxvb),
		Vbuuids:   make([]uint64, 0, maxvb),
		Snapshots: make([]*Snapshot, 0, maxvb),
	}
}

// Append a vbucket detail to TsVbuuid
func (ts *TsVbuuid) Append(
	vbno uint16, seqno, vbuuid, start, end uint64) *TsVbuuid {

	snapshot := NewSnapshot(start, end)
	ts.Vbnos = append(ts.Vbnos, uint32(vbno))
	ts.Seqnos = append(ts.Seqnos, seqno)
	ts.Vbuuids = append(ts.Vbuuids, vbuuid)
	ts.Snapshots = append(ts.Snapshots, snapshot)
	return ts
}

// FromTsVbuuid converts timestamp from common.TsVbuuid to protobuf
// format.
func (ts *TsVbuuid) FromTsVbuuid(nativeTs *c.TsVbuuid) *TsVbuuid {
	for vbno, seqno := range nativeTs.Seqnos {
		s := nativeTs.Snapshots[vbno]
		snapshot := NewSnapshot(s[0], s[1])
		ts.Vbnos = append(ts.Vbnos, uint32(vbno))
		ts.Seqnos = append(ts.Seqnos, seqno)
		ts.Vbuuids = append(ts.Vbuuids, nativeTs.Vbuuids[vbno])
		ts.Snapshots = append(ts.Snapshots, snapshot)
	}
	return ts
}

// ToTsVbuuid converts timestamp from protobuf format to common.TsVbuuid.
// TODO: semantics of c.TsVbuuid has changed.
//func (ts *TsVbuuid) ToTsVbuuid() *c.TsVbuuid {
//    vbnos := make([]uint16, len(
//    ss := make([][2]uint64, 0)
//    for _, s := range ts.GetSnapshots() {
//        ss = append(ss, [2]uint64{s.GetStart(), s.GetEnd()})
//    }
//    nativeTs := &c.TsVbuuid{
//        Bucket:    ts.GetBucket(),
//        Vbnos:     c.Vbno32to16(ts.GetVbnos()),
//        Seqnos:    ts.GetSeqnos(),
//        Vbuuids:   ts.GetVbuuids(),
//        Snapshots: ss,
//    }
//    return nativeTs
//}

// Union will return a union set of timestamps based on
// Vbuckets. Duplicate vbucket entries in `other` timestamp
// will be skipped.
func (ts *TsVbuuid) Union(other *TsVbuuid) *TsVbuuid {
	if ts == nil {
		return other
	} else if other == nil {
		return ts
	}

	maxVbuckets := len(ts.Seqnos)
	newts := NewTsVbuuid(ts.GetPool(), ts.GetBucket(), maxVbuckets)

	// copy from other
	newts.Vbnos = append(newts.Vbnos, other.Vbnos...)
	newts.Seqnos = append(newts.Seqnos, other.Seqnos...)
	newts.Vbuuids = append(newts.Vbuuids, other.Vbuuids...)
	newts.Snapshots = append(newts.Snapshots, other.Snapshots...)

	cache := make([]byte, maxVbuckets)
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

// SelectByVbuckets will select vbuckets from `ts`
// for a subset of `vbuckets`, both `ts` and `vbuckets`
// are expected to be pre-sorted.
func (ts *TsVbuuid) SelectByVbuckets(vbuckets []uint16) *TsVbuuid {
	if ts == nil || vbuckets == nil {
		return ts
	}

	maxVbuckets := len(ts.Seqnos)
	newts := NewTsVbuuid(ts.GetPool(), ts.GetBucket(), maxVbuckets)
	if len(ts.Vbnos) == 0 {
		return newts
	}

	cache := make([]byte, maxVbuckets)
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

// FilterByVbuckets will exclude `vbuckets` from `ts`,
// both `ts` and `vbuckets` are expected to be pre-sorted.
func (ts *TsVbuuid) FilterByVbuckets(vbuckets []uint16) *TsVbuuid {
	if ts == nil || vbuckets == nil {
		return ts
	}

	maxVbuckets := len(ts.Seqnos)
	newts := NewTsVbuuid(ts.GetPool(), ts.GetBucket(), maxVbuckets)
	if len(ts.Vbnos) == 0 {
		return newts
	}

	cache := make([]byte, maxVbuckets)
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

// VerifyBranch shall verify whether the timestamp
// branch-id for each vbucket matches with input arguments.
func (ts *TsVbuuid) VerifyBranch(vbnos []uint16, vbuuids []uint64) bool {
	tsVbuuids := ts.GetVbuuids()
	for i, vbno := range vbnos {
		for j, tsVbno := range ts.GetVbnos() {
			if vbno == uint16(tsVbno) {
				if vbuuids[i] != tsVbuuids[j] {
					return false
				}
			}
		}
	}
	return true
}

// ComputeFailoverTs computes TsVbuuid timestamp using
// failover logs obtained from ns_server.
func (ts *TsVbuuid) ComputeFailoverTs(flogs couchbase.FailoverLog) *TsVbuuid {
	failoverTs := NewTsVbuuid(ts.GetPool(), ts.GetBucket(), cap(ts.Vbnos))
	for vbno, flog := range flogs {
		x := flog[len(flog)-1]
		vbuuid, seqno := x[0], x[1]
		failoverTs.Append(vbno, seqno, vbuuid, seqno, seqno)
	}
	return failoverTs
}

// InitialRestartTs for a subset of vbuckets.
func (ts *TsVbuuid) InitialRestartTs(flogs couchbase.FailoverLog) *TsVbuuid {

	for vbno, flog := range flogs {
		x := flog[len(flog)-1]
		ts.Append(vbno, 0, x[0], 0, 0)
	}
	return ts
}

// TODO: Once we confirm to use seqno as snapshot-start
// and snapshot-end we can let go of this function.
func (ts *TsVbuuid) ComputeRestartTs(flogs couchbase.FailoverLog) *TsVbuuid {
	restartTs := NewTsVbuuid(ts.GetPool(), ts.GetBucket(), cap(ts.Vbnos))
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

func (ts *TsVbuuid) SeqnoFor(vbno uint16) (uint64, error) {
	seqnos := ts.GetSeqnos()
	for i, x := range ts.GetVbnos() {
		if vbno == uint16(x) {
			return seqnos[i], nil
		}
	}
	return 0, c.ErrorNotFound
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
