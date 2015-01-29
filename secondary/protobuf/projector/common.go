package protobuf

import "sort"
import "errors"
import "fmt"

import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/dcp"
import "github.com/couchbaselabs/goprotobuf/proto"

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

// IsEmpty returns true if the timestamp does not contain any vbucket entries.
func (ts *TsVbuuid) IsEmpty() bool {
	return len(ts.Vbnos) == 0
}

// Append a vbucket detail to TsVbuuid.
func (ts *TsVbuuid) Append(
	vbno uint16, seqno, vbuuid, start, end uint64) *TsVbuuid {

	snapshot := NewSnapshot(start, end)
	ts.Vbnos = append(ts.Vbnos, uint32(vbno))
	ts.Seqnos = append(ts.Seqnos, seqno)
	ts.Vbuuids = append(ts.Vbuuids, vbuuid)
	ts.Snapshots = append(ts.Snapshots, snapshot)
	sort.Sort(ts)
	return ts
}

// Clone creates new copy of timestamp.
func (ts *TsVbuuid) Clone() *TsVbuuid {
	vbnos := ts.GetVbnos()
	seqnos := ts.GetSeqnos()
	vbuuids := ts.GetVbuuids()
	snapshots := ts.GetSnapshots()
	newts := NewTsVbuuid(ts.GetPool(), ts.GetBucket(), len(vbnos))
	for i, vbno := range vbnos {
		newts.Vbnos = append(newts.Vbnos, vbno)
		newts.Seqnos = append(newts.Seqnos, seqnos[i])
		newts.Vbuuids = append(newts.Vbuuids, vbuuids[i])
		newts.Snapshots = append(newts.Snapshots, snapshots[i])
	}
	return newts
}

// Get entry details like seqno, vbuuid, snapshot for `vbno`.
func (ts *TsVbuuid) Get(
	vbno uint16) (seqno, vbuuid, sStart, sEnd uint64, err error) {

	if ts == nil {
		return seqno, vbuuid, sStart, sEnd, errors.New("timestamp empty")
	}

	seqnos, vbuuids := ts.GetSeqnos(), ts.GetVbuuids()
	snapshots := ts.GetSnapshots()
	for i, x := range ts.GetVbnos() {
		if x == uint32(vbno) {
			seqno, vbuuid, snapshot := seqnos[i], vbuuids[i], snapshots[i]
			sStart, sEnd = snapshot.GetStart(), snapshot.GetEnd()
			return seqno, vbuuid, sStart, sEnd, nil
		}
	}
	return seqno, vbuuid, sStart, sEnd, errors.New("Not Found")
}

// Set entry details like seqno, vbuuid, snapshot for `vbno`.
func (ts *TsVbuuid) Set(
	vbno uint16, seqno, vbuuid, sStart, sEnd uint64) (err error) {

	if ts == nil {
		return errors.New("bucket-timestamp empty")
	}

	snapshot := NewSnapshot(sStart, sEnd)
	seqnos, vbuuids := ts.GetSeqnos(), ts.GetVbuuids()
	snapshots := ts.GetSnapshots()
	for i, x := range ts.GetVbnos() {
		if x == uint32(vbno) {
			seqnos[i], vbuuids[i], snapshots[i] = seqno, vbuuid, snapshot
			ts.Seqnos, ts.Vbuuids, ts.Snapshots = seqnos, vbuuids, snapshots
			return nil
		}
	}
	return errors.New("Not Found")
}

// Contains with check whether `vbno` has an entry in the timestamp.
func (ts *TsVbuuid) Contains(vbno uint16) bool {
	for _, vbno := range ts.GetVbnos() {
		if vbno == vbno {
			return true
		}
	}
	return false
}

// FromTsVbuuid converts timestamp from common.TsVbuuid to protobuf
// format.
func (ts *TsVbuuid) FromTsVbuuid(nativeTs *c.TsVbuuid) *TsVbuuid {
	for _, vbno := range nativeTs.GetVbnos() {
		s := nativeTs.Snapshots[vbno]
		snapshot := NewSnapshot(s[0], s[1])
		ts.Snapshots = append(ts.Snapshots, snapshot)
		ts.Vbnos = append(ts.Vbnos, uint32(vbno))
		ts.Seqnos = append(ts.Seqnos, nativeTs.Seqnos[vbno])
		ts.Vbuuids = append(ts.Vbuuids, nativeTs.Vbuuids[vbno])
	}
	return ts
}

// ToTsVbuuid converts timestamp from protobuf format to common.TsVbuuid,
// later requires the full set of timestamp.
func (ts *TsVbuuid) ToTsVbuuid() *c.TsVbuuid {
	nativeTs := c.NewTsVbuuid(ts.GetBucket(), cap(ts.Seqnos))
	seqnos, vbuuids, ss := ts.GetSeqnos(), ts.GetVbuuids(), ts.GetSnapshots()
	for i, vbno := range ts.GetVbnos() {
		nativeTs.Seqnos[vbno] = seqnos[i]
		nativeTs.Vbuuids[vbno] = vbuuids[i]
		nativeTs.Snapshots[vbno] = [2]uint64{ss[i].GetStart(), ss[i].GetEnd()}
	}
	return nativeTs
}

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

	cache := make(map[uint32]bool)
	for _, vbno := range other.Vbnos {
		cache[vbno] = true
	}

	// deduplicate this
	for i, vbno := range ts.Vbnos {
		if _, ok := cache[vbno]; ok {
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

	cache := make(map[uint32]bool)
	for _, vbno := range vbuckets {
		cache[uint32(vbno)] = true
	}
	for i, vbno := range ts.Vbnos {
		if _, ok := cache[vbno]; ok {
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

	cache := make(map[uint32]bool)
	for _, vbno := range vbuckets {
		cache[uint32(vbno)] = true
	}
	for i, vbno := range ts.Vbnos {
		if _, ok := cache[vbno]; ok {
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
		vbuuid, _, _ := flog.Latest()
		ts.Append(vbno, 0, vbuuid, 0, 0)
	}
	return ts
}

// TODO: Once we confirm to use seqno as snapshot-start
// and snapshot-end we can let go of this function.
func (ts *TsVbuuid) ComputeRestartTs(flogs couchbase.FailoverLog) *TsVbuuid {
	restartTs := NewTsVbuuid(ts.GetPool(), ts.GetBucket(), cap(ts.Vbnos))
	i := 0
	for vbno, flog := range flogs {
		vbuuid, _, _ := flog.Latest()
		s := ts.Snapshots[i]
		start, end := s.GetStart(), s.GetEnd()
		restartTs.Append(vbno, start, vbuuid, start, end)
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

func (ts *TsVbuuid) Repr() string {
	vbnos := ts.GetVbnos()
	s := fmt.Sprintf("pool: %v, bucket: %v, vbuckets: %v -\n",
		ts.GetPool(), ts.GetBucket(), len(vbnos))
	seqnos, vbuuids := ts.GetSeqnos(), ts.GetVbuuids()
	snapshots := ts.GetSnapshots()
	s += fmt.Sprintf("    vbno, vbuuid, seqno, snapshot-start, snapshot-end\n")
	for i := 0; i < len(vbnos); i++ {
		start, end := snapshots[i].GetStart(), snapshots[i].GetEnd()
		s += fmt.Sprintf("    {%5d %16x %10d %10d %10d}\n",
			vbnos[i], vbuuids[i], seqnos[i], start, end)
	}
	return s
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
