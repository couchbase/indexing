package protobuf

import (
	"code.google.com/p/goprotobuf/proto"
	c "github.com/couchbase/indexing/secondary/common"
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

// transform protobuf's BranchTimestamp to native timestamp.
func ToTimestamp(ts *BranchTimestamp) *c.Timestamp {
	t := c.NewTimestamp(ts.GetBucket(), 1024) // TODO: no magic numbers
	t.Vbnos = c.Vbno32to16(ts.GetVbnos())
	t.Seqnos = ts.GetSeqnos()
	t.Vbuuids = ts.GetVbuuids()

	// convert snapshot {start, end} from protobuf to native golang.
	snapshots := ts.GetSnapshots()
	t.Snapshots = make([][2]uint64, 0, len(snapshots))
	for _, s := range snapshots {
		t.Snapshots = append(t.Snapshots, [2]uint64{s.GetStart(), s.GetEnd()})
	}
	return t
}

// transform local timestamp to protobuf's BranchTimestamp
func ToBranchTimestamp(ts *c.Timestamp) *BranchTimestamp {
	bTs := &BranchTimestamp{
		Bucket:  proto.String(ts.Bucket),
		Vbnos:   c.Vbno16to32(ts.Vbnos),
		Seqnos:  ts.Seqnos,
		Vbuuids: ts.Vbuuids,
	}

	// convert native snapshot to protobuf format.
	bTs.Snapshots = make([]*Snapshot, 0, len(ts.Snapshots))
	for _, s := range ts.Snapshots {
		bTs.Snapshots = append(
			bTs.Snapshots,
			&Snapshot{Start: proto.Uint64(s[0]), End: proto.Uint64(s[1])})
	}
	return bTs
}
