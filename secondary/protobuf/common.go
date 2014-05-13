package protobuf

import (
	"code.google.com/p/goprotobuf/proto"
	c "github.com/couchbase/indexing/secondary/common"
)

// NewError create a protobuf message `Error` and return its reference back to
// the caller.
func NewError(err error) *Error {
	return &Error{Error: proto.String(err.Error())}
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

// transform protobuf's BranchTimestamp to local timestamp.
func ToTimestamp(ts *BranchTimestamp) *c.Timestamp {
	t := c.NewTimestamp(ts.GetBucket(), 1024) // TODO: no magic numbers
	t.Vbnos = c.Vbno32to16(ts.GetVbnos())
	t.Seqnos = ts.GetSeqnos()
	t.Vbuuids = ts.GetVbuuids()
	return t
}

// transform local timestamp to protobuf's BranchTimestamp
func ToBranchTimestamp(ts *c.Timestamp) *BranchTimestamp {
	return &BranchTimestamp{
		Bucket:  proto.String(ts.Bucket),
		Vbnos:   c.Vbno16to32(ts.Vbnos),
		Seqnos:  ts.Seqnos,
		Vbuuids: ts.Vbuuids,
	}
}
