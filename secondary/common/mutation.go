// KV mutations are projected to secondary keys, based on index
// definitions, for each bucket. Projected secondary keys are defined by
// Mutation structure and transported as stream payload from,
//   projector -> router -> indexers & coordinator.

package common

import (
	"code.google.com/p/goprotobuf/proto"
	"github.com/couchbase/indexing/secondary/protobuf"
)

// List of possible mutation commands. Mutation messages are broadly divided
// into data and control messages. The division is based on the command field.
const (
	Upsert         byte = iota + 1 // data command
	Deletion                       // data command
	UpsertDeletion                 // data command
	Sync                           // control command
	StreamBegin                    // control command
	StreamEnd                      // control command
)

// Mutation structure describing KV mutations.
type Mutation struct {
	// Required fields
	Version byte // protocol version: TBD
	Command byte
	Vbucket uint16 // vbucket number
	Vbuuid  uint64 // unique id to detect branch history

	// Optional fields
	Docid    []byte   // primary document id
	Seqno    uint64   // vbucket sequence number for this mutation
	Keys     [][]byte // list of key-versions
	Oldkeys  [][]byte // previous key-versions, if available
	Indexids []uint32 // each key-version is for an active index defined on this bucket.
}

// Encode Mutation structure into protobuf array of bytes. Returned `data` can
// be transported to the other end and decoded back to Mutation structure.
func (m *Mutation) Encode() (data []byte, err error) {
	mp := protobuf.Mutation{
		Version: proto.Uint32(uint32(m.Version)),
		Command: proto.Uint32(uint32(m.Command)),
		Vbucket: proto.Uint32(uint32(m.Vbucket)),
		Vbuuid:  proto.Uint64(uint64(m.Vbuuid)),
	}
	if m.Docid != nil && len(m.Docid) > 0 {
		mp.Docid = m.Docid
	}
	if m.Seqno > 0 {
		mp.Seqno = proto.Uint64(m.Seqno)
	}
	if m.Keys != nil {
		mp.Keys = m.Keys
		mp.Oldkeys = m.Oldkeys
		mp.Indexids = m.Indexids
	}
	data, err = proto.Marshal(&mp)
	return
}

// Decode complements Encode() API. `data` returned by encode can be converted
// back to Mutation structure.
func (m *Mutation) Decode(data []byte) (err error) {
	mp := protobuf.Mutation{}
	if err = proto.Unmarshal(data, &mp); err != nil {
		return
	}
	m.Version = byte(mp.GetVersion())
	m.Command = byte(mp.GetCommand())
	m.Vbucket = uint16(mp.GetVbucket())
	m.Vbuuid = mp.GetVbuuid()
	m.Docid = mp.GetDocid()
	m.Seqno = mp.GetSeqno()
	m.Keys = mp.GetKeys()
	m.Oldkeys = mp.GetOldkeys()
	m.Indexids = mp.GetIndexids()
	return
}

// TBD: Yet to be defined. Just a place holder for now.
func ProtobufVersion() byte {
	return 1
}
