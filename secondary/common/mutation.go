// KV mutations are projected to secondary keys, based on index
// definitions, for each bucket. Projected secondary keys are defined by
// Mutation structure and transported as stream payload from,
//   projector -> router -> indexers & coordinator.

package common

import (
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	"github.com/couchbase/indexing/secondary/protobuf"
)

const (
	PAYLOAD_KEYVERSIONS byte = iota + 1
	PAYLOAD_VBMAP
)

// List of possible mutation commands. Mutation messages are broadly divided
// into data and control messages. The division is based on the command field.
const (
	Upsert         byte = iota + 1 // data command
	Deletion                       // data command
	UpsertDeletion                 // data command
	Sync                           // control command
	DropData                       // control command
	StreamBegin                    // control command
	StreamEnd                      // control command
)

// KeyVersions for each mutation from KV for a subset of index.
type KeyVersions struct {
	Command  byte
	Vbucket  uint16   // vbucket number
	Seqno    uint64   // vbucket sequence number for this mutation
	Vbuuid   uint64   // unique id to detect branch history
	Docid    []byte   // primary document id
	Keys     [][]byte // list of key-versions
	Oldkeys  [][]byte // previous key-versions, if available
	Indexids []uint32 // each key-version is for an active index defined on this bucket.
}

// Mutation message either carrying KeyVersions or protobuf.VbConnectionMap
type Mutation struct {
	version    byte // protocol Version: TBD
	keys       []*KeyVersions
	vbuckets   []uint16
	vbuuids    []uint64
	payltyp    byte
	maxKeyvers int
}

func NewMutation(maxKeyvers int) *Mutation {
	m := &Mutation{
		version:    ProtobufVersion(),
		keys:       make([]*KeyVersions, 0, maxKeyvers),
		maxKeyvers: maxKeyvers,
	}
	return m
}

func (m *Mutation) NewPayload(payltyp byte) {
	m.keys = m.keys[:0]
	m.payltyp = payltyp
}

func (m *Mutation) AddKeyVersions(k *KeyVersions) (err error) {
	if m.payltyp != PAYLOAD_KEYVERSIONS {
		return fmt.Errorf("expected key version for payload")
	} else if len(m.keys) == m.maxKeyvers {
		return fmt.Errorf("cannot pack anymore key-versions")
	}
	m.keys = append(m.keys, k)
	return nil
}

func (m *Mutation) SetVbuckets(vbuckets []uint16, vbuuids []uint64) (err error) {
	if m.payltyp != PAYLOAD_VBMAP {
		return fmt.Errorf("expected key version for payload")
	}
	m.vbuckets = vbuckets
	m.vbuuids = vbuuids
	return nil
}

func (m *Mutation) GetKeyVersion() []*KeyVersions {
	return m.keys
}

// Encode Mutation structure into protobuf array of bytes. Returned `data` can
// be transported to the other end and decoded back to Mutation structure.
func (m *Mutation) Encode() (data []byte, err error) {
	mp := protobuf.Mutation{
		Version: proto.Uint32(uint32(m.version)),
	}

	switch m.payltyp {
	case PAYLOAD_KEYVERSIONS:
		mp.Keys = make([]*protobuf.KeyVersions, 0, len(m.keys))
		if len(m.keys) == 0 {
			err = fmt.Errorf("empty mutation")
			break
		}
		for _, k := range m.keys {
			kp := &protobuf.KeyVersions{
				Command: proto.Uint32(uint32(k.Command)),
				Vbucket: proto.Uint32(uint32(k.Vbucket)),
				Vbuuid:  proto.Uint64(uint64(k.Vbuuid)),
			}
			if k.Docid != nil && len(k.Docid) > 0 {
				kp.Docid = k.Docid
			}
			if k.Seqno > 0 {
				kp.Seqno = proto.Uint64(k.Seqno)
			}
			if k.Keys != nil {
				kp.Keys = k.Keys
				kp.Oldkeys = k.Oldkeys
				kp.Indexids = k.Indexids
			}
			mp.Keys = append(mp.Keys, kp)
		}
	case PAYLOAD_VBMAP:
		vbuckets := make([]uint32, 0, len(m.vbuckets))
		for _, vb := range m.vbuckets {
			vbuckets = append(vbuckets, uint32(vb))
		}
		mp.Vbuckets = &protobuf.VbConnectionMap{
			Vbuuids:  m.vbuuids,
			Vbuckets: vbuckets,
		}
	}

	if err == nil {
		data, err = proto.Marshal(&mp)
	}
	return
}

// Decode complements Encode() API. `data` returned by encode can be converted
// back to either *protobuf.VbConnectionMap, or []*protobuf.KeyVersions
func (m *Mutation) Decode(data []byte) (interface{}, error) {
	var err error

	mp := protobuf.Mutation{}
	if err = proto.Unmarshal(data, &mp); err != nil {
		return nil, err
	}
	if ver := byte(mp.GetVersion()); ver != ProtobufVersion() {
		return nil, fmt.Errorf("mismatch in transport version %v", ver)
	}

	if vbuckets := mp.GetVbuckets(); vbuckets != nil {
		return vbuckets, nil
	} else if keys := mp.GetKeys(); keys != nil {
		return keys, nil
	}
	return nil, fmt.Errorf("mutation does not have payload")
}

// TBD: Yet to be defined. Just a place holder for now.
func ProtobufVersion() byte {
	return 1
}
