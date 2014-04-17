// Transport independent library for mutation streaming.
//
// Provide APIs to create KeyVersions.
//
// TODO: use slab allocated or memory pool to manage KeyVersions

package common

import (
	"fmt"
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

type VbConnectionMap struct {
	Vbuckets []uint16
	Vbuuids  []uint64
}

// Mutation message either carrying KeyVersions or protobuf.VbConnectionMap
type Mutation struct {
	keys       []*KeyVersions
	vbmap      VbConnectionMap
	payltyp    byte
	maxKeyvers int
}

// NewMutation creates internal mutation Mutation object that can be used to
// compose or de-compose on the wire mutation messages.
// Same object can be re-used to compose or de-compose messages.
func NewMutation(maxKeyvers int) *Mutation {
	m := &Mutation{
		keys:       make([]*KeyVersions, 0, maxKeyvers),
		maxKeyvers: maxKeyvers,
	}
	return m
}

// NewPayload resets the mutation object for next payload.
func (m *Mutation) NewPayload(payltyp byte) {
	m.keys = m.keys[:0]
	m.payltyp = payltyp
	m.vbmap.Vbuckets, m.vbmap.Vbuuids = nil, nil
}

// AddKeyVersions will add a KeyVersions to current Mutation payload.
// Note that SetVbuckets() and AddKeyVersions() cannot be used together as
// payload.
func (m *Mutation) AddKeyVersions(k *KeyVersions) (err error) {
	if m.payltyp != PAYLOAD_KEYVERSIONS {
		return fmt.Errorf("expected key version for payload")
	} else if len(m.keys) == m.maxKeyvers {
		return fmt.Errorf("cannot pack anymore key-versions")
	}
	m.keys = append(m.keys, k)
	return nil
}

// SetVbuckets will set a list of vbuckets and corresponding vbuuids as
// payload for next Mutation Message.
// Note that SetVbuckets() and AddKeyVersions() cannot be used together as
// payload.
func (m *Mutation) SetVbuckets(vbuckets []uint16, vbuuids []uint64) (err error) {
	if m.payltyp != PAYLOAD_VBMAP {
		return fmt.Errorf("expected key version for payload")
	}
	m.vbmap.Vbuckets = vbuckets
	m.vbmap.Vbuuids = vbuuids
	return nil
}

// GetKeyVersions return the list of reference to current payload's KeyVersions.
func (m *Mutation) GetKeyVersions() []*KeyVersions {
	return m.keys
}

// GetVbuckets return the list of vbuckets and corresponding vbuuids on this
func (m *Mutation) GetVbmap() VbConnectionMap {
	return m.vbmap
}

// NewUpsert construct a Upsert message corresponding to KV's MUTATION
// message. Caller's responsibility to initialize `Keys`, `Oldkeys` and
// `Indexids`  as applicable.
func NewUpsert(vb uint16, vbuuid uint64, docid []byte, seqno uint64) *KeyVersions {
	return &KeyVersions{
		Command: Upsert,
		Vbucket: vb,
		Vbuuid:  vbuuid,
		Docid:   docid,
		Seqno:   seqno,
	}
}

// NewDeletion construct a Deletion message corresponding to KV's DELETION
// message. Caller's responsibility to initialize `Indexids`.
func NewDeletion(vb uint16, vbuuid uint64, docid []byte, seqno uint64) *KeyVersions {
	return &KeyVersions{
		Command: Deletion,
		Vbucket: vb,
		Vbuuid:  vbuuid,
		Docid:   docid,
		Seqno:   seqno,
	}
}

// NewUpsertDeletion construct a UpsertDeletion message. It is locally
// generated message to delete older key version. Caller's responsibility to
// initialize `Keys` and `Indexids`
func NewUpsertDeletion(vb uint16, vbuuid uint64, docid []byte, seqno uint64) *KeyVersions {
	return &KeyVersions{
		Command: UpsertDeletion,
		Vbucket: vb,
		Vbuuid:  vbuuid,
		Docid:   docid,
		Seqno:   seqno,
	}
}

// NewSync construct a Sync control message.
func NewSync(vb uint16, vbuuid uint64, seqno uint64) *KeyVersions {
	return &KeyVersions{
		Command: Sync,
		Vbucket: vb,
		Vbuuid:  vbuuid,
		Seqno:   seqno,
	}
}

// NewDropData construct a DropData control message.
func NewDropData(vb uint16, vbuuid uint64, seqno uint64) *KeyVersions {
	return &KeyVersions{
		Command: DropData,
		Vbucket: vb,
		Vbuuid:  vbuuid,
		Seqno:   seqno,
	}
}

// NewStreamBegin construct a StreamBegin control message.
func NewStreamBegin(vb uint16, vbuuid uint64, seqno uint64) *KeyVersions {
	return &KeyVersions{
		Command: StreamBegin,
		Vbucket: vb,
		Vbuuid:  vbuuid,
		Seqno:   seqno,
	}
}

// NewStreamEnd construct a StreamEnd control message.
func NewStreamEnd(vb uint16, vbuuid uint64, seqno uint64) *KeyVersions {
	return &KeyVersions{
		Command: StreamEnd,
		Vbucket: vb,
		Vbuuid:  vbuuid,
		Seqno:   seqno,
	}
}
