// - Transport independent library for mutation streaming.
// - Provide APIs to create KeyVersions.
//
// TODO: use slab allocated or memory pool to manage KeyVersions
// TODO: change KeyVersions command to a specific type.

package common

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/couchbase/indexing/secondary/logging"
	"strings"
)

// Stream Response Status Code
type StreamStatus byte

const (
	STREAM_SUCCESS StreamStatus = iota // 0
	STREAM_FAIL
	STREAM_ROLLBACK
)

// types of payload
const (
	PayloadKeyVersions byte = iota + 1
	PayloadVbmap
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
	Snapshot                       // control command
)

type ProjectorVersion byte

//Projector Version
const (
	ProjVer_5_1_0 ProjectorVersion = iota + 1
	ProjVer_5_1_1
	ProjVer_5_5_0
	ProjVer_6_5_0
)

// Payload either carries `vbmap` or `vbs`.
type Payload struct {
	Payltyp byte
	Vbmap   *VbConnectionMap
	Vbs     []*VbKeyVersions // for N number of vbuckets
}

// StreamID is unique id for a vbucket across buckets.
func StreamID(bucket string, vbno uint16) string {
	return bucket + fmt.Sprintf("#%v", vbno)
}

// NewStreamPayload returns a reference to payload, `nVb` provides the maximum
// number of vbuckets that can be carried by a payload.
func NewStreamPayload(payltyp byte, nVb int) *Payload {
	p := &Payload{
		Payltyp: payltyp,
		Vbs:     make([]*VbKeyVersions, 0, nVb),
	}
	return p
}

// Reset the payload structure for next transport.
func (p *Payload) Reset(payltyp byte) {
	p.Payltyp = payltyp
	p.Vbmap = nil
	p.Vbs = p.Vbs[:0]
}

// AddVbKeyVersions add a VbKeyVersions as payload, one or more VbKeyVersions
// can be added before transport.
func (p *Payload) AddVbKeyVersions(vb *VbKeyVersions) (err error) {
	if vb == nil || p.Payltyp != PayloadKeyVersions {
		return ErrorUnexpectedPayload
	}
	p.Vbs = append(p.Vbs, vb)
	return nil
}

// SetVbmap set vbmap as payload.
func (p *Payload) SetVbmap(bucket string, vbnos []uint16, vbuuids []uint64) error {
	if p.Payltyp != PayloadVbmap {
		return ErrorUnexpectedPayload
	}
	p.Vbmap = &VbConnectionMap{
		Bucket:   bucket,
		Vbuckets: vbnos,
		Vbuuids:  vbuuids,
	}
	return nil
}

// VbConnectionMap specifies list of vbuckets and current vbuuids for each
// vbucket.
type VbConnectionMap struct {
	Bucket   string
	Vbuckets []uint16
	Vbuuids  []uint64
}

// Equal compares to VbConnectionMap objects.
func (vbmap *VbConnectionMap) Equal(other *VbConnectionMap) bool {
	if vbmap.Bucket != other.Bucket {
		return false
	}
	if len(vbmap.Vbuckets) != len(other.Vbuckets) ||
		len(vbmap.Vbuuids) != len(other.Vbuuids) {
		return false
	}
	for i, vbno := range vbmap.Vbuckets {
		if vbno != other.Vbuckets[i] || vbmap.Vbuuids[i] != other.Vbuuids[i] {
			return false
		}
	}
	return true
}

// GetVbuuid returns vbuuid for specified vbucket-number from VbConnectionMap
// object.
func (vbmap *VbConnectionMap) GetVbuuid(vbno uint16) (uint64, error) {
	for i, num := range vbmap.Vbuckets {
		if num == vbno {
			return vbmap.Vbuuids[i], nil
		}
	}
	return 0, ErrorNotMyVbucket
}

// VbKeyVersions carries per vbucket key-versions for one or more mutations.
type VbKeyVersions struct {
	Bucket  string
	Vbucket uint16         // vbucket number
	Vbuuid  uint64         // unique id to detect branch history
	Kvs     []*KeyVersions // N number of mutations
	Uuid    string
	ProjVer ProjectorVersion
	Opaque2 uint64
}

// NewVbKeyVersions return a reference to a single vbucket payload
func NewVbKeyVersions(bucket string, vbno uint16,
	vbuuid uint64, opaque2 uint64, maxMutations int) *VbKeyVersions {
	vb := &VbKeyVersions{Bucket: bucket, Vbucket: vbno,
		Vbuuid: vbuuid, Opaque2: opaque2, ProjVer: ProjVer_6_5_0}
	vb.Kvs = make([]*KeyVersions, 0, maxMutations)
	vb.Uuid = StreamID(bucket, vbno)
	return vb
}

// AddKeyVersions will add KeyVersions for a single mutation.
func (vb *VbKeyVersions) AddKeyVersions(kv *KeyVersions) error {
	vb.Kvs = append(vb.Kvs, kv)
	return nil
}

// Equal compare equality of two VbKeyVersions object.
func (vb *VbKeyVersions) Equal(other *VbKeyVersions) bool {
	if vb.Vbucket != other.Vbucket ||
		vb.Vbuuid != other.Vbuuid {
		return false
	}
	if len(vb.Kvs) != len(other.Kvs) {
		return false
	}
	for i, kv := range vb.Kvs {
		if kv.Equal(other.Kvs[i]) == false {
			return false
		}
	}
	return true
}

// Free this object.
func (vb *VbKeyVersions) Free() {
	for _, kv := range vb.Kvs {
		kv.Free()
	}
	vb.Kvs = vb.Kvs[:0]
	// TODO: give `vb` back to pool
}

// FreeKeyVersions free mutations contained by this object.
func (vb *VbKeyVersions) FreeKeyVersions() {
	for _, kv := range vb.Kvs {
		kv.Free()
	}
	vb.Kvs = vb.Kvs[:0]
}

// KeyVersions for a single mutation from KV for a subset of index.
type KeyVersions struct {
	Seqno     uint64   // vbucket sequence number for this mutation
	Docid     []byte   // primary document id
	Uuids     []uint64 // list of unique ids, like index-ids
	Commands  []byte   // list of commands for each index
	Keys      [][]byte // list of key-versions for each index
	Oldkeys   [][]byte // previous key-versions, if available
	Partnkeys [][]byte // partition key for each key-version
	Ctime     int64
}

// NewKeyVersions return a reference KeyVersions for a single mutation.
func NewKeyVersions(seqno uint64, docid []byte, maxCount int, ctime int64) *KeyVersions {
	kv := &KeyVersions{Seqno: seqno}
	if docid != nil {
		kv.Docid = make([]byte, len(docid))
		copy(kv.Docid, docid)
	}

	kv.Uuids = make([]uint64, 0, maxCount)
	kv.Commands = make([]byte, 0, maxCount)
	kv.Keys = make([][]byte, 0, maxCount)
	kv.Oldkeys = make([][]byte, 0, maxCount)
	kv.Partnkeys = make([][]byte, 0, maxCount)
	kv.Ctime = ctime
	return kv
}

// addKey will add key-version for a single index.
func (kv *KeyVersions) addKey(uuid uint64, command byte, key, oldkey, pkey []byte) {
	kv.Uuids = append(kv.Uuids, uuid)
	kv.Commands = append(kv.Commands, command)
	kv.Keys = append(kv.Keys, key)
	kv.Oldkeys = append(kv.Oldkeys, oldkey)
	kv.Partnkeys = append(kv.Partnkeys, pkey)
}

// Equal compares for equality of two KeyVersions object.
func (kv *KeyVersions) Equal(other *KeyVersions) bool {
	if kv.Seqno != other.Seqno || bytes.Compare(kv.Docid, other.Docid) != 0 {
		return false
	}
	if len(kv.Uuids) != len(other.Uuids) {
		return false
	}
	for i, uuid := range kv.Uuids {
		if uuid != other.Uuids[i] ||
			kv.Commands[i] != other.Commands[i] ||
			bytes.Compare(kv.Keys[i], other.Keys[i]) != 0 ||
			bytes.Compare(kv.Oldkeys[i], other.Oldkeys[i]) != 0 ||
			bytes.Compare(kv.Partnkeys[i], other.Partnkeys[i]) != 0 {
			return false
		}
	}
	return true
}

// Free this object.
func (kv *KeyVersions) Free() {
	// TODO: give `kv` back to pool
}

// Length number of key-versions are stored.
func (kv *KeyVersions) Length() int {
	return len(kv.Uuids)
}

// AddUpsert add a new keyversion for same OpMutation.
func (kv *KeyVersions) AddUpsert(uuid uint64, key, oldkey, pkey []byte) {
	kv.addKey(uuid, Upsert, key, oldkey, pkey)
}

// AddDeletion add a new keyversion for same OpDeletion.
func (kv *KeyVersions) AddDeletion(uuid uint64, oldkey, pkey []byte) {
	kv.addKey(uuid, Deletion, nil, oldkey, pkey)
}

// AddUpsertDeletion add a keyversion command to delete old entry.
func (kv *KeyVersions) AddUpsertDeletion(uuid uint64, oldkey, pkey []byte) {
	kv.addKey(uuid, UpsertDeletion, nil, oldkey, pkey)
}

// AddSync add Sync command for vbucket heartbeat.
func (kv *KeyVersions) AddSync() {
	kv.addKey(0, Sync, nil, nil, nil)
}

// AddDropData add DropData command for trigger downstream catchup.
func (kv *KeyVersions) AddDropData() {
	kv.addKey(0, DropData, nil, nil, nil)
}

// AddStreamBegin add StreamBegin command for a new vbucket.
func (kv *KeyVersions) AddStreamBegin(status byte, code byte) {
	kv.addKey(0, StreamBegin, []byte{status, code}, nil, nil)
}

// AddStreamEnd add StreamEnd command for a vbucket shutdown.
func (kv *KeyVersions) AddStreamEnd() {
	kv.addKey(0, StreamEnd, nil, nil, nil)
}

// AddSnapshot add Snapshot command for a vbucket shutdown.
// * type is sent via uuid field
// * start and end values are big-ending encoded to as key and old-key
func (kv *KeyVersions) AddSnapshot(typ uint32, start, end uint64) {
	var key, okey [8]byte
	binary.BigEndian.PutUint64(key[:8], start)
	binary.BigEndian.PutUint64(okey[:8], end)
	kv.addKey(uint64(typ), Snapshot, key[:8], okey[:8], nil)
}

func (kv *KeyVersions) GetSnapshot() (uint32, uint64, uint64) {
	start := binary.BigEndian.Uint64(kv.Keys[0])
	end := binary.BigEndian.Uint64(kv.Oldkeys[0])
	typ := uint32(kv.Uuids[0])
	return typ, start, end
}

func (kv *KeyVersions) String() string {
	s := fmt.Sprintf("`%s` - Seqno:%v\n", string(kv.Docid), kv.Seqno)
	for i, uuid := range kv.Uuids {
		s += fmt.Sprintf("    %v Cmd(%v) `%s`",
			uuid, kv.Commands[i], string(kv.Keys[i]))
	}
	return s
}

func (kv *KeyVersions) GetDebugInfo() string {
	s := fmt.Sprintf("Docidx %v, Seqno %v, Ctime %v, Uuids %v, Commands %v",
		logging.TagStrUD(kv.Docid), kv.Seqno, kv.Ctime, kv.Uuids, kv.Commands)

	s += "\n"

	// Add Keys
	ss := make([]string, 0)
	for _, k := range kv.Keys {
		ss = append(ss, fmt.Sprintf("Key %v", logging.TagStrUD(k)))
	}
	s += strings.Join(ss, ", ")

	s += "\n"

	// Add OldKeys
	ss = make([]string, 0)
	for _, k := range kv.Oldkeys {
		ss = append(ss, fmt.Sprintf("Key %v", logging.TagStrUD(k)))
	}
	s += strings.Join(ss, ", ")

	// AddPartnKeys
	ss = make([]string, 0)
	for _, k := range kv.Partnkeys {
		ss = append(ss, fmt.Sprintf("Key %v", logging.TagStrUD(k)))
	}
	s += strings.Join(ss, ", ")

	return s
}

// DataportKeyVersions accepted by this endpoint.
type DataportKeyVersions struct {
	Bucket  string
	Vbno    uint16
	Vbuuid  uint64
	Kv      *KeyVersions
	Opaque2 uint64
}
