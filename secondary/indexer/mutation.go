//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package indexer

import (
	"fmt"
	"sync"

	c "github.com/couchbase/indexing/secondary/common"
)

// MutationMeta represents meta information for a KV Mutation
type MutationMeta struct {
	keyspaceId string  //keyspaceId for the mutation
	vbucket    Vbucket //vbucket
	vbuuid     Vbuuid  //uuid for vbucket
	seqno      uint64  //vbucket sequence number for this mutation
	firstSnap  bool    //belongs to first DCP snapshot
	projVer    c.ProjectorVersion
	opaque     uint64
}

var mutMetaPool = sync.Pool{New: newMutationMeta}
var useMutationSyncPool bool = false

func NewMutationMeta() *MutationMeta {
	if useMutationSyncPool {
		return mutMetaPool.Get().(*MutationMeta)
	}

	return &MutationMeta{}
}

func newMutationMeta() interface{} {
	return &MutationMeta{}
}

func (m *MutationMeta) SetVBId(vbid int) {
	m.vbucket = Vbucket(vbid)
}

func (m *MutationMeta) Reset() {
	m.keyspaceId = ""
	m.vbucket = 0
	m.vbuuid = 0
	m.seqno = 0
	m.firstSnap = false
	m.projVer = c.ProjVer_5_1_0
	m.opaque = 0
}

func (m *MutationMeta) Clone() *MutationMeta {
	meta := NewMutationMeta()
	meta.keyspaceId = m.keyspaceId
	meta.vbucket = m.vbucket
	meta.vbuuid = m.vbuuid
	meta.seqno = m.seqno
	meta.firstSnap = m.firstSnap
	meta.projVer = m.projVer
	meta.opaque = m.opaque
	return meta
}

func (m *MutationMeta) Size() int64 {

	size := int64(len(m.keyspaceId))
	size += 8 + 4 + 8 + 8 + 8 //fixed cost of members
	return size

}

func (m *MutationMeta) Free() {
	if useMutationSyncPool {
		mutMetaPool.Put(m)
	}
}

func (m MutationMeta) String() string {

	str := fmt.Sprintf("KeyspaceId: %v ", m.keyspaceId)
	str += fmt.Sprintf("Vbucket: %v ", m.vbucket)
	str += fmt.Sprintf("Vbuuid: %v ", m.vbuuid)
	str += fmt.Sprintf("Seqno: %v ", m.seqno)
	str += fmt.Sprintf("FirstSnap: %v ", m.firstSnap)
	return str

}

// MutationKeys holds the Secondary Keys from a single KV Mutation
type MutationKeys struct {
	meta  *MutationMeta
	docid []byte      // primary document id
	mut   []*Mutation //list of mutations for each index-id
	size  int64       // Total size of all the contents of MutationKeys
}

var mutkeysPool = sync.Pool{New: newMutationKeys}

func NewMutationKeys() *MutationKeys {
	if useMutationSyncPool {
		return mutkeysPool.Get().(*MutationKeys)
	}

	return &MutationKeys{}
}

func newMutationKeys() interface{} {
	return &MutationKeys{}
}

func (mk *MutationKeys) Size() int64 {

	var size int64
	size = mk.meta.Size()
	size += int64(len(mk.docid))
	for _, m := range mk.mut {
		size += m.Size()
	}

	size += 8 + 16 + 16 //fixed cost of members
	return size
}

func (mk *MutationKeys) Free() {
	if useMutationSyncPool {
		mk.meta.Free()
		mk.docid = mk.docid[:0]
		for _, m := range mk.mut {
			m.Free()
		}
		mk.mut = mk.mut[:0]
		mutkeysPool.Put(mk)
	}
}

type Mutation struct {
	uuid        c.IndexInstId // index-id
	command     byte          // command the index
	key         []byte        // key-version for index
	oldkey      []byte        // previous key-version, if available
	partnkey    []byte        // partition key
	vectors     [][]float32   // Array of vector embeddings for vector indexes
	centroidPos []int32       // Position of the centroidId in the encoded key
}

var mutPool = sync.Pool{New: newMutation}

func NewMutation() *Mutation {
	if useMutationSyncPool {
		return mutPool.Get().(*Mutation)
	}

	return &Mutation{}
}

func newMutation() interface{} {
	return &Mutation{}
}

func (m *Mutation) Size() int64 {

	var size int64
	size = int64(len(m.key))
	size += int64(len(m.partnkey))
	size += 8 + 1        //instId + command
	size += 16 + 16 + 16 //fixed cost of members

	if m.vectors != nil {
		size += 16 // For slice of vectors
		if len(m.vectors) >= 0 {
			size += int64((16 + (4 * len(m.vectors[0]))) * len(m.vectors))
		}
	}
	return size

}

func (m *Mutation) Free() {
	if useMutationSyncPool {
		m.key = m.key[:0]
		m.oldkey = m.oldkey[:0]
		m.partnkey = m.partnkey[:0]
		if m.vectors != nil {
			m.vectors = m.vectors[:0]
		}
		mutPool.Put(m)
	}
}
