//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package indexer

import (
	"fmt"
	"sync"

	c "github.com/couchbase/indexing/secondary/common"
)

//MutationMeta represents meta information for a KV Mutation
type MutationMeta struct {
	bucket    string  //bucket for the mutation
	vbucket   Vbucket //vbucket
	vbuuid    Vbuuid  //uuid for vbucket
	seqno     Seqno   //vbucket sequence number for this mutation
	projVer   c.ProjectorVersion
	firstSnap bool //belongs to first DCP snapshot
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

func (m *MutationMeta) Clone() *MutationMeta {
	meta := NewMutationMeta()
	meta.bucket = m.bucket
	meta.vbucket = m.vbucket
	meta.vbuuid = m.vbuuid
	meta.seqno = m.seqno
	meta.projVer = m.projVer
	meta.firstSnap = m.firstSnap
	return meta
}

func (m *MutationMeta) Size() int64 {

	size := int64(len(m.bucket))
	size += 8 + 4 + 8 + 8 //fixed cost of members
	return size

}

func (m *MutationMeta) Free() {
	if useMutationSyncPool {
		mutMetaPool.Put(m)
	}
}

func (m MutationMeta) String() string {

	str := fmt.Sprintf("Bucket: %v ", m.bucket)
	str += fmt.Sprintf("Vbucket: %v ", m.vbucket)
	str += fmt.Sprintf("Vbuuid: %v ", m.vbuuid)
	str += fmt.Sprintf("Seqno: %v ", m.seqno)
	str += fmt.Sprintf("FirstSnap: %v ", m.firstSnap)
	return str

}

//MutationKeys holds the Secondary Keys from a single KV Mutation
type MutationKeys struct {
	meta  *MutationMeta
	docid []byte      // primary document id
	mut   []*Mutation //list of mutations for each index-id
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
	uuid     c.IndexInstId // index-id
	command  byte          // command the index
	key      []byte        // key-version for index
	oldkey   []byte        // previous key-version, if available
	partnkey []byte        // partition key
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
	return size

}

func (m *Mutation) Free() {
	if useMutationSyncPool {
		m.key = m.key[:0]
		m.oldkey = m.oldkey[:0]
		m.partnkey = m.partnkey[:0]
		mutPool.Put(m)
	}
}
