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
	"github.com/couchbase/indexing/secondary/common"
	"sync"
)

//MutationMeta represents meta information for a KV Mutation
type MutationMeta struct {
	bucket  string  //bucket for the mutation
	vbucket Vbucket //vbucket
	vbuuid  Vbuuid  //uuid for vbucket
	seqno   Seqno   //vbucket sequence number for this mutation
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

func (m *MutationMeta) Clone() *MutationMeta {
	meta := NewMutationMeta()
	meta.bucket = m.bucket
	meta.vbucket = m.vbucket
	meta.vbuuid = m.vbuuid
	meta.seqno = m.seqno
	return meta
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
	uuid     common.IndexInstId // index-id
	command  byte               // command the index
	key      []byte             // key-version for index
	oldkey   []byte             // previous key-version, if available
	partnkey []byte             // partition key
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

func (m *Mutation) Free() {
	if useMutationSyncPool {
		m.key = m.key[:0]
		m.oldkey = m.oldkey[:0]
		m.partnkey = m.partnkey[:0]
		mutPool.Put(m)
	}
}
