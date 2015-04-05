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
)

//MutationMeta represents meta information for a KV Mutation
type MutationMeta struct {
	bucket  string  //bucket for the mutation
	vbucket Vbucket //vbucket
	vbuuid  Vbuuid  //uuid for vbucket
	seqno   Seqno   //vbucket sequence number for this mutation
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

type Mutation struct {
	uuid     common.IndexInstId // index-id
	command  byte               // command the index
	key      []byte             // key-version for index
	oldkey   []byte             // previous key-version, if available
	partnkey []byte             // partition key
}
