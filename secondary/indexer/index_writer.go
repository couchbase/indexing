// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package indexer

type IndexWriter interface {

	//Persist a key/value pair
	Insert(key Key, value Value) error

	//Delete a key/value pair by docId
	Delete(docid string) error

	//Commit the pending operations
	Commit() error

	//Snapshot
	Snapshot() (Snapshot, error)

	//Close the index. Should be able to reopen after this operation
	Close() error

	//Destroy/Wipe the index completely
	Destroy() error
}
