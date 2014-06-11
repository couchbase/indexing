// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package indexer

import (
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbaselabs/goforestdb"
	"log"
)

type fdbSlice struct {
	name string
	id   SliceId //slice id

	main *forestdb.Database //db handle for forward index
	back *forestdb.Database //db handle for reverse index

	idxDefnId common.IndexDefnId
	idxInstId common.IndexInstId

	status   SliceStatus
	isActive bool

	sc SnapshotContainer //snapshot container
}

func NewForestDBSlice(name string, sliceId SliceId, idxDefnId common.IndexDefnId,
	idxInstId common.IndexInstId) (*fdbSlice, error) {

	slice := &fdbSlice{}

	var err error
	if slice.main, err = goforestdb.Open(name); err != nil {
		return nil, err
	}

	//create a separate back-index
	if slice.back, err = goforestdb.Open(name + "_back"); err != nil {
		return nil, err
	}

	slice.name = name
	slice.idxInstId = indexInstId
	slice.idxDefnId = idxDefnId
	slice.id = sliceId

	slice.sc = NewSnapshotContainer()

	return slice, nil
}

func (s *fdbSlice) Id() SliceId {
	return s.id
}

func (s *fdbSlice) Name() string {
	return s.name
}

func (s *fdbSlice) Status() SliceStatus {
	return s.status
}

func (s *fdbSlice) IndexInstId() common.IndexInstId {
	return s.idxInstId
}

func (s *fdbSlice) IndexDefnId() common.IndexDefnId {
	return s.idxDefnId
}

func (s *fdbSlice) IsActive() bool {
	return s.isActive
}

func (s *fdbSlice) SetActive(isActive bool) {
	s.isActive = isActive
}

func (s *fdbSlice) SetStatus(status SliceStatus) {
	s.status = status
}

func (s *fdbSlice) GetSnapshotContainer() SnapshotContainer {
	return s.sc
}

//Persist a key/value pair
func (fdb *fdbSlice) Insert(k Key, v Value) error {
	var err error
	var oldkey Key

	log.Printf("ForestDBSlice: Set Key - %s Value - %s", k.String(), v.String())

	//check if the docid exists in the back index
	if oldkey, err = fdb.getBackIndexEntry(v.Docid()); err != nil {
		log.Printf("ForestDBSlice: Error locating backindex entry %v", err)
		return err
	} else if oldkey.EncodedBytes() != nil {
		//there is already an entry in main index for this docid
		//delete from main index
		if err = fdb.main.DeleteKV(oldkey.EncodedBytes()); err != nil {
			log.Printf("ForestDBSlice: Error deleting entry from main index %v", err)
			return err
		}
	}

	//if secondary-key is nil, no further processing is required. If this was a KV insert,
	//nothing needs to be done./if this was a KV update, only delete old back/main index entry
	if v.KeyBytes() == nil {
		log.Printf("ForestDBSlice: Received NIL secondary key. Skipping Index Insert.")
		return nil
	}

	//TODO: Handle the case if old-value from backindex matches with the
	//new-value(false mutation). Skip It.

	//set the back index entry <docid, encodedkey>
	if err = fdb.back.SetKV([]byte(v.Docid()), k.EncodedBytes()); err != nil {
		return err
	}

	//set in main index
	if err = fdb.main.SetKV(k.EncodedBytes(), v.EncodedBytes()); err != nil {
		return err
	}

	return err
}

//Delete a key/value pair by docId
func (fdb *fdbSlice) Delete(docid string) error {
	log.Printf("ForestDBSlice: Delete Key - %s", docid)

	var oldkey Key
	var err error

	if oldkey, err = fdb.getBackIndexEntry(docid); err != nil {
		log.Printf("ForestDBSlice: Error locating backindex entry %v", err)
		return err
	}

	//delete from main index
	if err = fdb.main.DeleteKV(oldkey.EncodedBytes()); err != nil {
		log.Printf("ForestDBSlice: Error deleting entry from main index %v", err)
		return err
	}

	//delete from the back index
	if err = fdb.back.DeleteKV([]byte(docid)); err != nil {
		log.Printf("ForestDBSlice: Error deleting entry from back index %v", err)
		return err
	}

	return nil
}

//Get an existing key/value pair by key
func (fdb *fdbSlice) getBackIndexEntry(docid string) (Key, error) {

	var k Key
	var kbyte []byte
	var err error

	log.Printf("ForestDBSlice: Get BackIndex Key - %s", docid)

	if kbyte, err = fdb.back.GetKV([]byte(docid)); err != nil {
		return k, err
	}

	k, err = NewKeyFromEncodedBytes(kbyte)

	return k, err
}

//Snapshot
func (fdb *fdbSlice) Snapshot() (Snapshot, error) {

	s := &fdbSnapshot{id: slice.Id(),
		idxDefnId: slice.IndexDefnId(),
		idxInstId: slice.IndexInstanceId()}

	//store snapshot seqnum for main index
	{
		i, err := fdb.main.DbInfo()
		if err != nil {
			return nil, err
		}
		seq := i.LastSeqNum()
		s.mainSeqnum = seq
	}

	//store snapshot seqnum for back index
	{
		i, err := fdb.back.DbInfo()
		if err != nil {
			return nil, err
		}
		seq := i.LastSeqNum()
		s.backSeqnum = seq
	}
	return s, err
}

//Commit
func (fdb *fdbSlice) Commit() error {

	var err error
	//Commit the back index
	if err = fdb.back.Commit(); err != nil {
		//TODO: what else needs to be done here
		return err
	}
	//Commit the main index
	if err = fdb.main.Commit(); err != nil {
		return err
	}
	return nil
}

//Close the db. Should be able to reopen after this operation
func (fdb *fdbSlice) Close() error {

	//close the main index
	if fdb.main != nil {
		fdb.main.Close()
	}
	//close the back index
	if fdb.back != nil {
		fdb.back.Close()
	}
	return nil
}
