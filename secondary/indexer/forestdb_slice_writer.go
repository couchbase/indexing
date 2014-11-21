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
	"encoding/json"
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbaselabs/goforestdb"
	"time"
)

const STABILITY_TS_KEY_NAME = "stability-ts"

//NewForestDBSlice initiailizes a new slice with forestdb backend.
//Both main and back index gets initialized with default config.
//Slice methods are not thread-safe and application needs to
//handle the synchronization. The only exception being Insert and
//Delete can be called concurrently.
//Returns error in case slice cannot be initialized.
func NewForestDBSlice(name string, sliceId SliceId, idxDefnId common.IndexDefnId,
	idxInstId common.IndexInstId) (*fdbSlice, error) {

	slice := &fdbSlice{}

	var err error

	config := forestdb.DefaultConfig()
	config.SetDurabilityOpt(forestdb.DRB_ASYNC)
	kvconfig := forestdb.DefaultKVStoreConfig()

	if slice.dbfile, err = forestdb.Open(name, config); err != nil {
		return nil, err
	}

	slice.main = make([]*forestdb.KVStore, NUM_WRITER_THREADS_PER_SLICE)
	for i := 0; i < NUM_WRITER_THREADS_PER_SLICE; i++ {
		if slice.main[i], err = slice.dbfile.OpenKVStore("main", kvconfig); err != nil {
			return nil, err
		}
	}

	//create a separate back-index
	slice.back = make([]*forestdb.KVStore, NUM_WRITER_THREADS_PER_SLICE)
	for i := 0; i < NUM_WRITER_THREADS_PER_SLICE; i++ {
		if slice.back[i], err = slice.dbfile.OpenKVStore("back", kvconfig); err != nil {
			return nil, err
		}
	}

	// Make use of default kvstore provided by forestdb
	if slice.meta, err = slice.dbfile.OpenKVStore("default", kvconfig); err != nil {
		return nil, err
	}

	slice.name = name
	slice.idxInstId = idxInstId
	slice.idxDefnId = idxDefnId
	slice.id = sliceId

	slice.sc = NewSnapshotContainer()

	slice.cmdCh = make(chan interface{}, SLICE_COMMAND_BUFFER_SIZE)
	slice.workerDone = make([]chan bool, NUM_WRITER_THREADS_PER_SLICE)
	slice.stopCh = make([]DoneChannel, NUM_WRITER_THREADS_PER_SLICE)

	for i := 0; i < NUM_WRITER_THREADS_PER_SLICE; i++ {
		slice.stopCh[i] = make(DoneChannel)
		slice.workerDone[i] = make(chan bool)
		go slice.handleCommandsWorker(i)
	}

	common.Debugf("ForestDBSlice:NewForestDBSlice \n\t Created New Slice Id %v IndexInstId %v "+
		"WriterThreads %v", sliceId, idxInstId, NUM_WRITER_THREADS_PER_SLICE)

	return slice, nil
}

//kv represents a key/value pair in storage format
type kv struct {
	k Key
	v Value
}

//fdbSlice represents a forestdb slice
type fdbSlice struct {
	name string
	id   SliceId //slice id

	dbfile *forestdb.File
	meta   *forestdb.KVStore   // handle for index meta
	main   []*forestdb.KVStore // handle for forward index
	back   []*forestdb.KVStore // handle for reverse index

	idxDefnId common.IndexDefnId
	idxInstId common.IndexInstId

	status   SliceStatus
	isActive bool

	sc SnapshotContainer //snapshot container

	cmdCh  chan interface{} //internal channel to buffer commands
	stopCh []DoneChannel    //internal channel to signal shutdown

	workerDone []chan bool //worker status check channel

	fatalDbErr error //store any fatal DB error

	ts *common.TsVbuuid //timestamp for this slice

	//TODO: Remove this once these stats are
	//captured by the stats library
	totalFlushTime  time.Duration
	totalCommitTime time.Duration
}

//Insert will insert the given key/value pair from slice.
//Internally the request is buffered and executed async.
//If forestdb has encountered any fatal error condition,
//it will be returned as error.
func (fdb *fdbSlice) Insert(k Key, v Value) error {

	fdb.cmdCh <- kv{k: k, v: v}
	return fdb.fatalDbErr

}

//Delete will delete the given document from slice.
//Internally the request is buffered and executed async.
//If forestdb has encountered any fatal error condition,
//it will be returned as error.
func (fdb *fdbSlice) Delete(docid []byte) error {

	fdb.cmdCh <- docid
	return fdb.fatalDbErr

}

//handleCommands keep listening to any buffered
//write requests for the slice and processes
//those. This will shut itself down internal
//shutdown channel is closed.
func (fdb *fdbSlice) handleCommandsWorker(workerId int) {

loop:
	for {
		select {
		case c := <-fdb.cmdCh:
			switch c.(type) {
			case kv:
				cmd := c.(kv)
				start := time.Now()
				fdb.insert(cmd.k, cmd.v, workerId)
				elapsed := time.Since(start)
				fdb.totalFlushTime += elapsed
			case []byte:
				cmd := c.([]byte)
				start := time.Now()
				fdb.delete(cmd, workerId)
				elapsed := time.Since(start)
				fdb.totalFlushTime += elapsed
			default:
				common.Errorf("ForestDBSlice::handleCommandsWorker \n\tSliceId %v IndexInstId %v Received "+
					"Unknown Command %v", fdb.id, fdb.idxInstId, c)
			}

		case <-fdb.stopCh[workerId]:
			fdb.stopCh[workerId] <- true
			break loop

			//worker gets a status check message on this channel, it responds
			//when its not processing any mutation
		case <-fdb.workerDone[workerId]:
			fdb.workerDone[workerId] <- true

		}
	}

}

//insert does the actual insert in forestdb
func (fdb *fdbSlice) insert(k Key, v Value, workerId int) {

	var err error
	var oldkey Key

	common.Tracef("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Set Key - %s "+
		"Value - %s", fdb.id, fdb.idxInstId, k, v)

	//check if the docid exists in the back index
	if oldkey, err = fdb.getBackIndexEntry(v.Docid(), workerId); err != nil {
		fdb.checkFatalDbError(err)
		common.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error locating "+
			"backindex entry %v", fdb.id, fdb.idxInstId, err)
		return
	} else if oldkey.Encoded() != nil {
		//TODO: Handle the case if old-value from backindex matches with the
		//new-value(false mutation). Skip It.

		//there is already an entry in main index for this docid
		//delete from main index
		if err = fdb.main[workerId].DeleteKV(oldkey.Encoded()); err != nil {
			fdb.checkFatalDbError(err)
			common.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error deleting "+
				"entry from main index %v", fdb.id, fdb.idxInstId, err)
			return
		}

		//delete from back index
		if err = fdb.back[workerId].DeleteKV(v.Docid()); err != nil {
			fdb.checkFatalDbError(err)
			common.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error deleting "+
				"entry from back index %v", fdb.id, fdb.idxInstId, err)
			return
		}
	}

	//if the Key is nil, nothing needs to be done
	if k.Encoded() == nil {
		common.Tracef("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Received NIL Key for "+
			"Doc Id %v. Skipped.", fdb.id, fdb.idxInstId, v.Docid())
		return
	}

	//set the back index entry <docid, encodedkey>
	if err = fdb.back[workerId].SetKV([]byte(v.Docid()), k.Encoded()); err != nil {
		fdb.checkFatalDbError(err)
		common.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error in Back Index Set. "+
			"Skipped Key %s. Value %s. Error %v", fdb.id, fdb.idxInstId, v, k, err)
		return
	}

	//set in main index
	if err = fdb.main[workerId].SetKV(k.Encoded(), v.Encoded()); err != nil {
		fdb.checkFatalDbError(err)
		common.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error in Main Index Set. "+
			"Skipped Key %s. Value %s. Error %v", fdb.id, fdb.idxInstId, k, v, err)
		return
	}

}

//delete does the actual delete in forestdb
func (fdb *fdbSlice) delete(docid []byte, workerId int) {

	common.Tracef("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Delete Key - %s",
		fdb.id, fdb.idxInstId, docid)

	var oldkey Key
	var err error

	if oldkey, err = fdb.getBackIndexEntry(docid, workerId); err != nil {
		fdb.checkFatalDbError(err)
		common.Errorf("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Error locating "+
			"backindex entry for Doc %s. Error %v", fdb.id, fdb.idxInstId, docid, err)
		return
	}

	//if the oldkey is nil, nothing needs to be done. This is the case of deletes
	//which happened before index was created.
	if oldkey.Encoded() == nil {
		common.Tracef("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v Received NIL Key for "+
			"Doc Id %v. Skipped.", fdb.id, fdb.idxInstId, docid)
		return
	}

	//delete from main index
	if err = fdb.main[workerId].DeleteKV(oldkey.Encoded()); err != nil {
		fdb.checkFatalDbError(err)
		common.Errorf("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Error deleting "+
			"entry from main index for Doc %s. Key %v. Error %v", fdb.id, fdb.idxInstId,
			docid, oldkey, err)
		return
	}

	//delete from the back index
	if err = fdb.back[workerId].DeleteKV(docid); err != nil {
		fdb.checkFatalDbError(err)
		common.Errorf("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Error deleting "+
			"entry from back index for Doc %s. Error %v", fdb.id, fdb.idxInstId, docid, err)
		return
	}

}

//getBackIndexEntry returns an existing back index entry
//given the docid
func (fdb *fdbSlice) getBackIndexEntry(docid []byte, workerId int) (Key, error) {

	common.Tracef("ForestDBSlice::getBackIndexEntry \n\tSliceId %v IndexInstId %v Get BackIndex Key - %s",
		fdb.id, fdb.idxInstId, docid)

	var k Key
	var kbyte []byte
	var err error

	kbyte, err = fdb.back[workerId].GetKV([]byte(docid))

	//forestdb reports get in a non-existent key as an
	//error, skip that
	if err != nil && err.Error() != "key not found" {
		return k, err
	}

	k, err = NewKeyFromEncodedBytes(kbyte)

	return k, err
}

//checkFatalDbError checks if the error returned from DB
//is fatal and stores it. This error will be returned
//to caller on next DB operation
func (fdb *fdbSlice) checkFatalDbError(err error) {

	errStr := err.Error()
	switch errStr {

	case "checksum error", "file corruption", "no db instance",
		"alloc fail", "seek fail", "fsync fail":
		fdb.fatalDbErr = err

	}

}

//Snapshot creates a new snapshot from the latest
//committed data available in forestdb. Caller must
//first call Commit to ensure there is no outstanding
//write before calling Snapshot. Returns error if
//received from forestdb.
func (fdb *fdbSlice) Snapshot() (Snapshot, error) {

	s := &fdbSnapshot{id: fdb.id,
		idxDefnId: fdb.idxDefnId,
		idxInstId: fdb.idxInstId,
		main:      fdb.main[0],
		back:      fdb.back[0],
		ts:        fdb.ts}

	//store snapshot seqnum for main index
	{
		i, err := fdb.main[0].Info()
		if err != nil {
			return nil, err
		}
		seq := i.LastSeqNum()
		s.mainSeqNum = seq
	}

	//store snapshot seqnum for back index
	{
		i, err := fdb.back[0].Info()
		if err != nil {
			return nil, err
		}
		seq := i.LastSeqNum()
		s.backSeqNum = seq
	}

	common.Debugf("ForestDBSlice::Snapshot \n\tSliceId %v IndexInstId %v Created New "+
		"Snapshot %v", fdb.id, fdb.idxInstId, s)

	return s, nil
}

//Rollback slice to given snapshot. Return error if
//not possible
func (fdb *fdbSlice) Rollback(s Snapshot) error {

	//get the seqnum from snapshot
	mainSeqNum := s.(*fdbSnapshot).MainIndexSeqNum()
	backSeqNum := s.(*fdbSnapshot).BackIndexSeqNum()

	//call forestdb to rollback
	var err error
	err = fdb.main[0].Rollback(mainSeqNum)
	if err != nil {
		common.Errorf("ForestDBSlice::Rollback \n\tSliceId %v IndexInstId %v. Error Rollback "+
			"Main Index to Snapshot %v. Error %v", fdb.id, fdb.idxInstId, s, err)
		return err
	}

	err = fdb.back[0].Rollback(backSeqNum)
	if err != nil {
		common.Errorf("ForestDBSlice::Rollback \n\tSliceId %v IndexInstId %v. Error Rollback "+
			"Back Index to Snapshot %v. Error %v", fdb.id, fdb.idxInstId, s, err)
		return err
	}

	return nil

}

//RollbackToZero rollbacks the slice to initial state. Return error if
//not possible
func (fdb *fdbSlice) RollbackToZero() error {

	//get the seqnum from snapshot
	mainSeqNum := forestdb.SeqNum(0)
	backSeqNum := forestdb.SeqNum(0)

	//call forestdb to rollback
	var err error
	err = fdb.main[0].Rollback(mainSeqNum)
	if err != nil {
		common.Errorf("ForestDBSlice::Rollback \n\tSliceId %v IndexInstId %v. Error Rollback "+
			"Main Index to Zero. Error %v", fdb.id, fdb.idxInstId, err)
		return err
	}

	err = fdb.back[0].Rollback(backSeqNum)
	if err != nil {
		common.Errorf("ForestDBSlice::Rollback \n\tSliceId %v IndexInstId %v. Error Rollback "+
			"Back Index to Zero. Error %v", fdb.id, fdb.idxInstId, err)
		return err
	}

	return nil

}

//Commit persists the outstanding writes in underlying
//forestdb database. If Commit returns error, slice
//should be rolled back to previous snapshot.
func (fdb *fdbSlice) Commit() error {

	//every SLICE_COMMIT_POLL_INTERVAL milliseconds,
	//check for outstanding mutations. If there are
	//none, proceed with the commit.
	ticker := time.NewTicker(time.Millisecond * SLICE_COMMIT_POLL_INTERVAL)
	for _ = range ticker.C {
		if fdb.checkAllWorkersDone() {
			break
		}
	}

	// Commit database file
	start := time.Now()
	err := fdb.dbfile.Commit(forestdb.COMMIT_MANUAL_WAL_FLUSH)
	elapsed := time.Since(start)

	fdb.totalCommitTime += elapsed
	common.Debugf("ForestDBSlice::Commit \n\tSliceId %v IndexInstId %v TotalFlushTime %v "+
		"TotalCommitTime %v", fdb.id, fdb.idxInstId, fdb.totalFlushTime, fdb.totalCommitTime)

	if err != nil {
		common.Errorf("ForestDBSlice::Commit \n\tSliceId %v IndexInstId %v Error in "+
			"Index Commit %v", fdb.id, fdb.idxInstId, err)
		return err
	}

	return nil
}

//checkAllWorkersDone return true if all workers have
//finished processing
func (fdb *fdbSlice) checkAllWorkersDone() bool {

	//if there are mutations in the cmdCh, workers are
	//not yet done
	if len(fdb.cmdCh) > 0 {
		return false
	}

	//worker queue is empty, make sure both workers are done
	//processing the last mutation
	for i := 0; i < NUM_WRITER_THREADS_PER_SLICE; i++ {
		fdb.workerDone[i] <- true
		<-fdb.workerDone[i]
	}
	return true
}

//Close the db. Should be able to reopen after this operation
func (fdb *fdbSlice) Close() error {

	common.Infof("ForestDBSlice::Close \n\tClosing Slice Id %v, IndexInstId %v, "+
		"IndexDefnId %v", fdb.idxInstId, fdb.idxDefnId, fdb.id)

	//signal shutdown for command handler routines
	for i := 0; i < NUM_WRITER_THREADS_PER_SLICE; i++ {
		fdb.stopCh[i] <- true
		<-fdb.stopCh[i]
	}

	//close the main index
	if fdb.main[0] != nil {
		fdb.main[0].Close()
	}
	//close the back index
	if fdb.back[0] != nil {
		fdb.back[0].Close()
	}

	if fdb.meta != nil {
		fdb.meta.Close()
	}

	fdb.dbfile.Close()
	return nil
}

//Destroy removes the database file from disk.
//Slice is not recoverable after this.
func (fdb *fdbSlice) Destroy() error {

	common.Infof("ForestDBSlice::Destroy \n\tDestroying Slice Id %v, IndexInstId %v, "+
		"IndexDefnId %v", fdb.idxInstId, fdb.idxDefnId, fdb.id)

	//TODO
	return nil
}

//Id returns the Id for this Slice
func (fdb *fdbSlice) Id() SliceId {
	return fdb.id
}

//Name returns the Name for this Slice
func (fdb *fdbSlice) Name() string {
	return fdb.name
}

//IsActive returns if the slice is active
func (fdb *fdbSlice) IsActive() bool {
	return fdb.isActive
}

//SetActive sets the active state of this slice
func (fdb *fdbSlice) SetActive(isActive bool) {
	fdb.isActive = isActive
}

//Status returns the status for this slice
func (fdb *fdbSlice) Status() SliceStatus {
	return fdb.status
}

//SetStatus set new status for this slice
func (fdb *fdbSlice) SetStatus(status SliceStatus) {
	fdb.status = status
}

//IndexInstId returns the Index InstanceId this
//slice is associated with
func (fdb *fdbSlice) IndexInstId() common.IndexInstId {
	return fdb.idxInstId
}

//IndexDefnId returns the Index DefnId this slice
//is associated with
func (fdb *fdbSlice) IndexDefnId() common.IndexDefnId {
	return fdb.idxDefnId
}

//GetSnapshotContainer returns the snapshot container for
//this slice
func (fdb *fdbSlice) GetSnapshotContainer() SnapshotContainer {
	return fdb.sc
}

func (fdb *fdbSlice) Timestamp() *common.TsVbuuid {

	return fdb.ts
}

func (fdb *fdbSlice) SetTimestamp(ts *common.TsVbuuid) error {

	common.Debugf("ForestDBSlice::SetTimestamp \n\tSliceId %v IndexInstId %v. TS - %s",
		fdb.id, fdb.idxInstId, ts)

	//marshal TS
	var tsVal []byte
	var err error
	if tsVal, err = json.Marshal(ts); err != nil {
		common.Errorf("ForestDBSlice::SetTimestamp \n\t Error Marshalling TS %v. Err %v", ts, err)
		return err
	}

	if err = fdb.meta.SetKV([]byte(STABILITY_TS_KEY_NAME), tsVal); err != nil {
		common.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error"+
			"in storing timestamp %s (%v)", fdb.id, fdb.idxInstId, tsVal, err)
		return err
	}

	fdb.ts = ts

	return nil
}

func (fdb *fdbSlice) String() string {

	str := fmt.Sprintf("SliceId: %v ", fdb.id)
	str += fmt.Sprintf("Name: %v ", fdb.name)
	str += fmt.Sprintf("Index: %v ", fdb.idxInstId)

	return str

}
