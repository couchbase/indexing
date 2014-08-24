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
	"sync"
)

//TODO
//For any query request, check if the replica is available. And use replica in case
//its more recent or serving less queries.

//ScanCoordinator handles scanning for an incoming index query. It will figure out
//the partitions/slices required to be scanned as per query parameters.

type ScanCoordinator interface {
}

type scanCoordinator struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any async message to supervisor

	indexInstMap  common.IndexInstMap
	indexPartnMap IndexPartnMap
}

//NewStorageManager returns an instance of scanCoordinator or err message
//It listens on supvCmdch for command and every command is followed
//by a synchronous response on the supvCmdch.
//Any async response to supervisor is sent to supvRespch.
//If supvCmdch get closed, ScanCoordinator will shut itself down.
func NewScanCoordinator(supvCmdch MsgChannel, supvRespch MsgChannel) (
	ScanCoordinator, Message) {

	//Init the scanCoordinator struct
	s := &scanCoordinator{
		supvCmdch:  supvCmdch,
		supvRespch: supvRespch,
	}

	//start ScanCoordinator loop which listens to commands from its supervisor
	go s.run()

	return s, &MsgSuccess{}

}

//run starts the storage manager loop which listens to messages
//from its supervisor(indexer)
func (s *scanCoordinator) run() {

	//main ScanCoordinator loop
loop:
	for {
		select {

		case cmd, ok := <-s.supvCmdch:
			if ok {
				if cmd.GetMsgType() == SCAN_COORD_SHUTDOWN {
					common.Infof("ScanCoordinator: Shutting Down")
					s.supvCmdch <- &MsgSuccess{}
					break loop
				}
				s.handleSupvervisorCommands(cmd)
			} else {
				//supervisor channel closed. exit
				break loop
			}

		}
	}
}

func (s *scanCoordinator) handleSupvervisorCommands(cmd Message) {

	switch cmd.GetMsgType() {

	case SCAN_COORD_SCAN_INDEX:
		s.handleScanIndex(cmd)

	case UPDATE_INDEX_INSTANCE_MAP:
		s.handleUpdateIndexInstMap(cmd)

	case UPDATE_INDEX_PARTITION_MAP:
		s.handleUpdateIndexPartnMap(cmd)

	default:
		common.Errorf("ScanCoordinator: Received Unknown Command %v", cmd)
		s.supvCmdch <- &MsgError{
			err: Error{code: ERROR_SCAN_COORD_UNKNOWN_COMMAND,
				severity: NORMAL,
				category: SCAN_COORD}}
	}

}

//handleScanIndex will scan the index and return the
//results/errors on the channels provided in the scan request.
func (s *scanCoordinator) handleScanIndex(cmd Message) {

	common.Debugf("ScanCoordinator: Received Command to Scan Index %v", cmd)

	idxInstId := cmd.(*MsgScanIndex).GetIndexInstId()
	p := cmd.(*MsgScanIndex).GetParams()

	//error if indexInstId is unknown
	if _, ok := s.indexInstMap[idxInstId]; !ok {
		common.Errorf("ScanCoordinator: Unable to find Index Instance for "+
			"InstanceId %v", idxInstId)

		errCh := cmd.(*MsgScanIndex).GetErrorChannel()
		errMsg := &MsgError{
			err: Error{code: ERROR_SCAN_COORD_INTERNAL_ERROR,
				severity: NORMAL,
				category: SCAN_COORD}}

		errCh <- errMsg

		//close the channel to indicate the caller that no further
		//processing will be done for this scan request
		close(errCh)

		//send error to supervisor as well
		s.supvCmdch <- errMsg
		return
	}

	//if partnKey is present in query params,
	//scan the matching partition only
	if p.partnKey != nil {
		go s.scanSinglePartition(cmd, s.indexInstMap, s.indexPartnMap)
	} else {
		go s.scanAllPartitions(cmd, s.indexInstMap, s.indexPartnMap)
	}

	s.supvCmdch <- &MsgSuccess{}

}

//scanSinglePartition scans the single partition for the index
//based on the partition key. The partition can be local,
//remote or have multiple endpoints(either local or remote),
//The results/errors are sent on result/err channel provided
//in the scan request,
func (s *scanCoordinator) scanSinglePartition(cmd Message,
	indexInstMap common.IndexInstMap, indexPartnMap IndexPartnMap) {

	common.Debugf("ScanCoordinator: ScanSinglePartition %v", cmd)

	idxInstId := cmd.(*MsgScanIndex).GetIndexInstId()

	var idxInst common.IndexInst
	var ok bool
	if idxInst, ok = indexInstMap[idxInstId]; !ok {

		common.Errorf("ScanCoordinator: Unable to find Index Instance for "+
			"InstanceId %v", idxInstId)

		errCh := cmd.(*MsgScanIndex).GetErrorChannel()
		errCh <- &MsgError{
			err: Error{code: ERROR_SCAN_COORD_INTERNAL_ERROR,
				severity: NORMAL,
				category: SCAN_COORD}}

		//close the channel to indicate the caller that no further
		//processing will be done for this scan request
		close(errCh)
		return
	}

	//get partitioncontainer for this index
	pc := idxInst.Pc

	//figure out the partition this query needs to run on
	p := cmd.(*MsgScanIndex).GetParams()
	partnId := pc.GetPartitionIdByPartitionKey(p.partnKey)

	var partnInstMap PartitionInstMap
	if partnInstMap, ok = indexPartnMap[idxInstId]; !ok {

		common.Errorf("ScanCoordinator: Unable to find partition map for "+
			"Index InstanceId %v", idxInstId)

		errCh := cmd.(*MsgScanIndex).GetErrorChannel()
		errCh <- &MsgError{
			err: Error{code: ERROR_SCAN_COORD_INTERNAL_ERROR,
				severity: FATAL,
				category: SCAN_COORD}}

		//close the channel to indicate the caller that no further
		//processing will be done for this scan request
		close(errCh)
		return
	}

	var wg sync.WaitGroup
	workerStopChannel := make([]StopChannel, 1)

	var partnInst PartitionInst
	wg.Add(1)
	if partnInst, ok = partnInstMap[partnId]; ok {
		//run local scan for partition
		go s.scanLocalPartition(cmd, partnInst, workerStopChannel[0], &wg)

	} else {
		//partition doesn't exist on this node,
		//run remote scan
		go s.scanRemotePartition(cmd, pc.GetPartitionById(partnId),
			workerStopChannel[0], &wg)
	}

	stopch := cmd.(*MsgScanIndex).GetStopChannel()
	s.monitorWorkers(&wg, stopch, workerStopChannel, "scanSinglePartition")

	//close error channel to indicate scan is done
	errCh := cmd.(*MsgScanIndex).GetErrorChannel()
	close(errCh)

}

//scanAllPartitions scans all the partitions for the index
//for the scan params. The partitions can be local,
//remote or have multiple endpoints(either local or remote),
//The results/errors are sent on result/err channel provided
//in the scan request,
func (s *scanCoordinator) scanAllPartitions(cmd Message,
	indexInstMap common.IndexInstMap, indexPartnMap IndexPartnMap) {

	common.Debugf("ScanCoordinator: ScanAllPartitions %v", cmd)

	idxInstId := cmd.(*MsgScanIndex).GetIndexInstId()

	var idxInst common.IndexInst
	var ok bool
	if idxInst, ok = indexInstMap[idxInstId]; !ok {

		common.Errorf("ScanCoordinator: Unable to find Index Instance for "+
			"InstanceId %v", idxInstId)

		errCh := cmd.(*MsgScanIndex).GetErrorChannel()
		errCh <- &MsgError{
			err: Error{code: ERROR_SCAN_COORD_INTERNAL_ERROR,
				severity: NORMAL,
				category: SCAN_COORD}}

		//close the channel to indicate the caller that no further
		//processing will be done for this scan request
		close(errCh)

	}

	var partnInstMap PartitionInstMap
	if partnInstMap, ok = indexPartnMap[idxInstId]; !ok {
		common.Errorf("ScanCoordinator: Unable to find partition map for "+
			"Index InstanceId %v", idxInstId)

		errCh := cmd.(*MsgScanIndex).GetErrorChannel()
		errCh <- &MsgError{
			err: Error{code: ERROR_SCAN_COORD_INTERNAL_ERROR,
				severity: FATAL,
				category: SCAN_COORD}}

		//close the channel to indicate the caller that no further
		//processing will be done for this scan request
		close(errCh)
	}

	//get partitioncontainer for this index
	pc := idxInst.Pc

	var wg sync.WaitGroup
	workerStopChannels := make([]StopChannel, pc.GetNumPartitions())

	//for all the partitions of this index
	for i, partnDefn := range pc.GetAllPartitions() {

		wg.Add(1)
		var partnInst PartitionInst
		if partnInst, ok = partnInstMap[partnDefn.GetPartitionId()]; !ok {
			//partition doesn't exist on this node, run remote scan
			go s.scanRemotePartition(cmd, partnDefn, workerStopChannels[i], &wg)
		} else {
			//run local scan for local partition
			go s.scanLocalPartition(cmd, partnInst, workerStopChannels[i], &wg)
		}
	}

	stopch := cmd.(*MsgScanIndex).GetStopChannel()
	s.monitorWorkers(&wg, stopch, workerStopChannels, "scanAllPartitions")

	//close error channel to indicate scan is done
	errCh := cmd.(*MsgScanIndex).GetErrorChannel()
	close(errCh)
}

//monitorWorkers waits for the provided workers to finish and returns.
//It also listens to the stop channel and if that gets closed, all workers
//are stopped using workerStopChannels. Once all workers stop, the
//method retuns.
func (s *scanCoordinator) monitorWorkers(wg *sync.WaitGroup,
	stopch StopChannel, workerStopChannels []StopChannel, debugStr string) {

	allWorkersDoneCh := make(DoneChannel)

	//wait for all workers to finish
	go func() {
		common.Tracef("ScanCoordinator: %s: Waiting for workers to finish.", debugStr)
		wg.Wait()
		//send signal on channel to indicate all workers have finished
		common.Tracef("ScanCoordinator: %s: All workers finished", debugStr)
		close(allWorkersDoneCh)
	}()

	//wait for upstream to signal stop or for all workers to signal done
	select {
	case <-stopch:
		common.Debugf("ScanCoordinator: %s: Stopping All Workers.", debugStr)
		//stop all workers
		for _, ch := range workerStopChannels {
			close(ch)
		}
		//wait for all workers to stop
		<-allWorkersDoneCh
		common.Debugf("ScanCoordinator: %s: Stopped All Workers.", debugStr)

		//wait for notification of all workers finishing
	case <-allWorkersDoneCh:

	}

}

//scanLocalPartition scan a local partition. Some slices of the
//partition may be present on remote node. For those slices
//request is sent to remote indexer and results are returned.
func (s *scanCoordinator) scanLocalPartition(cmd Message,
	partnInst PartitionInst, stopch StopChannel,
	wg *sync.WaitGroup) {

	defer wg.Done()

	common.Debugf("ScanCoordinator: ScanLocalPartition %v", cmd)

	var workerWg sync.WaitGroup
	var workerStopChannels []StopChannel

	//if this partition has multiple endpoints, then remote
	//slices of the partition need to be scanned as well
	partnDefn := partnInst.Defn
	if len(partnDefn.Endpoints()) > 1 {
		workerWg.Add(1)
		workerStopCh := make(StopChannel)
		workerStopChannels = append(workerStopChannels, workerStopCh)
		go s.scanRemoteSlice(cmd, partnDefn, workerStopCh, &workerWg)
	}

	sliceList := partnInst.Sc.GetAllSlices()

	for _, slice := range sliceList {
		workerWg.Add(1)
		workerStopCh := make(StopChannel)
		workerStopChannels = append(workerStopChannels, workerStopCh)
		go s.scanLocalSlice(cmd, slice, workerStopCh, &workerWg)
	}

	s.monitorWorkers(&workerWg, stopch, workerStopChannels, "scanLocalPartition")
}

//scanRemotePartition request remote indexer to scan a partition
//and return results based on scan params.
func (s *scanCoordinator) scanRemotePartition(cmd Message,
	partnDefn common.PartitionDefn, stopch StopChannel,
	wg *sync.WaitGroup) {

	defer wg.Done()

	common.Debugf("ScanCoordinator: ScanRemotePartition %v", cmd)

	//TODO Send request to remote indexer's admin-port
	//to fulfill the scan request
}

//scanLocalSlice scans a local slice based on latest snapshot
//and returns results on the channels provided in request.
func (s *scanCoordinator) scanLocalSlice(cmd Message,
	slice Slice, stopch StopChannel, wg *sync.WaitGroup) {

	defer wg.Done()

	common.Debugf("ScanCoordinator: ScanLocalSlice %v. SliceId %v", cmd, slice.Id())

	snapContainer := slice.GetSnapshotContainer()
	snap := snapContainer.GetLatestSnapshot()

	if snap != nil {
		s.executeLocalScan(cmd, snap, stopch)
	} else {
		scanId := cmd.(*MsgScanIndex).GetScanId()
		idxInstId := cmd.(*MsgScanIndex).GetIndexInstId()
		common.Infof("ScanCoordinator: No Snapshot Available for ScanId %v "+
			"IndexInstId %v, SliceId %v", scanId, idxInstId, slice.Id())
	}
}

//scanRemoteSlice requests remote indexer to scan a slice
//and return results based on scan params.
func (s *scanCoordinator) scanRemoteSlice(cmd Message,
	partnDefn common.PartitionDefn, stopch StopChannel,
	wg *sync.WaitGroup) {

	defer wg.Done()

	common.Debugf("ScanCoordinator: ScanRemoteSlice %v", cmd)

	//TODO Send request to remote indexer's admin-port
	//to fulfill the scan request
}

//executeLocalScan executes the actual scan of the snapshot.
//Scan can be stopped anytime by closing the stop channel.
func (s *scanCoordinator) executeLocalScan(cmd Message, snap Snapshot, stopch StopChannel) {

	q := cmd.(*MsgScanIndex).GetParams()

	switch q.scanType {

	case COUNT:
		s.countQuery(cmd, snap, stopch)

	case EXISTS:
		s.existsQuery(cmd, snap, stopch)

	case LOOKUP:
		s.lookupQuery(cmd, snap, stopch)

	case RANGESCAN:
		s.rangeQuery(cmd, snap, stopch)

	case FULLSCAN:
		s.scanQuery(cmd, snap, stopch)

	case RANGECOUNT:
		s.rangeCountQuery(cmd, snap, stopch)

	}
}

//executeRemoteScan sends the scan request to admin port of remote
//indexer and gets the results
func (s *scanCoordinator) executeRemoteScan() {

}

//countQuery executes countTotal method of snapshot and returns result/error
//on channel provided in scan request.
func (s *scanCoordinator) countQuery(cmd Message, snap Snapshot, stopch StopChannel) {

	id := cmd.(*MsgScanIndex).GetScanId()
	countCh := cmd.(*MsgScanIndex).GetCountChannel()
	errCh := cmd.(*MsgScanIndex).GetErrorChannel()

	common.Debugf("ScanCoordinator: CountQuery ScanId %v", id)

	count, err := snap.CountTotal(stopch)
	if err != nil {
		common.Errorf("ScanCoordinator: ScanId %v CountQuery Got Error %v", id, err)
		errCh <- &MsgError{
			err: Error{code: ERROR_SCAN_COORD_INTERNAL_ERROR,
				severity: NORMAL,
				cause:    err,
				category: SCAN_COORD}}
	} else {
		countCh <- count
	}

}

//existsQuery executes Exists method of snapshot and returns result/error
//on channel provided in scan request.
func (s *scanCoordinator) existsQuery(cmd Message, snap Snapshot, stopch StopChannel) {

	id := cmd.(*MsgScanIndex).GetScanId()
	p := cmd.(*MsgScanIndex).GetParams()
	countCh := cmd.(*MsgScanIndex).GetCountChannel()
	errCh := cmd.(*MsgScanIndex).GetErrorChannel()

	common.Debugf("ScanCoordinator: ExistsQuery ScanId %v", id)

	exists, err := snap.Exists(p.low, stopch)
	if err != nil {
		common.Errorf("ScanCoordinator: ScanId %v ExistsQuery Got Error %v", id, err)
		errCh <- &MsgError{
			err: Error{code: ERROR_SCAN_COORD_INTERNAL_ERROR,
				severity: NORMAL,
				cause:    err,
				category: SCAN_COORD}}
	} else {
		if exists {
			countCh <- 1
		} else {
			countCh <- 0
		}
	}
}

//lookupQuery executes Lookup method of snapshot and returns result/error
//on channel provided in scan request.
func (s *scanCoordinator) lookupQuery(cmd Message, snap Snapshot, stopch StopChannel) {

	id := cmd.(*MsgScanIndex).GetScanId()
	p := cmd.(*MsgScanIndex).GetParams()

	common.Debugf("ScanCoordinator: LookupQuery ScanId %v", id)

	ch, cherr := snap.Lookup(p.low, stopch)
	s.receiveValue(cmd, ch, cherr)
}

//rangeQuery executes ValueRange method of snapshot and returns result/error
//on channel provided in scan request.
func (s *scanCoordinator) rangeQuery(cmd Message, snap Snapshot, stopch StopChannel) {

	id := cmd.(*MsgScanIndex).GetScanId()
	p := cmd.(*MsgScanIndex).GetParams()

	common.Debugf("ScanCoordinator: RangeQuery ScanId %v", id)

	ch, cherr, _ := snap.ValueRange(p.low, p.high, p.incl, stopch)
	s.receiveValue(cmd, ch, cherr)
}

//lookupQuery executes ValueSet method of snapshot and returns result/error
//on channel provided in scan request.
func (s *scanCoordinator) scanQuery(cmd Message, snap Snapshot, stopch StopChannel) {

	id := cmd.(*MsgScanIndex).GetScanId()

	common.Debugf("ScanCoordinator: ScanQuery ScanId %v", id)

	ch, cherr := snap.ValueSet(stopch)
	s.receiveValue(cmd, ch, cherr)
}

//lookupQuery executes CountRange method of snapshot and returns result/error
//on channel provided in scan request.
func (s *scanCoordinator) rangeCountQuery(cmd Message, snap Snapshot, stopch StopChannel) {

	id := cmd.(*MsgScanIndex).GetScanId()
	p := cmd.(*MsgScanIndex).GetParams()
	countCh := cmd.(*MsgScanIndex).GetCountChannel()
	errCh := cmd.(*MsgScanIndex).GetErrorChannel()

	common.Debugf("ScanCoordinator: RangeCountQuery ScanId %v", id)

	totalRows, err := snap.CountRange(p.low, p.high, p.incl, stopch)
	if err != nil {
		common.Errorf("ScanCoordinator: ScanId %v RangeCountQuery Got Error %v", id, err)
		errCh <- &MsgError{
			err: Error{code: ERROR_SCAN_COORD_INTERNAL_ERROR,
				severity: NORMAL,
				cause:    err,
				category: SCAN_COORD}}
	} else {
		countCh <- totalRows
	}
}

//receiveValue receives results/errors from snapshot reader and forwards it to
//the caller till the result channel is closed by the snapshot reader
func (s *scanCoordinator) receiveValue(cmd Message, chval chan Value, cherr chan error) {

	id := cmd.(*MsgScanIndex).GetScanId()
	resCh := cmd.(*MsgScanIndex).GetResultChannel()
	resErrCh := cmd.(*MsgScanIndex).GetErrorChannel()

	ok := true
	var value Value
	var err error
	for ok {
		select {
		case value, ok = <-chval:
			if ok {
				common.Tracef("ScanCoordinator: ScanId %v Received Value %s", id, value.String())
				resCh <- value
			}
		case err, _ = <-cherr:
			if err != nil {
				common.Errorf("ScanCoordinator: ScanId %v Received Error %s", id, err)
				resErrCh <- &MsgError{
					err: Error{code: ERROR_SCAN_COORD_INTERNAL_ERROR,
						severity: NORMAL,
						cause:    err,
						category: SCAN_COORD}}
			}
		}
	}

}

func (s *scanCoordinator) handleUpdateIndexInstMap(cmd Message) {

	common.Infof("ScanCoordinator::handleUpdateIndexInstMap %v", cmd)
	s.indexInstMap = cmd.(*MsgUpdateInstMap).GetIndexInstMap()

	s.supvCmdch <- &MsgSuccess{}
}

func (s *scanCoordinator) handleUpdateIndexPartnMap(cmd Message) {

	common.Infof("ScanCoordinator::handleUpdateIndexPartnMap %v", cmd)
	s.indexPartnMap = cmd.(*MsgUpdatePartnMap).GetIndexPartnMap()

	s.supvCmdch <- &MsgSuccess{}
}
