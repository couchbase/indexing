// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// TODO:
// 1. functions in this package directly access SystemConfig, instead it
//    is suggested to pass config via function argument.

package indexer

import (
	"errors"
	"fmt"
	
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	projClient "github.com/couchbase/indexing/secondary/projector/client"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
	"github.com/golang/protobuf/proto"
	"net"
	"sort"
	"strings"
	"time"
)

const (
	HTTP_PREFIX             string = "http://"
	MAX_KV_REQUEST_RETRY    int    = 0
	BACKOFF_FACTOR          int    = 2
	MAX_CLUSTER_FETCH_RETRY int    = 600

	// op_monitor constants
	KV_SENDER_OP_MONITOR_SIZE     int = 1024
	KV_SENDER_OP_MONITOR_BATCHSZ  int = 0          // No batching
	KV_SENDER_OP_MONITOR_INTERVAL int = 1000       // In Milliseconds
	TOPIC_REQUEST_TIMEOUT         int = 300 * 1000 // In Milliseconds
)

//KVSender provides the mechanism to talk to KV(projector, router etc)
type KVSender interface {
}

type kvSender struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any message to supervisor

	cInfoClient *c.ClusterInfoClient
	config      c.Config
	monitor     *c.OperationsMonitor
}

func NewKVSender(supvCmdch MsgChannel, supvRespch MsgChannel,
	config c.Config) (KVSender, Message) {

	cic, err := c.NewClusterInfoClient(config["clusterAddr"].String(), DEFAULT_POOL, config)
	if err != nil {
		c.CrashOnError(err)
	}
	cic.SetUserAgent("kvsender")
	cic.WatchRebalanceChanges()

	//Init the kvSender struct
	k := &kvSender{
		supvCmdch:   supvCmdch,
		supvRespch:  supvRespch,
		cInfoClient: cic,
		config:      config,
		monitor: c.NewOperationsMonitor(
			"kvSender",
			KV_SENDER_OP_MONITOR_SIZE,
			KV_SENDER_OP_MONITOR_INTERVAL,
			KV_SENDER_OP_MONITOR_BATCHSZ,
		),
	}

	cinfo := k.cInfoClient.GetClusterInfoCache()
	cinfo.Lock()
	defer cinfo.Unlock()
	cinfo.SetMaxRetries(MAX_CLUSTER_FETCH_RETRY)
	//start kvsender loop which listens to commands from its supervisor
	go k.run()

	return k, &MsgSuccess{}

}

//run starts the kvsender loop which listens to messages
//from it supervisor(indexer)
func (k *kvSender) run() {

	//main KVSender loop
loop:
	for {
		select {

		case cmd, ok := <-k.supvCmdch:
			if ok {
				if cmd.GetMsgType() == KV_SENDER_SHUTDOWN {
					logging.Infof("KVSender::run Shutting Down")
					k.supvCmdch <- &MsgSuccess{}
					break loop
				}
				k.handleSupvervisorCommands(cmd)
			} else {
				//supervisor channel closed. exit
				k.monitor.Close()
				break loop
			}

		}
	}
}

func (k *kvSender) handleSupvervisorCommands(cmd Message) {

	switch cmd.GetMsgType() {

	case OPEN_STREAM:
		k.handleOpenStream(cmd)

	case ADD_INDEX_LIST_TO_STREAM:
		k.handleAddIndexListToStream(cmd)

	case REMOVE_INDEX_LIST_FROM_STREAM:
		k.handleRemoveIndexListFromStream(cmd)

	case REMOVE_BUCKET_FROM_STREAM:
		k.handleRemoveBucketFromStream(cmd)

	case CLOSE_STREAM:
		k.handleCloseStream(cmd)

	case KV_SENDER_RESTART_VBUCKETS:
		k.handleRestartVbuckets(cmd)

	case CONFIG_SETTINGS_UPDATE:
		k.handleConfigUpdate(cmd)

	default:
		logging.Errorf("KVSender::handleSupvervisorCommands "+
			"Received Unknown Command %v", cmd)
	}

}

func (k *kvSender) handleOpenStream(cmd Message) {

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()
	indexInstList := cmd.(*MsgStreamUpdate).GetIndexList()
	restartTs := cmd.(*MsgStreamUpdate).GetRestartTs()
	respCh := cmd.(*MsgStreamUpdate).GetResponseChannel()
	stopCh := cmd.(*MsgStreamUpdate).GetStopChannel()
	bucket := cmd.(*MsgStreamUpdate).GetBucket()
	async := cmd.(*MsgStreamUpdate).GetAsync()
	sessionId := cmd.(*MsgStreamUpdate).GetSessionId()

	logging.LazyDebug(func() string {
		return fmt.Sprintf("KVSender::handleOpenStream %v %v %v",
			streamId, bucket, cmd)
	})

	go k.openMutationStream(streamId, indexInstList,
		restartTs, async, sessionId, respCh, stopCh)

	k.supvCmdch <- &MsgSuccess{}

}

func (k *kvSender) handleAddIndexListToStream(cmd Message) {

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()
	bucket := cmd.(*MsgStreamUpdate).GetBucket()
	addIndexList := cmd.(*MsgStreamUpdate).GetIndexList()
	respCh := cmd.(*MsgStreamUpdate).GetResponseChannel()
	stopCh := cmd.(*MsgStreamUpdate).GetStopChannel()

	logging.LazyDebug(func() string {
		return fmt.Sprintf("KVSender::handleAddIndexListToStream %v %v %v", streamId, bucket, cmd)
	})

	go k.addIndexForExistingBucket(streamId, bucket, addIndexList, respCh, stopCh)

	k.supvCmdch <- &MsgSuccess{}
}

func (k *kvSender) handleRemoveIndexListFromStream(cmd Message) {

	logging.LazyDebug(func() string {
		return fmt.Sprintf("KVSender::handleRemoveIndexListFromStream %v", cmd)
	})

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()
	delIndexList := cmd.(*MsgStreamUpdate).GetIndexList()
	respCh := cmd.(*MsgStreamUpdate).GetResponseChannel()
	stopCh := cmd.(*MsgStreamUpdate).GetStopChannel()

	go k.deleteIndexesFromStream(streamId, delIndexList, respCh, stopCh)

	k.supvCmdch <- &MsgSuccess{}
}

func (k *kvSender) handleRemoveBucketFromStream(cmd Message) {

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()
	bucket := cmd.(*MsgStreamUpdate).GetBucket()
	respCh := cmd.(*MsgStreamUpdate).GetResponseChannel()
	stopCh := cmd.(*MsgStreamUpdate).GetStopChannel()

	logging.LazyDebug(func() string {
		return fmt.Sprintf("KVSender::handleRemoveBucketFromStream %v %v %v", streamId, bucket, cmd)
	})

	go k.deleteBucketsFromStream(streamId, []string{bucket}, respCh, stopCh)

	k.supvCmdch <- &MsgSuccess{}
}

func (k *kvSender) handleCloseStream(cmd Message) {

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()
	bucket := cmd.(*MsgStreamUpdate).GetBucket()
	respCh := cmd.(*MsgStreamUpdate).GetResponseChannel()
	stopCh := cmd.(*MsgStreamUpdate).GetStopChannel()

	logging.LazyDebug(func() string {
		return fmt.Sprintf("KVSender::handleCloseStream %v %v %v", streamId, bucket, cmd)
	})

	go k.closeMutationStream(streamId, bucket, respCh, stopCh)

	k.supvCmdch <- &MsgSuccess{}
}

func (k *kvSender) handleRestartVbuckets(cmd Message) {

	streamId := cmd.(*MsgRestartVbuckets).GetStreamId()
	bucket := cmd.(*MsgRestartVbuckets).GetBucket()
	restartTs := cmd.(*MsgRestartVbuckets).GetRestartTs()
	respCh := cmd.(*MsgRestartVbuckets).GetResponseCh()
	stopCh := cmd.(*MsgRestartVbuckets).GetStopChannel()
	connErrVbs := cmd.(*MsgRestartVbuckets).ConnErrVbs()
	repairVbs := cmd.(*MsgRestartVbuckets).RepairVbs()
	sessionId := cmd.(*MsgRestartVbuckets).GetSessionId()

	logging.LazyDebug(func() string {
		return fmt.Sprintf("KVSender::handleRestartVbuckets %v %v %v",
			streamId, bucket, cmd)
	})

	go k.restartVbuckets(streamId, restartTs, connErrVbs, repairVbs,
		sessionId, respCh, stopCh)
	k.supvCmdch <- &MsgSuccess{}
}

func (k *kvSender) openMutationStream(streamId c.StreamId, indexInstList []c.IndexInst,
	restartTs *c.TsVbuuid, async bool, sessionId uint64, respCh MsgChannel, stopCh StopChannel) {

	if len(indexInstList) == 0 {
		logging.Warnf("KVSender::openMutationStream Empty IndexList. Nothing to do.")
		respCh <- &MsgSuccess{}
		return
	}

	protoInstList := convertIndexListToProto(k.config, k.cInfoClient, indexInstList, streamId)
	bucket := indexInstList[0].Defn.Bucket

	//use any bucket as list of vbs remain the same for all buckets
	vbnos, addrs, err := k.getAllVbucketsInCluster(bucket)
	if err != nil {
		// This failure could be due to stale cluster info cache
		// Force fetch cluster info cache so that the next call
		// might succeed
		k.cInfoClient.FetchWithLock()
		logging.Errorf("KVSender::openMutationStream %v %v Error in fetching vbuckets info %v",
			streamId, bucket, err)
		respCh <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
		return
	}

	numVbuckets := k.config["numVbuckets"].Int()
	if len(vbnos) != numVbuckets {
		logging.Warnf("KVSender::openMutationStream mismatch in number of configured "+
			"vbuckets. conf %v actual %v", numVbuckets, vbnos)
	}

	restartTsList, err := k.makeRestartTsForVbs(bucket, restartTs, vbnos)
	if err != nil {
		logging.Errorf("KVSender::openMutationStream %v %v Error making restart ts %v",
			streamId, bucket, err)
		respCh <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
		return
	}

	var rollbackTs *protobuf.TsVbuuid
	var activeTs *protobuf.TsVbuuid
	var pendingTs *protobuf.TsVbuuid
	topic := getTopicForStreamId(streamId)

	fn := func(r int, err error) error {

		//clear the error before every retry
		err = nil
		for _, addr := range addrs {

			execWithStopCh(func() {
				doneCh := make(chan bool)
				timeout := time.Duration(TOPIC_REQUEST_TIMEOUT) * time.Millisecond
				_ = k.monitor.AddOperation(
					c.NewOperation(timeout, doneCh, func(elapsed time.Duration) {
						msg := "Slow/Hung Operation: KVSender::sendMutationTopicRequest"
						msg += " did not respond for %v for projector %v topic %v bucket %v"
						logging.Warnf(msg, elapsed, addr, topic, bucket)
					},
					),
				)

				if ap, ret := newProjClient(addr); ret != nil {
					logging.Errorf("KVSender::openMutationStream %v %v Error %v when "+
						" creating HTTP client to %v", streamId, bucket, ret, addr)
					err = ret
				} else if res, ret := k.sendMutationTopicRequest(ap, topic, restartTsList,
					protoInstList, async, sessionId); ret != nil {
					//for all errors, retry
					logging.Errorf("KVSender::openMutationStream %v %v Error Received %v from %v",
						streamId, bucket, ret, addr)
					err = ret
				} else {
					activeTs = updateActiveTsFromResponse(bucket, activeTs, res)
					rollbackTs = updateRollbackTsFromResponse(bucket, rollbackTs, res)
					pendingTs = updatePendingTsFromResponse(bucket, pendingTs, res)
					if rollbackTs != nil {
						logging.Infof("KVSender::openMutationStream %v %v Projector %v Rollback Received %v",
							streamId, bucket, addr, rollbackTs)
					}
				}
				close(doneCh)
			}, stopCh)
		}

		if rollbackTs != nil {
			//no retry required for rollback
			return nil
		} else if err != nil {
			//retry for any error
			return err
		} else {
			//check if we have received activeTs for all vbuckets
			numVb := 0

			if activeTs != nil {
				numVb = activeTs.Len()
			}

			// Take into account for both activeTs and pendingTs
			if async && pendingTs != nil {
				if ts := pendingTs.Union(activeTs); ts != nil {
					numVb = ts.Len()
				}
			}

			if numVb != len(vbnos) {
				return errors.New("ErrPartialVbStart")
			} else {
				return nil
			}
		}
	}

	rh := c.NewRetryHelper(MAX_KV_REQUEST_RETRY, time.Second, BACKOFF_FACTOR, fn)
	err = rh.Run()

	if rollbackTs != nil {
		//convert from protobuf to native format
		var nativeTs *c.TsVbuuid
		if restartTsList != nil {
			logging.Infof("KVSender::openMutationStream restartTsList %v", restartTsList)
			nativeTs = restartTsList.Union(rollbackTs).ToTsVbuuid(numVbuckets)
		} else {
			nativeTs = rollbackTs.ToTsVbuuid(numVbuckets)
		}

		respCh <- &MsgRollback{streamId: streamId,
			bucket:     bucket,
			rollbackTs: nativeTs}
	} else if err != nil {
		// The failure could have been due to stale cluster info cache
		// Force update cluster info cache on failure so that the next
		// retry might succeed
		k.cInfoClient.FetchWithLock()

		logging.Errorf("KVSender::openMutationStream %v %v Error from Projector %v",
			streamId, bucket, err)
		respCh <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
	} else {
		resp := &MsgSuccessOpenStream{}

		if activeTs != nil {
			resp.activeTs = activeTs.ToTsVbuuid(numVbuckets)
		}

		if pendingTs != nil {
			resp.pendingTs = pendingTs.ToTsVbuuid(numVbuckets)
		}

		respCh <- resp
	}
}

func (k *kvSender) restartVbuckets(streamId c.StreamId,
	restartTs *c.TsVbuuid, connErrVbs []Vbucket, repairVbs []Vbucket,
	sessionId uint64, respCh MsgChannel, stopCh StopChannel) {

	addrs, err := k.getProjAddrsForVbuckets(restartTs.Bucket, repairVbs)
	if err != nil {
		k.cInfoClient.FetchWithLock()

		logging.Errorf("KVSender::restartVbuckets %v %v Error in fetching cluster info %v",
			streamId, restartTs.Bucket, err)
		respCh <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err},
			sessionId: sessionId,
		}

		return
	}

	//convert TS to protobuf format
	var protoRestartTs *protobuf.TsVbuuid
	numVbuckets := k.config["numVbuckets"].Int()
	protoTs := protobuf.NewTsVbuuid(DEFAULT_POOL, restartTs.Bucket, numVbuckets)
	protoRestartTs = protoTs.FromTsVbuuid(restartTs)

	// Add any missing vbs to repairTs
	for _, vbno := range repairVbs {
		found := false
		for _, vbno2 := range protoRestartTs.GetVbnos() {
			if uint32(vbno) == vbno2 {
				found = true
				break
			}
		}

		if !found {
			protoRestartTs.Vbnos = append(protoRestartTs.Vbnos, uint32(vbno))
			protoRestartTs.Seqnos = append(protoRestartTs.Seqnos, 0)
			protoRestartTs.Vbuuids = append(protoRestartTs.Vbuuids, 0)
			protoRestartTs.Snapshots = append(protoRestartTs.Snapshots, protobuf.NewSnapshot(0, 0))
		}
	}
	sort.Sort(protoRestartTs)

	var rollbackTs *protobuf.TsVbuuid
	var activeTs *protobuf.TsVbuuid
	var pendingTs *protobuf.TsVbuuid
	topic := getTopicForStreamId(streamId)
	rollback := false
	aborted := false

	fn := func(r int, err error) error {

		for _, addr := range addrs {
			aborted = execWithStopCh(func() {
				doneCh := make(chan bool)
				timeout := time.Duration(TOPIC_REQUEST_TIMEOUT) * time.Millisecond
				_ = k.monitor.AddOperation(
					c.NewOperation(timeout, doneCh, func(elapsed time.Duration) {
						msg := "Slow/Hung Operation: KVSender::sendRestartVbuckets"
						msg += " did not respond for %v for projector %v topic %v bucket %v"
						logging.Warnf(msg, elapsed, addr, topic, restartTs.Bucket)
					},
					),
				)

				if ap, ret := newProjClient(addr); ret != nil {
					logging.Errorf("KVSender::restartVbuckets %v %v Error %v when creating HTTP client to %v",
						streamId, restartTs.Bucket, ret, addr)
					err = ret
				} else if res, ret := k.sendRestartVbuckets(ap, topic, connErrVbs,
					protoRestartTs, sessionId); ret != nil {
					//retry for all errors
					logging.Errorf("KVSender::restartVbuckets %v %v Error Received %v from %v",
						streamId, restartTs.Bucket, ret, addr)
					err = ret
				} else {
					rollbackTs = updateRollbackTsFromResponse(restartTs.Bucket, rollbackTs, res)
					activeTs = updateActiveTsFromResponse(restartTs.Bucket, activeTs, res)
					pendingTs = updatePendingTsFromResponse(restartTs.Bucket, pendingTs, res)
				}
				close(doneCh)
			}, stopCh)
		}

		if rollbackTs != nil && checkVbListInTS(protoRestartTs.GetVbnos(), rollbackTs) {
			//if rollback, no need to retry
			rollback = true
			return nil
		} else {
			return err
		}
	}

	rh := c.NewRetryHelper(MAX_KV_REQUEST_RETRY, time.Second, BACKOFF_FACTOR, fn)
	err = rh.Run()

	if aborted {
		respCh <- &MsgRepairAbort{streamId: streamId,
			bucket: restartTs.Bucket}
	} else if rollback {
		//if any of the requested vb is in rollback ts, send rollback
		//msg to caller
		//convert from protobuf to native format
		nativeTs := rollbackTs.ToTsVbuuid(numVbuckets)

		respCh <- &MsgRollback{streamId: streamId,
			rollbackTs: nativeTs,
			sessionId:  sessionId,
		}
	} else if err != nil {
		//if there is a topicMissing/genServer.Closed error, a fresh
		//MutationTopicRequest is required.
		if err.Error() == projClient.ErrorTopicMissing.Error() ||
			err.Error() == c.ErrorClosed.Error() ||
			err.Error() == projClient.ErrorInvalidBucket.Error() {
			respCh <- &MsgKVStreamRepair{
				streamId:  streamId,
				bucket:    restartTs.Bucket,
				sessionId: sessionId,
			}
		} else {
			// The failure could have been due to stale cluster info cache
			// Force update cluster info cache on failure so that the next
			// retry might succeed
			k.cInfoClient.FetchWithLock()

			respCh <- &MsgError{
				err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
					severity: FATAL,
					cause:    err},
				sessionId: sessionId,
			}

		}
	} else {
		resp := &MsgRestartVbucketsResponse{
			streamId:  streamId,
			bucket:    restartTs.Bucket,
			sessionId: sessionId,
		}

		if activeTs != nil {
			resp.activeTs = activeTs.ToTsVbuuid(numVbuckets)
		}

		if pendingTs != nil {
			resp.pendingTs = pendingTs.ToTsVbuuid(numVbuckets)
		}

		respCh <- resp
	}
}

func (k *kvSender) addIndexForExistingBucket(streamId c.StreamId, bucket string, indexInstList []c.IndexInst,
	respCh MsgChannel, stopCh StopChannel) {

	_, addrs, err := k.getAllVbucketsInCluster(bucket)
	if err != nil {
		k.cInfoClient.FetchWithLock()

		logging.Errorf("KVSender::addIndexForExistingBucket %v %v Error in fetching cluster info %v",
			streamId, bucket, err)
		respCh <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
		return
	}

	var currentTs *protobuf.TsVbuuid
	protoInstList := convertIndexListToProto(k.config, k.cInfoClient, indexInstList, streamId)
	topic := getTopicForStreamId(streamId)

	fn := func(r int, err error) error {

		//clear the error before every retry
		err = nil
		for _, addr := range addrs {
			execWithStopCh(func() {
				doneCh := make(chan bool)
				timeout := time.Duration(TOPIC_REQUEST_TIMEOUT) * time.Millisecond
				_ = k.monitor.AddOperation(
					c.NewOperation(timeout, doneCh, func(elapsed time.Duration) {
						msg := "Slow/Hung Operation: KVSender::sendAddInstancesRequest"
						msg += " did not respond for %v for projector %v topic %v bucket %v"
						logging.Warnf(msg, elapsed, addr, topic, bucket)
					},
					),
				)

				if ap, ret := newProjClient(addr); ret != nil {
					logging.Errorf("KVSender::addIndexForExistingBucket %v %v Error %v when creating HTTP client to %v",
						streamId, bucket, ret, addr)
					err = ret
				} else if res, ret := sendAddInstancesRequest(ap, topic, protoInstList); ret != nil {
					logging.Errorf("KVSender::addIndexForExistingBucket %v %v Error Received %v from %v",
						streamId, bucket, ret, addr)
					err = ret
				} else {
					currentTs = updateCurrentTsFromResponse(bucket, currentTs, res)
				}
				close(doneCh)
			}, stopCh)
		}

		//check if we have received currentTs for all vbuckets
		numVbuckets := k.config["numVbuckets"].Int()
		if currentTs == nil || currentTs.Len() != numVbuckets {
			return errors.New("ErrPartialVbStart")
		} else {
			return err
		}

	}

	rh := c.NewRetryHelper(MAX_KV_REQUEST_RETRY, time.Second, BACKOFF_FACTOR, fn)
	err = rh.Run()
	if err != nil {
		// The failure could have been due to stale cluster info cache
		// Force update cluster info cache on failure so that the next
		// retry might succeed
		k.cInfoClient.FetchWithLock()

		logging.Errorf("KVSender::addIndexForExistingBucket %v %v Error from Projector %v",
			streamId, bucket, err)
		respCh <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
		return
	}

	numVbuckets := k.config["numVbuckets"].Int()
	nativeTs := currentTs.ToTsVbuuid(numVbuckets)

	respCh <- &MsgStreamUpdate{mType: MSG_SUCCESS,
		streamId:  streamId,
		bucket:    bucket,
		restartTs: nativeTs}
}

func (k *kvSender) deleteIndexesFromStream(streamId c.StreamId, indexInstList []c.IndexInst,
	respCh MsgChannel, stopCh StopChannel) {

	addrs, err := k.getAllProjectorAddrs()
	if err != nil {
		k.cInfoClient.FetchWithLock()

		logging.Errorf("KVSender::deleteIndexesFromStream %v %v Error in fetching cluster info %v",
			streamId, indexInstList[0].Defn.Bucket, err)
		respCh <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
		return
	}

	var uuids []uint64
	for _, indexInst := range indexInstList {
		uuids = append(uuids, uint64(indexInst.InstId))
	}

	topic := getTopicForStreamId(streamId)

	fn := func(r int, err error) error {

		//clear the error before every retry
		err = nil
		for _, addr := range addrs {
			execWithStopCh(func() {
				doneCh := make(chan bool)
				timeout := time.Duration(TOPIC_REQUEST_TIMEOUT) * time.Millisecond
				_ = k.monitor.AddOperation(
					c.NewOperation(timeout, doneCh, func(elapsed time.Duration) {
						msg := "Slow/Hung Operation: KVSender::sendDelInstancesRequest"
						msg += " did not respond for %v for projector %v topic %v bucket %v"
						logging.Warnf(msg, elapsed, addr, topic, indexInstList[0].Defn.Bucket)
					},
					),
				)

				if ap, ret := newProjClient(addr); ret != nil {
					logging.Errorf("KVSender::deleteIndexesFromStream %v %v Error %v when creating HTTP client to %v",
						streamId, indexInstList[0].Defn.Bucket, ret, addr)
					err = ret
				} else if ret := sendDelInstancesRequest(ap, topic, uuids); ret != nil {

					logging.Errorf("KVSender::deleteIndexesFromStream %v %v Error Received %v from %v",
						streamId, indexInstList[0].Defn.Bucket, ret, addr)
					//Treat TopicMissing/GenServer.Closed/InvalidBucket as success
					if ret.Error() == projClient.ErrorTopicMissing.Error() ||
						ret.Error() == c.ErrorClosed.Error() ||
						ret.Error() == projClient.ErrorInvalidBucket.Error() {
						logging.Infof("KVSender::deleteIndexesFromStream %v %v Treating %v As Success",
							streamId, indexInstList[0].Defn.Bucket, ret)
					} else {
						err = ret
					}
				}
				close(doneCh)
			}, stopCh)
		}
		return err
	}

	rh := c.NewRetryHelper(MAX_KV_REQUEST_RETRY, time.Second, BACKOFF_FACTOR, fn)
	err = rh.Run()
	if err != nil {
		// The failure could have been due to stale cluster info cache
		// Force update cluster info cache on failure so that the next
		// retry might succeed
		k.cInfoClient.FetchWithLock()

		logging.Errorf("KVSender::deleteIndexesFromStream %v %v Error from Projector %v",
			streamId, indexInstList[0].Defn.Bucket, err)
		respCh <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
		return
	}

	respCh <- &MsgSuccess{}
}

func (k *kvSender) deleteBucketsFromStream(streamId c.StreamId, buckets []string,
	respCh MsgChannel, stopCh StopChannel) {

	addrs, err := k.getAllProjectorAddrs()
	if err != nil {
		k.cInfoClient.FetchWithLock()

		logging.Errorf("KVSender::deleteBucketsFromStream %v %v Error in fetching cluster info %v",
			streamId, buckets[0], err)
		respCh <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
		return
	}

	topic := getTopicForStreamId(streamId)

	fn := func(r int, err error) error {

		//clear the error before every retry
		err = nil
		for _, addr := range addrs {
			execWithStopCh(func() {
				doneCh := make(chan bool)
				timeout := time.Duration(TOPIC_REQUEST_TIMEOUT) * time.Millisecond
				_ = k.monitor.AddOperation(
					c.NewOperation(timeout, doneCh, func(elapsed time.Duration) {
						msg := "Slow/Hung Operation: KVSender::sendDelBucketsRequest"
						msg += " did not respond for %v for projector %v topic %v buckets %v"
						logging.Warnf(msg, elapsed, addr, topic, buckets)
					},
					),
				)

				if ap, ret := newProjClient(addr); ret != nil {
					logging.Errorf("KVSender::deleteBucketsFromStream %v %v Error %v when creating HTTP client to %v",
						streamId, buckets[0], ret, addr)
					err = ret
				} else if ret := sendDelBucketsRequest(ap, topic, buckets); ret != nil {
					logging.Errorf("KVSender::deleteBucketsFromStream %v %v Error Received %v from %v",
						streamId, buckets[0], ret, addr)
					//Treat TopicMissing/GenServer.Closed as success
					if ret.Error() == projClient.ErrorTopicMissing.Error() ||
						ret.Error() == c.ErrorClosed.Error() {
						logging.Infof("KVSender::deleteBucketsFromStream %v %v Treating %v As Success",
							streamId, buckets[0], ret)
					} else {
						err = ret
					}
				}
				close(doneCh)
			}, stopCh)
		}
		return err
	}

	rh := c.NewRetryHelper(MAX_KV_REQUEST_RETRY, time.Second, BACKOFF_FACTOR, fn)
	err = rh.Run()
	if err != nil {
		// The failure could have been due to stale cluster info cache
		// Force update cluster info cache on failure so that the next
		// retry might succeed
		k.cInfoClient.FetchWithLock()

		logging.Errorf("KVSender::deleteBucketsFromStream %v %v Error from Projector %v",
			streamId, buckets[0], err)
		respCh <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
		return
	}

	respCh <- &MsgSuccess{}
}

func (k *kvSender) closeMutationStream(streamId c.StreamId, bucket string,
	respCh MsgChannel, stopCh StopChannel) {

	addrs, err := k.getAllProjectorAddrs()
	if err != nil {
		k.cInfoClient.FetchWithLock()

		logging.Errorf("KVSender::closeMutationStream %v %v Error in fetching cluster info %v",
			streamId, bucket, err)
		respCh <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
		return
	}

	topic := getTopicForStreamId(streamId)

	fn := func(r int, err error) error {

		//clear the error before every retry
		err = nil
		for _, addr := range addrs {
			execWithStopCh(func() {
				doneCh := make(chan bool)
				timeout := time.Duration(TOPIC_REQUEST_TIMEOUT) * time.Millisecond
				_ = k.monitor.AddOperation(
					c.NewOperation(timeout, doneCh, func(elapsed time.Duration) {
						msg := "Slow/Hung Operation: KVSender::sendShutdownTopic"
						msg += " did not respond for %v for projector %v topic %v bucket %v"
						logging.Warnf(msg, elapsed, addr, topic, bucket)
					},
					),
				)

				if ap, ret := newProjClient(addr); ret != nil {
					logging.Errorf("KVSender::closeMutationStream %v %v Error %v when creating HTTP client to %v",
						streamId, bucket, ret, addr)
					err = ret
				} else if ret := sendShutdownTopic(ap, topic); ret != nil {
					logging.Errorf("KVSender::closeMutationStream %v %v Error Received %v from %v",
						streamId, bucket, ret, addr)
					//Treat TopicMissing/GenServer.Closed as success
					if ret.Error() == projClient.ErrorTopicMissing.Error() ||
						ret.Error() == c.ErrorClosed.Error() {
						logging.Infof("KVSender::closeMutationStream %v %v Treating %v As Success",
							streamId, bucket, ret)
					} else {
						err = ret
					}
				}
				close(doneCh)
			}, stopCh)
		}
		return err
	}

	rh := c.NewRetryHelper(MAX_KV_REQUEST_RETRY, time.Second, BACKOFF_FACTOR, fn)
	err = rh.Run()
	if err != nil {
		// The failure could have been due to stale cluster info cache
		// Force update cluster info cache on failure so that the next
		// retry might succeed
		k.cInfoClient.FetchWithLock()

		logging.Errorf("KVSender::closeMutationStream, %v %v Error from Projector %v",
			streamId, bucket, err)
		respCh <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
		return
	}

	respCh <- &MsgSuccess{}

}

//send the actual MutationStreamRequest on adminport
func (k *kvSender) sendMutationTopicRequest(ap *projClient.Client, topic string,
	reqTimestamps *protobuf.TsVbuuid, instances []*protobuf.Instance,
	async bool, sessionId uint64) (*protobuf.TopicResponse, error) {

	logging.Infof("KVSender::sendMutationTopicRequest Projector %v Topic %v %v \n\tInstances %v",
		ap, topic, reqTimestamps.GetBucket(), formatInstances(instances))

	logging.LazyVerbosef("KVSender::sendMutationTopicRequest RequestTS %v", reqTimestamps.Repr)

	endpointType := "dataport"

	if res, err := ap.MutationTopicRequest(topic, endpointType,
		[]*protobuf.TsVbuuid{reqTimestamps}, instances, async, sessionId); err != nil {
		logging.Errorf("KVSender::sendMutationTopicRequest Projector %v Topic %v %v \n\tUnexpected Error %v", ap,
			topic, reqTimestamps.GetBucket(), err)

		return res, err
	} else {
		logging.Infof("KVSender::sendMutationTopicRequest Success Projector %v Topic %v %v InstanceIds %v",
			ap, topic, reqTimestamps.GetBucket(), res.GetInstanceIds())
		if logging.IsEnabled(logging.Verbose) {
			logging.Verbosef("KVSender::sendMutationTopicRequest ActiveTs %v \n\tRollbackTs %v",
				debugPrintTs(res.GetActiveTimestamps(), reqTimestamps.GetBucket()),
				debugPrintTs(res.GetRollbackTimestamps(), reqTimestamps.GetBucket()))
		}
		return res, nil
	}
}

func (k *kvSender) sendRestartVbuckets(ap *projClient.Client,
	topic string, connErrVbs []Vbucket,
	restartTs *protobuf.TsVbuuid, sessionId uint64) (*protobuf.TopicResponse, error) {

	logging.Infof("KVSender::sendRestartVbuckets Projector %v Topic %v %v", ap, topic, restartTs.GetBucket())
	logging.LazyVerbosef("KVSender::sendRestartVbuckets RestartTs %v", restartTs.Repr)

	//Shutdown the vbucket before restart if there was a ConnErr. If the vbucket is already
	//running, projector will ignore the request otherwise
	if len(connErrVbs) != 0 {

		logging.Infof("KVSender::sendRestartVbuckets ShutdownVbuckets %v Topic %v %v ConnErrVbs %v",
			ap, topic, restartTs.GetBucket(), connErrVbs)

		// Only shutting down the Vb that receieve connection error.  It is probably not harmful
		// to shutdown every VB in the repairTS, including those that only receive StreamEnd.
		// But due to network / projecctor latency, a VB StreamBegin may be coming on the way
		// for those VB (especially when RepairStream has already retried a couple of times).
		// So shutting all VB in restartTs may unnecessarily causing race condition and
		// make the protocol longer to converge. ShutdownVbuckets should have no effect on
		// projector that does not own the Vb.
		shutdownTs := k.computeShutdownTs(restartTs, connErrVbs)

		logging.Infof("KVSender::sendRestartVbuckets ShutdownVbuckets Projector %v Topic %v %v \n\tShutdownTs %v",
			ap, topic, restartTs.GetBucket(), shutdownTs.Repr())

		if err := ap.ShutdownVbuckets(topic, []*protobuf.TsVbuuid{shutdownTs}); err != nil {
			logging.Errorf("KVSender::sendRestartVbuckets Unexpected Error During "+
				"ShutdownVbuckets Request for Projector %v Topic %v. Err %v.", ap,
				topic, err)

			//all shutdownVbuckets errors are treated as success as it is a best-effort call.
			//RestartVbuckets errors will be acted upon.
		}
	}

	if res, err := ap.RestartVbuckets(topic, sessionId, []*protobuf.TsVbuuid{restartTs}); err != nil {
		logging.Errorf("KVSender::sendRestartVbuckets Unexpected Error During "+
			"Restart Vbuckets Request for Projector %v Topic %v %v . Err %v.", ap,
			topic, restartTs.GetBucket(), err)

		return res, err
	} else {
		logging.Infof("KVSender::sendRestartVbuckets Success Projector %v Topic %v %v",
			ap, topic, restartTs.GetBucket())
		if logging.IsEnabled(logging.Verbose) {
			logging.Verbosef("KVSender::sendRestartVbuckets \nActiveTs %v \nRollbackTs %v",
				debugPrintTs(res.GetActiveTimestamps(), restartTs.GetBucket()),
				debugPrintTs(res.GetRollbackTimestamps(), restartTs.GetBucket()))
		}
		return res, nil
	}
}

//send the actual AddInstances request on adminport
func sendAddInstancesRequest(ap *projClient.Client,
	topic string,
	instances []*protobuf.Instance) (*protobuf.TimestampResponse, error) {

	logging.Infof("KVSender::sendAddInstancesRequest Projector %v Topic %v \nInstances %v",
		ap, topic, formatInstances(instances))

	if res, err := ap.AddInstances(topic, instances); err != nil {
		logging.Errorf("KVSender::sendAddInstancesRequest Unexpected Error During "+
			"Add Instances Request Projector %v Topic %v IndexInst %v. Err %v", ap,
			topic, formatInstances(instances), err)

		return res, err
	} else {
		logging.Infof("KVSender::sendAddInstancesRequest Success Projector %v Topic %v",
			ap, topic)
		logging.LazyDebug(func() string {
			return fmt.Sprintf(
				"KVSender::sendAddInstancesRequest \n\tActiveTs %v ", debugPrintTs(res.GetCurrentTimestamps(), ""))
		})
		return res, nil

	}

}

//send the actual DelInstances request on adminport
func sendDelInstancesRequest(ap *projClient.Client,
	topic string,
	uuids []uint64) error {

	logging.Infof("KVSender::sendDelInstancesRequest Projector %v Topic %v Instances %v",
		ap, topic, uuids)

	if err := ap.DelInstances(topic, uuids); err != nil {
		logging.Errorf("KVSender::sendDelInstancesRequest Unexpected Error During "+
			"Del Instances Request Projector %v Topic %v Instances %v. Err %v", ap,
			topic, uuids, err)

		return err
	} else {
		logging.Infof("KVSender::sendDelInstancesRequest Success Projector %v Topic %v",
			ap, topic)
		return nil

	}

}

//send the actual DelBuckets request on adminport
func sendDelBucketsRequest(ap *projClient.Client,
	topic string,
	buckets []string) error {

	logging.Infof("KVSender::sendDelBucketsRequest Projector %v Topic %v Buckets %v",
		ap, topic, buckets)

	if err := ap.DelBuckets(topic, buckets); err != nil {
		logging.Errorf("KVSender::sendDelBucketsRequest Unexpected Error During "+
			"Del Buckets Request Projector %v Topic %v Buckets %v. Err %v", ap,
			topic, buckets, err)

		return err
	} else {
		logging.Infof("KVSender::sendDelBucketsRequest Success Projector %v Topic %v Buckets %v",
			ap, topic, buckets)
		return nil
	}
}

//send the actual ShutdownStreamRequest on adminport
func sendShutdownTopic(ap *projClient.Client,
	topic string) error {

	logging.Infof("KVSender::sendShutdownTopic Projector %v Topic %v", ap, topic)

	if err := ap.ShutdownTopic(topic); err != nil {
		logging.Errorf("KVSender::sendShutdownTopic Unexpected Error During "+
			"Shutdown Projector %v Topic %v. Err %v", ap, topic, err)

		return err
	} else {
		logging.Infof("KVSender::sendShutdownTopic Success Projector %v Topic %v", ap, topic)
		return nil
	}
}

func getTopicForStreamId(streamId c.StreamId) string {

	return StreamTopicName[streamId]

}

func (k *kvSender) computeShutdownTs(restartTs *protobuf.TsVbuuid, connErrVbs []Vbucket) *protobuf.TsVbuuid {

	numVbuckets := k.config["numVbuckets"].Int()
	shutdownTs := protobuf.NewTsVbuuid(*restartTs.Pool, *restartTs.Bucket, numVbuckets)
	for _, vbno1 := range connErrVbs {
		for i, vbno2 := range restartTs.Vbnos {
			// connErrVbs is a subset of Vb in restartTs.
			if uint32(vbno1) == vbno2 {
				shutdownTs.Append(uint16(vbno1), restartTs.Seqnos[i], restartTs.Vbuuids[i],
					*restartTs.Snapshots[i].Start, *restartTs.Snapshots[i].End)
			}
		}
	}

	return shutdownTs
}

func (k *kvSender) makeRestartTsForVbs(bucket string, tsVbuuid *c.TsVbuuid,
	vbnos []uint32) (*protobuf.TsVbuuid, error) {

	var err error

	var ts *protobuf.TsVbuuid
	if tsVbuuid == nil {
		ts, err = k.makeInitialTs(bucket, vbnos)
	} else {
		ts, err = makeRestartTsFromTsVbuuid(bucket, tsVbuuid, vbnos)
	}
	if err != nil {
		return nil, err
	}

	return ts, nil
}

func updateActiveTsFromResponse(bucket string,
	activeTs *protobuf.TsVbuuid, res *protobuf.TopicResponse) *protobuf.TsVbuuid {

	activeTsList := res.GetActiveTimestamps()
	for _, ts := range activeTsList {
		if ts != nil && !ts.IsEmpty() && ts.GetBucket() == bucket {
			if activeTs == nil {
				activeTs = ts.Clone()
			} else {
				activeTs = activeTs.Union(ts)
			}
		}
	}
	return activeTs

}

func updateRollbackTsFromResponse(bucket string,
	rollbackTs *protobuf.TsVbuuid, res *protobuf.TopicResponse) *protobuf.TsVbuuid {

	rollbackTsList := res.GetRollbackTimestamps()
	for _, ts := range rollbackTsList {
		if ts != nil && !ts.IsEmpty() && ts.GetBucket() == bucket {
			if rollbackTs == nil {
				rollbackTs = ts.Clone()
			} else {
				rollbackTs = rollbackTs.Union(ts)
			}
		}
	}

	return rollbackTs

}

func updatePendingTsFromResponse(bucket string,
	pendingTs *protobuf.TsVbuuid, res *protobuf.TopicResponse) *protobuf.TsVbuuid {

	pendingTsList := res.GetPendingTimestamps()
	for _, ts := range pendingTsList {
		if ts != nil && !ts.IsEmpty() && ts.GetBucket() == bucket {
			if pendingTs == nil {
				pendingTs = ts.Clone()
			} else {
				pendingTs = pendingTs.Union(ts)
			}
		}
	}

	return pendingTs
}

func updateCurrentTsFromResponse(bucket string,
	currentTs *protobuf.TsVbuuid, res *protobuf.TimestampResponse) *protobuf.TsVbuuid {

	currentTsList := res.GetCurrentTimestamps()
	for _, ts := range currentTsList {
		if ts != nil && !ts.IsEmpty() && ts.GetBucket() == bucket {
			if currentTs == nil {
				currentTs = ts.Clone()
			} else {
				currentTs = currentTs.Union(ts)
			}
		}
	}
	return currentTs

}

func (k *kvSender) makeInitialTs(bucket string,
	vbnos []uint32) (*protobuf.TsVbuuid, error) {

	ts := protobuf.NewTsVbuuid(DEFAULT_POOL, bucket, len(vbnos))

	for vbno := range vbnos {
		ts.Append(uint16(vbno), 0, 0, 0, 0)
	}
	return ts, nil
}

func (k *kvSender) makeRestartTsFromKV(bucket string,
	vbnos []uint32) (*protobuf.TsVbuuid, error) {

	flogs, err := k.getFailoverLogs(bucket, vbnos)
	if err != nil {
		logging.Errorf("KVSender::makeRestartTS Unexpected Error During Failover "+
			"Log Request for Bucket %v. Err %v", bucket, err)
		return nil, err
	}

	ts := protobuf.NewTsVbuuid(DEFAULT_POOL, bucket, len(vbnos))
	ts = ts.ComputeRestartTs(flogs.ToFailoverLog(c.Vbno32to16(vbnos)))

	return ts, nil
}

func makeRestartTsFromTsVbuuid(bucket string, tsVbuuid *c.TsVbuuid,
	vbnos []uint32) (*protobuf.TsVbuuid, error) {

	ts := protobuf.NewTsVbuuid(DEFAULT_POOL, bucket, len(vbnos))
	for _, vbno := range vbnos {
		ts.Append(uint16(vbno), tsVbuuid.Seqnos[vbno],
			tsVbuuid.Vbuuids[vbno], tsVbuuid.Snapshots[vbno][0],
			tsVbuuid.Snapshots[vbno][1])
	}

	return ts, nil

}

func (k *kvSender) getFailoverLogs(bucket string,
	vbnos []uint32) (*protobuf.FailoverLogResponse, error) {

	var err error
	var client *projClient.Client
	var res *protobuf.FailoverLogResponse

	addrs, err := k.getAllProjectorAddrs()
	if err != nil {
		k.cInfoClient.FetchWithLock()
		return nil, err
	}

loop:
	for _, addr := range addrs {
		//create client for node's projectors
		client, err = newProjClient(addr)
		if err != nil {
			logging.Errorf("KVSender::getFailoverlogs %v Error %v when creating HTTP client to %v",
				bucket, err, addr)
			continue
		}

		if res, err = client.GetFailoverLogs(DEFAULT_POOL, bucket, vbnos); err == nil {
			break loop
		}
	}

	if logging.IsEnabled(logging.Debug) {
		s := ""
		if res != nil {
			for _, l := range res.GetLogs() {
				s += fmt.Sprintf("\t%v\n", l)
			}
		}
		logging.Debugf("KVSender::getFailoverLogs Failover Log Response Error %v \n%v", err, s)
	}

	return res, err
}

func (k *kvSender) getAllVbucketsInCluster(bucket string) ([]uint32, []string, error) {

	cinfo := k.cInfoClient.GetClusterInfoCache()

	err := cinfo.FetchBucketInfo(bucket)
	if err != nil {
		return nil, nil, err
	}

	cinfo.RLock()
	defer cinfo.RUnlock()

	//get all kv nodes
	nodes, err := cinfo.GetNodesByBucket(bucket)
	if err != nil {
		return nil, nil, err
	}

	var vbs []uint32
	var addrList []string

	for _, nid := range nodes {
		//get the list of vbnos for this kv
		if vbnos, err := cinfo.GetVBuckets(nid, bucket); err != nil {
			return nil, nil, err
		} else {
			vbs = append(vbs, vbnos...)
			addr, err := cinfo.GetServiceAddress(nid, "projector")
			if err != nil {
				return nil, nil, err
			}
			addrList = append(addrList, addr)

		}
	}
	return vbs, addrList, nil
}

func (k *kvSender) getAllProjectorAddrs() ([]string, error) {

	cinfo := k.cInfoClient.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	nodes := cinfo.GetNodesByServiceType("projector")

	var addrList []string
	for _, nid := range nodes {
		addr, err := cinfo.GetServiceAddress(nid, "projector")
		if err != nil {
			return nil, err
		}
		addrList = append(addrList, addr)
	}
	return addrList, nil

}

func (k *kvSender) getProjAddrsForVbuckets(bucket string, vbnos []Vbucket) ([]string, error) {

	cinfo := k.cInfoClient.GetClusterInfoCache()

	err := cinfo.FetchBucketInfo(bucket)
	if err != nil {
		return nil, err
	}

	cinfo.RLock()
	defer cinfo.RUnlock()

	var addrList []string

	nodes, err := cinfo.GetNodesByBucket(bucket)
	if err != nil {
		return nil, err
	}

	for _, n := range nodes {
		vbs, err := cinfo.GetVBuckets(n, bucket)
		if err != nil {
			return nil, err
		}
		found := false
	outerloop:
		for _, vb := range vbs {
			for _, vbc := range vbnos {
				if vb == uint32(vbc) {
					found = true
					break outerloop
				}
			}
		}

		if found {
			addr, err := cinfo.GetServiceAddress(n, "projector")
			if err != nil {
				return nil, err
			}
			addrList = append(addrList, addr)
		}
	}
	return addrList, nil
}

func (k *kvSender) handleConfigUpdate(cmd Message) {
	cfgUpdate := cmd.(*MsgConfigUpdate)
	k.config = cfgUpdate.GetConfig()

	k.supvCmdch <- &MsgSuccess{}
}

// convert IndexInst to protobuf format
func convertIndexListToProto(cfg c.Config, cic *c.ClusterInfoClient, indexList []c.IndexInst,
	streamId c.StreamId) []*protobuf.Instance {

	protoList := make([]*protobuf.Instance, 0)
	for _, index := range indexList {
		protoInst := convertIndexInstToProtoInst(cfg, cic, index, streamId)
		protoList = append(protoList, protoInst)
	}

	for _, index := range indexList {
		if c.IsPartitioned(index.Defn.PartitionScheme) && index.RealInstId != 0 {
			for _, protoInst := range protoList {
				if protoInst.IndexInstance.GetInstId() == uint64(index.RealInstId) {
					addPartnInfoToProtoInst(cfg, cic, index, streamId, protoInst.IndexInstance)
				}
			}
		}
	}

	return protoList

}

// convert IndexInst to protobuf format
func convertIndexInstToProtoInst(cfg c.Config, cic *c.ClusterInfoClient,
	indexInst c.IndexInst, streamId c.StreamId) *protobuf.Instance {

	protoDefn := convertIndexDefnToProtobuf(indexInst.Defn)
	protoInst := convertIndexInstToProtobuf(cfg, indexInst, protoDefn)

	addPartnInfoToProtoInst(cfg, cic, indexInst, streamId, protoInst)

	return &protobuf.Instance{IndexInstance: protoInst}
}

func convertIndexDefnToProtobuf(indexDefn c.IndexDefn) *protobuf.IndexDefn {

	using := protobuf.StorageType(
		protobuf.StorageType_value[strings.ToLower(string(indexDefn.Using))]).Enum()
	exprType := protobuf.ExprType(
		protobuf.ExprType_value[strings.ToUpper(string(indexDefn.ExprType))]).Enum()
	partnScheme := protobuf.PartitionScheme(
		protobuf.PartitionScheme_value[string(c.SINGLE)]).Enum()
	if c.IsPartitioned(indexDefn.PartitionScheme) {
		partnScheme = protobuf.PartitionScheme(
			protobuf.PartitionScheme_value[string(c.KEY)]).Enum()
	}

	defn := &protobuf.IndexDefn{
		DefnID:             proto.Uint64(uint64(indexDefn.DefnId)),
		Bucket:             proto.String(indexDefn.Bucket),
		IsPrimary:          proto.Bool(indexDefn.IsPrimary),
		Name:               proto.String(indexDefn.Name),
		Using:              using,
		ExprType:           exprType,
		SecExpressions:     indexDefn.SecExprs,
		PartitionScheme:    partnScheme,
		PartnExpressions:   indexDefn.PartitionKeys,
		HashScheme:         protobuf.HashScheme(indexDefn.HashScheme).Enum(),
		WhereExpression:    proto.String(indexDefn.WhereExpr),
		RetainDeletedXATTR: proto.Bool(indexDefn.RetainDeletedXATTR),
	}

	return defn

}

func convertIndexInstToProtobuf(cfg c.Config, indexInst c.IndexInst,
	protoDefn *protobuf.IndexDefn) *protobuf.IndexInst {

	state := protobuf.IndexState(int32(indexInst.State)).Enum()
	instance := &protobuf.IndexInst{
		InstId:     proto.Uint64(uint64(indexInst.InstId)),
		State:      state,
		Definition: protoDefn,
	}
	return instance
}

func addPartnInfoToProtoInst(cfg c.Config, cic *c.ClusterInfoClient,
	indexInst c.IndexInst, streamId c.StreamId, protoInst *protobuf.IndexInst) {

	switch partn := indexInst.Pc.(type) {
	case *c.KeyPartitionContainer:

		//Right now the fill the SinglePartition as that is the only
		//partition structure supported
		partnDefn := partn.GetAllPartitions()

		//TODO move this to indexer init. These addresses cannot change.
		//Better to get these once and store.
		cinfo := cic.GetClusterInfoCache()
		cinfo.RLock()
		defer cinfo.RUnlock()

		host, err := cinfo.GetLocalHostname()
		c.CrashOnError(err)

		streamMaintAddr := net.JoinHostPort(host, cfg["streamMaintPort"].String())
		streamInitAddr := net.JoinHostPort(host, cfg["streamInitPort"].String())
		streamCatchupAddr := net.JoinHostPort(host, cfg["streamCatchupPort"].String())

		endpointsMap := make(map[c.Endpoint]bool)
		for _, p := range partnDefn {
			for _, e := range p.Endpoints() {
				//Set the right endpoint based on streamId
				switch streamId {
				case c.MAINT_STREAM:
					e = c.Endpoint(streamMaintAddr)
				case c.CATCHUP_STREAM:
					e = c.Endpoint(streamCatchupAddr)
				case c.INIT_STREAM:
					e = c.Endpoint(streamInitAddr)
				}
				endpointsMap[e] = true
			}
		}

		endpoints := make([]string, 0, len(endpointsMap))
		for e, _ := range endpointsMap {
			endpoints = append(endpoints, string(e))
		}

		if !c.IsPartitioned(indexInst.Defn.PartitionScheme) {
			protoInst.SinglePartn = &protobuf.SinglePartition{
				Endpoints: endpoints,
			}
		} else {
			partIds := make([]uint64, len(partnDefn))
			for i, p := range partnDefn {
				partIds[i] = uint64(p.GetPartitionId())
			}

			if protoInst.KeyPartn == nil {
				protoInst.KeyPartn = protobuf.NewKeyPartition(uint64(indexInst.Pc.GetNumPartitions()), endpoints, partIds)
			} else {
				protoInst.KeyPartn.AddPartitions(partIds)
			}
		}
	}
}

//create client for node's projectors
func newProjClient(addr string) (*projClient.Client, error) {

	config := c.SystemConfig.SectionConfig("indexer.projectorclient.", true)
	config.SetValue("retryInterval", 0) //no retry
	maxvbs := c.SystemConfig["maxVbuckets"].Int()
	return projClient.NewClient(addr, maxvbs, config)

}

func compareIfActiveTsEqual(origTs, compTs *c.TsVbuuid) bool {

	vbnosOrig := origTs.GetVbnos()

	vbnosComp := compTs.GetVbnos()

	for i, vb := range vbnosOrig {
		if vbnosComp[i] != vb {
			return false
		}
	}
	return true

}

//check if any vb in vbList is part of the given ts
func checkVbListInTS(vbList []uint32, ts *protobuf.TsVbuuid) bool {

	for _, vb := range vbList {
		if ts.Contains(uint16(vb)) == true {
			return true
		}
	}
	return false

}

func execWithStopCh(fn func(), stopCh StopChannel) bool {

	select {

	case <-stopCh:
		return true

	default:
		fn()
		return false

	}

}

func debugPrintTs(tsList []*protobuf.TsVbuuid, bucket string) string {

	if len(tsList) == 0 {
		return ""
	}

	for _, ts := range tsList {
		if bucket == "" {
			return ts.Repr()
		} else if ts.GetBucket() == bucket {
			return ts.Repr()
		}
	}

	return ""
}

func formatInstances(instances []*protobuf.Instance) string {
	instanceStr := "["
	for _, instance := range instances {
		inst := instance.GetIndexInstance()
		defn := inst.GetDefinition()
		instanceStr += "indexInstance:<"
		instanceStr += fmt.Sprintf("instId:%v ", inst.GetInstId())
		instanceStr += fmt.Sprintf("state:%v ", inst.GetState())
		instanceStr += fmt.Sprintf(" definition:<defnID:%v bucket:%v isPrimary:%v name:%v using:%v "+
			"exprType:%v secExpressions:%v partitionScheme:%v whereExpression:%v > ",
			defn.GetDefnID(), defn.GetBucket(), defn.GetIsPrimary(),
			defn.GetName(), defn.GetUsing(), defn.GetExprType(),
			logging.TagUD(defn.GetSecExpressions()), defn.GetPartitionScheme(),
			logging.TagUD(defn.GetWhereExpression()))
		instanceStr += fmt.Sprintf("singlePartn:%v", inst.GetSinglePartn())
		instanceStr += "> "
	}
	instanceStr += "]"
	return instanceStr
}
