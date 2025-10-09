// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

// TODO:
// 1. functions in this package directly access SystemConfig, instead it
//    is suggested to pass config via function argument.

package indexer

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/common/collections"
	"github.com/couchbase/indexing/secondary/logging"
	projClient "github.com/couchbase/indexing/secondary/projector/client"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
	"github.com/golang/protobuf/proto"
)

const (
	HTTP_PREFIX             string = "http://"
	MAX_KV_REQUEST_RETRY    int    = 0
	BACKOFF_FACTOR          int    = 2
	MAX_CLUSTER_FETCH_RETRY uint32 = 600

	// op_monitor constants
	KV_SENDER_OP_MONITOR_SIZE     int = 1024
	KV_SENDER_OP_MONITOR_BATCHSZ  int = 0          // No batching
	KV_SENDER_OP_MONITOR_INTERVAL int = 1000       // In Milliseconds
	TOPIC_REQUEST_TIMEOUT         int = 300 * 1000 // In Milliseconds
)

// KVSender provides the mechanism to talk to KV(projector, router etc)
type KVSender interface {
}

type kvSender struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any message to supervisor

	config  c.Config
	monitor *c.OperationsMonitor

	cinfoProvider     c.ClusterInfoProvider
	cinfoProviderLock sync.RWMutex
}

func NewKVSender(supvCmdch MsgChannel, supvRespch MsgChannel,
	config c.Config) (KVSender, Message) {

	// Disabled due to MB-51636
	// useCInfoLite := config["use_cinfo_lite"].Bool()
	useCInfoLite := false
	cip, err := common.NewClusterInfoProvider(useCInfoLite, config["clusterAddr"].String(),
		DEFAULT_POOL, "kvsender", config)
	if err != nil {
		logging.Errorf("NewKVSender Unable to get new ClusterInfoProvider err: %v use_cinfo_lite: %v",
			err, useCInfoLite)
		common.CrashOnError(err)
	}
	cip.SetMaxRetries(MAX_CLUSTER_FETCH_RETRY)

	//Init the kvSender struct
	k := &kvSender{
		supvCmdch:  supvCmdch,
		supvRespch: supvRespch,
		config:     config,
		monitor: c.NewOperationsMonitor(
			"kvSender",
			KV_SENDER_OP_MONITOR_SIZE,
			KV_SENDER_OP_MONITOR_INTERVAL,
			KV_SENDER_OP_MONITOR_BATCHSZ,
		),
	}

	k.cinfoProviderLock.Lock()
	defer k.cinfoProviderLock.Unlock()
	k.cinfoProvider = cip

	//start kvsender loop which listens to commands from its supervisor
	go k.run()

	return k, &MsgSuccess{}

}

func (k *kvSender) FetchCInfoWithLock() {
	k.cinfoProviderLock.RLock()
	defer k.cinfoProviderLock.RUnlock()

	k.cinfoProvider.ForceFetch()
}

func (k *kvSender) getNumVBucketsWithLock(bucket string) (int, error) {
	k.cinfoProviderLock.RLock()
	defer k.cinfoProviderLock.RUnlock()

	return k.cinfoProvider.GetNumVBuckets(bucket)
}

// run starts the kvsender loop which listens to messages
// from it supervisor(indexer)
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

	case REMOVE_KEYSPACE_FROM_STREAM:
		k.handleRemoveKeyspaceFromStream(cmd)

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
	keyspaceId := cmd.(*MsgStreamUpdate).GetKeyspaceId()
	collectionId := cmd.(*MsgStreamUpdate).GetCollectionId()
	async := cmd.(*MsgStreamUpdate).GetAsync()
	sessionId := cmd.(*MsgStreamUpdate).GetSessionId()
	collectionAware := cmd.(*MsgStreamUpdate).CollectionAware()
	enableOSO := cmd.(*MsgStreamUpdate).EnableOSO()
	timeBarrier := cmd.(*MsgStreamUpdate).GetTimeBarrier()
	projNumVbWorkers := cmd.(*MsgStreamUpdate).GetProjNumVbWorkers()
	projNumDcpConns := cmd.(*MsgStreamUpdate).GetProjNumDcpConns()

	logging.LazyDebug(func() string {
		return fmt.Sprintf("KVSender::handleOpenStream %v %v %v",
			streamId, keyspaceId, cmd)
	})

	go k.openMutationStream(streamId, keyspaceId, collectionId,
		indexInstList, restartTs, async, sessionId, collectionAware, enableOSO,
		respCh, stopCh, timeBarrier, projNumVbWorkers, projNumDcpConns)

	k.supvCmdch <- &MsgSuccess{}

}

func (k *kvSender) handleAddIndexListToStream(cmd Message) {

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()
	keyspaceId := cmd.(*MsgStreamUpdate).GetKeyspaceId()
	addIndexList := cmd.(*MsgStreamUpdate).GetIndexList()
	respCh := cmd.(*MsgStreamUpdate).GetResponseChannel()
	stopCh := cmd.(*MsgStreamUpdate).GetStopChannel()

	logging.LazyDebug(func() string {
		return fmt.Sprintf("KVSender::handleAddIndexListToStream %v %v %v",
			streamId, keyspaceId, cmd)
	})

	go k.addIndexForExistingKeyspace(streamId, keyspaceId,
		addIndexList, respCh, stopCh)

	k.supvCmdch <- &MsgSuccess{}
}

func (k *kvSender) handleRemoveIndexListFromStream(cmd Message) {

	logging.LazyDebug(func() string {
		return fmt.Sprintf("KVSender::handleRemoveIndexListFromStream %v", cmd)
	})

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()
	keyspaceId := cmd.(*MsgStreamUpdate).GetKeyspaceId()
	delIndexList := cmd.(*MsgStreamUpdate).GetIndexList()
	respCh := cmd.(*MsgStreamUpdate).GetResponseChannel()
	stopCh := cmd.(*MsgStreamUpdate).GetStopChannel()

	go k.deleteIndexesFromStream(streamId, keyspaceId,
		delIndexList, respCh, stopCh)

	k.supvCmdch <- &MsgSuccess{}
}

func (k *kvSender) handleRemoveKeyspaceFromStream(cmd Message) {

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()
	keyspaceId := cmd.(*MsgStreamUpdate).GetKeyspaceId()
	respCh := cmd.(*MsgStreamUpdate).GetResponseChannel()
	stopCh := cmd.(*MsgStreamUpdate).GetStopChannel()

	logging.LazyDebug(func() string {
		return fmt.Sprintf("KVSender::handleRemoveKeyspaceFromStream %v %v %v", streamId, keyspaceId, cmd)
	})

	go k.deleteKeyspacesFromStream(streamId, []string{keyspaceId},
		respCh, stopCh)

	k.supvCmdch <- &MsgSuccess{}
}

func (k *kvSender) handleCloseStream(cmd Message) {

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()
	keyspaceId := cmd.(*MsgStreamUpdate).GetKeyspaceId()
	respCh := cmd.(*MsgStreamUpdate).GetResponseChannel()
	stopCh := cmd.(*MsgStreamUpdate).GetStopChannel()

	logging.LazyDebug(func() string {
		return fmt.Sprintf("KVSender::handleCloseStream %v %v %v", streamId, keyspaceId, cmd)
	})

	go k.closeMutationStream(streamId, keyspaceId, respCh, stopCh)

	k.supvCmdch <- &MsgSuccess{}
}

func (k *kvSender) handleRestartVbuckets(cmd Message) {

	streamId := cmd.(*MsgRestartVbuckets).GetStreamId()
	keyspaceId := cmd.(*MsgRestartVbuckets).GetKeyspaceId()
	collectionId := cmd.(*MsgRestartVbuckets).GetCollectionId()
	restartTs := cmd.(*MsgRestartVbuckets).GetRestartTs()
	respCh := cmd.(*MsgRestartVbuckets).GetResponseCh()
	stopCh := cmd.(*MsgRestartVbuckets).GetStopChannel()
	connErrVbs := cmd.(*MsgRestartVbuckets).ConnErrVbs()
	repairVbs := cmd.(*MsgRestartVbuckets).RepairVbs()
	sessionId := cmd.(*MsgRestartVbuckets).GetSessionId()

	logging.LazyDebug(func() string {
		return fmt.Sprintf("KVSender::handleRestartVbuckets %v %v %v",
			streamId, keyspaceId, cmd)
	})

	go k.restartVbuckets(streamId, keyspaceId, collectionId,
		restartTs, connErrVbs, repairVbs, sessionId, respCh, stopCh)

	k.supvCmdch <- &MsgSuccess{}
}

func (k *kvSender) openMutationStream(streamId c.StreamId, keyspaceId string,
	collectionId string, indexInstList []c.IndexInst, restartTs *c.TsVbuuid,
	async bool, sessionId uint64, collectionAware bool, enableOSO bool,
	respCh MsgChannel, stopCh StopChannel, timeBarrier time.Time,
	projNumVbWorkers int, projNumDcpConns int) {

	if len(indexInstList) == 0 {
		logging.Warnf("KVSender::openMutationStream Empty IndexList. Nothing to do.")
		respCh <- &MsgSuccess{}
		return
	}

	for {
		if time.Now().Before(timeBarrier) {
			time.Sleep(time.Second)
		} else {
			break
		}
	}

	k.cinfoProviderLock.RLock()
	protoInstList := convertIndexListToProto(k.config, k.cinfoProvider, indexInstList, streamId)
	k.cinfoProviderLock.RUnlock()

	bucket, _, _ := SplitKeyspaceId(keyspaceId)

	//use any bucket as list of vbs remain the same for all buckets
	vbnos, addrs, numVbuckets, err := k.getAllVbucketsInCluster(bucket)
	if err != nil {
		// This failure could be due to stale cluster info cache
		// Force fetch cluster info cache so that the next call
		// might succeed
		k.FetchCInfoWithLock()
		logging.Errorf("KVSender::openMutationStream %v %v Error in fetching vbuckets info %v",
			streamId, bucket, err)
		respCh <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
		return
	}

	if len(vbnos) != numVbuckets {
		logging.Warnf("KVSender::openMutationStream mismatch in number of configured "+
			"vbuckets. conf %v actual %v", numVbuckets, vbnos)
	}

	restartTsList, err := k.makeRestartTsForVbs(bucket, collectionId, restartTs, vbnos)
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
						msg += " did not respond for %v for projector %v topic %v keyspaceId %v"
						logging.Warnf(msg, elapsed, addr, topic, keyspaceId)
					},
					),
				)

				if ap, ret := newProjClient(addr); ret != nil {
					logging.Errorf("KVSender::openMutationStream %v %v Error %v when "+
						" creating HTTP client to %v", streamId, keyspaceId, ret, addr)
					err = ret
				} else if res, ret := k.sendMutationTopicRequest(ap, topic, keyspaceId,
					restartTsList, protoInstList, async, sessionId, collectionAware,
					enableOSO, projNumVbWorkers, projNumDcpConns); ret != nil {
					//for all errors, retry
					logging.Errorf("KVSender::openMutationStream %v %v Error Received %v from %v",
						streamId, keyspaceId, ret, addr)
					err = ret
				} else {
					activeTs = updateActiveTsFromResponse(keyspaceId, activeTs, res)
					rollbackTs = updateRollbackTsFromResponse(keyspaceId, rollbackTs, res)
					pendingTs = updatePendingTsFromResponse(keyspaceId, pendingTs, res)
					if rollbackTs != nil {
						logging.Infof("KVSender::openMutationStream %v %v Projector %v Rollback Received %v",
							streamId, keyspaceId, addr, rollbackTs)
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
			keyspaceId: keyspaceId,
			rollbackTs: nativeTs}
	} else if err != nil {
		// The failure could have been due to stale cluster info cache
		// Force update cluster info cache on failure so that the next
		// retry might succeed
		k.FetchCInfoWithLock()

		logging.Errorf("KVSender::openMutationStream %v %v Error from Projector %v",
			streamId, keyspaceId, err)
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

func (k *kvSender) restartVbuckets(streamId c.StreamId, keyspaceId string,
	collectionId string, restartTs *c.TsVbuuid, connErrVbs []Vbucket,
	repairVbs []Vbucket, sessionId uint64, respCh MsgChannel, stopCh StopChannel) {

	bucket, _, _ := SplitKeyspaceId(keyspaceId)
	addrs, numVbuckets, err := k.getProjAddrsForVbuckets(bucket, repairVbs)
	if err != nil {
		k.FetchCInfoWithLock()

		logging.Errorf("KVSender::restartVbuckets %v %v Error in fetching cluster info %v",
			streamId, keyspaceId, err)
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
	protoTs := protobuf.NewTsVbuuid(DEFAULT_POOL, bucket, numVbuckets)
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
			protoRestartTs.ManifestUIDs = append(protoRestartTs.ManifestUIDs, collections.MANIFEST_UID_EPOCH)
			protoRestartTs.Snapshots = append(protoRestartTs.Snapshots, protobuf.NewSnapshot(0, 0))
		}
	}
	sort.Sort(protoRestartTs)

	if collectionId != "" {
		protoRestartTs.CollectionIDs = []string{collectionId}
	}

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
						msg += " did not respond for %v for projector %v topic %v keyspaceId %v"
						logging.Warnf(msg, elapsed, addr, topic, keyspaceId)
					},
					),
				)

				if ap, ret := newProjClient(addr); ret != nil {
					logging.Errorf("KVSender::restartVbuckets %v %v Error %v when creating HTTP client to %v",
						streamId, keyspaceId, ret, addr)
					err = ret
				} else if res, ret := k.sendRestartVbuckets(ap, topic, keyspaceId, connErrVbs,
					protoRestartTs, sessionId); ret != nil {
					//retry for all errors
					logging.Errorf("KVSender::restartVbuckets %v %v Error Received %v from %v",
						streamId, keyspaceId, ret, addr)
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
			keyspaceId: keyspaceId}
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
				streamId:   streamId,
				keyspaceId: keyspaceId,
				sessionId:  sessionId,
			}
		} else {
			// The failure could have been due to stale cluster info cache
			// Force update cluster info cache on failure so that the next
			// retry might succeed
			k.FetchCInfoWithLock()

			respCh <- &MsgError{
				err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
					severity: FATAL,
					cause:    err},
				sessionId: sessionId,
			}

		}
	} else {
		resp := &MsgRestartVbucketsResponse{
			streamId:   streamId,
			keyspaceId: keyspaceId,
			sessionId:  sessionId,
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

func (k *kvSender) addIndexForExistingKeyspace(streamId c.StreamId, keyspaceId string, indexInstList []c.IndexInst,
	respCh MsgChannel, stopCh StopChannel) {

	bucket, _, _ := SplitKeyspaceId(keyspaceId)
	_, addrs, numVbuckets, err := k.getAllVbucketsInCluster(bucket)
	if err != nil {
		k.FetchCInfoWithLock()

		logging.Errorf("KVSender::addIndexForExistingKeyspace %v %v Error in fetching cluster info %v",
			streamId, keyspaceId, err)
		respCh <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
		return
	}

	var currentTs *protobuf.TsVbuuid
	k.cinfoProviderLock.RLock()
	protoInstList := convertIndexListToProto(k.config, k.cinfoProvider, indexInstList, streamId)
	k.cinfoProviderLock.RUnlock()
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
						msg += " did not respond for %v for projector %v topic %v keyspaceId %v"
						logging.Warnf(msg, elapsed, addr, topic, keyspaceId)
					},
					),
				)

				if ap, ret := newProjClient(addr); ret != nil {
					logging.Errorf("KVSender::addIndexForExistingKeyspace %v %v Error %v when creating HTTP client to %v",
						streamId, keyspaceId, ret, addr)
					err = ret
				} else if res, ret := sendAddInstancesRequest(ap, topic, keyspaceId, protoInstList); ret != nil {
					logging.Errorf("KVSender::addIndexForExistingKeyspace %v %v Error Received %v from %v",
						streamId, keyspaceId, ret, addr)
					err = ret
				} else {
					currentTs = updateCurrentTsFromResponse(bucket, currentTs, res)
				}
				close(doneCh)
			}, stopCh)
		}

		//check if we have received currentTs for all vbuckets
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
		k.FetchCInfoWithLock()

		logging.Errorf("KVSender::addIndexForExistingKeyspace %v %v Error from Projector %v",
			streamId, keyspaceId, err)
		respCh <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
		return
	}

	nativeTs := currentTs.ToTsVbuuid(numVbuckets)

	respCh <- &MsgStreamUpdate{mType: MSG_SUCCESS,
		streamId:   streamId,
		keyspaceId: keyspaceId,
		restartTs:  nativeTs}
}

func (k *kvSender) deleteIndexesFromStream(streamId c.StreamId, keyspaceId string,
	indexInstList []c.IndexInst, respCh MsgChannel, stopCh StopChannel) {

	addrs, err := k.getAllProjectorAddrs()
	if err != nil {
		k.FetchCInfoWithLock()

		logging.Errorf("KVSender::deleteIndexesFromStream %v %v Error in fetching cluster info %v",
			streamId, keyspaceId, err)
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
						msg += " did not respond for %v for projector %v topic %v keyspaceId %v"
						logging.Warnf(msg, elapsed, addr, topic, keyspaceId)
					},
					),
				)

				if ap, ret := newProjClient(addr); ret != nil {
					logging.Errorf("KVSender::deleteIndexesFromStream %v %v Error %v when creating HTTP client to %v",
						streamId, keyspaceId, ret, addr)
					err = ret
				} else if ret := sendDelInstancesRequest(ap, topic, keyspaceId, uuids); ret != nil {

					logging.Errorf("KVSender::deleteIndexesFromStream %v %v Error Received %v from %v",
						streamId, keyspaceId, ret, addr)
					//Treat TopicMissing/GenServer.Closed/InvalidBucket as success
					if ret.Error() == projClient.ErrorTopicMissing.Error() ||
						ret.Error() == c.ErrorClosed.Error() ||
						ret.Error() == projClient.ErrorInvalidBucket.Error() {
						logging.Infof("KVSender::deleteIndexesFromStream %v %v Treating %v As Success",
							streamId, keyspaceId, ret)
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
		k.FetchCInfoWithLock()

		logging.Errorf("KVSender::deleteIndexesFromStream %v %v Error from Projector %v",
			streamId, keyspaceId, err)
		respCh <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
		return
	}

	respCh <- &MsgSuccess{}
}

func (k *kvSender) deleteKeyspacesFromStream(streamId c.StreamId, keyspaceIds []string,
	respCh MsgChannel, stopCh StopChannel) {

	addrs, err := k.getAllProjectorAddrs()
	if err != nil {
		k.FetchCInfoWithLock()

		logging.Errorf("KVSender::deleteKeyspacesFromStream %v %v Error in fetching cluster info %v",
			streamId, keyspaceIds, err)
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
						msg := "Slow/Hung Operation: KVSender::sendDelKeyspacesRequest"
						msg += " did not respond for %v for projector %v topic %v keyspaceIds %v"
						logging.Warnf(msg, elapsed, addr, topic, keyspaceIds)
					},
					),
				)

				if ap, ret := newProjClient(addr); ret != nil {
					logging.Errorf("KVSender::deleteKeyspacesFromStream %v %v Error %v when creating HTTP client to %v",
						streamId, keyspaceIds, ret, addr)
					err = ret
				} else if ret := sendDelKeyspacesRequest(ap, topic, keyspaceIds); ret != nil {
					logging.Errorf("KVSender::deleteKeyspacesFromStream %v %v Error Received %v from %v",
						streamId, keyspaceIds, ret, addr)
					//Treat TopicMissing/GenServer.Closed as success
					if ret.Error() == projClient.ErrorTopicMissing.Error() ||
						ret.Error() == c.ErrorClosed.Error() {
						logging.Infof("KVSender::deleteKeyspacesFromStream %v %v Treating %v As Success",
							streamId, keyspaceIds, ret)
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
		k.FetchCInfoWithLock()

		logging.Errorf("KVSender::deleteKeyspacesFromStream %v %v Error from Projector %v",
			streamId, keyspaceIds, err)
		respCh <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
		return
	}

	respCh <- &MsgSuccess{}
}

func (k *kvSender) closeMutationStream(streamId c.StreamId, keyspaceId string,
	respCh MsgChannel, stopCh StopChannel) {

	addrs, err := k.getAllProjectorAddrs()
	if err != nil {
		k.FetchCInfoWithLock()

		logging.Errorf("KVSender::closeMutationStream %v %v Error in fetching cluster info %v",
			streamId, keyspaceId, err)
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
						msg += " did not respond for %v for projector %v topic %v keyspaceId %v"
						logging.Warnf(msg, elapsed, addr, topic, keyspaceId)
					},
					),
				)

				if ap, ret := newProjClient(addr); ret != nil {
					logging.Errorf("KVSender::closeMutationStream %v %v Error %v when creating HTTP client to %v",
						streamId, keyspaceId, ret, addr)
					err = ret
				} else if ret := sendShutdownTopic(ap, topic); ret != nil {
					logging.Errorf("KVSender::closeMutationStream %v %v Error Received %v from %v",
						streamId, keyspaceId, ret, addr)
					//Treat TopicMissing/GenServer.Closed as success
					if ret.Error() == projClient.ErrorTopicMissing.Error() ||
						ret.Error() == c.ErrorClosed.Error() {
						logging.Infof("KVSender::closeMutationStream %v %v Treating %v As Success",
							streamId, keyspaceId, ret)
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
		k.FetchCInfoWithLock()

		logging.Errorf("KVSender::closeMutationStream, %v %v Error from Projector %v",
			streamId, keyspaceId, err)
		respCh <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
		return
	}

	respCh <- &MsgSuccess{}

}

// send the actual MutationStreamRequest on adminport
func (k *kvSender) sendMutationTopicRequest(ap *projClient.Client, topic string,
	keyspaceId string, reqTimestamps *protobuf.TsVbuuid, instances []*protobuf.Instance,
	async bool, sessionId uint64, collectionAware bool, enableOSO bool,
	projNumVbWorkers int, projNumDcpConns int) (*protobuf.TopicResponse, error) {

	logging.Infof("KVSender::sendMutationTopicRequest Projector %v Topic %v %v \n\tInstances %v",
		ap, topic, keyspaceId, formatInstances(instances))

	logging.LazyVerbosef("KVSender::sendMutationTopicRequest RequestTS %v", reqTimestamps.Repr)

	endpointType := "dataport"

	if res, err := ap.MutationTopicRequest(topic, endpointType,
		[]*protobuf.TsVbuuid{reqTimestamps}, instances, async,
		sessionId, []string{keyspaceId}, collectionAware, enableOSO,
		true, uint32(projNumVbWorkers), uint32(projNumDcpConns)); err != nil {
		logging.Errorf("KVSender::sendMutationTopicRequest Projector %v Topic %v %v \n\tUnexpected Error %v", ap,
			topic, keyspaceId, err)

		return res, err
	} else {
		logging.Infof("KVSender::sendMutationTopicRequest Success Projector %v Topic %v %v InstanceIds %v",
			ap, topic, keyspaceId, res.GetInstanceIds())
		if logging.IsEnabled(logging.Verbose) {
			logging.Verbosef("KVSender::sendMutationTopicRequest ActiveTs %v \n\tRollbackTs %v",
				debugPrintTs(res.GetActiveTimestamps(), reqTimestamps.GetBucket()),
				debugPrintTs(res.GetRollbackTimestamps(), reqTimestamps.GetBucket()))
		}
		return res, nil
	}
}

func (k *kvSender) sendRestartVbuckets(ap *projClient.Client,
	topic string, keyspaceId string, connErrVbs []Vbucket,
	restartTs *protobuf.TsVbuuid, sessionId uint64) (*protobuf.TopicResponse, error) {

	logging.Infof("KVSender::sendRestartVbuckets Projector %v Topic %v %v %v", ap,
		topic, keyspaceId, restartTs.GetBucket())
	logging.LazyVerbosef("KVSender::sendRestartVbuckets RestartTs %v", restartTs.Repr)

	//Shutdown the vbucket before restart if there was a ConnErr. If the vbucket is already
	//running, projector will ignore the request otherwise
	if len(connErrVbs) != 0 {

		logging.Infof("KVSender::sendRestartVbuckets ShutdownVbuckets %v Topic %v %v %v ConnErrVbs %v",
			ap, topic, keyspaceId, restartTs.GetBucket(), connErrVbs)

		// Ignoring error as this is only used to reserve the capacity for
		// shutdownTs worst case we start from 0 and append vb to the ts.
		numVbuckets, _ := k.getNumVBucketsWithLock(restartTs.GetBucket())

		// Only shutting down the Vb that receieve connection error.  It is probably not harmful
		// to shutdown every VB in the repairTS, including those that only receive StreamEnd.
		// But due to network / projecctor latency, a VB StreamBegin may be coming on the way
		// for those VB (especially when RepairStream has already retried a couple of times).
		// So shutting all VB in restartTs may unnecessarily causing race condition and
		// make the protocol longer to converge. ShutdownVbuckets should have no effect on
		// projector that does not own the Vb.
		shutdownTs := k.computeShutdownTs(restartTs, connErrVbs, numVbuckets)

		logging.Infof("KVSender::sendRestartVbuckets ShutdownVbuckets Projector %v Topic %v %v %v \n\tShutdownTs %v",
			ap, topic, keyspaceId, restartTs.GetBucket(), shutdownTs.Repr())

		if err := ap.ShutdownVbuckets(topic,
			[]*protobuf.TsVbuuid{shutdownTs}, []string{keyspaceId}); err != nil {
			logging.Errorf("KVSender::sendRestartVbuckets Unexpected Error During "+
				"ShutdownVbuckets Request for Projector %v Topic %v %v. Err %v.", ap,
				topic, keyspaceId, err)

			//all shutdownVbuckets errors are treated as success as it is a best-effort call.
			//RestartVbuckets errors will be acted upon.
		}
	}

	if res, err := ap.RestartVbuckets(topic, sessionId,
		[]*protobuf.TsVbuuid{restartTs}, []string{keyspaceId}, true); err != nil {
		logging.Errorf("KVSender::sendRestartVbuckets Unexpected Error During "+
			"Restart Vbuckets Request for Projector %v Topic %v %v %v . Err %v.", ap,
			topic, keyspaceId, restartTs.GetBucket(), err)

		return res, err
	} else {
		logging.Infof("KVSender::sendRestartVbuckets Success Projector %v Topic %v %v %v",
			ap, topic, keyspaceId, restartTs.GetBucket())
		if logging.IsEnabled(logging.Verbose) {
			logging.Verbosef("KVSender::sendRestartVbuckets \nActiveTs %v \nRollbackTs %v",
				debugPrintTs(res.GetActiveTimestamps(), restartTs.GetBucket()),
				debugPrintTs(res.GetRollbackTimestamps(), restartTs.GetBucket()))
		}
		return res, nil
	}
}

// send the actual AddInstances request on adminport
func sendAddInstancesRequest(ap *projClient.Client,
	topic string, keyspaceId string,
	instances []*protobuf.Instance) (*protobuf.TimestampResponse, error) {

	logging.Infof("KVSender::sendAddInstancesRequest Projector %v Topic %v KeyspaceId %v"+
		" \nInstances %v", ap, topic, keyspaceId, formatInstances(instances))

	if res, err := ap.AddInstances(topic, instances, keyspaceId, true); err != nil {
		logging.Errorf("KVSender::sendAddInstancesRequest Unexpected Error During "+
			"Add Instances Request Projector %v Topic %v KeyspaceId %v IndexInst %v. Err %v", ap,
			topic, keyspaceId, formatInstances(instances), err)

		return res, err
	} else {
		logging.Infof("KVSender::sendAddInstancesRequest Success Projector %v Topic %v KeyspaceId %v",
			ap, topic, keyspaceId)
		logging.LazyDebug(func() string {
			return fmt.Sprintf(
				"KVSender::sendAddInstancesRequest \n\tActiveTs %v ", debugPrintTs(res.GetCurrentTimestamps(), ""))
		})
		return res, nil

	}

}

// send the actual DelInstances request on adminport
func sendDelInstancesRequest(ap *projClient.Client,
	topic string, keyspaceId string,
	uuids []uint64) error {

	logging.Infof("KVSender::sendDelInstancesRequest Projector %v Topic %v "+
		"KeyspaceId %v Instances %v", ap, topic, keyspaceId, uuids)

	if err := ap.DelInstances(topic, uuids, keyspaceId); err != nil {
		logging.Errorf("KVSender::sendDelInstancesRequest Unexpected Error During "+
			"Del Instances Request Projector %v Topic %v KeyspaceId %v Instances %v."+
			" Err %v", ap, topic, keyspaceId, uuids, err)

		return err
	} else {
		logging.Infof("KVSender::sendDelInstancesRequest Success Projector %v Topic %v "+
			"KeyspaceId %v", ap, topic, keyspaceId)
		return nil

	}

}

// send the actual DelBuckets request on adminport
func sendDelKeyspacesRequest(ap *projClient.Client,
	topic string,
	keyspaceIds []string) error {

	logging.Infof("KVSender::sendDelKeyspacesRequest Projector %v Topic %v KeyspaceIds %v",
		ap, topic, keyspaceIds)

	if err := ap.DelBuckets(topic, keyspaceIds, keyspaceIds); err != nil {
		logging.Errorf("KVSender::sendDelKeyspacesRequest Unexpected Error During "+
			"Del Buckets Request Projector %v Topic %v KeyspaceIds %v. Err %v", ap,
			topic, keyspaceIds, err)

		return err
	} else {
		logging.Infof("KVSender::sendDelKeyspacesRequest Success Projector %v Topic %v KeyspaceIds %v",
			ap, topic, keyspaceIds)
		return nil
	}
}

// send the actual ShutdownStreamRequest on adminport
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

func (k *kvSender) computeShutdownTs(restartTs *protobuf.TsVbuuid, connErrVbs []Vbucket, numVbuckets int) *protobuf.TsVbuuid {

	shutdownTs := protobuf.NewTsVbuuid(*restartTs.Pool, *restartTs.Bucket, numVbuckets)
	for _, vbno1 := range connErrVbs {
		for i, vbno2 := range restartTs.Vbnos {
			// connErrVbs is a subset of Vb in restartTs.
			if uint32(vbno1) == vbno2 {
				shutdownTs.Append(uint16(vbno1), restartTs.Seqnos[i], restartTs.Vbuuids[i],
					*restartTs.Snapshots[i].Start, *restartTs.Snapshots[i].End,
					restartTs.ManifestUIDs[i])
			}
		}
	}

	return shutdownTs
}

func (k *kvSender) makeRestartTsForVbs(bucket string, collectionId string,
	tsVbuuid *c.TsVbuuid, vbnos []uint32) (*protobuf.TsVbuuid, error) {

	var err error

	var ts *protobuf.TsVbuuid
	if tsVbuuid == nil {
		ts, err = k.makeInitialTs(bucket, collectionId, vbnos)
	} else {
		ts, err = makeRestartTsFromTsVbuuid(bucket, collectionId, tsVbuuid, vbnos)
	}
	if err != nil {
		return nil, err
	}

	return ts, nil
}

func updateActiveTsFromResponse(keyspaceId string,
	activeTs *protobuf.TsVbuuid, res *protobuf.TopicResponse) *protobuf.TsVbuuid {

	activeTsList := res.GetActiveTimestamps()
	respKeyspaceIds := res.GetKeyspaceIds()

	var respKeyspaceId string
	for i, ts := range activeTsList {
		if respKeyspaceIds != nil {
			respKeyspaceId = respKeyspaceIds[i]
		} else {
			//backward compatible
			respKeyspaceId = ts.GetBucket()
		}
		if ts != nil && !ts.IsEmpty() && respKeyspaceId == keyspaceId {
			if activeTs == nil {
				activeTs = ts.Clone()
			} else {
				activeTs = activeTs.Union(ts)
			}
		}
	}
	return activeTs

}

func updateRollbackTsFromResponse(keyspaceId string,
	rollbackTs *protobuf.TsVbuuid, res *protobuf.TopicResponse) *protobuf.TsVbuuid {

	rollbackTsList := res.GetRollbackTimestamps()
	respKeyspaceIds := res.GetKeyspaceIds()

	var respKeyspaceId string
	for i, ts := range rollbackTsList {
		if respKeyspaceIds != nil {
			respKeyspaceId = respKeyspaceIds[i]
		} else {
			//backward compatible
			respKeyspaceId = ts.GetBucket()
		}
		if ts != nil && !ts.IsEmpty() && respKeyspaceId == keyspaceId {
			if rollbackTs == nil {
				rollbackTs = ts.Clone()
			} else {
				rollbackTs = rollbackTs.Union(ts)
			}
		}
	}

	return rollbackTs

}

func updatePendingTsFromResponse(keyspaceId string,
	pendingTs *protobuf.TsVbuuid, res *protobuf.TopicResponse) *protobuf.TsVbuuid {

	pendingTsList := res.GetPendingTimestamps()
	respKeyspaceIds := res.GetKeyspaceIds()

	var respKeyspaceId string
	for i, ts := range pendingTsList {
		if respKeyspaceIds != nil {
			respKeyspaceId = respKeyspaceIds[i]
		} else {
			//backward compatible
			respKeyspaceId = ts.GetBucket()
		}
		if ts != nil && !ts.IsEmpty() && respKeyspaceId == keyspaceId {
			if pendingTs == nil {
				pendingTs = ts.Clone()
			} else {
				pendingTs = pendingTs.Union(ts)
			}
		}
	}

	return pendingTs
}

func updateCurrentTsFromResponse(keyspaceId string,
	currentTs *protobuf.TsVbuuid, res *protobuf.TimestampResponse) *protobuf.TsVbuuid {

	currentTsList := res.GetCurrentTimestamps()
	respKeyspaceIds := res.GetKeyspaceIds()

	var respKeyspaceId string
	for i, ts := range currentTsList {
		if respKeyspaceIds != nil {
			respKeyspaceId = respKeyspaceIds[i]
		} else {
			//backward compatible
			respKeyspaceId = ts.GetBucket()
		}
		if ts != nil && !ts.IsEmpty() && respKeyspaceId == keyspaceId {
			if currentTs == nil {
				currentTs = ts.Clone()
			} else {
				currentTs = currentTs.Union(ts)
			}
		}
	}
	return currentTs

}

func (k *kvSender) makeInitialTs(bucket string, collectionId string,
	vbnos []uint32) (*protobuf.TsVbuuid, error) {

	ts := protobuf.NewTsVbuuid(DEFAULT_POOL, bucket, len(vbnos))

	for vbno := range vbnos {
		ts.Append(uint16(vbno), 0, 0, 0, 0, collections.MANIFEST_UID_EPOCH)
	}

	if collectionId != "" {
		ts.CollectionIDs = []string{collectionId}
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

func makeRestartTsFromTsVbuuid(bucket string, collectionId string,
	tsVbuuid *c.TsVbuuid, vbnos []uint32) (*protobuf.TsVbuuid, error) {

	ts := protobuf.NewTsVbuuid(DEFAULT_POOL, bucket, len(vbnos))
	for _, vbno := range vbnos {
		ts.Append(uint16(vbno), tsVbuuid.Seqnos[vbno],
			tsVbuuid.Vbuuids[vbno], tsVbuuid.Snapshots[vbno][0],
			tsVbuuid.Snapshots[vbno][1], tsVbuuid.ManifestUIDs[vbno])
	}
	if collectionId != "" {
		ts.CollectionIDs = []string{collectionId}
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
		k.FetchCInfoWithLock()
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

func (k *kvSender) getAllVbucketsInCluster(bucket string) ([]uint32, []string, int, error) {

	k.cinfoProviderLock.RLock()
	binfo, err := k.cinfoProvider.GetBucketInfoProvider(bucket)
	k.cinfoProviderLock.RUnlock()
	if err != nil {
		return nil, nil, 0, err
	}

	err = binfo.FetchBucketInfo(bucket)
	if err != nil {
		return nil, nil, 0, err
	}

	binfo.RLock()
	defer binfo.RUnlock()

	// get NumVBuckets for this bucket
	numVBuckets, err := binfo.GetNumVBuckets(bucket)
	if err != nil {
		return nil, nil, 0, err
	}

	//get all kv nodes
	nodes, err := binfo.GetNodesByBucket(bucket)
	if err != nil {
		return nil, nil, 0, err
	}

	var vbs []uint32
	var addrList []string

	for _, nid := range nodes {
		//get the list of vbnos for this kv
		if vbnos, err := binfo.GetVBuckets(nid, bucket); err != nil {
			return nil, nil, 0, err
		} else {
			vbs = append(vbs, vbnos...)
			addr, err := binfo.GetServiceAddress(nid, "projector", true)
			if err != nil {
				return nil, nil, 0, err
			}
			addrList = append(addrList, addr)

		}
	}

	return vbs, addrList, numVBuckets, nil
}

func (k *kvSender) getAllProjectorAddrs() ([]string, error) {

	k.cinfoProviderLock.RLock()
	ninfo, err := k.cinfoProvider.GetNodesInfoProvider()
	k.cinfoProviderLock.RUnlock()
	if err != nil {
		return nil, err
	}

	ninfo.RLock()
	defer ninfo.RUnlock()

	nodes := ninfo.GetNodeIdsByServiceType("projector")

	var addrList []string
	for _, nid := range nodes {
		addr, err := ninfo.GetServiceAddress(nid, "projector", true)
		if err != nil {
			return nil, err
		}
		addrList = append(addrList, addr)
	}
	return addrList, nil

}

func (k *kvSender) getProjAddrsForVbuckets(bucket string, vbnos []Vbucket) ([]string, int, error) {

	k.cinfoProviderLock.RLock()
	binfo, err := k.cinfoProvider.GetBucketInfoProvider(bucket)
	k.cinfoProviderLock.RUnlock()
	if err != nil {
		return nil, 0, err
	}

	err = binfo.FetchBucketInfo(bucket)
	if err != nil {
		return nil, 0, err
	}

	binfo.RLock()
	defer binfo.RUnlock()

	var addrList []string

	numVBuckets, err := binfo.GetNumVBuckets(bucket)
	if err != nil {
		return nil, 0, err
	}

	nodes, err := binfo.GetNodesByBucket(bucket)
	if err != nil {
		return nil, 0, err
	}

	for _, n := range nodes {
		vbs, err := binfo.GetVBuckets(n, bucket)
		if err != nil {
			return nil, 0, err
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
			addr, err := binfo.GetServiceAddress(n, "projector", true)
			if err != nil {
				return nil, 0, err
			}
			addrList = append(addrList, addr)
		}
	}
	return addrList, numVBuckets, nil
}

func (k *kvSender) handleConfigUpdate(cmd Message) {
	cfgUpdate := cmd.(*MsgConfigUpdate)

	newConfig := cfgUpdate.GetConfig()

	// Disabled due to MB-51636

	// oldConfig := k.config
	// newUseCInfoLite := newConfig["use_cinfo_lite"].Bool()
	// oldUseCInfoLite := oldConfig["use_cinfo_lite"].Bool()

	// if oldUseCInfoLite != newUseCInfoLite {
	// 	logging.Infof("KVSender::handleConfigUpdate Updating ClusterInfoProvider in kvsender")

	// 	cip, err := common.NewClusterInfoProvider(newUseCInfoLite,
	// 		newConfig["clusterAddr"].String(), DEFAULT_POOL, "kvsender", newConfig)
	// 	if err != nil {
	// 		logging.Errorf("KVSender::handleConfigUpdate Unable to update ClusterInfoProvider in kvsender err: %v, use_cinfo_lite: old %v new %v",
	// 			err, oldUseCInfoLite, newUseCInfoLite)
	// 		common.CrashOnError(err)
	// 	}
	// 	cip.SetMaxRetries(MAX_CLUSTER_FETCH_RETRY)

	// 	k.cinfoProviderLock.Lock()
	// 	oldProvider := k.cinfoProvider
	// 	k.cinfoProvider = cip
	// 	k.cinfoProviderLock.Unlock()

	// 	if oldProvider != nil {
	// 		oldProvider.Close()
	// 	}

	// 	logging.Infof("KVSender::handleConfigUpdate Updated ClusterInfoProvider in Indexer use_cinfo_lite: old %v new %v",
	// 		oldUseCInfoLite, newUseCInfoLite)
	// }

	k.config = newConfig

	k.supvCmdch <- &MsgSuccess{}
}

// convert IndexInst to protobuf format
func convertIndexListToProto(cfg c.Config, cip c.ClusterInfoProvider, indexList []c.IndexInst,
	streamId c.StreamId) []*protobuf.Instance {

	protoList := make([]*protobuf.Instance, 0)
	for _, index := range indexList {
		protoInst := convertIndexInstToProtoInst(cfg, cip, index, streamId)
		protoList = append(protoList, protoInst)
	}

	for _, index := range indexList {
		if c.IsPartitioned(index.Defn.PartitionScheme) && index.RealInstId != 0 {
			for _, protoInst := range protoList {
				if protoInst.IndexInstance.GetInstId() == uint64(index.RealInstId) {
					addPartnInfoToProtoInst(cfg, cip, index, streamId, protoInst.IndexInstance)
				}
			}
		}
	}

	return protoList

}

// convert IndexInst to protobuf format
func convertIndexInstToProtoInst(cfg c.Config, cip c.ClusterInfoProvider,
	indexInst c.IndexInst, streamId c.StreamId) *protobuf.Instance {

	protoDefn := convertIndexDefnToProtobuf(indexInst.Defn)
	protoInst := convertIndexInstToProtobuf(cfg, indexInst, protoDefn)

	addPartnInfoToProtoInst(cfg, cip, indexInst, streamId, protoInst)

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

	secExprs, _, _ := common.GetUnexplodedExprs(indexDefn.SecExprs, nil)
	defn := &protobuf.IndexDefn{
		DefnID:                 proto.Uint64(uint64(indexDefn.DefnId)),
		Bucket:                 proto.String(indexDefn.Bucket),
		IsPrimary:              proto.Bool(indexDefn.IsPrimary),
		Name:                   proto.String(indexDefn.Name),
		Using:                  using,
		ExprType:               exprType,
		SecExpressions:         secExprs,
		PartitionScheme:        partnScheme,
		PartnExpressions:       indexDefn.PartitionKeys,
		HashScheme:             protobuf.HashScheme(indexDefn.HashScheme).Enum(),
		WhereExpression:        proto.String(indexDefn.WhereExpr),
		RetainDeletedXATTR:     proto.Bool(indexDefn.RetainDeletedXATTR),
		Scope:                  proto.String(indexDefn.Scope),
		ScopeID:                proto.String(indexDefn.ScopeId),
		Collection:             proto.String(indexDefn.Collection),
		CollectionID:           proto.String(indexDefn.CollectionId),
		IndexMissingLeadingKey: proto.Bool(indexDefn.IndexMissingLeadingKey),
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

func addPartnInfoToProtoInst(cfg c.Config, cip c.ClusterInfoProvider,
	indexInst c.IndexInst, streamId c.StreamId, protoInst *protobuf.IndexInst) {

	switch partn := indexInst.Pc.(type) {
	case *c.KeyPartitionContainer:

		//Right now the fill the SinglePartition as that is the only
		//partition structure supported
		partnDefn := partn.GetAllPartitions()

		//TODO move this to indexer init. These addresses cannot change.
		//Better to get these once and store.
		ninfo, err := cip.GetNodesInfoProvider()
		c.CrashOnError(err)

		ninfo.RLock()
		defer ninfo.RUnlock()

		host, err := ninfo.GetLocalHostname()
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

			if protoInst.KeyPartn == nil || len(protoInst.KeyPartn.Partitions) == 0 {
				protoInst.KeyPartn = protobuf.NewKeyPartition(uint64(indexInst.Pc.GetNumPartitions()), endpoints, partIds)
			} else {
				protoInst.KeyPartn.AddPartitions(partIds)
			}
		}
	}
}

// create client for node's projectors
func newProjClient(addr string) (*projClient.Client, error) {

	config := c.SystemConfig.SectionConfig("indexer.projectorclient.", true)
	config.SetValue("retryInterval", 0) //no retry
	return projClient.NewClient(addr, config)

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

// check if any vb in vbList is part of the given ts
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
		instanceStr += fmt.Sprintf(" definition:<defnID:%v bucket:%v scope:%v "+
			"collection:%v isPrimary:%v name:%v using:%v "+
			"exprType:%v secExpressions:%v partitionScheme:%v whereExpression:%v > ",
			defn.GetDefnID(), defn.GetBucket(), defn.GetScope(), defn.GetCollection(),
			defn.GetIsPrimary(), defn.GetName(), defn.GetUsing(), defn.GetExprType(),
			logging.TagUD(defn.GetSecExpressions()), defn.GetPartitionScheme(),
			logging.TagUD(defn.GetWhereExpression()))
		instanceStr += fmt.Sprintf("partn:%v", inst.GetPartitionObject())
		instanceStr += "> "
	}
	instanceStr += "]"
	return instanceStr
}
