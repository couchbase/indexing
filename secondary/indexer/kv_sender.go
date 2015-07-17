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
	"time"
)

const (
	HTTP_PREFIX             string = "http://"
	MAX_KV_REQUEST_RETRY    int    = 1
	BACKOFF_FACTOR          int    = 2
	MAX_CLUSTER_FETCH_RETRY int    = 600
)

//KVSender provides the mechanism to talk to KV(projector, router etc)
type KVSender interface {
}

type kvSender struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any message to supervisor

	cInfoCache *c.ClusterInfoCache
	config     c.Config
}

func NewKVSender(supvCmdch MsgChannel, supvRespch MsgChannel,
	config c.Config) (KVSender, Message) {

	var cinfo *c.ClusterInfoCache
	url, err := c.ClusterAuthUrl(config["clusterAddr"].String())
	if err == nil {
		cinfo, err = c.NewClusterInfoCache(url, DEFAULT_POOL)
	}
	if err != nil {
		panic("Unable to initialize cluster_info - " + err.Error())
	}
	//Init the kvSender struct
	k := &kvSender{
		supvCmdch:  supvCmdch,
		supvRespch: supvRespch,
		cInfoCache: cinfo,
		config:     config,
	}

	k.cInfoCache.SetMaxRetries(MAX_CLUSTER_FETCH_RETRY)
	k.cInfoCache.SetLogPrefix("KVSender: ")
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

	logging.Debugf("KVSender::handleOpenStream %v %v %v", streamId, bucket, cmd)

	go k.openMutationStream(streamId, indexInstList, restartTs, respCh, stopCh)

	k.supvCmdch <- &MsgSuccess{}

}

func (k *kvSender) handleAddIndexListToStream(cmd Message) {

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()
	bucket := cmd.(*MsgStreamUpdate).GetBucket()
	addIndexList := cmd.(*MsgStreamUpdate).GetIndexList()
	respCh := cmd.(*MsgStreamUpdate).GetResponseChannel()
	stopCh := cmd.(*MsgStreamUpdate).GetStopChannel()

	logging.Debugf("KVSender::handleAddIndexListToStream %v %v %v", streamId, bucket, cmd)

	go k.addIndexForExistingBucket(streamId, bucket, addIndexList, respCh, stopCh)

	k.supvCmdch <- &MsgSuccess{}
}

func (k *kvSender) handleRemoveIndexListFromStream(cmd Message) {

	logging.Debugf("KVSender::handleRemoveIndexListFromStream %v", cmd)

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

	logging.Debugf("KVSender::handleRemoveBucketFromStream %v %v %v", streamId, bucket, cmd)

	go k.deleteBucketsFromStream(streamId, []string{bucket}, respCh, stopCh)

	k.supvCmdch <- &MsgSuccess{}
}

func (k *kvSender) handleCloseStream(cmd Message) {

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()
	bucket := cmd.(*MsgStreamUpdate).GetBucket()
	respCh := cmd.(*MsgStreamUpdate).GetResponseChannel()
	stopCh := cmd.(*MsgStreamUpdate).GetStopChannel()

	logging.Debugf("KVSender::handleCloseStream %v %v %v", streamId, bucket, cmd)

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

	logging.Debugf("KVSender::handleRestartVbuckets %v %v %v", streamId, bucket, cmd)

	go k.restartVbuckets(streamId, restartTs, connErrVbs, respCh, stopCh)
	k.supvCmdch <- &MsgSuccess{}
}

func (k *kvSender) openMutationStream(streamId c.StreamId, indexInstList []c.IndexInst,
	restartTs *c.TsVbuuid, respCh MsgChannel, stopCh StopChannel) {

	if len(indexInstList) == 0 {
		logging.Warnf("KVSender::openMutationStream Empty IndexList. Nothing to do.")
		respCh <- &MsgSuccess{}
		return
	}

	protoInstList := convertIndexListToProto(k.config, k.cInfoCache, indexInstList, streamId)
	bucket := indexInstList[0].Defn.Bucket

	//use any bucket as list of vbs remain the same for all buckets
	vbnos, err := k.getAllVbucketsInCluster(bucket)
	if err != nil {
		logging.Errorf("KVSender::openMutationStream %v %v Error in fetching vbuckets info %v",
			streamId, restartTs.Bucket, err)
		respCh <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
		return
	}

	restartTsList, err := k.makeRestartTsForVbs(bucket, restartTs, vbnos)
	if err != nil {
		logging.Errorf("KVSender::openMutationStream %v %v Error making restart ts %v",
			streamId, restartTs.Bucket, err)
		respCh <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
		return
	}

	addrs, err := k.getAllProjectorAddrs()
	if err != nil {
		logging.Errorf("KVSender::openMutationStream %v %v Error Fetching Projector Addrs %v",
			streamId, restartTs.Bucket, err)
		respCh <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
		return
	}

	var rollbackTs *protobuf.TsVbuuid
	var activeTs *protobuf.TsVbuuid
	topic := getTopicForStreamId(streamId)

	fn := func(r int, err error) error {

		for _, addr := range addrs {

			execWithStopCh(func() {
				ap := newProjClient(addr)
				if res, ret := k.sendMutationTopicRequest(ap, topic, restartTsList, protoInstList); ret != nil {
					//for all errors, retry
					logging.Errorf("KVSender::openMutationStream %v %v Error Received %v from %v",
						streamId, restartTs.Bucket, ret, addr)
					err = ret
				} else {
					activeTs = updateActiveTsFromResponse(bucket, activeTs, res)
					rollbackTs = updateRollbackTsFromResponse(bucket, rollbackTs, res)
				}
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
			retry := false
			if activeTs == nil || activeTs.Len() != len(vbnos) {
				retry = true
			}

			if retry {
				return errors.New("ErrPartialVbStart")
			} else {
				return nil
			}

		}
	}

	rh := c.NewRetryHelper(MAX_KV_REQUEST_RETRY, time.Second, BACKOFF_FACTOR, fn)
	err = rh.Run()

	if rollbackTs != nil {
		logging.Infof("KVSender::openMutationStream %v %v Rollback Received %v",
			streamId, restartTs.Bucket, rollbackTs)
		//convert from protobuf to native format
		numVbuckets := k.config["numVbuckets"].Int()
		nativeTs := rollbackTs.ToTsVbuuid(numVbuckets)
		respCh <- &MsgRollback{streamId: streamId,
			bucket:     bucket,
			rollbackTs: nativeTs}
	} else if err != nil {
		logging.Errorf("KVSender::openMutationStream %v %v Error Received %v",
			streamId, restartTs.Bucket, err)
		respCh <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
	} else {
		numVbuckets := k.config["numVbuckets"].Int()
		respCh <- &MsgSuccessOpenStream{activeTs: activeTs.ToTsVbuuid(numVbuckets)}
	}
}

func (k *kvSender) restartVbuckets(streamId c.StreamId, restartTs *c.TsVbuuid,
	connErrVbs []Vbucket, respCh MsgChannel, stopCh StopChannel) {

	addrs, err := k.getProjAddrsForVbuckets(restartTs.Bucket, restartTs.GetVbnos())
	if err != nil {
		logging.Errorf("KVSender::restartVbuckets %v %v Error in fetching cluster info %v",
			streamId, restartTs.Bucket, err)
		respCh <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}

		return
	}

	//convert TS to protobuf format
	var protoRestartTs *protobuf.TsVbuuid
	numVbuckets := k.config["numVbuckets"].Int()
	protoTs := protobuf.NewTsVbuuid(DEFAULT_POOL, restartTs.Bucket, numVbuckets)
	protoRestartTs = protoTs.FromTsVbuuid(restartTs)

	var rollbackTs *protobuf.TsVbuuid
	topic := getTopicForStreamId(streamId)
	rollback := false

	fn := func(r int, err error) error {

		for _, addr := range addrs {
			ap := newProjClient(addr)

			if res, ret := k.sendRestartVbuckets(ap, topic, connErrVbs, protoRestartTs); ret != nil {
				//retry for all errors
				logging.Errorf("KVSender::restartVbuckets %v %v Error Received %v from %v",
					streamId, restartTs.Bucket, ret, addr)
				err = ret
			} else {
				rollbackTs = updateRollbackTsFromResponse(restartTs.Bucket, rollbackTs, res)
			}
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

	//if any of the requested vb is in rollback ts, send rollback
	//msg to caller
	if rollback {
		//convert from protobuf to native format
		nativeTs := rollbackTs.ToTsVbuuid(numVbuckets)

		respCh <- &MsgRollback{streamId: streamId,
			rollbackTs: nativeTs}
	} else if err != nil {
		//if there is a topicMissing/genServer.Closed error, a fresh
		//MutationTopicRequest is required.
		if err.Error() == projClient.ErrorTopicMissing.Error() ||
			err.Error() == c.ErrorClosed.Error() ||
			err.Error() == projClient.ErrorInvalidBucket.Error() {
			respCh <- &MsgKVStreamRepair{
				streamId: streamId,
				bucket:   restartTs.Bucket,
			}
		} else {
			respCh <- &MsgError{
				err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
					severity: FATAL,
					cause:    err}}

		}
	} else {
		respCh <- &MsgSuccess{}
	}
}

func (k *kvSender) addIndexForExistingBucket(streamId c.StreamId, bucket string, indexInstList []c.IndexInst,
	respCh MsgChannel, stopCh StopChannel) {

	addrs, err := k.getAllProjectorAddrs()
	if err != nil {
		logging.Errorf("KVSender::addIndexForExistingBucket %v %v Error in fetching cluster info %v",
			streamId, bucket, err)
		respCh <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
		return
	}

	var currentTs *protobuf.TsVbuuid
	protoInstList := convertIndexListToProto(k.config, k.cInfoCache, indexInstList, streamId)
	topic := getTopicForStreamId(streamId)

	fn := func(r int, err error) error {

		for _, addr := range addrs {
			execWithStopCh(func() {
				ap := newProjClient(addr)
				if res, ret := sendAddInstancesRequest(ap, topic, protoInstList); ret != nil {
					logging.Errorf("KVSender::addIndexForExistingBucket %v %v Error Received %v from %v",
						streamId, bucket, ret, addr)
					err = ret
				} else {
					currentTs = updateCurrentTsFromResponse(bucket, currentTs, res)
				}
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
		logging.Errorf("KVSender::addIndexForExistingBucket %v %v Error Received %v",
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

		for _, addr := range addrs {
			execWithStopCh(func() {
				ap := newProjClient(addr)
				if ret := sendDelInstancesRequest(ap, topic, uuids); ret != nil {
					logging.Errorf("KVSender::deleteIndexesFromStream %v %v Error Received %v from %v",
						streamId, indexInstList[0].Defn.Bucket, ret, addr)
					//Treat TopicMissing/GenServer.Closed as success
					if ret.Error() == projClient.ErrorTopicMissing.Error() ||
						ret.Error() == c.ErrorClosed.Error() {
						logging.Infof("KVSender::deleteIndexesFromStream %v %v Treating TopicMissing As Success",
							streamId, indexInstList[0].Defn.Bucket)
					} else {
						err = ret
					}
				}
			}, stopCh)
		}
		return err
	}

	rh := c.NewRetryHelper(MAX_KV_REQUEST_RETRY, time.Second, BACKOFF_FACTOR, fn)
	err = rh.Run()
	if err != nil {
		logging.Errorf("KVSender::deleteIndexesFromStream %v %v Error Received %v",
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

		for _, addr := range addrs {
			execWithStopCh(func() {
				ap := newProjClient(addr)
				if ret := sendDelBucketsRequest(ap, topic, buckets); ret != nil {
					logging.Errorf("KVSender::deleteBucketsFromStream %v %v Error Received %v from %v",
						streamId, buckets[0], ret, addr)
					//Treat TopicMissing/GenServer.Closed as success
					if ret.Error() == projClient.ErrorTopicMissing.Error() ||
						ret.Error() == c.ErrorClosed.Error() {
						logging.Infof("KVSender::deleteBucketsFromStream %v %v Treating TopicMissing As Success",
							streamId, buckets[0])
					} else {
						err = ret
					}
				}
			}, stopCh)
		}
		return err
	}

	rh := c.NewRetryHelper(MAX_KV_REQUEST_RETRY, time.Second, BACKOFF_FACTOR, fn)
	err = rh.Run()
	if err != nil {
		logging.Errorf("KVSender::deleteBucketsFromStream %v %v Error Received %v",
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

		for _, addr := range addrs {
			execWithStopCh(func() {
				ap := newProjClient(addr)
				if ret := sendShutdownTopic(ap, topic); ret != nil {
					logging.Errorf("KVSender::closeMutationStream %v %v Error Received %v from %v",
						streamId, bucket, ret, addr)
					//Treat TopicMissing/GenServer.Closed as success
					if ret.Error() == projClient.ErrorTopicMissing.Error() ||
						ret.Error() == c.ErrorClosed.Error() {
						logging.Infof("KVSender::closeMutationStream %v %v Treating TopicMissing As Success",
							streamId, bucket)
					} else {
						err = ret
					}
				}
			}, stopCh)
		}
		return err
	}

	rh := c.NewRetryHelper(MAX_KV_REQUEST_RETRY, time.Second, BACKOFF_FACTOR, fn)
	err = rh.Run()
	if err != nil {
		logging.Errorf("KVSender::closeMutationStream %v %v Error Received %v", streamId, bucket, err)
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
	reqTimestamps *protobuf.TsVbuuid,
	instances []*protobuf.Instance) (*protobuf.TopicResponse, error) {

	logging.Infof("KVSender::sendMutationTopicRequest Projector %v Topic %v %v \n\tInstances %v",
		ap, topic, reqTimestamps.GetBucket(), instances)

	logging.LazyDebugf("KVSender::sendMutationTopicRequest RequestTS %v", reqTimestamps.Repr)

	endpointType := "dataport"

	if res, err := ap.MutationTopicRequest(topic, endpointType,
		[]*protobuf.TsVbuuid{reqTimestamps}, instances); err != nil {
		logging.Fatalf("KVSender::sendMutationTopicRequest Projector %v Topic %v %v \n\tUnexpected Error %v", ap,
			topic, reqTimestamps.GetBucket(), err)

		return res, err
	} else {
		if logging.IsEnabled(logging.Debug) {
			logging.Debugf("KVSender::sendMutationTopicRequest Response Projector %v Topic %v %v "+
				"\n\tInstanceIds %v \n\tActiveTs %v \n\tRollbackTs %v", ap, topic, reqTimestamps.GetBucket(),
				res.GetInstanceIds(), debugPrintTs(res.GetActiveTimestamps()), debugPrintTs(res.GetRollbackTimestamps()))
		}
		return res, nil
	}
}

func (k *kvSender) sendRestartVbuckets(ap *projClient.Client,
	topic string, connErrVbs []Vbucket,
	restartTs *protobuf.TsVbuuid) (*protobuf.TopicResponse, error) {

	logging.Infof("KVSender::sendRestartVbuckets Projector %v Topic %v %v", ap, topic, restartTs.GetBucket())
	logging.LazyDebugf("KVSender::sendRestartVbuckets RestartTs %v", restartTs.Repr)

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

	if res, err := ap.RestartVbuckets(topic, []*protobuf.TsVbuuid{restartTs}); err != nil {
		logging.Fatalf("KVSender::sendRestartVbuckets Unexpected Error During "+
			"Restart Vbuckets Request for Projector %v Topic %v %v . Err %v.", ap,
			topic, restartTs.GetBucket(), err)

		return res, err
	} else {
		if logging.IsEnabled(logging.Debug) {
			logging.Debugf("KVSender::sendRestartVbuckets Response Projector %v Topic %v %v "+
				"\nInstanceIds %v \nActiveTs %v \nRollbackTs %v", ap, topic, restartTs.GetBucket(),
				res.GetInstanceIds(), debugPrintTs(res.GetActiveTimestamps()), debugPrintTs(res.GetRollbackTimestamps()))
		}
		return res, nil
	}
}

//send the actual AddInstances request on adminport
func sendAddInstancesRequest(ap *projClient.Client,
	topic string,
	instances []*protobuf.Instance) (*protobuf.TimestampResponse, error) {

	logging.Infof("KVSender::sendAddInstancesRequest Projector %v Topic %v \nInstances %v",
		ap, topic, instances)

	if res, err := ap.AddInstances(topic, instances); err != nil {
		logging.Fatalf("KVSender::sendAddInstancesRequest Unexpected Error During "+
			"Add Instances Request Projector %v Topic %v IndexInst %v. Err %v", ap,
			topic, instances, err)

		return res, err
	} else {
		logging.LazyDebug(func() string {
			return fmt.Sprintf(
				"KVSender::sendAddInstancesRequest Response Projector %v Topic %v "+
					"\n\tActiveTs %v ", ap, topic, debugPrintTs(res.GetCurrentTimestamps()))
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
		logging.Fatalf("KVSender::sendDelInstancesRequest Unexpected Error During "+
			"Del Instances Request Projector %v Topic %v Instances %v. Err %v", ap,
			topic, uuids, err)

		return err
	} else {
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
		logging.Fatalf("KVSender::sendDelBucketsRequest Unexpected Error During "+
			"Del Buckets Request Projector %v Topic %v Buckets %v. Err %v", ap,
			topic, buckets, err)

		return err
	} else {
		return nil
	}
}

//send the actual ShutdownStreamRequest on adminport
func sendShutdownTopic(ap *projClient.Client,
	topic string) error {

	logging.Infof("KVSender::sendShutdownTopic Projector %v Topic %v", ap, topic)

	if err := ap.ShutdownTopic(topic); err != nil {
		logging.Fatalf("KVSender::sendShutdownTopic Unexpected Error During "+
			"Shutdown Projector %v Topic %v. Err %v", ap, topic, err)

		return err
	} else {
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

	flogs, err := k.getFailoverLogs(bucket, vbnos)
	if err != nil {
		logging.Fatalf("KVSender::makeInitialTs Unexpected Error During Failover "+
			"Log Request for Bucket %v. Err %v", bucket, err)
		return nil, err
	}

	ts := protobuf.NewTsVbuuid(DEFAULT_POOL, bucket, len(vbnos))
	ts = ts.InitialRestartTs(flogs.ToFailoverLog(c.Vbno32to16(vbnos)))

	return ts, nil
}

func (k *kvSender) makeRestartTsFromKV(bucket string,
	vbnos []uint32) (*protobuf.TsVbuuid, error) {

	flogs, err := k.getFailoverLogs(bucket, vbnos)
	if err != nil {
		logging.Fatalf("KVSender::makeRestartTS Unexpected Error During Failover "+
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
	var res *protobuf.FailoverLogResponse

	addrs, err := k.getAllProjectorAddrs()
	if err != nil {
		return nil, err
	}

loop:
	for _, addr := range addrs {
		//create client for node's projectors
		client := newProjClient(addr)
		if res, err = client.GetFailoverLogs(DEFAULT_POOL, bucket, vbnos); err == nil {
			break loop
		}
	}

	if logging.IsEnabled(logging.Debug) {
		s := ""
		for _, l := range res.GetLogs() {
			s += fmt.Sprintf("\t%v\n", l)
		}
		logging.Debugf("KVSender::getFailoverLogs Failover Log Response Error %v \n%v", err, s)
	}

	return res, err
}

func (k *kvSender) getAllVbucketsInCluster(bucket string) ([]uint32, error) {

	k.cInfoCache.Lock()
	defer k.cInfoCache.Unlock()

	err := k.cInfoCache.Fetch()
	if err != nil {
		return nil, err
	}

	//get all kv nodes
	nodes, err := k.cInfoCache.GetNodesByBucket(bucket)
	if err != nil {
		return nil, err
	}

	var vbs []uint32
	for _, nid := range nodes {
		//get the list of vbnos for this kv
		if vbnos, err := k.cInfoCache.GetVBuckets(nid, bucket); err != nil {
			return nil, err
		} else {
			vbs = append(vbs, vbnos...)
		}
	}
	return vbs, nil
}

func (k *kvSender) getAllProjectorAddrs() ([]string, error) {

	k.cInfoCache.Lock()
	defer k.cInfoCache.Unlock()

	err := k.cInfoCache.Fetch()
	if err != nil {
		return nil, err
	}

	nodes := k.cInfoCache.GetNodesByServiceType("projector")

	var addrList []string
	for _, nid := range nodes {
		addr, err := k.cInfoCache.GetServiceAddress(nid, "projector")
		if err != nil {
			return nil, err
		}
		addrList = append(addrList, addr)
	}

	return addrList, nil
}

func (k *kvSender) getProjAddrsForVbuckets(bucket string, vbnos []uint16) ([]string, error) {

	k.cInfoCache.Lock()
	defer k.cInfoCache.Unlock()

	err := k.cInfoCache.Fetch()
	if err != nil {
		return nil, err
	}

	var addrList []string

	nodes := k.cInfoCache.GetNodesByServiceType("projector")

	for _, n := range nodes {
		vbs, err := k.cInfoCache.GetVBuckets(n, bucket)
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
			addr, err := k.cInfoCache.GetServiceAddress(n, "projector")
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
func convertIndexListToProto(cfg c.Config, cinfo *c.ClusterInfoCache, indexList []c.IndexInst,
	streamId c.StreamId) []*protobuf.Instance {

	protoList := make([]*protobuf.Instance, 0)
	for _, index := range indexList {
		protoInst := convertIndexInstToProtoInst(cfg, cinfo, index, streamId)
		protoList = append(protoList, protoInst)
	}

	return protoList

}

// convert IndexInst to protobuf format
func convertIndexInstToProtoInst(cfg c.Config, cinfo *c.ClusterInfoCache,
	indexInst c.IndexInst, streamId c.StreamId) *protobuf.Instance {

	protoDefn := convertIndexDefnToProtobuf(indexInst.Defn)
	protoInst := convertIndexInstToProtobuf(cfg, indexInst, protoDefn)

	addPartnInfoToProtoInst(cfg, cinfo, indexInst, streamId, protoInst)

	return &protobuf.Instance{IndexInstance: protoInst}
}

func convertIndexDefnToProtobuf(indexDefn c.IndexDefn) *protobuf.IndexDefn {

	using := protobuf.StorageType(
		protobuf.StorageType_value[string(indexDefn.Using)]).Enum()
	exprType := protobuf.ExprType(
		protobuf.ExprType_value[string(indexDefn.ExprType)]).Enum()
	partnScheme := protobuf.PartitionScheme(
		protobuf.PartitionScheme_value[string(indexDefn.PartitionScheme)]).Enum()

	defn := &protobuf.IndexDefn{
		DefnID:          proto.Uint64(uint64(indexDefn.DefnId)),
		Bucket:          proto.String(indexDefn.Bucket),
		IsPrimary:       proto.Bool(indexDefn.IsPrimary),
		Name:            proto.String(indexDefn.Name),
		Using:           using,
		ExprType:        exprType,
		SecExpressions:  indexDefn.SecExprs,
		PartitionScheme: partnScheme,
		PartnExpression: proto.String(indexDefn.PartitionKey),
		WhereExpression: proto.String(indexDefn.WhereExpr),
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

func addPartnInfoToProtoInst(cfg c.Config, cinfo *c.ClusterInfoCache,
	indexInst c.IndexInst, streamId c.StreamId, protoInst *protobuf.IndexInst) {

	switch partn := indexInst.Pc.(type) {
	case *c.KeyPartitionContainer:

		//Right now the fill the SinglePartition as that is the only
		//partition structure supported
		partnDefn := partn.GetAllPartitions()

		//TODO move this to indexer init. These addresses cannot change.
		//Better to get these once and store.
		cinfo.Lock()
		defer cinfo.Unlock()

		err := cinfo.Fetch()
		c.CrashOnError(err)

		nid := cinfo.GetCurrentNode()
		streamMaintAddr, err := cinfo.GetServiceAddress(nid, "indexStreamMaint")
		c.CrashOnError(err)
		streamInitAddr, err := cinfo.GetServiceAddress(nid, "indexStreamInit")
		c.CrashOnError(err)
		streamCatchupAddr, err := cinfo.GetServiceAddress(nid, "indexStreamCatchup")
		c.CrashOnError(err)

		var endpoints []string
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
				endpoints = append(endpoints, string(e))
			}
		}
		protoInst.SinglePartn = &protobuf.SinglePartition{
			Endpoints: endpoints,
		}
	}
}

//create client for node's projectors
func newProjClient(addr string) *projClient.Client {

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

func execWithStopCh(fn func(), stopCh StopChannel) {

	select {

	case <-stopCh:
		stopCh <- true
		return

	default:
		fn()

	}

}

func debugPrintTs(tsList []*protobuf.TsVbuuid) string {

	if len(tsList) == 0 {
		return ""
	}

	ts := tsList[0]
	return ts.Repr()
}
