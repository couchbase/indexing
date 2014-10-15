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
	"github.com/couchbase/indexing/secondary/adminport"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/projector"
	"github.com/couchbase/indexing/secondary/protobuf"
	"github.com/couchbaselabs/goprotobuf/proto"
	"net"
	"strconv"
	"strings"
	"time"
)

var HTTP_PREFIX string = "http://"
var MAX_KV_REQUEST_RETRY int = 5

//KVSender provides the mechanism to talk to KV(projector, router etc)
type KVSender interface {
}

type kvSender struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any message to supervisor

	streamStatus              StreamStatusMap
	streamBucketIndexCountMap map[c.StreamId]BucketIndexCountMap

	numVbuckets uint16
	kvListCache []string
}

func NewKVSender(supvCmdch MsgChannel, supvRespch MsgChannel,
	numVbuckets uint16) (KVSender, Message) {

	//Init the kvSender struct
	k := &kvSender{
		supvCmdch:                 supvCmdch,
		supvRespch:                supvRespch,
		streamStatus:              make(StreamStatusMap),
		numVbuckets:               numVbuckets,
		streamBucketIndexCountMap: make(map[c.StreamId]BucketIndexCountMap),
		kvListCache:               make([]string, 0),
	}

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
					c.Infof("KVSender::run Shutting Down")
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

	case CLOSE_STREAM:
		k.handleCloseStream(cmd)

	case KV_SENDER_GET_CURR_KV_TS:
		k.handleGetCurrKVTimestamp(cmd)

	default:
		c.Errorf("KVSender::handleSupvervisorCommands "+
			"Received Unknown Command %v", cmd)
	}

}

func (k *kvSender) handleOpenStream(cmd Message) {

	c.Infof("KVSender::handleOpenStream %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	if status, _ := k.streamStatus[streamId]; status {
		k.supvCmdch <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_ALREADY_OPEN,
				severity: FATAL}}
		return
	}

	indexInstList := cmd.(*MsgStreamUpdate).GetIndexList()
	bucketRestartTs := cmd.(*MsgStreamUpdate).GetBucketRestartTs()

	//start mutation stream, if error return to supervisor
	resp := k.openMutationStream(streamId, indexInstList, bucketRestartTs)
	if resp.GetMsgType() != MSG_SUCCESS {
		k.supvCmdch <- resp
		return
	}

	//increment index count for this bucket
	bucketIndexCountMap := make(BucketIndexCountMap)
	for _, indexInst := range indexInstList {
		bucketIndexCountMap[indexInst.Defn.Bucket] += 1
	}
	k.streamBucketIndexCountMap[streamId] = bucketIndexCountMap

	k.streamStatus[streamId] = true
	k.supvCmdch <- &MsgSuccess{}

}

func (k *kvSender) handleAddIndexListToStream(cmd Message) {

	c.Debugf("KVSender::handleAddIndexListToStream %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	//If Stream is not yet open, return an error
	if status, _ := k.streamStatus[streamId]; !status {
		k.supvCmdch <- &MsgError{
			err: Error{code: ERROR_KV_SENDER_UNKNOWN_STREAM,
				severity: FATAL}}
		return
	}

	indexInstList := cmd.(*MsgStreamUpdate).GetIndexList()
	//For now, only one index comes in the request
	//TODO Add Batching support
	indexInst := indexInstList[0]

	//if this is the first index for this bucket, add new bucket to stream
	if c, ok := k.streamBucketIndexCountMap[streamId][indexInst.Defn.Bucket]; c == 0 || !ok {

		resp := k.addIndexForNewBucket(streamId, indexInst)
		if resp.GetMsgType() != MSG_SUCCESS {
			k.supvCmdch <- resp
			return
		}

		//increment index count for this bucket
		k.streamBucketIndexCountMap[streamId][indexInst.Defn.Bucket] = 1

	} else {
		resp := k.addIndexForExistingBucket(streamId, indexInst)
		if resp.GetMsgType() != MSG_SUCCESS {
			k.supvCmdch <- resp
			return
		}
		//increment index count for this bucket
		k.streamBucketIndexCountMap[streamId][indexInst.Defn.Bucket]++
	}

	k.supvCmdch <- &MsgSuccess{}
}

func (k *kvSender) handleRemoveIndexListFromStream(cmd Message) {

	c.Debugf("KVSender::handleRemoveIndexListFromStream %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	//if stream is not yet open, return an error
	if status, _ := k.streamStatus[streamId]; !status {
		k.supvCmdch <- &MsgError{
			err: Error{code: ERROR_KV_SENDER_UNKNOWN_STREAM,
				severity: FATAL}}
		return
	}

	indexInstList := cmd.(*MsgStreamUpdate).GetIndexList()
	//For now, only one index comes in the request
	//TODO Add Batching support
	indexInst := indexInstList[0]

	bucketIndexCountMap := k.streamBucketIndexCountMap[streamId]
	bucketIndexCountMap[indexInst.Defn.Bucket]--

	//if this is the last bucket in stream, delete bucket from map
	if bucketIndexCountMap[indexInst.Defn.Bucket] == 0 {
		delete(bucketIndexCountMap, indexInst.Defn.Bucket)
	}

	//if this is the last index in the stream, the stream needs to be closed.
	//projector cannot work with empty streams. deleting an instance
	//or bucket in this case would result in problem.
	if len(bucketIndexCountMap) == 0 {

		resp := k.closeMutationStream(streamId, indexInst.Defn.Bucket)
		if resp.GetMsgType() != MSG_SUCCESS {
			k.supvCmdch <- resp
			return
		}
		//clean internal maps
		delete(k.streamBucketIndexCountMap, streamId)
		k.streamStatus[streamId] = false
	} else {
		//if this is the last index for this bucket, the bucket needs to be deleted
		//from stream. projector cannot work with empty bucket in a stream.
		//deleting an index instance in this case would result in problem.
		if c, ok := bucketIndexCountMap[indexInst.Defn.Bucket]; c == 0 || !ok {
			resp := k.deleteBucketFromStream(streamId, indexInst.Defn.Bucket)
			if resp.GetMsgType() != MSG_SUCCESS {
				k.supvCmdch <- resp
				return
			}

		} else { //there are more indexes for this bucket in stream
			resp := k.deleteIndexFromStream(streamId, indexInst)
			if resp.GetMsgType() != MSG_SUCCESS {
				k.supvCmdch <- resp
				return

			}
		}
	}

	k.supvCmdch <- &MsgSuccess{}
}

func (k *kvSender) handleCloseStream(cmd Message) {

	c.Infof("KVSender::handleCloseStream %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()
	bucket := cmd.(*MsgStreamUpdate).GetBucket()

	//if stream is already closed, return error
	if status, _ := k.streamStatus[streamId]; !status {
		k.supvCmdch <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_ALREADY_CLOSED,
				severity: FATAL}}
		return
	}

	//if no bucket has been specified, use any bucket name
	//it doesn't matter while closing the stream
	if bucket == "" {
		bucket = k.getAnyBucketName()
	}
	resp := k.closeMutationStream(streamId, bucket)

	//clean internal maps
	delete(k.streamBucketIndexCountMap, streamId)
	k.streamStatus[streamId] = false

	//TODO handle partial failure
	k.supvCmdch <- resp
}

func (k *kvSender) handleGetCurrKVTimestamp(cmd Message) {

	//TODO For now Indexer is getting the TS directly from
	//KV. Once Projector API is ready, use that.

}

func (k *kvSender) openMutationStream(streamId c.StreamId, indexInstList []c.IndexInst,
	bucketRestartTs map[string]*c.TsVbuuid) Message {

	if len(indexInstList) == 0 {
		c.Warnf("KVSender::openMutationStream Empty IndexList. Nothing to do.")
		return &MsgSuccess{}
	}

	var protoInstList []*protobuf.Instance
	for _, indexInst := range indexInstList {
		protoInstList = append(protoInstList, convertIndexInstToProtoInst(indexInst, streamId))
	}

	//Get the Vbmap of all nodes in the cluster. As vbmap is symmetric for all buckets,
	//choose any bucket to get the map.
	vbmap, err := k.getVbmap(indexInstList[0].Defn.Bucket, nil)
	if err != nil {
		c.Errorf("KVSender::openMutationStream \n\t Error In GetVbMap %v", err)
		return &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
	}

	vbnosList := vbmap.GetKvvbnos()

	//for all the nodes in vbmap
	for i, kv := range vbmap.GetKvaddrs() {

		//get projector address from kv address
		projAddr := getProjectorAddrFromKVAddr(kv)

		//create client for node's projectors
		ap := projector.NewClient(projAddr)

		//get the list of vbnos for this kv
		vbnos := vbnosList[i].GetVbnos()

		var restartTsList []*protobuf.TsVbuuid
		var err error
		for bucket, tsVbuuid := range bucketRestartTs {
			var ts *protobuf.TsVbuuid
			if tsVbuuid == nil {
				ts, err = k.makeInitialTs(bucket, vbnos)
			} else {
				ts, err = makeRestartTsFromTsVbuuid(bucket, tsVbuuid, vbnos)
			}
			if err != nil {
				return &MsgError{
					err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
						severity: FATAL,
						cause:    err}}
			}
			restartTsList = append(restartTsList, ts)
		}

		topic := getTopicForStreamId(streamId)
		if _, errMsg := sendMutationTopicRequest(ap, topic, restartTsList, protoInstList); errMsg.GetMsgType() != MSG_SUCCESS {
			//TODO send message to all KVs to revert the previous requests sent
			return errMsg
		} else {
			//TODO check if KV has asked us to rollback. Construct rollbackTs to be sent back to Indexer.
		}

	}

	return &MsgSuccess{}
}

func (k *kvSender) addIndexForNewBucket(streamId c.StreamId, indexInst c.IndexInst) Message {

	protoInst := convertIndexInstToProtoInst(indexInst, streamId)

	//Get the Vbmap of all nodes in the cluster
	vbmap, err := k.getVbmap(indexInst.Defn.Bucket, nil)
	if err != nil {
		c.Errorf("KVSender::addIndexForNewBucket \n\t Error In GetVbMap %v", err)
		return &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
	}

	vbnosList := vbmap.GetKvvbnos()

	//for all the nodes in vbmap
	for i, kv := range vbmap.GetKvaddrs() {

		//get projector address from kv address
		projAddr := getProjectorAddrFromKVAddr(kv)

		//create client for node's projectors
		ap := projector.NewClient(projAddr)

		//get the list of vbnos for this kv
		vbnos := vbnosList[i].GetVbnos()

		ts, err := k.makeInitialTs(indexInst.Defn.Bucket, vbnos)
		if err != nil {
			return &MsgError{
				err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
					severity: FATAL,
					cause:    err}}
		}
		topic := getTopicForStreamId(streamId)
		restartTs := []*protobuf.TsVbuuid{ts}
		instances := []*protobuf.Instance{protoInst}

		if _, errMsg := sendAddBucketsRequest(ap, topic, restartTs, instances); errMsg.GetMsgType() != MSG_SUCCESS {
			//TODO send message to all KVs to revert the previous requests sent
			return errMsg
		}
	}

	return &MsgSuccess{}
}

func (k *kvSender) addIndexForExistingBucket(streamId c.StreamId, indexInst c.IndexInst) Message {

	protoInst := convertIndexInstToProtoInst(indexInst, streamId)

	//Get the Vbmap of all nodes in the cluster
	vbmap, err := k.getVbmap(indexInst.Defn.Bucket, nil)
	if err != nil {
		c.Errorf("KVSender::addIndexForExistingBucket \n\t Error In GetVbMap %v", err)
		return &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
	}

	//for all the nodes in vbmap
	for _, kv := range vbmap.GetKvaddrs() {

		//get projector address from kv address
		projAddr := getProjectorAddrFromKVAddr(kv)

		//create client for node's projectors
		ap := projector.NewClient(projAddr)

		//add new engine(index) to existing stream
		topic := getTopicForStreamId(streamId)
		instances := []*protobuf.Instance{protoInst}

		if errMsg := sendAddInstancesRequest(ap, topic, instances); errMsg.GetMsgType() != MSG_SUCCESS {
			//TODO send message to all KVs to revert the previous requests sent
			return errMsg
		}
	}

	return &MsgSuccess{}
}

func (k *kvSender) deleteIndexFromStream(streamId c.StreamId, indexInst c.IndexInst) Message {

	//Get the Vbmap of all nodes in the cluster
	vbmap, err := k.getVbmap(indexInst.Defn.Bucket, nil)
	if err != nil {
		c.Errorf("KVSender::deleteIndexFromStream \n\t Error In GetVbMap %v", err)
		return &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
	}

	//for all the nodes in vbmap
	for _, kv := range vbmap.GetKvaddrs() {

		//get projector address from kv address
		projAddr := getProjectorAddrFromKVAddr(kv)

		//create client for node's projectors
		ap := projector.NewClient(projAddr)

		//delete engine(index) from the existing stream
		topic := getTopicForStreamId(streamId)
		uuids := []uint64{uint64(indexInst.InstId)}

		if errMsg := sendDelInstancesRequest(ap, topic, uuids); errMsg.GetMsgType() != MSG_SUCCESS {
			//TODO send message to all KVs to revert the previous requests sent
			return errMsg
		}

	}

	return &MsgSuccess{}
}

func (k *kvSender) deleteBucketFromStream(streamId c.StreamId, bucket string) Message {

	//Get the Vbmap of all nodes in the cluster
	vbmap, err := k.getVbmap(bucket, nil)
	if err != nil {
		c.Errorf("KVSender::deleteBucketFromStream \n\t Error In GetVbMap %v", err)
		return &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
	}

	//for all the nodes in vbmap
	for _, kv := range vbmap.GetKvaddrs() {

		//get projector address from kv address
		projAddr := getProjectorAddrFromKVAddr(kv)

		//create client for node's projectors
		ap := projector.NewClient(projAddr)

		topic := getTopicForStreamId(streamId)
		buckets := []string{bucket}

		if errMsg := sendDelBucketsRequest(ap, topic, buckets); errMsg.GetMsgType() != MSG_SUCCESS {
			//TODO send message to all KVs to revert the previous requests sent
			return errMsg
		}

	}

	return &MsgSuccess{}
}

func (k *kvSender) closeMutationStream(streamId c.StreamId, bucket string) Message {

	//Get the Vbmap of all nodes in the cluster
	vbmap, err := k.getVbmap(bucket, nil)
	if err != nil {
		c.Errorf("KVSender::closeMutationStream \n\t Error In GetVbMap %v", err)
		return &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
	}

	//for all the nodes in vbmap
	for _, kv := range vbmap.GetKvaddrs() {

		//get projector address from kv address
		projAddr := getProjectorAddrFromKVAddr(kv)

		//create client for node's projectors
		ap := projector.NewClient(projAddr)

		topic := getTopicForStreamId(streamId)
		if errMsg := sendShutdownTopic(ap, topic); errMsg.GetMsgType() != MSG_SUCCESS {
			return errMsg
		}

	}

	return &MsgSuccess{}

}

//send the actual MutationStreamRequest on adminport
func sendMutationTopicRequest(ap *projector.Client, topic string,
	reqTimestamps []*protobuf.TsVbuuid,
	instances []*protobuf.Instance) (*protobuf.TopicResponse, Message) {

	c.Debugf("KVSender::sendMutationTopicRequest Projector %v Topic %v Instances %v",
		ap, topic, instances)

	sleepTime := 1
	retry := 0
	eps := map[string]interface{}{
		"type": "dataport",
	}

	for {
		if res, err := ap.MutationTopicRequest(topic, eps, reqTimestamps, instances); err != nil {
			if isRetryReqd(err) && retry < MAX_KV_REQUEST_RETRY {
				c.Errorf("KVSender::sendMutationTopicRequest \n\tError Connecting to Projector %v. "+
					"Retry in %v seconds...\n\t Err %v", ap, sleepTime, err)
				time.Sleep(time.Duration(sleepTime) * time.Second)
				sleepTime *= 2
				retry++
			} else {
				c.Errorf("KVSender::sendMutationTopicRequest \n\tUnexpected Error During Mutation Stream "+
					"Request %v for IndexInst %v. Err %v", topic, instances, err)

				return res, &MsgError{
					err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
						severity: FATAL,
						cause:    err}}
			}
		} else {
			c.Debugf("KVSender::sendMutationTopicRequest \n\tMutationStream Response %v", res)
			return res, &MsgSuccess{}
		}
	}
}

//send the actual UpdateMutationStreamRequest on adminport
func sendAddBucketsRequest(ap *projector.Client,
	topic string,
	restartTs []*protobuf.TsVbuuid,
	instances []*protobuf.Instance) (*protobuf.TopicResponse, Message) {

	c.Debugf("KVSender::sendAddBucketsRequest Projector %v Topic %v Instances %v",
		ap, topic, instances)

	sleepTime := 1
	retry := 0

	for {
		if res, err := ap.AddBuckets(topic, restartTs, instances); err != nil {
			if isRetryReqd(err) && retry < MAX_KV_REQUEST_RETRY {
				c.Errorf("KVSender::sendAddBucketsRequest \n\tError Connecting to Projector %v. "+
					"Retry in %v seconds... \n\t Error %v ", ap, sleepTime, err)
				time.Sleep(time.Duration(sleepTime) * time.Second)
				sleepTime *= 2
				retry++
			} else {
				c.Errorf("KVSender::sendAddBucketsRequest \n\tUnexpected Error During "+
					"Mutation Stream Request %v for IndexInst %v. Err %v.",
					topic, instances, err)

				return res, &MsgError{
					err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
						severity: FATAL,
						cause:    err}}
			}
		} else {

			c.Debugf("KVSender::sendAddBucketsRequest \n\tMutationStreamResponse %v", res)

			return res, &MsgSuccess{}
		}
	}
}

//send the actual AddInstances request on adminport
func sendAddInstancesRequest(ap *projector.Client,
	topic string,
	instances []*protobuf.Instance) Message {

	c.Debugf("KVSender::sendAddInstancesRequest Projector %v Topic %v Instances %v",
		ap, topic, instances)

	sleepTime := 1
	retry := 0

	for {
		if err := ap.AddInstances(topic, instances); err != nil {
			if isRetryReqd(err) && retry < MAX_KV_REQUEST_RETRY {
				c.Errorf("KVSender::sendAddInstancesRequest \n\tError Connecting to Projector %v. "+
					"Retry in %v seconds... \n\t Err %v", ap, sleepTime, err)
				time.Sleep(time.Duration(sleepTime) * time.Second)
				sleepTime *= 2
				retry++
			} else {
				c.Errorf("KVSender::sendAddInstancesRequest \n\tUnexpected Error During "+
					"Add Instances Request Topic %v IndexInst %v. Err %v",
					topic, instances, err)

				return &MsgError{
					err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
						severity: FATAL,
						cause:    err}}
			}
		} else {
			return &MsgSuccess{}

		}
	}

}

//send the actual DelInstances request on adminport
func sendDelInstancesRequest(ap *projector.Client,
	topic string,
	uuids []uint64) Message {

	c.Debugf("KVSender::sendDelInstancesRequest Projector %v Topic %v Instances %v",
		ap, topic, uuids)

	sleepTime := 1
	retry := 0

	for {
		if err := ap.DelInstances(topic, uuids); err != nil {
			if isRetryReqd(err) && retry < MAX_KV_REQUEST_RETRY {
				c.Errorf("KVSender::sendDelInstancesRequest \n\tError Connecting to Projector %v. "+
					"Retry in %v seconds... \n\t Err %v", ap, sleepTime, err)
				time.Sleep(time.Duration(sleepTime) * time.Second)
				sleepTime *= 2
				retry++
			} else {
				c.Errorf("KVSender::sendDelInstancesRequest \n\tUnexpected Error During "+
					"Del Instances Request Topic %v Instances %v. Err %v",
					topic, uuids, err)

				return &MsgError{
					err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
						severity: FATAL,
						cause:    err}}
			}
		} else {
			return &MsgSuccess{}

		}
	}

}

//send the actual DelBuckets request on adminport
func sendDelBucketsRequest(ap *projector.Client,
	topic string,
	buckets []string) Message {

	c.Debugf("KVSender::sendDelBucketsRequest Projector %v Topic %v Buckets %v",
		ap, topic, buckets)

	sleepTime := 1
	retry := 0

	for {
		if err := ap.DelBuckets(topic, buckets); err != nil {
			if isRetryReqd(err) && retry < MAX_KV_REQUEST_RETRY {
				c.Errorf("KVSender::sendDelBucketsRequest \n\tError Connecting to Projector %v. "+
					"Retry in %v seconds... \n\t Err %v", ap, sleepTime, err)
				time.Sleep(time.Duration(sleepTime) * time.Second)
				sleepTime *= 2
				retry++
			} else {
				c.Errorf("KVSender::sendDelBucketsRequest \n\tUnexpected Error During "+
					"Del Buckets Request Topic %v Buckets %v. Err %v",
					topic, buckets, err)

				return &MsgError{
					err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
						severity: FATAL,
						cause:    err}}
			}
		} else {
			return &MsgSuccess{}

		}
	}

}

//send the actual ShutdownStreamRequest on adminport
func sendShutdownTopic(ap *projector.Client,
	topic string) Message {

	c.Debugf("KVSender::sendShutdownTopic Projector %v Topic %v", ap, topic)

	sleepTime := 1
	retry := 0
	for {
		if err := ap.ShutdownTopic(topic); err != nil {
			if isRetryReqd(err) && retry < MAX_KV_REQUEST_RETRY {
				c.Errorf("KVSender::sendShutdownTopic \n\tError Connecting to Projector %v. "+
					"Retry in %v seconds... \n\tErr %v", ap, sleepTime, err)
				time.Sleep(time.Duration(sleepTime) * time.Second)
				sleepTime *= 2
				retry++
			} else {
				c.Errorf("KVSender::sendShutdownTopic \n\tUnexpected Error During "+
					"Shutdown Topic %v. Err %v", topic, err)

				return &MsgError{
					err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
						severity: FATAL,
						cause:    err}}
			}
		} else {

			return &MsgSuccess{}
		}
	}
}

func getTopicForStreamId(streamId c.StreamId) string {

	var topic string

	switch streamId {
	case c.MAINT_STREAM:
		topic = MAINT_TOPIC
	case c.CATCHUP_STREAM:
		topic = CATCHUP_TOPIC
	case c.INIT_STREAM:
		topic = INIT_TOPIC
	}

	return topic
}

func (k *kvSender) makeInitialTs(bucket string,
	vbnos []uint32) (*protobuf.TsVbuuid, error) {

	flogs, err := k.getFailoverLogs(bucket, vbnos)
	if err != nil {
		c.Errorf("KVSender::makeRestartTS \n\tUnexpected Error During Failover "+
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
		c.Errorf("KVSender::makeRestartTS \n\tUnexpected Error During Failover "+
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

func (k *kvSender) getVbmap(bucket string,
	kvaddrs []string) (*protobuf.VbmapResponse, error) {

	//if list of KVs is not there yet, build it
	if len(k.kvListCache) == 0 {
		if err := k.initKVListCache(bucket); err != nil {
			c.Errorf("KVSender::getVbmap Error in Init Cache")
			return nil, err
		}
	}

	var err error
	var res *protobuf.VbmapResponse

outerloop:
	for _, kv := range k.kvListCache {
		c.Debugf("KVSender::getVbmap \n\tSending Request to KV %v", kv)
		projAddr := getProjectorAddrFromKVAddr(kv)
		client := projector.NewClient(projAddr)
		sleepTime := 1
		retry := 0
	innerloop:
		for {
			if res, err = client.GetVbmap(DEFAULT_POOL, bucket, kvaddrs); err != nil {
				if isRetryReqd(err) && retry < MAX_KV_REQUEST_RETRY {
					c.Errorf("KVSender::getVbmap \n\tError Connecting to Projector %v. "+
						"Retry in %v seconds... \n\tError Details %v", projAddr, sleepTime, err)
					time.Sleep(time.Duration(sleepTime) * time.Second)
					sleepTime *= 2
					retry++
				} else {
					break innerloop
				}
			} else {
				break outerloop
			}
		}
	}

	c.Debugf("KVSender::getVbmap \n\tVbMap Response %v", res)

	if err == nil {
		k.updateKVListCache(res)
	}

	return res, err
}

func (k *kvSender) getFailoverLogs(bucket string,
	vbnos []uint32) (*protobuf.FailoverLogResponse, error) {

	var err error
	var res *protobuf.FailoverLogResponse

	//get failover log from any node
outerloop:
	for _, kv := range k.kvListCache {
		c.Debugf("KVSender::getFailoverLogs \n\tSending Request to KV %v", kv)
		projAddr := getProjectorAddrFromKVAddr(kv)
		client := projector.NewClient(projAddr)
		sleepTime := 1
		retry := 0
	innerloop:
		for {
			if res, err = client.GetFailoverLogs(DEFAULT_POOL, bucket, vbnos); err != nil {
				if isRetryReqd(err) && retry < MAX_KV_REQUEST_RETRY {
					c.Errorf("KVSender::getFailoverLogs \n\tError Connecting to Projector %v. "+
						"Retry in %v seconds... \n\tError Details %v", projAddr, sleepTime, err)
					time.Sleep(time.Duration(sleepTime) * time.Second)
					sleepTime *= 2
					retry++
				} else {
					break innerloop
				}
			} else {
				break outerloop
			}
		}
	}

	c.Debugf("KVSender::getFailoverLogs \n\tFailover Log Response %v", res)

	return res, err
}

func (k *kvSender) initKVListCache(bucket string) error {

	//TODO Is there a better way to do this rather than send
	//a vbmap request
	req := &protobuf.VbmapRequest{
		Pool:    proto.String(DEFAULT_POOL),
		Bucket:  proto.String(bucket),
		Kvaddrs: nil,
	}

	res := &protobuf.VbmapResponse{}

	ap := adminport.NewHTTPClient(HTTP_PREFIX+PROJECTOR_ADMIN_PORT_ENDPOINT, "/adminport/")
	if err := ap.Request(req, res); err != nil {
		c.Errorf("KVSender::initKVListCache \n\tError Connecting to Projector %v. ",
			PROJECTOR_ADMIN_PORT_ENDPOINT)
		return err
	}

	k.updateKVListCache(res)
	return nil

}

//update the server list cache
func (k *kvSender) updateKVListCache(vbmap *protobuf.VbmapResponse) {

	//clear the cache
	k.kvListCache = k.kvListCache[:0]

	//update the cache
	for _, kv := range vbmap.GetKvaddrs() {
		k.kvListCache = append(k.kvListCache, kv)
	}

}

func getProjectorAddrFromKVAddr(kv string) string {
	var projAddr string
	if host, port, err := net.SplitHostPort(kv); err == nil {
		if IsIPLocal(host) {

			if port == KV_DCP_PORT {
				projAddr = LOCALHOST + ":" + PROJECTOR_PORT
			} else {
				iportProj, _ := strconv.Atoi(PROJECTOR_PORT)
				iportKV, _ := strconv.Atoi(port)
				iportKV0, _ := strconv.Atoi(KV_DCP_PORT_CLUSTER_RUN)

				//In cluster_run, port number increments by 2
				nodeNum := (iportKV - iportKV0) / 2
				p := iportProj + nodeNum
				projAddr = LOCALHOST + ":" + strconv.Itoa(p)
			}
			c.Debugf("KVSender::getProjectorAddrFromKVAddr \n\t Local Projector Addr: %v", projAddr)
		} else {
			projAddr = host + ":" + PROJECTOR_PORT
			c.Debugf("KVSender::getProjectorAddrFromKVAddr \n\t Remote Projector Addr: %v", projAddr)
		}
	}
	return projAddr
}

//convert IndexInst to protobuf format
func convertIndexListToProto(indexList []c.IndexInst, streamId c.StreamId) []*protobuf.Instance {

	protoList := make([]*protobuf.Instance, 0)
	for _, index := range indexList {
		protoInst := convertIndexInstToProtoInst(index, streamId)
		protoList = append(protoList, protoInst)
	}

	return protoList

}

//convert IndexInst to protobuf format
func convertIndexInstToProtoInst(indexInst c.IndexInst, streamId c.StreamId) *protobuf.Instance {

	protoDefn := convertIndexDefnToProtobuf(indexInst.Defn)
	protoInst := convertIndexInstToProtobuf(indexInst, protoDefn)

	addPartnInfoToProtoInst(indexInst, streamId, protoInst)

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
		SecExpressions:  indexDefn.OnExprList,
		PartitionScheme: partnScheme,
		PartnExpression: proto.String(indexDefn.PartitionKey),
	}

	return defn

}

func convertIndexInstToProtobuf(indexInst c.IndexInst,
	protoDefn *protobuf.IndexDefn) *protobuf.IndexInst {

	state := protobuf.IndexState(int32(indexInst.State)).Enum()
	instance := &protobuf.IndexInst{
		InstId:     proto.Uint64(uint64(indexInst.InstId)),
		State:      state,
		Definition: protoDefn,
	}
	return instance
}

func addPartnInfoToProtoInst(indexInst c.IndexInst,
	streamId c.StreamId, protoInst *protobuf.IndexInst) {

	switch partn := indexInst.Pc.(type) {
	case *c.KeyPartitionContainer:

		//Right now the fill the TestPartition as that is the only
		//partition structure supported
		partnDefn := partn.GetAllPartitions()

		var endpoints []string
		for _, p := range partnDefn {
			for _, e := range p.Endpoints() {
				//Set the right endpoint based on streamId
				switch streamId {
				case c.MAINT_STREAM:
					e = c.Endpoint(INDEXER_MAINT_DATA_PORT_ENDPOINT)
				case c.CATCHUP_STREAM:
					e = c.Endpoint(INDEXER_CATCHUP_DATA_PORT_ENDPOINT)
				case c.INIT_STREAM:
					e = c.Endpoint(INDEXER_INIT_DATA_PORT_ENDPOINT)
				}
				endpoints = append(endpoints, string(e))
			}

		}
		protoInst.Tp = &protobuf.TestPartition{
			Endpoints: endpoints,
		}
	}
}

func isRetryReqd(err error) bool {

	//TODO Add more conditions
	return strings.Contains(err.Error(), "connection refused")

}

//Return any valid bucket name
func (k *kvSender) getAnyBucketName() string {

	for _, bucketMap := range k.streamBucketIndexCountMap {
		for bucket, _ := range bucketMap {
			return bucket
		}
	}

	return ""

}
