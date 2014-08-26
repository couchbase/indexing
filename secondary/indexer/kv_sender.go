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
	"code.google.com/p/goprotobuf/proto"
	"errors"
	"github.com/couchbase/indexing/secondary/adminport"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	"net"
	"strconv"
)

var HTTP_PREFIX string = "http://"

//KVSender provides the mechanism to talk to KV(projector, router etc)
type KVSender interface {
}

type kvSender struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any message to supervisor

	streamStatus              StreamStatusMap
	streamBucketIndexCountMap map[c.StreamId]BucketIndexCountMap

	numVbuckets     uint16
	serverListCache []string
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
		serverListCache:           make([]string, 0),
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

	//For now, only one index comes in the request
	//TODO Add Batching support
	indexInst := indexInstList[0]

	//start mutation stream, if error return to supervisor
	resp := k.openMutationStream(streamId, indexInst)
	if resp.GetMsgType() != MSG_SUCCESS {
		k.supvCmdch <- resp
		return
	}

	//increment index count for this bucket
	bucketIndexCountMap := make(BucketIndexCountMap)
	bucketIndexCountMap[indexInst.Defn.Bucket] = 1
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
		bucketIndexCountMap := make(BucketIndexCountMap)
		bucketIndexCountMap[indexInst.Defn.Bucket] = 1
		k.streamBucketIndexCountMap[streamId] = bucketIndexCountMap

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

	resp := k.deleteIndexFromStream(streamId, indexInst)
	if resp.GetMsgType() != MSG_SUCCESS {
		k.supvCmdch <- resp
		return
	}
	k.streamBucketIndexCountMap[streamId][indexInst.Defn.Bucket]--

	//if this is the last index for this bucket, delete bucket
	//from the stream
	if c, ok := k.streamBucketIndexCountMap[streamId][indexInst.Defn.Bucket]; c == 0 || !ok {

		resp := k.deleteBucketFromStream(streamId, indexInst.Defn.Bucket)
		if resp.GetMsgType() != MSG_SUCCESS {
			k.supvCmdch <- resp
			return
		}

		//TODO verify this
		delete(k.streamBucketIndexCountMap[streamId], indexInst.Defn.Bucket)
	}

	//if this was the last index in the stream, close it
	if len(k.streamBucketIndexCountMap[streamId]) == 0 {

		resp := k.closeMutationStream(streamId, indexInst.Defn.Bucket)
		if resp.GetMsgType() != MSG_SUCCESS {
			k.supvCmdch <- resp
			return
		}

		//clean internal maps
		delete(k.streamBucketIndexCountMap, streamId)
		k.streamStatus[streamId] = false
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

	resp := k.closeMutationStream(streamId, bucket)
	if resp.GetMsgType() != MSG_SUCCESS {
		k.supvCmdch <- resp
		return
	}

	//clean internal maps
	delete(k.streamBucketIndexCountMap, streamId)
	k.streamStatus[streamId] = false

}

func (k *kvSender) handleGetCurrKVTimestamp(cmd Message) {

	//TODO For now Indexer is getting the TS directly from
	//KV. Once Projector API is ready, use that.

}

func (k *kvSender) openMutationStream(streamId c.StreamId, indexInst c.IndexInst) Message {

	protoInst := convertIndexInstToProtoInst(indexInst, streamId)

	ap := adminport.NewHTTPClient(HTTP_PREFIX+PROJECTOR_ADMIN_PORT_ENDPOINT, "/adminport/")

	//Get the Vbmap of all nodes in the cluster
	vbmap, err := getVbmap(ap, indexInst.Defn.Bucket, nil)
	if err != nil {
		c.Errorf("KVSender::openMutationStream \n\t Error In GetVbMap %v", err)
		return &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
	}

	vbnosList := vbmap.GetKvvbnos()

	//update the list of servers in local cache.
	//in case primary node goes down, other nodes
	//can be used for communication.
	k.updateServerListCache(vbmap)

	//for all the nodes in vbmap
	for i, kv := range vbmap.GetKvaddrs() {

		//get projector address from kv address
		projAddr := getProjectorAddrFromKVAddr(kv)

		//create client for node's projectors
		ap := adminport.NewHTTPClient(HTTP_PREFIX+projAddr, "/adminport/")

		//get the list of vbnos for this kv
		vbnos := vbnosList[i].GetVbnos()

		ts, err := makeRestartTimestamp(ap, indexInst.Defn.Bucket, vbnos)
		if err != nil {
			return &MsgError{
				err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
					severity: FATAL,
					cause:    err}}
		}
		topic := getTopicForStreamId(streamId)
		mReq := protobuf.MutationStreamRequest{
			Topic:             proto.String(topic),
			Pools:             []string{DEFAULT_POOL},
			Buckets:           []string{indexInst.Defn.Bucket},
			RestartTimestamps: []*protobuf.TsVbuuid{ts},
			Instances:         []*protobuf.IndexInst{protoInst},
		}

		mReq.SetStartFlag()

		if _, resp := sendMutationStreamRequest(ap, mReq); resp.GetMsgType() != MSG_SUCCESS {
			//TODO send message to all KVs to revert the previous requests sent
			return resp
		}

	}

	return &MsgSuccess{}
}

func (k *kvSender) addIndexForNewBucket(streamId c.StreamId, indexInst c.IndexInst) Message {

	protoInst := convertIndexInstToProtoInst(indexInst, streamId)

	ap := adminport.NewHTTPClient(HTTP_PREFIX+PROJECTOR_ADMIN_PORT_ENDPOINT, "/adminport/")

	//Get the Vbmap of all nodes in the cluster
	vbmap, err := getVbmap(ap, indexInst.Defn.Bucket, nil)
	if err != nil {
		c.Errorf("KVSender::addIndexForNewBucket \n\t Error In GetVbMap %v", err)
		return &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
	}

	vbnosList := vbmap.GetKvvbnos()

	//update the list of servers in local cache.
	//in case primary node goes down, other nodes
	//can be used for communication.
	k.updateServerListCache(vbmap)

	//for all the nodes in vbmap
	for i, kv := range vbmap.GetKvaddrs() {

		//get projector address from kv address
		projAddr := getProjectorAddrFromKVAddr(kv)

		//create client for node's projectors
		ap := adminport.NewHTTPClient(HTTP_PREFIX+projAddr, "/adminport/")

		//get the list of vbnos for this kv
		vbnos := vbnosList[i].GetVbnos()

		ts, err := makeRestartTimestamp(ap, indexInst.Defn.Bucket, vbnos)
		if err != nil {
			return &MsgError{
				err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
					severity: FATAL,
					cause:    err}}
		}
		topic := getTopicForStreamId(streamId)
		mReq := protobuf.UpdateMutationStreamRequest{
			Topic:             proto.String(topic),
			Pools:             []string{DEFAULT_POOL},
			Buckets:           []string{indexInst.Defn.Bucket},
			RestartTimestamps: []*protobuf.TsVbuuid{ts},
			Instances:         []*protobuf.IndexInst{protoInst},
		}

		mReq.SetAddBucketFlag()

		if _, resp := sendUpdateMutationStreamRequest(ap, mReq); resp.GetMsgType() != MSG_SUCCESS {
			//TODO send message to all KVs to revert the previous requests sent
			return resp
		}
	}

	return &MsgSuccess{}
}

func (k *kvSender) addIndexForExistingBucket(streamId c.StreamId, indexInst c.IndexInst) Message {

	protoInst := convertIndexInstToProtoInst(indexInst, streamId)

	ap := adminport.NewHTTPClient(HTTP_PREFIX+PROJECTOR_ADMIN_PORT_ENDPOINT, "/adminport/")

	//Get the Vbmap of all nodes in the cluster
	vbmap, err := getVbmap(ap, indexInst.Defn.Bucket, nil)
	if err != nil {
		c.Errorf("KVSender::addIndexForExistingBucket \n\t Error In GetVbMap %v", err)
		return &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
	}

	//update the list of servers in local cache.
	//in case primary node goes down, other nodes
	//can be used for communication.
	k.updateServerListCache(vbmap)

	//for all the nodes in vbmap
	for _, kv := range vbmap.GetKvaddrs() {

		//get projector address from kv address
		projAddr := getProjectorAddrFromKVAddr(kv)

		//create client for node's projectors
		ap := adminport.NewHTTPClient(HTTP_PREFIX+projAddr, "/adminport/")

		//add new engine(index) to existing stream
		topic := getTopicForStreamId(streamId)
		mReq := protobuf.SubscribeStreamRequest{
			Topic:     proto.String(topic),
			Instances: []*protobuf.IndexInst{protoInst},
		}

		mReq.SetAddEnginesFlag()

		if _, resp := sendSubscribeStreamRequest(ap, mReq); resp.GetMsgType() != MSG_SUCCESS {
			//TODO send message to all KVs to revert the previous requests sent
			return resp
		}
	}

	return &MsgSuccess{}
}

func (k *kvSender) deleteIndexFromStream(streamId c.StreamId, indexInst c.IndexInst) Message {

	protoInst := convertIndexInstToProtoInst(indexInst, streamId)

	ap := adminport.NewHTTPClient(HTTP_PREFIX+PROJECTOR_ADMIN_PORT_ENDPOINT, "/adminport/")

	//Get the Vbmap of all nodes in the cluster
	vbmap, err := getVbmap(ap, indexInst.Defn.Bucket, nil)
	if err != nil {
		c.Errorf("KVSender::deleteIndexFromStream \n\t Error In GetVbMap %v", err)
		return &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
	}

	//update the list of servers in local cache.
	//in case primary node goes down, other nodes
	//can be used for communication.
	k.updateServerListCache(vbmap)

	//for all the nodes in vbmap
	for _, kv := range vbmap.GetKvaddrs() {

		//get projector address from kv address
		projAddr := getProjectorAddrFromKVAddr(kv)

		//create client for node's projectors
		ap := adminport.NewHTTPClient(HTTP_PREFIX+projAddr, "/adminport/")

		//delete engine(index) from the existing stream
		topic := getTopicForStreamId(streamId)
		mReq := protobuf.SubscribeStreamRequest{
			Topic:     proto.String(topic),
			Instances: []*protobuf.IndexInst{protoInst},
		}

		mReq.SetDeleteEnginesFlag()

		if _, resp := sendSubscribeStreamRequest(ap, mReq); resp.GetMsgType() != MSG_SUCCESS {
			//TODO send message to all KVs to revert the previous requests sent
			return resp
		}

	}

	return &MsgSuccess{}
}

func (k *kvSender) deleteBucketFromStream(streamId c.StreamId, bucket string) Message {

	ap := adminport.NewHTTPClient(HTTP_PREFIX+PROJECTOR_ADMIN_PORT_ENDPOINT, "/adminport/")

	//Get the Vbmap of all nodes in the cluster
	vbmap, err := getVbmap(ap, bucket, nil)
	if err != nil {
		c.Errorf("KVSender::deleteBucketFromStream \n\t Error In GetVbMap %v", err)
		return &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
	}

	//update the list of servers in local cache.
	//in case primary node goes down, other nodes
	//can be used for communication.
	k.updateServerListCache(vbmap)

	//for all the nodes in vbmap
	for _, kv := range vbmap.GetKvaddrs() {

		//get projector address from kv address
		projAddr := getProjectorAddrFromKVAddr(kv)

		//create client for node's projectors
		ap := adminport.NewHTTPClient(HTTP_PREFIX+projAddr, "/adminport/")

		topic := getTopicForStreamId(streamId)
		mReq := protobuf.UpdateMutationStreamRequest{
			Topic:   proto.String(topic),
			Buckets: []string{bucket},
		}

		mReq.SetDelBucketFlag()

		if _, resp := sendUpdateMutationStreamRequest(ap, mReq); resp.GetMsgType() != MSG_SUCCESS {
			//TODO send message to all KVs to revert the previous requests sent
			return resp
		}

	}

	return &MsgSuccess{}
}

func (k *kvSender) closeMutationStream(streamId c.StreamId, bucket string) Message {

	ap := adminport.NewHTTPClient(HTTP_PREFIX+PROJECTOR_ADMIN_PORT_ENDPOINT, "/adminport/")

	//Get the Vbmap of all nodes in the cluster
	vbmap, err := getVbmap(ap, bucket, nil)
	if err != nil {
		c.Errorf("KVSender::closeMutationStream \n\t Error In GetVbMap %v", err)
		return &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
	}

	//update the list of servers in local cache.
	//in case primary node goes down, other nodes
	//can be used for communication.
	k.updateServerListCache(vbmap)

	//for all the nodes in vbmap
	for _, kv := range vbmap.GetKvaddrs() {

		//get projector address from kv address
		projAddr := getProjectorAddrFromKVAddr(kv)

		//create client for node's projectors
		ap := adminport.NewHTTPClient(HTTP_PREFIX+projAddr, "/adminport/")

		topic := getTopicForStreamId(streamId)
		sReq := protobuf.ShutdownStreamRequest{
			Topic: proto.String(topic),
		}
		if _, resp := sendShutdownStreamRequest(ap, sReq); resp.GetMsgType() != MSG_SUCCESS {
			return resp
		}

	}

	return &MsgSuccess{}

}

//send the actual MutationStreamRequest on adminport
func sendMutationStreamRequest(ap adminport.Client,
	mReq protobuf.MutationStreamRequest) (protobuf.MutationStreamResponse, Message) {

	c.Debugf("KVSender::sendMutationStreamRequest \n\t%v", mReq)
	mRes := protobuf.MutationStreamResponse{}

	if err := ap.Request(&mReq, &mRes); err != nil {
		c.Errorf("KVSender::sendMutationStreamRequest \n\tUnexpected Error During Mutation Stream "+
			"Request %v for IndexInst %v. Err %v", mReq, mReq.GetInstances(), err)

		return mRes, &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
	} else if err := mRes.GetErr(); err != nil {
		c.Errorf("KVSender::sendMutationStreamRequest \n\tUnexpected Error During Mutation Stream "+
			"Request %v for IndexInst %v. Err %v", mReq, mReq.GetInstances(), err.GetError())

		return mRes, &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    errors.New(err.GetError())}}
	}

	c.Debugf("KVSender::sendMutationStreamRequest \n\tMutationStream Response %v", mRes)

	return mRes, &MsgSuccess{}
}

//send the actual UpdateMutationStreamRequest on adminport
func sendUpdateMutationStreamRequest(ap adminport.Client,
	mReq protobuf.UpdateMutationStreamRequest) (protobuf.MutationStreamResponse, Message) {

	c.Debugf("KVSender::sendUpdateMutationStreamRequest \n\t%v", mReq)

	mRes := protobuf.MutationStreamResponse{}
	if err := ap.Request(&mReq, &mRes); err != nil {
		c.Errorf("KVSender::sendUpdateMutationStreamRequest \n\tUnexpected Error During "+
			"Mutation Stream Request %v for IndexInst %v. Err %v. Resp %v.",
			mReq, mReq.GetInstances(), err, mRes)

		return mRes, &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
	} else if mRes.GetErr() != nil {
		err := mRes.GetErr()
		c.Errorf("KVSender::sendUpdateMutationStreamRequest \n\tUnexpected Error During "+
			"Mutation Stream Request %v for IndexInst %v. Err %v",
			mReq, mReq.GetInstances(), err.GetError())

		return mRes, &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    errors.New(err.GetError())}}
	}

	c.Debugf("KVSender::sendUpdateMutationStreamRequest \n\tMutationStreamResponse %v", mReq)

	return mRes, &MsgSuccess{}
}

//send the actual UpdateMutationStreamRequest on adminport
func sendSubscribeStreamRequest(ap adminport.Client,
	mReq protobuf.SubscribeStreamRequest) (protobuf.Error, Message) {

	c.Debugf("KVSender::sendSubscribeStreamRequest \n\t%v", mReq)

	mRes := protobuf.Error{}
	if err := ap.Request(&mReq, &mRes); err != nil {
		c.Errorf("KVSender::sendSubscribeStreamRequest \n\tUnexpected Error During "+
			"Subscribe Stream Request %v for IndexInst %v. Err %v. Resp %v.",
			mReq, mReq.GetInstances(), err, mRes)

		return mRes, &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}

	} else if mRes.GetError() != "" {
		c.Errorf("KVSender::sendSubscribeStreamRequest \n\tUnexpected Error During "+
			"Subscribe Stream Request %v for IndexInst %v. Err %v",
			mReq, mReq.GetInstances(), mRes.GetError())

		return mRes, &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    errors.New(mRes.GetError())}}
	}

	return mRes, &MsgSuccess{}
}

//send the actual ShutdownStreamRequest on adminport
func sendShutdownStreamRequest(ap adminport.Client,
	sReq protobuf.ShutdownStreamRequest) (protobuf.Error, Message) {

	c.Debugf("KVSender::sendShutdownStreamRequest \n\t%v", sReq)

	sRes := protobuf.Error{}
	if err := ap.Request(&sReq, &sRes); err != nil {
		c.Errorf("KVSender::sendShutdownStreamRequest \n\tUnexpected Error During "+
			"Close Mutation Stream Request %v. Err %v. Resp %v.",
			sReq, err, sRes)

		return sRes, &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}

	} else if sRes.GetError() != "" {
		c.Errorf("KVSender::sendShutdownStreamRequest \n\tUnexpected Error During "+
			"Close Mutation Stream Request %v. Err %v. Resp %v.", sReq, sRes.GetError(), sRes)

		return sRes, &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    errors.New(sRes.GetError())}}
	}

	return sRes, &MsgSuccess{}
}

func getTopicForStreamId(streamId c.StreamId) string {

	var topic string

	switch streamId {
	case c.MAINT_STREAM:
		topic = MAINT_TOPIC
	case c.INIT_STREAM:
		topic = INIT_TOPIC
	}

	return topic
}

func makeRestartTimestamp(client adminport.Client, bucket string,
	vbnos []uint32) (*protobuf.TsVbuuid, error) {

	flogs, err := getFailoverLogs(client, bucket, vbnos)
	if err != nil {
		c.Errorf("KVSender::makeRestartTimestamp \n\tUnexpected Error During Failover "+
			"Log Request for Bucket %v. Err %v", bucket, err)
		return nil, err
	}

	ts := protobuf.NewTsVbuuid(bucket, len(vbnos))
	ts = ts.InitialRestartTs(c.Vbno32to16(vbnos))
	ts = ts.ComputeRestartTs(flogs.ToFailoverLog(c.Vbno32to16(vbnos)))

	return ts, nil
}

func getVbmap(client adminport.Client, bucket string,
	kvaddrs []string) (*protobuf.VbmapResponse, error) {

	req := &protobuf.VbmapRequest{
		Pool:    proto.String(DEFAULT_POOL),
		Bucket:  proto.String(bucket),
		Kvaddrs: kvaddrs,
	}

	c.Debugf("KVSender::getVbmap \n\tVbMap Request %v", req)

	res := &protobuf.VbmapResponse{}
	if err := client.Request(req, res); err != nil {
		return nil, err
	}

	c.Debugf("KVSender::getVbmap \n\tVbMap Response %v", res)

	return res, nil
}

func getFailoverLogs(client adminport.Client, bucket string,
	vbnos []uint32) (*protobuf.FailoverLogResponse, error) {

	req := &protobuf.FailoverLogRequest{
		Pool:   proto.String(DEFAULT_POOL),
		Bucket: proto.String(bucket),
		Vbnos:  vbnos,
	}

	c.Debugf("KVSender::getFailoverLogs \n\tFailover Log Request %v", req)

	res := &protobuf.FailoverLogResponse{}
	if err := client.Request(req, res); err != nil {
		return nil, err
	}

	c.Debugf("KVSender::getFailoverLogs \n\tFailover Log Response %v", res)

	return res, nil
}

//update the server list cache
func (k *kvSender) updateServerListCache(vbmap *protobuf.VbmapResponse) {

	//clear the cache
	k.serverListCache = k.serverListCache[:0]

	//update the cache
	for _, kv := range vbmap.GetKvaddrs() {
		k.serverListCache = append(k.serverListCache, kv)
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
func convertIndexInstToProtoInst(indexInst c.IndexInst, streamId c.StreamId) *protobuf.IndexInst {

	protoDefn := convertIndexDefnToProtobuf(indexInst.Defn)
	protoInst := convertIndexInstToProtobuf(indexInst, protoDefn)

	addPartnInfoToProtoInst(indexInst, streamId, protoInst)

	return protoInst
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
