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
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	projClient "github.com/couchbase/indexing/secondary/projector/client"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
	"github.com/couchbaselabs/goprotobuf/proto"
	"strings"
	"time"
)

const (
	HTTP_PREFIX             string = "http://"
	MAX_KV_REQUEST_RETRY    int    = 5
	MAX_CLUSTER_FETCH_RETRY int    = 600
)

//KVSender provides the mechanism to talk to KV(projector, router etc)
type KVSender interface {
}

type kvSender struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any message to supervisor

	streamStatus              StreamStatusMap
	streamBucketIndexCountMap map[c.StreamId]BucketIndexCountMap

	cInfoCache *c.ClusterInfoCache
	config     c.Config
}

func NewKVSender(supvCmdch MsgChannel, supvRespch MsgChannel,
	config c.Config) (KVSender, Message) {

	cinfo, err := c.NewClusterInfoCache(config["clusterAddr"].String(), DEFAULT_POOL)
	if err != nil {
		panic("Unable to initialize cluster_info - " + err.Error())
	}
	//Init the kvSender struct
	k := &kvSender{
		supvCmdch:                 supvCmdch,
		supvRespch:                supvRespch,
		streamStatus:              make(StreamStatusMap),
		streamBucketIndexCountMap: make(map[c.StreamId]BucketIndexCountMap),
		cInfoCache:                cinfo,
		config:                    config,
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

	case KV_SENDER_RESTART_VBUCKETS:
		k.handleRestartVbuckets(cmd)

	case KV_SENDER_REPAIR_ENDPOINTS:
		k.handleRepairEndpoints(cmd)

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
	restartTs := cmd.(*MsgStreamUpdate).GetRestartTs()

	//start mutation stream, if error return to supervisor
	resp := k.openMutationStream(streamId, indexInstList, restartTs)

	//increment index count for this bucket
	bucketIndexCountMap := make(BucketIndexCountMap)
	for _, indexInst := range indexInstList {
		bucketIndexCountMap[indexInst.Defn.Bucket] += 1
	}
	k.streamBucketIndexCountMap[streamId] = bucketIndexCountMap

	k.streamStatus[streamId] = true

	k.supvCmdch <- resp
	return

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

	if len(indexInstList) == 0 {
		//nothing to do
		k.supvCmdch <- &MsgSuccess{}
		return
	}

	var emptyBucketList []string

	bucketIndexCountMap := k.streamBucketIndexCountMap[streamId]
	for _, index := range indexInstList {
		bucketIndexCountMap[index.Defn.Bucket]--
		if bucketIndexCountMap[index.Defn.Bucket] == 0 {
			//add this bucket to list of empty buckets
			emptyBucketList = append(emptyBucketList, index.Defn.Bucket)
			delete(bucketIndexCountMap, index.Defn.Bucket)
		}
	}

	//for any empty bucket, delete the indexes from the list of to-be-deleted indexes
	//as the bucket itself is going to be deleted from the stream anyway
	var delIndexList []c.IndexInst
	for _, index := range indexInstList {
		exclude := false
		for _, bucket := range emptyBucketList {
			if bucket == index.Defn.Bucket {
				exclude = true
			}
		}
		if !exclude {
			delIndexList = append(delIndexList, index)
		}
	}

	//if this is the last index in the stream, the stream needs to be closed.
	//projector cannot work with empty streams. deleting an instance
	//or bucket in this case would result in problem.
	if len(bucketIndexCountMap) == 0 {

		resp := k.closeMutationStream(streamId, indexInstList[0].Defn.Bucket)
		if resp.GetMsgType() != MSG_SUCCESS {
			k.supvCmdch <- resp
			return
		}
		//clean internal maps
		delete(k.streamBucketIndexCountMap, streamId)
		k.streamStatus[streamId] = false
	} else {
		//for all the buckets where no more index is left, the bucket needs to be
		//delete from stream. projector cannot work with empty bucket in a stream.
		//deleting an index instance in this case would result in problem.
		if len(emptyBucketList) != 0 {
			resp := k.deleteBucketsFromStream(streamId, emptyBucketList)
			if resp.GetMsgType() != MSG_SUCCESS {
				k.supvCmdch <- resp
				return
			}

		}

		//for the remaining ones, delete the indexes
		if len(delIndexList) != 0 {
			resp := k.deleteIndexesFromStream(streamId, delIndexList)
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

func (k *kvSender) handleRestartVbuckets(cmd Message) {

	c.Infof("KVSender::handleRestartVbuckets %v", cmd)

	streamId := cmd.(*MsgRestartVbuckets).GetStreamId()

	if status, _ := k.streamStatus[streamId]; !status {
		k.supvCmdch <- &MsgError{
			err: Error{code: ERROR_KV_SENDER_UNKNOWN_STREAM,
				severity: FATAL}}
		return
	}

	restartTs := cmd.(*MsgRestartVbuckets).GetRestartTs()

	resp := k.restartVbuckets(streamId, restartTs)
	k.supvCmdch <- resp
}

func (k *kvSender) handleRepairEndpoints(cmd Message) {

	c.Infof("KVSender::handleRepairEndpoints %v", cmd)

	streamId := cmd.(*MsgRepairEndpoints).GetStreamId()

	if status, _ := k.streamStatus[streamId]; !status {
		k.supvCmdch <- &MsgError{
			err: Error{code: ERROR_KV_SENDER_UNKNOWN_STREAM,
				severity: FATAL}}
		return
	}

	endpoints := cmd.(*MsgRepairEndpoints).GetEndpoints()

	resp := k.repairEndpoints(streamId, endpoints)
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

	err := k.cInfoCache.Fetch()
	if err != nil {
		c.Errorf("KVSender::openMutationStream \n\t Error in fetching cluster info", err)
		return &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
	}

	var protoInstList []*protobuf.Instance
	for _, indexInst := range indexInstList {
		protoInstList = append(protoInstList,
			convertIndexInstToProtoInst(k.config, k.cInfoCache, indexInst, streamId))
	}

	bucket := indexInstList[0].Defn.Bucket
	nodes, _ := k.cInfoCache.GetNodesByBucket(bucket)

	rollbackTs := make(map[string]*protobuf.TsVbuuid)
	for _, nid := range nodes {

		//get the list of vbnos for this kv
		vbnos, _ := k.cInfoCache.GetVBuckets(nid, bucket)
		if len(vbnos) == 0 {
			continue
		}

		addr, _ := k.cInfoCache.GetServiceAddress(nid, "projector")
		//create client for node's projectors
		config := c.SystemConfig.SectionConfig("projector.client.", true)
		maxvbs := c.SystemConfig["maxVbuckets"].Int()
		ap := projClient.NewClient(addr, maxvbs, config)

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
		if res, errMsg := sendMutationTopicRequest(ap, topic, restartTsList, protoInstList); errMsg.GetMsgType() != MSG_SUCCESS {
			//TODO send message to all KVs to revert the previous requests sent
			return errMsg
		} else {
			respTsList := res.GetRollbackTimestamps()
			for _, respTs := range respTsList {
				if respTs != nil && !respTs.IsEmpty() {
					if ts, ok := rollbackTs[respTs.GetBucket()]; ok {
						ts.Union(respTs)
					} else {
						rollbackTs[respTs.GetBucket()] = respTs
					}
				}
			}
		}
	}

	if len(rollbackTs) != 0 {
		//convert from protobuf to native format
		nativeTs := make(map[string]*c.TsVbuuid)
		for bucket, ts := range rollbackTs {
			nativeTs[bucket] = ts.ToTsVbuuid()
		}

		return &MsgRollback{streamId: streamId,
			rollbackTs: nativeTs}
	}

	return &MsgSuccess{}
}

func (k *kvSender) addIndexForNewBucket(streamId c.StreamId, indexInst c.IndexInst) Message {
	err := k.cInfoCache.Fetch()
	if err != nil {
		c.Errorf("KVSender::addIndexForNewBucket \n\t Error in fetching cluster info", err)
		return &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
	}

	protoInst := convertIndexInstToProtoInst(k.config, k.cInfoCache, indexInst, streamId)
	bucket := indexInst.Defn.Bucket
	nodes, _ := k.cInfoCache.GetNodesByBucket(bucket)

	for _, nid := range nodes {
		addr, _ := k.cInfoCache.GetServiceAddress(nid, "projector")
		//create client for node's projectors
		config := c.SystemConfig.SectionConfig("projector.client.", true)
		maxvbs := c.SystemConfig["maxVbuckets"].Int()
		ap := projClient.NewClient(addr, maxvbs, config)

		//get the list of vbnos for this kv
		vbnos, _ := k.cInfoCache.GetVBuckets(nid, bucket)

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
	err := k.cInfoCache.Fetch()
	if err != nil {
		c.Errorf("KVSender::addIndexForExistingBucket \n\t Error in fetching cluster info", err)
		return &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
	}

	protoInst := convertIndexInstToProtoInst(k.config, k.cInfoCache, indexInst, streamId)
	bucket := indexInst.Defn.Bucket
	nodes, _ := k.cInfoCache.GetNodesByBucket(bucket)

	for _, nid := range nodes {
		addr, _ := k.cInfoCache.GetServiceAddress(nid, "projector")
		//create client for node's projectors
		config := c.SystemConfig.SectionConfig("projector.client.", true)
		maxvbs := c.SystemConfig["maxVbuckets"].Int()
		ap := projClient.NewClient(addr, maxvbs, config)

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

func (k *kvSender) deleteIndexesFromStream(streamId c.StreamId, indexInstList []c.IndexInst) Message {
	err := k.cInfoCache.Fetch()
	if err != nil {
		c.Errorf("KVSender::deleteIndexesFromStream \n\t Error in fetching cluster info", err)
		return &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
	}

	bucket := indexInstList[0].Defn.Bucket
	nodes, _ := k.cInfoCache.GetNodesByBucket(bucket)

	for _, nid := range nodes {

		//get the list of vbnos for this kv
		vbnos, _ := k.cInfoCache.GetVBuckets(nid, bucket)
		if len(vbnos) == 0 {
			continue
		}

		addr, _ := k.cInfoCache.GetServiceAddress(nid, "projector")
		//create client for node's projectors
		config := c.SystemConfig.SectionConfig("projector.client.", true)
		maxvbs := c.SystemConfig["maxVbuckets"].Int()
		ap := projClient.NewClient(addr, maxvbs, config)

		//delete engine(index) from the existing stream
		topic := getTopicForStreamId(streamId)

		var uuids []uint64
		for _, indexInst := range indexInstList {
			uuids = append(uuids, uint64(indexInst.InstId))
		}

		if errMsg := sendDelInstancesRequest(ap, topic, uuids); errMsg.GetMsgType() != MSG_SUCCESS {
			//TODO send message to all KVs to revert the previous requests sent
			return errMsg
		}

	}

	return &MsgSuccess{}
}

func (k *kvSender) deleteBucketsFromStream(streamId c.StreamId, buckets []string) Message {
	err := k.cInfoCache.Fetch()
	if err != nil {
		c.Errorf("KVSender::deleteBucketsFromStream \n\t Error in fetching cluster info", err)
		return &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
	}

	nodes := k.cInfoCache.GetNodesByServiceType("projector")
	for _, nid := range nodes {

		//get the list of vbnos for this kv
		vbnos, _ := k.cInfoCache.GetVBuckets(nid, buckets[0])
		if len(vbnos) == 0 {
			continue
		}

		addr, _ := k.cInfoCache.GetServiceAddress(nid, "projector")
		//create client for node's projectors
		config := c.SystemConfig.SectionConfig("projector.client.", true)
		maxvbs := c.SystemConfig["maxVbuckets"].Int()
		ap := projClient.NewClient(addr, maxvbs, config)

		topic := getTopicForStreamId(streamId)

		if errMsg := sendDelBucketsRequest(ap, topic, buckets); errMsg.GetMsgType() != MSG_SUCCESS {
			//TODO send message to all KVs to revert the previous requests sent
			return errMsg
		}

	}

	return &MsgSuccess{}
}

func (k *kvSender) closeMutationStream(streamId c.StreamId, bucket string) Message {
	err := k.cInfoCache.Fetch()
	if err != nil {
		c.Errorf("KVSender::closeMutationStream \n\t Error in fetching cluster info", err)
		return &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
	}
	var nodes []c.NodeId
	// All projectors
	if bucket == "" {
		nodes = k.cInfoCache.GetNodesByServiceType("projector")
	} else {
		nodes, _ = k.cInfoCache.GetNodesByBucket(bucket)
	}

	for _, nid := range nodes {

		addr, _ := k.cInfoCache.GetServiceAddress(nid, "projector")
		//create client for node's projectors
		config := c.SystemConfig.SectionConfig("projector.client.", true)
		maxvbs := c.SystemConfig["maxVbuckets"].Int()
		ap := projClient.NewClient(addr, maxvbs, config)

		topic := getTopicForStreamId(streamId)
		sendShutdownTopic(ap, topic)
	}

	return &MsgSuccess{}

}

func (k *kvSender) restartVbuckets(streamId c.StreamId, restartTs map[string]*c.TsVbuuid) Message {
	err := k.cInfoCache.Fetch()
	if err != nil {
		c.Errorf("KVSender::restartVbuckets \n\t Error in fetching cluster info", err)
		return &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
	}
	nodes := k.cInfoCache.GetNodesByServiceType("projector")
	rollbackTs := make(map[string]*protobuf.TsVbuuid)

	for _, nid := range nodes {
		addr, _ := k.cInfoCache.GetServiceAddress(nid, "projector")
		//create client for node's projectors
		config := c.SystemConfig.SectionConfig("projector.client.", true)
		maxvbs := c.SystemConfig["maxVbuckets"].Int()
		ap := projClient.NewClient(addr, maxvbs, config)

		topic := getTopicForStreamId(streamId)

		//convert TS to protobuf format
		var protoRestartTs []*protobuf.TsVbuuid
		for _, ts := range restartTs {
			numVbuckets := k.config["numVbuckets"].Int()
			protoTs := protobuf.NewTsVbuuid(DEFAULT_POOL, ts.Bucket, numVbuckets)
			protoRestartTs = append(protoRestartTs, protoTs.FromTsVbuuid(ts))
		}

		if res, errMsg := sendRestartVbuckets(ap, topic, protoRestartTs); errMsg.GetMsgType() != MSG_SUCCESS {
			//TODO send message to all KVs to revert the previous requests sent
			return errMsg
		} else {
			respTsList := res.GetRollbackTimestamps()
			for _, respTs := range respTsList {
				if respTs != nil && !respTs.IsEmpty() {
					if ts, ok := rollbackTs[respTs.GetBucket()]; ok {
						ts.Union(respTs)
					} else {
						rollbackTs[respTs.GetBucket()] = respTs
					}
				}
			}
		}
	}

	if len(rollbackTs) != 0 {
		//convert from protobuf to native format
		nativeTs := make(map[string]*c.TsVbuuid)
		for bucket, ts := range rollbackTs {
			nativeTs[bucket] = ts.ToTsVbuuid()
		}
		return &MsgRollback{streamId: streamId,
			rollbackTs: nativeTs}
	} else {
		return &MsgSuccess{}
	}
}

func (k *kvSender) repairEndpoints(streamId c.StreamId, endpoints []string) Message {
	err := k.cInfoCache.Fetch()
	if err != nil {
		c.Errorf("KVSender::closeMutationStream \n\t Error in fetching cluster info", err)
		return &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
	}

	nodes := k.cInfoCache.GetNodesByServiceType("projector")
	for _, nid := range nodes {
		addr, _ := k.cInfoCache.GetServiceAddress(nid, "projector")
		//create client for node's projectors
		config := c.SystemConfig.SectionConfig("projector.client.", true)
		maxvbs := c.SystemConfig["maxVbuckets"].Int()
		ap := projClient.NewClient(addr, maxvbs, config)

		topic := getTopicForStreamId(streamId)

		if errMsg := sendRepairEndpoints(ap, topic, endpoints); errMsg.GetMsgType() != MSG_SUCCESS {
			return errMsg
		}

	}

	return &MsgSuccess{}
}

//send the actual MutationStreamRequest on adminport
func sendMutationTopicRequest(ap *projClient.Client, topic string,
	reqTimestamps []*protobuf.TsVbuuid,
	instances []*protobuf.Instance) (*protobuf.TopicResponse, Message) {

	c.Debugf("KVSender::sendMutationTopicRequest Projector %v Topic %v Instances %v RequestTS %v",
		ap, topic, instances, reqTimestamps)

	sleepTime := 1
	retry := 0
	endpointType := "dataport"

	for {
		if res, err := ap.MutationTopicRequest(topic, endpointType, reqTimestamps, instances); err != nil {
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
func sendAddBucketsRequest(ap *projClient.Client,
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
func sendAddInstancesRequest(ap *projClient.Client,
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
func sendDelInstancesRequest(ap *projClient.Client,
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
func sendDelBucketsRequest(ap *projClient.Client,
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

func sendRestartVbuckets(ap *projClient.Client,
	topic string,
	restartTs []*protobuf.TsVbuuid) (*protobuf.TopicResponse, Message) {

	c.Debugf("KVSender::sendRestartVbuckets Projector %v Topic %v RestartTs %v",
		ap, topic, restartTs)

	sleepTime := 1
	retry := 0

	for {
		if res, err := ap.RestartVbuckets(topic, restartTs); err != nil {
			if isRetryReqd(err) && retry < MAX_KV_REQUEST_RETRY {
				c.Errorf("KVSender::sendRestartVbuckets \n\tError Connecting to Projector %v. "+
					"Retry in %v seconds... \n\t Error %v ", ap, sleepTime, err)
				time.Sleep(time.Duration(sleepTime) * time.Second)
				sleepTime *= 2
				retry++
			} else {
				c.Errorf("KVSender::sendRestartVbuckets \n\tUnexpected Error During "+
					"Restart Vbuckets Request for Topic %v. Err %v.",
					topic, err)

				return res, &MsgError{
					err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
						severity: FATAL,
						cause:    err}}
			}
		} else {

			c.Debugf("KVSender::sendRestartVbuckets \n\tRestartVbuckets Response %v", res)

			return res, &MsgSuccess{}
		}
	}
}

func sendRepairEndpoints(ap *projClient.Client,
	topic string,
	endpoints []string) Message {

	c.Debugf("KVSender::sendRepairEndpoints Projector %v Topic %v Endpoints %v",
		ap, topic, endpoints)

	sleepTime := 1
	retry := 0

	for {
		if err := ap.RepairEndpoints(topic, endpoints); err != nil {
			if isRetryReqd(err) && retry < MAX_KV_REQUEST_RETRY {
				c.Errorf("KVSender::sendRepairEndpoints \n\tError Connecting to Projector %v. "+
					"Retry in %v seconds... \n\t Err %v", ap, sleepTime, err)
				time.Sleep(time.Duration(sleepTime) * time.Second)
				sleepTime *= 2
				retry++
			} else {
				c.Errorf("KVSender::sendRepairEndpoints \n\tUnexpected Error During "+
					"Repair Endpoints Request Topic %v Endpoints %v. Err %v",
					topic, endpoints, err)

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
func sendShutdownTopic(ap *projClient.Client,
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
		ts.Append(uint16(vbno), tsVbuuid.Snapshots[vbno][1],
			tsVbuuid.Vbuuids[vbno], tsVbuuid.Snapshots[vbno][0],
			tsVbuuid.Snapshots[vbno][1])
	}

	return ts, nil

}

func (k *kvSender) getFailoverLogs(bucket string,
	vbnos []uint32) (*protobuf.FailoverLogResponse, error) {

	var err error
	var res *protobuf.FailoverLogResponse

	//get failover log from any node
	err = k.cInfoCache.Fetch()
	if err != nil {
		err = fmt.Errorf("Error in fetching cluster info %v", err)
		c.Errorf("KVSender::getFailoverLogs \n\t %v", err)
		return nil, err
	}

	nodes := k.cInfoCache.GetNodesByServiceType("projector")
outerloop:
	for _, nid := range nodes {
		addr, _ := k.cInfoCache.GetServiceAddress(nid, "projector")
		//create client for node's projectors
		config := c.SystemConfig.SectionConfig("projector.client.", true)
		maxvbs := c.SystemConfig["maxVbuckets"].Int()
		client := projClient.NewClient(addr, maxvbs, config)
		sleepTime := 1
		retry := 0
	innerloop:
		for {
			if res, err = client.GetFailoverLogs(DEFAULT_POOL, bucket, vbnos); err != nil {
				if isRetryReqd(err) && retry < MAX_KV_REQUEST_RETRY {
					c.Errorf("KVSender::getFailoverLogs \n\tError Connecting to Projector %v. "+
						"Retry in %v seconds... \n\tError Details %v", addr, sleepTime, err)
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

// convert IndexInst to protobuf format
// NOTE: cluster_info.Fetch() should be called before executing this function
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
// NOTE: cluster_info.Fetch() should be called before executing this function
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
