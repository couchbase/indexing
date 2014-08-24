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
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
)

var HTTP_PREFIX string = "http://"

//KVSender provides the mechanism to talk to KV(projector, router etc)
type KVSender interface {
}

type kvSender struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any message to supervisor

	streamStatus              StreamStatusMap
	streamBucketIndexCountMap map[common.StreamId]BucketIndexCountMap

	numVbuckets uint16
}

func NewKVSender(supvCmdch MsgChannel, supvRespch MsgChannel,
	numVbuckets uint16) (KVSender, Message) {

	//Init the kvSender struct
	k := &kvSender{
		supvCmdch:                 supvCmdch,
		supvRespch:                supvRespch,
		streamStatus:              make(StreamStatusMap),
		numVbuckets:               numVbuckets,
		streamBucketIndexCountMap: make(map[common.StreamId]BucketIndexCountMap),
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
					common.Infof("KVSender::run Shutting Down")
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
		common.Errorf("KVSender::handleSupvervisorCommands "+
			"Received Unknown Command %v", cmd)
	}

}

func (k *kvSender) handleOpenStream(cmd Message) {

	common.Infof("KVSender::handleOpenStream %v", cmd)

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

	bTs, err := makeBranchTimestamp(streamId, indexInst.Defn.Bucket, k.numVbuckets)

	if err != nil {
		k.supvCmdch <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
		return
	}

	protoDefn := convertIndexDefnToProtobuf(indexInst.Defn)
	protoInst := convertIndexInstToProtobuf(indexInst, protoDefn)

	addPartnInfoToProtoInst(indexInst, streamId, protoInst)

	topic := getTopicForStreamId(streamId)
	mReq := protobuf.MutationStreamRequest{
		Topic:             proto.String(topic),
		Pools:             []string{DEFAULT_POOL},
		Buckets:           []string{indexInst.Defn.Bucket},
		RestartTimestamps: []*protobuf.BranchTimestamp{bTs},
		Instances:         []*protobuf.IndexInst{protoInst},
	}

	mReq.SetStartFlag()

	ap := adminport.NewHTTPClient(HTTP_PREFIX+PROJECTOR_ADMIN_PORT_ENDPOINT, "/adminport/")
	mRes := protobuf.MutationStreamResponse{}

	common.Debugf("KVSender::handleOpenStream \n\tMutationStream Request %v", mReq)

	if err := ap.Request(&mReq, &mRes); err != nil {
		common.Errorf("KVSender::handleOpenStream \n\tUnexpected Error During Mutation Stream "+
			"Request %v for Create Index %v. Err %v", mReq, indexInst, err)

		k.supvCmdch <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
		return
	} else if mRes.GetErr() != nil {
		err := mRes.GetErr()
		common.Errorf("KVSender::handleOpenStream \n\tUnexpected Error During Mutation Stream "+
			"Request %v for Create Index %v. Err %v", mReq, indexInst, err.GetError())

		k.supvCmdch <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    errors.New(err.GetError())}}
		return
	}

	common.Debugf("KVSender:handleOpenStream \n\tMutationStream Response %v", mRes)

	//increment index count for this bucket
	bucketIndexCountMap := make(BucketIndexCountMap)
	bucketIndexCountMap[indexInst.Defn.Bucket] = 1
	k.streamBucketIndexCountMap[streamId] = bucketIndexCountMap

	k.streamStatus[streamId] = true
	k.supvCmdch <- &MsgSuccess{}

}

func (k *kvSender) handleAddIndexListToStream(cmd Message) {

	common.Debugf("KVSender::handleAddIndexListToStream %v", cmd)

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

	protoDefn := convertIndexDefnToProtobuf(indexInst.Defn)
	protoInst := convertIndexInstToProtobuf(indexInst, protoDefn)

	addPartnInfoToProtoInst(indexInst, streamId, protoInst)

	ap := adminport.NewHTTPClient(HTTP_PREFIX+PROJECTOR_ADMIN_PORT_ENDPOINT, "/adminport/")

	//if this is the first index for this bucket, add new bucket to stream
	if c, ok := k.streamBucketIndexCountMap[streamId][indexInst.Defn.Bucket]; c == 0 || !ok {
		bTs, err := makeBranchTimestamp(streamId, indexInst.Defn.Bucket, k.numVbuckets)

		if err != nil {
			k.supvCmdch <- &MsgError{
				err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
					severity: FATAL,
					cause:    err}}
			return
		}

		topic := getTopicForStreamId(streamId)
		mReq := protobuf.UpdateMutationStreamRequest{
			Topic:             proto.String(topic),
			Pools:             []string{DEFAULT_POOL},
			Buckets:           []string{indexInst.Defn.Bucket},
			RestartTimestamps: []*protobuf.BranchTimestamp{bTs},
			Instances:         []*protobuf.IndexInst{protoInst},
		}

		mReq.SetAddBucketFlag()

		common.Debugf("KVSender::handleAddIndexListToStream \n\tUpdateMutationStreamRequest %v", mReq)

		mRes := protobuf.MutationStreamResponse{}
		if err := ap.Request(&mReq, &mRes); err != nil {
			common.Errorf("KVSender::handleAddIndexListToStream \n\tUnexpected Error During "+
				"Mutation Stream Request %v for IndexInst %v. Err %v. Resp %v.", mReq, indexInst, err, mRes)

			k.supvCmdch <- &MsgError{
				err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
					severity: FATAL,
					cause:    err}}
			return
		} else if mRes.GetErr() != nil {
			err := mRes.GetErr()
			common.Errorf("KVSender::handleAddIndexListToStream \n\tUnexpected Error During "+
				"Mutation Stream Request %v for IndexInst %v. Err %v", mReq, indexInst, err.GetError())

			k.supvCmdch <- &MsgError{
				err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
					severity: FATAL,
					cause:    errors.New(err.GetError())}}
			return
		}
		common.Debugf("KVSender::handleAddIndexListToStream \n\tMutationStreamResponse %v", mReq)

		//increment index count for this bucket
		bucketIndexCountMap := make(BucketIndexCountMap)
		bucketIndexCountMap[indexInst.Defn.Bucket] = 1
		k.streamBucketIndexCountMap[streamId] = bucketIndexCountMap

	} else {
		//add new engine(index) to existing stream
		topic := getTopicForStreamId(streamId)
		mReq := protobuf.SubscribeStreamRequest{
			Topic:     proto.String(topic),
			Instances: []*protobuf.IndexInst{protoInst},
		}

		mReq.SetAddEnginesFlag()

		common.Debugf("KVSender::handleAddIndexListToStream \n\tSubscribeStreamRequest %v", mReq)

		mRes := protobuf.Error{}
		if err := ap.Request(&mReq, &mRes); err != nil {
			common.Errorf("KVSender::handleAddIndexListToStream \n\tUnexpected Error During "+
				"Mutation Stream Request %v for IndexInst %v. Err %v. Resp %v.", mReq, indexInst, err, mRes)

			k.supvCmdch <- &MsgError{
				err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
					severity: FATAL,
					cause:    err}}
			return
		} else if mRes.GetError() != "" {
			common.Errorf("KVSender::handleAddIndexListToStream \n\tUnexpected Error During "+
				"Mutation Stream Request %v for IndexInst %v. Err %v", mReq, indexInst, mRes.GetError())

			k.supvCmdch <- &MsgError{
				err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
					severity: FATAL,
					cause:    errors.New(mRes.GetError())}}
			return
		}
		//increment index count for this bucket
		k.streamBucketIndexCountMap[streamId][indexInst.Defn.Bucket]++
	}

	k.supvCmdch <- &MsgSuccess{}
}

func (k *kvSender) handleRemoveIndexListFromStream(cmd Message) {

	common.Debugf("KVSender::handleRemoveIndexListFromStream %v", cmd)

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

	protoDefn := convertIndexDefnToProtobuf(indexInst.Defn)
	protoInst := convertIndexInstToProtobuf(indexInst, protoDefn)

	//delete engine(index) from the existing stream
	topic := getTopicForStreamId(streamId)
	mReq := protobuf.SubscribeStreamRequest{
		Topic:     proto.String(topic),
		Instances: []*protobuf.IndexInst{protoInst},
	}

	mReq.SetDeleteEnginesFlag()

	common.Debugf("KVSender::handleRemoveIndexListFromStream \n\tSubscribeStreamRequest %v", mReq)

	ap := adminport.NewHTTPClient(HTTP_PREFIX+PROJECTOR_ADMIN_PORT_ENDPOINT, "/adminport/")

	mRes := protobuf.Error{}
	if err := ap.Request(&mReq, &mRes); err != nil {
		common.Errorf("KVSender::handleRemoveIndexListFromStream \n\tUnexpected Error During "+
			"Mutation Stream Request %v for IndexInst %v. Err %v. Resp %v.", mReq, indexInst, err, mRes)

		k.supvCmdch <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
		return
	} else if mRes.GetError() != "" {
		common.Errorf("KVSender::handleRemoveIndexListFromStream \n\tUnexpected Error During "+
			"Mutation Stream Request %v for IndexInst %v. Err %v. Resp %v.", mReq,
			indexInst, mRes.GetError(), mRes)

		k.supvCmdch <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    errors.New(mRes.GetError())}}
		return
	}

	k.streamBucketIndexCountMap[streamId][indexInst.Defn.Bucket]--

	//if this is the last index for this bucket, delete bucket
	//from the stream
	if c, ok := k.streamBucketIndexCountMap[streamId][indexInst.Defn.Bucket]; c == 0 || !ok {

		topic := getTopicForStreamId(streamId)
		mReq := protobuf.UpdateMutationStreamRequest{
			Topic:   proto.String(topic),
			Buckets: []string{indexInst.Defn.Bucket},
		}

		mReq.SetDelBucketFlag()

		common.Debugf("KVSender::handleRemoveIndexListFromStream \n\tUpdateMutationStreamRequest %v", mReq)

		mRes := protobuf.MutationStreamResponse{}
		if err := ap.Request(&mReq, &mRes); err != nil {
			common.Errorf("KVSender::handleRemoveIndexListFromStream \n\tUnexpected Error During "+
				"Mutation Stream Request %v for IndexInst %v. Err %v. Resp %v.", mReq, indexInst, err, mRes)

			k.supvCmdch <- &MsgError{
				err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
					severity: FATAL,
					cause:    err}}
			return
		} else if mRes.GetErr() != nil {
			err := mRes.GetErr()
			common.Errorf("KVSender::handleRemoveIndexListFromStream \n\tUnexpected Error During "+
				"Mutation Stream Request %v for IndexInst %v. Err %v. Resp %v.", mReq,
				indexInst, err.GetError(), mRes)

			k.supvCmdch <- &MsgError{
				err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
					severity: FATAL,
					cause:    errors.New(err.GetError())}}
			return
		}
		//TODO verify this
		delete(k.streamBucketIndexCountMap[streamId], indexInst.Defn.Bucket)
	}

	//if this was the last index in the stream, close it
	if len(k.streamBucketIndexCountMap[streamId]) == 0 {

		topic := getTopicForStreamId(streamId)
		sReq := protobuf.ShutdownStreamRequest{
			Topic: proto.String(topic),
		}
		common.Debugf("KVSender::handleRemoveIndexListFromStream \n\tShutdownStreamRequest %v", mReq)

		sRes := protobuf.Error{}
		if err := ap.Request(&sReq, &sRes); err != nil {
			common.Errorf("KVSender::handleRemoveIndexListFromStream \n\tUnexpected Error During "+
				"Close Mutation Stream Request %v Err %v. Resp %v.", mReq, err, mRes)

			k.supvCmdch <- &MsgError{
				err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
					severity: FATAL,
					cause:    err}}
			return
		} else if mRes.GetError() != "" {
			common.Errorf("KVSender::handleRemoveIndexListFromStream \n\tUnexpected Error During "+
				"Close Mutation Stream Request %v Err %v. Resp %v.", mReq, mRes.GetError(), mRes)

			k.supvCmdch <- &MsgError{
				err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
					severity: FATAL,
					cause:    errors.New(mRes.GetError())}}
			return
		}
		//clean internal maps
		delete(k.streamBucketIndexCountMap, streamId)
		k.streamStatus[streamId] = false
	}

	k.supvCmdch <- &MsgSuccess{}
}

func (k *kvSender) handleCloseStream(cmd Message) {

	common.Infof("KVSender::handleCloseStream %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	//if stream is already closed, return error
	if status, _ := k.streamStatus[streamId]; !status {
		k.supvCmdch <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_ALREADY_CLOSED,
				severity: FATAL}}
		return
	}

	topic := getTopicForStreamId(streamId)

	sReq := protobuf.ShutdownStreamRequest{
		Topic: proto.String(topic),
	}
	common.Debugf("KVSender::handleCloseStream \n\tShutdownStreamRequest %v", sReq)

	ap := adminport.NewHTTPClient(HTTP_PREFIX+PROJECTOR_ADMIN_PORT_ENDPOINT, "/adminport/")
	sRes := protobuf.Error{}
	if err := ap.Request(&sReq, &sRes); err != nil {
		common.Errorf("KVSender::handleCloseStream \n\tUnexpected Error During "+
			"Close Mutation Stream Request %v Err %v. Resp %v.", sReq, err, sRes)

		k.supvCmdch <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    err}}
		return
	} else if sRes.GetError() != "" {
		common.Errorf("KVSender::handleCloseStream \n\tUnexpected Error During Close "+
			"Mutation Stream Request %v Err %v. Resp %v.", sReq, sRes.GetError(), sRes)

		k.supvCmdch <- &MsgError{
			err: Error{code: ERROR_KVSENDER_STREAM_REQUEST_ERROR,
				severity: FATAL,
				cause:    errors.New(sRes.GetError())}}
		return
	}
	//clean internal maps
	delete(k.streamBucketIndexCountMap, streamId)
	k.streamStatus[streamId] = false

}

func convertIndexDefnToProtobuf(indexDefn common.IndexDefn) *protobuf.IndexDefn {

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

func convertIndexInstToProtobuf(indexInst common.IndexInst,
	protoDefn *protobuf.IndexDefn) *protobuf.IndexInst {

	state := protobuf.IndexState(int32(indexInst.State)).Enum()
	instance := &protobuf.IndexInst{
		InstId:     proto.Uint64(uint64(indexInst.InstId)),
		State:      state,
		Definition: protoDefn,
	}
	return instance
}

func addPartnInfoToProtoInst(indexInst common.IndexInst,
	streamId common.StreamId, protoInst *protobuf.IndexInst) {

	switch partn := indexInst.Pc.(type) {
	case *common.KeyPartitionContainer:

		//Right now the fill the TestPartition as that is the only
		//partition structure supported
		partnDefn := partn.GetAllPartitions()

		var endpoints []string
		for _, p := range partnDefn {
			for _, e := range p.Endpoints() {
				//Set the right endpoint based on streamId
				switch streamId {
				case common.MAINT_STREAM:
					e = common.Endpoint(INDEXER_MAINT_DATA_PORT_ENDPOINT)
				case common.INIT_STREAM:
					e = common.Endpoint(INDEXER_INIT_DATA_PORT_ENDPOINT)
				}
				endpoints = append(endpoints, string(e))
			}

		}
		protoInst.Tp = &protobuf.TestPartition{
			Endpoints: endpoints,
		}
	}
}

func makeBranchTimestamp(streamId common.StreamId, bucket string,
	numVbuckets uint16) (*protobuf.BranchTimestamp, error) {

	//TODO the vbNums should be based on the actual vbuckets being
	//served from a projector, for now assume single projector and send
	//list of all vbuckets
	vbnos := make([]uint32, numVbuckets)
	for i := 0; i < int(numVbuckets); i++ {
		vbnos[i] = uint32(i)
	}

	fReq := protobuf.FailoverLogRequest{
		Pool:   proto.String(DEFAULT_POOL),
		Bucket: proto.String(bucket),
		Vbnos:  vbnos,
	}
	fRes := protobuf.FailoverLogResponse{}

	ap := adminport.NewHTTPClient(HTTP_PREFIX+PROJECTOR_ADMIN_PORT_ENDPOINT, "/adminport/")

	common.Debugf("KVSender::makeBranchTimestamp \n\tFailover Log Request %v", fReq)

	if err := ap.Request(&fReq, &fRes); err != nil {
		common.Errorf("KVSender::makeBranchTimestamp \n\tUnexpected Error During Failover "+
			"Log Request %v for Bucket %v. Err %v", fReq, bucket, err)
		return nil, err
	}

	common.Debugf("KVSender::makeBranchTimestamp \n\tFailover Log Response %v", fRes)

	vbuuids := make(map[uint32]uint64)
	for _, flog := range fRes.GetLogs() {
		vbno := uint32(flog.GetVbno())
		vbuuid := flog.Vbuuids[len(flog.Vbuuids)-1]
		vbuuids[vbno] = vbuuid
	}

	vbuuidsSorted := make([]uint64, numVbuckets)
	for i, vbno := range vbnos {
		vbuuidsSorted[i] = vbuuids[vbno]
	}

	seqnos := make([]uint64, numVbuckets)

	bTs := &protobuf.BranchTimestamp{
		Bucket:  proto.String(bucket),
		Vbnos:   vbnos,
		Seqnos:  seqnos,
		Vbuuids: vbuuidsSorted,
	}

	return bTs, nil
}

func (k *kvSender) handleGetCurrKVTimestamp(cmd Message) {

	//TODO For now Indexer is getting the TS directly from
	//KV. Once Projector API is ready, use that.

}

func getTopicForStreamId(streamId common.StreamId) string {

	var topic string

	switch streamId {
	case common.MAINT_STREAM:
		topic = MAINT_TOPIC
	case common.INIT_STREAM:
		topic = INIT_TOPIC
	}

	return topic
}
