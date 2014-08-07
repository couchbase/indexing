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

//KVSender provides the mechanism to talk to KV(projector, router etc)
type KVSender interface {
}

//map from bucket name to index count
type BucketIndexCountMap map[string]int

type kvSender struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any message to supervisor

	indexInstMap common.IndexInstMap
	streamStatus StreamStatusMap

	bucketIndexCountMap BucketIndexCountMap

	numVbuckets uint16
}

func NewKVSender(supvCmdch MsgChannel, supvRespch MsgChannel,
	numVbuckets uint16) (KVSender, Message) {

	//Init the kvSender struct
	k := &kvSender{
		supvCmdch:           supvCmdch,
		supvRespch:          supvRespch,
		streamStatus:        make(StreamStatusMap),
		indexInstMap:        make(common.IndexInstMap),
		numVbuckets:         numVbuckets,
		bucketIndexCountMap: make(BucketIndexCountMap),
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
					common.Infof("KVSender: Shutting Down")
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

	case INDEXER_CREATE_INDEX_DDL:
		k.handleCreateIndex(cmd)

	case INDEXER_DROP_INDEX_DDL:
		k.handleDropIndex(cmd)

	default:
		common.Errorf("KVSender: Received Unknown Command %v", cmd)
	}

}

func (k *kvSender) handleCreateIndex(cmd Message) {

	common.Infof("KVSender: Received Create Index %v", cmd)

	var newStreamRequest bool

	if status, _ := k.streamStatus[MAINT_STREAM]; !status {
		newStreamRequest = true
	}

	if newStreamRequest {
		k.handleNewMutationStreamRequest(cmd)
	} else {
		k.handleUpdateMutationStreamRequest(cmd)
	}
}

func (k *kvSender) handleNewMutationStreamRequest(cmd Message) {

	common.Infof("KVSender: handleNewMutationStreamRequest Processing"+
		"Create Index %v", cmd)

	indexInst := cmd.(*MsgCreateIndex).GetIndexInst()

	bTs, err := makeBranchTimestamp(indexInst.Defn.Bucket, k.numVbuckets)

	if err != nil {
		k.supvCmdch <- &MsgError{
			err: Error{code: ERROR_CREATE_INDEX_FAILED,
				severity: FATAL,
				cause:    err}}
		return
	}

	protoDefn := convertIndexDefnToProtobuf(indexInst.Defn)
	protoInst := convertIndexInstToProtobuf(indexInst, protoDefn)

	addPartnInfoToProtoInst(indexInst, protoInst)

	mReq := protobuf.MutationStreamRequest{
		Topic:             proto.String(MAINT_TOPIC),
		Pools:             []string{DEFAULT_POOL},
		Buckets:           []string{indexInst.Defn.Bucket},
		RestartTimestamps: []*protobuf.BranchTimestamp{bTs},
		Instances:         []*protobuf.IndexInst{protoInst},
	}

	mReq.SetStartFlag()

	ap := adminport.NewHTTPClient(PROJECTOR_ADMIN_PORT_ENDPOINT, "/adminport/")
	mRes := protobuf.MutationStreamResponse{}

	common.Debugf("KVSender: MutationStream Request %v", mReq)

	if err := ap.Request(&mReq, &mRes); err != nil {
		common.Errorf("KVSender: Unexpected Error During Mutation Stream Request %v "+
			"for Create Index %v. Err %v", mReq, indexInst, err)

		k.supvCmdch <- &MsgError{
			err: Error{code: ERROR_CREATE_INDEX_FAILED,
				severity: FATAL,
				cause:    err}}
		return
	} else if mRes.GetErr() != nil {
		err := mRes.GetErr()
		common.Errorf("KVSender: Unexpected Error During Mutation Stream Request %v "+
			"for Create Index %v. Err %v", mReq, indexInst, err.GetError())

		k.supvCmdch <- &MsgError{
			err: Error{code: ERROR_CREATE_INDEX_FAILED,
				severity: FATAL,
				cause:    errors.New(err.GetError())}}
		return
	}

	common.Debugf("KVSender: MutationStream Response %v", mRes)

	//update index map with new index
	k.indexInstMap[indexInst.InstId] = indexInst

	//increment index count for this bucket
	k.bucketIndexCountMap[indexInst.Defn.Bucket]++

	k.streamStatus[MAINT_STREAM] = true
	k.supvCmdch <- &MsgSuccess{}

}

func (k *kvSender) handleUpdateMutationStreamRequest(cmd Message) {

	common.Infof("KVSender: handleUpdateMutationStreamRequest Processing"+
		"Create Index %v", cmd)

	indexInst := cmd.(*MsgCreateIndex).GetIndexInst()

	protoDefn := convertIndexDefnToProtobuf(indexInst.Defn)
	protoInst := convertIndexInstToProtobuf(indexInst, protoDefn)

	addPartnInfoToProtoInst(indexInst, protoInst)

	ap := adminport.NewHTTPClient(PROJECTOR_ADMIN_PORT_ENDPOINT, "/adminport/")

	//if this is the first index for this bucket, add new bucket to stream
	if c, ok := k.bucketIndexCountMap[indexInst.Defn.Bucket]; c == 0 || !ok {
		bTs, err := makeBranchTimestamp(indexInst.Defn.Bucket, k.numVbuckets)

		if err != nil {
			k.supvCmdch <- &MsgError{
				err: Error{code: ERROR_CREATE_INDEX_FAILED,
					severity: FATAL,
					cause:    err}}
			return
		}

		mReq := protobuf.UpdateMutationStreamRequest{
			Topic:             proto.String(MAINT_TOPIC),
			Pools:             []string{DEFAULT_POOL},
			Buckets:           []string{indexInst.Defn.Bucket},
			RestartTimestamps: []*protobuf.BranchTimestamp{bTs},
			Instances:         []*protobuf.IndexInst{protoInst},
		}

		mReq.SetAddBucketFlag()

		common.Debugf("KVSender: UpdateMutationStreamRequest %v", mReq)

		mRes := protobuf.MutationStreamResponse{}
		if err := ap.Request(&mReq, &mRes); err != nil {
			common.Errorf("KVSender: Unexpected Error During Mutation Stream Request %v "+
				"for Create Index %v. Err %v. Resp %v.", mReq, indexInst, err, mRes)

			k.supvCmdch <- &MsgError{
				err: Error{code: ERROR_CREATE_INDEX_FAILED,
					severity: FATAL,
					cause:    err}}
			return
		} else if mRes.GetErr() != nil {
			err := mRes.GetErr()
			common.Errorf("KVSender: Unexpected Error During Mutation Stream Request %v "+
				"for Create Index %v. Err %v", mReq, indexInst, err.GetError())

			k.supvCmdch <- &MsgError{
				err: Error{code: ERROR_CREATE_INDEX_FAILED,
					severity: FATAL,
					cause:    errors.New(err.GetError())}}
			return
		}
		common.Debugf("KVSender: MutationStreamResponse %v", mReq)

	} else {
		//add new engine(index) to existing stream
		mReq := protobuf.SubscribeStreamRequest{
			Topic:     proto.String(MAINT_TOPIC),
			Instances: []*protobuf.IndexInst{protoInst},
		}

		mReq.SetAddEnginesFlag()

		common.Debugf("KVSender: SubscribeStreamRequest %v", mReq)

		mRes := protobuf.Error{}
		if err := ap.Request(&mReq, &mRes); err != nil {
			common.Errorf("KVSender: Unexpected Error During Mutation Stream Request %v "+
				"for Create Index %v. Err %v. Resp %v.", mReq, indexInst, err, mRes)

			k.supvCmdch <- &MsgError{
				err: Error{code: ERROR_CREATE_INDEX_FAILED,
					severity: FATAL,
					cause:    err}}
			return
		} else if mRes.GetError() != "" {
			common.Errorf("KVSender: Unexpected Error During Mutation Stream Request %v "+
				"for Create Index %v. Err %v", mReq, indexInst, mRes.GetError())

			k.supvCmdch <- &MsgError{
				err: Error{code: ERROR_CREATE_INDEX_FAILED,
					severity: FATAL,
					cause:    errors.New(mRes.GetError())}}
			return
		}
	}

	//update index map with new index
	k.indexInstMap[indexInst.InstId] = indexInst

	//increment index count for this bucket
	k.bucketIndexCountMap[indexInst.Defn.Bucket]++

	k.supvCmdch <- &MsgSuccess{}
}

func (k *kvSender) handleDropIndex(cmd Message) {

	common.Infof("KVSender: Received Drop Index %v", cmd)

	indexInstId := cmd.(*MsgDropIndex).GetIndexInstId()

	var indexInst common.IndexInst
	var ok bool

	if indexInst, ok = k.indexInstMap[indexInstId]; !ok {

		common.Errorf("KVSender: Unknown IndexInstId %v in Drop Index Request", indexInstId)

		k.supvCmdch <- &MsgError{
			err: Error{code: ERROR_DROP_INDEX_FAILED,
				severity: FATAL}}
		return
	}

	protoDefn := convertIndexDefnToProtobuf(indexInst.Defn)
	protoInst := convertIndexInstToProtobuf(indexInst, protoDefn)

	//delete engine(index) from the existing stream
	mReq := protobuf.SubscribeStreamRequest{
		Topic:     proto.String(MAINT_TOPIC),
		Instances: []*protobuf.IndexInst{protoInst},
	}

	mReq.SetDeleteEnginesFlag()

	common.Debugf("KVSender: SubscribeStreamRequest %v", mReq)

	ap := adminport.NewHTTPClient(PROJECTOR_ADMIN_PORT_ENDPOINT, "/adminport/")

	mRes := protobuf.Error{}
	if err := ap.Request(&mReq, &mRes); err != nil {
		common.Errorf("KVSender: Unexpected Error During Mutation Stream Request %v "+
			"for Drop Index %v. Err %v. Resp %v.", mReq, indexInst, err, mRes)

		k.supvCmdch <- &MsgError{
			err: Error{code: ERROR_DROP_INDEX_FAILED,
				severity: FATAL,
				cause:    err}}
		return
	} else if mRes.GetError() != "" {
		common.Errorf("KVSender: Unexpected Error During Mutation Stream Request %v "+
			"for Drop Index %v. Err %v. Resp %v.", mReq, indexInst, mRes.GetError(), mRes)

		k.supvCmdch <- &MsgError{
			err: Error{code: ERROR_DROP_INDEX_FAILED,
				severity: FATAL,
				cause:    errors.New(mRes.GetError())}}
		return
	}

	delete(k.indexInstMap, indexInstId)
	k.bucketIndexCountMap[indexInst.Defn.Bucket]--

	//if this is the last index for this bucket, delete bucket
	//from the stream
	if c, ok := k.bucketIndexCountMap[indexInst.Defn.Bucket]; c == 0 || !ok {

		mReq := protobuf.UpdateMutationStreamRequest{
			Topic:   proto.String(MAINT_TOPIC),
			Buckets: []string{indexInst.Defn.Bucket},
		}

		mReq.SetDelBucketFlag()

		common.Debugf("KVSender: UpdateMutationStreamRequest %v", mReq)

		mRes := protobuf.MutationStreamResponse{}
		if err := ap.Request(&mReq, &mRes); err != nil {
			common.Errorf("KVSender: Unexpected Error During Mutation Stream Request %v "+
				"for Drop Index %v. Err %v. Resp %v.", mReq, indexInst, err, mRes)

			k.supvCmdch <- &MsgError{
				err: Error{code: ERROR_DROP_INDEX_FAILED,
					severity: FATAL,
					cause:    err}}
			return
		} else if mRes.GetErr() != nil {
			err := mRes.GetErr()
			common.Errorf("KVSender: Unexpected Error During Mutation Stream Request %v "+
				"for Drop Index %v. Err %v. Resp %v.", mReq, indexInst, err.GetError(), mRes)

			k.supvCmdch <- &MsgError{
				err: Error{code: ERROR_DROP_INDEX_FAILED,
					severity: FATAL,
					cause:    errors.New(err.GetError())}}
			return
		}
	}

	//if this was the last index in the stream, close it
	if len(k.indexInstMap) == 0 {

		sReq := protobuf.ShutdownStreamRequest{
			Topic: proto.String(MAINT_TOPIC),
		}
		common.Debugf("KVSender: ShutdownStreamRequest %v", mReq)

		sRes := protobuf.Error{}
		if err := ap.Request(&sReq, &sRes); err != nil {
			common.Errorf("KVSender: Unexpected Error During Close Mutation Stream Request %v "+
				"Err %v. Resp %v.", mReq, err, mRes)

			k.supvCmdch <- &MsgError{
				err: Error{code: ERROR_DROP_INDEX_FAILED,
					severity: FATAL,
					cause:    err}}
			return
		} else if mRes.GetError() != "" {
			common.Errorf("KVSender: Unexpected Error During Close Mutation Stream Request %v "+
				"Err %v. Resp %v.", mReq, mRes.GetError(), mRes)

			k.supvCmdch <- &MsgError{
				err: Error{code: ERROR_DROP_INDEX_FAILED,
					severity: FATAL,
					cause:    errors.New(mRes.GetError())}}
			return
		}
		k.streamStatus[MAINT_STREAM] = false
	}

	k.supvCmdch <- &MsgSuccess{}
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
	protoInst *protobuf.IndexInst) {

	switch partn := indexInst.Pc.(type) {
	case *common.KeyPartitionContainer:

		//Right now the fill the TestPartition as that is the only
		//partition structure supported
		partnDefn := partn.GetAllPartitions()

		var endpoints []string
		for _, p := range partnDefn {
			for _, e := range p.Endpoints() {
				endpoints = append(endpoints, string(e))
			}

		}
		protoInst.Tp = &protobuf.TestPartition{
			Endpoints: endpoints,
		}
	}
}

func makeBranchTimestamp(bucket string, numVbuckets uint16) (*protobuf.BranchTimestamp, error) {

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

	ap := adminport.NewHTTPClient(PROJECTOR_ADMIN_PORT_ENDPOINT, "/adminport/")

	common.Debugf("KVSender: Failover Log Request %v", fReq)

	if err := ap.Request(&fReq, &fRes); err != nil {
		common.Errorf("KVSender: Unexpected Error During Failover Log Request %v "+
			"for Bucket %v. Err %v", fReq, bucket, err)
		return nil, err
	}

	common.Debugf("KVSender: Failover Log Response %v", fRes)

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
