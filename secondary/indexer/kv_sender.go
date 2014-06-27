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
	"github.com/couchbase/indexing/secondary/adminport"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"

	"log"
)

//KVSender provides the mechanism to talk to KV(projector, router etc)
type KVSender interface {
}

type kvSender struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any message to supervisor

	indexInstMap common.IndexInstMap
	streamStatus StreamStatusMap

	numVbuckets uint16
}

func NewKVSender(supvCmdch MsgChannel, supvRespch MsgChannel,
	numVbuckets uint16) (KVSender, Message) {

	//Init the clustMgrSender struct
	k := &kvSender{
		supvCmdch:    supvCmdch,
		supvRespch:   supvRespch,
		streamStatus: make(StreamStatusMap),
		indexInstMap: make(common.IndexInstMap),
		numVbuckets:  numVbuckets,
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
		log.Printf("KVSender: Received Unknown Command %v", cmd)
	}

}

func (k *kvSender) handleCreateIndex(cmd Message) {

	log.Printf("KVSender: Received Create Index %v", cmd)

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

func (k *kvSender) handleDropIndex(cmd Message) {

	log.Printf("KVSender: Received Drop Index %v", cmd)

	indexInstId := cmd.(*MsgDropIndex).GetIndexInstId()

	var indexInst common.IndexInst
	var ok bool

	if indexInst, ok = k.indexInstMap[indexInstId]; !ok {

		log.Printf("KVSender: Unknown IndexInstId %v in Drop Index Request", indexInstId)

		k.supvCmdch <- &MsgError{mType: ERROR,
			err: Error{code: ERROR_DROP_INDEX_FAILED,
				severity: FATAL}}
		return
	}

	using := protobuf.StorageType(
		protobuf.StorageType_value[string(indexInst.Defn.Using)]).Enum()
	exprType := protobuf.ExprType(
		protobuf.ExprType_value[string(indexInst.Defn.ExprType)]).Enum()
	partnScheme := protobuf.PartitionScheme(
		protobuf.PartitionScheme_value[string(indexInst.Defn.PartitionScheme)]).Enum()

	defn := &protobuf.IndexDefn{
		DefnID:          proto.Uint64(uint64(indexInst.Defn.DefnId)),
		Bucket:          proto.String(indexInst.Defn.Bucket),
		IsPrimary:       proto.Bool(indexInst.Defn.IsPrimary),
		Name:            proto.String(indexInst.Defn.Name),
		Using:           using,
		ExprType:        exprType,
		SecExpressions:  indexInst.Defn.OnExprList,
		PartitionScheme: partnScheme,
		PartnExpression: proto.String(indexInst.Defn.PartitionKey),
	}

	state := protobuf.IndexState(int32(indexInst.State)).Enum()
	instance := &protobuf.IndexInst{
		InstId:     proto.Uint64(uint64(indexInst.InstId)),
		State:      state,
		Definition: defn,
	}

	mReq := protobuf.SubscribeStreamRequest{
		Topic:     proto.String(MAINT_TOPIC),
		Instances: []*protobuf.IndexInst{instance},
	}

	mReq.SetDeleteSubscriptionFlag()

	ap := adminport.NewHTTPClient(PROJECTOR_ADMIN_PORT_ENDPOINT, "/adminport/")

	mRes := protobuf.Error{}
	if err := ap.Request(&mReq, &mRes); err != nil {
		log.Printf("Unexpected Error During Mutation Stream Request %v "+
			"for Drop Index %v. Err %v. Resp %v.", mReq, indexInst, err, mRes)

		k.supvCmdch <- &MsgError{mType: ERROR,
			err: Error{code: ERROR_DROP_INDEX_FAILED,
				severity: FATAL,
				cause:    err}}
		return
	}

	delete(k.indexInstMap, indexInstId)

	//if this was the last index in the stream, close it
	if len(k.indexInstMap) == 0 {

		sReq := protobuf.ShutdownStreamRequest{
			Topic: proto.String(MAINT_TOPIC),
		}
		sRes := protobuf.Error{}
		if err := ap.Request(&sReq, &sRes); err != nil {
			log.Printf("Unexpected Error During Close Mutation Stream Request %v "+
				"Err %v. Resp %v.", mReq, indexInst, err, mRes)

			k.supvCmdch <- &MsgError{mType: ERROR,
				err: Error{code: ERROR_DROP_INDEX_FAILED,
					severity: FATAL,
					cause:    err}}
			return
		}
		k.streamStatus[MAINT_STREAM] = false
	}

	k.supvCmdch <- &MsgSuccess{}
}

func (k *kvSender) handleNewMutationStreamRequest(cmd Message) {

	log.Printf("KVSender: handleNewMutationStreamRequest Processing"+
		"Create Index %v", cmd)

	indexInst := cmd.(*MsgCreateIndex).GetIndexInst()

	//TODO the vbNums should be based on the actual vbuckets being
	//served from a projector, for now assume single projector and send
	//list of all vbuckets
	var vbnos []uint32
	for i := 0; i < int(k.numVbuckets); i++ {
		vbnos = append(vbnos, uint32(i))
	}

	fReq := protobuf.FailoverLogRequest{
		Pool:   proto.String(DEFAULT_POOL),
		Bucket: proto.String(indexInst.Defn.Bucket),
		Vbnos:  vbnos,
	}
	fRes := protobuf.FailoverLogResponse{}

	ap := adminport.NewHTTPClient(PROJECTOR_ADMIN_PORT_ENDPOINT, "/adminport/")

	if err := ap.Request(&fReq, &fRes); err != nil {

		log.Printf("Unexpected Error During Failover Log Request %v "+
			"for Create Index %v. Err %v", fReq, indexInst, err)

		k.supvCmdch <- &MsgError{mType: ERROR,
			err: Error{code: ERROR_CREATE_INDEX_FAILED,
				severity: FATAL,
				cause:    err}}

		return
	}
	vbuuids := make([]uint64, 0)
	for _, flog := range fRes.GetLogs() {
		vbuuids = append(vbuuids, flog.Vbuuids[len(flog.Vbuuids)-1])
	}

	//seqnos
	var seqnos []uint64
	for i := 0; i < int(k.numVbuckets); i++ {
		seqnos = append(seqnos, 0)
	}

	bTs := &protobuf.BranchTimestamp{
		Bucket:  proto.String(indexInst.Defn.Bucket),
		Vbnos:   vbnos,
		Seqnos:  seqnos,
		Vbuuids: vbuuids,
	}

	using := protobuf.StorageType(
		protobuf.StorageType_value[string(indexInst.Defn.Using)]).Enum()
	exprType := protobuf.ExprType(
		protobuf.ExprType_value[string(indexInst.Defn.ExprType)]).Enum()
	partnScheme := protobuf.PartitionScheme(
		protobuf.PartitionScheme_value[string(indexInst.Defn.PartitionScheme)]).Enum()

	defn := &protobuf.IndexDefn{
		DefnID:          proto.Uint64(uint64(indexInst.Defn.DefnId)),
		Bucket:          proto.String(indexInst.Defn.Bucket),
		IsPrimary:       proto.Bool(indexInst.Defn.IsPrimary),
		Name:            proto.String(indexInst.Defn.Name),
		Using:           using,
		ExprType:        exprType,
		SecExpressions:  indexInst.Defn.OnExprList,
		PartitionScheme: partnScheme,
		PartnExpression: proto.String(indexInst.Defn.PartitionKey),
	}

	state := protobuf.IndexState(int32(indexInst.State)).Enum()
	instance := &protobuf.IndexInst{
		InstId:     proto.Uint64(uint64(indexInst.InstId)),
		State:      state,
		Definition: defn,
	}

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
		instance.Tp = &protobuf.TestPartition{
			Endpoints: endpoints,
		}
	}

	mReq := protobuf.MutationStreamRequest{
		Topic:             proto.String(MAINT_TOPIC),
		Pools:             []string{DEFAULT_POOL},
		Buckets:           []string{indexInst.Defn.Bucket},
		RestartTimestamps: []*protobuf.BranchTimestamp{bTs},
		Instances:         []*protobuf.IndexInst{instance},
	}

	mReq.SetStartFlag()

	mRes := protobuf.MutationStreamResponse{}
	if err := ap.Request(&mReq, &mRes); err != nil {
		log.Printf("Unexpected Error During Mutation Stream Request %v "+
			"for Create Index %v. Err %v", mReq, indexInst, err)

		k.supvCmdch <- &MsgError{mType: ERROR,
			err: Error{code: ERROR_CREATE_INDEX_FAILED,
				severity: FATAL,
				cause:    err}}
		return
	}

	k.indexInstMap[indexInst.InstId] = indexInst

	k.streamStatus[MAINT_STREAM] = true
	k.supvCmdch <- &MsgSuccess{}

}

func (k *kvSender) handleUpdateMutationStreamRequest(cmd Message) {

	log.Printf("KVSender: handleUpdateMutationStreamRequest Processing"+
		"Create Index %v", cmd)

	indexInst := cmd.(*MsgCreateIndex).GetIndexInst()

	using := protobuf.StorageType(
		protobuf.StorageType_value[string(indexInst.Defn.Using)]).Enum()
	exprType := protobuf.ExprType(
		protobuf.ExprType_value[string(indexInst.Defn.ExprType)]).Enum()
	partnScheme := protobuf.PartitionScheme(
		protobuf.PartitionScheme_value[string(indexInst.Defn.PartitionScheme)]).Enum()

	defn := &protobuf.IndexDefn{
		DefnID:          proto.Uint64(uint64(indexInst.Defn.DefnId)),
		Bucket:          proto.String(indexInst.Defn.Bucket),
		IsPrimary:       proto.Bool(indexInst.Defn.IsPrimary),
		Name:            proto.String(indexInst.Defn.Name),
		Using:           using,
		ExprType:        exprType,
		SecExpressions:  indexInst.Defn.OnExprList,
		PartitionScheme: partnScheme,
		PartnExpression: proto.String(indexInst.Defn.PartitionKey),
	}

	state := protobuf.IndexState(int32(indexInst.State)).Enum()
	instance := &protobuf.IndexInst{
		InstId:     proto.Uint64(uint64(indexInst.InstId)),
		State:      state,
		Definition: defn,
	}

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
		instance.Tp = &protobuf.TestPartition{
			Endpoints: endpoints,
		}
	}

	mReq := protobuf.SubscribeStreamRequest{
		Topic:     proto.String(MAINT_TOPIC),
		Instances: []*protobuf.IndexInst{instance},
	}

	mReq.SetUpdateSubscriptionFlag()

	ap := adminport.NewHTTPClient(PROJECTOR_ADMIN_PORT_ENDPOINT, "/adminport/")

	mRes := protobuf.Error{}
	if err := ap.Request(&mReq, &mRes); err != nil {
		log.Printf("Unexpected Error During Mutation Stream Request %v "+
			"for Create Index %v. Err %v. Resp %v.", mReq, indexInst, err, mRes)

		k.supvCmdch <- &MsgError{mType: ERROR,
			err: Error{code: ERROR_CREATE_INDEX_FAILED,
				severity: FATAL,
				cause:    err}}
		return
	}

	k.indexInstMap[indexInst.InstId] = indexInst

	k.supvCmdch <- &MsgSuccess{}
}
