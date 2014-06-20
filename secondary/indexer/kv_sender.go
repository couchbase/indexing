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
)

//KVSender provides the mechanism to talk to KV(projector, router etc)
type KVSender interface {
}

type kvSender struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any message to supervisor

	indexInstMap common.IndexInstMap
	streamStatus StreamStatusMap
}

//TODO move to config
const ADMIN_PORT_ENDPOINT = "http://localhost:9100"
const MAINT_TOPIC = "MAINT_STREAM_TOPIC"

func NewKVSender(supvCmdch MsgChannel, supvRespch MsgChannel) (
	KVSender, Message) {

	//Init the clustMgrSender struct
	k := &kvSender{
		supvCmdch:    supvCmdch,
		supvRespch:   supvRespch,
		streamStatus: make(StreamStatusMap),
		indexInstMap: make(common.IndexInstMap),
	}

	//start kvsender loop which listens to commands from its supervisor
	go k.run()

	return k, nil

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

		indexInst := cmd.(*MsgCreateIndex).GetIndexInst()

		k.indexInstMap[indexInst.InstId] = indexInst

		var newStreamRequest bool

		if status, ok := k.streamStatus[MAINT_STREAM]; !status {
			newStreamRequest = true
		}

		bTs := &protobuf.BranchTimestamp{
			Bucket: indexInst.Defn.Bucket,
		}

		defn := &protobuf.IndexDefn{
			DefnID:          indexInst.Defn.DefnId,
			Bucket:          proto.String(indexInst.Defn.Bucket),
			IsPrimary:       indexInst.Defn.IsPrimary,
			Name:            indexInst.Defn.Name,
			Using:           indexInst.Defn.Using,
			ExprType:        indexInst.Defn.Exprtype,
			SecExpressions:  indexInst.Defn.OnExprList,
			PartnExpression: indexInst.Defn.PartitionKey,
		}

		instance := &protobuf.IndexInst{
			InstId:     indexInst.InstId,
			State:      indexInst.State,
			Definition: defn,
			Tp:         indexInst.Pc,
		}

		//TODO How to set the flags field
		req := protobuf.MutationStreamRequest{
			Topic:             proto.String(MAINT_TOPIC),
			Pools:             []string{"default"},
			Buckets:           indexInst.Defn.Bucket,
			RestartTimestamps: []*protobuf.BranchTimestamp{bTs},
			Instances:         []*protobuf.IndexInst{instance},
		}

		ap := adminport.NewHTTPClient(ADMIN_PORT_ENDPOINT, "/adminport/")
		res := protobuf.MutationStreamResponse{}
		if err := ap.Request(req, &res); err != nil {
			k.supvCmdch <- err
		}

		if newStreamRequest {
			k.streamStatus[MAINT_STREAM] = true
		}

		k.supvCmdch <- &MsgSuccess{}

	case INDEXER_DROP_INDEX_DDL:

		//TODO

	}

}
