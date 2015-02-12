// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package manager

import (
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dataport"
	data "github.com/couchbase/indexing/secondary/protobuf/data"
)

///////////////////////////////////////////////////////
// Type Definition
///////////////////////////////////////////////////////

//
// Stream represents a specific flow of mutations for consumption.  There are 3 types of stream:
// 1) Incremental Stream for live mutation update.   This is primarily used for index maintenance.
// 2) Init Stream for initial index build.   This is essentially a backfill stream.
// 3) Catch-up Stream is a dedicated stream for each index node.  This is used when indexer is in recovery
//      or being slow.  So catch-up stream allows independent flow control for the specific node.
// A stream aggregates mutations across all buckets as well as all vbuckets.   All the KV nodes will send
// the mutation through the stream.   The mutation itself (VbKeyVersions) has metadata to differentiate the
// origination of the mutation (bucket, vbucket, vbuuid).
//
type Stream struct {
	id common.StreamId

	// struct member for book keeping
	status bool

	// struct member for data streaming
	hostStr  string
	receiver *dataport.Server
	mutch    chan interface{} //channel for mutations sent by Dataport

	// struct member for handling stream mutation
	handler MutationHandler

	// struct member for admin
	stopch chan bool
}

///////////////////////////////////////////////////////
// private function - Stream
///////////////////////////////////////////////////////

func newStream(id common.StreamId, hostStr string, handler MutationHandler) (*Stream, error) {

	// TODO: use constant
	mutch := make(chan interface{}, 1000)
	stopch := make(chan bool)

	s := &Stream{id: id,
		hostStr: hostStr,
		handler: handler,
		mutch:   mutch,
		stopch:  stopch,
		status:  false}

	return s, nil
}

func (s *Stream) getEndpoint() string {
	return s.hostStr
}

func (s *Stream) start() (err error) {
	config := common.SystemConfig.SectionConfig("indexer.dataport.", true)
	maxvbs := common.SystemConfig["maxVbuckets"].Int()
	s.receiver, err = dataport.NewServer(s.hostStr, maxvbs, config, s.mutch)
	if err != nil {
		logging.Errorf("StreamManager: Error returned from dataport.NewServer = %s.", err.Error())
		close(s.stopch)
		return err
	}
	logging.Debugf("Stream.run(): dataport server started on addr %s", s.hostStr)

	// Start the listening go-routine.
	go s.run()

	// start dataport stream
	return nil
}

func (s *Stream) Close() {
	close(s.stopch)
}

func (s *Stream) run() {

	logging.Debugf("Stream.run(): starts")

	defer s.receiver.Close()

	for {
		select {
		case mut := <-s.mutch:

			func() {
				defer func() {
					if r := recover(); r != nil {
						logging.Debugf("panic in Stream.run() : error ignored.  Error = %v\n", r)
					}
				}()

				switch d := mut.(type) {
				case ([]*data.VbKeyVersions):
					logging.Debugf("Stream.run(): recieve VbKeyVersion")
					s.handleVbKeyVersions(d)
				case dataport.ConnectionError:
					logging.Debugf("Stream.run(): recieve ConnectionError")
					s.handler.HandleConnectionError(s.id, d)
				}
			}()

		case <-s.stopch:
			logging.Debugf("Stream.run(): stop")
			return
		}
	}
}

/*
message VbKeyVersions {
    required uint32      vbucket    = 2; // 16 bit vbucket in which document is located
    required uint64      vbuuid     = 3; // unique id to detect branch history
    optional string      bucketname = 4;
    repeated KeyVersions kvs        = 5; // list of key-versions
}
*/
func (s *Stream) handleVbKeyVersions(vbKeyVers []*data.VbKeyVersions) {
	for _, vb := range vbKeyVers {
		s.handleKeyVersions(vb.GetBucketname(), vb.GetVbucket(),
			vb.GetVbuuid(), vb.GetKvs())
	}
}

func (s *Stream) handleKeyVersions(bucket string,
	vbucket uint32,
	vbuuid uint64,
	kvs []*data.KeyVersions) {
	for _, kv := range kvs {
		s.handleSingleKeyVersion(bucket, vbucket, vbuuid, kv)
	}
}

/*
message KeyVersions {
    required uint64 seqno    = 1; // sequence number corresponding to this mutation
    optional bytes  docid    = 2; // primary document id
    repeated uint64 uuids    = 3; // uuids, hosting key-version
    repeated uint32 commands = 4; // list of command for each uuid
    repeated bytes  keys     = 5; // key-versions for each uuids listed above
    repeated bytes  oldkeys  = 6; // key-versions from old copy of the document
    repeated bytes  partnkeys = 7; // partition key for each key-version
}
*/
func (s *Stream) handleSingleKeyVersion(bucket string,
	vbucket uint32,
	vbuuid uint64,
	kv *data.KeyVersions) {

	for i, cmd := range kv.GetCommands() {
		logging.Debugf("Stream.handleSingleKeyVersion(): recieve command %v", cmd)
		switch byte(cmd) {
		case common.Upsert:
			s.handler.HandleUpsert(s.id, bucket, vbucket, vbuuid, kv, i)
		case common.Deletion:
			s.handler.HandleDeletion(s.id, bucket, vbucket, vbuuid, kv, i)
		case common.UpsertDeletion:
			s.handler.HandleUpsertDeletion(s.id, bucket, vbucket, vbuuid, kv, i)
		case common.Sync:
			s.handler.HandleSync(s.id, bucket, vbucket, vbuuid, kv, i)
		case common.DropData:
			s.handler.HandleDropData(s.id, bucket, vbucket, vbuuid, kv, i)
		case common.StreamBegin:
			s.handler.HandleStreamBegin(s.id, bucket, vbucket, vbuuid, kv, i)
		case common.StreamEnd:
			s.handler.HandleStreamEnd(s.id, bucket, vbucket, vbuuid, kv, i)
		case common.Snapshot:
			s.handler.HandleSnapshot(s.id, bucket, vbucket, vbuuid, kv, i)
		}
	}
}
