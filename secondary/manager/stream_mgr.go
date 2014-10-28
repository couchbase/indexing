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
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dataport"
	"github.com/couchbase/indexing/secondary/protobuf"
	couchbase "github.com/couchbaselabs/go-couchbase"
	"net"
	"sync"
)

///////////////////////////////////////////////////////
// Type Definition
///////////////////////////////////////////////////////

/*
	Upsert         byte = iota + 1 // data command
	Deletion                       // data command
	UpsertDeletion                 // data command
	Sync                           // control command
	DropData                       // control command
	StreamBegin                    // control command
	StreamEnd                      // control command
	Snapshot                       // control command
*/
type MutationHandler interface {
	HandleUpsert(streamId common.StreamId, bucket string, vbucket uint32, vbuuid uint64, kv *protobuf.KeyVersions, offset int)
	HandleDeletion(streamId common.StreamId, bucket string, vbucket uint32, vbuuid uint64, kv *protobuf.KeyVersions, offset int)
	HandleUpsertDeletion(streamId common.StreamId, bucket string, vbucket uint32, vbuuid uint64, kv *protobuf.KeyVersions, offset int)
	HandleSync(streamId common.StreamId, bucket string, vbucket uint32, vbuuid uint64, kv *protobuf.KeyVersions, offset int)
	HandleDropData(streamId common.StreamId, bucket string, vbucket uint32, vbuuid uint64, kv *protobuf.KeyVersions, offset int)
	HandleStreamBegin(streamId common.StreamId, bucket string, vbucket uint32, vbuuid uint64, kv *protobuf.KeyVersions, offset int)
	HandleStreamEnd(streamId common.StreamId, bucket string, vbucket uint32, vbuuid uint64, kv *protobuf.KeyVersions, offset int)
	HandleSnapshot(streamId common.StreamId, bucket string, vbucket uint32, vbuuid uint64, kv *protobuf.KeyVersions, offset int)
	HandleConnectionError(streamId common.StreamId, err dataport.ConnectionError)
}

type StreamAdmin interface {
	OpenStreamForBucket(streamId common.StreamId, bucket string, topology []*protobuf.Instance, requestTs *common.TsVbuuid) error
	RepairStreamForEndpoint(streamId common.StreamId, bucketVbnosMap map[string][]uint16, endpoint string) error
	AddIndexToStream(streamId common.StreamId, bucket string, instances []*protobuf.Instance) error
	DeleteIndexFromStream(streamId common.StreamId, bucket string, instances []uint64) error
}

type StreamManager struct {
	streams  map[common.StreamId]*Stream
	handler  MutationHandler
	admin    StreamAdmin
	indexMgr *IndexManager

	mutex    sync.Mutex
	isClosed bool
}

type Stream struct {
	id common.StreamId

	// struct member for book keeping
	status        bool
	indexCountMap map[string]int // key : bucket, value : index count (per bucket)

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
// public function - Stream Manager
///////////////////////////////////////////////////////

//
// Create new stream managaer
//
func NewStreamManager(indexMgr *IndexManager, handler MutationHandler, admin StreamAdmin) (*StreamManager, error) {

	streams := make(map[common.StreamId]*Stream)
	mgr := &StreamManager{streams: streams,
		handler:  handler,
		indexMgr: indexMgr,
		admin:    admin,
		isClosed: false}

	return mgr, nil
}

//
// Close all the streams
//
func (s *StreamManager) Close() {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.isClosed {
		return
	}

	for name, stream := range s.streams {
		stream.Close()
		s.closeStreamNoLock(stream.id)
		delete(s.streams, name)
	}

	s.isClosed = true
}

//
// Is all the stream closed?
//
func (s *StreamManager) IsClosed() bool {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.isClosed
}

//
// Get the stream
//
func (s *StreamManager) getStream(streamId common.StreamId) *Stream {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.streams[streamId]
}

//
// Start a stream
//
func (s *StreamManager) StartStream(streamId common.StreamId, port string) error {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.isClosed {
		return nil
	}

	// Verify if the stream is already open
	if stream, ok := s.streams[streamId]; ok && stream.status {
		return NewError2(ERROR_STREAM_ALREADY_OPEN, STREAM)
	}

	// Create a new stream.  This will prepare the reciever to be ready for receving mutation.
	stream, err := newStream(streamId, net.JoinHostPort(LOCALHOST, port), s.handler)
	if err != nil {
		return err
	}

	s.streams[streamId] = stream
	stream.start()

	return nil
}

//
// Start Streaming for specific bucket
//
func (s *StreamManager) OpenStreamForBucket(streamId common.StreamId, bucket string, port string) error {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.isClosed {
		return nil
	}

	stream, ok := s.streams[streamId]
	if !ok || stream.status {
		return NewError2(ERROR_STREAM_NOT_OPEN, STREAM)
	}

	if _, ok := stream.indexCountMap[bucket]; ok {
		return NewError2(ERROR_STREAM_BUCKET_ALREADY_OPEN, STREAM)
	}

	// Start the timer before start the stream.  Once the stream comes, the timer needs to be ready.
	s.indexMgr.getTimer().start(streamId, bucket)

	// Genereate the index instance protobuf messages based on distribution topology
	instances, err := GetTopologyAsInstanceProtoMsg(s.indexMgr, bucket, port)
	if err != nil {
		return err
	}

	// Open the mutation stream for the specific bucket
	if err = s.admin.OpenStreamForBucket(streamId, bucket, instances, nil); err != nil {
		return err
	}

	// Just book keeping
	stream.indexCountMap[bucket] = len(instances)
	stream.status = true

	return nil
}

//
// Open stream for all the buckets
//
func (s *StreamManager) OpenStreamForAllBuckets(streamId common.StreamId) error {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.isClosed {
		return nil
	}

	client, err := couchbase.Connect(COUCHBASE_INTERNAL_BUCKET_URL)
	if err != nil {
		return err
	}

	pool, err := client.GetPool(COUCHBASE_DEFAULT_POOL_NAME)
	if err != nil {
		return err
	}

	port := getPortForStreamId(streamId)
	for bucket, _ := range pool.BucketMap {
		if err := s.OpenStreamForBucket(streamId, bucket, port); err != nil {
			// TODO: We may get error saying that the stream has already been opened.  Need to
			// figure out the error code from projector.
			return err
		}
	}

	return nil
}

//
// Close a stream
//
func (s *StreamManager) CloseStream(streamId common.StreamId) error {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.closeStreamNoLock(streamId)
}

//
// Add an index to a stream
//
func (s *StreamManager) AddIndexToStream(streamId common.StreamId, bucket string, indexId common.IndexDefnId, port string) error {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.isClosed {
		return nil
	}

	//if stream not open, return error
	stream, ok := s.streams[streamId]
	if !ok || !stream.status {
		return NewError2(ERROR_STREAM_NOT_OPEN, STREAM)
	}

	// Re-generate the full distribution topology as protobuf message
	instances, err := GetIndexInstanceAsProtoMsg(s.indexMgr, bucket, indexId, port)
	if err != nil {
		return err
	}

	// Pass the new topology to the projector
	if err := s.admin.AddIndexToStream(streamId, bucket, instances); err != nil {
		return err
	}

	// book keeping
	stream.indexCountMap[bucket] = len(instances)

	return nil
}

//
// Remove an index from a stream
//
func (s *StreamManager) RemoveIndexFromStream(streamId common.StreamId, bucket string, indexId common.IndexDefnId) error {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.isClosed {
		return nil
	}

	//if stream not open, return error
	stream, ok := s.streams[streamId]
	if !ok || !stream.status {
		return NewError2(ERROR_STREAM_NOT_OPEN, STREAM)
	}

	// Find out the index instances for this index definition
	instances, err := GetIndexInstancesByDefn(s.indexMgr, bucket, indexId)
	if err != nil {
		return err
	}

	// Remove those index instances from the stream
	if err := s.admin.DeleteIndexFromStream(streamId, bucket, instances); err != nil {
		return err
	}

	// bookkeeping
	// TODO: Projector call is not atomic, so it is possible that "Some" instances are closed.  So
	// the count is not going to be reliable.
	stream.indexCountMap[bucket] = stream.indexCountMap[bucket] - len(instances)

	return nil
}

///////////////////////////////////////////////////////
// private function - StreamManager
///////////////////////////////////////////////////////

func (s *StreamManager) closeStreamNoLock(streamId common.StreamId) error {

	if s.isClosed {
		return nil
	}

	//if stream not open, return error
	stream, ok := s.streams[streamId]
	if !ok || !stream.status {
		return NewError2(ERROR_STREAM_NOT_OPEN, STREAM)
	}

	// Start the timer before start the stream.  Once the stream comes, the timer needs to be ready.
	s.indexMgr.getTimer().stopForStream(streamId)

	/*
		if err := CloseStreamForBucket(streamId); err != nil {
			return err
		}
	*/

	// book keeping
	stream.indexCountMap = make(map[string]int)
	stream.status = false

	return nil
}

///////////////////////////////////////////////////////
// private function - Stream
///////////////////////////////////////////////////////

func newStream(id common.StreamId, hostStr string, handler MutationHandler) (*Stream, error) {

	mutch := make(chan interface{})
	stopch := make(chan bool)

	s := &Stream{id: id,
		hostStr:       hostStr,
		handler:       handler,
		mutch:         mutch,
		stopch:        stopch,
		status:        false,
		indexCountMap: make(map[string]int)}

	return s, nil
}

func (s *Stream) getEndpoint() string {
	return s.hostStr
}

func (s *Stream) start() (err error) {

	// start the listening go-routine
	go s.run()

	// start dataport stream
	if s.receiver, err = dataport.NewServer(s.hostStr, common.SystemConfig, s.mutch); err != nil {
		common.Errorf("StreamManager: Error returned from dataport.NewServer = %s.", err.Error())
		return err
	}
	common.Debugf("Stream.run(): dataport server started on addr %s", s.hostStr)

	return nil
}

func (s *Stream) Close() {
	close(s.stopch)
}

func (s *Stream) run() {

	common.Debugf("Stream.run(): starts")

	defer s.receiver.Close()

	for {
		select {
		case mut := <-s.mutch:
			switch d := mut.(type) {
			case ([]*protobuf.VbKeyVersions):
				common.Debugf("Stream.run(): recieve VbKeyVersion")
				s.handleVbKeyVersions(d)
			case dataport.ConnectionError:
				common.Debugf("Stream.run(): recieve ConnectionError")
				s.handler.HandleConnectionError(s.id, d)
			}
		case <-s.stopch:
			common.Debugf("Stream.run(): stop")
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
func (s *Stream) handleVbKeyVersions(vbKeyVers []*protobuf.VbKeyVersions) {
	for _, vb := range vbKeyVers {
		s.handleKeyVersions(vb.GetBucketname(), vb.GetVbucket(),
			vb.GetVbuuid(), vb.GetKvs())
	}
}

func (s *Stream) handleKeyVersions(bucket string,
	vbucket uint32,
	vbuuid uint64,
	kvs []*protobuf.KeyVersions) {
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
	kv *protobuf.KeyVersions) {

	for i, cmd := range kv.GetCommands() {
		common.Debugf("Stream.handleSingleKeyVersion(): recieve command %v", cmd)
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
