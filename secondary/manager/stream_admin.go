// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package manager

import (
	"github.com/couchbase/indexing/secondary/common"
	projector "github.com/couchbase/indexing/secondary/projector/client"
	"github.com/couchbase/indexing/secondary/protobuf"
	couchbase "github.com/couchbaselabs/go-couchbase"
	"net"
	"strconv"
)

/////////////////////////////////////////////////////////////////////////
// Type Definition
/////////////////////////////////////////////////////////////////////////

type VbMap map[string][]uint16

type ProjectorAdmin struct {
}

/////////////////////////////////////////////////////////////////////////
// Public Function
/////////////////////////////////////////////////////////////////////////

func newProjectorAdmin() *ProjectorAdmin {
	return new(ProjectorAdmin)
}

//
// Start a new stream for the current index topology
//
func (p *ProjectorAdmin) OpenStreamForBucket(streamId common.StreamId,
	bucket string,
	topology []*protobuf.Instance,
	requestTs *common.TsVbuuid) error {

	common.Infof("StreamAdmin::OpenStreamRequestForBucket(): streamId=%d, bucket=%s", streamId.String(), bucket)

	// Get the vbmap
	vbMap, err := getVbMap(bucket)
	if err != nil {
		return err
	}

	// For all the nodes in vbmap, start a stream
	for server, vbnos := range vbMap {

		//get projector client for the particular node
		client := getClientForNode(server)

		ts, err := makeRestartTimestamp(client, bucket, vbnos, requestTs)
		if err != nil {
			return NewError(ERROR_STREAM_REQUEST_ERROR, NORMAL, STREAM, err, "")
		}

		topic := getTopicForStreamId(streamId)
		response, err := client.MutationTopicRequest(topic, "dataport", []*protobuf.TsVbuuid{ts}, topology)
		if err != nil {
			// Encounter an error.  For those projectors that have already been opened, let's leave it open.
			// Eventually those projectors will fill up the buffer and terminate the connection by itself.
			return err

		} else if err := response.GetErr(); err != nil {

			// TODO: We may get error saying that the stream has already been opened.  Need to
			// figure out the error code from projector.
			
			// Encounter an error.  For those projectors that have already been opened, let's leave it open.
			// Eventually those projectors will fill up the buffer and terminate the connection by itself.
			common.Errorf("streamProxy::OpenStreamRequestForBucket(): Error encountered when sending adminport request. Error=%v",
				err.GetError())

			return NewError(ERROR_STREAM_REQUEST_ERROR, NORMAL, STREAM, nil, err.GetError())

		}

		// Projector may reject the restart timestamp for some vbucket.  In this case, will need to restart those
		// vb using the timestamp provided by the projector.
		if err := restartVBIfNecessary(client, topic, bucket, "dataport", topology,
			response.GetRollbackTimestamps(), 0); err != nil {
			// Encounter an error.  For those projectors that have already been opened, let's leave it open.
			// Eventually those projectors will fill up the buffer and terminate the connection by itself.
			return err
		}
	}

	return nil
}

//
// Repair the stream by asking the provider to reconnect to the list of endpoints.
// Once connected, the provider will stream mutations from the current vbucket seqno.
// In other words, the provider will not reset the seqno.
//
func (p *ProjectorAdmin) RepairStreamForEndpoint(streamId common.StreamId,
	bucketVbnosMap map[string][]uint16,
	endpoint string) error {

	common.Infof("StreamAdmin::RepairStreamForEndpoint(): streamId = %d", streamId.String())

	serverList := make(map[string]string)

	for bucket, repairVbnos := range bucketVbnosMap {
		if len(repairVbnos) > 0 {

			// Get the vbmap
			vbMap, err := getVbMap(bucket)
			if err != nil {
				return err
			}

			// from the bucket vbmap, find the server that contain vb that requires repair
			for server, serverVbnos := range vbMap {
				intersect := common.Intersection(repairVbnos, serverVbnos)
				if intersect != nil && len(intersect) > 0 {
					serverList[server] = server
				}
			}
		}
	}

	// Now given the list of server, call each one of them to repair endpoint.
	if serverList != nil && len(serverList) > 0 {

		topic := getTopicForStreamId(streamId)

		for server, _ := range serverList {
			//get projector client for the particular node
			client := getClientForNode(server)

			// call projector to repair endpoint
			if err := client.RepairEndpoints(topic, []string{endpoint}); err != nil {
				// TODO: It is expected that the projector will return an error if the
				// stream has not yet started -- this will indicate that the projector
				// may have terminated unexpectedly.
				return err
			}
		}
	}

	return nil
}

//
// Close a stream
//
/*
func CloseStreamFor(streamId StreamId) error {

	common.Infof("StreamAdmin::CloseStream(): streamId = %d, bucket = %s", streamId.String(), bucket)

	// get the vbmap
	vbMap, err := getVbMap(bucket)
	if err != nil {
		return err
	}

	// For all the nodes in vbmap, start a stream
	for server, vbnos := range vbMap {

		//get projector client for the particular node
		client := getClientForNode(server)

		topic := getTopicForStreamId(streamId)

		if err := client.ShutdownTopic(topic); err != nil {
			return err
		}
	}

	return nil
}
*/

//
// Add Index to stream
//
func (p *ProjectorAdmin) AddIndexToStream(streamId common.StreamId, bucket string, instances []*protobuf.Instance) error {

	common.Infof("StreamAdmin::AddIndexToStream(): streamId=%d, bucket=%s", streamId.String(), bucket)

	// Get the vbmap
	vbMap, err := getVbMap(bucket)
	if err != nil {
		return err
	}

	// For all the nodes in vbmap, start a stream
	for server, _ := range vbMap {

		//get projector client for the particular node
		client := getClientForNode(server)

		topic := getTopicForStreamId(streamId)
		if err := client.AddInstances(topic, instances); err != nil {
			// Encounter an error.  For those projectors that have already been opened, let's leave it open.
			// Eventually those projectors will fill up the buffer and terminate the connection by itself.
			return err
		}
	}

	return nil
}

//
// Delete Index to stream
//
func (p *ProjectorAdmin) DeleteIndexFromStream(streamId common.StreamId, bucket string, instances []uint64) error {

	common.Infof("StreamAdmin::DeleteIndexFromStream(): streamId=%d, bucket=%s", streamId.String(), bucket)

	// Get the vbmap
	vbMap, err := getVbMap(bucket)
	if err != nil {
		return err
	}

	// For all the nodes in vbmap, start a stream
	for server, _ := range vbMap {

		//get projector client for the particular node
		client := getClientForNode(server)

		topic := getTopicForStreamId(streamId)
		if err := client.DelInstances(topic, instances); err != nil {
			// Encounter an error.  For those projectors that have already been opened, let's leave it open.
			// Eventually those projectors will fill up the buffer and terminate the connection by itself.
			return err
		}
	}

	return nil
}

/////////////////////////////////////////////////////////////////////////
// Private Function - Messaging Support Function
/////////////////////////////////////////////////////////////////////////

func restartVBIfNecessary(client *projector.Client,
	topic string,
	bucket string,
	setting string,
	topology []*protobuf.Instance,
	rollbackTimestamps []*protobuf.TsVbuuid,
	counter int) error {

	if len(rollbackTimestamps) != 0 {
		for _, rollbackTs := range rollbackTimestamps {
			if rollbackTs.GetBucket() == bucket {

				result := protobuf.NewTsVbuuid(COUCHBASE_DEFAULT_POOL_NAME, bucket, 1024)
				for i, vbno := range rollbackTs.GetVbnos() {
					result.Vbnos[vbno] = vbno
					result.Seqnos[vbno] = rollbackTs.GetSeqnos()[i]
					result.Vbuuids[vbno] = rollbackTs.GetVbuuids()[i]
				}

				response, err := client.MutationTopicRequest(topic, setting, []*protobuf.TsVbuuid{result}, topology)
				if err != nil && counter < MAX_TOPIC_REQUEST_RETRY_COUNT {
					return restartVBIfNecessary(client, topic, bucket, setting, topology,
						response.GetRollbackTimestamps(), counter+1)
				}
			}
		}
	}

	return nil
}

/////////////////////////////////////////////////////////////////////////
// Private Function
/////////////////////////////////////////////////////////////////////////

//
// Get the VB Map
//
func getVbMap(bucket string) (VbMap, error) {

	// TODO: figure out security in a trusted domain
	bucketRef, err := couchbase.GetBucket(COUCHBASE_INTERNAL_BUCKET_URL, COUCHBASE_DEFAULT_POOL_NAME, bucket)
	if err != nil {
		return nil, err
	}
	defer bucketRef.Close()

	bucketRef.Refresh()
	result, err := bucketRef.GetVBmap(nil)

	return VbMap(result), err
}

//
// Get the projector client for the given node
//
func getClientForNode(server string) *projector.Client {

	var projAddr string

	if host, port, err := net.SplitHostPort(server); err == nil {
		if common.IsIPLocal(host) {

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
			common.Debugf("StreamAdmin::getClientForNode(): Local Projector Addr: %v", projAddr)

		} else {
			projAddr = host + ":" + PROJECTOR_PORT
			common.Debugf("StreamAdmin::getClientForNode(): Remote Projector Addr: %v", projAddr)
		}
	}

	//create client for node's projectors
	ap := projector.NewClient(HTTP_PREFIX+projAddr+"/adminport/", common.SystemConfig)
	return ap
}

//
// Convert StreamId into topic string
//
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

//
// Convert StreamId into port
//
func getPortForStreamId(streamId common.StreamId) string {

	var port string

	switch streamId {
	case common.MAINT_STREAM:
		port = COORD_MAINT_STREAM_PORT
	case common.INIT_STREAM:
		port = COORD_INIT_STREAM_PORT
	}

	return port
}

//
// Create the restart timetamp
//
func makeRestartTimestamp(client *projector.Client,
	bucket string,
	vbnos []uint16,
	requestTs *common.TsVbuuid) (*protobuf.TsVbuuid, error) {

	ts := protobuf.NewTsVbuuid(COUCHBASE_DEFAULT_POOL_NAME, bucket, len(vbnos))

	if requestTs == nil {
		newVbnos := common.Vbno16to32(vbnos)
		flogs, err := client.GetFailoverLogs(COUCHBASE_DEFAULT_POOL_NAME, bucket, newVbnos)
		if err != nil {
			common.Errorf("StreamAdmin::makeRestartTimestamp(): Unexpected Error for retrieving failover log for bucket %s = %s ",
				bucket, err.Error())
			return nil, err
		}

		ts = ts.ComputeFailoverTs(flogs.ToFailoverLog(vbnos))

	} else {

		for _, vbno := range vbnos {
			ts.Append(uint16(vbno), requestTs.Seqnos[vbno], requestTs.Vbuuids[vbno],
				requestTs.Snapshots[vbno][0], requestTs.Snapshots[vbno][1])
		}
	}

	return ts, nil
}
