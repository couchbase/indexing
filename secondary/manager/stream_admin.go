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
	couchbase "github.com/couchbase/indexing/secondary/dcp"
	projector "github.com/couchbase/indexing/secondary/projector/client"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
	"net"
	"strconv"
	"strings"
	"time"
)

/////////////////////////////////////////////////////////////////////////
// Type Definition
/////////////////////////////////////////////////////////////////////////

type VbMap map[string][]uint16

//
// ProjectorAdmin handles interaction with projector w.r.t streaming admin functions.
// The interaction with projector are based on the following rules:
// 1) Each stream has a <state>
//    - A timestmap for each bucket.   A timestamp is an array of seqno for each vbucket.
//    - A set of index instances.  Each instance contains a index definition and a set of endpoints.
// 2) A projector consumer/client can start a stream based on its own <local state>, without having to
//    know whether the projector is going to accept the request based on that <local state>.
// 3) When projector recieves a request to start a stream, it uses its own <local state> to filter out the
//    the <client request state>.  The resulting <active state> is
//    - timestamp filtered by <bucket, vbucket> that is applicable for that projector
//    - the set of index instances as proposed in the client <local state>
// 4)  Projector client can augment existing <state> by proposing new <state> to projector. Agumentation allows
//    - adding new instance
//    - changing endpoint of an instance?
//    - activate new vbucket
// 5) State agumentation will not allow
//    - remove instance
//    - deactive vbucket
//    - restart vbucket
//    - repair connection
//
type ProjectorAdmin struct {
	activeTimestamps map[string][]*protobuf.TsVbuuid
	factory          ProjectorStreamClientFactory
	env              ProjectorClientEnv
}

type adminWorker struct {
	admin            *ProjectorAdmin
	server           string
	streamId         common.StreamId
	activeTimestamps []*protobuf.TsVbuuid
	err              error
	killch           chan bool
}

type ProjectorStreamClient interface {
	MutationTopicRequest(topic, endpointType string, reqTimestamps []*protobuf.TsVbuuid,
		instances []*protobuf.Instance) (*protobuf.TopicResponse, error)
	DelInstances(topic string, uuids []uint64) error
	RepairEndpoints(topic string, endpoints []string) error
	InitialRestartTimestamp(pooln, bucketn string) (*protobuf.TsVbuuid, error)
	RestartVbuckets(topic string, restartTimestamps []*protobuf.TsVbuuid) (*protobuf.TopicResponse, error)
}

type ProjectorStreamClientFactory interface {
	GetClientForNode(server string) ProjectorStreamClient
}

type ProjectorStreamClientFactoryImpl struct {
}

type ProjectorClientEnv interface {
	GetNodeListForBuckets(buckets []string) (map[string]string, error)
	GetNodeListForTimestamps(timestamps []*common.TsVbuuid) (map[string][]*protobuf.TsVbuuid, error)
}

type ProjectorClientEnvImpl struct {
}

/////////////////////////////////////////////////////////////////////////
// ProjectorAdmin - Public Function
/////////////////////////////////////////////////////////////////////////

func NewProjectorAdmin(factory ProjectorStreamClientFactory, env ProjectorClientEnv) *ProjectorAdmin {
	if factory == nil {
		factory = newProjectorStreamClientFactoryImpl()
	}
	if env == nil {
		env = newProjectorClientEnvImpl()
	}
	return &ProjectorAdmin{activeTimestamps: make(map[string][]*protobuf.TsVbuuid),
		factory: factory,
		env:     env}
}

//
// Add new index instances to a stream
//
func (p *ProjectorAdmin) AddIndexToStream(streamId common.StreamId,
	buckets []string,
	instances []*protobuf.Instance,
	requestTimestamps []*common.TsVbuuid) error {

	common.Debugf("ProjectorAdmin::AddIndexToStream(): streamId=%v", streamId)

	// If there is no bucket or index instances, nothing to start.
	if len(buckets) == 0 || len(instances) == 0 {
		common.Debugf("ProjectorAdmin::AddIndexToStream(): len(buckets)=%v, len(instances)=%v",
			len(buckets), len(instances))
		return nil
	}

	shouldRetry := true
	for shouldRetry {
		shouldRetry = false

		nodes, err := p.env.GetNodeListForBuckets(buckets)
		if err != nil {
			return err
		}
		common.Debugf("ProjectorAdmin::AddIndexToStream(): len(nodes)=%v", len(nodes))

		// start worker to create mutation stream
		workers := make(map[string]*adminWorker)
		donech := make(chan *adminWorker, len(nodes))

		for _, server := range nodes {
			worker := &adminWorker{
				admin:            p,
				server:           server,
				streamId:         streamId,
				killch:           make(chan bool, 1),
				activeTimestamps: nil,
				err:              nil}
			workers[server] = worker
			go worker.addInstances(instances, buckets, requestTimestamps, donech)
		}

		common.Debugf("ProjectorAdmin::AddIndexToStream(): len(workers)=%v", len(workers))

		// now wait for the worker to be done
		// TODO: timeout?
		for len(workers) != 0 {
			worker := <-donech

			common.Debugf("ProjectorAdmin::AddIndexToStream(): worker %v done", worker.server)
			p.activeTimestamps[worker.server] = worker.activeTimestamps
			delete(workers, worker.server)

			if worker.err != nil {
				common.Debugf("ProjectorAdmin::AddIndexToStream(): worker % has error=%v", worker.server, worker.err)

				// cleanup : kill the other workers
				for _, worker := range workers {
					worker.killch <- true
				}

				// if it is not a recoverable error, then just return
				if worker.err.(Error).code != ERROR_STREAM_WRONG_VBUCKET &&
					worker.err.(Error).code != ERROR_STREAM_INVALID_TIMESTAMP &&
					worker.err.(Error).code != ERROR_STREAM_INVALID_KVADDRS &&
					worker.err.(Error).code != ERROR_STREAM_PROJECTOR_TIMEOUT {
					return worker.err
				}

				common.Debugf("ProjectorAdmin::AddIndexToStream(): retry adding instances to nodes")
				shouldRetry = true
				break
			}
		}
	}

	return nil
}

//
// Delete Index from stream
//
func (p *ProjectorAdmin) DeleteIndexFromStream(streamId common.StreamId, buckets []string, instances []uint64) error {

	common.Debugf("StreamAdmin::DeleteIndexFromStream(): streamId=%d", streamId.String())

	// If there is no bucket or index instances, nothing to start.
	if len(buckets) == 0 || len(instances) == 0 {
		common.Debugf("ProjectorAdmin::DeleteIndexToStream(): len(buckets)=%v, len(instances)=%v",
			len(buckets), len(instances))
		return nil
	}

	shouldRetry := true
	for shouldRetry {
		shouldRetry = false

		nodes, err := p.env.GetNodeListForBuckets(buckets)
		if err != nil {
			return err
		}

		// start worker to create mutation stream
		workers := make(map[string]*adminWorker)
		donech := make(chan *adminWorker, len(nodes))

		for _, server := range nodes {
			worker := &adminWorker{
				admin:            p,
				server:           server,
				streamId:         streamId,
				killch:           make(chan bool, 1),
				activeTimestamps: nil,
				err:              nil}
			workers[server] = worker
			go worker.deleteInstances(instances, donech)
		}

		common.Debugf("ProjectorAdmin::DeleteIndexToStream(): len(workers)=%v", len(workers))

		// now wait for the worker to be done
		// TODO: timeout?
		for len(workers) != 0 {
			worker := <-donech

			common.Debugf("ProjectorAdmin::DeleteIndexToStream(): worker %v done", worker.server)
			delete(workers, worker.server)

			if worker.err != nil {
				common.Debugf("ProjectorAdmin::DeleteIndexFromStream(): worker % has error=%v", worker.server, worker.err)

				// cleanup : kill the other workers
				for _, worker := range workers {
					worker.killch <- true
				}

				// if it is not a recoverable error, then just return
				if worker.err.(Error).code != ERROR_STREAM_PROJECTOR_TIMEOUT {
					return worker.err
				}

				common.Debugf("ProjectorAdmin::DeleteIndexToStream(): retry adding instances to nodes")
				shouldRetry = true
				break
			}
		}
	}

	return nil
}

//
// Repair the stream by asking the provider to reconnect to the list of endpoints.
// Once connected, the provider will stream mutations from the current vbucket seqno.
// In other words, the provider will not reset the seqno.
//
func (p *ProjectorAdmin) RepairEndpointForStream(streamId common.StreamId,
	bucketVbnosMap map[string][]uint16,
	endpoint string) error {

	common.Debugf("ProjectorAdmin::RepairStreamForEndpoint(): streamId = %d", streamId.String())

	// If there is no bucket, nothing to start.
	if len(bucketVbnosMap) == 0 {
		return nil
	}

	shouldRetry := true
	for shouldRetry {
		shouldRetry = false

		var buckets []string = nil
		for bucket, _ := range bucketVbnosMap {
			buckets = append(buckets, bucket)
		}

		nodes, err := p.env.GetNodeListForBuckets(buckets)
		if err != nil {
			return err
		}

		// start worker to create mutation stream
		workers := make(map[string]*adminWorker)
		donech := make(chan *adminWorker, len(nodes))

		for _, server := range nodes {
			worker := &adminWorker{
				admin:            p,
				server:           server,
				streamId:         streamId,
				killch:           make(chan bool, 1),
				activeTimestamps: nil,
				err:              nil}
			workers[server] = worker
			go worker.repairEndpoint(endpoint, donech)
		}

		// now wait for the worker to be done
		// TODO: timeout?
		for len(workers) != 0 {
			worker := <-donech
			delete(workers, worker.server)

			if worker.err != nil {
				common.Debugf("ProjectorAdmin::RepairEndpointFromStream(): worker % has error=%v", worker.server, worker.err)

				// cleanup : kill the other workers
				for _, worker := range workers {
					worker.killch <- true
				}

				// if it is not a recoverable error, then just return
				if worker.err.(Error).code != ERROR_STREAM_PROJECTOR_TIMEOUT {
					return worker.err
				}

				shouldRetry = true
				break
			}
		}
	}

	return nil
}

//
// Restart partial stream using the restart timestamp for the particular <bucket, vbucket>
// specified in the restart timestamp.   The partial stream for <bucket, vbucket> is only
// restarted if it is not active.
//
func (p *ProjectorAdmin) RestartStreamIfNecessary(streamId common.StreamId,
	restartTimestamps []*common.TsVbuuid) error {

	common.Debugf("ProjectorAdmin::RestartStreamIfNecessary(): streamId=%v", streamId)

	if len(restartTimestamps) == 0 {
		common.Debugf("ProjectorAdmin::RestartStreamIfNecessary(): len(restartTimestamps)=%v",
			len(restartTimestamps))
		return nil
	}

	shouldRetry := true
	for shouldRetry {
		shouldRetry = false

		nodes, err := p.env.GetNodeListForTimestamps(restartTimestamps)
		if err != nil {
			if err.(Error).code == ERROR_STREAM_INCONSISTENT_VBMAP {
				shouldRetry = true
				continue
			}
			return err
		}
		common.Debugf("ProjectorAdmin::RestartStreamIfNecessary(): len(nodes)=%v", len(nodes))

		// start worker to create mutation stream
		workers := make(map[string]*adminWorker)
		donech := make(chan *adminWorker, len(nodes))

		for server, timestamps := range nodes {
			worker := &adminWorker{
				admin:            p,
				server:           server,
				streamId:         streamId,
				killch:           make(chan bool, 1),
				activeTimestamps: nil,
				err:              nil}
			workers[server] = worker
			go worker.restartStream(timestamps, donech)
		}

		common.Debugf("ProjectorAdmin::RestartStreamIfNecessary(): len(workers)=%v", len(workers))

		// now wait for the worker to be done
		// TODO: timeout?
		for len(workers) != 0 {
			worker := <-donech

			common.Debugf("ProjectorAdmin::RestartStreamIfNecessary(): worker %v done", worker.server)
			p.activeTimestamps[worker.server] = worker.activeTimestamps
			delete(workers, worker.server)

			if worker.err != nil {
				common.Debugf("ProjectorAdmin::RestartStreamIfNecessary(): worker % has error=%v", worker.server, worker.err)

				// cleanup : kill the other workers
				for _, worker := range workers {
					worker.killch <- true
				}

				// if it is not a recoverable error, then just return.
				if worker.err.(Error).code != ERROR_STREAM_WRONG_VBUCKET &&
					worker.err.(Error).code != ERROR_STREAM_INVALID_TIMESTAMP &&
					worker.err.(Error).code != ERROR_STREAM_FEEDER &&
					worker.err.(Error).code != ERROR_STREAM_STREAM_END &&
					worker.err.(Error).code != ERROR_STREAM_PROJECTOR_TIMEOUT {

					return worker.err
				}

				common.Debugf("ProjectorAdmin::RestartStreamIfNecessary(): retry adding instances to nodes")
				shouldRetry = true
				break
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

    common.Debugf("StreamAdmin::CloseStream(): streamId = %d, bucket = %s", streamId.String(), bucket)

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

/////////////////////////////////////////////////////////////////////////
// Private Function - Worker
/////////////////////////////////////////////////////////////////////////

//
// Add index instances to a specific projector node
//
func (worker *adminWorker) addInstances(instances []*protobuf.Instance,
	buckets []string,
	requestTimestamps []*common.TsVbuuid,
	doneCh chan *adminWorker) {

	defer func() {
		doneCh <- worker
	}()

	common.Debugf("adminWorker::addInstances(): start")

	// Get projector client for the particular node.  This function does not
	// return an error even if the server is an invalid host name, but subsequent
	// call to client may fail.  Also note that there is no method to close the client
	// (no need to close upon termination).
	client := worker.admin.factory.GetClientForNode(worker.server)
	if client == nil {
		common.Debugf("adminWorker::addInstances(): no client returns from factory")
		return
	}

	// compute the restart timestamp for each bucket.  If there is a request timestamp for the
	// bucket, it will just convert it to protobuf format.  If the bucket does not have a request
	// timestamp (nil), it will use the failover log to compute the timestamp.
	var timestamps []*protobuf.TsVbuuid = nil
	for _, bucket := range buckets {

		var bucketTs *common.TsVbuuid = nil
		for _, requestTs := range requestTimestamps {
			if requestTs.Bucket == bucket {
				bucketTs = requestTs
				break
			}
		}

		ts, err := makeRestartTimestamp(client, bucket, bucketTs)
		if err != nil {
			// udpate the error string and put myself in the done channel
			worker.err = NewError(ERROR_STREAM_REQUEST_ERROR, NORMAL, STREAM, err, "Unable to make restart timestamp")
			return
		}
		timestamps = append(timestamps, ts)
	}

	// open the stream for the specific node for the set of <bucket, timestamp>
	topic := getTopicForStreamId(worker.streamId)

	retry := true
	startTime := time.Now().Unix()
	for retry {
		select {
		case <-worker.killch:
			return
		default:
			response, err := client.MutationTopicRequest(topic, "dataport", timestamps, instances)
			if err == nil {
				// no error, it is successful for this node
				worker.activeTimestamps = response.GetActiveTimestamps()
				worker.err = nil
				return
			}

			timestamps, err = worker.shouldRetryAddInstances(timestamps, response, err)
			if err != nil {
				// Either it is a non-recoverable error or an error that cannot be retry by this worker.
				// Terminate this worker.
				worker.activeTimestamps = response.GetActiveTimestamps()
				worker.err = err
				return
			}

			retry = time.Now().Unix()-startTime < MAX_PROJECTOR_RETRY_ELAPSED_TIME
		}
	}

	// When we reach here, it passes the elaspse time that the projector is supposed to response.
	// Projector may die or it can be a network partition, need to return an error since it may
	// require another worker to retry.
	worker.err = NewError4(ERROR_STREAM_PROJECTOR_TIMEOUT, NORMAL, STREAM, "Projector Call timeout after retry.")
}

//
// Handle error for adding instance.  The following error can be returned from projector:
// 1) Unconditional Recoverable error by worker
// 		* generic http error
// 		* ErrorStreamRequest
// 		* ErrorResposneTimeout
// 		* ErrorFeeder
// 2) Non Recoverable error
// 		* ErrorInconsistentFeed
// 3) Recoverable error by other worker
// 		* ErrorInvalidVbucketBranch
// 		* ErrorNotMyVbucket
//	    * ErrorInvalidKVaddrs
// 4) Error that may not need retry
//		* ErrorTopicExist
//
func (worker *adminWorker) shouldRetryAddInstances(requestTs []*protobuf.TsVbuuid,
	response *protobuf.TopicResponse,
	err error) ([]*protobuf.TsVbuuid, error) {

	common.Debugf("adminWorker::shouldRetryAddInstances(): start")

	// First of all, let's check for any non-recoverable error.
	errStr := err.Error()
	common.Debugf("adminWorker::shouldRetryAddInstances(): Error encountered when calling MutationTopicRequest. Error=%v", errStr)

	if strings.Contains(errStr, "ErrorTopicExist") {
		// TODO: Need pratap to define the semantic of ErrorTopExist.   Right now return as an non-recoverable error.
		return nil, NewError(ERROR_STREAM_REQUEST_ERROR, NORMAL, STREAM, err, "")

	} else if strings.Contains(errStr, "ErrorInconsistentFeed") {
		// This is fatal error.  Should only happen due to coding error.   Need to return this error.
		// For those projectors that have already been opened, let's leave it open. Eventually those
		// projectors will fill up the buffer and terminate the connection by itself.
		return nil, NewError(ERROR_STREAM_REQUEST_ERROR, NORMAL, STREAM, err, "")

	} else if strings.Contains(errStr, "ErrorNotMyVbucket") {
		return nil, NewError(ERROR_STREAM_WRONG_VBUCKET, NORMAL, STREAM, err, "")

	} else if strings.Contains(errStr, "ErrorInvalidVbucketBranch") {
		return nil, NewError(ERROR_STREAM_INVALID_TIMESTAMP, NORMAL, STREAM, err, "")

	} else if strings.Contains(errStr, "ErrorInvalidKVaddrs") {
		return nil, NewError(ERROR_STREAM_INVALID_KVADDRS, NORMAL, STREAM, err, "")
	}

	// There is no non-recoverable error, so we can retry.  For retry, recompute the new set of timestamps based on the response.
	rollbackTimestamps := response.GetRollbackTimestamps()
	var newRequestTs []*protobuf.TsVbuuid = nil
	for _, ts := range requestTs {
		ts = recomputeRequestTimestamp(ts, rollbackTimestamps)
		newRequestTs = append(newRequestTs, ts)
	}

	return newRequestTs, nil
}

//
// Delete index instances from a specific projector node
//
func (worker *adminWorker) deleteInstances(instances []uint64, doneCh chan *adminWorker) {

	defer func() {
		doneCh <- worker
	}()

	common.Debugf("adminWorker::deleteInstances(): start")

	// Get projector client for the particular node.  This function does not
	// return an error even if the server is an invalid host name, but subsequent
	// call to client may fail.  Also note that there is no method to close the client
	// (no need to close upon termination).
	client := worker.admin.factory.GetClientForNode(worker.server)
	if client == nil {
		common.Debugf("adminWorker::deleteInstances(): no client returns from factory")
		return
	}

	// open the stream for the specific node for the set of <bucket, timestamp>
	topic := getTopicForStreamId(worker.streamId)

	retry := true
	startTime := time.Now().Unix()
	for retry {
		select {
		case <-worker.killch:
			return
		default:
			err := client.DelInstances(topic, instances)
			if err == nil {
				// no error, it is successful for this node
				worker.err = nil
				return
			}

			common.Debugf("adminWorker::deleteInstances(): Error encountered when calling DelInstances. Error=%v", err.Error())
			if strings.Contains(err.Error(), "ErrorTopicMissing") {
				// It is OK if topic is missing
				worker.err = nil
				return
			}

			retry = time.Now().Unix()-startTime < MAX_PROJECTOR_RETRY_ELAPSED_TIME
		}
	}

	// When we reach here, it passes the elaspse time that the projector is supposed to response.
	// Projector may die or it can be a network partition, need to return an error since it may
	// require another worker to retry.
	worker.err = NewError4(ERROR_STREAM_PROJECTOR_TIMEOUT, NORMAL, STREAM, "Projector Call timeout after retry.")
}

//
// Repair endpoint for a specific projector node
//
func (worker *adminWorker) repairEndpoint(endpoint string, doneCh chan *adminWorker) {

	defer func() {
		doneCh <- worker
	}()

	common.Debugf("adminWorker::repairEndpoint(): start")

	// Get projector client for the particular node.  This function does not
	// return an error even if the server is an invalid host name, but subsequent
	// call to client may fail.  Also note that there is no method to close the client
	// (no need to close upon termination).
	client := worker.admin.factory.GetClientForNode(worker.server)
	if client == nil {
		common.Debugf("adminWorker::repairEndpoints(): no client returns from factory")
		return
	}

	// open the stream for the specific node for the set of <bucket, timestamp>
	topic := getTopicForStreamId(worker.streamId)

	retry := true
	startTime := time.Now().Unix()
	for retry {
		select {
		case <-worker.killch:
			return
		default:

			err := client.RepairEndpoints(topic, []string{endpoint})
			if err == nil {
				// no error, it is successful for this node
				worker.err = nil
				return
			}

			common.Debugf("adminWorker::repairEndpiont(): Error encountered when calling RepairEndpoint. Error=%v", err.Error())
			if strings.Contains(err.Error(), "ErrorTopicMissing") {
				// It is OK if topic is missing
				worker.err = nil
				return
			}

			retry = time.Now().Unix()-startTime < MAX_PROJECTOR_RETRY_ELAPSED_TIME
		}
	}

	// When we reach here, it passes the elaspse time that the projector is supposed to response.
	// Projector may die or it can be a network partition, need to return an error since it may
	// require another worker to retry.
	worker.err = NewError4(ERROR_STREAM_PROJECTOR_TIMEOUT, NORMAL, STREAM, "Projector Call timeout after retry.")
}

//
// Add index instances to a specific projector node
//
func (worker *adminWorker) restartStream(timestamps []*protobuf.TsVbuuid, doneCh chan *adminWorker) {

	defer func() {
		doneCh <- worker
	}()

	common.Debugf("adminWorker::restartStream(): start")

	// Get projector client for the particular node.  This function does not
	// return an error even if the server is an invalid host name, but subsequent
	// call to client may fail.  Also note that there is no method to close the client
	// (no need to close upon termination).
	client := worker.admin.factory.GetClientForNode(worker.server)
	if client == nil {
		common.Debugf("adminWorker::restartStream(): no client returns from factory")
		return
	}

	// open the stream for the specific node for the set of <bucket, timestamp>
	topic := getTopicForStreamId(worker.streamId)

	retry := true
	startTime := time.Now().Unix()
	for retry {
		select {
		case <-worker.killch:
			return
		default:
			response, err := client.RestartVbuckets(topic, timestamps)
			if err == nil {
				// no error, it is successful for this node
				worker.activeTimestamps = response.GetActiveTimestamps()
				worker.err = nil
				return
			}

			timestamps, err = worker.shouldRetryRestartVbuckets(timestamps, response, err)
			if err != nil {
				// Either it is a non-recoverable error or an error that cannot be retry by this worker.
				// Terminate this worker.
				worker.activeTimestamps = response.GetActiveTimestamps()
				worker.err = err
				return
			}

			retry = time.Now().Unix()-startTime < MAX_PROJECTOR_RETRY_ELAPSED_TIME
		}
	}

	// When we reach here, it passes the elaspse time that the projector is supposed to response.
	// Projector may die or it can be a network partition, need to return an error since it may
	// require another worker to retry.
	worker.err = NewError4(ERROR_STREAM_PROJECTOR_TIMEOUT, NORMAL, STREAM, "Projector Call timeout after retry.")
}

//
// Handle error for restart vbuckets.  The following error can be returned from projector:
// 1) Unconditional Recoverable error by worker
// 		* generic http error
// 		* ErrorStreamRequest
// 		* ErrorResposneTimeout
// 2) Non Recoverable error
// 		* ErrorTopicMissing
// 		* ErrorInvalidBucket
// 3) Recoverable error by other worker
// 		* ErrorInvalidVbucketBranch
// 		* ErrorNotMyVbucket
// 		* ErrorFeeder
// 		* ErrorStreamEnd
//
func (worker *adminWorker) shouldRetryRestartVbuckets(requestTs []*protobuf.TsVbuuid,
	response *protobuf.TopicResponse,
	err error) ([]*protobuf.TsVbuuid, error) {

	common.Debugf("adminWorker::shouldRetryRestartVbuckets(): start")

	// First of all, let's check for any non-recoverable error.
	errStr := err.Error()
	common.Debugf("adminWorker::shouldRetryRestartVbuckets(): Error encountered when calling RestartVbuckets. Error=%v", errStr)

	if strings.Contains(errStr, "ErrorTopicMissing") {
		return nil, NewError(ERROR_STREAM_REQUEST_ERROR, NORMAL, STREAM, err, "")

	} else if strings.Contains(errStr, "ErrorInvalidBucket") {
		return nil, NewError(ERROR_STREAM_REQUEST_ERROR, NORMAL, STREAM, err, "")

	} else if strings.Contains(errStr, "ErrorFeeder") {
		return nil, NewError(ERROR_STREAM_FEEDER, NORMAL, STREAM, err, "")

	} else if strings.Contains(errStr, "ErrorNotMyVbucket") {
		return nil, NewError(ERROR_STREAM_WRONG_VBUCKET, NORMAL, STREAM, err, "")

	} else if strings.Contains(errStr, "ErrorInvalidVbucketBranch") {
		return nil, NewError(ERROR_STREAM_INVALID_TIMESTAMP, NORMAL, STREAM, err, "")

	} else if strings.Contains(errStr, "ErrorStreamEnd") {
		return nil, NewError(ERROR_STREAM_STREAM_END, NORMAL, STREAM, err, "")
	}

	// There is no non-recoverable error, so we can retry.  For retry, recompute the new set of timestamps based on the response.
	rollbackTimestamps := response.GetRollbackTimestamps()
	var newRequestTs []*protobuf.TsVbuuid = nil
	for _, ts := range requestTs {
		ts = recomputeRequestTimestamp(ts, rollbackTimestamps)
		newRequestTs = append(newRequestTs, ts)
	}

	return newRequestTs, nil
}

/////////////////////////////////////////////////////////////////////////
// Private Function - Timestamp
/////////////////////////////////////////////////////////////////////////

//
// Create the restart timetamp
//
func makeRestartTimestamp(client ProjectorStreamClient,
	bucket string,
	requestTs *common.TsVbuuid) (*protobuf.TsVbuuid, error) {

	if requestTs == nil {
		// Get the request timestamp from each server that has the bucket (last arg is nil).
		// This should return a full timestamp of all the vbuckets. There is no guarantee that this
		// method will get the latest seqno though (it computes the timestamp from failover log).
		//
		// If the cluster configuration changes:
		// 1) rebalancing - should be fine since vbuuid remains unchanged
		// 2) failover.  This can mean that the timestamp can have stale vbuuid.   Subsequent
		//    call to projector will detect this.
		return client.InitialRestartTimestamp(DEFAULT_POOL_NAME, bucket)

	} else {
		newTs := protobuf.NewTsVbuuid(DEFAULT_POOL_NAME, requestTs.Bucket, len(requestTs.Seqnos))
		for i, _ := range requestTs.Seqnos {
			newTs.Append(uint16(i), requestTs.Seqnos[i], requestTs.Vbuuids[i],
				requestTs.Snapshots[i][0], requestTs.Snapshots[i][1])
		}
		return newTs, nil
	}
}

//
// Compute a new request timestamp based on the response from projector.
// If all the vb is active for the given requestTs, then this function returns nil.
//
func recomputeRequestTimestamp(requestTs *protobuf.TsVbuuid,
	rollbackTimestamps []*protobuf.TsVbuuid) *protobuf.TsVbuuid {

	newTs := protobuf.NewTsVbuuid(DEFAULT_POOL_NAME, requestTs.GetBucket(), len(requestTs.GetVbnos()))
	rollbackTs := findTimestampForBucket(rollbackTimestamps, requestTs.GetBucket())

	for i, vbno := range requestTs.GetVbnos() {
		offset := findTimestampOffsetForVb(rollbackTs, vbno)
		if offset != -1 {
			// there is a failover Ts for this vbno.  Use that one for retry.
			newTs.Append(uint16(vbno), rollbackTs.Seqnos[offset], rollbackTs.Vbuuids[offset],
				rollbackTs.Snapshots[offset].GetStart(), rollbackTs.Snapshots[offset].GetEnd())
		} else {
			// the vb is not active, just copy from the original requestTS
			newTs.Append(uint16(vbno), requestTs.Seqnos[i], requestTs.Vbuuids[i],
				requestTs.Snapshots[i].GetStart(), requestTs.Snapshots[i].GetEnd())
		}
	}

	return newTs
}

//
// Find timestamp for the corresponding bucket withing the array of timestamps
//
func findTimestampForBucket(timestamps []*protobuf.TsVbuuid, bucket string) *protobuf.TsVbuuid {

	for _, ts := range timestamps {
		if ts.GetBucket() == bucket {
			return ts
		}
	}

	return nil
}

//
// Find the offset/index in the timestamp for the given vbucket no.  Return
// -1 if no matching vbno being found.
//
func findTimestampOffsetForVb(ts *protobuf.TsVbuuid, vbno uint32) int {

	if ts == nil {
		return -1
	}

	for i, ts_vbno := range ts.GetVbnos() {
		if ts_vbno == vbno {
			return i
		}
	}

	return -1
}

/////////////////////////////////////////////////////////////////////////
// Private Function -  ProjectorStreamClientFactory
/////////////////////////////////////////////////////////////////////////

func newProjectorStreamClientFactoryImpl() ProjectorStreamClientFactory {
	return new(ProjectorStreamClientFactoryImpl)
}

//
// Get the projector client for the given node
//
func (p *ProjectorStreamClientFactoryImpl) GetClientForNode(server string) ProjectorStreamClient {

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
			common.Debugf("StreamAdmin::GetClientForNode(): Local Projector Addr: %v", projAddr)

		} else {
			projAddr = host + ":" + PROJECTOR_PORT
			common.Debugf("StreamAdmin::GetClientForNode(): Remote Projector Addr: %v", projAddr)
		}
	}

	//create client for node's projectors
	config := common.SystemConfig.SectionConfig("projector.client.", true)
	maxvbs := common.SystemConfig["maxVbuckets"].Int()
	ap := projector.NewClient(HTTP_PREFIX+projAddr+"/adminport/", maxvbs, config)
	return ap
}

/////////////////////////////////////////////////////////////////////////
// Private Function -  ProjectorClientEnv
/////////////////////////////////////////////////////////////////////////

func newProjectorClientEnvImpl() ProjectorClientEnv {
	return new(ProjectorClientEnvImpl)
}

//
// Get the set of nodes for all the given buckets
//
func (p *ProjectorClientEnvImpl) GetNodeListForBuckets(buckets []string) (map[string]string, error) {

	common.Debugf("ProjectorCLientEnvImpl::getNodeListForBuckets(): start")

	nodes := make(map[string]string)

	for _, bucket := range buckets {

		bucketRef, err := couchbase.GetBucket(COUCHBASE_INTERNAL_BUCKET_URL, DEFAULT_POOL_NAME, bucket)
		if err != nil {
			return nil, err
		}

		if err := bucketRef.Refresh(); err != nil {
			return nil, err
		}

		for _, node := range bucketRef.NodeAddresses() {
			// TODO: This may not work for cluster_run when all processes are run in the same node.  Need to check.
			common.Debugf("ProjectorCLientEnvImpl::getNodeListForBuckets(): node=%v for bucket %v", node, bucket)
			nodes[node] = node
		}
	}

	return nodes, nil
}

//
// Get the set of nodes for all the given timestamps
//
func (p *ProjectorClientEnvImpl) GetNodeListForTimestamps(timestamps []*common.TsVbuuid) (map[string][]*protobuf.TsVbuuid, error) {

	common.Debugf("ProjectorCLientEnvImpl::getNodeListForTimestamps(): start")

	nodes := make(map[string][]*protobuf.TsVbuuid)

	for _, ts := range timestamps {

		bucketRef, err := couchbase.GetBucket(COUCHBASE_INTERNAL_BUCKET_URL, DEFAULT_POOL_NAME, ts.Bucket)
		if err != nil {
			return nil, err
		}

		if err := bucketRef.Refresh(); err != nil {
			return nil, err
		}

		vbmap, err := bucketRef.GetVBmap(nil)
		if err != nil {
			return nil, err
		}

		for i, seqno := range ts.Seqnos {
			if seqno != 0 {
				found := false
				for kvaddr, vbnos := range vbmap {
					for _, vbno := range vbnos {
						if vbno == uint16(i) {
							newTs := p.findTimestamp(nodes, kvaddr, ts.Bucket)
							newTs.Append(uint16(i), ts.Seqnos[i], ts.Vbuuids[i],
								ts.Snapshots[i][0], ts.Snapshots[i][1])
							found = true
							break
						}
					}

					if found {
						break
					}
				}

				if !found {
					return nil, NewError2(ERROR_STREAM_INCONSISTENT_VBMAP, STREAM)
				}
			}
		}
	}

	return nodes, nil
}

func (p *ProjectorClientEnvImpl) findTimestamp(timestampMap map[string][]*protobuf.TsVbuuid,
	kvaddr string,
	bucket string) *protobuf.TsVbuuid {

	timestamps, ok := timestampMap[kvaddr]
	if !ok {
		timestamps = nil
	}

	for _, ts := range timestamps {
		if ts.GetBucket() == bucket {
			return ts
		}
	}

	newTs := protobuf.NewTsVbuuid(DEFAULT_POOL_NAME, bucket, NUM_VB)
	timestamps = append(timestamps, newTs)
	timestampMap[kvaddr] = timestamps
	return newTs
}

/////////////////////////////////////////////////////////////////////////
// Private Function - Utilty
/////////////////////////////////////////////////////////////////////////

//
// Convert StreamId into topic string
//
func getTopicForStreamId(streamId common.StreamId) string {

	var topic string

	switch streamId {
	case common.MAINT_STREAM:
		if !TESTING {
			topic = MAINT_TOPIC
		} else {
			topic = "testing " + MAINT_TOPIC
		}
	case common.INIT_STREAM:
		if !TESTING {
			topic = INIT_TOPIC
		} else {
			topic = "testing " + INIT_TOPIC
		}
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
