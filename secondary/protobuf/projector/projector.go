package protobuf

import "errors"
import "sort"

import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/dcp"
import mc "github.com/couchbase/indexing/secondary/dcp/transport/client"
import "github.com/golang/protobuf/proto"

var ErrorInvalidVbmap = errors.New("protobuf.errorInvalidVbmap")

//************
//VbmapRequest
//************

// NewVbmapRequest will compose a adminport request for
// fetching vbmap for specified list of kvnodes.
func NewVbmapRequest(pool, bucket string, kvaddrs []string) *VbmapRequest {
	return &VbmapRequest{
		Pool:    proto.String(pool),
		Bucket:  proto.String(bucket),
		Kvaddrs: kvaddrs,
	}
}

// Name implement MessageMarshaller{} interface
func (req *VbmapRequest) Name() string {
	return "vbmapRequest"
}

// ContentType implement MessageMarshaller{} interface
func (req *VbmapRequest) ContentType() string {
	return "application/protobuf"
}

// Encode implement MessageMarshaller{} interface
func (req *VbmapRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

// Decode implement MessageMarshaller{} interface
func (req *VbmapRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

//*************
//VbmapResponse
//*************

// NewVbmapResponse will compose result message for VbmapRequest
func NewVbmapResponse() *VbmapResponse {
	return &VbmapResponse{}
}

// Name implement MessageMarshaller{} interface
func (resp *VbmapResponse) Name() string {
	return "vbmapResponse"
}

// ContentType implement MessageMarshaller{} interface
func (resp *VbmapResponse) ContentType() string {
	return "application/protobuf"
}

// Encode implement MessageMarshaller{} interface
func (resp *VbmapResponse) Encode() (data []byte, err error) {
	return proto.Marshal(resp)
}

// Decode implement MessageMarshaller{} interface
func (resp *VbmapResponse) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, resp)
}

// AppendVbmap for `kvaddr` to vbmap-response.
func (resp *VbmapResponse) AppendVbmap(kvaddr string, vbnos []uint16) *VbmapResponse {
	resp.Kvaddrs = append(resp.Kvaddrs, kvaddr)
	resp.Kvvbnos = append(resp.Kvvbnos, &Vbuckets{Vbnos: c.Vbno16to32(vbnos)})
	return resp
}

// AppendVbmaps for a list of `kvaddrs` to vbmap-response.
func (resp *VbmapResponse) AppendVbmaps(kvaddrs []string, vbnos [][]uint16) *VbmapResponse {
	for i, kvaddr := range kvaddrs {
		resp.Kvaddrs = append(resp.Kvaddrs, kvaddr)
		resp.Kvvbnos = append(resp.Kvvbnos, &Vbuckets{Vbnos: c.Vbno16to32(vbnos[i])})
	}
	return resp
}

// SetErr for vbmap-response.
func (resp *VbmapResponse) SetErr(err error) *VbmapResponse {
	resp.Err = NewError(err)
	return resp
}

// GetVbmaps return a map of kvaddr -> list-of-vbuckets in node.
func (resp *VbmapResponse) GetVbmaps() (map[string][]uint16, error) {
	vbm := make(map[string][]uint16)
	kvaddrs := resp.GetKvaddrs()
	kvvbnos := resp.GetKvvbnos()

	if len(kvaddrs) != len(kvvbnos) {
		return nil, ErrorInvalidVbmap
	}

	for i, kvaddr := range kvaddrs {
		vbm[kvaddr] = c.Vbno32to16(kvvbnos[i].GetVbnos())
	}
	return vbm, nil
}

// LocateVbucket will identify the kvnode that is hosting the vbucket.
func (resp *VbmapResponse) LocateVbucket(vbno uint32) string {
	kvvbnos := resp.GetKvvbnos()
	for i, kvaddr := range resp.GetKvaddrs() {
		for _, v := range kvvbnos[i].GetVbnos() {
			if v == vbno {
				return kvaddr
			}
		}
	}
	return ""
}

// AllVbuckets32 return all vbuckets hosted by all kvnodes
// in sort order. vbuckets are returned as 32-bit values.
func (resp *VbmapResponse) AllVbuckets32() []uint32 {
	vbs := make([]uint32, 0)
	for _, vs := range resp.GetKvvbnos() {
		vbs = append(vbs, vs.GetVbnos()...)
	}
	vbuckets := c.Vbuckets(c.Vbno32to16(vbs))
	sort.Sort(vbuckets)
	return vbuckets.To32()
}

// AllVbuckets16 return all vbuckets hosted by all kvnodes
// in sort order. vbuckets are returned as 16-bit values.
func (resp *VbmapResponse) AllVbuckets16() []uint16 {
	vbs := make([]uint16, 0)
	for _, vs := range resp.GetKvvbnos() {
		vbs = append(vbs, c.Vbno32to16(vs.GetVbnos())...)
	}
	vbuckets := c.Vbuckets(vbs)
	sort.Sort(vbuckets)
	return []uint16(vbuckets)
}

//******************
//FailoverLogRequest
//******************

// Name implement MessageMarshaller interface
func (req *FailoverLogRequest) Name() string {
	return "failoverLogRequest"
}

// ContentType implement MessageMarshaller interface
func (req *FailoverLogRequest) ContentType() string {
	return "application/protobuf"
}

// Encode implement MessageMarshaller interface
func (req *FailoverLogRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

// Decode implement MessageMarshaller interface
func (req *FailoverLogRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

//*******************
//FailoverLogResponse
//*******************

// Name implement MessageMarshaller interface
func (resp *FailoverLogResponse) Name() string {
	return "failoverLogResponse"
}

// ContentType implement MessageMarshaller interface
func (resp *FailoverLogResponse) ContentType() string {
	return "application/protobuf"
}

// Encode implement MessageMarshaller interface
func (resp *FailoverLogResponse) Encode() (data []byte, err error) {
	return proto.Marshal(resp)
}

// Decode implement MessageMarshaller interface
func (resp *FailoverLogResponse) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, resp)
}

// LatestBranch return a map of vbucket -> latest-vbuuid from
// failoverlog.
func (resp *FailoverLogResponse) LatestBranch() map[uint16]uint64 {
	vbuuids := make(map[uint16]uint64)
	for _, flog := range resp.GetLogs() {
		vbno := uint16(flog.GetVbno())
		vbuuids[vbno] = flog.Vbuuids[0]
	}
	return vbuuids
}

// ToFailoverLog return couchbase.FailoverLog for `vbnos` from
// FailoverLogResponse.
func (resp *FailoverLogResponse) ToFailoverLog(vbnos []uint16) couchbase.FailoverLog {
	flogs := make(couchbase.FailoverLog)
	for _, f := range resp.GetLogs() {
		fvbno := uint16(f.GetVbno())
		for _, vbno := range vbnos {
			if fvbno == vbno {
				seqnos := f.GetSeqnos()
				m := make(mc.FailoverLog, 0, len(seqnos))
				for i, vbuuid := range f.GetVbuuids() {
					m = append(m, [2]uint64{vbuuid, seqnos[i]})
				}
				flogs[vbno] = m
			}
		}
	}
	return flogs
}

//********************
//MutationTopicRequest
//********************

// NewMutationTopicRequest creates a new MutationTopicRequest
// for `topic`.
func NewMutationTopicRequest(
	topic, endpointType string, instances []*Instance) *MutationTopicRequest {

	return &MutationTopicRequest{
		Topic:         proto.String(topic),
		EndpointType:  proto.String(endpointType),
		ReqTimestamps: make([]*TsVbuuid, 0),
		Instances:     instances,
	}
}

// AddStreams will add a subset of vbuckets to for a
// bucket to the new topic.
func (req *MutationTopicRequest) AddStreams(
	pool, bucket string, ts *c.TsVbuuid) *MutationTopicRequest {

	reqTs := TsVbuuid{
		Pool:   proto.String(pool),
		Bucket: proto.String(bucket),
	}
	req.ReqTimestamps = append(req.ReqTimestamps, reqTs.FromTsVbuuid(ts))
	return req
}

// Append add a request-timestamp for {pool,bucket} to this topic request.
func (req *MutationTopicRequest) Append(reqTs *TsVbuuid) *MutationTopicRequest {
	req.ReqTimestamps = append(req.ReqTimestamps, reqTs)
	return req
}

// ReqTimestampFor will get the requested vbucket-stream
// timestamps for specified `bucket`.
// TODO: Semantics of TsVbuuid has changed.
//func (req *MutationTopicRequest) ReqTimestampFor(bucket string) *c.TsVbuuid {
//    for _, ts := range req.GetReqTimestamps() {
//        if ts.GetBucket() == bucket {
//            return ts.ToTsVbuuid()
//        }
//    }
//    return nil
//}

// GetEvaluators impelement Subscriber{} interface
func (req *MutationTopicRequest) GetEvaluators() (map[uint64]c.Evaluator, error) {
	return getEvaluators(req.GetInstances())
}

// GetRouters impelement Subscriber{} interface
func (req *MutationTopicRequest) GetRouters() (map[uint64]c.Router, error) {
	return getRouters(req.GetInstances())
}

// Name implement MessageMarshaller{} interface
func (req *MutationTopicRequest) Name() string {
	return "mutationTopicRequest"
}

// ContentType implement MessageMarshaller{} interface
func (req *MutationTopicRequest) ContentType() string {
	return "application/protobuf"
}

// Encode implement MessageMarshaller{} interface
func (req *MutationTopicRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

// Decode implement MessageMarshaller{} interface
func (req *MutationTopicRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

// *************
// TopicResponse
// *************

// Name implement MessageMarshaller{} interface
func (resp *TopicResponse) Name() string {
	return "topicResponse"
}

// ContentType implement MessageMarshaller{} interface
func (resp *TopicResponse) ContentType() string {
	return "application/protobuf"
}

// Encode implement MessageMarshaller{} interface
func (resp *TopicResponse) Encode() (data []byte, err error) {
	return proto.Marshal(resp)
}

// Decode implement MessageMarshaller{} interface
func (resp *TopicResponse) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, resp)
}

// AddRollbackTimestamp will add a subset of vbucket's
// rollback-timestamp for a `bucket`.
func (resp *TopicResponse) AddRollbackTimestamp(
	pool, bucket string, rollbTs *c.TsVbuuid) *TopicResponse {

	// add rollback timestamp
	ts := TsVbuuid{
		Pool:   proto.String(pool),
		Bucket: proto.String(bucket),
	}
	resp.RollbackTimestamps = append(
		resp.RollbackTimestamps, ts.FromTsVbuuid(rollbTs))

	// prune active timestamp, that received rollback.
	actTss := make([]*TsVbuuid, len(resp.GetActiveTimestamps()))
	for i, actTs := range resp.GetActiveTimestamps() {
		if actTs.GetBucket() == bucket {
			vbnos := c.Vbno32to16(ts.GetVbnos())
			actTss[i] = actTs.FilterByVbuckets(vbnos)
		} else {
			actTss[i] = actTs
		}
	}
	resp.ActiveTimestamps = actTss
	return resp
}

// SetErr update error value in response's.
func (resp *TopicResponse) SetErr(err error) *TopicResponse {
	resp.Err = NewError(err)
	return resp
}

// *****************
// TimestampResponse
// *****************

// Name implement MessageMarshaller{} interface
func (tsResp *TimestampResponse) Name() string {
	return "timestampResponse"
}

// ContentType implement MessageMarshaller{} interface
func (tsResp *TimestampResponse) ContentType() string {
	return "application/protobuf"
}

// Encode implement MessageMarshaller{} interface
func (tsResp *TimestampResponse) Encode() (data []byte, err error) {
	return proto.Marshal(tsResp)
}

// Decode implement MessageMarshaller{} interface
func (tsResp *TimestampResponse) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, tsResp)
}

// AddCurrentTimestamp will add a subset of vbucket's
// rollback-timestamp for a `bucket`.
func (tsResp *TimestampResponse) AddCurrentTimestamp(
	pooln, bucketn string, curSeqnos map[uint16]uint64) *TimestampResponse {

	ts := NewTsVbuuid(pooln, bucketn, len(curSeqnos))
	for vbno, seqno := range curSeqnos {
		ts.Append(vbno, seqno, 0 /*vbuuid*/, 0 /*start*/, 0 /*end*/)
	}
	tsResp.CurrentTimestamps = append(tsResp.CurrentTimestamps, ts)
	return tsResp
}

// SetErr update error value in response's.
func (tsResp *TimestampResponse) SetErr(err error) *TimestampResponse {
	tsResp.Err = NewError(err)
	return tsResp
}

// **********************
// RestartVbucketsRequest
// **********************

// NewRestartVbucketsRequest creates a RestartVbucketsRequest
// for topic, later a list of {pool,bucket,timestamp} need to
// be added before posting the request.
func NewRestartVbucketsRequest(topic string) *RestartVbucketsRequest {
	return &RestartVbucketsRequest{
		Topic: proto.String(topic),
	}
}

// Name implement MessageMarshaller{} interface
func (resp *RestartVbucketsRequest) Name() string {
	return "restartVbucketsRequest"
}

// ContentType implement MessageMarshaller{} interface
func (resp *RestartVbucketsRequest) ContentType() string {
	return "application/protobuf"
}

// Encode implement MessageMarshaller{} interface
func (resp *RestartVbucketsRequest) Encode() (data []byte, err error) {
	return proto.Marshal(resp)
}

// Decode implement MessageMarshaller{} interface
func (resp *RestartVbucketsRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, resp)
}

// AddStreams will add a subset of vbuckets to be restarted for
// a bucket.
func (req *RestartVbucketsRequest) AddStreams(
	pool, bucket string, ts *c.TsVbuuid) *RestartVbucketsRequest {

	restartTs := TsVbuuid{
		Pool:   proto.String(pool),
		Bucket: proto.String(bucket),
	}
	req.RestartTimestamps = append(
		req.RestartTimestamps, restartTs.FromTsVbuuid(ts))
	return req
}

// Append add a restart-timestamp for {pool,bucket,[]vbuckets}
// to this topic request.
func (req *RestartVbucketsRequest) Append(
	restartTs *TsVbuuid) *RestartVbucketsRequest {

	req.RestartTimestamps = append(req.RestartTimestamps, restartTs)
	return req
}

// RestartTimestampFor will get the requested vbucket-stream
// timestamps for specified `bucket`.
// TODO: Semantics of TsVbuuid has changed.
//func (req *RestartVbucketsRequest) RestartTimestampFor(b string) *c.TsVbuuid {
//    for _, ts := range req.GetRestartTimestamps() {
//        if ts.GetBucket() == b {
//            return ts.ToTsVbuuid()
//        }
//    }
//    return nil
//}

// ***********************
// ShutdownVbucketsRequest
// ***********************

// NewShutdownVbucketsRequest creates a ShutdownVbucketsRequest
// for topic, later a list of {pool,bucket,timestamp} need to
// be added before posting the request.
func NewShutdownVbucketsRequest(topic string) *ShutdownVbucketsRequest {
	return &ShutdownVbucketsRequest{
		Topic: proto.String(topic),
	}
}

// Name implement MessageMarshaller{} interface
func (resp *ShutdownVbucketsRequest) Name() string {
	return "shutdownVbucketsRequest"
}

// ContentType implement MessageMarshaller{} interface
func (resp *ShutdownVbucketsRequest) ContentType() string {
	return "application/protobuf"
}

// Encode implement MessageMarshaller{} interface
func (resp *ShutdownVbucketsRequest) Encode() (data []byte, err error) {
	return proto.Marshal(resp)
}

// Decode implement MessageMarshaller{} interface
func (resp *ShutdownVbucketsRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, resp)
}

// Append add a shutdown-timestamp for {pool,bucket,[]vbuckets}
// to this topic.
func (req *ShutdownVbucketsRequest) Append(
	shutdownTs *TsVbuuid) *ShutdownVbucketsRequest {

	req.ShutdownTimestamps = append(req.ShutdownTimestamps, shutdownTs)
	return req
}

// AddStreams will add a subset of vbuckets to be restarted for
// a bucket.
func (req *ShutdownVbucketsRequest) AddStreams(
	pool, bucket string, ts *c.TsVbuuid) *ShutdownVbucketsRequest {

	shutdownTs := TsVbuuid{
		Pool:   proto.String(pool),
		Bucket: proto.String(bucket),
	}
	req.ShutdownTimestamps = append(
		req.ShutdownTimestamps, shutdownTs.FromTsVbuuid(ts))
	return req
}

// ShutdownTimestampFor will get the requested vbucket-stream
// timestamps for specified `bucket`.
// TODO: Semantics of TsVbuuid has changed.
//func (req *ShutdownVbucketsRequest) ShutdownTimestampFor(b string) *c.TsVbuuid {
//    for _, ts := range req.GetShutdownTimestamps() {
//        if ts.GetBucket() == b {
//            return ts.ToTsVbuuid()
//        }
//    }
//    return nil
//}

// *****************
// AddBucketsRequest
// *****************

// NewAddBucketsRequest creates an AddBucketsRequest
// for topic to add one or more new instances/engines to a topic.
func NewAddBucketsRequest(
	topic string, instances []*Instance) *AddBucketsRequest {

	return &AddBucketsRequest{
		Topic:         proto.String(topic),
		ReqTimestamps: make([]*TsVbuuid, 0),
		Instances:     instances,
	}
}

// AddStreams will add a subset of vbuckets to for a
// bucket to the new topic.
func (req *AddBucketsRequest) AddStreams(
	pool, bucket string, ts *c.TsVbuuid) *AddBucketsRequest {

	reqTs := TsVbuuid{
		Pool:   proto.String(pool),
		Bucket: proto.String(bucket),
	}
	req.ReqTimestamps = append(req.ReqTimestamps, reqTs.FromTsVbuuid(ts))
	return req
}

// Name implement MessageMarshaller{} interface
func (req *AddBucketsRequest) Name() string {
	return "addBucketsRequest"
}

// ContentType implement MessageMarshaller{} interface
func (req *AddBucketsRequest) ContentType() string {
	return "application/protobuf"
}

// Encode implement MessageMarshaller{} interface
func (req *AddBucketsRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

// Decode implement MessageMarshaller{} interface
func (req *AddBucketsRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

// GetEvaluators impelement Subscriber{} interface
func (req *AddBucketsRequest) GetEvaluators() (map[uint64]c.Evaluator, error) {
	return getEvaluators(req.GetInstances())
}

// GetRouters impelement Subscriber{} interface
func (req *AddBucketsRequest) GetRouters() (map[uint64]c.Router, error) {
	return getRouters(req.GetInstances())
}

// *****************
// DelBucketsRequest
// *****************

// NewDelBucketsRequest creates an DelBucketsRequest
// for topic to add one or more new instances/engines to a topic.
func NewDelBucketsRequest(
	topic string, buckets []string) *DelBucketsRequest {

	return &DelBucketsRequest{
		Topic:   proto.String(topic),
		Buckets: buckets,
	}
}

// Name implement MessageMarshaller{} interface
func (req *DelBucketsRequest) Name() string {
	return "delBucketsRequest"
}

// ContentType implement MessageMarshaller{} interface
func (req *DelBucketsRequest) ContentType() string {
	return "application/protobuf"
}

// Encode implement MessageMarshaller{} interface
func (req *DelBucketsRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

// Decode implement MessageMarshaller{} interface
func (req *DelBucketsRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

// *******************
// AddInstancesRequest
// *******************

// NewAddInstancesRequest creates an AddInstancesRequest
// for topic to add one or more new instances/engines to a topic.
func NewAddInstancesRequest(
	topic string, instances []*Instance) *AddInstancesRequest {

	return &AddInstancesRequest{
		Topic:     proto.String(topic),
		Instances: instances,
	}
}

// Name implement MessageMarshaller{} interface
func (req *AddInstancesRequest) Name() string {
	return "addInstancesRequest"
}

// ContentType implement MessageMarshaller{} interface
func (req *AddInstancesRequest) ContentType() string {
	return "application/protobuf"
}

// Encode implement MessageMarshaller{} interface
func (req *AddInstancesRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

// Decode implement MessageMarshaller{} interface
func (req *AddInstancesRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

// GetEvaluators impelement Subscriber{} interface
func (req *AddInstancesRequest) GetEvaluators() (map[uint64]c.Evaluator, error) {
	return getEvaluators(req.GetInstances())
}

// GetRouters impelement Subscriber{} interface
func (req *AddInstancesRequest) GetRouters() (map[uint64]c.Router, error) {
	return getRouters(req.GetInstances())
}

// *******************
// DelInstancesRequest
// *******************

// NewDelInstancesRequest creates an DelInstancesRequest
// for topic to add one or more new instances/engines to a topic.
func NewDelInstancesRequest(topic string, uuids []uint64) *DelInstancesRequest {
	return &DelInstancesRequest{
		Topic:       proto.String(topic),
		InstanceIds: uuids,
	}
}

// Name implement MessageMarshaller{} interface
func (req *DelInstancesRequest) Name() string {
	return "delInstancesRequest"
}

// ContentType implement MessageMarshaller{} interface
func (req *DelInstancesRequest) ContentType() string {
	return "application/protobuf"
}

// Encode implement MessageMarshaller{} interface
func (req *DelInstancesRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

// Decode implement MessageMarshaller{} interface
func (req *DelInstancesRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

// **********************
// RepairEndpointsRequest
// **********************

// NewRepairEndpoints creates a RepairEndpointsRequest
// for a topic's one or more endpoints.
func NewRepairEndpointsRequest(
	topic string, endpoints []string) *RepairEndpointsRequest {

	return &RepairEndpointsRequest{
		Topic:     proto.String(topic),
		Endpoints: endpoints,
	}
}

// Name implement MessageMarshaller{} interface
func (req *RepairEndpointsRequest) Name() string {
	return "repairEndpointsRequest"
}

// ContentType implement MessageMarshaller{} interface
func (req *RepairEndpointsRequest) ContentType() string {
	return "application/protobuf"
}

// Encode implement MessageMarshaller{} interface
func (req *RepairEndpointsRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

// Decode implement MessageMarshaller{} interface
func (req *RepairEndpointsRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

// *************************
// ShutdownTopicRequest
// *************************

// NewShutdownTopicRequest creates a ShutdownTopicRequest
// for a topic's one or more endpoints.
func NewShutdownTopicRequest(topic string) *ShutdownTopicRequest {
	return &ShutdownTopicRequest{Topic: proto.String(topic)}
}

// Name implement MessageMarshaller{} interface
func (req *ShutdownTopicRequest) Name() string {
	return "shutdownTopicRequest"
}

// ContentType implement MessageMarshaller{} interface
func (req *ShutdownTopicRequest) ContentType() string {
	return "application/protobuf"
}

// Encode implement MessageMarshaller{} interface
func (req *ShutdownTopicRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

// Decode implement MessageMarshaller{} interface
func (req *ShutdownTopicRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

//-- local functions

// TODO: add other types of engines
func getEvaluators(instances []*Instance) (map[uint64]c.Evaluator, error) {
	engines := make(map[uint64]c.Evaluator)
	for _, instance := range instances {
		uuid := instance.GetUuid()
		if val := instance.GetIndexInstance(); val != nil {
			ie, err := NewIndexEvaluator(val)
			if err != nil {
				return nil, err
			}
			engines[uuid] = ie
		} else {
			//TODO: should we panic ?
		}
	}
	return engines, nil
}

// TODO: add other types of engines
func getRouters(instances []*Instance) (map[uint64]c.Router, error) {
	routers := make(map[uint64]c.Router)
	for _, instance := range instances {
		uuid := instance.GetUuid()
		if val := instance.GetIndexInstance(); val != nil {
			routers[uuid] = val
		} else {
			//TODO: should we panic ?
		}
	}
	return routers, nil
}

// GetUuid will get unique-id for this instance.
func (instance *Instance) GetUuid() (uuid uint64) {
	if val := instance.GetIndexInstance(); val != nil {
		return val.GetInstId()
	} else {
		// TODO: should we panic ?
	}
	return
}

// GetBucket will get bucket-name in which instance is defined.
func (instance *Instance) GetBucket() (bucket string) {
	if val := instance.GetIndexInstance(); val != nil {
		return val.GetDefinition().GetBucket()
	} else {
		// TODO: should we panic ?
	}
	return
}
