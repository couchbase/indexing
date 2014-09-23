package protobuf

import (
	"fmt"
	"strings"

	"github.com/couchbaselabs/goprotobuf/proto"
	mc "github.com/couchbase/gomemcached/client"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbaselabs/go-couchbase"
)

// NewMutationStreamResponse creates a new instance of MutationStreamResponse
// that can be sent back to the client.
func NewMutationStreamResponse(req interface{}) *MutationStreamResponse {
	var m *MutationStreamResponse
	switch val := req.(type) {
	case *MutationStreamRequest:
		indexes := val.GetInstances()
		uuids := make([]uint64, 0, len(indexes))
		for _, index := range indexes {
			uuids = append(uuids, index.GetDefinition().GetDefnID())
		}
		m = &MutationStreamResponse{
			Topic:      proto.String(val.GetTopic()),
			Flag:       proto.Uint32(val.GetFlag()),
			Pools:      val.GetPools(),
			Buckets:    val.GetBuckets(),
			IndexUuids: uuids,
		}

	case *UpdateMutationStreamRequest:
		m = &MutationStreamResponse{
			Topic:   proto.String(val.GetTopic()),
			Flag:    proto.Uint32(val.GetFlag()),
			Pools:   val.GetPools(),
			Buckets: val.GetBuckets(),
		}
	}
	return m
}

// UpdateErr update error value in response's.
func (m *MutationStreamResponse) UpdateErr(err error) {
	m.Err = NewError(err)
}

// UpdateTimestamps update timestamps value in response.
func (m *MutationStreamResponse) UpdateTimestamps(failoverTs, kvTs []*TsVbuuid) {
	m.FailoverTimestamps = failoverTs
	m.KvTimestamps = kvTs
}

// VbmapRequest implement MessageMarshaller interface
func (req *VbmapRequest) Name() string {
	return "vbmapRequest"
}

func (req *VbmapRequest) ContentType() string {
	return "application/protobuf"
}

func (req *VbmapRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *VbmapRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

// VbmapResponse implement MessageMarshaller interface
func (res *VbmapResponse) Name() string {
	return "vbmapResponse"
}

func (res *VbmapResponse) ContentType() string {
	return "application/protobuf"
}

func (res *VbmapResponse) Encode() (data []byte, err error) {
	return proto.Marshal(res)
}

func (res *VbmapResponse) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, res)
}

// LocateVbucket will identify the kvnode from VbmapResponse that is hosting
// the vbucket `vbno`.
// Return the address of the kvnode if vbucket is successfully located, else
// return empty string.
func (res *VbmapResponse) LocateVbucket(vbno uint32) string {
	for i, kvaddr := range res.GetKvaddrs() {
		for _, v := range res.GetKvvbnos()[i].Vbnos {
			if v == vbno {
				return kvaddr
			}
		}
	}
	return ""
}

func (res *VbmapResponse) Vbuckets32() []uint32 {
	vbs := make([]uint32, 0)
	for _, vs := range res.GetKvvbnos() {
		vbs = append(vbs, vs.GetVbnos()...)
	}
	return vbs
}

func (res *VbmapResponse) Vbuckets16() []uint16 {
	return c.Vbno32to16(res.Vbuckets32())
}

// FailoverLogRequest implement MessageMarshaller interface
func (req *FailoverLogRequest) Name() string {
	return "failoverLogRequest"
}

func (req *FailoverLogRequest) ContentType() string {
	return "application/protobuf"
}

func (req *FailoverLogRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *FailoverLogRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

// FailoverLogResponse implement MessageMarshaller interface
func (res *FailoverLogResponse) Name() string {
	return "failoverLogResponse"
}

func (res *FailoverLogResponse) ContentType() string {
	return "application/protobuf"
}

func (res *FailoverLogResponse) Encode() (data []byte, err error) {
	return proto.Marshal(res)
}

func (res *FailoverLogResponse) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, res)
}

func (res *FailoverLogResponse) LatestBranch() map[uint16]uint64 {
	vbuuids := make(map[uint16]uint64)
	for _, flog := range res.GetLogs() {
		vbno := uint16(flog.GetVbno())
		vbuuids[vbno] = flog.Vbuuids[len(flog.Vbuuids)-1]
	}
	return vbuuids
}

func (res *FailoverLogResponse) ToFailoverLog(vbnos []uint16) couchbase.FailoverLog {
	flogs := make(couchbase.FailoverLog)
	for _, f := range res.GetLogs() {
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
	return couchbase.FailoverLog(flogs)
}

// MutationStreamRequest implement MessageMarshaller interface
func (req *MutationStreamRequest) Name() string {
	return "mutationStreamRequest"
}

func (req *MutationStreamRequest) ContentType() string {
	return "application/protobuf"
}

func (req *MutationStreamRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *MutationStreamRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

// MutationStreamResponse implement MessageMarshaller interface
func (res *MutationStreamResponse) Name() string {
	return "mutationStreamResponse"
}

func (res *MutationStreamResponse) ContentType() string {
	return "application/protobuf"
}

func (res *MutationStreamResponse) Encode() (data []byte, err error) {
	return proto.Marshal(res)
}

func (res *MutationStreamResponse) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, res)
}

func (res *MutationStreamResponse) Repr() string {
	err := res.GetErr().GetError()
	topic := res.GetTopic()
	if err != "" {
		return fmt.Sprintf("[MutationStreamResponse:%s] error %q", topic, err)
	}
	buckets := res.GetBuckets()
	kvtss := res.GetKvTimestamps()
	lines := []string{}
	for i, bucket := range buckets {
		kvts := kvtss[i]
		line := fmt.Sprintf(
			"[MutationStreamResponse:%s:%s] %v", topic, bucket, kvts)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

// UpdateMutationStreamRequest implement MessageMarshaller interface
func (req *UpdateMutationStreamRequest) Name() string {
	return "updateMutationStreamRequest"
}

func (req *UpdateMutationStreamRequest) ContentType() string {
	return "application/protobuf"
}

func (req *UpdateMutationStreamRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *UpdateMutationStreamRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

// SubscribeStreamRequest implement MessageMarshaller interface
func (req *SubscribeStreamRequest) Name() string {
	return "subscribeStreamRequest"
}

func (req *SubscribeStreamRequest) ContentType() string {
	return "application/protobuf"
}

func (req *SubscribeStreamRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *SubscribeStreamRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

// RepairDownstreamEndpoints implement MessageMarshaller interface
func (req *RepairDownstreamEndpoints) Name() string {
	return "repairDownstreamEndpoints"
}

func (req *RepairDownstreamEndpoints) ContentType() string {
	return "application/protobuf"
}

func (req *RepairDownstreamEndpoints) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *RepairDownstreamEndpoints) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

// ShutdownStreamRequest implement MessageMarshaller interface
func (req *ShutdownStreamRequest) Name() string {
	return "shutdownStreamRequest"
}

func (req *ShutdownStreamRequest) ContentType() string {
	return "application/protobuf"
}

func (req *ShutdownStreamRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *ShutdownStreamRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}
