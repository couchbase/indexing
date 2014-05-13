package protobuf

import (
	"code.google.com/p/goprotobuf/proto"
)

// NewMutationStreamResponse creates a new instance of MutationStreamResponse
// that can be sent back to the client.
func NewMutationStreamResponse(req interface{}) *MutationStreamResponse {
	var m *MutationStreamResponse
	switch val := req.(type) {
	case *MutationStreamRequest:
		indexes := val.GetIndexes()
		uuids := make([]uint64, 0, len(indexes))
		for _, index := range indexes {
			uuids = append(uuids, index.GetIndexinfo().GetUuid())
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
func (m *MutationStreamResponse) UpdateTimestamps(failoverTs, kvTs []*BranchTimestamp) {
	m.FailoverTimestamps = failoverTs
	m.KvTimestamps = kvTs
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

func (req *FailoverLogResponse) Name() string {
	return "failoverLogResponse"
}

func (req *FailoverLogResponse) ContentType() string {
	return "application/protobuf"
}

func (req *FailoverLogResponse) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *FailoverLogResponse) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
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

func (req *MutationStreamResponse) Name() string {
	return "mutationStreamResponse"
}

func (req *MutationStreamResponse) ContentType() string {
	return "application/protobuf"
}

func (req *MutationStreamResponse) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *MutationStreamResponse) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
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
