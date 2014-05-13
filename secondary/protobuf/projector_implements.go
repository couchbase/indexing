package protobuf

import (
	c "github.com/couchbase/indexing/secondary/common"
)

// Mask values for stream flag
const (
	// start vbucket-streams if it is not already active.
	maskMutationStreamStart = 0x00000001

	// start vbucket-streams whether it is already active or not.
	maskMutationStreamRestart = 0x00000002

	// shutdown vbucket-streams
	maskMutationStreamShutdown = 0x00000004

	// update subscription
	maskUpdateStreamSubscription = 0x00000020

	// delete subscription
	maskDeleteStreamSubscription = 0x00000010
)

// Interface API for RequestReader and Subscriber

func (req *MutationStreamRequest) IsStart() bool {
	flag := req.GetFlag()
	return (flag & maskMutationStreamStart) > 0
}

func (req *MutationStreamRequest) IsRestart() bool {
	flag := req.GetFlag()
	return (flag & maskMutationStreamRestart) > 0
}

func (req *MutationStreamRequest) IsShutdown() bool {
	flag := req.GetFlag()
	return (flag & maskMutationStreamShutdown) > 0
}

func (req *MutationStreamRequest) IsUpdateSubscription() bool {
	flag := req.GetFlag()
	return (flag & maskUpdateStreamSubscription) > 0
}

func (req *MutationStreamRequest) IsDeleteSubscription() bool {
	flag := req.GetFlag()
	return (flag & maskDeleteStreamSubscription) > 0
}

func (req *MutationStreamRequest) RestartTimestamp(bucket string) *c.Timestamp {
	restartTimestamps := req.GetRestartTimestamps()
	for i, b := range req.GetBuckets() {
		if bucket == b {
			return ToTimestamp(restartTimestamps[i])
		}
	}
	return nil
}

func (req *MutationStreamRequest) GetEvaluators() (map[uint64]c.Evaluator, error) {
	return getEvaluators(req.GetIndexes())
}

func (req *MutationStreamRequest) GetRouters() (map[uint64]c.Router, error) {
	return getRouters(req.GetIndexes())
}

// interface API for RequestReader and Subscriber

func (req *UpdateMutationStreamRequest) IsStart() bool {
	flag := req.GetFlag()
	return (flag & maskMutationStreamStart) > 0
}

func (req *UpdateMutationStreamRequest) IsRestart() bool {
	flag := req.GetFlag()
	return (flag & maskMutationStreamRestart) > 0
}

func (req *UpdateMutationStreamRequest) IsShutdown() bool {
	flag := req.GetFlag()
	return (flag & maskMutationStreamShutdown) > 0
}

func (req *UpdateMutationStreamRequest) IsDeleteSubscription() bool {
	flag := req.GetFlag()
	return (flag & maskDeleteStreamSubscription) > 0
}

func (req *UpdateMutationStreamRequest) IsUpdateSubscription() bool {
	flag := req.GetFlag()
	return (flag & maskUpdateStreamSubscription) > 0
}

func (req *UpdateMutationStreamRequest) RestartTimestamp(bucket string) *c.Timestamp {
	restartTimestamps := req.GetRestartTimestamps()
	for i, b := range req.GetBuckets() {
		if bucket == b {
			return ToTimestamp(restartTimestamps[i])
		}
	}
	return nil
}

func (req *UpdateMutationStreamRequest) GetEvaluators() (map[uint64]c.Evaluator, error) {
	return getEvaluators(req.GetIndexes())
}

func (req *UpdateMutationStreamRequest) GetRouters() (map[uint64]c.Router, error) {
	return getRouters(req.GetIndexes())
}

// interface API for flags and Subscriber

func (req *SubscribeStreamRequest) IsUpdateSubscription() bool {
	flag := req.GetFlag()
	return (flag & maskUpdateStreamSubscription) > 0
}

func (req *SubscribeStreamRequest) IsDeleteSubscription() bool {
	flag := req.GetFlag()
	return (flag & maskDeleteStreamSubscription) > 0
}

func (req *SubscribeStreamRequest) GetEvaluators() (map[uint64]c.Evaluator, error) {
	return getEvaluators(req.GetIndexes())
}

func (req *SubscribeStreamRequest) GetRouters() (map[uint64]c.Router, error) {
	return getRouters(req.GetIndexes())
}

// TODO: add other types of entities
func getEvaluators(indexes []*Index) (map[uint64]c.Evaluator, error) {
	var err error

	entities := make(map[uint64]c.Evaluator, 0)
	for _, index := range indexes {
		info := index.GetIndexinfo()
		ie := NewIndexEvaluator(index)
		if err = ie.Compile(); err != nil {
			return nil, err
		}
		entities[info.GetUuid()] = ie
	}
	return entities, nil
}

// TODO: add other types of entities
func getRouters(indexes []*Index) (map[uint64]c.Router, error) {
	entities := make(map[uint64]c.Router, 0)
	for _, index := range indexes {
		info := index.GetIndexinfo()
		entities[info.GetUuid()] = index
	}
	return entities, nil
}
