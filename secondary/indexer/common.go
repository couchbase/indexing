//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package indexer

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

var ErrorNotImplemented = errors.New("Functionality not implemented")
var ErrorCodebookNotInitialized = errors.New("Codebook is not initialized")

type StreamAddressMap map[common.StreamId]common.Endpoint

type StreamStatus byte

const (
	//Stream is inactive i.e. not processing mutations
	STREAM_INACTIVE StreamStatus = iota
	//Stream is active i.e. processing mutations
	STREAM_ACTIVE
	//Stream is preparing for recovery(i.e. it has received
	//a control or error message and it is doing a cleanup
	//before initiating Catchup
	STREAM_PREPARE_RECOVERY
	//Prepare is done before recovery
	STREAM_PREPARE_DONE
	//Stream is using a Catchup to recover
	STREAM_RECOVERY
)

func (s StreamStatus) String() string {

	switch s {
	case STREAM_ACTIVE:
		return "STREAM_ACTIVE"
	case STREAM_INACTIVE:
		return "STREAM_INACTIVE"
	case STREAM_PREPARE_RECOVERY:
		return "STREAM_PREPARE_RECOVERY"
	case STREAM_PREPARE_DONE:
		return "STREAM_PREPARE_DONE"
	case STREAM_RECOVERY:
		return "STREAM_RECOVERY"
	default:
		return "STREAM_STATE_INVALID"
	}
}

// a generic channel which can be closed when you
// want someone to stop doing something
type StopChannel chan bool

// a generic channel which can be closed when you
// want to indicate the caller that you are done
type DoneChannel chan bool

type MsgChannel chan Message

type MutationChannel chan *MutationKeys

// IndexMutationQueue comprising of a mutation queue
// and a slab manager
type IndexerMutationQueue struct {
	queue   MutationQueue
	slabMgr SlabManager //slab allocator for mutation memory allocation
}

// IndexQueueMap is a map between IndexId and IndexerMutationQueue
type IndexQueueMap map[common.IndexInstId]IndexerMutationQueue

type Vbucket uint32
type Vbuuid uint64
type Seqno uint64

type Vbuckets []Vbucket

// Len implements sort.Interface{}.
func (vbuckets Vbuckets) Len() int {
	return len(vbuckets)
}

// Less implements sort.Interface{}.
func (vbuckets Vbuckets) Less(i, j int) bool {
	return vbuckets[i] < vbuckets[j]
}

// Swap implements sort.Interface{}
func (vbuckets Vbuckets) Swap(i, j int) {
	vbuckets[i], vbuckets[j] = vbuckets[j], vbuckets[i]
}

// MutationSnapshot represents snapshot information of KV
type MutationSnapshot struct {
	snapType uint32
	start    uint64
	end      uint64
}

func (m MutationSnapshot) String() string {

	str := fmt.Sprintf("Type: %v ", m.snapType)
	str += fmt.Sprintf("Start: %v ", m.start)
	str += fmt.Sprintf("End: %v ", m.end)

	return str

}

func (m MutationSnapshot) CanProcess() bool {
	// Snapshot marker can be processed only if
	// they belong to ondisk type or inmemory type.
	if m.snapType&(0x1|0x2) != 0 {
		return true
	}

	return false
}

// Represents storage stats for an index instance
type IndexStorageStats struct {
	InstId     common.IndexInstId
	PartnId    common.PartitionId
	Name       string
	Bucket     string
	Scope      string
	Collection string
	Stats      StorageStatistics
}

func (s *IndexStorageStats) String() string {
	return fmt.Sprintf("IndexInstId: %v Data:%v, Disk:%v, "+
		"ExtraSnapshotData:%v, Fragmentation:%v%%",
		s.InstId, s.Stats.DataSize, s.Stats.DiskSize,
		s.Stats.ExtraSnapDataSize, s.GetFragmentation())
}

func (s *IndexStorageStats) GetFragmentation() float64 {
	var fragPercent float64

	var wastedSpace int64
	if s.Stats.DataSize != 0 && s.Stats.DiskSize > s.Stats.DataSize {
		wastedSpace = s.Stats.DiskSize - s.Stats.DataSize
	}

	if s.Stats.DiskSize > 0 {
		fragPercent = float64(wastedSpace) * 100 / float64(s.Stats.DiskSize)
	}

	return fragPercent
}

func (s *IndexStorageStats) GetInternalData() []string {
	return s.Stats.InternalData
}

func (s *IndexStorageStats) GetInternalDataMap() map[string]interface{} {
	return s.Stats.InternalDataMap
}

func (s *IndexStorageStats) IsLoggingDisabled() bool {
	return s.Stats.LoggingDisabled
}

type VbStatus uint64

const (
	VBS_INIT = iota
	VBS_STREAM_BEGIN
	VBS_STREAM_END
	VBS_CONN_ERROR
	VBS_REPAIR
)

func (v VbStatus) String() string {
	switch v {
	case VBS_INIT:
		return "VBS_INIT"
	case VBS_STREAM_BEGIN:
		return "VBS_STREAM_BEGIN"
	case VBS_STREAM_END:
		return "VBS_STREAM_END"
	case VBS_CONN_ERROR:
		return "VBS_CONN_ERROR"
	case VBS_REPAIR:
		return "VBS_REPAIR"
	default:
		return "VBS_STATUS_INVALID"
	}
}

type MetaUpdateFields struct {
	state           bool
	stream          bool
	err             bool
	buildTs         bool
	rstate          bool
	partitions      bool
	version         bool
	partnShardIdMap common.PartnShardIdMap
	trainingPhase   bool
}

type EncodeCompatMode int

const (
	CHECK_VERSION EncodeCompatMode = iota
	FORCE_ENABLE
	FORCE_DISABLE
)

var gEncodeCompatMode EncodeCompatMode

// SplitKeyspaceId will return all three parts of a
// bucket:scope:collection key. If only the bucket name is
// present, the last two return values will be empty.
// If you only need the bucket name, use GetBucketFromKeyspaceId
// instead as it will perform better, especially when the
// input is a 3-part key.
func SplitKeyspaceId(keyspaceId string) (string, string, string) {

	var ret []string
	ret = strings.Split(keyspaceId, ":")

	if len(ret) == 3 {
		return ret[0], ret[1], ret[2]
	} else if len(ret) == 1 {
		return ret[0], "", ""
	} else {
		return "", "", ""
	}

}

// GetBucketFromKeyspaceId will return the bucket name from
// either a 1-part ("bucket") or 3-part ("bucket:scope:collection")
// key. It is optimized to stop splitting at the first colon.
func GetBucketFromKeyspaceId(keyspaceId string) string {
	return strings.SplitN(keyspaceId, ":", 2)[0]
}

func logSnapInfoAtTimeout(snapTs, reqTs *common.TsVbuuid, instId common.IndexInstId, caller string, lastSnapTime int64) {
	if snapTs == nil {
		logging.Infof("%v::logSnapInfoAtTimeout nil snapTs at timeout for instId: %v", caller, instId)
		return
	}

	if reqTs == nil {
		logging.Infof("%v::logSnapInfoAtTimeout nil reqTs at timeout for instId: %v", caller, instId)
		return
	}

	if snapTs.Bucket != reqTs.Bucket {
		logging.Infof("%v::logSnapInfoAtTimeout Mismatch in bucket names at timeout. "+
			"InstId: %v, snapTs.Bucket: %v, reqTs.Bucket: %v", caller, instId, snapTs.Bucket, reqTs.Bucket)
		return
	}

	if len(snapTs.Seqnos) > len(reqTs.Seqnos) {
		logging.Infof("%v::logSnapInfoAtTimeout Mismatch in seqnos length at timeout. "+
			"InstId: %v, snapTs.Seqnos: %v, reqTs.Seqnos: %v", caller, instId, snapTs.Seqnos, reqTs.Seqnos)
		return
	}

	for i, seqno := range snapTs.Seqnos {
		if seqno < reqTs.Seqnos[i] {
			logging.Infof("%v::logSnapInfoAtTimeout Vbucket seqno in snapTs is lagging reqTs at timeout. "+
				"InstId: %v, snapTs.Seqnos[%v]: %v, reqTs.Seqnos[%v]: %v", caller, instId, i, snapTs.Seqnos[i], i, reqTs.Seqnos[i])

			if time.Now().UnixNano()-lastSnapTime > int64(60*time.Second) {
				logging.Infof("%v::logSnapInfoAtTimeout No snapshot has been generated since last 60 seconds, snapTs.Seqnos: %v, reqTs.Seqnos: %v",
					caller, snapTs.Seqnos, reqTs.Seqnos)
			}
			return
		}
	}
}
