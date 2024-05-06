package projector

import (
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"unsafe"

	c "github.com/couchbase/indexing/secondary/common"
)

type EngineMap map[uint64]*Engine //Index instanceId -> Engine mapping

type CollectionsEngineMapHolder struct {
	ptr unsafe.Pointer
}

func (e *CollectionsEngineMapHolder) Get() map[uint32]EngineMap {
	if ptr := atomic.LoadPointer(&e.ptr); ptr != nil {
		return *(*map[uint32]EngineMap)(atomic.LoadPointer(&e.ptr))
	} else {
		return make(map[uint32]EngineMap) // return empty map
	}
}

func (e *CollectionsEngineMapHolder) Set(engines map[uint32]EngineMap) {
	atomic.StorePointer(&e.ptr, unsafe.Pointer(&engines))
}

func CloneEngines(engines map[uint32]EngineMap) map[uint32]EngineMap {
	clone := make(map[uint32]EngineMap)
	for collId, enginesPerColl := range engines {
		if _, ok := clone[collId]; !ok {
			clone[collId] = make(EngineMap)
		}
		for instId, engine := range enginesPerColl {
			clone[collId][instId] = engine
		}
	}
	return clone
}

type EndpointMapHolder struct {
	ptr unsafe.Pointer
}

func (e *EndpointMapHolder) Get() map[string]c.RouterEndpoint {
	if ptr := atomic.LoadPointer(&e.ptr); ptr != nil {
		return *(*map[string]c.RouterEndpoint)(atomic.LoadPointer(&e.ptr))
	} else {
		return make(map[string]c.RouterEndpoint) // return empty map
	}
}

func (e *EndpointMapHolder) Set(endpoints map[string]c.RouterEndpoint) {
	atomic.StorePointer(&e.ptr, unsafe.Pointer(&endpoints))
}

func CloneEndpoints(endpoints map[string]c.RouterEndpoint) map[string]c.RouterEndpoint {
	clone := make(map[string]c.RouterEndpoint)
	for k, v := range endpoints {
		clone[k] = v
	}
	return clone
}

type VbucketMapHolder struct {
	ptr unsafe.Pointer
}

func (v *VbucketMapHolder) Get() map[uint16]*Vbucket {
	if ptr := atomic.LoadPointer(&v.ptr); ptr != nil {
		return *(*map[uint16]*Vbucket)(ptr)
	} else {
		return make(map[uint16]*Vbucket) // return empty map
	}
}

func (v *VbucketMapHolder) Set(vbmap map[uint16]*Vbucket) {
	atomic.StorePointer(&v.ptr, unsafe.Pointer(&vbmap))
}

func CloneVbucketMap(vbmap map[uint16]*Vbucket) map[uint16]*Vbucket {
	clone := make(map[uint16]*Vbucket)
	for k, v := range vbmap {
		clone[k] = v
	}
	return clone
}

func getKvdtLogPrefix(topic, keyspace string, opaque uint16) string {
	return strings.Join([]string{
		"KVDT",
		topic,
		keyspace,
		fmt.Sprintf("##%x", opaque),
	}, ":")
}

func getWorkerStatPrefix(topic, keyspace string, opaque uint16) string {
	return strings.Join([]string{
		"WRKR",
		topic,
		keyspace,
		fmt.Sprintf("##%x", opaque),
	}, ":")
}

func getWorkerLogPrefix(topic, keyspace string, opaque uint16, workerId int) string {
	return strings.Join([]string{
		getWorkerStatPrefix(topic, keyspace, opaque),
		strconv.Itoa(workerId),
	}, ":")
}

func getEvalStatsLogPRefix(topic, keyspace string, opaque uint16) string {
	return strings.Join([]string{
		"EVAL",
		topic,
		keyspace,
		fmt.Sprintf("##%x", opaque),
	}, ":")
}
