// TODO: clean up this file

package common

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"
)

// Statistics provide a type and method receivers for marshalling and
// un-marshalling statistics, as JSON, for components across the network.
type Statistics map[string]interface{}

// NewStatistics return a new instance of stat structure initialized with
// data.
func NewStatistics(data interface{}) (stat Statistics, err error) {
	var statm Statistics

	switch v := data.(type) {
	case string:
		statm = make(Statistics)
		err = json.Unmarshal([]byte(v), &statm)
	case []byte:
		statm = make(Statistics)
		err = json.Unmarshal(v, &statm)
	case map[string]interface{}:
		statm = Statistics(v)
	case nil:
		statm = make(Statistics)
	}
	return statm, err
}

// Name is part of MessageMarshaller interface.
func (s Statistics) Name() string {
	return "stats"
}

// Encode is part of MessageMarshaller interface.
func (s Statistics) Encode() (data []byte, err error) {
	data, err = json.Marshal(s)
	return
}

// Decode is part of MessageMarshaller interface.
func (s Statistics) Decode(data []byte) (err error) {
	return json.Unmarshal(data, &s)
}

// ContentType is part of MessageMarshaller interface.
func (s Statistics) ContentType() string {
	return "application/json"
}

// Statistic operations.

// Incr increments stat value(s) by `vals`.
func (s Statistics) Incr(path string, vals ...int) {
	l := len(vals)
	if l == 0 {
		logging.Warnf("Incr called without value")
		return
	}

	switch vs := s[path].(type) {
	case float64:
		s[path] = vs + float64(vals[0])

	case []interface{}:
		if l != len(vs) {
			logging.Warnf("Incr expected %v values, got %v", len(vs), l)
			return
		}
		for i, v := range vs {
			vs[i] = v.(float64) + float64(vals[i])
		}

	case []float64:
		if l != len(vs) {
			logging.Warnf("Incr expected %v values, got %v", len(vs), l)
			return
		}
		for i, v := range vs {
			vs[i] = v + float64(vals[i])
		}
	}
}

// Decr increments stat value(s) by `vals`.
func (s Statistics) Decr(path string, vals ...int) {
	l := len(vals)
	if l == 0 {
		logging.Warnf("Decr called without value")
		return
	}

	switch vs := s[path].(type) {
	case float64:
		s[path] = vs - float64(vals[0])

	case []interface{}:
		if l != len(vs) {
			logging.Warnf("Decr expected %v values, got %v", len(vs), l)
			return
		}
		for i, v := range vs {
			vs[i] = v.(float64) - float64(vals[i])
		}

	case []float64:
		if l != len(vs) {
			logging.Warnf("Incr expected %v values, got %v", len(vs), l)
			return
		}
		for i, v := range vs {
			vs[i] = v - float64(vals[i])
		}
	}
}

// Set stat value
func (s Statistics) Set(path string, val interface{}) {
	s[path] = val
}

// Get stat value
func (s Statistics) Get(path string) interface{} {
	return s[path]
}

// ToMap converts Statistics to map.
func (s Statistics) ToMap() map[string]interface{} {
	return map[string]interface{}(s)
}

// Lines will convert JSON to human readable list of statistics.
func (s Statistics) Lines() string {
	return valueString("", s)
}

func valueString(prefix string, val interface{}) string {
	// a shot in the dark, may be val is a map.
	m, ok := val.(map[string]interface{})
	if !ok {
		stats, ok := val.(Statistics) // or val is a Statistics
		if ok {
			m = map[string]interface{}(stats)
		}
	}
	switch v := val.(type) {
	case map[string]interface{}, Statistics:
		keys := make([]string, 0, len(m))
		for key := range m {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		ss := make([]string, 0, len(m))
		for _, key := range keys {
			val := m[key]
			ss = append(ss, valueString(fmt.Sprintf("%s%s.", prefix, key), val))
		}
		return strings.Join(ss, "\n")

	case []interface{}:
		ss := make([]string, 0, len(v))
		for i, x := range v {
			ss = append(ss, fmt.Sprintf("%s%d : %s", prefix, i, x))
		}
		return strings.Join(ss, "\n")

	default:
		prefix = strings.Trim(prefix, ".")
		return fmt.Sprintf("%v : %v", prefix, val)
	}
}

func GetStatsPrefix(bucket, scope, collection, index string, replicaId, partnId int, isPartn bool) string {
	var name string
	if isPartn {
		name = FormatIndexPartnDisplayName(index, replicaId, partnId, isPartn)
	} else {
		name = FormatIndexInstDisplayName(index, replicaId)
	}

	var strs []string
	if scope == DEFAULT_SCOPE && collection == DEFAULT_COLLECTION {
		strs = []string{bucket, name, ""}
	} else if scope == "" && collection == "" {
		// TODO: Eventually, we need to remove this hack.
		strs = []string{bucket, name, ""}
	} else {
		strs = []string{bucket, scope, collection, name, ""}
	}

	return strings.Join(strs, ":")
}

// Note that the input prefix passed to this function should be exactly
// the same as return value of GetStatsPrefix()
func GetIndexStatKey(prefix, stat string) string {
	return strings.Join([]string{prefix, stat}, "")
}

type StatsIndexSpec struct {
	Instances []IndexInstId `json:"instances,omitempty"`
}

func (spec *StatsIndexSpec) GetInstances() []IndexInstId {
	if spec == nil {
		return nil
	}

	return spec.Instances
}

const STATS_LOG_DUR = 30 * time.Second // minimum duration of a stats action before logging it

// GetIndexStats gets the index and/or indexer stats selected by the given filter from all indexer
// nodes in parallel (generalized from planner/proxy.go getIndexStats, which is integrated with
// Planner data structures) and returns them in a map from nodeUUID to stats. The filter strings
// can be found in stats_manager.go statsFilterMap. If there is any error, the first one will be
// returned in err (decorated with its nodeUUID if it is from a stats REST call). If some REST
// calls succeeded and others failed, the successful ones will be returned in statsMap while
// errors from the failing ones will be returned in errMap (and the first one decorated in err).
func GetIndexStats(clusterURL string, filter string, httpTimeoutSecs uint32) (statsMap map[string]*Statistics, errMap map[string]error, err error) {
	const method string = "stats::GetIndexStats:" // for logging

	timeStart := time.Now()
	cinfo, err := FetchNewClusterInfoCache2(clusterURL, DEFAULT_POOL, "GetIndexStats")
	timeFinish := time.Now()
	if dur := timeFinish.Sub(timeStart); dur >= STATS_LOG_DUR {
		logging.Warnf("%v Slow: FetchNewClusterInfoCache2 %v", method, dur)
	}
	if err != nil {
		logging.Errorf("%v Error fetching cluster info cache: %v", method, err)
		return nil, nil, err
	}

	timeStart = timeFinish
	err = cinfo.FetchNodesAndSvsInfo()
	timeFinish = time.Now()
	if dur := timeFinish.Sub(timeStart); dur >= STATS_LOG_DUR {
		logging.Warnf("%v Slow: FetchNodesAndSvsInfo %v", method, dur)
	}
	if err != nil {
		logging.Errorf("%v Error fetching node and service info: %v", method, err)
		return nil, nil, err
	}

	timeStart = timeFinish
	nids := cinfo.GetNodeIdsByServiceType(INDEX_HTTP_SERVICE)
	timeFinish = time.Now()
	if dur := timeFinish.Sub(timeStart); dur >= STATS_LOG_DUR {
		logging.Warnf("%v Slow: GetNodeIdsByServiceType %v", method, dur)
	}

	timeStart = timeFinish
	statsMap, errMap = parallelStatsRestCall(cinfo, nids, filter, httpTimeoutSecs)
	for nodeUUID, nodeErr := range errMap {
		err = fmt.Errorf("%v Error retrieving stats for nodeUUID %v. Error: %v",
			method, nodeUUID, nodeErr)
		break // just wanted the first one found
	}
	timeFinish = time.Now()
	if dur := timeFinish.Sub(timeStart); dur >= STATS_LOG_DUR {
		logging.Warnf("%v Slow: parallelStatsRestCall %v", method, dur)
	}

	return statsMap, errMap, err
}

// parallelStatsRestCall makes the same stats REST call to all Index nodes in parallel and returns
// the results mapped by nodeUUID (string). A nodeUUID will be added to respMap on success or errMap
// on failure and will be left out of the other map. Adapted from planner/proxy.go restHelperNoLock.
func parallelStatsRestCall(cinfo *ClusterInfoCache, nids []NodeId, filter string, httpTimeoutSecs uint32) (respMap map[string]*Statistics, errMap map[string]error) {
	const method = "stats::parallelStatsRestCall:" // for logging

	respMap = make(map[string]*Statistics) // return 1: indexer stats by nodeUUID
	errMap = make(map[string]error)        // return 2: errors by nodeUUID
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, nid := range nids {
		nodeUUID := cinfo.GetNodeUUID(nid)

		// obtain the admin port for the indexer node
		addr, err := cinfo.GetServiceAddress(nid, INDEX_HTTP_SERVICE, true)
		if err != nil {
			logging.Errorf("%v Error getting service address for nodeUUID %v. Error: %v",
				method, nodeUUID, err)
			errMap[nodeUUID] = err
			continue
		}

		wg.Add(1)
		go restCall(nid, addr, filter, httpTimeoutSecs, nodeUUID, &mu, &wg, respMap, errMap)
	}

	wg.Wait()
	mu.Lock()
	mu.Unlock()

	return respMap, errMap
}

// restCall runs in a goroutine as a helper for parallelStatsRestCall, which launches one of these
// for each Index node so they can run in parallel.
func restCall(nid NodeId, addr string, filter string, httpTimeoutSecs uint32, nodeUUID string,
	mu *sync.Mutex, wg *sync.WaitGroup, respMap map[string]*Statistics, errMap map[string]error) {
	const method = "stats::restCall:" // for logging

	defer wg.Done()

	stats := new(Statistics)
	timeStart := time.Now()
	resp, err := restGetStats(addr, filter, httpTimeoutSecs) // make the REST call
	if dur := time.Since(timeStart); dur >= STATS_LOG_DUR {
		logging.Warnf("%v Slow: restGetStats %v for addr %v", method, dur, addr)
	}
	if err != nil {
		logging.Errorf("%v restGetStats for addr %v returned err: %v", method, addr, err)
	}
	if err == nil {
		err = security.ConvertHttpResponse(resp, stats)
		if err != nil {
			logging.Errorf("%v ConvertHttpResponse for addr %v returned err: %v",
				method, addr, err)
		}
	}

	mu.Lock()
	defer mu.Unlock()

	if err != nil {
		errMap[nodeUUID] = err
	} else {
		respMap[nodeUUID] = stats
	}
}

// restGetStats gets the marshalled index stats from a specific indexer host using the given filter.
// Adapted from planner/proxy.go getLocalStatsResp.
func restGetStats(addr string, filter string, httpTimeoutSecs uint32) (*http.Response, error) {

	var filterString string
	if filter != "" {
		filterString = "&consumerFilter=" + filter
	}

	resp, err := security.GetWithAuthAndTimeout(
		addr+"/stats?async=false&partition=true"+filterString, httpTimeoutSecs)
	if err != nil {
		logging.Warnf("stats::restGetStats: Failed to get the most recent stats from node: %v, err: %v"+
			" Try fetch cached stats.", addr, err)
		resp, err = security.GetWithAuthAndTimeout(
			addr+"/stats?async=true&partition=true"+filterString, httpTimeoutSecs)
		if err != nil {
			logging.Errorf("stats::restGetStats: Failed to get cached stats from node: %v, err: %v", addr, err)
			return nil, err
		}
	}
	return resp, nil
}

// 0-2ms, 2-10ms, 10-100ms, 100-500ms, 500ms-1s, 1-10s, 10-30s, 30s-Inf
var PortBlockDist = []int64{0, 2, 10, 100, 500, 1000, 10000, 30000}
var PortBlockdThresholdDur = int64(time.Duration(PortBlockDist[len(PortBlockDist)-1]) * time.Millisecond)
