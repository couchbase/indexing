// TODO: clean up this file

package common

import "encoding/json"
import "sort"
import "fmt"
import "strings"
import "github.com/couchbase/indexing/secondary/logging"

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
