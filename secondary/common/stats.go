//
// Example client {
//    client := NewHTTPClient("http://localhost:8888", "/adminport/")
//    req  := common.Statistics{"componentName": "indexer"}
//    client.Request(req, req)
// }
//
// Note:
//  1. statistics key should not have "/" character.

package common

import (
	"encoding/json"
	"regexp"
	"strings"
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
		Warnf("Incr called without value")
		return
	}

	switch vs := s[path].(type) {
	case float64:
		s[path] = vs + float64(vals[0])

	case []interface{}:
		if l != len(vs) {
			Warnf("Incr expected %v values, got %v", len(vs), l)
			return
		}
		for i, v := range vs {
			vs[i] = v.(float64) + float64(vals[i])
		}

	case []float64:
		if l != len(vs) {
			Warnf("Incr expected %v values, got %v", len(vs), l)
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
		Warnf("Decr called without value")
		return
	}

	switch vs := s[path].(type) {
	case float64:
		s[path] = vs - float64(vals[0])

	case []interface{}:
		if l != len(vs) {
			Warnf("Decr expected %v values, got %v", len(vs), l)
			return
		}
		for i, v := range vs {
			vs[i] = v.(float64) - float64(vals[i])
		}

	case []float64:
		if l != len(vs) {
			Warnf("Incr expected %v values, got %v", len(vs), l)
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

// StatsURLPath construct url path for statistics.
// TODO: make stats-path configurable.
func StatsURLPath(prefix, path string) string {
	prefix = strings.TrimRight(prefix, UrlSep)
	return strings.Join([]string{prefix, "stats", path}, UrlSep)
}

var regxStatPath, _ = regexp.Compile(`(.*)/stats/(.*)`)

// ParseStatsPath is opposite of StatsURLPath
func ParseStatsPath(urlPath string) string {
	matches := regxStatPath.FindStringSubmatch(urlPath)
	if len(matches) != 3 {
		Fatalf("ParseStatsPath(%q)\n", urlPath)
	}
	return matches[2]
}
