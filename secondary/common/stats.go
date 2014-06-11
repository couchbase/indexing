// ComponentStat provide a type and method receivers for marshalling and
// un-marshalling statistics for components across the network.
//
// Example client {
//    client := NewHTTPClient("http://localhost:8888", "/adminport/")
//    req  := common.ComponentStat{"componentName": "indexer"}
//    client.Request(req, req)
// }
//
// Note:
//  - component statistics are marshalled and unmarshalled as JSON
//  - JSON interprets integers as float.

package common

import (
	"encoding/json"
)

// ComponentStat is unmarshalled JSON and represented using Golang's type
// system.
//
// Mandatory fields in ComponentStat,
//  "componentName", name of the component that provide statistics for itself.
type ComponentStat map[string]interface{}

// componentName -> stat-structure.
var statistics = make(map[string]*ComponentStat)

// GetStat for registered name.
func GetStat(name string) *ComponentStat {
	return statistics[name]
}

// StatsEncode marshal all component's statistics.
func StatsEncode() ([]byte, error) {
	return json.Marshal(&statistics)
}

// NewComponentStat return a new instance of stat structure initialized with
// data and the same will be registered under component-name `name`.
func NewComponentStat(name string, data interface{}) (stat *ComponentStat, err error) {
	var statm ComponentStat

	switch v := data.(type) {
	case string:
		statm = make(ComponentStat)
		err = json.Unmarshal([]byte(v), &statm)
	case []byte:
		statm = make(ComponentStat)
		err = json.Unmarshal(v, &statm)
	case map[string]interface{}:
		statm = ComponentStat(v)
	case nil:
		statm = make(ComponentStat)
	}
	statistics[name] = &statm
	return &statm, err
}

// Name is part of MessageMarshaller interface.
func (s *ComponentStat) Name() string {
	return (*s)["componentName"].(string) + ".stat"
}

// Encode is part of MessageMarshaller interface.
func (s *ComponentStat) Encode() (data []byte, err error) {
	data, err = json.Marshal(s)
	return
}

// Decode is part of MessageMarshaller interface.
func (s *ComponentStat) Decode(data []byte) (err error) {
	return json.Unmarshal(data, s)
}

// ContentType is part of MessageMarshaller interface.
func (s *ComponentStat) ContentType() string {
	return "application/json"
}

// Statistic operations.

// Incr increments stat value by `val`
func (s *ComponentStat) Incr(key string, val int) {
	sval := *s
	sval[key] = sval[key].(float64) + float64(val)
}

// Incrs increments an array of stat value by `val`
func (s *ComponentStat) Incrs(key string, vals ...int) {
	sval := *s
	values := sval[key].([]float64)
	for i, val := range vals {
		values[i] += float64(val)
	}
}

// Decr increments stat value by `val`
func (s *ComponentStat) Decr(key string, val int) {
	sval := *s
	sval[key] = sval[key].(float64) - float64(val)
}
