// ComponentStat provide a type and method receivers for marshalling and
// un-marshalling statistics for components across the network.
//
// Example client {
//    client := NewHTTPClient("http://localhost:8888")
//    req  := &common.ComponentStat{"componentName": "indexer"}
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

// Name is part of MessageMarshaller interface.
func (s ComponentStat) Name() string {
	return "stats/" + s["componentName"].(string)
}

// Encode is part of MessageMarshaller interface.
func (s ComponentStat) Encode() (data []byte, err error) {
	data, err = json.Marshal(&s)
	return
}

// Decode is part of MessageMarshaller interface.
func (s ComponentStat) Decode(data []byte) (err error) {
	return json.Unmarshal(data, &s)
}

// ContentType is part of MessageMarshaller interface.
func (s ComponentStat) ContentType() string {
	return "application/json"
}

// Statistic operations.

// Incr increments stat value by `val`
func (s ComponentStat) Incr(key string, val int) {
	value := s[key].(uint64)
	s[key] = value + uint64(val)
}

// Incrs increments an array of stat value by `val`
func (s ComponentStat) Incrs(key string, vals ...int) {
	values := s[key].([]uint64)
	for i, val := range vals {
		values[i] += uint64(val)
	}
	s[key] = values
}
