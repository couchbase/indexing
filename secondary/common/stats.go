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
	"github.com/prataprc/go-jsonpointer"
	"regexp"
)

// ComponentStat is unmarshalled JSON and represented using Golang's type
// system.
//
// Mandatory fields in ComponentStat,
//  "componentName", name of the component that provide statistics for itself.
type ComponentStat map[string]interface{}

// NewComponentStat return a new instance of stat structure initialized with
// data.
func NewComponentStat(data interface{}) (stat *ComponentStat, err error) {
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
	return &statm, err
}

// Name is part of MessageMarshaller interface.
func (s *ComponentStat) Name() string {
	return "stats"
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
func (s *ComponentStat) Incr(path string, val int) {
	m := map[string]interface{}(*s)
	err := jsonpointer.Incr(m, path, val)
	if err != nil {
		Fatalf("error Incr() ComponentStat %v\n", err)
	}
}

// Incrs increments an array of stat value by `val`
func (s *ComponentStat) Incrs(path string, vals ...int) {
	m := map[string]interface{}(*s)
	err := jsonpointer.Incrs(m, path, vals...)
	if err != nil {
		Fatalf("error Incrs() ComponentStat %v\n", err)
	}
}

// Decr increments stat value by `val`
func (s *ComponentStat) Decr(path string, val int) {
	m := map[string]interface{}(*s)
	err := jsonpointer.Decr(m, path, val)
	if err != nil {
		Fatalf("error Decr() ComponentStat %v\n", err)
	}
}

// Set stat value
func (s *ComponentStat) Set(path string, val interface{}) {
	m := map[string]interface{}(*s)
	err := jsonpointer.Set(m, path, val)
	if err != nil {
		Fatalf("error Set() ComponentStat %v\n", err)
	}
}

// Get stat value
func (s *ComponentStat) Get(path string) interface{} {
	m := map[string]interface{}(*s)
	val, err := jsonpointer.Get(m, path)
	if err != nil {
		Fatalf("error Get() ComponentStat %v\n", err)
	}
	return val
}

// ToMap converts *ComponentStat to map.
func (s *ComponentStat) ToMap() map[string]interface{} {
	return map[string]interface{}(*s)
}

// StatsURLPath construct url path for component-stats using path json-pointer.
func StatsURLPath(prefix, path string) string {
	if prefix[len(prefix)-1] != '/' {
		prefix = prefix + "/"
	}
	return prefix + "stats" + path
}

var regxStatPath, _ = regexp.Compile(`(.*)stats(.*)`)

// ParseStatsPath is opposite StatsURLPath
func ParseStatsPath(urlPath string) string {
	matches := regxStatPath.FindStringSubmatch(urlPath)
	if len(matches) != 3 {
		Fatalf("ParseStatsPath() couldn't match %s\n", urlPath)
	}
	return matches[2]
}
