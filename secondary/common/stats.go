// ComponentStat provides a type and method receivers for marshalling and
// un-marshalling statistics for components across the network.
//
// Example client {
//    client := NewHttpClient("http://localhost:8888")
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

func (s ComponentStat) Name() string {
	return "stats/" + s["componentName"].(string)
}

func (s ComponentStat) Encode() (data []byte, err error) {
	data, err = json.Marshal(&s)
	return
}

func (s ComponentStat) Decode(data []byte) (err error) {
	return json.Unmarshal(data, &s)
}

func (s ComponentStat) ContentType() string {
	return "application/json"
}
