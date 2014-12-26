package datautility

import (
	"strings"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
)

// ToDo : Create a datatype generic ExpectedScanResponse method

/* 
Inclusion: 
0 - exclude low, exclude high: value > low && value < high
1 - include low, exclude high: value >= low && value < high
2 - exclude low, include high: value > low && value <= high
3 - include low, include high: value >= low && value <= high

Default value is 0

For lookup scenarios, use Inclusion 3 with low = high values. 
*/


// Currently supports only inclusion 1. ToDo - Add other inclusions
// Supports simple happy path of simple index without nesting
func ExpectedScanResponse_float64(docs []kvutility.KeyValue, jsonPath string, low, high float64, inclusion int64) tc.ScanResponse {
	results := make(tc.ScanResponse)
	fields := strings.Split(jsonPath, ".")
	var json map[string]interface{}
	var f string
	var i int
	
	for _, kv := range docs {		
		// Access the nested field
		json = kv.JsonValue
		for i = 0; i < len(fields) - 1; i++ {
			f = fields[i]
			switch json[f].(type) {
				case map[string]interface{}: json = json[f].(map[string]interface{})
				default: break
			}
		}
		
		switch json[fields[i]].(type) {
			case float64:
	 				field := json[fields[i]].(float64)
					switch inclusion {
						case 0: if field > low && field < high {
									results[kv.Key] = []interface {} {field}
								}
						case 1: if field >= low && field < high {
									results[kv.Key] = []interface {} {field}
								} 
						case 2: if field > low && field <= high {
									results[kv.Key] = []interface {} {field}
								}
						case 3: if field >= low && field <= high {
									results[kv.Key] = []interface {} {field}
								}
						default: if field > low && field < high {
									results[kv.Key] = []interface {} {field}
								}
					}
			default:
		}
	}

	return results
}

func ExpectedScanResponse_string(docs []kvutility.KeyValue, jsonPath string, low, high string, inclusion int64) tc.ScanResponse {
	results := make(tc.ScanResponse)
	fields := strings.Split(jsonPath, ".")
	var json map[string]interface{}
	var f string
	var i int
	
	for _, kv := range docs {
		// Access the nested field
		json = kv.JsonValue
		for i = 0; i < len(fields) - 1; i++ {
			f = fields[i]
			switch json[f].(type) {
				case map[string]interface{}: json = json[f].(map[string]interface{})
				default: break
			}
		}
		switch json[fields[i]].(type) {
			case string:
					field := json[fields[i]].(string)
					switch inclusion {
						case 0: if field > low && field < high {
									results[kv.Key] = []interface {} {field}
								}
						case 1: if field >= low && field < high {
									results[kv.Key] = []interface {} {field}
								} 
						case 2: if field > low && field <= high {
									results[kv.Key] = []interface {} {field}
								}
						case 3: if field >= low && field <= high {
									results[kv.Key] = []interface {} {field}
								}
						default: if field > low && field < high {
									results[kv.Key] = []interface {} {field}
								}
					}
			default:
		}
	}

	return results
}

// Currently supports only inclusion 3
func ExpectedScanResponse_bool(docs []kvutility.KeyValue, jsonPath string, value bool, inclusion int64) tc.ScanResponse {
	results := make(tc.ScanResponse)
	fields := strings.Split(jsonPath, ".")
	var json map[string]interface{}
	var f string
	var i int
	
	for _, kv := range docs {
		// Access the nested field
		json = kv.JsonValue
		for i = 0; i < len(fields) - 1; i++ {
			f = fields[i]
			switch json[f].(type) {
				case map[string]interface{}: json = json[f].(map[string]interface{})
				default: break
			}
		}
		switch json[fields[i]].(type) {
			case bool:
					field := json[fields[i]].(bool)
					switch inclusion {
						case 3: if field == value {
									results[kv.Key] = []interface {} {field}
								}
						default: if field == value {
									results[kv.Key] = []interface {} {field}
								}
					}
			default:
		}
	}

	return results
}