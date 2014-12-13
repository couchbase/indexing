package datautility

import (
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
)

// ToDo : Create a datatype generic ExpectedScanResponse method

// Currently supports only inclusion 1. ToDo - Add other inclusions
// Supports simple happy path of simple index without nesting
func ExpectedScanResponse_float64(docs []kvutility.KeyValue, jsonPath string, low, high float64, inclusion int64) tc.ScanResponse {
	results := make(tc.ScanResponse)
	for _, kv := range docs {
		switch kv.JsonValue[jsonPath].(type) {
			case float64:
	 				field := kv.JsonValue[jsonPath].(float64)
					if field >= low && field < high {
						results[kv.Key] = []interface {} {field}
					}
			default:
		}
	}

	return results
}

func ExpectedScanResponse_string(docs []kvutility.KeyValue, jsonPath string, low, high string, inclusion int64) tc.ScanResponse {
	results := make(tc.ScanResponse)
	for _, kv := range docs {
		switch kv.JsonValue[jsonPath].(type) {
			case string:
					field := kv.JsonValue[jsonPath].(string)
					if field >= low && field < high {
						results[kv.Key] = []interface {} {field}
					}
			default:
		}
	}

	return results
}

// Currently supports only inclusion 3
func ExpectedScanResponse_bool(docs []kvutility.KeyValue, jsonPath string, value bool, inclusion int64) tc.ScanResponse {
	results := make(tc.ScanResponse)
	for _, kv := range docs {
		switch kv.JsonValue[jsonPath].(type) {
			case bool:
					field := kv.JsonValue[jsonPath].(bool)
					if field == value {
						results[kv.Key] = []interface {} {field}
					}
			default:
		}
	}

	return results
}