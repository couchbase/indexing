package datautility

import (
	"bytes"
	"encoding/json"
	c "github.com/couchbase/indexing/secondary/common"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"log"
	"reflect"
	"sort"
	"strings"
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

func ExpectedScanResponse_float64(docs tc.KeyValues, jsonPath string, low, high float64, inclusion int64) tc.ScanResponse {
	results := make(tc.ScanResponse)
	fields := strings.Split(jsonPath, ".")
	var json map[string]interface{}
	var f string
	var i int

	for k, v := range docs {
		// Access the nested field
		json = v.(map[string]interface{})
		for i = 0; i < len(fields)-1; i++ {
			f = fields[i]
			switch json[f].(type) {
			case map[string]interface{}:
				json = json[f].(map[string]interface{})
			default:
				break
			}
		}

		switch json[fields[i]].(type) {
		case float64:
			field := json[fields[i]].(float64)
			switch inclusion {
			case 0:
				if field > low && field < high {
					results[k] = []interface{}{field}
				}
			case 1:
				if field >= low && field < high {
					results[k] = []interface{}{field}
				}
			case 2:
				if field > low && field <= high {
					results[k] = []interface{}{field}
				}
			case 3:
				if field >= low && field <= high {
					results[k] = []interface{}{field}
				}
			default:
				if field > low && field < high {
					results[k] = []interface{}{field}
				}
			}
		default:
		}
	}

	return results
}

func ExpectedScanResponse_string(docs tc.KeyValues, jsonPath string, low, high string, inclusion int64) tc.ScanResponse {
	results := make(tc.ScanResponse)
	fields := strings.Split(jsonPath, ".")
	var json map[string]interface{}
	var f string
	var i int

	for k, v := range docs {
		// Access the nested field
		json = v.(map[string]interface{})
		for i = 0; i < len(fields)-1; i++ {
			f = fields[i]
			switch json[f].(type) {
			case map[string]interface{}:
				json = json[f].(map[string]interface{})
			default:
				break
			}
		}
		switch json[fields[i]].(type) {
		case string:
			field := json[fields[i]].(string)
			switch inclusion {
			case 0:
				if field > low && field < high {
					results[k] = []interface{}{field}
				}
			case 1:
				if field >= low && field < high {
					results[k] = []interface{}{field}
				}
			case 2:
				if field > low && field <= high {
					results[k] = []interface{}{field}
				}
			case 3:
				if field >= low && field <= high {
					results[k] = []interface{}{field}
				}
			default:
				if field > low && field < high {
					results[k] = []interface{}{field}
				}
			}
		default:
		}
	}

	return results
}

func ExpectedScanLimitResponse_string(docs tc.KeyValues, jsonPath string, low, high string, inclusion, limit int64) tc.ScanResponse {
	results := make(tc.ScanResponse)
	fields := strings.Split(jsonPath, ".")
	var json map[string]interface{}
	var f string
	var i int

	for k, v := range docs {
		// Access the nested field
		json = v.(map[string]interface{})
		for i = 0; i < len(fields)-1; i++ {
			f = fields[i]
			switch json[f].(type) {
			case map[string]interface{}:
				json = json[f].(map[string]interface{})
			default:
				break
			}
		}
		switch json[fields[i]].(type) {
		case string:
			field := json[fields[i]].(string)
			switch inclusion {
			case 0:
				if field > low && field < high {
					results[k] = []interface{}{field}
				}
			case 1:
				if field >= low && field < high {
					results[k] = []interface{}{field}
				}
			case 2:
				if field > low && field <= high {
					results[k] = []interface{}{field}
				}
			case 3:
				if field >= low && field <= high {
					results[k] = []interface{}{field}
				}
			default:
				if field > low && field < high {
					results[k] = []interface{}{field}
				}
			}
		default:
		}
	}
	keys := make([]string, len(results))
	for key := range results {
		keys = append(keys, key)
	}
	m := make(tc.ScanResponse)
	sort.Strings(keys)
	if int64(len(keys)) < limit {
		limit = int64(len(keys))
	}
	for _, key := range keys[:limit] {
		m[key] = results[key]
	}

	return results
}

// Currently supports only inclusion 3
func ExpectedScanResponse_bool(docs tc.KeyValues, jsonPath string, value bool, inclusion int64) tc.ScanResponse {
	results := make(tc.ScanResponse)
	fields := strings.Split(jsonPath, ".")
	var json map[string]interface{}
	var f string
	var i int

	for k, v := range docs {
		// Access the nested field
		json = v.(map[string]interface{})
		for i = 0; i < len(fields)-1; i++ {
			f = fields[i]
			switch json[f].(type) {
			case map[string]interface{}:
				json = json[f].(map[string]interface{})
			default:
				break
			}
		}
		switch json[fields[i]].(type) {
		case bool:
			field := json[fields[i]].(bool)
			switch inclusion {
			case 3:
				if field == value {
					results[k] = []interface{}{field}
				}
			default:
				if field == value {
					results[k] = []interface{}{field}
				}
			}
		default:
		}
	}

	return results
}

// ScanAll for all datatypes
func ExpectedScanAllResponse(docs tc.KeyValues, jsonPath string) tc.ScanResponse {
	results := make(tc.ScanResponse)
	fields := strings.Split(jsonPath, ".")
	var json map[string]interface{}
	var f string
	var i int

	for k, v := range docs {
		// Access the nested field
		json = v.(map[string]interface{})
		for i = 0; i < len(fields)-1; i++ {
			f = fields[i]
			switch json[f].(type) {
			case map[string]interface{}:
				json = json[f].(map[string]interface{})
			default:
				break
			}
		}

		// Add the field only if is present
		if field, ok := json[fields[i]]; ok {
			results[k] = []interface{}{field}
		}
	}

	return results
}

// ScanAll for all json object types
func ExpectedScanAllResponse_json(docs tc.KeyValues, jsonPath string) tc.ScanResponse {
	results := make(tc.ScanResponse)
	fields := strings.Split(jsonPath, ".")
	var json map[string]interface{}
	var f string
	var i int

	for k, v := range docs {
		// Access the nested field
		json = v.(map[string]interface{})
		for i = 0; i < len(fields)-1; i++ {
			f = fields[i]
			switch json[f].(type) {
			case map[string]interface{}:
				json = json[f].(map[string]interface{})
			default:
				break
			}
		}
		field := json[fields[i]]
		switch field.(type) {
		case map[string]interface{}:
			results[k] = []interface{}{field}
		default:
		}
	}

	return results
}

// Lookup for nil
func ExpectedLookupResponse_nil(docs tc.KeyValues, jsonPath string) tc.ScanResponse {
	results := make(tc.ScanResponse)
	fields := strings.Split(jsonPath, ".")
	var json map[string]interface{}
	var f string
	var i int

	for k, v := range docs {
		// Access the nested field
		json = v.(map[string]interface{})
		for i = 0; i < len(fields)-1; i++ {
			f = fields[i]
			switch json[f].(type) {
			case map[string]interface{}:
				json = json[f].(map[string]interface{})
			default:
				break
			}
		}
		field := json[fields[i]]
		if field == nil {
			results[k] = []interface{}{field}
		}
	}

	return results
}

// Lookup for Json object from list of docs
func ExpectedLookupResponse_json(docs tc.KeyValues, jsonPath string, value map[string]interface{}) tc.ScanResponse {
	results := make(tc.ScanResponse)
	fields := strings.Split(jsonPath, ".")
	var json map[string]interface{}
	var f string
	var i int

	for k, v := range docs {
		// Access the nested field
		json = v.(map[string]interface{})
		for i = 0; i < len(fields)-1; i++ {
			f = fields[i]
			switch json[f].(type) {
			case map[string]interface{}:
				json = json[f].(map[string]interface{})
			default:
				break
			}
		}
		jsonValue := json[fields[i]]
		switch jsonValue.(type) {
		case map[string]interface{}:
			if reflect.DeepEqual(value, jsonValue) {
				results[k] = []interface{}{jsonValue}
			}
		default:
		}
	}
	return results
}

// Range scan for Primary index
func ExpectedScanResponse_RangePrimary(docs tc.KeyValues, low, high string, inclusion int64) tc.ScanResponse {
	results := make(tc.ScanResponse)

	// var json map[string]interface{}
	// var f string
	// var i int

	for k, _ := range docs {
		field := k
		switch inclusion {
		case 0:
			if field > low && field < high {
				// results[k] = []interface{}{}
				results[k] = nil
			}
		case 1:
			if field >= low && field < high {
				results[k] = nil
			}
		case 2:
			if field > low && field <= high {
				results[k] = nil
			}
		case 3:
			if field >= low && field <= high {
				results[k] = nil
			}
		default:
			if field > low && field < high {
				results[k] = nil
			}
		}
	}

	return results
}

func ExpectedArrayScanResponse_string(docs tc.KeyValues, jsonPath string, low, high string, inclusion int64, isDistinct bool) tc.ArrayIndexScanResponse {
	results := make(tc.ArrayIndexScanResponse)
	fields := strings.Split(jsonPath, ".")
	var json map[string]interface{}
	var f string
	var i int

	for k, v := range docs {
		// Access the nested field
		json = v.(map[string]interface{})
		for i = 0; i < len(fields)-1; i++ {
			f = fields[i]
			switch json[f].(type) {
			case map[string]interface{}:
				json = json[f].(map[string]interface{})
			default:
				break
			}
		}
		switch json[fields[i]].(type) {
		case []interface{}:
			arrayItems := json[fields[i]].([]interface{})
			sortedArray := make([]string, len(arrayItems))
			for i, arrayItem := range arrayItems {
				sortedArray[i] = arrayItem.(string)
			}
			sort.Strings(sortedArray)
			compressedArray := make([]string, len(arrayItems))
			if isDistinct {
				i := 0
				for i < len(sortedArray) {
					j := i + 1
					for ; j < len(sortedArray); j++ {
						if sortedArray[i] != sortedArray[j] {
							compressedArray = append(compressedArray, sortedArray[i])
							break
						}
					}

					if j == len(sortedArray) {
						compressedArray = append(compressedArray, sortedArray[i])
						break
					}
					i = j
				}
				sortedArray = compressedArray
			}

			for _, item := range sortedArray {
				switch inclusion {
				case 0:
					if item > low && item < high {
						results[k] = append(results[k], []interface{}{item})
					}
				case 1:
					if item >= low && item < high {
						results[k] = append(results[k], []interface{}{item})
					}
				case 2:
					if item > low && item <= high {
						results[k] = append(results[k], []interface{}{item})
					}
				case 3:
					if item >= low && item <= high {
						results[k] = append(results[k], []interface{}{item})
					}
				default:
					if item > low && item < high {
						results[k] = append(results[k], []interface{}{item})
					}
				}
			}
		case []string:
			arrayItems := json[fields[i]].([]string)
			sortedArray := make([]string, len(arrayItems))
			for i, arrayItem := range arrayItems {
				sortedArray[i] = arrayItem
			}
			sort.Strings(sortedArray)
			compressedArray := make([]string, len(arrayItems))
			if isDistinct {
				i := 0
				for i < len(sortedArray) {
					j := i + 1
					for ; j < len(sortedArray); j++ {
						if sortedArray[i] != sortedArray[j] {
							compressedArray = append(compressedArray, sortedArray[i])
							break
						}
					}

					if j == len(sortedArray) {
						compressedArray = append(compressedArray, sortedArray[i])
						break
					}
					i = j
				}
				sortedArray = compressedArray
			}

			for _, item := range sortedArray {
				switch inclusion {
				case 0:
					if item > low && item < high {
						results[k] = append(results[k], []interface{}{item})
					}
				case 1:
					if item >= low && item < high {
						results[k] = append(results[k], []interface{}{item})
					}
				case 2:
					if item > low && item <= high {
						results[k] = append(results[k], []interface{}{item})
					}
				case 3:
					if item >= low && item <= high {
						results[k] = append(results[k], []interface{}{item})
					}
				default:
					if item > low && item < high {
						results[k] = append(results[k], []interface{}{item})
					}
				}
			}

		default:
		}
	}

	return results
}

func ExpectedMultiScanResponse(docs tc.KeyValues, compositeFieldPaths []string, scans qc.Scans,
	reverse, distinct bool, offset, limit int64, isScanAll bool) tc.ScanResponse {

	results := make(tc.ScanResponse)
	var json map[string]interface{}
	var f string
	var i int
	offsetCount := int64(0)

	for k, v := range docs {
		var compositeValues []interface{}
		for _, compositeFieldPath := range compositeFieldPaths {

			fields := strings.Split(compositeFieldPath, ".")
			// Access the nested field
			json = v.(map[string]interface{})
			for i = 0; i < len(fields)-1; i++ {
				f = fields[i]
				switch json[f].(type) {
				case map[string]interface{}:
					json = json[f].(map[string]interface{})
				default:
					break
				}
			}
			compositeValues = append(compositeValues, json[fields[i]])
		}

		if isScanAll {
			if offsetCount >= offset {
				results[k] = compositeValues
			} else {
				offsetCount++
			}
			continue
		}

		skipDoc := true
		for _, scan := range scans {
			if scan.Seek != nil { // Equals
				if isEqual(scan.Seek, compositeValues) {
					skipDoc = false
					break
				}
			} else { // Composite filtering
				if applyFilters(compositeValues, scan.Filter) {
					skipDoc = false
					break
				}
			}
		}

		if !skipDoc {
			if offsetCount >= offset {
				results[k] = compositeValues
			} else {
				offsetCount++
			}
		}
	}

	return results
}

func isEqual(seek c.SecondaryKey, values []interface{}) bool {
	for i, eqVal := range seek {
		if eqVal != values[i] {
			return false
		}
	}
	return true
}

func applyFilters(compositekeys []interface{}, compositefilters []*qc.CompositeElementFilter) bool {
	var err error
	var low, high, ck []byte
	for i, filter := range compositefilters {
		ck, err = json.Marshal(compositekeys[i])
		if err != nil {
			log.Printf("Error in marshalling value %v", compositekeys[i])
			tc.HandleError(err, "Error in applyFilters")
		}

		low, err = json.Marshal(filter.Low)
		if err != nil {
			log.Printf("Error in low value %v", filter.Low)
			tc.HandleError(err, "Error in applyFilters")
		}

		high, err = json.Marshal(filter.High)
		if err != nil {
			log.Printf("Error in low value %v", filter.High)
			tc.HandleError(err, "Error in applyFilters")
		}

		switch filter.Inclusion {
		case 0:
			// if ck > low and ck < high
			if !(bytes.Compare(ck, low) > 0 && bytes.Compare(ck, high) < 0) {
				return false
			}
		case 1:
			// if ck >= low and ck < high
			if !(bytes.Compare(ck, low) >= 0 && bytes.Compare(ck, high) < 0) {
				return false
			}
		case 2:
			// if ck > low and ck <= high
			if !(bytes.Compare(ck, low) > 0 && bytes.Compare(ck, high) <= 0) {
				return false
			}
		case 3:
			// if ck >= low and ck <= high
			if !(bytes.Compare(ck, low) >= 0 && bytes.Compare(ck, high) <= 0) {
				return false
			}
		}
	}

	return true
}

/*func applyFilter(compositekeys [][]byte, compositefilters []CompositeElementFilter) bool {

	for i, filter := range compositefilters {
		ck := compositekeys[i]
		switch filter.Inclusion {
		case Neither:
			// if ck > low and ck < high
			if !(bytes.Compare(ck, filter.Low.Bytes()) > 0 && bytes.Compare(ck, filter.High.Bytes()) < 0) {
				return false
			}
		case Low:
			// if ck >= low and ck < high
			if !(bytes.Compare(ck, filter.Low.Bytes()) >= 0 && bytes.Compare(ck, filter.High.Bytes()) < 0) {
				return false
			}
		case High:
			// if ck > low and ck <= high
			if !(bytes.Compare(ck, filter.Low.Bytes()) > 0 && bytes.Compare(ck, filter.High.Bytes()) <= 0) {
				return false
			}
		case Both:
			// if ck >= low and ck <= high
			if !(bytes.Compare(ck, filter.Low.Bytes()) >= 0 && bytes.Compare(ck, filter.High.Bytes()) <= 0) {
				return false
			}
		}
	}

	return true
}*/
