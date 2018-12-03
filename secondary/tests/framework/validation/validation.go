package validation

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/couchbase/gocb"
	c "github.com/couchbase/indexing/secondary/common"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/query/value"
)

func Validate(expectedResponse, actualResponse tc.ScanResponse) error {
	if len(expectedResponse) != len(actualResponse) {
		errorStr := fmt.Sprintf("Expected scan count %d does not match actual scan count %d", len(expectedResponse), len(actualResponse))
		log.Printf("%v", errorStr)
		e := errors.New(errorStr)
		return e
	}
	eq := reflect.DeepEqual(expectedResponse, actualResponse)
	if eq {
		log.Printf("Expected and Actual scan responses are the same")
	} else {
		log.Printf("Expected and Actual scan responses below are different")
		tc.PrintScanResults(expectedResponse, "expectedResponse")
		tc.PrintScanResults(actualResponse, "actualResponse")
		time.Sleep(5 * time.Second)
		e := errors.New("Expected and Actual scan responses are different")
		return e
	}
	return nil
}

func ValidateArrayResult(expectedResponse, actualResponse tc.ArrayIndexScanResponse) error {
	if len(expectedResponse) != len(actualResponse) {
		errorStr := fmt.Sprintf("Expected scan count %d does not match actual scan count %d", len(expectedResponse), len(actualResponse))
		log.Printf("%v", errorStr)
		e := errors.New(errorStr)
		return e
	}
	eq := reflect.DeepEqual(expectedResponse, actualResponse)
	if eq {
		log.Printf("Expected and Actual scan responses are the same")
	} else {
		log.Printf("Expected and Actual scan responses below are different")
		tc.PrintArrayScanResults(expectedResponse, "expectedResponse")
		tc.PrintArrayScanResults(actualResponse, "actualResponse")
		time.Sleep(5 * time.Second)
		e := errors.New("Expected and Actual scan responses are different")
		return e
	}
	return nil
}

func ValidateGroupAggrResult(expectedResponse, actualResponse tc.GroupAggrScanResponse) error {
	if len(expectedResponse) != len(actualResponse) {
		errorStr := fmt.Sprintf("Expected scan count %d does not match actual scan count %d", len(expectedResponse), len(actualResponse))
		log.Printf("%v", errorStr)
		e := errors.New(errorStr)
		return e
	}
	eq := reflect.DeepEqual(expectedResponse, actualResponse)
	if eq {
		log.Printf("Expected and Actual scan responses are the same")
	} else {
		log.Printf("Expected and Actual scan responses below are different")
		tc.PrintGroupAggrResults(expectedResponse, "expectedResponse")
		tc.PrintGroupAggrResults(actualResponse, "actualResponse")
		time.Sleep(5 * time.Second)
		e := errors.New("Expected and Actual scan responses are different")
		return e
	}
	return nil
}

// Note: Requires testcase to force select order by aliases a, b, c and so on
// This is needed to correlate order from n1ql results and GSI scan
// Ex: SELECT Year as a, Month as b, SUM(Sale) as c from default GROUP BY Year, Month
// Also, this re-reduces partial results from GSI and forms collapsed result set
func ValidateGroupAggrWithN1QL(clusterAddr, bucketName, username, password, n1qlstatement string,
	groupAggr *qc.GroupAggr, indexProjection *qc.IndexProjection,
	actualResp tc.GroupAggrScanResponse) error {

	results, err := tc.ExecuteN1QLStatement(clusterAddr, bucketName, username, password, n1qlstatement, true, gocb.NotBounded)
	if err != nil {
		return err
	}

	entriesPerRow := len(indexProjection.EntryKeys)
	// Generate expectedResponse based on results
	expectedResponse := make(tc.GroupAggrScanResponse, 0)
	for _, result := range results {
		res := result.(map[string]interface{})
		row := make([]interface{}, entriesPerRow)

		for i := 0; i < entriesPerRow; i++ {
			key := string(97 + i)
			if val, ok := res[key]; ok {
				row[i] = val
			} else {
				row[i] = tc.MissingLiteral
			}
		}
		expectedResponse = append(expectedResponse, row)
	}

	// Below is needed gocb returns numbers as float64
	actualJson, _ := json.Marshal(actualResp)
	var actualResponse tc.GroupAggrScanResponse
	json.Unmarshal(actualJson, &actualResponse)

	// Re-reduce the actualResponse as GSI can return partial results

	// Out of p projected values, make a list of [0 to p-1]
	// with detail of whether it is group key or AGGR
	// and if AGGR, which func

	// Take the q (which is <= p) groupkeys and find unique groups in n rows
	// While collapsing groups, for each position of remaining projected values,
	// compute AGGR. For SUM - Add values. For COUNT, COUNTN - Increment
	// For MIN, MAX, convert interface to n1ql value and using collate, find min or max

	// 1. Populate ProjectionInfo
	pinfo := make([]ProjectionInfo, 0)
	gpos := make([]int, 0)
	apos := make([]int, 0)
	for i, id := range indexProjection.EntryKeys {
		for _, g := range groupAggr.Group {
			if id == int64(g.EntryKeyId) {
				pi := ProjectionInfo{position: i, isGroupKey: true, isAggr: false}
				pinfo = append(pinfo, pi)
				gpos = append(gpos, i)
				continue
			}
		}
		for _, a := range groupAggr.Aggrs {
			if id == int64(a.EntryKeyId) {
				pi := ProjectionInfo{position: i, isGroupKey: false, isAggr: true, aggrFunc: a.AggrFunc}
				pinfo = append(pinfo, pi)
				apos = append(apos, i)
				continue
			}
		}
	}

	reducedActualResponse := make(tc.GroupAggrScanResponse, 0)
	for _, act := range actualResponse {
		groupFound := false
		var collapsePostion int
		for j, redRow := range reducedActualResponse {
			skip := false
			for _, gid := range gpos {
				if !reflect.DeepEqual(act[gid], redRow[gid]) {
					skip = true
					break
				}
			}
			if !skip {
				// we found a matching group. Collapse!!
				// Add delta to j position of collapsePostion
				groupFound = true
				collapsePostion = j
				break
			}
		}
		if groupFound { // collapse
			// Add aggr deltas from act to reducedActualResponse[collapsePostion]
			for _, aid := range apos {
				newVal := getDelta(reducedActualResponse[collapsePostion][aid], act[aid], pinfo[aid].aggrFunc)
				reducedActualResponse[collapsePostion][aid] = newVal
			}
		} else { // create new in reducedActualResponse
			newRow := make([]interface{}, len(act))
			copy(newRow, act)
			reducedActualResponse = append(reducedActualResponse, newRow)
		}
	}

	// Validate
	if len(expectedResponse) != len(reducedActualResponse) {
		errorStr := fmt.Sprintf("Expected scan count %d does not match actual scan count %d", len(expectedResponse), len(reducedActualResponse))
		tc.PrintGroupAggrResults(expectedResponse, "expectedResponse")
		tc.PrintGroupAggrResults(reducedActualResponse, "reducedActualResponse")
		log.Printf("%v", errorStr)
		e := errors.New(errorStr)
		return e
	}
	for _, exp := range expectedResponse {
		found := false
		for _, act := range reducedActualResponse {
			if reflect.DeepEqual(exp, act) {
				found = true
				break
			}
		}
		if !found {
			log.Printf("Expected row %v not found in actualResponse", exp)
			tc.PrintGroupAggrResults(expectedResponse, "expectedResponse")
			tc.PrintGroupAggrResults(reducedActualResponse, "reducedActualResponse")
			time.Sleep(5 * time.Second)
			e := errors.New("Expected and Actual scan responses are different")
			return e
		}
	}
	return nil
}

func getDelta(val1, val2 interface{}, aggrFunc c.AggrFuncType) interface{} {
	switch aggrFunc {
	case c.AGG_MIN:
		return aggr_min(val1, val2)
	case c.AGG_MAX:
		return aggr_max(val1, val2)
	case c.AGG_SUM, c.AGG_COUNT, c.AGG_COUNTN:
		v1 := val1.(float64)
		v2 := val2.(float64)
		return v1 + v2
	}
	return nil
}

func aggr_min(val1, val2 interface{}) interface{} {
	v1 := value.NewValue(val1)
	v2 := value.NewValue(val2)

	if v1.Collate(v2) < 0 {
		return val1
	} else {
		return val2
	}
}

func aggr_max(val1, val2 interface{}) interface{} {
	v1 := value.NewValue(val1)
	v2 := value.NewValue(val2)

	if v1.Collate(v2) > 0 {
		return val1
	} else {
		return val2
	}
}

type ProjectionInfo struct {
	position   int
	isGroupKey bool
	isAggr     bool
	aggrFunc   c.AggrFuncType
}
