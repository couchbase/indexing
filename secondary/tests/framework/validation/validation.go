package validation

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/query/value"
	"gopkg.in/couchbase/gocb.v1"
)

func convertExpectedResponse(exp tc.ScanResponse) tc.ScanResponseActual {
	var newExpectedResults tc.ScanResponseActual
	newExpectedResults = make(tc.ScanResponseActual, len(exp))
	for i, e := range exp {
		if e == nil {
			newExpectedResults[i] = nil
			continue
		}
		v := make(value.Values, 0, len(e))
		for _, ec := range e {
			if ec == value.MISSING_VALUE {
				v = append(v, value.NewMissingValue())
			} else {
				v = append(v, value.NewValue(ec))
			}
		}
		newExpectedResults[i] = v
	}
	return newExpectedResults
}

func convertExpectedGroupAggrResponse(exp tc.GroupAggrScanResponse) tc.GroupAggrScanResponseActual {
	var newExpectedResults tc.GroupAggrScanResponseActual
	newExpectedResults = make(tc.GroupAggrScanResponseActual, len(exp))
	for i, e := range exp {
		if e == nil {
			newExpectedResults[i] = nil
			continue
		}
		v := make(value.Values, 0, len(e))
		for _, ec := range e {
			if ec == value.MISSING_VALUE {
				v = append(v, value.NewMissingValue())
			} else {
				v = append(v, value.NewValue(ec))
			}
		}
		newExpectedResults[i] = v
	}
	return newExpectedResults
}

func ValidateOld(expectedResponse1, actualResponse1 tc.ScanResponse) error {
	expectedResponse := convertExpectedResponse(expectedResponse1)
	actualResponse := convertExpectedResponse(actualResponse1)
	return ValidateActual(expectedResponse, actualResponse)
}

func Validate(expectedResponse1 tc.ScanResponse, actualResponse tc.ScanResponseActual) error {
	expectedResponse := convertExpectedResponse(expectedResponse1)
	return ValidateActual(expectedResponse, actualResponse)
}

func ValidateEmptyProjection(expectedResponse tc.ScanResponse,
	actualResponse tc.ScanResponseActual) error {

	expectedResponse1 := convertExpectedResponse(expectedResponse)
	expectedResponse2 := fixEmptyProjection(expectedResponse1)
	return ValidateActual(expectedResponse2, actualResponse)
}

func fixEmptyProjection(exp tc.ScanResponseActual) tc.ScanResponseActual {
	newExp := make(tc.ScanResponseActual, len(exp))
	for i, e := range exp {
		if exp[i] == nil {
			newExp[i] = make([]value.Value, 0)
		} else {
			newExp[i] = e
		}
	}
	return newExp
}

func ValidateActual(expectedResponse tc.ScanResponseActual, actualResponse tc.ScanResponseActual) error {
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
		tc.PrintScanResultsActual(expectedResponse, "expectedResponse")
		tc.PrintScanResultsActual(actualResponse, "actualResponse")
		time.Sleep(5 * time.Second)
		e := errors.New("Expected and Actual scan responses are different")
		return e
	}
	return nil
}

func ValidateArrayResult(expectedResponse, actualResponse tc.ArrayIndexScanResponseActual) error {
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
		tc.PrintArrayScanResultsActual(expectedResponse, "expectedResponse")
		tc.PrintArrayScanResultsActual(actualResponse, "actualResponse")
		time.Sleep(5 * time.Second)
		e := errors.New("Expected and Actual scan responses are different")
		return e
	}
	return nil
}

func ValidateGroupAggrResult(expectedResponse1 tc.GroupAggrScanResponse, actualResponse tc.GroupAggrScanResponseActual) error {
	expectedResponse := convertExpectedGroupAggrResponse(expectedResponse1)
	if len(expectedResponse) != len(actualResponse) {
		errorStr := fmt.Sprintf("Expected scan count %d does not match actual scan count %d", len(expectedResponse), len(actualResponse))
		tc.PrintGroupAggrResultsActual(expectedResponse, "expectedResponse")
		tc.PrintGroupAggrResultsActual(actualResponse, "actualResponse")
		log.Printf("%v", errorStr)
		e := errors.New(errorStr)
		return e
	}
	eq := reflect.DeepEqual(expectedResponse, actualResponse)
	// if len(expectedResponse) == len(actualResponse) == 0,
	// we are only comparing data types (and not data itself)
	if eq || len(expectedResponse) == 0 {
		log.Printf("Expected and Actual scan responses are the same")
	} else {
		log.Printf("Expected and Actual scan responses below are different")
		tc.PrintGroupAggrResultsActual(expectedResponse, "expectedResponse")
		tc.PrintGroupAggrResultsActual(actualResponse, "actualResponse")
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
	actualResp tc.GroupAggrScanResponseActual) error {

	results, err := tc.ExecuteN1QLStatement(clusterAddr, bucketName, username, password, n1qlstatement, true, gocb.RequestPlus)
	if err != nil {
		return err
	}

	entriesPerRow := len(indexProjection.EntryKeys)
	// Generate expectedResponse based on results
	expectedResponse := make(tc.GroupAggrScanResponseActual, 0)
	for _, result := range results {
		res := result.(map[string]interface{})
		row := make(value.Values, entriesPerRow)

		for i := 0; i < entriesPerRow; i++ {
			key := string(97 + i)
			if val, ok := res[key]; ok {
				row[i] = value.NewValue(val)
			} else {
				row[i] = value.NewMissingValue()
			}
		}
		expectedResponse = append(expectedResponse, row)
	}

	// Below is needed gocb returns numbers as float64
	expectedResponse = transformActualResponse(expectedResponse)
	actualResponse := transformActualResponse(actualResp)

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

	reducedActualResponse := make(tc.GroupAggrScanResponseActual, 0)
	for _, act := range actualResponse {
		groupFound := false
		var collapsePostion int
		for j, redRow := range reducedActualResponse {
			skip := false
			for _, gid := range gpos {
				if !reflect.DeepEqual(value.NewValue(act[gid]), redRow[gid]) {
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
				// if reducedActualResponse[collapsePostion][aid] == value.MISSING_VALUE {
				val1 := reducedActualResponse[collapsePostion][aid].Actual()
				newVal := getDelta(val1, act[aid], pinfo[aid].aggrFunc)
				reducedActualResponse[collapsePostion][aid] = value.NewValue(newVal)
			}
		} else { // create new in reducedActualResponse
			newRow := make(value.Values, 0)
			for i := 0; i < len(act); i++ {
				newRow = append(newRow, value.NewValue(act[i]))
			}
			reducedActualResponse = append(reducedActualResponse, newRow)
		}
	}

	// Validate
	if len(expectedResponse) != len(reducedActualResponse) {
		errorStr := fmt.Sprintf("Expected scan count %d does not match actual scan count %d", len(expectedResponse), len(reducedActualResponse))
		tc.PrintGroupAggrResultsActual(expectedResponse, "expectedResponse")
		tc.PrintGroupAggrResultsActual(reducedActualResponse, "reducedActualResponse")
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
			tc.PrintGroupAggrResultsActual(expectedResponse, "expectedResponse")
			tc.PrintGroupAggrResultsActual(reducedActualResponse, "reducedActualResponse")
			time.Sleep(5 * time.Second)
			e := errors.New("Expected and Actual scan responses are different")
			return e
		}
	}
	return nil
}

func transformValue(act value.Value) value.Value {
	var v value.Value
	switch act.Type() {
	case value.MISSING:
		v = act
	case value.NULL:
		v = act
	case value.BOOLEAN:
		v = act
	case value.NUMBER:
		v = value.NewValue(act.Actual())
	case value.STRING:
		v = value.NewValue(act.Actual())
	case value.ARRAY:
		fv := make([]interface{}, 0)
		nv := act.Actual()
		tv := nv.([]interface{})
		for _, val := range tv {
			fv = append(fv, transformValue(value.NewValue(val)))
		}
		v = value.NewValue(fv)
	case value.OBJECT:
		fv := make(map[string]interface{})
		nv := act.Actual()
		tv := nv.(map[string]interface{})
		for k, val := range tv {
			fv[k] = transformValue(value.NewValue(val))
		}
		v = value.NewValue(fv)
	case value.JSON:
		// Not sure when this is required.
		v = act
	case value.BINARY:
		v = act
	}
	return v
}

func transformActual(act value.Values) value.Values {
	newAct := make(value.Values, 0)
	for _, a := range act {
		newAct = append(newAct, transformValue(a))
	}

	return newAct
}

func transformActualResponse(actualResp tc.GroupAggrScanResponseActual) tc.GroupAggrScanResponseActual {
	actualResponse := make(tc.GroupAggrScanResponseActual, 0)
	for _, act := range actualResp {
		newAct := transformActual(act)
		actualResponse = append(actualResponse, newAct)
	}
	return actualResponse
}

func getVal(val interface{}) float64 {
	var v float64
	switch val.(type) {
	case value.Value:
		va := value.NewValue(val)
		if va.Type() != value.NUMBER {
			msg := fmt.Sprintf("unexpected value %v %T", va, va)
			panic(msg)
		}
		va1 := va.Actual()
		v = va1.(float64)
	case float64:
		v = val.(float64)
	default:
		msg := fmt.Sprintf("unexpected value %v %T", val, val)
		panic(msg)
	}
	return v
}

func getDelta(val1, val2 interface{}, aggrFunc c.AggrFuncType) interface{} {
	switch aggrFunc {
	case c.AGG_MIN:
		return aggr_min(val1, val2)
	case c.AGG_MAX:
		return aggr_max(val1, val2)
	case c.AGG_SUM, c.AGG_COUNT, c.AGG_COUNTN:
		v1 := getVal(val1)
		v2 := getVal(val2)
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
