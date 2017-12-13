package validation

import (
	"errors"
	"fmt"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"log"
	"reflect"
	"time"
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
