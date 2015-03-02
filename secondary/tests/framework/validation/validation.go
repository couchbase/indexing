package validation

import (
	"errors"
	"fmt"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"reflect"
	"time"
)

func Validate(expectedResponse, actualResponse tc.ScanResponse) error {
	if len(expectedResponse) != len(actualResponse) {
		errorStr := fmt.Sprintf("Expected scan count %d does not match actual scan count %d: ", len(expectedResponse), len(actualResponse))
		fmt.Println(errorStr)
		e := errors.New(errorStr)
		return e
	}
	eq := reflect.DeepEqual(expectedResponse, actualResponse)
	if eq {
		fmt.Println("Expected and Actual scan responses are the same")
	} else {
		fmt.Println("Expected and Actual scan responses below are different")
		tc.PrintScanResults(expectedResponse, "expectedResponse")
		tc.PrintScanResults(actualResponse, "actualResponse")
		time.Sleep(5 * time.Second)
		e := errors.New("Expected and Actual scan responses are different")
		return e
	}
	return nil
}
