package validation

import (
	"fmt"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"reflect"
	"time"
)

func Validate(expectedResponse, actualResponse tc.ScanResponse) {
	if len(expectedResponse) != len(actualResponse) {
		fmt.Println("Lengths of Expected and Actual scan responses are different: ", len(expectedResponse), len(actualResponse))
		panic("Expected and Actual scan responses are different")
	}
	eq := reflect.DeepEqual(expectedResponse, actualResponse)
	if eq {
		fmt.Println("Expected and Actual scan responses are the same")
	} else {
		fmt.Println("Expected and Actual scan responses below are different")
		tc.PrintScanResults(expectedResponse, "expectedResponse")
		tc.PrintScanResults(actualResponse, "actualResponse")
		time.Sleep(5 * time.Second)
		panic("Expected and Actual scan responses are different")
	}
}
