// Package faiss provides bindings to Faiss, a library for vector similarity
// search.
// More detailed documentation can be found at the Faiss wiki:
// https://github.com/facebookresearch/faiss/wiki.
package faiss

/*
#cgo LDFLAGS: -lfaiss_c

#include <faiss/c_api/Index_c.h>
#include <faiss/c_api/error_c.h>
*/
import "C"
import "errors"

func getLastError() error {
	return errors.New(C.GoString(C.faiss_get_last_error()))
}

// Metric type
const (
	MetricInnerProduct = C.METRIC_INNER_PRODUCT
	MetricL2           = C.METRIC_L2
)
