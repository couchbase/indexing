// Package faiss provides bindings to Faiss, a library for vector similarity
// search.
// More detailed documentation can be found at the Faiss wiki:
// https://github.com/facebookresearch/faiss/wiki.
package faiss

/*
#cgo LDFLAGS: -lfaiss_c

#include <faiss/c_api/Index_c.h>
#include <faiss/c_api/error_c.h>
#include <faiss/c_api/IndexScalarQuantizer_c.h>
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

//Quantizer type
const (
	QT_8bit         = C.QT_8bit
	QT_4bit         = C.QT_4bit
	QT_8bit_uniform = C.QT_8bit_uniform
	QT_4bit_uniform = C.QT_4bit_uniform
	QT_fp16         = C.QT_fp16
	QT_8bit_direct  = C.QT_8bit_direct
	QT_6bit         = C.QT_6bit
)

const DEFAULT_EF_SEARCH = 16
