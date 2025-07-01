package codebook

import (
	"errors"
	"os"
	"strconv"

	"github.com/couchbase/indexing/secondary/common"
)

var (
	ErrCodebookNotTrained = errors.New("Codebook is not trained")
	ErrInvalidVersion     = errors.New("Invalid codebook version")
	ErrChecksumMismatch   = errors.New("Checksum mismatch")
	ErrUnknownType        = errors.New("Unknown Codebook Type")
	ErrCodebookClosed     = errors.New("Codebook closed")
	ErrUnsupportedMetric  = errors.New("Unsupported Distance Metric")
)

type MetricType int

const (
	METRIC_L2 MetricType = iota
	METRIC_INNER_PRODUCT
)

func (m MetricType) String() string {

	switch m {

	case METRIC_L2:
		return "L2"
	case METRIC_INNER_PRODUCT:
		return "INNER_PRODUCT"
	}

	return ""
}

func ConvertSimilarityToMetric(similarity common.VectorSimilarity) (MetricType, bool) {
	switch similarity {
	case common.EUCLIDEAN_SQUARED, common.L2_SQUARED, common.EUCLIDEAN, common.L2:
		return METRIC_L2, false // Default to L2
	case common.DOT:
		return METRIC_INNER_PRODUCT, false
	case common.COSINE:
		return METRIC_INNER_PRODUCT, true
	}
	return METRIC_L2, false // Always default to L2
}

type Codebook interface {
	//Train the codebook using input vectors.
	Train(vecs []float32) error

	//IsTrained returns true if codebook has been trained.
	IsTrained() bool

	//CodeSize returns the size of produced code in bytes.
	CodeSize() (int, error)

	//CoarseSize returns the size of the coarse code for IVF index.
	CoarseSize() (int, error)

	//Compute the quantized code for a given input vector.
	//Must be run on a trained codebook.
	EncodeVector(vec []float32, code []byte) error

	//Compute the quantized codes for a given list of input vectors.
	//Must be run on a trained codebook.
	EncodeVectors(vecs []float32, codes []byte) error

	//Compute the quantized code and find the nearest centroidID
	//for a given list of vectors. Must be run on a trained codebook.
	EncodeAndAssignVectors(vecs []float32, codes []byte, labels []int64) error

	//Size returns the memory size in bytes.
	Size() int64

	//Find the nearest k centroidIDs for a given vector.
	//Must be run on a trained codebook.
	FindNearestCentroids(vec []float32, k int64) ([]int64, error)

	//Computes the distance table for given vector.
	//Distance table contains the precomputed distance of the given
	//vector from each subvector m(determined by the number of subquantizers).
	//Distance table is a matrix of dimension M * ksub where
	//M = number of subquantizers
	//ksub = number of centroids for each subquantizer (2**nbits)
	ComputeDistanceTable(qvec []float32, dtable []float32) error

	//Compute the distance between a vector using distance table and
	//quantized code of another vector.
	ComputeDistanceWithDT(code []byte, dtable []float32) float32

	//Compute the distance between a vector with another given set of vectors.
	ComputeDistance(qvec []float32, fvecs []float32, dist []float32) error

	//Compute the distance between a vector and flat quantized codes.
	//Quantized codes are decoded first before distance comparison.
	//Codes must be provided without coarse code(i.e. centroid ID).
	//This function only works with vectors belonging to the same centroid(input as listno).
	ComputeDistanceEncoded(qvec []float32, n int, codes []byte,
		dists []float32, dtable []float32, listno int64) error

	//Decode the quantized code and return float32 vector.
	//Must be run on a trained codebook.
	DecodeVector(code []byte, vec []float32) error

	//Decode the quantized codes and return float32 vectors.
	//Must be run on a trained codebook.
	DecodeVectors(n int, codes []byte, vecs []float32) error

	//Close frees the memory used by codebook.
	Close() error

	//Marshal the codebook to a slice of bytes
	Marshal() ([]byte, error)

	// Number of centroids used in training the instance
	NumCentroids() int

	MetricType() MetricType

	//Dimension for the vector stored in codebook
	Dimension() int
}

// SetMaxCPU sets the max number of cores that can be used by the
// underlying library
func SetMaxCPU(n int) {
	os.Setenv("OMP_THREAD_LIMIT", strconv.Itoa(n))
}
