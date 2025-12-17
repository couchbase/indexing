package randdocs

import (
	"encoding/binary"
	"fmt"
	"math"
	rnd "math/rand"
	"os"
	"sync"
	"sync/atomic"
)

// SparseVector represents a sparse vector with indices and values
type SparseVector struct {
	Indices []int32
	Values  []float32
}

// SparseData holds the CSR matrix data and file handles for sparse dataset
type SparseData struct {
	sync.Mutex
	vecCount int // Vector Count
	overflow int // Overflow of Vectors

	// CSR matrix dimensions
	nrow int64
	ncol int64
	nnz  int64

	// Memory-mapped or loaded data
	indptr  []int64
	indices []int32
	data    []float32

	filename string

	// Query and ground truth support
	queryData   *SparseData
	truthIndptr []int64
	truthData   []int32
}

// OpenSparseData opens a CSR format sparse matrix file
// Format: [nrow:int64][ncol:int64][nnz:int64][indptr:(nrow+1)*int64][indices:nnz*int32][data:nnz*float32]
func OpenSparseData(filename string) (*SparseData, error) {
	fd, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening sparse file %s: %v", filename, err)
	}
	defer fd.Close()

	// Read header
	var header [3]int64
	if err := binary.Read(fd, binary.LittleEndian, &header); err != nil {
		return nil, fmt.Errorf("error reading header: %v", err)
	}

	nrow, ncol, nnz := header[0], header[1], header[2]
	fmt.Printf("SparseData: nrow=%d, ncol=%d, nnz=%d\n", nrow, ncol, nnz)

	// Read indptr
	indptr := make([]int64, nrow+1)
	if err := binary.Read(fd, binary.LittleEndian, indptr); err != nil {
		return nil, fmt.Errorf("error reading indptr: %v", err)
	}

	// Verify indptr
	if indptr[nrow] != nnz {
		return nil, fmt.Errorf("indptr mismatch: expected %d, got %d", nnz, indptr[nrow])
	}

	// Read indices
	indices := make([]int32, nnz)
	if err := binary.Read(fd, binary.LittleEndian, indices); err != nil {
		return nil, fmt.Errorf("error reading indices: %v", err)
	}

	// Read data
	data := make([]float32, nnz)
	if err := binary.Read(fd, binary.LittleEndian, data); err != nil {
		return nil, fmt.Errorf("error reading data: %v", err)
	}

	return &SparseData{
		nrow:     nrow,
		ncol:     ncol,
		nnz:      nnz,
		indptr:   indptr,
		indices:  indices,
		data:     data,
		filename: filename,
	}, nil
}

// GetValue returns the next sparse vector from the dataset
func (sd *SparseData) GetValue() (docid string, vecNum int, overflow int, value SparseVector, err error) {
	sd.Lock()
	defer sd.Unlock()

	if sd.vecCount >= int(sd.nrow) {
		sd.reset()
	}

	row := sd.vecCount
	start := sd.indptr[row]
	end := sd.indptr[row+1]

	vec := SparseVector{
		Indices: make([]int32, end-start),
		Values:  make([]float32, end-start),
	}
	copy(vec.Indices, sd.indices[start:end])
	copy(vec.Values, sd.data[start:end])

	c, o := sd.vecCount, sd.overflow
	sd.vecCount++
	docid = fmt.Sprintf("%v_%v", o, c)

	return docid, c, o, vec, nil
}

// GetDimension returns the number of columns (dimension) of the sparse matrix
func (sd *SparseData) GetDimension() int64 {
	return sd.ncol
}

// GetNumVectors returns the number of vectors (rows) in the sparse matrix
func (sd *SparseData) GetNumVectors() int64 {
	return sd.nrow
}

// reset resets the vector counter and increments overflow
func (sd *SparseData) reset() {
	sd.vecCount = 0
	sd.overflow++
}

// SparseVectorToMap converts a SparseVector to a map representation for JSON storage
func SparseVectorToMap(vec SparseVector) map[string]interface{} {
	return map[string]interface{}{
		"indices": vec.Indices,
		"values":  vec.Values,
	}
}

// SparseVectorToArrays converts a sparse vector to the format [[Index Array], [Value Array]]
func SparseVectorToArrays(vec SparseVector) []interface{} {
	dimSize := len(vec.Indices)

	// Create indices array
	indicesArray := make([]interface{}, dimSize)
	for i, idx := range vec.Indices {
		indicesArray[i] = idx
	}

	// Create values array
	valuesArray := make([]interface{}, dimSize)
	for i, val := range vec.Values {
		valuesArray[i] = val
	}

	return []interface{}{indicesArray, valuesArray}
}

// SparseVectorToDenseSlice converts a sparse vector to a dense float32 slice
// This is useful when the indexer expects dense vectors
func SparseVectorToDenseSlice(vec SparseVector, dimension int) []float32 {
	dense := make([]float32, dimension)
	for i, idx := range vec.Indices {
		if int(idx) < dimension {
			dense[idx] = vec.Values[i]
		}
	}
	return dense
}

var sparseOtherStringData = map[string][]string{
	"type":     {"Casual", "Formal", "Both", "None"},
	"category": {"Document", "Query", "Passage", "Article"},
	"source":   {"MSMARCO", "Wikipedia", "News", "Academic"},
	"language": {"English", "Spanish", "French", "German"},
	"topic":    {"Science", "Technology", "History", "Arts"},
}

var sparseOtherIntData = map[string][]int{
	"relevance": {1, 2, 3, 4, 5},
}

// getSparseData creates a document with sparse vector data following siftsmall pattern
func getSparseData(cfg Config, sd *SparseData, cnt *int64) (string, map[string]interface{}, error) {
	randgen := rnd.New(rnd.NewSource(int64(cfg.VecSeed)))
	value := make(map[string]interface{})

	docid, vecnum, overflow, sparseVec, err := sd.GetValue()
	if err != nil {
		return "", nil, err
	}

	value["docid"] = docid
	value["vectornum"] = vecnum
	value["overflow"] = overflow
	value["sparse"] = SparseVectorToArrays(sparseVec)
	value["nnz"] = len(sparseVec.Indices) // Number of non-zero elements in this vector
	value["count"] = atomic.LoadInt64(cnt)

	value["gender"] = "male"
	if overflow%2 == 0 {
		value["gender"] = "female"
	}

	if overflow%3 == 0 {
		value["floats"] = math.Pi
	} else if overflow%3 == 1 {
		value["floats"] = math.E
	} else {
		value["floats"] = math.Phi
	}

	if overflow%4 == 0 {
		value["direction"] = "east"
	} else if overflow%4 == 1 {
		value["direction"] = "west"
	} else if overflow%4 == 2 {
		value["direction"] = "north"
	} else {
		value["direction"] = "south"
	}

	if overflow%10 != 0 {
		value["missing"] = "NotMissing"
	}

	for strKey, strValList := range sparseOtherStringData {
		value[strKey] = strValList[overflow%len(strValList)]
	}

	for strKey, intValList := range sparseOtherIntData {
		value[strKey] = intValList[overflow%len(intValList)]
	}

	value["phone"] = (10000000000 * (overflow % 10)) + randgen.Intn(100000000)
	value["docnum"] = overflow*100000 + vecnum

	return docid, value, nil
}
