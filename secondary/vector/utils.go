package vector

import (
	"math/rand"
	"time"
)

func genRandomVecs(dims int, n int, seed int64) [][]float32 {
	if seed != 0 {
		rand.Seed(seed)
	} else {
		rand.Seed(time.Now().UnixNano())
	}
	
	vecs := make([][]float32, n)
	for i := 0; i < n; i++ {
		vecs[i] = make([]float32, dims)
		for j := 0; j < dims; j++ {
			vecs[i][j] = rand.Float32()
		}
	}

	return vecs
}

func convertTo1D(vecs [][]float32) []float32 {

	var vecs1D []float32
	for _, vec := range vecs {
		vecs1D = append(vecs1D, vec...)
	}
	return vecs1D
}

func compareVecs(vec1, vec2 []float32) bool {
	// Check if vectors have the same dimension
	if len(vec1) != len(vec2) {
		return false
	}

	// Iterate over each element and compare
	for i := 0; i < len(vec1); i++ {
		if vec1[i] != vec2[i] {
			return false
		}
	}

	return true
}
