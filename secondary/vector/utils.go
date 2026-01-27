package vector

import (
	"math/rand"
	"sort"
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

func genRandomSparseVecs(size int, n int, seed int64) [][]float32 {
	if seed != 0 {
		rand.Seed(seed)
	} else {
		rand.Seed(time.Now().UnixNano())
	}

	if size < 0 {
		size = 0
	}
	if size > 30000 {
		size = 30000
	}

	vecs := make([][]float32, n)
	for i := 0; i < n; i++ {
		vec := make([]float32, 1+(2*size))
		vec[0] = float32(size)

		positions := make([]int, 0, size)
		if size > 0 {
			const maxPos = 30000
			chosen := make(map[int]struct{}, size)
			for j := maxPos - size + 1; j <= maxPos; j++ {
				t := rand.Intn(j) + 1
				if _, ok := chosen[t]; ok {
					chosen[j] = struct{}{}
				} else {
					chosen[t] = struct{}{}
				}
			}
			for pos := range chosen {
				positions = append(positions, pos)
			}
		}
		sort.Ints(positions)
		for j := 0; j < size; j++ {
			vec[1+j] = float32(uint32(positions[j] + 1))
		}
		for j := 0; j < size; j++ {
			vec[1+size+j] = rand.Float32()
		}

		vecs[i] = vec
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
