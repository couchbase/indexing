package randdocs

import (
	"math/rand"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func generateJson() map[string]interface{} {
	inp := randString(100)
	seed := rand.New(rand.NewSource(time.Now().UnixNano()))
	return generateJsonFromInp(inp, seed)
}

func generateJsonFromInp(inp string, seed *rand.Rand) map[string]interface{} {
	doc := make(map[string]interface{})
	doc["name"] = getName(inp, seed)
	doc["email"] = getEmail(inp, seed)
	doc["alt_email"] = getAltEmail(inp, seed)
	doc["city"] = getCity(inp, seed)
	doc["county"] = getCounty(inp, seed)
	doc["state"] = getState(inp, seed)
	doc["full_state"] = getFullState(inp, seed)
	doc["country"] = getCountry(inp, seed)
	doc["realm"] = getRealm(inp, seed)
	doc["coins"] = getCoins(seed)
	doc["mobile"] = getMobile(seed)
	return doc
}

func getName(inp string, seed *rand.Rand) string {
	start := seed.Intn(len(inp) - 10)
	return inp[start : start+9]
}

func getEmail(inp string, seed *rand.Rand) string {
	start := seed.Intn(len(inp) - 20)
	return inp[start:start+8] + "@" + inp[start+8:start+16]
}

func getAltEmail(inp string, seed *rand.Rand) string {
	start := seed.Intn(len(inp) - 30)
	return inp[start:start+12] + "@" + inp[start+13:start+26]
}

func getCity(inp string, seed *rand.Rand) string {
	start := seed.Intn(len(inp) - 10)
	return inp[start : start+9]
}

func getCounty(inp string, seed *rand.Rand) string {
	start := seed.Intn(len(inp) - 10)
	return inp[start : start+9]
}

func getState(inp string, seed *rand.Rand) string {
	start := seed.Intn(len(inp) - 10)
	return inp[start : start+9]
}

func getFullState(inp string, seed *rand.Rand) string {
	start := seed.Intn(len(inp) - 30)
	return inp[start : start+20]
}

func getCountry(inp string, seed *rand.Rand) string {
	start := seed.Intn(len(inp) - 10)
	return inp[start : start+9]
}

func getRealm(inp string, seed *rand.Rand) string {
	start := seed.Intn(len(inp) - 10)
	return inp[start : start+9]
}

func getCoins(seed *rand.Rand) int {
	return seed.Intn(1000)
}

func getMobile(seed *rand.Rand) int {
	// 10 digit mobile number
	return 90000000000 + seed.Intn(100000000)
}

func generateVectors(dimension int, seed int) []float32 {
	rand.Seed(time.Now().UnixNano())

	vecs := make([]float32, dimension)
	for i := 0; i < dimension; i++ {
		vecs[i] = rand.Float32()
	}

	return vecs
}

func generateSparseVector(dimension int, seed int) []interface{} {
	rng := rand.New(rand.NewSource(int64(seed)))

	// Determine the number of non-zero dimensions
	var numDimensions int
	if dimension == 0 {
		// Variable dimensions: random between 1 and 100
		numDimensions = 1 + rng.Intn(100)
	} else {
		// Fixed dimensions
		numDimensions = dimension
	}

	// Maximum index range for sparse vector (e.g., up to 10000 possible positions)
	maxIndex := int32(10000)

	// Generate unique random indices
	indexSet := make(map[int32]struct{})
	for len(indexSet) < numDimensions {
		idx := int32(rng.Intn(int(maxIndex)))
		indexSet[idx] = struct{}{}
	}

	// Convert set to sorted slice
	indices := make([]int32, 0, numDimensions)
	for idx := range indexSet {
		indices = append(indices, idx)
	}

	// Sort indices for proper sparse vector representation
	sortInt32Slice(indices)

	// Generate random values for each index
	values := make([]float32, numDimensions)
	for i := 0; i < numDimensions; i++ {
		values[i] = rng.Float32()
	}

	return SparseVectorToArrays(SparseVector{
		Indices: indices,
		Values:  values,
	})
}

func sortInt32Slice(s []int32) {
	// Simple insertion sort for small slices
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
