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
