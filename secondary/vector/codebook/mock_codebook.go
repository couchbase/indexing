package codebook

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/couchbase/indexing/secondary/collatejson"
	"github.com/couchbase/indexing/secondary/common"
)

type MockCodebook struct {
	Trained   bool
	Centroids [][]float32
	VecMeta   *common.VectorMetadata

	InjectedErr        error
	CompDistErrOnCount int
	compDistCurrCount  int
	CompDistDelay      time.Duration
}

func NewMockCodebook(vm *common.VectorMetadata) Codebook {
	return &MockCodebook{VecMeta: vm}
}

func (mc *MockCodebook) Train(vecs []float32) error {
	mc.Centroids = kMeans(vecs, mc.VecMeta.Dimension, mc.VecMeta.Similarity, 100)
	mc.Trained = true
	return nil
}

func (mc *MockCodebook) IsTrained() bool {
	return mc.Trained
}

// FindNearestCentroids returns the indices of the k nearest centroids to the query vector
func (mc *MockCodebook) FindNearestCentroids(vec []float32, k int64) ([]int64, error) {
	if !mc.Trained {
		return nil, fmt.Errorf("Codebook is not trained")
	}

	if len(vec) != mc.VecMeta.Dimension {
		return nil, fmt.Errorf("vector dimension does not match codebook dimension")
	}

	type distanceIndexPair struct {
		distance float32
		index    int64
	}

	// Calculate distances from query vector to each centroid
	distances := make([]distanceIndexPair, len(mc.Centroids))
	for i, centroid := range mc.Centroids {
		dist := calculateDistance(vec, centroid, mc.VecMeta.Similarity)
		distances[i] = distanceIndexPair{distance: dist, index: int64(i)}
	}

	// Sort distances
	sort.Slice(distances, func(i, j int) bool {
		return distances[i].distance < distances[j].distance
	})

	// Select k nearest centroids
	nearestIndices := make([]int64, k)
	for i := int64(0); i < k; i++ {
		nearestIndices[i] = distances[i].index
	}

	return nearestIndices, nil
}

func (mc *MockCodebook) ComputeDistance(qvec []float32, fvecs []float32, dist []float32) error {
	vecCount := 0
	for i := 0; i < len(fvecs); i = i + mc.VecMeta.Dimension {
		dist[vecCount] = calculateDistance(qvec, fvecs[i:i+mc.VecMeta.Dimension], mc.VecMeta.Similarity)
		vecCount++
	}
	if mc.CompDistDelay != 0 {
		time.Sleep(mc.CompDistDelay)
	}
	mc.compDistCurrCount++
	if mc.compDistCurrCount == mc.CompDistErrOnCount {
		return mc.InjectedErr
	}
	return nil
}

func (mc *MockCodebook) DecodeVector(code []byte, vec []float32) error {
	codec := collatejson.NewCodec(16)
	buf := make([]byte, 0, 1024)
	valArr, err := codec.DecodeN1QLValue(code, buf)
	if err != nil {
		return err
	}
	for i := 0; i < mc.VecMeta.Dimension; i++ {
		val, ok := valArr.Index(i)
		if !ok {
			return fmt.Errorf("not val for index %v", i)
		}
		vali := val.Actual()
		valf := vali.(float64)
		vec[i] = float32(valf)
	}
	return nil
}

func (mc *MockCodebook) CodeSize() (int, error)                                        { return 0, nil }
func (mc *MockCodebook) EncodeVector(vec []float32, code []byte) error                 { return nil }
func (mc *MockCodebook) EncodeVectors(vecs []float32, codes []byte) error              { return nil }
func (mc *MockCodebook) ComputeDistanceTable(vec []float32) ([][]float32, error)       { return nil, nil }
func (mc *MockCodebook) ComputeDistanceWithDT(code []byte, dtable [][]float32) float32 { return 0.0 }
func (mc *MockCodebook) DecodeVectors(n int, codes []byte, vecs []float32) error       { return nil }
func (mc *MockCodebook) Size() uint64                                                  { return 0 }
func (mc *MockCodebook) Close() error                                                  { return nil }
func (mc *MockCodebook) Marshal() ([]byte, error)                                      { return nil, nil }

// -----------------
// KMeans Clustering
// -----------------

func calculateDistance(a, b []float32, distanceType common.VectorSimilarity) float32 {
	dim := len(a)
	switch distanceType {
	case common.EUCLIDEAN_SQUARED, common.L2_SQUARED, common.EUCLIDEAN, common.L2:
		var sum float32
		for i := 0; i < dim; i++ {
			diff := a[i] - b[i]
			sum += diff * diff
		}

		return sum
	case common.COSINE:
		var dotProduct, normA, normB float32
		for i := 0; i < dim; i++ {
			dotProduct += a[i] * b[i]
			normA += a[i] * a[i]
			normB += b[i] * b[i]
		}
		return 1.0 - (dotProduct / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))) //
	case common.DOT:
		var dotProduct float32
		for i := 0; i < dim; i++ {
			dotProduct += a[i] * b[i]
		}
		return dotProduct
	default:
		return 0
	}
}

// Function to calculate the centroid of a cluster
func calculateCentroid(cluster [][]float32, dim int) []float32 {
	centroid := make([]float32, dim)
	numPoints := float32(len(cluster))
	for _, point := range cluster {
		for i := 0; i < dim; i++ {
			centroid[i] += point[i]
		}
	}
	for i := 0; i < dim; i++ {
		centroid[i] /= numPoints
	}
	return centroid
}

// Function to assign points to the nearest centroid
func assignPointsToCentroids(data [][]float32, centroids [][]float32, dim int, sim common.VectorSimilarity) [][][]float32 {
	clusters := make([][][]float32, len(centroids))
	for _, point := range data {
		minDist := float32(math.MaxFloat32)
		minIndex := 0
		for i, centroid := range centroids {
			dist := calculateDistance(point, centroid, sim)
			if dist < minDist {
				minDist = dist
				minIndex = i
			}
		}
		clusters[minIndex] = append(clusters[minIndex], point)
	}
	return clusters
}

// Function to check if two sets of centroids are equal
func areCentroidsEqual(a, b [][]float32, dim int) bool {
	for i := range a {
		for j := 0; j < dim; j++ {
			if a[i][j] != b[i][j] {
				return false
			}
		}
	}
	return true
}

// K-means clustering function
func kMeans(data []float32, dim int, sim common.VectorSimilarity, maxIterations int) [][]float32 {
	numPoints := len(data) / dim
	k := int(math.Sqrt(float64(numPoints / 2)))

	// Convert flat data to matrix form
	matrixData := make([][]float32, numPoints)
	for i := 0; i < numPoints; i++ {
		matrixData[i] = data[i*dim : (i+1)*dim]
	}

	// Initialize centroids randomly
	rand.Seed(time.Now().UnixNano())
	centroids := make([][]float32, k)
	for i := range centroids {
		centroids[i] = matrixData[rand.Intn(numPoints)]
	}

	for iter := 0; iter < maxIterations; iter++ {
		clusters := assignPointsToCentroids(matrixData, centroids, dim, sim)
		newCentroids := make([][]float32, k)
		for i, cluster := range clusters {
			if len(cluster) > 0 {
				newCentroids[i] = calculateCentroid(cluster, dim)
			} else {
				newCentroids[i] = centroids[i]
			}
		}
		if areCentroidsEqual(centroids, newCentroids, dim) {
			break
		}
		centroids = newCentroids
	}

	return centroids
}
