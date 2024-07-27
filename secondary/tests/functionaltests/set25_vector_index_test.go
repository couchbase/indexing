package functionaltests

import (
	"log"
	"strconv"
	"strings"
	"testing"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/queryport/client"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	kv "github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	"github.com/couchbase/indexing/secondary/tools/randdocs"
	"github.com/couchbase/query/datastore"
)

var bucket = "default"
var idx_sif10k = "idx_sift10k"
var idx_sif10k_partn = "idx_sift10k_partn"

// Fist queryvector from SIFT10K
var indexVector = &datastore.IndexVector{
	QueryVector: []float32{1, 3, 11, 110, 62, 22, 4, 0, 43, 21, 22, 18, 6, 28, 64, 9, 11, 1, 0, 0, 1, 40, 101, 21, 20, 2, 4, 2, 2, 9, 18, 35, 1, 1, 7, 25, 108, 116, 63, 2, 0, 0, 11, 74, 40, 101, 116, 3, 33, 1, 1, 11, 14, 18, 116, 116, 68, 12, 5, 4, 2, 2, 9, 102, 17, 3, 10, 18, 8, 15, 67, 63, 15, 0, 14, 116, 80, 0, 2, 22, 96, 37, 28, 88, 43, 1, 4, 18, 116, 51, 5, 11, 32, 14, 8, 23, 44, 17, 12, 9, 0, 0, 19, 37, 85, 18, 16, 104, 22, 6, 2, 26, 12, 58, 67, 82, 25, 12, 2, 2, 25, 18, 8, 2, 19, 42, 48, 11},
	Probes:      10,
}

// Top100 Nearest 0 based vector positions for above query vector from SIFT10K dataset
var expectedVectorPosTop100 = []uint32{2176, 3752, 882, 4009, 2837, 190, 3615, 816, 1045, 1884, 224, 3013, 292, 1272, 5307, 4938, 1295, 492, 9211, 3625, 1254, 1292, 1625, 3553, 1156, 146, 107, 5231, 1995, 9541, 3543, 9758, 9806, 1064, 9701, 4064, 2456, 2763, 3237, 1317, 3530, 641, 1710, 8887, 4263, 1756, 598, 370, 2776, 121, 4058, 7245, 1895, 124, 8731, 696, 4320, 4527, 4050, 2648, 1682, 2154, 1689, 2436, 2005, 3210, 4002, 2774, 924, 6630, 3449, 9814, 3515, 5375, 287, 1038, 4096, 4094, 942, 4321, 123, 3814, 97, 4293, 420, 9734, 1916, 2791, 149, 6139, 9576, 6837, 2952, 3138, 2890, 3066, 2852, 348, 3043, 3687}

var vectorsLoaded = false
var vecIndexCreated = false
var vecPartnIndexCreated = false

// Load Data
// Dataset uses SIFT10K Small and repeats this 10K multifold by adding scalar fields with different cardinality
// To load 40K Docs. 10K docs are repeated 4 times adding various scalar fields like gender, direction, floats,
// & missing.
// Fields in dataset
// sift      -> Sift vector
// vectornum -> Number from [0, 10K)
// overflow  -> Repetition number
// docid     -> {overflow}_{vectornum}
// gender    -> if (repitionCount % 2 == 0) "female" else "male"
// floats    -> switch(repitionCount % 3); case 0: math.Pi; case 1: math.E, case 2: math.Phi
// direction -> swithc(repitionCount % 4); case 0: "east"; case 1: "west"; case 2: "north"; case 3: "south"
// missing   -> if (repitionCount % 10 != 0) "NotMissing"
// docnum    -> overflow*10000 + vecnum
// count     -> atomic int value of number of docs loaded
func vectorSetup(t *testing.T) {
	skipIfNotPlasma(t)

	// Drop all indexes from earlier tests
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	kv.FlushBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	// Load Data
	cfg := randdocs.Config{
		ClusterAddr:    "127.0.0.1:9000",
		Bucket:         bucket,
		NumDocs:        40000,
		Iterations:     1,
		Threads:        8,
		OpsPerSec:      100000,
		UseSIFTSmall:   true,
		SkipNormalData: true,
		SIFTFVecsFile:  "../../tools/randdocs/siftsmall/siftsmall_base.fvecs",
	}
	randdocs.Run(cfg)

	vectorsLoaded = true
}

func TestVectorCreateIndex(t *testing.T) {
	skipIfNotPlasma(t)

	if !vectorsLoaded {
		vectorSetup(t)
	}

	// Create Index
	stmt := "CREATE INDEX " + idx_sif10k +
		" ON default(gender, sift VECTOR, docnum)" +
		" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\"};"
	_, err := execN1QL(bucket, stmt)
	FailTestIfError(err, "Error in creating idx_sift10k", t)

	vecIndexCreated = true

	// Restricting the scan to first two iteratiosn of 10K docs using the filter on docnum
	// Due to the Filter on male the search space reduced for second 10K items loaded
	// So all docIds returned should have 1_{vecNum} and {vecNums} is used on calclulation
	// of recall
	scans := qc.Scans{
		&qc.Scan{
			Filter: []*qc.CompositeElementFilter{
				&qc.CompositeElementFilter{
					Low:       "male",
					High:      "male",
					Inclusion: qc.Both,
				},
				&qc.CompositeElementFilter{},
				&qc.CompositeElementFilter{
					Low:       0,
					High:      20000,
					Inclusion: qc.Both,
				},
			},
		},
	}

	limit := int64(5)
	// Scan
	scanResults, err := secondaryindex.Scan6(idx_sif10k, bucket, kvaddress, scans, false, false, nil, 0, limit, nil, c.AnyConsistency, nil, indexVector)
	FailTestIfError(err, "Error during secondary index scan", t)

	vectorPosReturned := make([]uint32, 0)
	for k, _ := range scanResults {
		s := strings.Split(k, "_")
		vps := s[1]
		vp, err := strconv.Atoi(vps)
		if err != nil {
			t.Fatal(err)
		}
		vectorPosReturned = append(vectorPosReturned, uint32(vp))
	}

	recall := recallAtR(expectedVectorPosTop100[0:int(limit)], vectorPosReturned, int(limit))
	log.Printf("Recall: %v expected values: %v result: %v %+v", recall,
		expectedVectorPosTop100[0:int(limit)], vectorPosReturned, scanResults)
}

func TestVectorIndexScalarPredicates(t *testing.T) {
	skipIfNotPlasma(t)

	if !vecIndexCreated {
		TestVectorCreateIndex(t)
	}

	testScalarPredicates(t, idx_sif10k)
}

func TestVectorIndexWithDesc(t *testing.T) {
	skipIfNotPlasma(t)

	if !vectorsLoaded {
		vectorSetup(t)
	}

	idx_sif10k_desc := "idx_sif10k_desc"

	// Create Index
	stmt := "CREATE INDEX " + idx_sif10k_desc +
		" ON default(gender, docnum DESC, sift VECTOR)" +
		" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\"};"
	_, err := execN1QL(bucket, stmt)
	FailTestIfError(err, "Error in creating idx_sift10k", t)

	vecIndexCreated = true

	// Restricting the scan to first two iteratiosn of 10K docs using the filter on docnum
	// Due to the Filter on male the search space reduced for second 10K items loaded
	// So all docIds returned should have 1_{vecNum} and {vecNums} is used on calclulation
	// of recall
	scans := qc.Scans{
		&qc.Scan{
			Filter: []*qc.CompositeElementFilter{
				&qc.CompositeElementFilter{
					Low:       "male",
					High:      "male",
					Inclusion: qc.Both,
				},
				&qc.CompositeElementFilter{
					Low:       0,
					High:      20000,
					Inclusion: qc.Both,
				},
				&qc.CompositeElementFilter{},
			},
		},
	}

	limit := int64(5)
	// Scan
	scanResults, err := secondaryindex.Scan6(idx_sif10k_desc, bucket, kvaddress, scans, false, false, nil, 0, limit, nil, c.AnyConsistency, nil, indexVector)
	FailTestIfError(err, "Error during secondary index scan", t)

	vectorPosReturned := make([]uint32, 0)
	for k, _ := range scanResults {
		s := strings.Split(k, "_")
		vps := s[1]
		vp, err := strconv.Atoi(vps)
		if err != nil {
			t.Fatal(err)
		}
		vectorPosReturned = append(vectorPosReturned, uint32(vp))
	}

	recall := recallAtR(expectedVectorPosTop100[0:int(limit)], vectorPosReturned, int(limit))
	log.Printf("Recall: %v expected values: %v result: %v %+v", recall,
		expectedVectorPosTop100[0:int(limit)], vectorPosReturned, scanResults)
}

func TestVectorOnlyIndex(t *testing.T) {
	skipIfNotPlasma(t)

	if !vectorsLoaded {
		vectorSetup(t)
	}

	idx_vecOnly := "idx_vecOnly"

	// Drop all indexes from earlier tests
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	// Create Index
	stmt := "CREATE INDEX " + idx_vecOnly +
		" ON default(sift VECTOR)" +
		" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\"};"
	_, err := execN1QL(bucket, stmt)
	FailTestIfError(err, "Error in creating idx_sift10k", t)

	// 40K data has 10K 4 times so expecting repeated elements so making limit 5 * 4
	limit := int64(20)

	scans := qc.Scans{
		&qc.Scan{
			Filter: []*qc.CompositeElementFilter{
				&qc.CompositeElementFilter{},
			},
		},
	}

	// Scan
	scanResults, err := secondaryindex.Scan6(idx_vecOnly, bucket, kvaddress, scans, false, false, nil, 0, limit, nil, c.AnyConsistency, nil, indexVector)
	FailTestIfError(err, "Error during secondary index scan", t)

	vectorPosReturned := make([]uint32, 0)
	for k, _ := range scanResults {
		s := strings.Split(k, "_")
		vps := s[1]
		vp, err := strconv.Atoi(vps)
		if err != nil {
			t.Fatal(err)
		}
		vectorPosReturned = append(vectorPosReturned, uint32(vp))
	}

	recall := recallAtR(expectedVectorPosTop100[0:int(limit)], vectorPosReturned, int(limit))
	log.Printf("Recall: %v expected: %v result: %v %+v", recall,
		expectedVectorPosTop100[0:int(limit)], vectorPosReturned, scanResults)
}

func TestVectorPartialIndex(t *testing.T) {
	skipIfNotPlasma(t)

	if !vectorsLoaded {
		vectorSetup(t)
	}

	idx_partial := "idx_sift10k_partial"

	// Drop all indexes from earlier tests
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	// Create Index
	stmt := "CREATE INDEX " + idx_partial +
		" ON default(gender, sift VECTOR, direction)" +
		" WHERE direction = \"east\"" +
		" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\"};"
	_, err := execN1QL(bucket, stmt)
	FailTestIfError(err, "Error in creating idx_sift10k", t)

	// WHERE direction = \"east\"" will restrict the data to first 10K all of them will be female too
	scans := qc.Scans{
		&qc.Scan{
			Filter: []*qc.CompositeElementFilter{
				&qc.CompositeElementFilter{
					Low:       "female",
					High:      "female",
					Inclusion: qc.Both,
				},
				&qc.CompositeElementFilter{},
			},
		},
	}

	limit := int64(5)
	// Scan
	scanResults, err := secondaryindex.Scan6(idx_partial, bucket, kvaddress, scans, false, false, nil, 0, limit, nil, c.AnyConsistency, nil, indexVector)
	FailTestIfError(err, "Error during secondary index scan", t)

	vectorPosReturned := make([]uint32, 0)
	for k, _ := range scanResults {
		s := strings.Split(k, "_")
		vps := s[1]
		vp, err := strconv.Atoi(vps)
		if err != nil {
			t.Fatal(err)
		}
		vectorPosReturned = append(vectorPosReturned, uint32(vp))
	}

	recall := recallAtR(expectedVectorPosTop100[0:int(limit)], vectorPosReturned, int(limit))
	log.Printf("Recall: %v expected: %v result: %v %+v", recall,
		expectedVectorPosTop100[0:int(limit)], vectorPosReturned, scanResults)
}

func TestVectorIndexMissingTrailing(t *testing.T) {
	skipIfNotPlasma(t)

	if !vectorsLoaded {
		vectorSetup(t)
	}

	idx_missing_trailing := "idx_missing_trailing"

	// Drop all indexes from earlier tests
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	// Create Index
	stmt := "CREATE INDEX " + idx_missing_trailing +
		" ON default(gender, sift VECTOR, `missing`)" +
		" WHERE `missing` is not missing" +
		" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\"};"
	_, err := execN1QL(bucket, stmt)
	FailTestIfError(err, "Error in creating idx_sift10k", t)

	vecIndexCreated = true

	// In first 10K docs "missing" field is missing
	// Restricting the scan to only first 10K using " WHERE `missing` is not missing" in index defn
	scans := qc.Scans{
		&qc.Scan{
			Filter: []*qc.CompositeElementFilter{
				&qc.CompositeElementFilter{
					Low:       "female",
					High:      "female",
					Inclusion: qc.Both,
				},
				&qc.CompositeElementFilter{},
			},
		},
	}

	limit := int64(5)
	// Scan
	scanResults, err := secondaryindex.Scan6(idx_missing_trailing, bucket, kvaddress, scans, false, false, nil, 0, limit, nil, c.AnyConsistency, nil, indexVector)
	FailTestIfError(err, "Error during secondary index scan", t)

	vectorPosReturned := make([]uint32, 0)
	for k, _ := range scanResults {
		s := strings.Split(k, "_")
		vps := s[1]
		vp, err := strconv.Atoi(vps)
		if err != nil {
			t.Fatal(err)
		}
		vectorPosReturned = append(vectorPosReturned, uint32(vp))
	}

	recall := recallAtR(expectedVectorPosTop100[0:int(limit)], vectorPosReturned, int(limit))
	log.Printf("Recall: %v expected: %v result: %v %+v", recall,
		expectedVectorPosTop100[0:int(limit)], vectorPosReturned, scanResults)
}

func TestVectorPartitionedIndex(t *testing.T) {
	skipIfNotPlasma(t)

	if !vectorsLoaded {
		vectorSetup(t)
	}

	// Drop all indexes from earlier tests
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	// Create Index
	stmt := "CREATE INDEX " + idx_sif10k_partn +
		" ON default(gender, sift VECTOR, docnum)" +
		" PARTITION BY HASH(meta().id)" +
		" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\"};"
	_, err := execN1QL(bucket, stmt)
	FailTestIfError(err, "Error in creating idx_sift10k", t)

	vecPartnIndexCreated = true

	limit := int64(5)

	// Restricting the scan to first two iteratiosn of 10K docs using the filter on docnum
	// Due to the Filter on male the search space reduced for second 10K items loaded
	// So all docIds returned should have 1_{vecNum} and {vecNums} is used on calclulation
	// of recall
	scans := qc.Scans{
		&qc.Scan{
			Filter: []*qc.CompositeElementFilter{
				&qc.CompositeElementFilter{
					Low:       "male",
					High:      "male",
					Inclusion: qc.Both,
				},
				&qc.CompositeElementFilter{},
				&qc.CompositeElementFilter{
					Low:       0,
					High:      20000,
					Inclusion: qc.Both,
				},
			},
		},
	}

	// Scan
	scanResults, err := secondaryindex.Scan6(idx_sif10k_partn, bucket, kvaddress, scans, false, false, nil, 0, limit, nil, c.AnyConsistency, nil, indexVector)
	FailTestIfError(err, "Error during secondary index scan", t)

	vectorPosReturned := make([]uint32, 0)
	for k, _ := range scanResults {
		s := strings.Split(k, "_")
		vps := s[1]
		vp, err := strconv.Atoi(vps)
		if err != nil {
			t.Fatal(err)
		}
		vectorPosReturned = append(vectorPosReturned, uint32(vp))
	}

	recall := recallAtR(expectedVectorPosTop100[0:int(limit)], vectorPosReturned, int(limit))
	log.Printf("Recall: %v expected: %v result: %v %+v", recall,
		expectedVectorPosTop100[0:int(limit)], vectorPosReturned, scanResults)

	vecPartnIndexCreated = true
}

func TestVectorPartnIndexScalarPredicates(t *testing.T) {
	skipIfNotPlasma(t)

	if !vecPartnIndexCreated {
		TestVectorPartitionedIndex(t)
	}

	testScalarPredicates(t, idx_sif10k_partn)
}

func TestVectorPartnIndexMultipleNodes(t *testing.T) {
	skipIfNotPlasma(t)

	// Drop all indexes from earlier tests
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	addTwoNodesAndRebalance("vectorPartnTest", t)
	TestVectorPartitionedIndex(t)

	testScalarPredicates(t, idx_sif10k_partn)
}

func TestVectorResetCluster(t *testing.T) {
	skipIfNotPlasma(t)

	resetCluster(t)
}

// =================
// Utility Functions
// =================

func skipIfNotPlasma(t *testing.T) {
	if clusterconfig.IndexUsing != "plasma" {
		t.Skipf("Test %s is only valid with plasma storage", t.Name())
		return
	}
}

// recallAtR calculates the recall@R given the relevant items and the retrieved items
func recallAtR(relevantItems, retrievedItems []uint32, R int) float64 {
	if R > len(retrievedItems) {
		R = len(retrievedItems)
	}

	relevantSet := make(map[uint32]struct{})
	for _, item := range relevantItems {
		relevantSet[item] = struct{}{}
	}

	relevantCount := 0
	for i := 0; i < R; i++ {
		if _, found := relevantSet[retrievedItems[i]]; found {
			relevantCount++
		}
	}

	r := float64(relevantCount) / float64(len(relevantItems))

	if r < 0.5 {
		log.Printf("Recall value is less than 0.5 i.e. %v relevantItems: %v retrievedItems: %v",
			r, relevantItems, retrievedItems)
	}

	return r
}

// testScalarPredicates runs various scans of an given index
func testScalarPredicates(t *testing.T, idx string) {
	tests := []struct {
		name  string
		scans client.Scans
	}{
		{
			name: "InclusiveOneSidedLow",
			scans: qc.Scans{
				&qc.Scan{
					Filter: []*qc.CompositeElementFilter{
						&qc.CompositeElementFilter{
							Low:       "male",
							Inclusion: qc.Low,
						},
						&qc.CompositeElementFilter{},
						&qc.CompositeElementFilter{
							Low:       0,
							High:      20000,
							Inclusion: qc.Both,
						},
					},
				},
			},
		},
		{
			name: "InclusiveOneSidedHigh",
			scans: qc.Scans{
				&qc.Scan{
					Filter: []*qc.CompositeElementFilter{
						&qc.CompositeElementFilter{
							High:      "male",
							Inclusion: qc.High,
						},
						&qc.CompositeElementFilter{},
						&qc.CompositeElementFilter{
							Low:       10000,
							High:      20000,
							Inclusion: qc.Both,
						},
					},
				},
			},
		},
		{
			name: "ExclusiveOneSidedHigh",
			scans: qc.Scans{
				&qc.Scan{
					Filter: []*qc.CompositeElementFilter{
						&qc.CompositeElementFilter{
							High:      "male",
							Inclusion: qc.Neither,
						},
						&qc.CompositeElementFilter{},
						&qc.CompositeElementFilter{
							Low:       0,
							High:      10000,
							Inclusion: qc.Both,
						},
					},
				},
			},
		},
		{
			name: "ExclusiveOneSidedLow",
			scans: qc.Scans{
				&qc.Scan{
					Filter: []*qc.CompositeElementFilter{
						&qc.CompositeElementFilter{
							Low:       "female",
							Inclusion: qc.Neither,
						},
						&qc.CompositeElementFilter{},
						&qc.CompositeElementFilter{
							Low:       0,
							High:      20000,
							Inclusion: qc.Both,
						},
					},
				},
			},
		},
		{
			name: "EmptySpan",
			scans: qc.Scans{
				&qc.Scan{
					Filter: []*qc.CompositeElementFilter{
						&qc.CompositeElementFilter{
							Low:       "null",
							High:      "null",
							Inclusion: qc.Neither,
						},
					},
				},
			},
		},
		{
			name: "OR",
			scans: qc.Scans{
				&qc.Scan{
					Filter: []*qc.CompositeElementFilter{
						&qc.CompositeElementFilter{
							Low:       "male",
							High:      "male",
							Inclusion: qc.Both,
						},
						&qc.CompositeElementFilter{},
						&qc.CompositeElementFilter{
							Low:       0,
							High:      20000,
							Inclusion: qc.Both,
						},
					},
				},
				&qc.Scan{
					Filter: []*qc.CompositeElementFilter{
						&qc.CompositeElementFilter{
							Low:       "female",
							High:      "female",
							Inclusion: qc.Both,
						},
						&qc.CompositeElementFilter{},
						&qc.CompositeElementFilter{
							Low:       0,
							High:      20000,
							Inclusion: qc.Both,
						},
					},
				},
			},
		},
		{
			name: "NOT",
			scans: qc.Scans{
				&qc.Scan{
					Filter: []*qc.CompositeElementFilter{
						&qc.CompositeElementFilter{
							Low:       "null",
							High:      "female",
							Inclusion: qc.Neither,
						},
						&qc.CompositeElementFilter{},
						&qc.CompositeElementFilter{
							Low:       0,
							High:      20000,
							Inclusion: qc.Both,
						},
					},
				},
				&qc.Scan{
					Filter: []*qc.CompositeElementFilter{
						&qc.CompositeElementFilter{
							Low:       "female",
							Inclusion: qc.Neither,
						},
						&qc.CompositeElementFilter{},
						&qc.CompositeElementFilter{
							Low:       0,
							High:      20000,
							Inclusion: qc.Both,
						},
					},
				},
			},
		},
		{
			name: "MissingTrailingSpans",
			scans: qc.Scans{
				&qc.Scan{
					Filter: []*qc.CompositeElementFilter{
						&qc.CompositeElementFilter{
							Low:       "male",
							High:      "male",
							Inclusion: qc.Both,
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limit := int64(5)
			// Scan
			scanResults, err := secondaryindex.Scan6(idx, bucket, kvaddress, tt.scans, false, false, nil, 0, limit, nil, c.AnyConsistency, nil, indexVector)
			FailTestIfError(err, "Error during secondary index scan", t)

			vectorPosReturned := make([]uint32, 0)
			for k, _ := range scanResults {
				s := strings.Split(k, "_")
				vps := s[1]
				vp, err := strconv.Atoi(vps)
				if err != nil {
					t.Fatal(err)
				}
				vectorPosReturned = append(vectorPosReturned, uint32(vp))
			}

			recall := recallAtR(expectedVectorPosTop100[0:int(limit)], vectorPosReturned, int(limit))
			log.Printf("Recall: %v expected: %v result: %v %+v", recall,
				expectedVectorPosTop100[0:int(limit)], vectorPosReturned, scanResults)
		})
	}
}
