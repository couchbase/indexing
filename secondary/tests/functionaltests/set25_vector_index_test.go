package functionaltests

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/queryport/client"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	kv "github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	"github.com/couchbase/indexing/secondary/tools/randdocs"
	"github.com/couchbase/query/datastore"
)

var bucket = "default"
var idx_sif10k = "idx_sift10k"
var idx_sif10k_partn = "idx_sift10k_partn"
var idx_base64 = "idx_base64"
var vectorIndexActiveTimeout = int64(180) //3 min

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
var multiIndexerConfig = false

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
func vectorSetup(t *testing.T, bucket, scope, coll string, numDocs int) {
	skipIfNotPlasma(t)

	// Drop all indexes from earlier tests
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	kv.FlushBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	e = loadVectorData(t, bucket, scope, coll, numDocs)
	FailTestIfError(e, "Error in loading vector data", t)

	vectorsLoaded = true
}

func loadVectorData(t *testing.T, bucket, scope, coll string, numDocs int) error {
	if scope == "" {
		scope = c.DEFAULT_SCOPE
	}
	if coll == "" {
		coll = c.DEFAULT_COLLECTION
	}
	if numDocs == 0 {
		numDocs = 40000
	}

	// Load Data
	cfg := randdocs.Config{
		ClusterAddr:    "127.0.0.1:9000",
		Bucket:         bucket,
		Scope:          scope,
		Collection:     coll,
		NumDocs:        numDocs,
		Iterations:     1,
		Threads:        8,
		OpsPerSec:      100000,
		UseSIFTSmall:   true,
		SkipNormalData: true,
		SIFTFVecsFile:  "../../tools/randdocs/siftsmall/siftsmall_base.fvecs",
	}
	return randdocs.Run(cfg)
}

func loadCustomData(t *testing.T, fieldType string, bucket string, docid string, dimension int) {

	doc := make(map[string]interface{})
	doc["gender"] = "male"
	docnum := 100000 + randomNum(1, 100)
	doc["docnum"] = docnum
	switch fieldType {
	case "MISSING":
		break // nothing to do as doc does not contain any
	case "JSON_NULL":
		doc["sift"] = nil
	case "INVALID":
		doc["sift"] = getRandomVector(dimension-1, 1, 1000) // Have one less dimension to make the vector invalid
	}

	var err error
	b, err := common.ConnectBucket(clusterconfig.Nodes[0], "default", bucket)
	if err != nil {
		t.Fatalf("Error observed when connecting to bucket: %v, err: %v\n", bucket, err)
	}
	defer b.Close()

	for i := 0; i < 10; i++ {
		err = b.Set(docid, 0, doc)
		if err != nil {
			log.Printf("Error observed while setting doc: %v", err)
			continue
		}
	}
	FailTestIfError(err, "Error observed while loading custom data", t)
}

func vectorSetup2(t *testing.T, numdocs int) {
	skipIfNotPlasma(t)

	// Drop all indexes from earlier tests
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	kv.FlushBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	// Load Data
	cfg := randdocs.Config{
		ClusterAddr:    "127.0.0.1:9000",
		Bucket:         bucket,
		NumDocs:        numdocs,
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
		vectorSetup(t, bucket, "", "", 40000)
	}

	// Create Index
	stmt := "CREATE INDEX " + idx_sif10k +
		" ON default(gender, sift VECTOR, docnum)" +
		" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"defer_build\":true};"
	err := createWithDeferAndBuild(idx_sif10k, BUCKET, "", "", stmt, defaultIndexActiveTimeout*2)
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
	scanResults, err := secondaryindex.Scan6(idx_sif10k, bucket, "", "", kvaddress, scans, false, false, nil, 0, limit, nil, c.AnyConsistency, nil, indexVector)
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
		vectorSetup(t, bucket, "", "", 40000)
	}

	idx_sif10k_desc := "idx_sif10k_desc"

	// Create Index
	stmt := "CREATE INDEX " + idx_sif10k_desc +
		" ON default(gender, docnum DESC, sift VECTOR)" +
		" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"defer_build\":true};"
	err := createWithDeferAndBuild(idx_sif10k_desc, BUCKET, "", "", stmt, defaultIndexActiveTimeout*2)
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
	scanResults, err := secondaryindex.Scan6(idx_sif10k_desc, bucket, "", "", kvaddress, scans, false, false, nil, 0, limit, nil, c.AnyConsistency, nil, indexVector)
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
		vectorSetup(t, bucket, "", "", 40000)
	}

	idx_vecOnly := "idx_vecOnly"

	// Drop all indexes from earlier tests
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	// Create Index
	stmt := "CREATE INDEX " + idx_vecOnly +
		" ON default(sift VECTOR)" +
		" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"defer_build\":true};"
	err := createWithDeferAndBuild(idx_vecOnly, BUCKET, "", "", stmt, defaultIndexActiveTimeout*2)
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
	scanResults, err := secondaryindex.Scan6(idx_vecOnly, bucket, "", "", kvaddress, scans, false, false, nil, 0, limit, nil, c.AnyConsistency, nil, indexVector)
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

func TestIndexConfigs(t *testing.T) {
	skipIfNotPlasma(t)

	if !vectorsLoaded {
		vectorSetup(t, bucket, "", "", 40000)
	}

	var testIndexConfigs = []struct {
		name        string
		dim         string
		description string
		similarity  string
	}{
		{"idx_sift10k_SQ4", "128", "IVF256,SQ4", "L2_SQUARED"},
		{"idx_sift10k_SQ6", "128", "IVF256,SQ6", "L2_SQUARED"},
		{"idx_sift10k_SQ8", "128", "IVF256,SQ8", "L2_SQUARED"},
		{"idx_sift10k_SQfp16", "128", "IVF256,SQfp16", "L2_SQUARED"},
		{"idx_sift10k_SQ8_DOT", "128", "IVF256,SQ8", "DOT"},
		{"idx_sift10k_SQ8_COSINE", "128", "IVF256,SQ8", "COSINE"},
		{"idx_sift10k_PQFS", "128", "IVF256,PQ32x4FS", "L2_SQUARED"},
		{"idx_sift10k_PQ_DOT", "128", "IVF256,PQ32x4", "DOT"},
		{"idx_sift10k_PQ_COSINE", "128", "IVF256,PQ32x4", "COSINE"},
	}

	// Scan setting
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

	for _, tc := range testIndexConfigs {
		t.Run(tc.name, func(t *testing.T) {
			e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
			FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

			stmt := "CREATE INDEX " + tc.name +
				" ON default(gender, sift VECTOR, docnum)" +
				" WITH { \"dimension\":" + tc.dim + ", \"description\": \"" + tc.description +
				"\", \"similarity\":\"" + tc.similarity + "\", \"defer_build\":true};"
			err := createWithDeferAndBuild(tc.name, BUCKET, "", "", stmt, defaultIndexActiveTimeout*2)
			FailTestIfError(err, "Error in creating "+tc.name, t)
			// Scan
			scanResults, err := secondaryindex.Scan6(tc.name, bucket, "", "", kvaddress, scans, false, false, nil, 0, limit, nil, c.AnyConsistency, nil, indexVector)
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
		})
	}
}

func TestVectorPartialIndex(t *testing.T) {
	skipIfNotPlasma(t)

	if !vectorsLoaded {
		vectorSetup(t, bucket, "", "", 40000)
	}

	idx_partial := "idx_sift10k_partial"

	// Drop all indexes from earlier tests
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	// Create Index
	stmt := "CREATE INDEX " + idx_partial +
		" ON default(gender, sift VECTOR, direction)" +
		" WHERE direction = \"east\"" +
		" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"defer_build\":true};"
	err := createWithDeferAndBuild(idx_partial, BUCKET, "", "", stmt, defaultIndexActiveTimeout*2)
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
	scanResults, err := secondaryindex.Scan6(idx_partial, bucket, "", "", kvaddress, scans, false, false, nil, 0, limit, nil, c.AnyConsistency, nil, indexVector)
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
		vectorSetup(t, bucket, "", "", 40000)
	}

	idx_missing_trailing := "idx_missing_trailing"

	// Drop all indexes from earlier tests
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	// Create Index
	stmt := "CREATE INDEX " + idx_missing_trailing +
		" ON default(gender, sift VECTOR, `missing`)" +
		" WHERE `missing` is not missing" +
		" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"defer_build\":true};"
	err := createWithDeferAndBuild(idx_missing_trailing, BUCKET, "", "", stmt, defaultIndexActiveTimeout*2)
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
	scanResults, err := secondaryindex.Scan6(idx_missing_trailing, bucket, "", "", kvaddress, scans, false, false, nil, 0, limit, nil, c.AnyConsistency, nil, indexVector)
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

func TestBase64VectorIndex(t *testing.T) {
	skipIfNotPlasma(t)

	if !vectorsLoaded {
		vectorSetup(t, bucket, "", "", 40000)
	}

	// Add base64 vector field first 10k docs
	n1qlstatement := "UPDATE `default` SET siftbase = ENCODE_VECTOR(sift, false);"
	_, err := tc.ExecuteN1QLStatement("127.0.0.1:9000", clusterconfig.Username, clusterconfig.Password, bucket, n1qlstatement, false, nil)
	FailTestIfError(err, "Error in adding base64 vector field", t)

	// Create Index on encoded vector field
	stmt := "CREATE INDEX " + idx_base64 +
		" ON default(gender, DECODE_VECTOR(siftbase, false) VECTOR, docnum)" +
		" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"defer_build\":true};"
	err = createWithDeferAndBuild(idx_base64, bucket, "", "", stmt, defaultIndexActiveTimeout*2)
	FailTestIfError(err, "Error in creating idx_base64", t)

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
	scanResults, err := secondaryindex.Scan6(idx_base64, bucket, "", "", kvaddress, scans, false, false, nil, 0, limit, nil, c.AnyConsistency, nil, indexVector)
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

func TestVectorPartitionedIndex(t *testing.T) {
	skipIfNotPlasma(t)

	if !vectorsLoaded {
		vectorSetup(t, bucket, "", "", 40000)
	}

	// Drop all indexes from earlier tests
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	// Create Index
	stmt := "CREATE INDEX " + idx_sif10k_partn +
		" ON default(gender, sift VECTOR, docnum)" +
		" PARTITION BY HASH(meta().id)" +
		" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"defer_build\":true};"
	err := createWithDeferAndBuild(idx_sif10k_partn, BUCKET, "", "", stmt, defaultIndexActiveTimeout*2)
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
	scanResults, err := secondaryindex.Scan6(idx_sif10k_partn, bucket, "", "", kvaddress, scans, false, false, nil, 0, limit, nil, c.AnyConsistency, nil, indexVector)
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
	multiIndexerConfig = true

	TestVectorPartitionedIndex(t)

	testScalarPredicates(t, idx_sif10k_partn)
}

func TestVectorPartnIndexWithAllSIFTQueries(t *testing.T) {
	skipIfNotPlasma(t)

	if !multiIndexerConfig {
		addTwoNodesAndRebalance("vectorPartnTest", t)
		multiIndexerConfig = true
	}

	if !vectorsLoaded {
		vectorSetup(t, bucket, "", "", 10000)
	}

	// Drop all indexes from earlier tests
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	// Create Index
	stmt := "CREATE INDEX " + idx_sif10k +
		" ON default(`type`, `category`,`country`, `brand`, `color`, `size`, `sift` VECTOR, `vectornum`)" +
		" WITH { \"defer_build\":true, \"num_replica\":1, \"num_partition\": 16, \"dimension\": 128," +
		"      \"description\": \"IVF,SQ8\",  \"similarity\": \"COSINE\", \"scan_nprobes\": 50};"
	err := createWithDeferAndBuild(idx_sif10k, BUCKET, "", "", stmt, defaultIndexActiveTimeout*2)
	FailTestIfError(err, "Error in creating idx_sift10k", t)

	queryFVecs := "../../tools/randdocs/siftsmall/siftsmall_query.fvecs"
	truthIVecs := "../../tools/randdocs/siftsmall/siftsmall_groundtruth.ivecs"
	sd, err := randdocs.OpenSiftQueryAndGroundTruth(queryFVecs, truthIVecs)
	FailTestIfError(err, "Error while opening query and ground truth files", t)

	overallRecallSum := float64(0)
	numQueries := float64(0)
	var query []float32
	var truth []uint32
	for query, truth, err = sd.GetQueryAndTruth(); err == nil; query, truth, err = sd.GetQueryAndTruth() {
		var sb strings.Builder
		for i, val := range query {
			sb.WriteString(fmt.Sprintf("%v", val))
			if i < len(query)-1 {
				sb.WriteString(", ")
			}
		}
		queryStr := sb.String()

		queryStmtFmt := "SELECT vectornum FROM default " +
			"WHERE `type`=\"Casual\" AND category=\"Shoes\" AND country=\"USA\" AND brand=\"Nike\" AND color=\"Green\" AND size=5 " +
			"ORDER BY ANN(sift, [%v], \"COSINE\", 50) " +
			"LIMIT 10"

		queryStmt := fmt.Sprintf(queryStmtFmt, queryStr)

		explainQueryStmt := fmt.Sprintf("EXPLAIN %v", queryStmt)

		explainRes, err := execN1QL(BUCKET, explainQueryStmt)
		if err != nil {
			FailTestIfError(err, "Error in running query", t)
		}

		explainResText := fmt.Sprintf("%v", explainRes)
		contains := strings.Contains(explainResText, idx_sif10k)
		if !contains {
			log.Fatalf("Query: %v is not using index", explainResText)
		}

		logging.Infof("Running Query: %s", queryStmt)

		scanResults, err := execN1QL(BUCKET, queryStmt)
		if err != nil {
			FailTestIfError(err, "Error in running query", t)
		}

		scanResultsExtracted := make([]uint32, len(scanResults))
		for i, res := range scanResults {
			resDict, ok := res.(map[string]interface{})
			if !ok {
				log.Fatal("Invalid output 1 ", res, scanResults)
			}

			resFloat, ok := resDict["vectornum"].(float64)
			if !ok {
				log.Fatal("Invalid output 2 ", resDict, scanResults)
			}

			scanResultsExtracted[i] = uint32(resFloat)
		}

		recall := recallAtR(truth[0:10], scanResultsExtracted, 10)
		overallRecallSum += recall
		numQueries += 1
		log.Printf("Recall: %v query: %v \n Truth: %v \n Results: %v", recall, queryStmt, truth[0:10], scanResultsExtracted)
	}
	overallRecall := 100 * (overallRecallSum / numQueries)
	log.Printf("Overall Recall for SIFT10K dataset with %v Queries is %v", numQueries, overallRecall)
	if overallRecall < 90 {
		log.Fatal("Test failed as overall recall is less than 90%")
	}
}

// There are three possible combinations in this test.
// a) Vector is leading: Missing, null and invalid vectors are skipped from indexing
// b) Vector is non-leading: Missing is handled by indexing MISSING
// c) Vector is non-leading: Null/invalid vectors are handled by indexing NULL
func TestNullAndMissingForVectorIndex(t *testing.T) {
	skipIfNotPlasma(t)

	// Case-1: Vector is leading
	if !vectorsLoaded {
		vectorSetup(t, bucket, "", "", 40000)
	}

	// Add three documents to the setup:
	// a) vector field missing
	// b) Vector field is json "null"
	// c) Vector field is invalid
	loadCustomData(t, "MISSING", BUCKET, "custom_missing", 128)
	loadCustomData(t, "JSON_NULL", BUCKET, "custom_json_null", 128)
	loadCustomData(t, "INVALID", BUCKET, "custom_invalid", 128)

	// Create Index
	idx_leadingVec_null_missing := "idx_leadingVec_null_missing"
	stmt := "CREATE INDEX " + idx_leadingVec_null_missing +
		" ON default(sift VECTOR, gender, docnum)" +
		" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"defer_build\":true};"
	err := createWithDeferAndBuild(idx_leadingVec_null_missing, BUCKET, "", "", stmt, defaultIndexActiveTimeout*2)
	FailTestIfError(err, "Error in creating idx_leadingVec_null_missing", t)

	// Scan on the index with MISSING should result in "0" requests to indexer
	scanStmt := fmt.Sprintf("select meta().id from %v USE INDEX(`%v`) where sift is MISSING", BUCKET, idx_leadingVec_null_missing)
	_, err = execN1QL(BUCKET, scanStmt)
	if err != nil {
		t.Fatalf("Error observed while scanning the index: %v for MISSING, err: %v", idx_leadingVec_null_missing, err)
	}

	scanStmt = fmt.Sprintf("select meta().id from %v USE INDEX(`%v`) where sift is NULL", BUCKET, idx_leadingVec_null_missing)
	_, err = execN1QL(BUCKET, scanStmt)
	if err != nil {
		t.Fatalf("Error observed while scanning the index: %v for NULL err: %v", idx_leadingVec_null_missing, err)
	}

	// calculate the number of scan requests to indexer. This index should be having zero scan requests
	time.Sleep(5 * time.Second)

	// Validation-1: Index should contain only 40K items
	stats := secondaryindex.GetPerPartnStats(clusterconfig.Username, clusterconfig.Password, kvaddress)
	items_count := stats[fmt.Sprintf("%v:%v:items_count", BUCKET, idx_leadingVec_null_missing)].(float64)
	if items_count != 40000 {
		t.Fatalf("Incorrect items in the index. Expected 40K. Actual: %v, stats: %v", items_count, stats)
	}

	num_requests := stats[fmt.Sprintf("%v:%v:num_requests", BUCKET, idx_leadingVec_null_missing)].(float64)
	if num_requests != 0 {
		t.Fatalf("Incorrect number of requests for the index. Expected 0. Actual: %v, stats: %v", num_requests, stats)
	}

	// Drop all indexes
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	idx_nonLeadingVec_null_missing := "idx_nonLeadingVec_null_missing"
	// Create Index
	stmt = "CREATE INDEX " + idx_nonLeadingVec_null_missing +
		" ON default(gender, sift VECTOR, docnum)" +
		" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"defer_build\":true};"
	err = createWithDeferAndBuild(idx_nonLeadingVec_null_missing, BUCKET, "", "", stmt, defaultIndexActiveTimeout*2)
	FailTestIfError(err, "Error in creating idx_nonLeadingVec_null_missing", t)

	// calculate the number of scan requests to indexer. This index should be having zero scan requests
	time.Sleep(5 * time.Second)

	// Validation-1: Index should contain only 40003 items
	stats = secondaryindex.GetPerPartnStats(clusterconfig.Username, clusterconfig.Password, kvaddress)
	items_count = stats[fmt.Sprintf("%v:%v:items_count", BUCKET, idx_nonLeadingVec_null_missing)].(float64)
	if items_count != 40003 {
		t.Fatalf("Incorrect items in the index. Expected 40003. Actual: %v, stats: %v", items_count, stats)
	}
}

func TestVectorResetCluster(t *testing.T) {
	skipIfNotPlasma(t)

	resetCluster(t)
}

// This test covers case where we need to increase sample size nearer to items_count
// because number of docs with vector field(300) is close to number of centroids(256)
func TestVectorIndexRetry(t *testing.T) {
	skipIfNotPlasma(t)

	numVectorFieldDocs := 256
	totalDocs := 100000

	vectorSetup2(t, numVectorFieldDocs)

	docsToCreate := generateDocs(totalDocs-numVectorFieldDocs, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	kv.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	time.Sleep(2 * time.Second)
	log.Printf("Executing Create Index ...")
	// Create Index
	stmt := "CREATE INDEX idx_sif10k " +
		" ON default(gender, sift VECTOR, docnum)" +
		" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"defer_build\":true};"

	if _, err := execN1QL(bucket, stmt); err != nil {
		log.Printf("Error while creating index : %v", err)
		FailTestIfError(err, "Error in creating idx_sift10k", t)
	}

	err := secondaryindex.BuildIndexes([]string{"idx_sif10k"}, bucket, indexManagementAddress, indexActiveTimeout)
	if err != nil {
		log.Printf("Building Index failed err: %v", err)
		FailTestIfError(err, "Error in building idx_sift10k", t)
	}

	vectorSetup(t, bucket, "", "", 40000)
}

// This test creates vector index after loading bucket with num qualifying docs(248) < centroids (256)
// When index is in error state, total 256 qualifying docs are loaded and build is issued.
func TestVectorIndexRetry2(t *testing.T) {
	skipIfNotPlasma(t)

	numVectorFieldDocs := 248
	totalDocs := 100000

	vectorSetup2(t, numVectorFieldDocs)

	docsToCreate := generateDocs(totalDocs-numVectorFieldDocs, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	kv.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	time.Sleep(2 * time.Second)
	log.Printf("Executing Create Index ...")
	// Create Index
	stmt := "CREATE INDEX idx_sif10k " +
		" ON default(gender, sift VECTOR, docnum)" +
		" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\"};"

	if _, err := execN1QL(bucket, stmt); err != nil {
		log.Printf("Error while creating index : %v", err)
	}
	log.Printf("Now increasing qualifying docs before issuing build index")

	e := loadVectorData(t, bucket, "", "", 256)
	FailTestIfError(e, "Error in loading vector data", t)

	err := secondaryindex.BuildIndexes([]string{"idx_sif10k"}, bucket, indexManagementAddress, indexActiveTimeout)
	if err != nil {
		log.Printf("Building Index failed err: %v", err)
		FailTestIfError(err, "Error in building idx_sift10k", t)
	}

	vectorSetup(t, bucket, "", "", 40000)
}

// This test ensures that if build fails due to non availability of qualifying documents, rebalance doesn't wait indefinitely.
// Rebalance should not wait/fail & index should be in error state.
func TestVectorIndexRebalTrainFail(t *testing.T) {
	skipIfNotPlasma(t)

	TestRebalanceSetupCluster(t)
	vectorsLoaded = false

	vectorSetup(t, bucket, "", "", 9999)

	log.Printf("%v: Add Node %v to the cluster for partions to spread on two nodes", t.Name(), clusterconfig.Nodes[2])
	addNodeAndRebalance(clusterconfig.Nodes[2], "index", t)

	waitForRebalanceCleanup()

	time.Sleep(1 * time.Second)

	// Create Index
	stmt := "CREATE INDEX idx_sif10k " +
		" ON default(gender, sift VECTOR, docnum)" +
		" WITH { \"nodes\":[\"127.0.0.1:9002\"] ,\"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"defer_build\":true};"

	if _, err := execN1QL(bucket, stmt); err != nil {
		log.Printf("Error while creating index : %v", err)
		FailTestIfError(err, "Error in creating idx_sift10k", t)
	}

	err := secondaryindex.BuildIndexes([]string{"idx_sif10k"}, bucket, indexManagementAddress, vectorIndexActiveTimeout)
	if err != nil {
		log.Printf("Building Index failed err: %v", err)
		FailTestIfError(err, "Error in building idx_sift10k", t)
	}

	// Test should succeed as the codebook was generated in build during index creation and for rebalance build,
	// new sampling on bucket should have 0 qualifying documents.
	kv.FlushBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	CreateDocs(9999)
	// Wait sothat test doesn't fail with error bucket items less than centroids.
	time.Sleep(5 * time.Second)

	removeNode(clusterconfig.Nodes[2], t)
	expectedStatus := map[string][]string{clusterconfig.Nodes[0]: []string{"kv", "n1ql"}, clusterconfig.Nodes[1]: []string{"index"}}
	validateClusterStatus(expectedStatus, t.Name(), t)

	printClusterConfig(t.Name(), "exit")
	waitForRebalanceCleanup()

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
			scanResults, err := secondaryindex.Scan6(idx, bucket, "", "", kvaddress, tt.scans, false, false, nil, 0, limit, nil, c.AnyConsistency, nil, indexVector)
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

// This test with two indexer nodes creates partitioned index.
// Bucket is flushed and loaded with non-vector data.
// Later second indexer is removed thus partitions move to available indexer.
// Existing codebook should be used for new partitions getting merged. ErrTraining shouldn't be there.
// If partitions are not successfully built on destination after removing second indexer, scan and test should fail.
func TestVectorIndexDcpRebalCodebook(t *testing.T) {
	skipIfNotPlasma(t)

	TestRebalanceSetupCluster(t)
	vectorsLoaded = false

	vectorSetup(t, bucket, "", "", 10000)

	log.Printf("%v: Add Node %v to the cluster for partions to spread on two nodes", t.Name(), clusterconfig.Nodes[2])
	addNodeAndRebalance(clusterconfig.Nodes[2], "index", t)

	waitForRebalanceCleanup()

	time.Sleep(1 * time.Second)

	// Create Index
	stmt := "CREATE INDEX idx_sif10k " +
		" ON default(gender, sift VECTOR, docnum) PARTITION BY HASH(meta().id) " +
		" WITH { \"num_partition\":8,\"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"defer_build\":true};"

	if _, err := execN1QL(bucket, stmt); err != nil {
		log.Printf("Error while creating index : %v", err)
		FailTestIfError(err, "Error in creating idx_sift10k", t)
	}

	time.Sleep(5 * time.Second)

	err := secondaryindex.BuildIndexes([]string{"idx_sif10k"}, bucket, indexManagementAddress, vectorIndexActiveTimeout)
	if err != nil {
		log.Printf("Building Index failed err: %v", err)
		FailTestIfError(err, "Error in building idx_sift10k", t)
	}

	// Test should succeed as the codebook was generated in build during index creation and for rebalance build,
	// new sampling on bucket should have 0 qualifying documents.
	kv.FlushBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	docsToCreate := generateDocs(40000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	kv.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	// Wait sothat test doesn't fail with error bucket items less than centroids.
	time.Sleep(5 * time.Second)

	removeNode(clusterconfig.Nodes[2], t)
	expectedStatus := map[string][]string{clusterconfig.Nodes[0]: []string{"kv", "n1ql"}, clusterconfig.Nodes[1]: []string{"index"}}
	validateClusterStatus(expectedStatus, t.Name(), t)

	printClusterConfig(t.Name(), "exit")
	waitForRebalanceCleanup()

	time.Sleep(5 * time.Second)
	state, err := secondaryindex.IndexState("idx_sif10k", bucket, indexManagementAddress)
	FailTestIfError(err, "Error in retrieving state", t)
	if state != "INDEX_STATE_ACTIVE" {
		t.Fatalf("Index:%v, not found in the expected state:%v, found:%v", "idx_sif10k", "INDEX_STATE_ACTIVE", state)
	}
	log.Printf("Index:%v, found in the expected state:%v", "idx_sif10k", state)

	vecIndexCreated = true

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
	scanResults, err := secondaryindex.Scan6("idx_sif10k", bucket, "", "", kvaddress, scans, false, false, nil, 0, limit, nil, c.AnyConsistency, nil, indexVector)
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
	kv.FlushBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)

}

// Same as TestVectorIndexRebalCodebook.
// Difference is there are total 3 indexers & partitioned index is created with num_replica:1.
func TestVectorIndexDcpRebalCodebook2(t *testing.T) {
	t.Skipf("Skipping test for now...")
	skipIfNotPlasma(t)

	TestRebalanceSetupCluster(t)
	vectorsLoaded = false

	vectorSetup(t, bucket, "", "", 40000)

	log.Printf("%v: Add Node %v to the cluster.", t.Name(), clusterconfig.Nodes[2])
	addNodeAndRebalance(clusterconfig.Nodes[2], "index", t)

	log.Printf("%v: Add Node %v to the cluster.", t.Name(), clusterconfig.Nodes[3])
	addNodeAndRebalance(clusterconfig.Nodes[3], "index", t)

	waitForRebalanceCleanup()

	time.Sleep(1 * time.Second)

	// Create Index & ensure each node has at least one partition of single replica using nodes clause.
	stmt := "CREATE INDEX idx_sif10k " +
		" ON default(gender, sift VECTOR, docnum) PARTITION BY HASH(meta().id) " +
		" WITH { \"nodes\":[\"127.0.0.1:9001\",\"127.0.0.1:9002\",\"127.0.0.1:9003\"], \"num_partition\":8,\"num_replica\":1,\"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"defer_build\":true};"

	if _, err := execN1QL(bucket, stmt); err != nil {
		log.Printf("Error while creating index : %v", err)
		FailTestIfError(err, "Error in creating idx_sift10k", t)
	}

	err := secondaryindex.BuildIndexes([]string{"idx_sif10k"}, bucket, indexManagementAddress, vectorIndexActiveTimeout)
	if err != nil {
		log.Printf("Building Index failed err: %v", err)
		FailTestIfError(err, "Error in building idx_sift10k", t)
	}

	// Test should succeed as the codebook was generated in build during index creation and for rebalance build,
	// new sampling on bucket should have 0 qualifying documents.
	kv.FlushBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	CreateDocs(40000)
	// Wait sothat test doesn't fail with error bucket items less than centroids.
	time.Sleep(15 * time.Second)

	removeNodes([]string{clusterconfig.Nodes[3]}, t)
	expectedStatus := map[string][]string{clusterconfig.Nodes[0]: []string{"kv", "n1ql"}, clusterconfig.Nodes[1]: []string{"index"}, clusterconfig.Nodes[2]: []string{"index"}}
	validateClusterStatus(expectedStatus, t.Name(), t)

	//
	log.Printf("Removing node %v & rebalance successful.", clusterconfig.Nodes[3])

	removeNodes([]string{clusterconfig.Nodes[2]}, t)
	expectedStatus = map[string][]string{clusterconfig.Nodes[0]: []string{"kv", "n1ql"}, clusterconfig.Nodes[1]: []string{"index"}}
	validateClusterStatus(expectedStatus, t.Name(), t)

	printClusterConfig(t.Name(), "exit")
	waitForRebalanceCleanup()

}

// This test ensures that if centroids for index are not provided by user and centroids set by indexer are greater than qualifying documents in keyspace.
// After Index creation and build and last training retry, index should become active.
func TestVectorIndexDynamicCentroidPQ(t *testing.T) {
	skipIfNotPlasma(t)

	TestRebalanceSetupCluster(t)
	vectorsLoaded = false

	numVectorFieldDocs := 64
	totalDocs := 100000

	vectorSetup2(t, numVectorFieldDocs)

	docsToCreate := generateDocs(totalDocs-numVectorFieldDocs, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	kv.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	time.Sleep(2 * time.Second)

	// Create Index
	stmt := "CREATE INDEX idx_sif10k " +
		" ON default(gender, sift VECTOR, docnum)" +
		" WITH { \"dimension\":128, \"description\": \"IVF,PQ32x4\", \"similarity\":\"L2_SQUARED\", \"defer_build\":true};"

	if _, err := execN1QL(bucket, stmt); err != nil {
		log.Printf("Error while creating index : %v", err)
		FailTestIfError(err, "Error in creating idx_sift10k", t)
	}

	err := secondaryindex.BuildIndexes([]string{"idx_sif10k"}, bucket, indexManagementAddress, int64(180))
	if err != nil {
		log.Printf("Building Index failed err: %v", err)
		FailTestIfError(err, "Error in building idx_sift10k", t)
	}

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
	scanResults, err := secondaryindex.Scan6("idx_sif10k", bucket, "", "", kvaddress, scans, false, false, nil, 0, limit, nil, c.AnyConsistency, nil, indexVector)
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
	kv.FlushBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)

}

// Similar to TestVectorIndexDynamicCentroidPQ
func TestVectorIndexDynamicCentroidSQ(t *testing.T) {
	skipIfNotPlasma(t)

	TestRebalanceSetupCluster(t)
	vectorsLoaded = false

	numVectorFieldDocs := 8
	totalDocs := 100000

	vectorSetup2(t, numVectorFieldDocs)

	docsToCreate := generateDocs(totalDocs-numVectorFieldDocs, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	kv.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	time.Sleep(2 * time.Second)

	// Create Index
	stmt := "CREATE INDEX idx_sif10k " +
		" ON default(gender, sift VECTOR, docnum)" +
		" WITH { \"dimension\":128, \"description\": \"IVF,SQ8\", \"similarity\":\"L2_SQUARED\", \"defer_build\":true};"

	if _, err := execN1QL(bucket, stmt); err != nil {
		log.Printf("Error while creating index : %v", err)
		FailTestIfError(err, "Error in creating idx_sif10k", t)
	}

	err := secondaryindex.BuildIndexes([]string{"idx_sif10k"}, bucket, indexManagementAddress, int64(180))
	if err != nil {
		log.Printf("Building Index failed err: %v", err)
		FailTestIfError(err, "Error in building idx_sif10k", t)
	}

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
	scanResults, err := secondaryindex.Scan6("idx_sif10k", bucket, "", "", kvaddress, scans, false, false, nil, 0, limit, nil, c.AnyConsistency, nil, indexVector)
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

	kv.FlushBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
}
