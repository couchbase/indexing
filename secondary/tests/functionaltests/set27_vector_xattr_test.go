package functionaltests

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	"github.com/couchbase/indexing/secondary/tools/randdocs"

	"github.com/couchbase/gocb/v2"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
)

type User struct {
	Name      string   `json:"name"`
	Email     string   `json:"email"`
	Interests []string `json:"interests"`
	Gender    string   `json:"gender"`
	Vectornum int      `json:"vectornum"`
	Docnum    int      `json:"docnum"`
}

var indexActiveTimeout = int64(120) //2 min

// Note:
// gocb/v2 returning CONNECTION_ERROR with http/https 9000/19000 thus use memcached port
func getAddrWithMemcachedPort(addr string) (string, error) {
	address := strings.Split(addr, ":")
	if len(address) == 1 {
		return "", errors.New(fmt.Sprintf("Invalid clusterconfig KVAddress:%v", address[0]))
	}
	return address[0] + ":12000", nil
}

func getRandomVector(n int, min float64, max float64) []float64 {

	arr := make([]float64, n)
	for i := 0; i < n; i++ {
		arr[i] = min + rand.Float64()*(max-min)
	}
	return arr
}

func TestVectorIndexXATTR(t *testing.T) {

	skipIfNotPlasma(t)

	// Drop all indexes from earlier tests
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	kvutility.EnableBucketFlush("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.FlushBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	connectionString, err := getAddrWithMemcachedPort(clusterconfig.KVAddress)
	FailTestIfError(err, "Error in getAddrWithMemcachedPort", t)

	bucketName := "default"

	// For a secure cluster connection, use `couchbases://<your-cluster-ip>` instead.
	cluster, err := gocb.Connect("couchbase://"+connectionString, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: clusterconfig.Username,
			Password: clusterconfig.Password,
		},
	})
	if err != nil {
		log.Fatal(err)
		tc.HandleError(err, "Failed to connect to cluster")
	}
	defer cluster.Close(nil)

	kvutility.DeleteBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.CreateBucket(bucketName, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
	time.Sleep(2 * time.Second)
	bucket := cluster.Bucket(bucketName)

	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		log.Fatal(err)
	}

	col := bucket.Scope("_default").Collection("_default")

	var sd *randdocs.SiftData
	sd = randdocs.OpenSiftData("../../tools/randdocs/siftsmall/siftsmall_base.fvecs")
	//Load documents & insert XATTRs.
	numDocs := 40000
	for i := 0; i < numDocs; i++ {
		docid, vecnum, overflow, sift, err := sd.GetValue()

		gender := "male"
		if overflow%2 == 0 {
			gender = "female"
		}
		_, err = col.Upsert(docid,
			User{
				Name:      "Jade" + strconv.Itoa(i),
				Email:     "jade" + strconv.Itoa(i) + "@test-email.com",
				Gender:    gender,
				Vectornum: vecnum,
				Docnum:    overflow*10000 + vecnum,
				Interests: []string{"Rowing", "Swimming"},
			}, nil)
		if err != nil {
			log.Fatal(err)
		}

		// Use two non-array xattrs & one array xattr.
		newXattrs := interface{}([]interface{}{"abcde"})
		_, err = col.MutateIn(docid, []gocb.MutateInSpec{
			gocb.UpsertSpec("xattr1", newXattrs, &gocb.UpsertSpecOptions{IsXattr: true}),
		}, nil)
		if err != nil {
			fmt.Println("Failed to update document xattrs:", err)
			FailTestIfError(err, "Error while loading xattr1", t)
			return
		}

		newXattrs2 := interface{}(sift)
		_, err2 := col.MutateIn(docid, []gocb.MutateInSpec{
			gocb.UpsertSpec("xattr2", newXattrs2, &gocb.UpsertSpecOptions{IsXattr: true}),
		}, nil)
		if err2 != nil {
			fmt.Println("Failed to update document xattr2:", err)
			FailTestIfError(err, "Error while loading xattr2", t)
			return
		}

		newXattrs3 := interface{}([]interface{}{rand.Int()})
		_, err3 := col.MutateIn(docid, []gocb.MutateInSpec{
			gocb.UpsertSpec("xattr3", newXattrs3, &gocb.UpsertSpecOptions{IsXattr: true}),
		}, nil)
		if err3 != nil {
			fmt.Println("Failed to update document xattrs:", err)
			return
		}
	}

	//Create Index definition
	time.Sleep(2 * time.Second)
	idx := "idx_xattr"
	n1qlstatement := fmt.Sprintf("CREATE INDEX `%v` on `%v`(gender, meta().xattrs.xattr2 VECTOR, docnum) USING GSI WITH { \"dimension\":128, \"train_list\":10000, \"description\": \"IVF1024,PQ8x8\", \"similarity\":\"L2_SQUARED\"};", idx, bucketName)
	_, err = tc.ExecuteN1QLStatement(kvaddress, clusterconfig.Username, clusterconfig.Password, bucketName, n1qlstatement, false, nil)
	FailTestIfError(err, "Error while creating secondary index", t)
	log.Printf("Created index %v", idx)
	time.Sleep(2 * time.Second)

	//Issue build
	err = secondaryindex.BuildIndexes2([]string{idx}, bucketName, "_default", "_default", indexManagementAddress, indexActiveTimeout)
	FailTestIfError(err, "Error while building indexes ", t)

	stats1 := secondaryindex.GetStats(clusterconfig.Username, clusterconfig.Password, kvaddress)
	itm_cnt_1 := int(stats1[bucketName+":"+idx+":items_count"].(float64))
	log.Printf("Index: %v items_count:%v", idx, itm_cnt_1)

	if itm_cnt_1 != numDocs {
		t.Fatalf("Items count mismatch with numDocs:%v", numDocs)
	}

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
			},
		},
	}

	limit := int64(5)
	// Scan
	scanResults, err := secondaryindex.Scan6(idx, bucketName, "", "", kvaddress, scans, false, false, nil, 0, limit, nil, c.AnyConsistency, nil, indexVector)
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

// This test creates two vector indexes on seperate xattrs with one normal index.
func TestVectorIndexXATTRMultiple(t *testing.T) {

	skipIfNotPlasma(t)

	// Drop all indexes from earlier tests
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	kvutility.EnableBucketFlush("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.FlushBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	connectionString, err := getAddrWithMemcachedPort(clusterconfig.KVAddress)
	FailTestIfError(err, "Error in getAddrWithMemcachedPort", t)

	bucketName := "default"

	// For a secure cluster connection, use `couchbases://<your-cluster-ip>` instead.
	cluster, err := gocb.Connect("couchbase://"+connectionString, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: clusterconfig.Username,
			Password: clusterconfig.Password,
		},
	})
	if err != nil {
		log.Fatal(err)
		tc.HandleError(err, "Failed to connect to cluster")
	}
	defer cluster.Close(nil)

	kvutility.DeleteBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.CreateBucket(bucketName, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
	time.Sleep(2 * time.Second)
	bucket := cluster.Bucket(bucketName)

	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		log.Fatal(err)
	}

	col := bucket.Scope("_default").Collection("_default")

	//Load documents & insert XATTRs.
	numDocs := 50000
	var wg sync.WaitGroup
	wg.Add(1)
	wg.Add(1)
	for i := 0; i < numDocs; i++ {

		_, err = col.Upsert("docid"+strconv.Itoa(i),
			User{
				Name:      "Jade" + strconv.Itoa(i),
				Email:     "jade" + strconv.Itoa(i) + "@test-email.com",
				Interests: []string{"Swimming", "Rowing"},
			}, nil)
		if err != nil {
			log.Fatal(err)
		}
	}

	go func() {
		defer wg.Done()

		for i := 0; i < numDocs; i++ {

			// Use two non-array xattrs & two array xattr fields.
			newXattrs := interface{}([]interface{}{"abcde"})
			_, err := col.MutateIn("docid"+strconv.Itoa(i), []gocb.MutateInSpec{
				gocb.UpsertSpec("xattr1", newXattrs, &gocb.UpsertSpecOptions{IsXattr: true}),
			}, nil)
			if err != nil {
				fmt.Println("Failed to update document xattrs1:", err)
				FailTestIfError(err, "Error while loading xattr1", t)
				return
			}

			vector := getRandomVector(128, -1.0, 1.0)
			newXattrs2 := interface{}(vector)
			_, err2 := col.MutateIn("docid"+strconv.Itoa(i), []gocb.MutateInSpec{
				gocb.UpsertSpec("xattr2", newXattrs2, &gocb.UpsertSpecOptions{IsXattr: true}),
			}, nil)
			if err2 != nil {
				fmt.Println("Failed to update document xattrs2:", err)
				FailTestIfError(err, "Error while loading xattr2", t)
				return
			}

		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < numDocs; i++ {

			newXattrs3 := interface{}([]interface{}{rand.Int()})
			_, err3 := col.MutateIn("docid"+strconv.Itoa(i), []gocb.MutateInSpec{
				gocb.UpsertSpec("xattr3", newXattrs3, &gocb.UpsertSpecOptions{IsXattr: true}),
			}, nil)
			if err3 != nil {
				fmt.Println("Failed to update document xattrs3:", err)
				FailTestIfError(err, "Error while loading xattr3", t)
				return
			}

			vector := getRandomVector(128, -1.0, 1.0)
			newXattrs4 := interface{}(vector)
			_, err4 := col.MutateIn("docid"+strconv.Itoa(i), []gocb.MutateInSpec{
				gocb.UpsertSpec("xattr4", newXattrs4, &gocb.UpsertSpecOptions{IsXattr: true}),
			}, nil)
			if err4 != nil {
				fmt.Println("Failed to update document xattrs4:", err)
				FailTestIfError(err, "Error while loading xattr4", t)
				return
			}
		}
	}()

	wg.Wait()
	time.Sleep(2 * time.Second)
	log.Printf("Loaded bucket %v with %v docs.", bucketName, numDocs)

	//Create Index Definitions
	idx1 := "idx_xattr"
	n1qlstatement := fmt.Sprintf("CREATE INDEX `%v` on `%v`(name, meta().xattrs.xattr2 VECTOR) USING GSI WITH { \"defer_build\":true,\"dimension\":128, \"train_list\":10000, \"description\": \"IVF1024,PQ8x8\", \"similarity\":\"L2_SQUARED\"};", idx1, bucketName)
	_, err = tc.ExecuteN1QLStatement(kvaddress, clusterconfig.Username, clusterconfig.Password, bucketName, n1qlstatement, false, nil)
	FailTestIfError(err, "Error while creating secondary index", t)
	log.Printf("Created index defn: %v", n1qlstatement)

	idx2 := "idx_xattr2"
	n1qlstatement = fmt.Sprintf("CREATE INDEX `%v` on `%v`(email, meta().xattrs.xattr4 VECTOR) USING GSI WITH { \"defer_build\":true,\"dimension\":128, \"train_list\":10000, \"description\": \"IVF1024,PQ8x8\", \"similarity\":\"L2_SQUARED\"};", idx2, bucketName)
	_, err = tc.ExecuteN1QLStatement(kvaddress, clusterconfig.Username, clusterconfig.Password, bucketName, n1qlstatement, false, nil)
	FailTestIfError(err, "Error while creating secondary index", t)
	log.Printf("Created index defn: %v", n1qlstatement)

	idx3 := "idx_name"
	n1qlstatement = fmt.Sprintf("CREATE INDEX `%v` on `%v`(email, meta().xattrs.xattr4 VECTOR) USING GSI WITH { \"defer_build\":true,\"dimension\":128, \"train_list\":10000, \"description\": \"IVF1024,PQ8x8\", \"similarity\":\"L2_SQUARED\"};", idx3, bucketName)
	_, err = tc.ExecuteN1QLStatement(kvaddress, clusterconfig.Username, clusterconfig.Password, bucketName, n1qlstatement, false, nil)
	FailTestIfError(err, "Error while creating secondary index", t)
	log.Printf("Created index defn: %v", n1qlstatement)

	//Issue build
	err = secondaryindex.BuildIndexes2([]string{idx1, idx2, idx3}, bucketName, "_default", "_default", indexManagementAddress, indexActiveTimeout)
	FailTestIfError(err, "Error while building indexes ", t)

	stats1 := secondaryindex.GetStats(clusterconfig.Username, clusterconfig.Password, kvaddress)
	itm_cnt_1 := int(stats1[bucketName+":"+idx1+":items_count"].(float64))
	log.Printf("Index: %v items_count:%v", idx1, itm_cnt_1)

	stats2 := secondaryindex.GetStats(clusterconfig.Username, clusterconfig.Password, kvaddress)
	itm_cnt_2 := int(stats2[bucketName+":"+idx2+":items_count"].(float64))
	log.Printf("Index: %v items_count:%v", idx2, itm_cnt_2)

	stats3 := secondaryindex.GetStats(clusterconfig.Username, clusterconfig.Password, kvaddress)
	itm_cnt_3 := int(stats3[bucketName+":"+idx3+":items_count"].(float64))
	log.Printf("Index: %v items_count:%v", idx3, itm_cnt_3)

	if itm_cnt_1 != numDocs || itm_cnt_2 != numDocs || itm_cnt_3 != numDocs {
		t.Fatalf("Items count mismatch with numDocs:%v", numDocs)
	}

}

var sparseSmallXATTRLoaded = false

// vectorSetupSparseSmallXATTR loads the SparseSmall (MS MARCO) dataset with the sparse
// vector stored as XATTR (key: sparse_xattr) rather than in the main document body.
// Scalar fields (type, category, source, language, topic, relevance, vectornum, etc.)
// remain in the document. Note: VectorInXATTR overrides type="Document" for all docs.
func vectorSetupSparseSmallXATTR(t *testing.T) {
	skipIfNotPlasma(t)

	kvutility.FlushBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	resetVectorDataSetupFlags()
	sparseSmallXATTRLoaded = false

	cfg := randdocs.Config{
		ClusterAddr:    clusterconfig.KVAddress,
		Bucket:         bucket,
		NumDocs:        numSparseSmallDocs,
		Iterations:     1,
		Threads:        8,
		OpsPerSec:      100000,
		UseSparseSmall: true,
		SkipNormalData: true,
		SparseCSRFile:  sparseSmallDataPath + "base_small.csr",
		VectorInXATTR:  true,
		Username:       clusterconfig.Username,
		Password:       clusterconfig.Password,
	}
	err := randdocs.Run(cfg)
	FailTestIfError(err, "Error loading sparsesmall XATTR data", t)

	sparseSmallXATTRLoaded = true
}

// testSparseVectorXATTRSparseSmall is the shared implementation for both the bhive and
// composite sparse XATTR tests. It:
//  1. Loads sparsesmall data with vectors stored in XATTR (sparse_xattr key).
//  2. Creates the appropriate index on meta().xattrs.sparse_xattr.
//  3. Verifies items_count matches numSparseSmallDocs.
//  4. Verifies via EXPLAIN that the index is used by the query planner.
//  5. Runs 32 queries from the dataset ground truth and asserts 100% recall
//     (achievable because nprobes=256 is exhaustive for IVF256).
func testSparseVectorXATTRSparseSmall(t *testing.T, isBhive bool) {
	skipIfNotPlasma(t)

	if !sparseSmallXATTRLoaded {
		vectorSetupSparseSmallXATTR(t)
	}

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	const xattrField = "meta().xattrs.sparse_xattr"
	nprobes := 256

	var idxName, stmt string
	if isBhive {
		idxName = "idx_sparse_xattr_bhive"
		stmt = fmt.Sprintf(
			"CREATE VECTOR INDEX %v ON %v(%v SPARSE VECTOR)"+
				" WITH {\"defer_build\":true, \"description\":\"IVF256\", \"similarity\":\"DOT\", \"scan_nprobes\":%d};",
			idxName, bucket, xattrField, nprobes)
	} else {
		idxName = "idx_sparse_xattr_comp"
		stmt = fmt.Sprintf(
			"CREATE INDEX %v ON %v(`type`, `category`, `source`, `language`, `topic`, `relevance`, %v SPARSE VECTOR, `vectornum`)"+
				" WITH {\"defer_build\":true, \"description\":\"IVF256\", \"similarity\":\"DOT\", \"scan_nprobes\":%d};",
			idxName, bucket, xattrField, nprobes)
	}

	err := createWithDeferAndBuild(idxName, bucket, "", "", stmt, defaultIndexActiveTimeout*2)
	FailTestIfError(err, "Error creating "+idxName, t)

	// Validate items count
	stats := secondaryindex.GetPerPartnStats(clusterconfig.Username, clusterconfig.Password, kvaddress)
	itemsCount := int(stats[fmt.Sprintf("%v:%v:items_count", bucket, idxName)].(float64))
	if itemsCount != numSparseSmallDocs {
		t.Fatalf("%v: items_count mismatch: expected %v, got %v", idxName, numSparseSmallDocs, itemsCount)
	}
	log.Printf("%v: items_count=%v OK", idxName, itemsCount)

	// Verify index is used in the query plan
	var planCheckStmt string
	if isBhive {
		planCheckStmt = fmt.Sprintf(
			"EXPLAIN SELECT vectornum FROM %v ORDER BY sparse_vector_distance(%v, [[1055, 2002, 3000], [0.5, 1.2, 0.8]], %d) LIMIT 10",
			bucket, xattrField, nprobes)
	} else {
		planCheckStmt = fmt.Sprintf(
			"EXPLAIN SELECT vectornum FROM %v WHERE `type`=\"Document\" AND `category`=\"Document\" "+
				"ORDER BY sparse_vector_distance(%v, [[1055, 2002, 3000], [0.5, 1.2, 0.8]], %d) LIMIT 10",
			bucket, xattrField, nprobes)
	}
	planRes, perr := execN1QL(bucket, planCheckStmt)
	FailTestIfError(perr, "Error in EXPLAIN for "+idxName, t)
	if !strings.Contains(fmt.Sprintf("%v", planRes), idxName) {
		t.Fatalf("%v: index not used in query plan: %v", idxName, planRes)
	}
	log.Printf("%v: plan verification passed", idxName)

	// Load queries and ground truth from the SparseSmall dataset
	sd, err := randdocs.OpenSparseData(sparseSmallDataPath + "base_small.csr")
	FailTestIfError(err, "Error opening sparse base data", t)
	err = sd.LoadQueryAndTruth(sparseSmallDataPath+"queries.dev.csr", sparseSmallDataPath+"base_small.dev.gt")
	FailTestIfError(err, "Error loading query and ground truth files", t)

	groundTruthK := sd.GetGroundTruthK()
	if groundTruthK > 10 {
		groundTruthK = 10 // cap at recall@10
	}

	var totalRel, totalQueries int64
	numQueriesInDataset := 32

	for qi := 0; qi < numQueriesInDataset; qi++ {
		query, truth, qerr := sd.GetQueryAndTruth()
		if qerr != nil {
			break
		}

		queryVecStr := formatSparseQueryVector(query)

		var queryStmt string
		if isBhive {
			queryStmt = fmt.Sprintf(
				"SELECT vectornum FROM %v ORDER BY sparse_vector_distance(%v, %v, %d, %d) LIMIT %d",
				bucket, xattrField, queryVecStr, nprobes, 500, groundTruthK)
		} else {
			// VectorInXATTR sets type="Document" for all docs; other scalar fields use overflow=0 values.
			queryStmt = fmt.Sprintf(
				"SELECT vectornum FROM %v "+
					"WHERE `type`=\"Document\" AND `category`=\"Document\" AND `source`=\"MSMARCO\" "+
					"AND `language`=\"English\" AND `topic`=\"Science\" AND `relevance`=1 "+
					"ORDER BY sparse_vector_distance(%v, %v, %d) LIMIT %d",
				bucket, xattrField, queryVecStr, nprobes, groundTruthK)
		}

		scanResults, serr := execN1QL(bucket, queryStmt)
		FailTestIfError(serr, fmt.Sprintf("Error running query %d on %v", qi, idxName), t)

		truthTopK := truth
		if len(truthTopK) > groundTruthK {
			truthTopK = truthTopK[:groundTruthK]
		}
		truthSet := make(map[int32]struct{})
		for _, idx := range truthTopK {
			truthSet[idx] = struct{}{}
		}

		var matches int64
		for _, res := range scanResults {
			resDict, ok := res.(map[string]interface{})
			if !ok {
				t.Fatalf("Query %d: unexpected result type %T", qi, res)
			}
			vecnum, ok := resDict["vectornum"].(float64)
			if !ok {
				t.Fatalf("Query %d: missing vectornum in result: %v", qi, resDict)
			}
			if _, found := truthSet[int32(vecnum)]; found {
				matches++
			}
		}

		recall := float64(matches) / float64(len(truthTopK))
		totalRel += matches
		totalQueries++
		log.Printf("%v: Query %d: Recall=%.3f (%d/%d)", idxName, qi, recall, matches, len(truthTopK))
	}

	overallRecall := 100 * float64(totalRel) / (float64(groundTruthK) * float64(totalQueries))
	log.Printf("%v: Overall Recall=%.2f%% (%d rel / %d queries)", idxName, overallRecall, totalRel, totalQueries)

	if overallRecall < 100 {
		t.Fatalf("%v: expected 100%% recall with exhaustive scan (nprobes=%d), got %.2f%%",
			idxName, nprobes, overallRecall)
	}
}

// TestSparseBhiveVectorXATTRSparseSmall creates a bhive sparse vector index
// (CREATE VECTOR INDEX) on meta().xattrs.sparse_xattr loaded from the SparseSmall
// (MS MARCO) dataset, then validates index usage and 100% recall@10.
func TestSparseBhiveVectorXATTRSparseSmall(t *testing.T) {
	testSparseVectorXATTRSparseSmall(t, true /*isBhive*/)
}

// TestSparseCompositeVectorXATTRSparseSmall creates a composite sparse vector index
// (CREATE INDEX) on scalar fields + meta().xattrs.sparse_xattr from the SparseSmall
// (MS MARCO) dataset, then validates index usage and 100% recall@10.
func TestSparseCompositeVectorXATTRSparseSmall(t *testing.T) {
	testSparseVectorXATTRSparseSmall(t, false /*isBhive*/)
}
