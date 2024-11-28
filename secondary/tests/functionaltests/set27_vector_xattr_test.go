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
	}

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
	}

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
