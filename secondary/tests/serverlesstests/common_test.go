package serverlesstests

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log" //"os"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/cbauth"
	c "github.com/couchbase/indexing/secondary/common"
	json "github.com/couchbase/indexing/secondary/common/json"
	"github.com/couchbase/indexing/secondary/logging"
	cluster "github.com/couchbase/indexing/secondary/tests/framework/clusterutility"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	parsec "github.com/prataprc/goparsec"
	"github.com/prataprc/monster"
	"github.com/prataprc/monster/common"
	"gopkg.in/couchbase/gocb.v1"
)

///////////////////////////////////////////////////////////////////////////////
// common_test holds vars and funcs that are used by more than one test set, to
// avoid creating build dependencies between test sets, which enables running a
// single set in isolation by moving the other sets to a DISABLED/ directory.
///////////////////////////////////////////////////////////////////////////////

var docs, mut_docs tc.KeyValues
var defaultlimit int64 = 100000000000
var kvaddress, indexManagementAddress, indexScanAddress string
var clusterconfig tc.ClusterConfiguration
var dataFilePath, mutationFilePath string
var defaultIndexActiveTimeout int64 = 600 // 10 mins to wait for index to become active
var skipsetup bool
var indexerLogLevel string
var bucketOpWaitDur = time.Duration(15)
var seed int
var proddir, bagdir string
var tmpclient string

func TestMain(m *testing.M) {
	log.Printf("In TestMain()")
	logging.SetLogLevel(logging.Error)

	var configpath string
	flag.StringVar(&configpath, "cbconfig", "../config/clusterrun_conf.json", "Path of the configuration file with data about Couchbase Cluster")
	flag.StringVar(&secondaryindex.UseClient, "useclient", "gsi", "Client to be used for tests. Values can be gsi or n1ql")
	flag.BoolVar(&skipsetup, "skipsetup", false, "Skip setup steps like flush and drop all indexes")
	flag.StringVar(&indexerLogLevel, "loglevel", "info", "indexer.settings.log_level. info / debug")
	flag.Parse()
	clusterconfig = tc.GetClusterConfFromFile(configpath)
	kvaddress = clusterconfig.KVAddress
	indexManagementAddress = clusterconfig.KVAddress
	indexScanAddress = clusterconfig.KVAddress
	seed = 1
	proddir, bagdir = tc.FetchMonsterToolPath()

	// Initialises a 1KV + 2 index nodes in the cluster
	// Both index nodes belong to different server groups
	if err := initClusterFromREST(); err != nil {
		tc.HandleError(err, "Error while initialising cluster")
	}

	// setup cbauth
	if _, err := cbauth.InternalRetryDefaultInit(kvaddress, clusterconfig.Username, clusterconfig.Password); err != nil {
		log.Fatalf("Failed to initialize cbauth: %s", err)
	}

	//Disable backfill
	err := secondaryindex.ChangeIndexerSettings("queryport.client.settings.backfillLimit", float64(0), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	//Set queryport log level to Warn to reduce logging
	err = secondaryindex.ChangeIndexerSettings("queryport.client.log_level", "Warn", clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	//Enable QE Rest server
	err = secondaryindex.ChangeIndexerSettings("indexer.api.enableTestServer", true, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot_init_build.moi.interval", float64(60000), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot.moi.interval", float64(60000), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.log_level", indexerLogLevel, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.storage_mode.disable_upgrade", true, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in change setting for indexer.settings.storage_mode.disable_upgrade")

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.rebalance.blob_storage_scheme", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in change setting for indexer.settings.rebalance.blob_storage_scheme")

	err = secondaryindex.ChangeIndexerSettings("indexer.plasma.serverless.shardCopy.dbg", true, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in change setting for indexer.settings.rebalance.blob_storage_prefix")

	err = secondaryindex.ChangeIndexerSettings("indexer.rebalance.serverless.transferBatchSize", 2, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in change setting for indexer.settings.rebalance.blob_storage_prefix")

	err = secondaryindex.ChangeIndexerSettings("indexer.client_stats_refresh_interval", 500, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in change setting for indexer.client_stats_refresh_interval")

	if clusterconfig.IndexUsing != "" {
		// Set clusterconfig.IndexUsing only if it is specified in config file. Else let it default to gsi
		log.Printf("Using %v for creating indexes", clusterconfig.IndexUsing)
		secondaryindex.IndexUsing = clusterconfig.IndexUsing

		err := secondaryindex.ChangeIndexerSettings("indexer.settings.storage_mode", secondaryindex.IndexUsing, clusterconfig.Username, clusterconfig.Password, kvaddress)
		tc.HandleError(err, "Error in ChangeIndexerSettings")
	}

	time.Sleep(5 * time.Second)

	secondaryindex.CheckCollation = true

	// Working with Users10k and Users_mut dataset.
	u, _ := user.Current()
	dataFilePath = filepath.Join(u.HomeDir, "testdata/Users10k.txt.gz")
	mutationFilePath = filepath.Join(u.HomeDir, "testdata/Users_mut.txt.gz")
	tc.DownloadDataFile(tc.IndexTypesStaticJSONDataS3, dataFilePath, true)
	tc.DownloadDataFile(tc.IndexTypesMutationJSONDataS3, mutationFilePath, true)
	docs = datautility.LoadJSONFromCompressedFile(dataFilePath, "docid")
	mut_docs = datautility.LoadJSONFromCompressedFile(mutationFilePath, "docid")

	if !skipsetup {
		e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
		tc.HandleError(e, "Error in DropAllSecondaryIndexes")

		log.Printf("Emptying the default bucket")
		kvutility.DeleteBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
		for i := range buckets {
			kvutility.DeleteBucket(buckets[i], "", clusterconfig.Username, clusterconfig.Password, kvaddress)
		}
		time.Sleep(bucketOpWaitDur * time.Second)
		cleanupShardDir(&testing.T{})

		cleanupStorageDir(&testing.T{})
	}

	// For serverless tests, set fatal log-level to control logging
	secondaryindex.SetLogLevel(logging.Fatal)

	os.Exit(m.Run())
}

func FailTestIfError(err error, msg string, t *testing.T) {
	if err != nil {
		t.Fatalf("%v: %v\n", msg, err)
	}
}

func FailTestIfNoError(err error, msg string, t *testing.T) {
	if err == nil {
		t.Fatalf("%v: %v\n", msg, err)
	}
}

func addDocIfNotPresentInKV(docKey string) {
	if _, present := docs[docKey]; present == false {
		keysToBeSet := make(tc.KeyValues)
		keysToBeSet[docKey] = mut_docs[docKey]
		kvutility.SetKeyValues(keysToBeSet, "default", "", clusterconfig.KVAddress)
		// Update docs object with newly added keys and remove those keys from mut_docs
		docs[docKey] = mut_docs[docKey]
		delete(mut_docs, docKey)
	}
}

var options struct {
	bagdir  string
	outfile string
	nonterm string
	seed    int
	count   int
	help    bool
	debug   bool
}

func GenerateJsons(count, seed int, prodfile, bagdir string) tc.KeyValues {
	runtime.GOMAXPROCS(50)
	keyValues := make(tc.KeyValues)
	options.outfile = "./out.txt"
	options.bagdir = bagdir
	options.count = count
	options.seed = seed

	var err error

	// read production-file
	text, err := ioutil.ReadFile(prodfile)
	if err != nil {
		log.Fatal(err)
	}
	// compile
	scope := compile(parsec.NewScanner(text)).(common.Scope)
	scope = monster.BuildContext(scope, uint64(seed), bagdir, prodfile)
	nterms := scope["_nonterminals"].(common.NTForms)

	// evaluate
	for i := 0; i < options.count; i++ {
		scope = scope.RebuildContext()
		val := evaluate("root", scope, nterms["s"])
		jsonString := val.(string)
		byt := []byte(jsonString)
		var dat map[string]interface{}
		if err := json.Unmarshal(byt, &dat); err != nil {
			panic(err)
		}
		dockey := dat["docid"].(string)
		keyValues[dockey] = dat
	}

	return keyValues
}

func GenerateBinaryDocs(numDocs int, bucketName, bucketPassword, hostaddress, serverUsername, serverPassword string) tc.KeyValues {
	docsToCreate := generateDocs(numDocs, "users.prod")
	kvutility.SetBinaryValues(docsToCreate, bucketName, bucketPassword, hostaddress)
	return docsToCreate
}

func GenerateBinaryDocsWithXATTRS(numDocs int, bucketName, bucketPassword, hostaddress, serverUsername, serverPassword string) tc.KeyValues {
	docsToCreate := generateDocs(numDocs, "users.prod")
	kvutility.SetBinaryValuesWithXattrs(docsToCreate, bucketName, bucketPassword, hostaddress, serverUsername, serverPassword)
	return docsToCreate
}

func compile(s parsec.Scanner) parsec.ParsecNode {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("%v at %v", r, s.GetCursor())
		}
	}()
	root, _ := monster.Y(s)
	return root
}

func evaluate(name string, scope common.Scope, forms []*common.Form) interface{} {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("%v", r)
		}
	}()
	return monster.EvalForms(name, scope, forms)
}

func generateDocs(numDocs int, prodFileName string) tc.KeyValues {
	seed++
	prodfile := filepath.Join(proddir, prodFileName)
	docsToCreate := GenerateJsons(numDocs, seed, prodfile, bagdir)
	return docsToCreate
}

// Copy newDocs to docs
func UpdateKVDocs(newDocs, docs tc.KeyValues) {
	for k, v := range newDocs {
		docs[k] = v
	}
}

func CreateDocs(num int) {
	i := 0
	keysToBeSet := make(tc.KeyValues)
	for key, value := range mut_docs {
		keysToBeSet[key] = value
		i++
		if i == num {
			break
		}
	}
	kvutility.SetKeyValues(keysToBeSet, "default", "", clusterconfig.KVAddress)
	// Update docs object with newly added keys and remove those keys from mut_docs
	for key, value := range keysToBeSet {
		docs[key] = value
		delete(mut_docs, key)
	}
}

func CreateDocsForCollection(bucketName, collectionID string, num int) tc.KeyValues {
	kvdocs := generateDocs(num, "users.prod")
	kvutility.SetKeyValuesForCollection(kvdocs, bucketName, collectionID, "", clusterconfig.KVAddress)
	return kvdocs
}

func DeleteDocs(num int) {
	i := 0
	keysToBeDeleted := make(tc.KeyValues)
	for key, value := range docs {
		keysToBeDeleted[key] = value
		i++
		if i == num {
			break
		}
	}
	kvutility.DeleteKeys(keysToBeDeleted, "default", "", clusterconfig.KVAddress)
	// Update docs object with deleted keys and add those keys from mut_docs
	for key, value := range keysToBeDeleted {
		delete(docs, key)
		mut_docs[key] = value
	}
}

// delete docs and do not update mut_docs object
func DeleteDocs2(num int) {
	i := 0
	keysToBeDeleted := make(tc.KeyValues)
	for key, value := range docs {
		keysToBeDeleted[key] = value
		i++
		if i == num {
			break
		}
	}
	kvutility.DeleteKeys(keysToBeDeleted, "default", "", clusterconfig.KVAddress)
	// Update docs object with deleted keys
	for key, _ := range keysToBeDeleted {
		delete(docs, key)
	}
}

func UpdateDocs(num int) {
	i := 0

	// Pick some docs from mut_docs
	keysFromMutDocs := make(tc.KeyValues)
	for key, value := range mut_docs {
		keysFromMutDocs[key] = value
		i++
		if i == num {
			break
		}
	}
	// and , Add them to docs
	keysToBeSet := make(tc.KeyValues)
	for _, value := range keysFromMutDocs {
		n := randomNum(0, float64(len(docs)-1))
		i = 0
		var k string
		for k, _ = range docs {
			if i == n {
				break
			}
			i++
		}
		docs[k] = value
		keysToBeSet[k] = value
	}
	log.Printf("Num of keysFromMutDocs: %d", len(keysFromMutDocs))
	log.Printf("Updating number of documents: %d", len(keysToBeSet))
	kvutility.SetKeyValues(keysToBeSet, "default", "", clusterconfig.KVAddress)
}

func randomNum(min, max float64) int {
	rand.Seed(time.Now().UnixNano())
	return int(rand.Float64()*(max-min) + min)
}

func randString(n int) string {
	chars := []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ,#")
	rand.Seed(time.Now().UnixNano())
	b := make([]rune, n)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

func randSpecialString(n int) string {
	chars := []rune("0123vwxyz€ƒ†Š™š£§©®¶µß,#")
	rand.Seed(time.Now().UnixNano())
	b := make([]rune, n)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

func randomBool() bool {
	rand.Seed(time.Now().UnixNano())
	switch randomNum(0, 2) {
	case 0:
		return false
	case 1:
		return true
	}
	return true
}

func random_char() string {
	chars := []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	return string(chars[rand.Intn(len(chars))])
}

func verifyDeletedPath(Pth string) error {
	var err error
	_, errStat := os.Stat(Pth)
	if errStat == nil {
		err = errors.New("os.Stat was successful for deleted directory")
		return err
	}

	if !os.IsNotExist(errStat) {
		errMsg := fmt.Sprintf("errStat = %v", errStat)
		err = errors.New("Directory may exists even if it was expected to be deleted." + errMsg)
		return err
	}

	return nil
}

func verifyAtleastOnePathExists(paths []string) bool {
	out := false
	for _, path := range paths {
		_, errStat := os.Stat(path)
		if errStat == nil {
			return true
		}

		if os.IsNotExist(errStat) {
			out = out || false
		}
	}
	return out
}

func forceKillMemcacheD() {
	// restart mcd process
	fmt.Println("Restarting memcached process ...")
	tc.KillMemcacheD()
	time.Sleep(20 * time.Second)
}

func forceKillIndexer() {
	// restart the indexer
	fmt.Println("Restarting indexer process ...")
	tc.KillIndexer()
	time.Sleep(20 * time.Second)
}

func restful_clonebody(src map[string]interface{}) map[string]interface{} {
	dst := make(map[string]interface{})
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func restful_getall() (map[string]interface{}, error) {
	url, err := makeurl("/internal/indexes")
	if err != nil {
		return nil, err
	}

	log.Printf("GET all indexes\n")
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	log.Printf("%v\n", resp.Status)
	if restful_checkstatus(resp.Status) == true {
		return nil, fmt.Errorf("restful_getall() status: %v", resp.Status)
	}
	indexes := make(map[string]interface{}, 0)
	respbody, _ := ioutil.ReadAll(resp.Body)
	if len(respbody) == 0 {
		return nil, nil
	}
	err = json.Unmarshal(respbody, &indexes)
	if err != nil {
		return nil, err
	}
	return indexes, nil
}

// makeurl makes a URL to call a REST API on an arbitrary Index node. If there are no Index nodes,
// it returns an error.
func makeurl(path string) (string, error) {
	indexers, _ := secondaryindex.GetIndexerNodesHttpAddresses(indexManagementAddress)
	if len(indexers) == 0 {
		return "", fmt.Errorf("no indexer node")
	}
	return makeUrlForIndexNode(indexers[0], path), nil
}

// makeUrlForIndexNode makes a URL to call a REST API on a specific Index node.
//   nodeAddr - ipAddr:port of the target Index node
//   path - REST API path portion of the URL, including leading /
func makeUrlForIndexNode(nodeAddr string, path string) string {
	return fmt.Sprintf("http://%s:%s@%v%v",
		clusterconfig.Username, clusterconfig.Password, nodeAddr, path)
}

func restful_checkstatus(status string) bool {
	st := strings.ToLower(status)
	x := strings.Contains(st, strconv.Itoa(http.StatusBadRequest))
	x = x || strings.Contains(st, strconv.Itoa(http.StatusInternalServerError))
	x = x || strings.Contains(st, strconv.Itoa(http.StatusMethodNotAllowed))
	x = x || strings.Contains(st, strconv.Itoa(http.StatusNotFound))
	return x
}

// doHttpRequest is a helper that makes an HTTP request, then consumes and closes the body of the
// response to avoid leaking the TCP connection.
func doHttpRequest(req *http.Request) (resp *http.Response, err error) {
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		return resp, err
	}
	defer resp.Body.Close()
	_, err = ioutil.ReadAll(resp.Body)

	return resp, err
}

// doHttpRequestReturnBody is a helper that makes an HTTP request, then retrieves the contents of and
// closes the body of the response to avoid leaking the TCP connection. On success both the response
// and the contents of the body are returned.
func doHttpRequestReturnBody(req *http.Request) (resp *http.Response, respbody []byte, err error) {
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		return resp, nil, err
	}
	defer resp.Body.Close()
	respbody, err = ioutil.ReadAll(resp.Body)

	return resp, respbody, err
}

func waitForIndexStatus(bucket, scope, collection, index, indexStatus string, t *testing.T) {
	for {
		select {
		case <-time.After(time.Duration(3 * time.Minute)):
			t.Fatalf("Index: %v, bucket: %v, scope: %v, collection: %v did not become active after 3 minutes",
				index, bucket, scope, collection)
			break
		default:
			status, err := secondaryindex.GetIndexStatus(clusterconfig.Username, clusterconfig.Password, kvaddress)
			if status != nil && err == nil {
				indexes := status["indexes"].([]interface{})
				for _, indexEntry := range indexes {
					entry := indexEntry.(map[string]interface{})

					if bucket != entry["bucket"].(string) ||
						scope != entry["scope"].(string) ||
						collection != entry["collection"].(string) {
						continue
					}

					if index == entry["index"].(string) {
						if strings.ToLower(indexStatus) == strings.ToLower(entry["status"].(string)) {
							log.Printf("Index status is: %v for index: %v, bucket: %v, scope: %v, collection: %v",
								indexStatus, index, bucket, scope, collection)
							return
						}
					}
				}
			}
			if err != nil {
				log.Printf("waitForIndexActive:: Error while retrieving GetIndexStatus, err: %v", err)
			}
			time.Sleep(1 * time.Second)
		}
	}
}

func execN1qlAndWaitForStatus(n1qlStatement, bucket, scope, collection, index, status string, t *testing.T) {
	log.Printf("Executing N1ql statement: %v", n1qlStatement)
	// Create a partitioned index with defer_build:true
	_, err := tc.ExecuteN1QLStatement(kvaddress, clusterconfig.Username, clusterconfig.Password, url.PathEscape(bucket),
		n1qlStatement, false, gocb.RequestPlus)
	FailTestIfError(err, fmt.Sprintf("Error during n1qlExecute: %v", n1qlStatement), t)

	// Wait for index created
	waitForIndexStatus(bucket, scope, collection, index, status, t)
	waitForIndexStatus(bucket, scope, collection, index+" (replica 1)", status, t)
}

// scanIndexReplicas scan's the index and validates if all the replicas of the index are retruning
// valid results
func scanIndexReplicas(index, bucket, scope, collection string, replicaIds []int, numScans, numDocs, numPartitions int, t *testing.T) {

	statsBeforeScan := secondaryindex.GetPerPartnStats(clusterconfig.Username, clusterconfig.Password, kvaddress)
	log.Printf("scanIndexReplicas: Scanning all for index: %v, bucket: %v, scope: %v, collection: %v", index, bucket, scope, collection)
	// Scan the index num_scans times
	for i := 0; i < numScans; i++ {
		scanResults, err := secondaryindex.ScanAll2(index, bucket, scope, collection, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
		if err != nil {
			errStr := fmt.Sprintf("Observed error: %v while scanning index: %v, bucket: %v, scope: %v, collection: %v, scanResults: %v",
				numDocs, index, bucket, scope, collection, len(scanResults))
			FailTestIfError(err, errStr, t)
		}
		if len(scanResults) != numDocs {
			errStr := fmt.Sprintf("Error in ScanAll. Expected len(scanResults): %v, actual: %v", numDocs, len(scanResults))
			FailTestIfError(err, errStr, t)
		}
	}

	// Get parititioned stats for the index from all nodes
	statsAfterScan := secondaryindex.GetPerPartnStats(clusterconfig.Username, clusterconfig.Password, kvaddress)

	// construct corresponding replica strings's from replicaIds
	replicas := make([]string, len(replicaIds))
	for i, replicaId := range replicaIds {
		if replicaId == 0 {
			replicas[i] = ""
		} else {
			replicas[i] = fmt.Sprintf(" (replica %v)", replicaId)
		}
	}

	// For a non-parititioned index, numPartitions is `1` and partnID is `0`
	// For a partitioned index, partnId ranges from `1` to numPartitions
	startPartn := 0
	endPartn := 1
	if numPartitions > 1 {
		startPartn = 1
		endPartn = numPartitions + 1
	}

	// For each index, get num_requests (per partition), num_scan_errors, num_scan_timeouts
	num_requests := make([][]float64, len(replicas))
	for i := 0; i < len(replicas); i++ {
		num_requests[i] = make([]float64, endPartn)
	}
	num_scan_errors := 0.0
	num_scan_timeouts := 0.0

	for i := 0; i < len(replicas); i++ {
		for j := startPartn; j < endPartn; j++ {
			indexName := fmt.Sprintf("%s:%s:%s:%s %v%s", bucket, scope, collection, index, j, replicas[i])
			if collection == "_default" {
				indexName = fmt.Sprintf("%s:%s %v%s", bucket, index, j, replicas[i])
			}
			num_requests[i][j] = statsAfterScan[indexName+":num_requests"].(float64) - statsBeforeScan[indexName+":num_requests"].(float64)
			num_scan_errors += statsAfterScan[indexName+":num_scan_errors"].(float64) - statsBeforeScan[indexName+":num_scan_errors"].(float64)
			num_scan_timeouts += statsAfterScan[indexName+":num_scan_timeouts"].(float64) - statsBeforeScan[indexName+":num_scan_timeouts"].(float64)
		}
	}

	logStats := func() {
		log.Printf("ScanAllReplicas: Indexer stats are: %v", statsAfterScan)
	}
	if num_scan_errors > 0 {
		logStats()
		t.Fatalf("Expected '0' scan errors. Found: %v scan errors", num_scan_errors)
	}

	if num_scan_timeouts > 0 {
		logStats()
		t.Fatalf("Expected '0' scan timeouts. Found: %v scan timeouts", num_scan_errors)
	}

	// For each partition (across all replicas), total_scan_requests should match numScans
	for j := startPartn; j < endPartn; j++ {
		total_scan_requests := 0.0
		for i := 0; i < len(replicas); i++ {
			if num_requests[i][j] == 0 {
				logStats()
				instId, defnId, _ := getInstAndDefnId(bucket, scope, collection, index+replicas[i])
				t.Fatalf("Zero scan requests seen for index: %v, partnId: %v, bucket: %v, "+
					"scope: %v, collection: %v, instId: %v, defnId: %v", index+replicas[i], j, bucket, scope, collection, instId, defnId)
			}
			total_scan_requests += num_requests[i][j]
		}

		if total_scan_requests != (float64)(numScans) {
			logStats()
			t.Fatalf("Total scan requests for all partitions does not match the total scans. "+
				"Expected: %v, actual: %v, index (all replicas): %v, partitionID: %v, bucket: %v, "+
				"scope: %v, collection: %v", numScans, total_scan_requests, index, j, bucket, scope, collection)
		}
	}
}

func waitForRebalanceCleanup(nodeAddr string, t *testing.T) {
	indexerAddr := secondaryindex.GetIndexHttpAddrOnNode(clusterconfig.Username, clusterconfig.Password, nodeAddr)
	if indexerAddr == "" {
		t.Fatalf("indexerAddr is empty for nodeAddr: %v", nodeAddr)
	}

	var finalErr error

	for i := 0; i < 300; i++ {
		val := func() bool {
			client := &http.Client{}
			address := "http://" + indexerAddr + "/rebalanceCleanupStatus"

			req, _ := http.NewRequest("GET", address, nil)
			req.SetBasicAuth(clusterconfig.Username, clusterconfig.Password)
			req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
			resp, err := client.Do(req)
			if resp != nil {
				defer resp.Body.Close()
			}
			if err != nil { // Indexer's HTTP port might not be up yet if indexer restarts. Wait for some time and retry
				finalErr = err
				time.Sleep(1 * time.Second)
				return false
			} else {
				finalErr = nil
			}

			if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
				log.Printf(address)
				log.Printf("%v", req)
				log.Printf("%v", resp)
				log.Printf("rebalanceCleanupStatus failed")
			}

			body, _ := ioutil.ReadAll(resp.Body)
			if string(body) == "done" {
				return true
			}
			if i%5 == 0 {
				log.Printf("Waiting for rebalance cleanup to finish on node: %v", nodeAddr)
			}
			time.Sleep(1 * time.Second)
			return false
		}()

		if val {
			return
		}
	}
	// todo : error out if response is error
	tc.HandleError(finalErr, "Get RebalanceCleanupStatus")
}

func waitForTokenCleanup(nodeAddr string, t *testing.T) {
	indexerAddr := secondaryindex.GetIndexHttpAddrOnNode(clusterconfig.Username, clusterconfig.Password, nodeAddr)
	if indexerAddr == "" {
		t.Fatalf("indexerAddr is empty for nodeAddr: %v", nodeAddr)
	}

	var finalErr error

	for i := 0; i < 300; i++ {
		val := func() bool {
			client := &http.Client{}
			address := "http://" + indexerAddr + "/listRebalanceTokens"

			req, _ := http.NewRequest("GET", address, nil)
			req.SetBasicAuth(clusterconfig.Username, clusterconfig.Password)
			req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
			resp, err := client.Do(req)
			if resp != nil {
				defer resp.Body.Close()
			}
			if err != nil { // Indexer's HTTP port might not be up yet if indexer restarts. Wait for some time and retry
				finalErr = err
				time.Sleep(1 * time.Second)
				return false
			} else {
				finalErr = nil
			}

			if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
				log.Printf(address)
				log.Printf("%v", req)
				log.Printf("%v", resp)
				log.Printf("listRebalanceTokens failed")
			}

			body, _ := ioutil.ReadAll(resp.Body)
			if string(body) == "null\n" {
				return true
			}
			if i%5 == 0 {
				log.Printf("Waiting for rebalance cleanup (as rebalance tokens still exist) to finish on node: %v", nodeAddr)
			}
			time.Sleep(1 * time.Second)
			return false
		}()

		if val {
			return
		}
	}
	// todo : error out if response is error
	tc.HandleError(finalErr, "Get listRebalanceTokens")
}

// Return shardID's for each bucket on this node
func getShardIds(nodeAddr string, t *testing.T) map[string]map[string]bool {
	indexerAddr := secondaryindex.GetIndexHttpAddrOnNode(clusterconfig.Username, clusterconfig.Password, nodeAddr)
	if indexerAddr == "" {
		t.Fatalf("indexerAddr is empty for nodeAddr: %v", nodeAddr)
	}

	client := &http.Client{}
	address := "http://" + indexerAddr + "/getLocalIndexMetadata"

	req, _ := http.NewRequest("GET", address, nil)
	req.SetBasicAuth(clusterconfig.Username, clusterconfig.Password)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	resp, err := client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	// todo : error out if response is error
	tc.HandleError(err, "Get getLocalIndexMetadata")

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		log.Printf(address)
		log.Printf("%v", req)
		log.Printf("%v", resp)
		log.Printf("getLocalIndexMetadata failed")
	}

	response := make(map[string]interface{})
	body, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(body, &response)

	if _, ok := response["topologies"]; !ok {
		return nil
	}

	out := make(map[string]map[string]bool)

	for _, topology := range response["topologies"].([]interface{}) {
		topo := topology.(map[string]interface{})
		if _, ok := topo["definitions"]; !ok {
			continue
		}
		definitions := topo["definitions"].([]interface{})

		for _, definition := range definitions {
			defn := definition.(map[string]interface{})
			if _, ok := defn["bucket"]; !ok {
				continue
			}
			bucket := defn["bucket"].(string)

			if _, ok := defn["instances"]; !ok {
				continue
			}
			instances := defn["instances"].([]interface{})

			for _, instance := range instances {
				inst := instance.(map[string]interface{})
				if _, ok := inst["partitions"]; !ok {
					continue
				}
				partitions := inst["partitions"].([]interface{})

				for _, partition := range partitions {
					partn := partition.(map[string]interface{})
					if _, ok := partn["shardIds"]; !ok {
						continue
					}
					shardIds := partn["shardIds"].([]interface{})

					if _, ok := out[bucket]; !ok {
						out[bucket] = make(map[string]bool)
					}
					for _, shardId := range shardIds {
						switch shardId.(type) {
						case int64:
							out[bucket][fmt.Sprintf("%v", shardId.(int64))] = true
						case float64:
							out[bucket][fmt.Sprintf("%v", uint64(shardId.(float64)))] = true
						}
					}
				}
			}
		}
	}
	return out
}

func getIndexStorageDirOnNode(nodeAddr string, t *testing.T) string {

	var strIndexStorageDir string
	workspace := os.Getenv("WORKSPACE")
	if workspace == "" {
		workspace = "../../../../../../../../"
	}

	if strings.HasSuffix(workspace, "/") == false {
		workspace += "/"
	}

	switch nodeAddr {
	case clusterconfig.Nodes[0]:
		strIndexStorageDir = workspace + "ns_server" + "/data/n_0/data/@2i/"
	case clusterconfig.Nodes[1]:
		strIndexStorageDir = workspace + "ns_server" + "/data/n_1/data/@2i/"
	case clusterconfig.Nodes[2]:
		strIndexStorageDir = workspace + "ns_server" + "/data/n_2/data/@2i/"
	case clusterconfig.Nodes[3]:
		strIndexStorageDir = workspace + "ns_server" + "/data/n_3/data/@2i/"
	case clusterconfig.Nodes[4]:
		strIndexStorageDir = workspace + "ns_server" + "/data/n_4/data/@2i/"
	case clusterconfig.Nodes[5]:
		strIndexStorageDir = workspace + "ns_server" + "/data/n_5/data/@2i/"
	case clusterconfig.Nodes[6]:
		strIndexStorageDir = workspace + "ns_server" + "/data/n_6/data/@2i/"

	}

	absIndexStorageDir, err1 := filepath.Abs(strIndexStorageDir)
	FailTestIfError(err1, "Error while finding absolute path", t)
	return absIndexStorageDir
}

func getShardFiles(nodeAddr string, t *testing.T) []string {
	storageDir := getIndexStorageDirOnNode(nodeAddr, t)

	shardPath := storageDir + "/shards"
	return getFileList(shardPath)
}

func getFileList(path string) []string {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		if strings.Contains(err.Error(), "no such file or directory") {
			return nil
		}
		log.Fatal(err)
	}
	out := make([]string, 0)
	for _, file := range files {
		out = append(out, file.Name())
	}
	return out
}

// This function is called after rebalance to see if there are any
// stray files in SHARD_REBALANCE_DIR remaining. There should be no
// shard files remaining (Although there can be some directories with
// RebalanceToken & TransferToken names
func verifyStorageDirContents(t *testing.T) {
	files, err := ioutil.ReadDir(absRebalStorageDirPath)
	if err != nil {
		if strings.Contains(err.Error(), "no such file or directory") {
			return
		}
		log.Fatal(err)
	}
	r, _ := regexp.Compile("^plasma_storage($|/Rebalance($|/([0-9a-f]+)($|/TransferToken($|/TransferToken([0-9a-f]{1,2}:){7}([0-9a-f]{1,2})$))))")
	for _, file := range files {
		if !file.IsDir() {
			t.Fatalf("verifyStorageDirContents: Non directory file: %v found. All files are expected to be directories", file)
		}

		dirName := file.Name()
		rebalanceStoragePath := strings.Split(dirName, absRebalStorageDirPath)[0]
		if !r.MatchString(rebalanceStoragePath) {
			t.Fatalf("verifyStorageDirContents: Invalid directory path: %v seen", dirName)
		}
	}
}

// For each bucket, there should only be 2 shards - One for main index
// and one for back index. Having more than 2 shards is a bug (Until
// multiple shards per tenant are supported)
//
// Also, the shardId should be unique to a bucket. If two buckets share
// a shard, it is a bug
func isValidShardMapping(shardMapping map[string]map[string]bool) bool {
	shardToBucketMap := make(map[string]string)
	for bucket, shardMap := range shardMapping {
		if len(shardMap) != 2 {
			return false
		}
		for _, shardId := range shardToBucketMap {
			if _, ok := shardToBucketMap[shardId]; !ok {
				shardToBucketMap[shardId] = bucket
			} else {
				return false
			}
		}
	}
	return true
}

func validateShardIdMapping(node string, t *testing.T) {
	// All indexes are created now. Get the shardId's for each node
	shardIds := getShardIds(node, t)
	if isValidShardMapping(shardIds) == false {
		t.Fatalf("Invalid shard mapping: %v on node: %v", shardIds, node)
	}
}

func validateShardFiles(node string, t *testing.T) {
	// All indexes are created now. Get the shardId's for each node
	shardFiles := getShardFiles(node, t)
	if len(shardFiles) > 0 {
		t.Fatalf("Expected empty shard directory for node: %v, but it has files: %v", node, shardFiles)
	}
}

func validateIndexPlacement(nodes []string, t *testing.T) {
	// Validate placement after rebalance
	allIndexNodes, err := getIndexPlacement()
	if err != nil {
		t.Fatalf("Error while querying getIndexStatus endpoint, err: %v", err)
	}

	if len(allIndexNodes) != 2 {
		t.Fatalf("Expected indexes to be placed only on nodes: %v. Actual placement: %v",
			nodes, allIndexNodes)
	}
	for _, node := range nodes {
		if _, ok := allIndexNodes[node]; !ok {
			t.Fatalf("Expected indexes to be placed only on nodes: %v. Actual placement: %v",
				nodes, allIndexNodes)
		}
	}
}

// Indexer life cycle manager broadcasts stats every 5 seconds.
// For the purse of testing, this has been changed to 500ms - ONLY FOR SERVERLESS tests
// After the index is built, there exists a possibility
// that GSI/N1QL client has received stats from some indexer nodes but yet
// to receive from some other indexer nodes. In such a case, only the index
// for which stats have been received will be picked up for scan and the
// test fails with zero scan requests for other replicas.
//
// To avoid such a failure, sleep for 2500 milli seconds after the index
// is built so that the client has updated stats from all indexer nodes.
func waitForStatsUpdate() {
	time.Sleep(2500 * time.Millisecond)
}

func getIndexStatusFromIndexer() (*tc.IndexStatusResponse, error) {
	url, err := makeurl("/getIndexStatus?getAll=true")
	if err != nil {
		return nil, err
	}

	var resp *http.Response
	resp, err = http.Get(url)
	if resp != nil {
		defer resp.Body.Close()
	}

	if err != nil {
		return nil, err
	}

	var respbody []byte
	respbody, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var st tc.IndexStatusResponse
	err = json.Unmarshal(respbody, &st)
	if err != nil {
		return nil, err
	}

	return &st, nil
}

func getIndexPlacement() (map[string]bool, error) {
	status, err := getIndexStatusFromIndexer()
	if err != nil {
		return nil, err
	}

	allHosts := make(map[string]bool)
	for _, indexStatus := range status.Status {
		for _, host := range indexStatus.Hosts {
			allHosts[host] = true
		}

	}
	return allHosts, nil
}

func makeStorageDir(t *testing.T) {
	err := os.Mkdir(SHARD_REBALANCE_DIR, 0755)
	if err != nil {
		t.Fatalf("Error while creating rebalance dir: %v", err)
	}

	cwd, err := filepath.Abs(".")
	FailTestIfError(err, "Error while finding absolute path", t)
	log.Printf("TestShardRebalanceSetup: Using %v as storage dir for rebalance", absRebalStorageDirPath)

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.rebalance.blob_storage_bucket", cwd, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in change setting for indexer.settings.rebalance.blob_storage_bucket")

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.rebalance.blob_storage_prefix", SHARD_REBALANCE_DIR, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in change setting for indexer.settings.rebalance.blob_storage_bucket")

	absRebalStorageDirPath = cwd + "/" + SHARD_REBALANCE_DIR
}

func cleanupStorageDir(t *testing.T) {
	cwd, err := filepath.Abs(".")
	FailTestIfError(err, "Error while finding absolute path", t)

	absRebalStorageDirPath := cwd + "/" + SHARD_REBALANCE_DIR
	log.Printf("cleanupStorageDir: Cleaning up %v", absRebalStorageDirPath)

	err = os.RemoveAll(absRebalStorageDirPath)
	if err != nil {
		t.Fatalf("Error removing rebal storage dir: %v, err: %v", absRebalStorageDirPath, err)
	}
}

// Called only after bucket deletion to remove the shard files belonging to
// a bucket so that other rebalance tests can validate the presence of
// shard files on disk
func cleanupShardDir(t *testing.T) {
	for _, node := range clusterconfig.Nodes {
		storageDir := getIndexStorageDirOnNode(node, t)

		shardPath := storageDir + "/shards"
		err := os.RemoveAll(shardPath)
		if err != nil {
			t.Fatalf("Error removing shard dir dir: %v, err: %v", shardPath, err)
		}
	}

}

func getServerGroupForNode(node string) string {
	group_1 := "Group 1"
	group_2 := "Group 2"
	switch node {
	case clusterconfig.Nodes[0], clusterconfig.Nodes[2], clusterconfig.Nodes[4]:
		return group_1
	case clusterconfig.Nodes[1], clusterconfig.Nodes[3], clusterconfig.Nodes[5]:
		return group_2
	default:
		return ""
	}
}

func performSwapRebalance(addNodes []string, removeNodes []string, skipValidation, skipAdding, isRebalCancel bool, t *testing.T) {

	if !skipAdding {
		for _, node := range addNodes {
			serverGroup := getServerGroupForNode(node)
			if err := cluster.AddNodeWithServerGroup(kvaddress, clusterconfig.Username, clusterconfig.Password, node, "index", serverGroup); err != nil {
				FailTestIfError(err, fmt.Sprintf("Error while adding node %v cluster in server group: %v", node, serverGroup), t)
			}
		}
	}

	err := cluster.RemoveNodes(kvaddress, clusterconfig.Username, clusterconfig.Password, removeNodes)
	if skipValidation { // Some crash tests have their own validation. Hence, skip the validation here
		if err != nil && strings.Contains(err.Error(), "Rebalance failed") {
			return
		} else if !isRebalCancel && err == nil {
			t.Fatalf("Expected rebalance failure, but rebalance passed")
		} else {
			FailTestIfError(err, fmt.Sprintf("Observed error: %v during rebalabce", err), t)
		}
		return
	}

	if err != nil {
		FailTestIfError(err, fmt.Sprintf("Error while removing nodes: %v from cluster", removeNodes), t)
	}

	// This sleep will ensure that the stats are propagated to client
	// Also, any pending rebalance cleanup is expected to be done during
	// this time - so that validateShardFiles can see cleaned up directories
	waitForStatsUpdate()

	validateIndexPlacement(addNodes, t)
	// Validate the data files on nodes that have been rebalanced out
	for _, removedNode := range removeNodes {
		validateShardFiles(removedNode, t)
	}

	// All indexes are created now. Get the shardId's for each node
	for _, addedNode := range addNodes {
		validateShardIdMapping(addedNode, t)
	}

	verifyStorageDirContents(t)
}

// Due to shard locking, if there is a bug in shard
// locking management, then index DDL operations after a
// failed rebalance can be caught using this test
func testDDLAfterRebalance(indexNodes []string, t *testing.T) {
	// Drop indexes[0]
	index := indexes[0]
	collection := "c1"
	for _, bucket := range buckets {
		err := secondaryindex.DropSecondaryIndex2(index, bucket, scope, collection, indexManagementAddress)
		FailTestIfError(err, "Error while dropping index", t)
	}

	for _, bucket := range buckets {
		waitForReplicaDrop(index, bucket, scope, collection, 0, t) // wait for replica drop-0
		waitForReplicaDrop(index, bucket, scope, collection, 1, t) // wait for replica drop-1
	}

	// Recreate the index again
	for _, bucket := range buckets {
		n1qlStatement := fmt.Sprintf("create index %v on `%v`.`%v`.`%v`(age)", indexes[0], bucket, scope, collection)
		execN1qlAndWaitForStatus(n1qlStatement, bucket, scope, collection, indexes[0], "Ready", t)
	}

	waitForStatsUpdate()

	partns := indexPartnIds[0]
	for _, bucket := range buckets {
		// Scan only the newly created index
		scanIndexReplicas(index, bucket, scope, collection, []int{0, 1}, numScans, numDocs, len(partns), t)
	}

	validateIndexPlacement(indexNodes, t)
	for _, indexNode := range indexNodes {
		validateShardIdMapping(indexNode, t)
	}
}

func getHostsForBucket(bucket string) []string {
	hosts := make(map[string]bool)

	status, err := secondaryindex.GetIndexStatus(clusterconfig.Username, clusterconfig.Password, kvaddress)
	if status != nil && err == nil {
		indexes := status["indexes"].([]interface{})
		for _, indexEntry := range indexes {
			entry := indexEntry.(map[string]interface{})

			if bucket != entry["bucket"].(string) {
				continue
			}

			allHosts := entry["hosts"].([]interface{})
			for _, host := range allHosts {
				hosts[host.(string)] = true
			}
		}
	}
	var hostSlice []string
	for host, _ := range hosts {
		hostSlice = append(hostSlice, host)
	}
	return hostSlice
}

func getInstAndDefnId(bucket, scope, collection, name string) (c.IndexInstId, c.IndexDefnId, error) {
	url, err := makeurl("/getIndexStatus?getAll=true")
	if err != nil {
		return c.IndexInstId(0), c.IndexDefnId(0), err
	}

	var resp *http.Response
	resp, err = http.Get(url)
	if err != nil {
		return c.IndexInstId(0), c.IndexDefnId(0), err
	}

	var respbody []byte
	respbody, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return c.IndexInstId(0), c.IndexDefnId(0), err
	}

	var st tc.IndexStatusResponse
	err = json.Unmarshal(respbody, &st)
	if err != nil {
		return c.IndexInstId(0), c.IndexDefnId(0), err
	}

	for _, idx := range st.Status {
		if idx.Name == name && idx.Bucket == bucket &&
			idx.Scope == scope && idx.Collection == collection {
			return idx.InstId, idx.DefnId, nil
		}
	}

	return c.IndexInstId(0), c.IndexDefnId(0), errors.New("Index not found in getIndexStatus")
}
