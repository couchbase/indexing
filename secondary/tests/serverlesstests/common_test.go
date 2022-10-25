package serverlesstests

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log" //"os"
	"math/rand"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/cbauth"
	c "github.com/couchbase/indexing/secondary/common"
	json "github.com/couchbase/indexing/secondary/common/json"
	"github.com/couchbase/indexing/secondary/logging"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	parsec "github.com/prataprc/goparsec"
	"github.com/prataprc/monster"
	"github.com/prataprc/monster/common"
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
	err = secondaryindex.ChangeIndexerSettings("queryport.client.log_level", "Verbose", clusterconfig.Username, clusterconfig.Password, kvaddress)
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

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.rebalance.blob_storage_bucket", "/tmp", clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in change setting for indexer.settings.rebalance.blob_storage_bucket")

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.rebalance.blob_storage_prefix", "serverless_rebalance", clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in change setting for indexer.settings.rebalance.blob_storage_prefix")

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
	}

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

func verifyPathExists(Pth string) (bool, error) {
	_, errStat := os.Stat(Pth)
	if errStat == nil {
		return true, nil
	}

	if os.IsNotExist(errStat) {
		return false, nil
	}

	return false, errStat
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

func waitForIndexActive(bucket, index string, t *testing.T) {
	for {
		select {
		case <-time.After(time.Duration(3 * time.Minute)):
			t.Fatalf("Index did not become active after 3 minutes")
			break
		default:
			status, err := secondaryindex.GetIndexStatus(clusterconfig.Username, clusterconfig.Password, kvaddress)
			if status != nil && err == nil {
				indexes := status["indexes"].([]interface{})
				for _, indexEntry := range indexes {
					entry := indexEntry.(map[string]interface{})

					if index == entry["index"].(string) {
						if bucket == entry["bucket"].(string) {
							if "Ready" == entry["status"].(string) {
								log.Printf("Index status is: Ready for index: %v", index)
								return
							}
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

// scanIndexReplicas scan's the index and validates if all the replicas of the index are retruning
// valid results
func scanIndexReplicas(index, bucket, scope, collection string, replicaIds []int, numScans, numDocs, numPartitions int, t *testing.T) {
	// Scan the index num_scans times
	for i := 0; i < numScans; i++ {
		scanResults, err := secondaryindex.ScanAll2(index, bucket, scope, collection, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
		if len(scanResults) != numDocs {
			errStr := fmt.Sprintf("Error in ScanAll. Expected len(scanResults): %v, actual: %v", numDocs, len(scanResults))
			FailTestIfError(err, errStr, t)
		}
	}

	// Get parititioned stats for the index from all nodes
	stats := secondaryindex.GetPerPartnStats(clusterconfig.Username, clusterconfig.Password, kvaddress)

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
			num_requests[i][j] = stats[indexName+":num_requests"].(float64)
			num_scan_errors += stats[indexName+":num_scan_errors"].(float64)
			num_scan_timeouts += stats[indexName+":num_scan_timeouts"].(float64)
		}
	}

	logStats := func() {
		log.Printf("ScanAllReplicas: Indexer stats are: %v", stats)
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
				t.Fatalf("Zero scan requests seen for index: %v, partnId: %v", index+replicas[i], j)
			}
			total_scan_requests += num_requests[i][j]
		}

		if total_scan_requests != (float64)(numScans) {
			logStats()
			t.Fatalf("Total scan requests for all partitions does not match the total scans. Expected: %v, actual: %v, partitionID: %v", numScans, total_scan_requests, j)
		}
	}
}

// Indexer life cycle manager broadcasts stats every 5 seconds.
// After the index is built, there exists a possibility
// that GSI/N1QL client has received stats from some indexer nodes but yet
// to receive from some other indexer nodes. In such a case, only the index
// for which stats have been received will be picked up for scan and the
// test fails with zero scan requests for other replicas.
//
// To avoid such a failure, sleep for 10 seconds after the index is built
// so that the client has updated stats from all indexer nodes.
func waitForStatsUpdate() {
	time.Sleep(10100 * time.Millisecond)
}
