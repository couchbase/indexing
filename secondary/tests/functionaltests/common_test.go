package functionaltests

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
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
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
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

var reqscans = map[string]interface{}{
	"scans":      `[{"Seek":null,"Filter":[{"Low":"D","High":"F","Inclusion":3},{"Low":"A","High":"C","Inclusion":3}]},{"Seek":null,"Filter":[{"Low":"S","High":"V","Inclusion":3},{"Low":"A","High":"C","Inclusion":3}]}]`,
	"projection": `{"EntryKeys":[0],"PrimaryKey":false}`,
	"distinct":   false,
	"limit":      1000000,
	"reverse":    false,
	"offset":     int64(0),
	"stale":      "ok",
}

var reqscanscount = map[string]interface{}{
	"scans":      `[{"Seek":null,"Filter":[{"Low":"D","High":"F","Inclusion":3},{"Low":"A","High":"C","Inclusion":3}]},{"Seek":null,"Filter":[{"Low":"S","High":"V","Inclusion":3},{"Low":"A","High":"C","Inclusion":3}]}]`,
	"projection": `{"EntryKeys":[0],"PrimaryKey":false}`,
	"distinct":   false,
	"limit":      1000000,
	"reverse":    false,
	"offset":     int64(0),
	"stale":      "ok",
}

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
		kvutility.EnableBucketFlush("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
		kvutility.FlushBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)

		log.Printf("Create Index On the empty default Bucket()")
		var indexName = "index_eyeColor"
		var bucketName = "default"

		err = secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"eyeColor"}, false, nil, true, defaultIndexActiveTimeout, nil)
		tc.HandleError(err, "Error in creating the index")

		// Populate the bucket now
		log.Printf("Populating the default bucket")
		kvutility.SetKeyValues(docs, "default", "", clusterconfig.KVAddress)
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

func GenerateDocsWithXATTRS(numDocs int, bucketName, bucketPassword, hostaddress, serverUsername, serverPassword string, xattrs map[string]string) tc.KeyValues {
	docsToCreate := generateDocs(numDocs, "users.prod")
	kvutility.SetValuesWithXattrs(docsToCreate, bucketName, bucketPassword, hostaddress, serverUsername, serverPassword, xattrs)
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
	time.Sleep(30 * time.Second)
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

func getscans(id string, body map[string]interface{}) ([]interface{}, error) {
	url, err := makeurl(fmt.Sprintf("/internal/index/%v?multiscan=true", id))
	if err != nil {
		return nil, err
	}
	data, _ := json.Marshal(body)
	req, err := http.NewRequest("GET", url, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	log.Printf("getscans status : %v\n", resp.Status)

	dec := json.NewDecoder(resp.Body)
	entries := make([]interface{}, 0)
	for {
		var result interface{}
		if err := dec.Decode(&result); err != nil && err != io.EOF {
			return nil, err
		} else if result == nil {
			break
		}
		switch resval := result.(type) {
		case []interface{}:
			// log.Printf("GOT CHUNK: %v\n", len(resval))
			entries = append(entries, resval...)
		default:
			err := fmt.Errorf("ERROR CHUNK: %v\n", result)
			return nil, err
		}
	}
	return entries, nil
}

func getscanscount(id string, body map[string]interface{}) (int, error) {
	url, err := makeurl(fmt.Sprintf("/internal/index/%v?multiscancount=true", id))
	if err != nil {
		return 0, err
	}
	data, _ := json.Marshal(body)
	req, err := http.NewRequest("GET", url, bytes.NewBuffer(data))
	if err != nil {
		return 0, err
	}
	resp, respbody, err := doHttpRequestReturnBody(req)
	if err != nil {
		return 0, err
	}
	log.Printf("Status : %v\n", resp.Status)

	var result interface{}

	if len(respbody) == 0 {
		return 0, nil
	}
	err = json.Unmarshal(respbody, &result)
	if err != nil {
		return 0, err
	}
	if count, ok := result.(float64); ok {
		return int(count), nil
	}
	return 0, nil
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

func getScanAll_AllFiltersNil() qc.Scans {
	scans := make(qc.Scans, 2)

	filter1 := make([]*qc.CompositeElementFilter, 2)
	filter1[0] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	filter1[1] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "S", High: "V", Inclusion: qc.Inclusion(uint32(2))}
	filter2[1] = &qc.CompositeElementFilter{Low: "H", High: "J", Inclusion: qc.Inclusion(uint32(1))}
	scans[1] = &qc.Scan{Filter: filter2}
	return scans
}

func getSingleSeek() qc.Scans {
	scans := make(qc.Scans, 1)
	eq := c.SecondaryKey([]interface{}{"UTARIAN", "Michelle Mckay"})
	scans[0] = &qc.Scan{Seek: eq}
	return scans
}

func getMultipleSeek() qc.Scans {
	scans := make(qc.Scans, 2)
	eq := c.SecondaryKey([]interface{}{"UTARIAN", "Michelle Mckay"})
	scans[0] = &qc.Scan{Seek: eq}
	eq = c.SecondaryKey([]interface{}{"JUMPSTACK", "Loretta Wilkerson"})
	scans[1] = &qc.Scan{Seek: eq}
	return scans
}

func getSimpleRange() qc.Scans {
	scans := make(qc.Scans, 1)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "G", High: "N", Inclusion: qc.Inclusion(uint32(1))}
	scans[0] = &qc.Scan{Filter: filter1}
	return scans
}

func getNonOverlappingRanges() qc.Scans {
	scans := make(qc.Scans, 3)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "G", High: "K", Inclusion: qc.Inclusion(uint32(1))}
	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 1)
	filter2[0] = &qc.CompositeElementFilter{Low: "M", High: "R", Inclusion: qc.Inclusion(uint32(2))}
	scans[1] = &qc.Scan{Filter: filter2}

	filter3 := make([]*qc.CompositeElementFilter, 1)
	filter3[0] = &qc.CompositeElementFilter{Low: "T", High: "X", Inclusion: qc.Inclusion(uint32(0))}
	scans[2] = &qc.Scan{Filter: filter3}

	return scans
}

func getOverlappingRanges() qc.Scans {
	scans := make(qc.Scans, 3)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "G", High: "K", Inclusion: qc.Inclusion(uint32(1))}
	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 1)
	filter2[0] = &qc.CompositeElementFilter{Low: "I", High: "Q", Inclusion: qc.Inclusion(uint32(2))}
	scans[1] = &qc.Scan{Filter: filter2}

	filter3 := make([]*qc.CompositeElementFilter, 1)
	filter3[0] = &qc.CompositeElementFilter{Low: "M", High: "X", Inclusion: qc.Inclusion(uint32(0))}
	scans[2] = &qc.Scan{Filter: filter3}

	return scans
}

func getNonOverlappingFilters() qc.Scans {
	scans := make(qc.Scans, 2)
	filter1 := make([]*qc.CompositeElementFilter, 2)
	filter1[0] = &qc.CompositeElementFilter{Low: "D", High: "F", Inclusion: qc.Inclusion(uint32(0))}
	filter1[1] = &qc.CompositeElementFilter{Low: "A", High: "C", Inclusion: qc.Inclusion(uint32(1))}

	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "S", High: "V", Inclusion: qc.Inclusion(uint32(2))}
	filter2[1] = &qc.CompositeElementFilter{Low: "A", High: "C", Inclusion: qc.Inclusion(uint32(3))}
	scans[1] = &qc.Scan{Filter: filter2}
	return scans
}

// Noverlapping but first key is the same
func getNonOverlappingFilters2() qc.Scans {
	scans := make(qc.Scans, 2)
	filter1 := make([]*qc.CompositeElementFilter, 2)
	filter1[0] = &qc.CompositeElementFilter{Low: "DIGIAL", High: "DIGIAL", Inclusion: qc.Inclusion(uint32(3))}
	filter1[1] = &qc.CompositeElementFilter{Low: "A", High: "D", Inclusion: qc.Inclusion(uint32(3))}

	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "DIGIAL", High: "DIGIAL", Inclusion: qc.Inclusion(uint32(3))}
	filter2[1] = &qc.CompositeElementFilter{Low: "M", High: "Z", Inclusion: qc.Inclusion(uint32(3))}
	scans[1] = &qc.Scan{Filter: filter2}
	return scans
}

func getOverlappingFilters() qc.Scans {
	scans := make(qc.Scans, 3)
	filter1 := make([]*qc.CompositeElementFilter, 2)
	filter1[0] = &qc.CompositeElementFilter{Low: "B", High: "H", Inclusion: qc.Inclusion(uint32(3))}
	filter1[1] = &qc.CompositeElementFilter{Low: "T", High: "X", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "E", High: "M", Inclusion: qc.Inclusion(uint32(3))}
	filter2[1] = &qc.CompositeElementFilter{Low: "C", High: "R", Inclusion: qc.Inclusion(uint32(3))}
	scans[1] = &qc.Scan{Filter: filter2}

	filter3 := make([]*qc.CompositeElementFilter, 2)
	filter3[0] = &qc.CompositeElementFilter{Low: "S", High: "X", Inclusion: qc.Inclusion(uint32(3))}
	filter3[1] = &qc.CompositeElementFilter{Low: "A", High: "D", Inclusion: qc.Inclusion(uint32(3))}
	scans[2] = &qc.Scan{Filter: filter3}

	return scans
}

func getBoundaryFilters() qc.Scans {
	scans := make(qc.Scans, 2)
	filter1 := make([]*qc.CompositeElementFilter, 2)
	filter1[0] = &qc.CompositeElementFilter{Low: "GEEKWAGON", High: "INJOY", Inclusion: qc.Inclusion(uint32(3))}
	filter1[1] = &qc.CompositeElementFilter{Low: "Hendrix Orr", High: "Trina Mcfadden", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "INJOY", High: "ORBIN", Inclusion: qc.Inclusion(uint32(3))}
	filter2[1] = &qc.CompositeElementFilter{Low: "Trina Mcfadden", High: "ZZZZZ", Inclusion: qc.Inclusion(uint32(3))}
	scans[1] = &qc.Scan{Filter: filter2}

	return scans
}

func getSeekAndFilters_NonOverlapping() qc.Scans {
	scans := make(qc.Scans, 2)

	eq := c.SecondaryKey([]interface{}{"UTARIAN", "Michelle Mckay"})
	scans[0] = &qc.Scan{Seek: eq}

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "F", High: "K", Inclusion: qc.Inclusion(uint32(2))}
	filter2[1] = &qc.CompositeElementFilter{Low: "H", High: "L", Inclusion: qc.Inclusion(uint32(1))}
	scans[1] = &qc.Scan{Filter: filter2}

	return scans
}

func getSeekAndFilters_Overlapping() qc.Scans {
	scans := make(qc.Scans, 2)

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "F", High: "K", Inclusion: qc.Inclusion(uint32(2))}
	filter2[1] = &qc.CompositeElementFilter{Low: "H", High: "L", Inclusion: qc.Inclusion(uint32(1))}
	scans[0] = &qc.Scan{Filter: filter2}

	eq := c.SecondaryKey([]interface{}{"IMAGINART", "Janell Hyde"})
	scans[1] = &qc.Scan{Seek: eq}

	return scans
}

func getSimpleRangeLowUnbounded() qc.Scans {
	scans := make(qc.Scans, 1)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: "N", Inclusion: qc.Inclusion(uint32(1))}
	scans[0] = &qc.Scan{Filter: filter1}
	return scans
}

func getSimpleRangeHighUnbounded() qc.Scans {
	scans := make(qc.Scans, 1)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "P", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(1))}
	scans[0] = &qc.Scan{Filter: filter1}
	return scans
}

func getSimpleRangeMultipleUnbounded() qc.Scans {
	scans := make(qc.Scans, 2)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: "N", Inclusion: qc.Inclusion(uint32(1))}
	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 1)
	filter2[0] = &qc.CompositeElementFilter{Low: "D", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(1))}
	scans[1] = &qc.Scan{Filter: filter2}
	return scans
}

func getFiltersWithUnbounded() qc.Scans {
	scans := make(qc.Scans, 2)
	filter1 := make([]*qc.CompositeElementFilter, 2)
	filter1[0] = &qc.CompositeElementFilter{Low: "E", High: "L", Inclusion: qc.Inclusion(uint32(0))}
	filter1[1] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(1))}

	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "P", High: "T", Inclusion: qc.Inclusion(uint32(2))}
	filter2[1] = &qc.CompositeElementFilter{Low: "Q", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	scans[1] = &qc.Scan{Filter: filter2}
	return scans

	return scans
}

func getFiltersLowGreaterThanHigh() qc.Scans {
	scans := make(qc.Scans, 2)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "L", High: "E", Inclusion: qc.Inclusion(uint32(0))}
	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "P", High: "T", Inclusion: qc.Inclusion(uint32(2))}
	filter2[1] = &qc.CompositeElementFilter{Low: "Q", High: "Z", Inclusion: qc.Inclusion(uint32(3))}
	scans[1] = &qc.Scan{Filter: filter2}

	return scans
}

func getScanAllNoFilter() qc.Scans {
	scans := make(qc.Scans, 1)
	scans[0] = &qc.Scan{Filter: nil}
	return scans
}

func getScanAllFilterNil() qc.Scans {
	scans := make(qc.Scans, 2)
	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "S", High: "V", Inclusion: qc.Inclusion(uint32(0))}
	filter2[1] = &qc.CompositeElementFilter{Low: "H", High: "J", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter2}
	scans[1] = &qc.Scan{Filter: nil}
	return scans
}

func getSingleIndexSimpleRange() qc.Scans {
	scans := make(qc.Scans, 1)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "G", High: "N", Inclusion: qc.Inclusion(uint32(2))}
	scans[0] = &qc.Scan{Filter: filter1}
	return scans
}

func getSingleIndex_SimpleRanges_NonOverlapping() qc.Scans {
	scans := make(qc.Scans, 3)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "B", High: "GZZZZZ", Inclusion: qc.Inclusion(uint32(0))}
	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 1)
	filter2[0] = &qc.CompositeElementFilter{Low: "J", High: "OZZZZZ", Inclusion: qc.Inclusion(uint32(3))}
	scans[1] = &qc.Scan{Filter: filter2}

	filter3 := make([]*qc.CompositeElementFilter, 1)
	filter3[0] = &qc.CompositeElementFilter{Low: "R", High: "XZZZZZ", Inclusion: qc.Inclusion(uint32(1))}
	scans[2] = &qc.Scan{Filter: filter3}
	return scans
}

func getSingleIndex_SimpleRanges_Overlapping() qc.Scans {
	scans := make(qc.Scans, 4)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "B", High: "OZZZZZ", Inclusion: qc.Inclusion(uint32(0))}
	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 1)
	filter2[0] = &qc.CompositeElementFilter{Low: "E", High: "GZZZZZ", Inclusion: qc.Inclusion(uint32(3))}
	scans[1] = &qc.Scan{Filter: filter2}

	filter3 := make([]*qc.CompositeElementFilter, 1)
	filter3[0] = &qc.CompositeElementFilter{Low: "J", High: "RZZZZZ", Inclusion: qc.Inclusion(uint32(1))}
	scans[2] = &qc.Scan{Filter: filter3}

	filter4 := make([]*qc.CompositeElementFilter, 1)
	filter4[0] = &qc.CompositeElementFilter{Low: "S", High: "XZZZZZ", Inclusion: qc.Inclusion(uint32(1))}
	scans[3] = &qc.Scan{Filter: filter4}
	return scans
}

func get3FieldsSingleSeek() qc.Scans {
	scans := make(qc.Scans, 1)
	eq := c.SecondaryKey([]interface{}{"SOLAREN", "Michele Yang", int64(25)})
	scans[0] = &qc.Scan{Seek: eq}
	return scans
}

func get3FieldsMultipleSeeks() qc.Scans {
	scans := make(qc.Scans, 3)
	eq := c.SecondaryKey([]interface{}{"RODEOLOGY", "Tasha Dodson", int64(23)})
	scans[0] = &qc.Scan{Seek: eq}
	eq = c.SecondaryKey([]interface{}{"NETROPIC", "Lillian Mcneil", int64(24)})
	scans[1] = &qc.Scan{Seek: eq}
	eq = c.SecondaryKey([]interface{}{"ZYTREX", "Olga Patton", int64(29)})
	scans[2] = &qc.Scan{Seek: eq}
	return scans
}

func get3FieldsMultipleSeeks_Identical() qc.Scans {
	scans := make(qc.Scans, 3)
	eq := c.SecondaryKey([]interface{}{"RODEOLOGY", "Tasha Dodson", int64(23)})
	scans[0] = &qc.Scan{Seek: eq}
	eq = c.SecondaryKey([]interface{}{"NETROPIC", "Lillian Mcneil", int64(24)})
	scans[1] = &qc.Scan{Seek: eq}
	eq = c.SecondaryKey([]interface{}{"RODEOLOGY", "Tasha Dodson", int64(23)})
	scans[2] = &qc.Scan{Seek: eq}
	return scans
}

func getPrimaryFilter(low, high interface{}, incl int) []*qc.CompositeElementFilter {
	filter := make([]*qc.CompositeElementFilter, 1)
	filter[0] = &qc.CompositeElementFilter{Low: low, High: high, Inclusion: qc.Inclusion(uint32(incl))}
	return filter
}

func getPrimaryRange() qc.Scans {
	scans := make(qc.Scans, 1)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "A", High: "zzzzz", Inclusion: qc.Inclusion(uint32(1))}
	scans[0] = &qc.Scan{Filter: filter1}
	return scans
}

// Trailing high key unbounded
func getCompIndexHighUnbounded1() []qc.Scans {
	manyscans := make([]qc.Scans, 0, 2)

	scans1 := make(qc.Scans, 1)
	filter1 := make([]*qc.CompositeElementFilter, 2)
	filter1[0] = &qc.CompositeElementFilter{Low: "KANGLE", High: "KANGLE", Inclusion: qc.Inclusion(uint32(3))}
	filter1[1] = &qc.CompositeElementFilter{Low: "Daniel Beach", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(0))}
	scans1[0] = &qc.Scan{Filter: filter1}
	manyscans = append(manyscans, scans1)

	scans2 := make(qc.Scans, 1)
	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "KANGLE", High: "KANGLE", Inclusion: qc.Inclusion(uint32(0))}
	filter2[1] = &qc.CompositeElementFilter{Low: "Daniel Beach", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(0))}
	scans2[0] = &qc.Scan{Filter: filter2}
	manyscans = append(manyscans, scans2)

	scans3 := make(qc.Scans, 1)
	filter3 := make([]*qc.CompositeElementFilter, 2)
	filter3[0] = &qc.CompositeElementFilter{Low: "KANGLE", High: "KANGLE", Inclusion: qc.Inclusion(uint32(3))}
	filter3[1] = &qc.CompositeElementFilter{Low: "C", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	scans3[0] = &qc.Scan{Filter: filter3}
	manyscans = append(manyscans, scans3)
	return manyscans
}

// Leading high key unbounded
func getCompIndexHighUnbounded2() []qc.Scans {
	manyscans := make([]qc.Scans, 0, 2)

	scans1 := make(qc.Scans, 1)
	filter1 := make([]*qc.CompositeElementFilter, 2)
	filter1[0] = &qc.CompositeElementFilter{Low: "KANGLE", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	filter1[1] = &qc.CompositeElementFilter{Low: "Daniel Beach", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(0))}
	scans1[0] = &qc.Scan{Filter: filter1}
	manyscans = append(manyscans, scans1)

	scans2 := make(qc.Scans, 1)
	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "KANGLE", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(0))}
	filter2[1] = &qc.CompositeElementFilter{Low: "Daniel Beach", High: "P", Inclusion: qc.Inclusion(uint32(0))}
	scans2[0] = &qc.Scan{Filter: filter2}
	manyscans = append(manyscans, scans2)

	scans3 := make(qc.Scans, 1)
	filter3 := make([]*qc.CompositeElementFilter, 2)
	filter3[0] = &qc.CompositeElementFilter{Low: "KANGLE", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	filter3[1] = &qc.CompositeElementFilter{Low: "C", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	scans3[0] = &qc.Scan{Filter: filter3}
	manyscans = append(manyscans, scans3)
	return manyscans
}

// Trailing low key unbounded, Pratap's scenario
func getCompIndexHighUnbounded3() []qc.Scans {
	manyscans := make([]qc.Scans, 0, 1)

	scans1 := make(qc.Scans, 2)
	filter1 := make([]*qc.CompositeElementFilter, 2)
	filter1[0] = &qc.CompositeElementFilter{Low: "KANGLE", High: "MEDIFAX", Inclusion: qc.Inclusion(uint32(3))}
	filter1[1] = &qc.CompositeElementFilter{Low: "Snow Beach", High: "Travis Wilson", Inclusion: qc.Inclusion(uint32(0))}
	scans1[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "KANGLE", High: "OTHERWAY", Inclusion: qc.Inclusion(uint32(3))}
	filter2[1] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: "Robert Horne", Inclusion: qc.Inclusion(uint32(0))}
	scans1[1] = &qc.Scan{Filter: filter2}

	manyscans = append(manyscans, scans1)
	return manyscans
}

func getCompIndexHighUnbounded4() []qc.Scans {
	manyscans := make([]qc.Scans, 0, 1)

	scans1 := make(qc.Scans, 2)
	filter1 := make([]*qc.CompositeElementFilter, 2)
	filter1[0] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: "P", Inclusion: qc.Inclusion(uint32(3))}
	filter1[1] = &qc.CompositeElementFilter{Low: "A", High: "M", Inclusion: qc.Inclusion(uint32(0))}
	scans1[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: "R", Inclusion: qc.Inclusion(uint32(3))}
	filter2[1] = &qc.CompositeElementFilter{Low: "Q", High: "Z", Inclusion: qc.Inclusion(uint32(0))}
	scans1[1] = &qc.Scan{Filter: filter2}

	manyscans = append(manyscans, scans1)
	return manyscans
}

func getCompIndexHighUnbounded5() []qc.Scans {
	manyscans := make([]qc.Scans, 0, 1)

	scans1 := make(qc.Scans, 2)
	filter1 := make([]*qc.CompositeElementFilter, 2)
	filter1[0] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	filter1[1] = &qc.CompositeElementFilter{Low: "A", High: "M", Inclusion: qc.Inclusion(uint32(0))}
	scans1[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	filter2[1] = &qc.CompositeElementFilter{Low: "Q", High: "Z", Inclusion: qc.Inclusion(uint32(0))}
	scans1[1] = &qc.Scan{Filter: filter2}

	manyscans = append(manyscans, scans1)
	return manyscans
}

func getSeekBoundaries() []qc.Scans {
	manyscans := make([]qc.Scans, 0, 1)

	//1: Scan encompassing the seek
	scans1 := make(qc.Scans, 2)
	eq := c.SecondaryKey([]interface{}{"IMAGINART", "Janell Hyde"})
	scans1[0] = &qc.Scan{Seek: eq}

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "F", High: "K", Inclusion: qc.Inclusion(uint32(2))}
	filter2[1] = &qc.CompositeElementFilter{Low: "S", High: "V", Inclusion: qc.Inclusion(uint32(0))}
	scans1[1] = &qc.Scan{Filter: filter2}
	manyscans = append(manyscans, scans1)

	//2: Two seeks at same point
	scans2 := make(qc.Scans, 2)
	scans2[0] = &qc.Scan{Seek: c.SecondaryKey([]interface{}{"FORTEAN", "Marshall Chavez"})}
	scans2[1] = &qc.Scan{Seek: c.SecondaryKey([]interface{}{"FORTEAN", "Marshall Chavez"})}
	manyscans = append(manyscans, scans2)

	//3: Scan1 high is same as Seek
	scans3 := make(qc.Scans, 2)
	filter3 := make([]*qc.CompositeElementFilter, 2)
	filter3[0] = &qc.CompositeElementFilter{Low: "C", High: "FORTEAN", Inclusion: qc.Inclusion(uint32(3))}
	filter3[1] = &qc.CompositeElementFilter{Low: "H", High: "Marshall Chavez", Inclusion: qc.Inclusion(uint32(3))}
	scans3[0] = &qc.Scan{Filter: filter3}

	eq = c.SecondaryKey([]interface{}{"FORTEAN", "Marshall Chavez"})
	scans3[1] = &qc.Scan{Seek: eq}
	manyscans = append(manyscans, scans3)

	//3.1: Scan1 ends in Seek and Scan2 begins at the Seek
	scans4 := make(qc.Scans, 3)
	filter4 := make([]*qc.CompositeElementFilter, 2)
	filter4[0] = &qc.CompositeElementFilter{Low: "C", High: "FORTEAN", Inclusion: qc.Inclusion(uint32(3))}
	filter4[1] = &qc.CompositeElementFilter{Low: "H", High: "Marshall Chavez", Inclusion: qc.Inclusion(uint32(3))}
	scans4[0] = &qc.Scan{Filter: filter4}

	eq = c.SecondaryKey([]interface{}{"FORTEAN", "Marshall Chavez"})
	scans4[1] = &qc.Scan{Seek: eq}

	filter5 := make([]*qc.CompositeElementFilter, 2)
	filter5[0] = &qc.CompositeElementFilter{Low: "FORTEAN", High: "O", Inclusion: qc.Inclusion(uint32(3))}
	filter5[1] = &qc.CompositeElementFilter{Low: "Marshall Chavez", High: "P", Inclusion: qc.Inclusion(uint32(3))}
	scans4[2] = &qc.Scan{Filter: filter5}

	manyscans = append(manyscans, scans4)

	//4: Scan1 bordered by Seek1 and Seek2 on either sides
	scans5 := make(qc.Scans, 3)
	scans5[0] = &qc.Scan{Seek: c.SecondaryKey([]interface{}{"FORTEAN", "Marshall Chavez"})}

	filter6 := make([]*qc.CompositeElementFilter, 2)
	filter6[0] = &qc.CompositeElementFilter{Low: "FORTEAN", High: "IDEALIS", Inclusion: qc.Inclusion(uint32(3))}
	filter6[1] = &qc.CompositeElementFilter{Low: "Marshall Chavez", High: "Watts Calderon", Inclusion: qc.Inclusion(uint32(3))}
	scans5[1] = &qc.Scan{Filter: filter6}

	scans5[2] = &qc.Scan{Seek: c.SecondaryKey([]interface{}{"IDEALIS", "Watts Calderon"})}

	manyscans = append(manyscans, scans5)

	//5: S1 - single CEF  S2 (single CEf) overlaps with S3 (Multiple CEFs)
	scans6 := make(qc.Scans, 3)
	filter7 := make([]*qc.CompositeElementFilter, 1)
	filter7[0] = &qc.CompositeElementFilter{Low: "E", High: "J", Inclusion: qc.Inclusion(uint32(1))}
	scans6[0] = &qc.Scan{Filter: filter7}

	filter8 := make([]*qc.CompositeElementFilter, 1)
	filter8[0] = &qc.CompositeElementFilter{Low: "M", High: "S", Inclusion: qc.Inclusion(uint32(2))}
	scans6[1] = &qc.Scan{Filter: filter8}

	filter9 := make([]*qc.CompositeElementFilter, 2)
	filter9[0] = &qc.CompositeElementFilter{Low: "P", High: "X", Inclusion: qc.Inclusion(uint32(0))}
	filter9[1] = &qc.CompositeElementFilter{Low: "N", High: "Z", Inclusion: qc.Inclusion(uint32(3))}
	scans6[2] = &qc.Scan{Filter: filter9}

	manyscans = append(manyscans, scans6)

	//6
	scans7 := make(qc.Scans, 2)
	filter10 := make([]*qc.CompositeElementFilter, 2)
	filter10[0] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: "L", Inclusion: qc.Inclusion(uint32(1))}
	filter10[1] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(1))}
	scans7[0] = &qc.Scan{Filter: filter10}

	filter11 := make([]*qc.CompositeElementFilter, 2)
	filter11[0] = &qc.CompositeElementFilter{Low: "A", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(2))}
	filter11[1] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: "C", Inclusion: qc.Inclusion(uint32(2))}
	scans7[1] = &qc.Scan{Filter: filter11}
	manyscans = append(manyscans, scans7)

	//7
	scans8 := make(qc.Scans, 2)
	scans8[0] = &qc.Scan{Seek: c.SecondaryKey([]interface{}{"GEEKETRON", "Reese Fletcher"})}

	filter12 := make([]*qc.CompositeElementFilter, 2)
	filter12[0] = &qc.CompositeElementFilter{Low: "GEEKETRON", High: "GEEKETRON", Inclusion: qc.Inclusion(uint32(3))}
	filter12[1] = &qc.CompositeElementFilter{Low: "Hahn Fletcher", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	scans8[1] = &qc.Scan{Filter: filter12}
	manyscans = append(manyscans, scans8)

	return manyscans
}

func runMultiScanWithIndex(indexName string, fields []string, scans qc.Scans,
	reverse, distinct bool, projection *qc.IndexProjection, offset, limit int64,
	isScanAll bool, validateOnlyCount bool, scenario string, t *testing.T) {
	var bucketName = "default"
	log.Printf("\n--- %v ---", scenario)

	docScanResults := datautility.ExpectedMultiScanResponse(docs, fields, scans, reverse, distinct, projection, offset, limit, isScanAll)
	scanResults, err := secondaryindex.Scans(indexName, bucketName, indexScanAddress, scans, reverse, distinct, projection, offset, limit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)

	validateMultiScanResults(docScanResults, scanResults, validateOnlyCount, projection, t)
}

func runMultiScanCountWithIndex(indexName string, fields []string, scans qc.Scans,
	reverse, distinct bool, projection *qc.IndexProjection, offset, limit int64,
	isScanAll bool, validateOnlyCount bool, scenario string, t *testing.T) {
	var bucketName = "default"
	log.Printf("\n--- %v ---", scenario)

	expectedCount := datautility.ExpectedMultiScanCount(docs, fields, scans, reverse, distinct, isScanAll)

	multiScanCount, err := secondaryindex.MultiScanCount(indexName, bucketName, indexScanAddress, scans, distinct, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan count", t)

	log.Printf("MultiScanCount = %v ExpectedMultiScanCount = %v", multiScanCount, expectedCount)
	if expectedCount != int(multiScanCount) {
		msg := fmt.Sprintf("Actual and expected MultiScanCount do not match")
		FailTestIfError(errors.New(msg), "Error in multiscancount result validation", t)
	}
}

func runManyMultiScanWithIndex(indexName string, fields []string, manyscans []qc.Scans,
	reverse, distinct bool, projection *qc.IndexProjection, offset, limit int64,
	isScanAll bool, validateOnlyCount bool, scenario string, t *testing.T) {
	var bucketName = "default"
	log.Printf("\n--- %v ---", scenario)
	for i, scans := range manyscans {
		log.Printf("\n--- Multi Scan %v ---", i)
		docScanResults := datautility.ExpectedMultiScanResponse(docs, fields, scans, reverse, distinct, projection, offset, limit, isScanAll)
		scanResults, err := secondaryindex.Scans(indexName, bucketName, indexScanAddress, scans, reverse, distinct, projection, offset, limit, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)

		validateMultiScanResults(docScanResults, scanResults, validateOnlyCount, projection, t)
	}
}

func runManyMultiScanCountWithIndex(indexName string, fields []string, manyscans []qc.Scans,
	reverse, distinct bool, projection *qc.IndexProjection, offset, limit int64,
	isScanAll bool, validateOnlyCount bool, scenario string, t *testing.T) {
	var bucketName = "default"
	log.Printf("\n--- %v ---", scenario)
	for i, scans := range manyscans {
		log.Printf("\n--- Multi Scan %v ---", i)
		docScanResults := datautility.ExpectedMultiScanResponse(docs, fields, scans, reverse, distinct, projection, offset, limit, isScanAll)
		scanResults, err := secondaryindex.Scans(indexName, bucketName, indexScanAddress, scans, reverse, distinct, projection, offset, limit, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		multiScanCount, err := secondaryindex.MultiScanCount(indexName, bucketName, indexScanAddress, scans, distinct, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan count", t)

		log.Printf("len(scanResults) = %v MultiScanCount = %v", len(scanResults), multiScanCount)
		if len(scanResults) != int(multiScanCount) {
			msg := fmt.Sprintf("MultiScanCount not same as results from MultiScan")
			FailTestIfError(errors.New(msg), "Error in scan result validation", t)
		}

		validateMultiScanResults(docScanResults, scanResults, validateOnlyCount, projection, t)
	}
}

func runMultiScanForPrimaryIndex(indexName string, scans qc.Scans,
	reverse, distinct bool, projection *qc.IndexProjection, offset, limit int64,
	isScanAll bool, validateOnlyCount bool, scenario string, t *testing.T) {
	var bucketName = "default"
	log.Printf("\n--- %v ---", scenario)

	docScanResults := datautility.ExpectedMultiScanResponse_Primary(docs, scans, reverse, distinct, offset, limit)
	scanResults, err := secondaryindex.Scans(indexName, bucketName, indexScanAddress, scans, reverse, distinct, projection, offset, limit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	if validateOnlyCount {
		if len(scanResults) != len(docScanResults) {
			msg := fmt.Sprintf("Length of expected results %v is not equal to length of scan results %v", len(docScanResults), len(scanResults))
			FailTestIfError(errors.New(msg), "Error in scan result validation", t)
		}
	} else {
		err = tv.ValidateActual(docScanResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
	}
}

func validateMultiScanResults(docScanResults tc.ScanResponse,
	scanResults tc.ScanResponseActual, validateOnlyCount bool,
	projection *qc.IndexProjection, t *testing.T) {

	if validateOnlyCount {
		if len(scanResults) != len(docScanResults) {
			msg := fmt.Sprintf("Length of expected results %v is not equal to length of scan results %v", len(docScanResults), len(scanResults))
			FailTestIfError(errors.New(msg), "Error in scan result validation", t)
		}
	} else {
		var err error
		if projection != nil && len(projection.EntryKeys) == 0 {
			err = tv.ValidateEmptyProjection(docScanResults, scanResults)
			FailTestIfError(err, "Error in scan result validation", t)
		} else {
			err = tv.Validate(docScanResults, scanResults)
			FailTestIfError(err, "Error in scan result validation", t)
		}
	}
}

// scanIndexReplicas scan's the index and validates if all the replicas of the index are retruning
// valid results
func scanIndexReplicas(index, bucket string, replicaIds []int, numScans, numDocs, numPartitions int, t *testing.T) {
	// Scan the index num_scans times
	for i := 0; i < numScans; i++ {
		scanResults, err := secondaryindex.ScanAll(index, bucket, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
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
			indexName := fmt.Sprintf("%s:%s %v%s", bucket, index, j, replicas[i])
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

func executeGroupAggrTest(ga *qc.GroupAggr, proj *qc.IndexProjection,
	n1qlEquivalent, index string, t *testing.T) {
	var bucket = "default"
	_, scanResults, err := secondaryindex.Scan3(index, bucket, indexScanAddress, getScanAllNoFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.ValidateGroupAggrWithN1QL(kvaddress, clusterconfig.Username,
		clusterconfig.Password, bucket, n1qlEquivalent, ga, proj, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// with scans provided
func executeGroupAggrTest2(scans qc.Scans, ga *qc.GroupAggr, proj *qc.IndexProjection,
	n1qlEquivalent, index string, t *testing.T) {
	var bucket = "default"
	_, scanResults, err := secondaryindex.Scan3(index, bucket, indexScanAddress, scans, false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	if len(scanResults) == 1 {
		tc.PrintGroupAggrResultsActual(scanResults, "scanResults")
	}
	err = tv.ValidateGroupAggrWithN1QL(kvaddress, clusterconfig.Username,
		clusterconfig.Password, bucket, n1qlEquivalent, ga, proj, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}
