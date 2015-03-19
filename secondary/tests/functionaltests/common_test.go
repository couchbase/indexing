package functionaltests

import (
	"encoding/json"
	"flag"
	"github.com/couchbase/cbauth"
	// c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	// tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
	"github.com/prataprc/goparsec"
	"github.com/prataprc/monster"
	"github.com/prataprc/monster/common"
	"log"
	//"os"
	"io/ioutil"
	"os/user"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

var docs, mut_docs tc.KeyValues
var defaultlimit int64 = 10000000
var kvaddress, indexManagementAddress, indexScanAddress string
var clusterconfig tc.ClusterConfiguration
var dataFilePath, mutationFilePath string
var defaultIndexActiveTimeout int64 = 600 // 10 mins to wait for index to become active

func init() {
	log.Printf("In init()")
	logging.SetLogLevel(logging.Warn)

	var configpath string
	flag.StringVar(&configpath, "cbconfig", "../config/clusterrun_conf.json", "Path of the configuration file with data about Couchbase Cluster")
	flag.Parse()
	clusterconfig = tc.GetClusterConfFromFile(configpath)
	kvaddress = clusterconfig.KVAddress
	indexManagementAddress = clusterconfig.KVAddress
	indexScanAddress = clusterconfig.KVAddress
	seed = 1
	proddir, bagdir = tc.FetchMonsterToolPath()

	// setup cbauth
	if _, err := cbauth.InternalRetryDefaultInit(kvaddress, clusterconfig.Username, clusterconfig.Password); err != nil {
		log.Fatalf("Failed to initialize cbauth: %s", err)
	}
	secondaryindex.CheckCollation = true
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	tc.HandleError(e, "Error in DropAllSecondaryIndexes")

	time.Sleep(5 * time.Second)
	// Working with Users10k and Users_mut dataset.
	u, _ := user.Current()
	dataFilePath = filepath.Join(u.HomeDir, "testdata/Users10k.txt.gz")
	mutationFilePath = filepath.Join(u.HomeDir, "testdata/Users_mut.txt.gz")
	tc.DownloadDataFile(tc.IndexTypesStaticJSONDataS3, dataFilePath, true)
	tc.DownloadDataFile(tc.IndexTypesMutationJSONDataS3, mutationFilePath, true)
	docs = datautility.LoadJSONFromCompressedFile(dataFilePath, "docid")
	mut_docs = datautility.LoadJSONFromCompressedFile(mutationFilePath, "docid")
	log.Printf("Emptying the default bucket")
	kvutility.EnableBucketFlush("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.FlushBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	time.Sleep(5 * time.Second)

	log.Printf("Create Index On the empty default Bucket()")
	var indexName = "index_eyeColor"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"eyeColor"}, false, nil, true, defaultIndexActiveTimeout, nil)
	tc.HandleError(err, "Error in creating the index")

	// Populate the bucket now
	log.Printf("Populating the default bucket")
	kvutility.SetKeyValues(docs, "default", "", clusterconfig.KVAddress)
	/*docScanResults := datautility.ExpectedScanResponse_string(docs, "eyeColor", "b", "c", 3)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"b"}, []interface{}{"c"}, 3, true, defaultlimit, c.SessionConsistency, nil)
	tc.HandleError(err, "Error in scan")
	tv.Validate(docScanResults, scanResults)*/
}

func FailTestIfError(err error, msg string, t *testing.T) {
	if err != nil {
		t.Fatal("%v: %v\n", msg, err)
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
	root := compile(parsec.NewScanner(text))
	scope := root.(common.Scope)
	nterms := scope["_nonterminals"].(common.NTForms)
	scope = monster.BuildContext(scope, uint64(options.seed), options.bagdir)
	scope["_prodfile"] = prodfile

	// evaluate
	for i := 0; i < options.count; i++ {
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
	scope = scope.ApplyGlobalForms()
	return monster.EvalForms(name, scope, forms)
}
