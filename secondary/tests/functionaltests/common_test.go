package functionaltests

import (
	"flag"
	"io/ioutil"
	"log" //"os"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/couchbase/cbauth"
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

var docs, mut_docs tc.KeyValues
var defaultlimit int64 = 100000000000
var kvaddress, indexManagementAddress, indexScanAddress string
var clusterconfig tc.ClusterConfiguration
var dataFilePath, mutationFilePath string
var defaultIndexActiveTimeout int64 = 600 // 10 mins to wait for index to become active
var skipsetup bool
var indexerLogLevel string

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

	// setup cbauth
	if _, err := cbauth.InternalRetryDefaultInit(kvaddress, clusterconfig.Username, clusterconfig.Password); err != nil {
		log.Fatalf("Failed to initialize cbauth: %s", err)
	}

	//Disable backfill
	err := secondaryindex.ChangeIndexerSettings("queryport.client.settings.backfillLimit", float64(0), clusterconfig.Username, clusterconfig.Password, kvaddress)
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

	time.Sleep(5 * time.Second)
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
		time.Sleep(5 * time.Second)

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
