package perftests

import (
	"encoding/json"
	"flag"
	"github.com/couchbase/cbauth"
	"github.com/couchbase/indexing/secondary/logging"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	"github.com/prataprc/goparsec"
	"github.com/prataprc/monster"
	"github.com/prataprc/monster/common"
	"io/ioutil"
	"log"
	"runtime"
	"testing"
	"time"
)

const n1qperf = "n1qlperf"
const cbindexperf = "cbindexperf"

var seed int
var defaultlimit int64 = 10000000
var kvaddress, indexManagementAddress, indexScanAddress string
var usen1qlperf bool
var numdocs int

// var minthroughput, maxlatency int64
// var maxbuildtime float64
var clusterconfig tc.ClusterConfiguration
var proddir, bagdir string
var defaultIndexActiveTimeout int64 = 900 // 15 mins to wait for index to become active

func init() {
	log.Printf("In init()")

	logging.SetLogLevel(logging.Warn)
	var configpath string
	var perftool string
	seed = 1
	flag.StringVar(&configpath, "cbconfig", "../config/clusterrun_conf.json", "Path of the configuration file with data about Couchbase Cluster")
	flag.StringVar(&perftool, "perftool", n1qperf, "Perf tool to use for scan tests")
	flag.IntVar(&numdocs, "numdocs", 5000000, "Number of documents to load in the bucket")
	// flag.Int64Var(&minthroughput, "minthroughput", 17000, "Minimum throughput (in rows/sec) expected by the scan test run")
	// flag.Int64Var(&maxlatency, "maxlatency", 12000000, "Maximum average latency (in nanoseconds) expected for the scan test")
	// flag.Float64Var(&maxbuildtime, "maxbuildtime", 300, "Maximum initial build time in seconds")
	flag.Parse()
	clusterconfig = tc.GetClusterConfFromFile(configpath)
	kvaddress = clusterconfig.KVAddress
	indexManagementAddress = clusterconfig.KVAddress
	indexScanAddress = clusterconfig.KVAddress
	proddir, bagdir = tc.FetchMonsterToolPath()

	tc.LogPerformanceStat = true

	if perftool == n1qperf {
		usen1qlperf = true
	} else {
		usen1qlperf = false
	}

	// setup cbauth
	if _, err := cbauth.InternalRetryDefaultInit(kvaddress, clusterconfig.Username, clusterconfig.Password); err != nil {
		log.Fatalf("Failed to initialize cbauth: %s", err)
	}

	//Enable QE Rest server
	err := secondaryindex.ChangeIndexerSettings("indexer.api.enableTestServer", true, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	if clusterconfig.IndexUsing != "" {
		// Set clusterconfig.IndexUsing only if it is specified in config file. Else let it default to gsi
		log.Printf("Using %v for creating indexes", clusterconfig.IndexUsing)
		secondaryindex.IndexUsing = clusterconfig.IndexUsing

		err := secondaryindex.ChangeIndexerSettings("indexer.settings.storage_mode", secondaryindex.IndexUsing, clusterconfig.Username, clusterconfig.Password, kvaddress)
		tc.HandleError(err, "Error in ChangeIndexerSettings")
	}

	time.Sleep(5 * time.Second)
}

func FailTestIfError(err error, msg string, t *testing.T) {
	if err != nil {
		t.Fatalf("%v: %v\n", msg, err)
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
	runtime.GOMAXPROCS(45)
	keyValues := make(tc.KeyValues)
	options.outfile = "./out.txt"
	options.bagdir = bagdir
	options.count = count

	var err error

	// read production-file
	text, err := ioutil.ReadFile(prodfile)
	if err != nil {
		log.Fatal(err)
	}
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
