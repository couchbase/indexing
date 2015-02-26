package largedatatests

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/couchbase/cbauth"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/prataprc/goparsec"
	"github.com/prataprc/monster"
	"github.com/prataprc/monster/common"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"testing"
)

var seed int
var defaultlimit int64 = 10000000
var kvaddress, indexManagementAddress, indexScanAddress string
var clusterconfig tc.ClusterConfiguration
var proddir, bagdir string

func init() {
	fmt.Println("In init()")
	var configpath string
	seed = 1
	flag.StringVar(&configpath, "cbconfig", "../config/clusterrun_conf.json", "Path of the configuration file with data about Couchbase Cluster")
	flag.Parse()
	clusterconfig = tc.GetClusterConfFromFile(configpath)
	kvaddress = clusterconfig.KVAddress
	indexManagementAddress = clusterconfig.KVAddress
	indexScanAddress = clusterconfig.KVAddress

	// setup cbauth
	if _, err := cbauth.InternalRetryDefaultInit(kvaddress, clusterconfig.Username, clusterconfig.Password); err != nil {
		log.Fatalf("Failed to initialize cbauth: %s", err)
	}

	proddir, bagdir = tc.FetchMonsterToolPath()
}

func FailTestIfError(err error, msg string, t *testing.T) {
	if err != nil {
		t.Errorf("%v: %v\n", msg, err)
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
	outfd := os.Stdout
	if options.outfile != "-" && options.outfile != "" {
		outfd, err = os.Create(options.outfile)
		if err != nil {
			log.Fatal(err)
		}
	}

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

	if options.nonterm != "" {
		for i := 0; i < options.count; i++ {
			val := evaluate("root", scope, nterms[options.nonterm])
			fmt.Println(val)
			outtext := fmt.Sprintf("%v\n", val)
			if _, err := outfd.Write([]byte(outtext)); err != nil {
				log.Fatal(err)
			}
		}

	} else {
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
			outtext := fmt.Sprintf("%v\n", val)
			if _, err := outfd.Write([]byte(outtext)); err != nil {
				log.Fatal(err)
			}
		}
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
