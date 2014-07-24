package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/couchbaselabs/go-couchbase"
	"github.com/prataprc/monster"
)

var options struct {
	buckets  string
	addr     string
	seed     int
	count    int
	parallel int
}

var testDir string
var bagDir string
var bucketProds = map[string]string{
	"default":  "",
	"users":    "",
	"projects": "",
}
var done = make(chan bool, 16)

func argParse() []string {
	seed := time.Now().UTC().Second()
	buckets := "default"
	flag.StringVar(&options.buckets, "buckets", buckets, "buckets to populate")
	flag.IntVar(&options.seed, "seed", seed, "seed value")
	flag.IntVar(&options.count, "count", 0, "count of documents per bucket")
	flag.IntVar(&options.parallel, "par", 1, "count of documents per bucket")
	flag.Parse()

	// collect production files.
	_, filename, _, _ := runtime.Caller(1)
	testDir = path.Join(path.Dir(path.Dir(path.Dir(filename))), "testdata")
	bagDir = testDir
	bucketProds["default"] = path.Join(testDir, "users.prod")
	bucketProds["users"] = path.Join(testDir, "users.prod")
	bucketProds["projects"] = path.Join(testDir, "projects.prod")

	return flag.Args()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] <addr> \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	args := argParse()
	n := 0
	if len(args) < 1 {
		usage()
		os.Exit(1)
	}

	for _, bucket := range strings.Split(options.buckets, ",") {
		n += loadBucket(args[0], bucket, bucketProds[bucket], options.count)
	}
	for n > 0 {
		<-done
		n--
	}
}

func loadBucket(addr, bucket, prodfile string, count int) int {
	u, err := url.Parse(addr)
	mf(err, "parse")

	c, err := couchbase.Connect(u.String())
	mf(err, "connect - "+u.String())

	p, err := c.GetPool("default")
	mf(err, "pool")

	bs := make([]*couchbase.Bucket, 0, options.parallel)
	for i := 0; i < options.parallel; i++ {
		b, err := p.GetBucket(bucket)
		mf(err, "bucket")
		bs = append(bs, b)
		go genDocuments(b, prodfile, i+1, options.count)
	}
	return options.parallel
}

func genDocuments(b *couchbase.Bucket, prodfile string, idx, n int) {
	conf := make(map[string]interface{})
	start, err := monster.Parse(prodfile, conf)
	mf(err, "monster - ")
	nonterminals, root := monster.Build(start)
	c := map[string]interface{}{
		"_nonterminals": nonterminals,
		// rand.Rand is not thread safe.
		"_random":   rand.New(rand.NewSource(int64(options.seed))),
		"_bagdir":   bagDir,
		"_prodfile": prodfile,
	}
	msg := fmt.Sprintf("%s - set", b.Name)
	for i := 0; i < n; i++ {
		monster.Initialize(c)
		doc := root.Generate(c)
		key := fmt.Sprintf("%s-%v-%v", b.Name, idx, i+1)
		err = b.SetRaw(key, 0, []byte(doc))
		mf(err, msg)
	}
	fmt.Printf("routine %v generated %v documents for %s\n", idx, n, b.Name)
	done <- true
}

func mf(err error, msg string) {
	if err != nil {
		log.Fatalf("%v: %v", msg, err)
	}
}
