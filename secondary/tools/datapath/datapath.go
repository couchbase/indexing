package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"code.google.com/p/goprotobuf/proto"
	ap "github.com/couchbase/indexing/secondary/adminport"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/indexer"
	"github.com/couchbase/indexing/secondary/projector"
	"github.com/couchbase/indexing/secondary/protobuf"
	"github.com/couchbaselabs/go-couchbase"
)

var pooln = "default"

var options struct {
	buckets       string
	kvaddrs       string
	adminports    string
	endpoints     string
	coordEndpoint string
	maxVbno       int
}

func argParse() []string {
	buckets := "default"
	kvaddrs := "127.0.0.1:12000"
	adminports := "localhost:9010"
	endpoints := "localhost:9020"
	coordEndpoint := "localhost:9021"
	flag.StringVar(&options.buckets, "buckets", buckets, "buckets to project")
	flag.StringVar(&options.kvaddrs, "kvaddrs", kvaddrs, "kvaddrs to connect")
	flag.StringVar(&options.adminports, "adminports", adminports, "adminports for projector")
	flag.StringVar(&options.endpoints, "endpoints", endpoints, "endpoints for mutations stream")
	flag.StringVar(&options.coordEndpoint, "coorendp", coordEndpoint, "coordinator endpoint")
	flag.IntVar(&options.maxVbno, "maxvb", 1024, "max number of vbuckets")
	flag.Parse()
	return flag.Args()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] <addr> \n", os.Args[0])
	flag.PrintDefaults()
}

var done = make(chan bool)

var iid = []uint64{0x11, 0x12, 0x13}

func main() {
	args := argParse()
	if len(args) < 1 {
		usage()
		os.Exit(1)
	}
	cluster := args[0]

	c.SetLogLevel(c.LogLevelInfo)
	for _, endpoint := range strings.Split(options.endpoints, ",") {
		go endpointServer(endpoint)
	}
	go endpointServer(options.coordEndpoint)

	time.Sleep(100 * time.Millisecond)

	kvaddrs := strings.Split(options.kvaddrs, ",")
	adminports := strings.Split(options.adminports, ",")
	buckets := strings.Split(options.buckets, ",")
	if len(kvaddrs) != len(adminports) {
		log.Fatal("mismatch in kvaddrs and adminports")
	}
	for i, kvaddr := range kvaddrs {
		doProjector(cluster, buckets, []string{kvaddr}, adminports[i])
	}

	for i := 0; i < len(kvaddrs); i++ {
		<-done
	}
}

func doProjector(cluster string, buckets, kvaddrs []string, adminport string) {
	projector.NewProjector(cluster, kvaddrs, adminport)
	time.Sleep(100 * time.Millisecond)
	aport := ap.NewHTTPClient("http://"+adminport, "/adminport/")

	bucketinfo := make(map[string]map[uint16]uint64)
	for _, bucket := range buckets {
		b := couchbaseBucket("http://"+cluster, bucket)
		fReq := protobuf.FailoverLogRequest{
			Pool:   proto.String(pooln),
			Bucket: proto.String(bucket),
			Vbnos:  getvbmap(b, kvaddrs),
		}
		fRes := protobuf.FailoverLogResponse{}
		if err := aport.Request(&fReq, &fRes); err != nil {
			log.Fatal(err)
		}
		vbuuids := make(map[uint16]uint64)
		for _, flog := range fRes.GetLogs() {
			vbno := uint16(flog.GetVbno())
			vbuuids[vbno] = flog.Vbuuids[len(flog.Vbuuids)-1]
		}
		bucketinfo[bucket] = vbuuids
	}

	time.Sleep(100 * time.Millisecond)

	mReq := makeStartRequest(bucketinfo)
	mRes := protobuf.MutationStreamResponse{}
	if err := aport.Request(mReq, &mRes); err != nil {
		log.Fatal(err)
	}
}

func makeStartRequest(bucketinfo map[string]map[uint16]uint64) *protobuf.MutationStreamRequest {
	req := protobuf.MutationStreamRequest{
		Topic:             proto.String("maintanence"),
		Pools:             []string{},
		Buckets:           []string{},
		RestartTimestamps: []*protobuf.BranchTimestamp{},
	}
	buckets := make([]string, 0, len(bucketinfo))
	for bucket, vbuuids := range bucketinfo {
		bTs := makeBranchTimestamp(bucket, vbuuids)
		req.Pools = append(req.Pools, pooln)
		req.Buckets = append(req.Buckets, bucket)
		req.RestartTimestamps = append(req.RestartTimestamps, bTs)
		buckets = append(buckets, bucket)
	}
	req.Instances = makeIndexInstances(buckets)
	req.SetStartFlag()
	return &req
}

func makeIndexInstances(buckets []string) []*protobuf.IndexInst {
	sExprs := []string{`{"type":"property","path":"age"}`,
		`{"type":"property","path":"firstname"}`}
	defn1 := &protobuf.IndexDefn{
		DefnID:          proto.Uint64(iid[0]),
		Bucket:          proto.String("users"),
		IsPrimary:       proto.Bool(false),
		Name:            proto.String("index1"),
		Using:           protobuf.StorageType_View.Enum(),
		ExprType:        protobuf.ExprType_N1QL.Enum(),
		SecExpressions:  sExprs,
		PartitionScheme: protobuf.PartitionScheme_TEST.Enum(),
		PartnExpression: proto.String(`{"type":"property","path":"city"}`),
	}
	defn2 := &protobuf.IndexDefn{
		DefnID:          proto.Uint64(iid[1]),
		Bucket:          proto.String("users"),
		IsPrimary:       proto.Bool(false),
		Name:            proto.String("index2"),
		Using:           protobuf.StorageType_View.Enum(),
		ExprType:        protobuf.ExprType_N1QL.Enum(),
		SecExpressions:  []string{`{"type":"property","path":"city"}`},
		PartitionScheme: protobuf.PartitionScheme_TEST.Enum(),
		PartnExpression: proto.String(`{"type":"property","path":"gender"}`),
	}
	defn3 := &protobuf.IndexDefn{
		DefnID:          proto.Uint64(iid[2]),
		Bucket:          proto.String("projects"),
		IsPrimary:       proto.Bool(false),
		Name:            proto.String("index3"),
		Using:           protobuf.StorageType_View.Enum(),
		ExprType:        protobuf.ExprType_N1QL.Enum(),
		SecExpressions:  []string{`{"type":"property","path":"name"}`},
		PartitionScheme: protobuf.PartitionScheme_TEST.Enum(),
		PartnExpression: proto.String(`{"type":"property","path":"language"}`),
	}

	makeInstance := func(id uint64, defn *protobuf.IndexDefn) *protobuf.IndexInst {
		return &protobuf.IndexInst{
			InstId:     proto.Uint64(id),
			State:      protobuf.IndexState_IndexInitial.Enum(),
			Definition: defn,
			Tp: &protobuf.TestPartition{
				CoordEndpoint: proto.String(options.coordEndpoint),
				Endpoints:     strings.Split(options.endpoints, ","),
			},
		}
	}

	i1 := makeInstance(0x1, defn1)
	i2 := makeInstance(0x2, defn2)
	i3 := makeInstance(0x3, defn3)

	rs := make([]*protobuf.IndexInst, 0)
	for _, bucket := range buckets {
		switch bucket {
		case "users":
			rs = append(rs, i1, i2)
		case "projects":
			rs = append(rs, i3)
		}
	}
	return rs
}

func endpointServer(addr string) {
	mutChanSize := 100
	mutch := make(chan []*protobuf.VbKeyVersions, mutChanSize)
	sbch := make(chan interface{}, 100)
	_, err := indexer.NewMutationStream(addr, mutch, sbch)
	if err != nil {
		log.Fatal(err)
	}
	mutations, messages := 0, 0
	commandWise := make(map[byte]int)
	keys := map[uint64]map[string][]string{
		0x11: make(map[string][]string),
		0x12: make(map[string][]string),
		0x13: make(map[string][]string),
	}

	printTm := time.Tick(1000 * time.Millisecond)

loop:
	for {
		select {
		case vbs, ok := <-mutch:
			if ok {
				mutations += gatherKeys(vbs, commandWise, keys)
			} else {
				break loop
			}
		case s, ok := <-sbch:
			if ok {
				switch v := s.(type) {
				case []*indexer.RestartVbuckets:
					printRestartVbuckets(addr, v)
				}
				messages++
			} else {
				break loop
			}
		case <-time.After(4 * time.Second):
			break loop
		case <-printTm:
			log.Println(addr, "-- mutations", mutations)
			log.Println(addr, "-- commandWise", commandWise)
		}
	}

	log.Println(addr, "-- mutations", mutations, "-- messages", messages)
	log.Println(addr, "-- commandWise", commandWise)
	ks, ds := countKeysAndDocs(keys[0x11])
	log.Printf("%v -- for instance 0x11, %v unique keys found in %v docs\n", addr, ks, ds)
	ks, ds = countKeysAndDocs(keys[0x12])
	log.Printf("%v -- for instance 0x12, %v unique keys found in %v docs\n", addr, ks, ds)
	done <- true
}

func printRestartVbuckets(addr string, rs []*indexer.RestartVbuckets) {
	for _, r := range rs {
		log.Printf("restart: %s, %v %v\n", addr, r.Bucket, r.Vbuckets)
	}
}

func gatherKeys(
	vbs []*protobuf.VbKeyVersions,
	commandWise map[byte]int,
	keys map[uint64]map[string][]string,
) int {

	mutations := 0
	for _, vb := range vbs {
		kvs := vb.GetKvs()
		for _, kv := range kvs {
			mutations++
			docid := string(kv.GetDocid())
			uuids, seckeys := kv.GetUuids(), kv.GetKeys()
			for i, command := range kv.GetCommands() {
				cmd := byte(command)
				if _, ok := commandWise[cmd]; !ok {
					commandWise[cmd] = 0
				}
				commandWise[cmd]++

				if cmd == c.StreamBegin {
					continue
				}

				if cmd == c.Upsert {
					uuid := uuids[i]
					key := string(seckeys[i])
					if _, ok := keys[uuid][key]; !ok {
						keys[uuid][key] = make([]string, 0)
					}
					keys[uuid][key] = append(keys[uuid][key], docid)
				}
			}
		}
	}
	return mutations
}

func countKeysAndDocs(keys map[string][]string) (int, int) {
	countKs, countDs := 0, 0
	for _, docs := range keys {
		countKs++
		countDs += len(docs)
	}
	return countKs, countDs
}

func makeBranchTimestamp(bucket string, vbuuids map[uint16]uint64) *protobuf.BranchTimestamp {
	vbnos := make(c.Vbuckets, 0, options.maxVbno)
	for vbno := range vbuuids {
		vbnos = append(vbnos, vbno)
	}
	sort.Sort(vbnos)
	uuids := make([]uint64, 0, options.maxVbno)
	for _, vbno := range vbnos {
		uuids = append(uuids, vbuuids[vbno])
	}

	bTs := &protobuf.BranchTimestamp{
		Bucket:  proto.String(bucket),
		Vbnos:   c.Vbno16to32(vbnos),
		Seqnos:  make([]uint64, len(vbnos)),
		Vbuuids: uuids,
	}
	return bTs
}

func couchbaseBucket(addr, bucket string) *couchbase.Bucket {
	u, err := url.Parse(addr)
	mf(err, "parse")

	c, err := couchbase.Connect(u.String())
	mf(err, "connect - "+u.String())

	p, err := c.GetPool("default")
	mf(err, "pool")

	b, err := p.GetBucket(bucket)
	mf(err, "bucket")
	return b
}

func getvbmap(b *couchbase.Bucket, kvaddrs []string) []uint32 {
	vbnos := make([]uint32, 0, 1024)
	kvbs, err := b.GetVBmap(kvaddrs)
	mf(err, fmt.Sprintf("getvbmap - %s", b.Name))
	for _, vbs := range kvbs {
		for _, vb := range vbs {
			vbnos = append(vbnos, uint32(vb))
		}
	}
	return vbnos
}

func mf(err error, msg string) {
	if err != nil {
		log.Fatalf("%v: %v", msg, err)
	}
}
