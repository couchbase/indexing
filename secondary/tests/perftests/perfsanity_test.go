package perftests

import (
	"encoding/json"
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/platform"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	kv "github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

type ScanResult struct {
	Id           uint64
	Rows         uint64
	Duration     int64
	LatencyHisto string
	ErrorCount   platform.AlignedUint64

	// periodic stats
	iter          uint32
	statsRows     uint64
	statsDuration int64
}

type Result struct {
	ScanResults    []*ScanResult
	Rows           uint64
	Duration       float64
	WarmupDuration float64
}

func TestPerfInitialIndexBuild(t *testing.T) {
	log.Printf("In TestPerfInitialIndexBuild()")
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	prodfile := filepath.Join(proddir, "test3.prod")
	log.Printf("Generating JSON docs")
	keyValues := GenerateJsons(numdocs, 1, prodfile, bagdir)
	log.Printf("Setting JSON docs in KV")
	kv.SetKeyValues(keyValues, "default", "", clusterconfig.KVAddress)

	var indexName = "index_company"
	var bucketName = "default"

	os.Setenv("NS_SERVER_CBAUTH_URL", fmt.Sprintf("http://%s/_cbauth", indexManagementAddress))
	os.Setenv("NS_SERVER_CBAUTH_USER", clusterconfig.Username)
	os.Setenv("NS_SERVER_CBAUTH_PWD", clusterconfig.Password)
	os.Setenv("NS_SERVER_CBAUTH_RPC_URL", fmt.Sprintf("http://%s/cbauth-demo", indexManagementAddress))
	e = os.Setenv("CBAUTH_REVRPC_URL", fmt.Sprintf("http://%s:%s@%s/query", clusterconfig.Username, clusterconfig.Password, indexManagementAddress))

	log.Printf("Creating a 2i")
	start := time.Now()
	err := secondaryindex.N1QLCreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in creating the index", t)
	elapsed := time.Since(start)
	initialBuildtime := elapsed.Nanoseconds()
	log.Printf("PERFSTAT InitialBuild %v nanoseconds for %d documents\n", initialBuildtime, numdocs)

	// Disabling Pass/Fail for now
	/*if initialBuildtime > maxbuildtime {
		err := errors.New(fmt.Sprintf("The initial build time %v is more than the maximum expected build time %v for this run (in seconds)", initialBuildtime, maxbuildtime))
		FailTestIfError(err, "TestPerfInitialIndexBuild failed", t)
	}*/
}

func TestPerfScanLatency_Lookup_StaleOk(t *testing.T) {
	log.Printf("In TestPerfScanLatency_Lookup_StaleOk()")
	currpath, _ := os.Getwd()
	configfile := currpath + "/TestPerfScanLatency_Lookup_StaleOk.json"
	RunScanTest(t, configfile)
}

func TestPerfScanLatency_Range_StaleOk(t *testing.T) {
	log.Printf("In TestPerfScanLatency_Range_StaleOk()")
	currpath, _ := os.Getwd()
	configfile := currpath + "/TestPerfScanLatency_Range_StaleOk.json"
	RunScanTest(t, configfile)
}

func TestPerfScanLatency_Lookup_StaleFalse(t *testing.T) {
	log.Printf("In TestPerfScanLatency_Lookup_StaleFalse()")
	currpath, _ := os.Getwd()
	configfile := currpath + "/TestPerfScanLatency_Lookup_StaleFalse.json"
	prodfile := filepath.Join(proddir, "test3.prod")
	keyValues := GenerateJsons(100000, 1, prodfile, bagdir)
	go loaddata(t, keyValues, "default", "")
	RunScanTest(t, configfile)
}

func TestPerfScanLatency_Range_StaleFalse(t *testing.T) {
	log.Printf("In TestPerfScanLatency_Range_StaleFalse()")
	currpath, _ := os.Getwd()
	configfile := currpath + "/TestPerfScanLatency_Range_StaleFalse.json"
	prodfile := filepath.Join(proddir, "test3.prod")
	keyValues := GenerateJsons(100000, 1, prodfile, bagdir)
	go loaddata(t, keyValues, "default", "")
	log.Printf("After invoking loaddata")
	RunScanTest(t, configfile)
}

func RunScanTest(t *testing.T, configfile string) {
	log.Printf("Starting scan test")
	uuid, _ := c.NewUUID()
	random := strconv.Itoa(int(uuid.Uint64()))
	summaryfile := "/tmp/summary_" + random + ".json"
	statsfile := "/tmp/stats_" + random + ".json"
	currpath, _ := os.Getwd()         // Path of perfsanity test dir
	workingdir := currpath + "/../.." // Path of secondaryindex root
	// args := []string {"-cluster", indexManagementAddress, "-configfile", currpath+"/stale_ok_scan1.json", "-resultfile", resultfile}

	if usen1qlperf {
		log.Printf("Using n1qlperf tool for scanning")
		cmd := exec.Command(workingdir+"/tools/n1qlperf/n1qlperf",
			"-cluster", indexManagementAddress,
			"-configfile", configfile,
			"-resultfile", summaryfile,
			"-statsfile", statsfile)
		cmd.Env = getEnvironmentForN1qlPerf(cmd.Env)
		out, err := cmd.CombinedOutput()
		log.Printf("The output is %s\n", out)
		FailTestIfError(err, "Error in executing n1qlperf tool", t)
	} else {
		log.Printf("Using cbindexperf tool for scanning")
		out, err := exec.Command(workingdir+"/cmd/cbindexperf/cbindexperf",
			"-auth", clusterconfig.Username+":"+clusterconfig.Password,
			"-cluster", indexManagementAddress,
			"-configfile", configfile,
			"-resultfile", summaryfile,
			"-statsfile", statsfile).CombinedOutput()
		log.Printf("The output is %s\n", out)
		FailTestIfError(err, "Error in executing cbindexperf tool", t)
	}

	GetScanStats(summaryfile, statsfile, t)

	// Disabling Pass/Fail for now
	/*
		_, meanLatency, throughput := GetScanStats(summaryfile, statsfile, t)
		if meanLatency > maxlatency {
			err := errors.New(fmt.Sprintf("The scan latency %v is more than the maximum expected latency %v for this run (in nanoseconds)", meanLatency, maxlatency))
			FailTestIfError(err, "Scan perf test failed in scan stat validation", t)
		}

		if throughput < minthroughput {
			err := errors.New(fmt.Sprintf("The scan throughput %v is less than the minimum expected throughput %v for this run (in rows/sec)", throughput, minthroughput))
			FailTestIfError(err, "Scan perf test failed in scan stat validation", t)
		}*/
}

func GetScanStats(summaryfile, statsfile string, t *testing.T) (Result, int64, int64) {
	result := Result{}
	var meanLatency, throughput int64

	log.Printf("The scan result file is at %v\n", summaryfile)
	log.Printf("The scan stats file is at %v\n", statsfile)

	file1, err := os.Open(summaryfile)
	FailTestIfError(err, "Error in creating result file handle", t)
	decoder := json.NewDecoder(file1)
	err = decoder.Decode(&result)
	FailTestIfError(err, "Error in decoding scan result", t)

	dat2, err := ioutil.ReadFile(statsfile)
	FailTestIfError(err, "Error in reading stats file", t)
	stats := strings.Split(string(dat2), "\n")

	var latencySum int64
	latencies := make([]int, len(stats))
	latencySum = 0
	for _, stat := range stats {
		s := strings.Split(stat, ":")
		i := len(s) - 1
		latency, _ := strconv.ParseInt(s[i], 10, 64)
		latencies = append(latencies, int(latency))
		latencySum += latency
	}

	percentile95_ScanLatency := getPercentile(95, latencies)
	percentile80_ScanLatency := getPercentile(80, latencies)
	meanLatency = latencySum / int64(len(stats))
	throughput = int64(float64(result.Rows) / result.Duration)

	log.Printf("ScanResults summary:")
	log.Printf("Rows = %v", result.Rows)
	// log.Printf("Duration = %v seconds", result.Duration)
	log.Printf("PERFSTAT Duration %10.2f nanoseconds", result.Duration*1000000000)
	log.Printf("PERFSTAT Throughput %v rows/sec", throughput)
	log.Printf("PERFSTAT AverageLatency %v nanoseconds\n", meanLatency)
	log.Printf("PERFSTAT 95thPercentileLatency %v nanoseconds\n", percentile95_ScanLatency)
	log.Printf("PERFSTAT 80thPercentileLatency %v nanoseconds\n", percentile80_ScanLatency)
	histogram := strings.Split(result.ScanResults[0].LatencyHisto, ", ")
	var requests int64
	requests = 0
	var timeBucket string
	for _, hist := range histogram {
		s := strings.Split(hist, "=")
		req, _ := strconv.ParseInt(s[1], 10, 64)
		if req > requests {
			requests = req
			timeBucket = s[0]
		}
	}
	log.Printf("%v Requests took %v time to complete", requests, timeBucket)

	// Delete the generated files
	err = os.Remove(summaryfile)
	FailTestIfError(err, "Error in deleting generated result file", t)
	err = os.Remove(statsfile)
	FailTestIfError(err, "Error in deleting generated result file", t)

	return result, meanLatency, throughput
}

func getEnvironmentForN1qlPerf(cmdEnv []string) []string {
	return append(cmdEnv, fmt.Sprintf("NS_SERVER_CBAUTH_URL=http://%s/_cbauth", indexManagementAddress),
		fmt.Sprintf("NS_SERVER_CBAUTH_USER=%s", clusterconfig.Username),
		fmt.Sprintf("NS_SERVER_CBAUTH_PWD=%s", clusterconfig.Password),
		fmt.Sprintf("NS_SERVER_CBAUTH_RPC_URL=http://%s/cbauth-demo", indexManagementAddress),
		fmt.Sprintf("CBAUTH_REVRPC_URL=http://%s:%s@%s/query", clusterconfig.Username, clusterconfig.Password, indexManagementAddress))
}

func getPercentile(percentile float64, array []int) int {
	sort.Ints(array)
	index := int((percentile / 100) * float64(len(array)))
	return array[index]
}

func loaddata(t *testing.T, docsToCreate tc.KeyValues, bucketName, bucketPassword string) {
	log.Printf("In loaddata")
	log.Printf("Incrementally loading bucket %v", bucketName)
	kv.SetKeyValues(docsToCreate, bucketName, bucketPassword, clusterconfig.KVAddress)
}
