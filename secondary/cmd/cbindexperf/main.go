package main

import (
	"flag"
	"fmt"
	"github.com/couchbase/cbauth"
	"os"
	"runtime"
	"strings"
	"time"
)

func handleError(err error) {
	if err != nil {
		fmt.Printf("Error occured: %v\n", err)
		os.Exit(1)
	}
}

func main() {
	help := flag.Bool("help", false, "Help")
	config := flag.String("configfile", "config.json", "Scan load config file")
	outfile := flag.String("resultfile", "results.json", "Result report file")
	cpus := flag.Int("cpus", runtime.NumCPU(), "Number of CPUs")
	cluster := flag.String("cluster", "127.0.0.1:9000", "Cluster server address")
	auth := flag.String("auth", "Administrator:asdasd", "Auth")

	flag.Parse()

	if *help {
		flag.PrintDefaults()
		os.Exit(0)
	}

	runtime.GOMAXPROCS(*cpus)
	up := strings.Split(*auth, ":")
	_, err := cbauth.InternalRetryDefaultInit(*cluster, up[0], up[1])
	if err != nil {
		fmt.Println("Failed to initialize cbauth: %s\n", err)
		os.Exit(1)
	}

	cfg, err := parseConfig(*config)
	handleError(err)

	t0 := time.Now()
	res, err := RunCommands(*cluster, cfg)
	handleError(err)
	dur := time.Now().Sub(t0)

	totalRows := uint64(0)
	for _, result := range res.ScanResults {
		totalRows += result.Rows
	}

	res.Rows = totalRows
	res.Duration = dur.Seconds()

	rate := int(float64(totalRows) / dur.Seconds())

	fmt.Printf("Throughput = %d rows/sec\n", rate)

	os.Remove(*outfile)
	err = writeResults(res, *outfile)
	handleError(err)

}
