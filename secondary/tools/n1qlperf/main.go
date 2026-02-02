//go:build nolint

package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/indexing/secondary/logging"
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
	statsfile := flag.String("statsfile", "", "Periodic statistics report file")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	memprofile := flag.String("memprofile", "", "write mem profile to file")
	logLevel := flag.String("logLevel", "error", "Log Level")

	flag.Parse()

	logging.SetLogLevel(logging.Level(*logLevel))
	fmt.Println("Log Level =", *logLevel)

	if *help {
		flag.PrintDefaults()
		os.Exit(0)
	}

	if *cpuprofile != "" {
		fd, err := os.Create(*cpuprofile)
		if err != nil {
			fmt.Println("Failed create cpu profile file")
			os.Exit(1)
		}
		pprof.StartCPUProfile(fd)
		defer pprof.StopCPUProfile()
	}
	if *memprofile != "" {
		fd, err := os.Create(*memprofile)
		if err != nil {
			fmt.Println("Failed create mem profile file")
			os.Exit(1)
		}
		defer pprof.WriteHeapProfile(fd)
	}

	runtime.GOMAXPROCS(*cpus)
	up := strings.Split(*auth, ":")
	_, err := cbauth.InternalRetryDefaultInit(*cluster, up[0], up[1])
	if err != nil {
		fmt.Printf("Failed to initialize cbauth: %s\n", err)
		os.Exit(1)
	}

	cfg, err := parseConfig(*config)
	handleError(err)

	var statsW io.Writer
	if *statsfile != "" {
		if f, err := os.Create(*statsfile); err != nil {
			handleError(err)
		} else {
			statsW = f
			defer f.Close()
		}
	}

	t0 := time.Now()
	res, err := RunCommands(*cluster, cfg, statsW)
	handleError(err)
	dur := time.Now().Sub(t0)

	totalRows := uint64(0)
	for _, result := range res.ScanResults {
		totalRows += result.Rows
	}
	res.Rows = totalRows
	res.Duration = dur.Seconds() - res.WarmupDuration

	rate := int(float64(totalRows) / res.Duration)

	fmt.Printf("Throughput = %d rows/sec\n", rate)

	os.Remove(*outfile)
	err = writeResults(res, *outfile)
	handleError(err)

}
