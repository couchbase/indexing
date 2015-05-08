package main

import (
	"flag"
	"fmt"
	"github.com/couchbase/cbauth"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
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
	statsfile := flag.String("statsfile", "", "Periodic statistics report file")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")

	flag.Parse()

	if *help {
		flag.PrintDefaults()
		os.Exit(0)
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fmt.Println("Failed create cpu profile file")
			os.Exit(1)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
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
	res.Duration = dur.Seconds()

	rate := int(float64(totalRows) / dur.Seconds())

	fmt.Printf("Throughput = %d rows/sec\n", rate)

	os.Remove(*outfile)
	err = writeResults(res, *outfile)
	handleError(err)

}
