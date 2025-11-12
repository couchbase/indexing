package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/querycmd"
	"github.com/couchbase/indexing/secondary/security"
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
	gcpercent := flag.Int("gcpercent", 100, "GC percentage")
	useTls := flag.Bool("use_tls", false, "Set to enable TLS Connections")
	caCert := flag.String("cacert", "", "CA Cert")
	useTools := flag.Bool("use_tools", false, "Set to enable Tools config instead of CBAuth")

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
	creds := strings.Split(*auth, ":")
	if len(creds) < 2 || creds[0] == "" || creds[1] == "" {
		logging.Errorf("Error in setting config: use format -auth user:password with non-empty creds")
		return
	}

	if *useTools {
		insecureSkipVerify := false
		if *caCert == "" {
			insecureSkipVerify = true
		}
		err := security.SetToolsConfig(creds[0], creds[1], *caCert, insecureSkipVerify, true)
		if err != nil {
			logging.Errorf("Error in setting config: %v", err)
			return
		}
	} else {
		_, err := cbauth.InternalRetryDefaultInit(*cluster, creds[0], creds[1])
		if err != nil {
			fmt.Printf("Failed to initialize cbauth: %s\n", err)
			os.Exit(1)
		}
		if *useTls {
			querycmd.InitToolsSecurityContext(*cluster, "", "", "", *caCert, true)
		}
	}

	// Go runtime has default gc percent as 100.
	if *gcpercent != 100 {
		fmt.Println("Setting gc percentage to", *gcpercent)
		debug.SetGCPercent(*gcpercent)
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

	SeedInit()

	t0 := time.Now()
	res, err := RunCommands(*cluster, cfg, statsW, *useTools)
	handleError(err)
	dur := time.Now().Sub(t0)

	totalRows := uint64(0)
	for _, result := range res.ScanResults {
		totalRows += result.Rows
	}
	res.Rows = totalRows
	res.Duration = dur.Seconds() - res.WarmupDuration

	rate := float64(totalRows) / res.Duration

	fmt.Printf("Throughput = %v rows/sec\n", rate)

	os.Remove(*outfile)
	err = writeResults(res, *outfile)
	handleError(err)

}
