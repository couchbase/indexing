package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/couchbase/indexing/secondary/tools/randdocs"
	"io/ioutil"
	"os"
)

func main() {

	help := flag.Bool("help", false, "Help")
	config := flag.String("config", "config.json", "Config file")
	Threads := flag.Int("Threads", -1, "Number of threads")
	NumDocs := flag.Int("NumDocs", -1, "Number of docs")
	DocIdLen := flag.Int("DocIdLen", -1, "Length of docid")
	UseRandDocID := flag.Bool("UseRandDocID", false, "Use Random docid")
	FieldSize := flag.Int("FieldSize", -1, "Field size will be at least this much")
	OpsPerSec := flag.Int("OpsPerSec", -1, "How many ops per sec")
	Iterations := flag.Int("Iterations", -1, "How many times to repeat")

	flag.Parse()
	if *help {
		flag.PrintDefaults()
		os.Exit(0)
	}

	if *config == "" {
		fmt.Println("Config is empty!")
		flag.PrintDefaults()
		return
	}

	bs, err := ioutil.ReadFile(*config)
	if err != nil {
		fmt.Printf("Error occured: %v\n", err)
		os.Exit(1)
	}

	var cfg randdocs.Config
	if err := json.Unmarshal(bs, &cfg); err != nil {
		fmt.Printf("Error occured: %v\n", err)
		os.Exit(1)
	}

	if *Threads != -1 {
		cfg.Threads = *Threads
	}

	if *NumDocs != -1 {
		cfg.NumDocs = *NumDocs
	}

	if *UseRandDocID {
		cfg.UseRandDocID = *UseRandDocID
	}

	if *FieldSize != -1 {
		cfg.FieldSize = *FieldSize
	}

	if *DocIdLen != -1 {
		cfg.DocIdLen = *DocIdLen
	}

	if *OpsPerSec != -1 {
		cfg.OpsPerSec = *OpsPerSec
	}

	if *Iterations != -1 {
		cfg.Iterations = *Iterations
	}

	randdocs.Run(cfg)
}
