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

	flag.Parse()
	if *help {
		flag.PrintDefaults()
		os.Exit(0)
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

	randdocs.Run(cfg)
}
