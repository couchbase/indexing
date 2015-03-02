package main

import (
	"flag"
	"fmt"
	"os"
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
	cluster := flag.String("cluster", "127.0.0.1:9000", "Cluster server address")

	flag.Parse()

	if *help {
		flag.PrintDefaults()
		os.Exit(0)
	}

	cfg, err := parseConfig(*config)
	handleError(err)

	res, err := RunCommands(*cluster, cfg)
	handleError(err)

	err = writeResults(res, *outfile)
	handleError(err)

}
