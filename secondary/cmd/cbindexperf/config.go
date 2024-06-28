package main

import (
	"encoding/json"
	"io/ioutil"
	"os"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/queryport/client"
	"github.com/couchbase/indexing/secondary/stats"
)

type TestConfig struct {
	RandomKeyLen uint32
}

type ScanConfig struct {
	Id              uint64
	Bucket          string
	Scope           string
	Collection      string
	Index           string
	DefnId          uint64
	Type            string
	Limit           int64
	Low             c.SecondaryKey
	Lookups         []c.SecondaryKey
	High            c.SecondaryKey
	Inclusion       int
	Repeat          uint32
	NInterval       uint32 // Stats dump nrequests interval
	Consistency     bool   // Use session consistency
	Scans           client.Scans
	IndexProjection *client.IndexProjection
	IndexVector     *client.IndexVector
	GroupAggr       *client.GroupAggr
	TestSpec        *TestConfig

	iteration uint32
}

type Config struct {
	LatencyBuckets []int64
	ScanSpecs      []*ScanConfig
	Concurrency    int
	Clients        int
	ClientBootTime int
}

type ScanResult struct {
	Id           uint64
	Rows         uint64
	Duration     int64
	LatencyHisto stats.Histogram
	ErrorCount   uint64

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

func parseConfig(filepath string) (*Config, error) {
	var cfg Config
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(data, &cfg)
	return &cfg, err
}

func writeResults(r *Result, filepath string) error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(filepath, os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		return err
	}

	_, err = file.Write(data)

	return err
}
