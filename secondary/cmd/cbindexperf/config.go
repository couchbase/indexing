package main

import (
	"encoding/json"
	c "github.com/couchbase/indexing/secondary/common"
	"io/ioutil"
	"os"
)

type ScanConfig struct {
	Id        uint64
	Bucket    string
	Index     string
	DefnId    uint64
	Type      string
	Limit     int64
	Low       c.SecondaryKey
	Lookups   []c.SecondaryKey
	High      c.SecondaryKey
	Inclusion int
	Repeat    int
}

type Config struct {
	ScanSpecs   []*ScanConfig
	Concurrency int
	Clients     int
}

type ScanResult struct {
	Id         uint64
	Rows       uint64
	Duration   int64
	ErrorCount uint64
}

type Result struct {
	ScanResults []*ScanResult
	Rows        uint64
	Duration    float64
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
