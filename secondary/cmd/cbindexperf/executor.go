package main

import (
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	qclient "github.com/couchbase/indexing/secondary/queryport/client"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

var (
	defaultLatencyBuckets = []int64{
		500, 1000, 10000, 200000, 500000, 800000,
	}
)

type Job struct {
	spec   *ScanConfig
	result *ScanResult
}

func RunScan(client *qclient.GsiClient,
	spec *ScanConfig, result *ScanResult) {
	var err error
	var rows int64

	result.Id = spec.Id

	errFn := func(e string) {
		fmt.Printf("REQ:%d scan error occured: %s\n", spec.Id, e)
		atomic.AddUint64(&result.ErrorCount, 1)
	}

	callb := func(res qclient.ResponseReader) bool {
		if res.Error() != nil {
			errFn(res.Error().Error())
			return false
		} else {
			_, pkeys, err := res.GetEntries()
			if err != nil {
				errFn(err.Error())
				return false
			}

			rows += int64(len(pkeys))
		}

		return true
	}

	startTime := time.Now()
	switch spec.Type {
	case "All":
		err = client.ScanAll(spec.DefnId, spec.Limit, c.AnyConsistency, nil, callb)
	case "Range":
		err = client.Range(spec.DefnId, spec.Low, spec.High,
			qclient.Inclusion(spec.Inclusion), false, spec.Limit, c.AnyConsistency, nil, callb)
	case "Lookup":
		err = client.Lookup(spec.DefnId, spec.Lookups, false,
			spec.Limit, c.AnyConsistency, nil, callb)
	}

	if err != nil {
		errFn(err.Error())
	}

	var lat int64
	dur := time.Now().Sub(startTime)
	atomic.AddUint64(&result.Rows, uint64(rows))
	if rows > 0 {
		lat = dur.Nanoseconds() / rows
	}
	result.LatencyHisto.Add(lat)
	atomic.AddInt64(&result.Duration, dur.Nanoseconds())
}

func Worker(jobQ chan Job, c *qclient.GsiClient, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobQ {
		RunScan(c, job.spec, job.result)
	}
}

func RunCommands(cluster string, cfg *Config) (*Result, error) {
	var result Result

	var clients []*qclient.GsiClient
	var jobQ chan Job
	var wg sync.WaitGroup

	if len(cfg.LatencyBuckets) == 0 {
		cfg.LatencyBuckets = defaultLatencyBuckets
	}

	config := c.SystemConfig.SectionConfig("queryport.client.", true)
	client, err := qclient.NewGsiClient(cluster, config)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	indexes, err := client.Refresh()
	if err != nil {
		return nil, err
	}

	clients = make([]*qclient.GsiClient, cfg.Clients)
	for i := 0; i < cfg.Clients; i++ {
		c, err := qclient.NewGsiClient(cluster, config)
		if err != nil {
			return nil, err
		}

		defer c.Close()
		clients[i] = c
	}

	jobQ = make(chan Job, cfg.Concurrency*1000)
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go Worker(jobQ, clients[i%cfg.Clients], &wg)
	}

	for i, spec := range cfg.ScanSpecs {
		if spec.Id == 0 {
			spec.Id = uint64(i)
		}

		for _, index := range indexes {
			if index.Definition.Bucket == spec.Bucket &&
				index.Definition.Name == spec.Index {
				spec.DefnId = uint64(index.Definition.DefnId)
			}
		}

		hFn := func(v int64) string {
			if v == math.MinInt64 {
				return "0"
			} else if v == math.MaxInt64 {
				return "inf"
			}
			return fmt.Sprint(time.Nanosecond * time.Duration(v))
		}

		res := new(ScanResult)
		res.LatencyHisto.Init(cfg.LatencyBuckets, hFn)
		res.Id = spec.Id
		result.ScanResults = append(result.ScanResults, res)
	}

	// Round robin scheduling of jobs
	var allFinished bool
loop:
	for {
		allFinished = true
		for i, spec := range cfg.ScanSpecs {
			if spec.iteration < spec.Repeat {
				j := Job{
					spec:   spec,
					result: result.ScanResults[i],
				}

				jobQ <- j
				spec.iteration++
				allFinished = false
			}
		}

		if allFinished {
			break loop
		}
	}

	close(jobQ)
	wg.Wait()

	return &result, err
}
