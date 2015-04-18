package main

import (
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	qclient "github.com/couchbase/indexing/secondary/queryport/client"
	"sync"
	"sync/atomic"
	"time"
)

type Job struct {
	spec   *ScanConfig
	result *ScanResult
}

func RunScan(client *qclient.GsiClient,
	spec *ScanConfig, result *ScanResult) {
	var err error

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

			atomic.AddUint64(&result.Rows, uint64(len(pkeys)))
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

	dur := time.Now().Sub(startTime)
	atomic.AddInt64(&result.Duration, dur.Nanoseconds())
}

func Worker(jobQ chan Job, clientQ chan *qclient.GsiClient, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobQ {
		c := <-clientQ
		RunScan(c, job.spec, job.result)
		clientQ <- c
	}
}

func RunCommands(cluster string, cfg *Config) (*Result, error) {
	var result Result

	var clientQ chan *qclient.GsiClient
	var jobQ chan Job
	var wg sync.WaitGroup

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

	clientQ = make(chan *qclient.GsiClient, cfg.Clients)
	for i := 0; i < cfg.Clients; i++ {
		c, err := qclient.NewGsiClient(cluster, config)
		if err != nil {
			return nil, err
		}

		defer c.Close()
		clientQ <- c
	}

	jobQ = make(chan Job, cfg.Concurrency)
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go Worker(jobQ, clientQ, &wg)
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

		res := new(ScanResult)
		res.Id = spec.Id
		for i := 0; i < spec.Repeat+1; i++ {
			j := Job{
				spec:   spec,
				result: res,
			}

			jobQ <- j
		}

		result.ScanResults = append(result.ScanResults, res)
	}

	close(jobQ)
	wg.Wait()

	return &result, err
}
