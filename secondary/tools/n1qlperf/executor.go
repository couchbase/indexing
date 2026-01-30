//go:build nolint

package main

import (
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	nclient "github.com/couchbase/indexing/secondary/queryport/n1ql"
	"github.com/couchbase/query/auth"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/tenant"
	"github.com/couchbase/query/value"
)

var (
	defaultLatencyBuckets = []int64{
		500, 1000, 10000, 100000, 500000, 1000000, 5000000, 10000000, 50000000,
		100000000, 300000000, 500000000, 1000000000, 2000000000, 3000000000,
		5000000000, 10000000000,
	}

	clientBootTime = 5 // Seconds

	requestCounter = 0
)

type Job struct {
	spec   *ScanConfig
	result *ScanResult
	sw     io.Writer
}

type JobResult struct {
	job  *Job
	rows int64
	dur  int64
}

func RunJob(nclient datastore.Indexer, job *Job, aggrQ chan *JobResult) {
	var err error
	var rows int64

	spec := job.spec
	result := job.result
	if result != nil {
		result.Id = spec.Id
	}

	errFn := func(e string) {
		fmt.Printf("REQ:%d scan error occured: %s\n", spec.Id, e)
		if result != nil {
			atomic.AddUint64(&result.ErrorCount, 1)
		}
	}

	var cons datastore.ScanConsistency
	if spec.Consistency {
		cons = datastore.SCAN_PLUS
	} else {
		cons = datastore.UNBOUNDED
	}

	conn, err := datastore.NewSizedIndexConnection(256, &perfContext{})
	if err != nil {
		log.Fatalf("error creating SizedIndexConnection: %v\n", err)
	}

	startTime := time.Now()
	go func() {
		uuid := fmt.Sprintf("%d", atomic.AddUint64(&requestCounter, 1))
		switch spec.Type {
		case "All":
			requestID := os.Args[0] + uuid
			index, err := nclient.IndexByName(spec.Index)
			if err != nil {
				log.Fatalf("unable to find Index %v\n", spec.Index)
			}
			rng := datastore.Range{Low: nil, High: nil, Inclusion: datastore.BOTH}
			span := &datastore.Span{Seek: nil, Range: rng}
			index.Scan(requestID, span, false, spec.Limit, cons, nil, conn)

		case "Range":
			requestID := os.Args[0] + uuid
			index, err := nclient.IndexByName(spec.Index)
			if err != nil {
				log.Fatalf("unable to find Index %v\n", spec.Index)
			}

			low, high := skey2qkey(spec.Low), skey2qkey(spec.High)
			rng := datastore.Range{
				Low: low, High: high,
				Inclusion: datastore.Inclusion(spec.Inclusion),
			}
			span := &datastore.Span{Seek: nil, Range: rng}
			index.Scan(requestID, span, false, spec.Limit, cons, nil, conn)

		case "Lookup":
			requestID := os.Args[0] + uuid

			index, err := nclient.IndexByName(spec.Index)
			if err != nil {
				log.Fatalf("unable to find Index %v\n", spec.Index)
			}

			if len(spec.Lookups) == 0 {
				log.Fatalf("specify lookup values [][]interface{}")
			}
			low, high := skey2qkey(spec.Lookups[0]), skey2qkey(spec.Lookups[0])
			rng := datastore.Range{Low: low, High: high, Inclusion: datastore.BOTH}
			span := &datastore.Span{Seek: nil, Range: rng}
			index.Scan(requestID, span, false, spec.Limit, cons, nil, conn)
		}
		if err != nil {
			errFn(err.Error())
		}
	}()

	for range conn.EntryChannel() {
		rows++
	}

	dur := time.Now().Sub(startTime)

	if result != nil {
		aggrQ <- &JobResult{
			job:  job,
			dur:  dur.Nanoseconds(),
			rows: rows,
		}
	}
}

func Worker(jobQ chan *Job, c datastore.Indexer, aggrQ chan *JobResult, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobQ {
		RunJob(c, job, aggrQ)
	}
}

func ResultAggregator(ch chan *JobResult, sw io.Writer, wg *sync.WaitGroup) {
	defer wg.Done()

	for jr := range ch {
		var lat int64
		result := jr.job.result
		spec := jr.job.spec
		result.Rows += uint64(jr.rows)
		result.Duration += jr.dur

		result.statsRows += uint64(jr.rows)
		result.statsDuration += jr.dur

		if jr.rows > 0 {
			lat = jr.dur / jr.rows
		}
		result.LatencyHisto.Add(lat)

		result.iter++
		if sw != nil && spec.NInterval > 0 &&
			(result.iter%spec.NInterval == 0 || result.iter == spec.Repeat+1) {
			fmt.Fprintf(sw, "id:%d, rows:%d, duration:%d, Nth-latency:%d\n",
				spec.Id, result.statsRows, result.statsDuration, jr.dur)
			result.statsRows = 0
			result.statsDuration = 0
		}
	}
}

func RunCommands(cluster string, cfg *Config, statsW io.Writer) (*Result, error) {
	t0 := time.Now()
	var result Result

	var aggrQ chan *JobResult
	var wg1, wg2 sync.WaitGroup
	var err error

	if len(cfg.LatencyBuckets) == 0 {
		cfg.LatencyBuckets = defaultLatencyBuckets
	}

	if cfg.ClientBootTime == 0 {
		cfg.ClientBootTime = clientBootTime
	}

	setSystemConfig(cfg)

	if cfg.Clients <= 0 {
		log.Fatalf("no. of clients cannot be ZERO !!")
	}
	clients := make(map[string][]datastore.Indexer)
	for _, spec := range cfg.ScanSpecs {
		clients[spec.Bucket] = make([]datastore.Indexer, 0, 16)
		for i := 0; i < cfg.Clients; i++ {
			c, err := nclient.NewGSIIndexer(cluster, "default", spec.Bucket)
			if err != nil {
				return nil, err
			}
			clients[spec.Bucket] = append(clients[spec.Bucket], c)
		}
	}

	time.Sleep(time.Second * time.Duration(cfg.ClientBootTime))

	jobQm := map[string]chan *Job{}
	aggrQ = make(chan *JobResult, cfg.Concurrency*1000)
	for _, spec := range cfg.ScanSpecs {
		jobQ := make(chan *Job, cfg.Concurrency*1000)
		jobQm[spec.Bucket] = jobQ
		for i := 0; i < cfg.Concurrency; i++ {
			wg1.Add(1)
			go Worker(jobQ, clients[spec.Bucket][i%cfg.Clients], aggrQ, &wg1)
		}
	}

	wg2.Add(1)
	go ResultAggregator(aggrQ, statsW, &wg2)

	for i, spec := range cfg.ScanSpecs {
		if spec.Id == 0 {
			spec.Id = uint64(i)
		}

		index, e := clients[spec.Bucket][0].IndexByName(spec.Index)
		if e != nil {
			log.Fatal("cannot find index %v\n", spec.Index)
		}
		spec.DefnId, err = strconv.ParseUint(index.Id(), 16, 64)
		if err != nil {
			log.Fatalf("cannot parse index id %v\n", index.Id())
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
		res.ErrorCount = 0
		res.LatencyHisto.Init(cfg.LatencyBuckets, hFn)
		res.Id = spec.Id
		result.ScanResults = append(result.ScanResults, res)
	}

	// warming up GsiClient
	for _, spec := range cfg.ScanSpecs {
		for _, client := range clients[spec.Bucket] {
			job := &Job{spec: spec, result: nil}
			RunJob(client, job, nil)
			break
		}
	}

	fmt.Println("GsiClients warmed up ...")
	result.WarmupDuration = float64(time.Since(t0).Nanoseconds()) / float64(time.Second)

	// Round robin scheduling of jobs
	var allFinished bool

loop:
	for {
		allFinished = true
		for i, spec := range cfg.ScanSpecs {
			if iter := atomic.LoadUint32(&spec.iteration); iter < spec.Repeat+1 {
				j := &Job{
					spec:   spec,
					result: result.ScanResults[i],
				}

				jobQm[spec.Bucket] <- j
				atomic.AddUint32(&spec.iteration, 1)
				allFinished = false
			}
		}

		if allFinished {
			break loop
		}
	}

	for _, spec := range cfg.ScanSpecs {
		close(jobQm[spec.Bucket])
	}
	wg1.Wait()
	close(aggrQ)
	wg2.Wait()

	return &result, err
}

func setSystemConfig(cfg *Config) {
	c.SystemConfig["queryport.client.settings.poolSize"] = c.ConfigValue{
		int(cfg.Concurrency),
		"number simultaneous active connections connections in a pool",
		int(cfg.Concurrency),
		true, // immutable
		false,
	}
	c.SystemConfig["queryport.client.readDeadline"] = c.ConfigValue{
		0,
		"timeout, in milliseconds, is timeout while reading from socket",
		0,
		true,
		false,
	}
	c.SystemConfig["queryport.client.writeDeadline"] = c.ConfigValue{
		0,
		"timeout, in milliseconds, is timeout while writing to socket",
		0,
		true,
		false,
	}
}

type perfContext struct{}

func (ctxt *perfContext) Error(err errors.Error) {
	fmt.Printf("Scan error: %v\n", err)
}

func (ctxt *perfContext) Warning(wrn errors.Error) {
	fmt.Printf("scan warning: %v\n", wrn)
}

func (ctxt *perfContext) Fatal(fatal errors.Error) {
	fmt.Printf("scan fatal: %v\n", fatal)
}

func (ctxt *perfContext) GetScanCap() int64 {
	return 512 // Default index scan request size
}

func (ctxt *perfContext) MaxParallelism() int {
	return 1
}

// RecordFtsRU added for Elixir
func (ctxt *perfContext) RecordFtsRU(ru tenant.Unit) {
}

// RecordGsiRU added for Elixir
func (ctxt *perfContext) RecordGsiRU(ru tenant.Unit) {
}

// RecordKvRU added for Elixir
func (ctxt *perfContext) RecordKvRU(ru tenant.Unit) {
}

// RecordKvWU added for Elixir
func (ctxt *perfContext) RecordKvWU(wu tenant.Unit) {
}

func (ctxt *perfContext) Credentials() *auth.Credentials {
	return nil
}

func (ctxt *perfContext) SkipKey(key string) bool {
	return false
}

func (ctxt *perfContext) GetReqDeadline() time.Time {
	return time.Time{}
}

func (ctxt *perfContext) TenantCtx() tenant.Context {
	return nil
}

func (ctxt *perfContext) SetFirstCreds(creds string) {
}

func (ctxt *perfContext) FirstCreds() (string, bool) {
	return "", true
}

func skey2qkey(skey c.SecondaryKey) value.Values {
	qkey := make(value.Values, 0, len(skey))
	for _, x := range skey {
		qkey = append(qkey, value.NewValue(x))
	}
	return qkey
}
