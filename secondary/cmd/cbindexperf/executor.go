package main

import (
	c "github.com/couchbase/indexing/secondary/common"
	qclient "github.com/couchbase/indexing/secondary/queryport/client"
	"time"
)

func RunScan(client *qclient.GsiClient,
	spec *ScanConfig) *ScanResult {
	var err error
	var result ScanResult

	result.Id = spec.Id

	callb := func(res qclient.ResponseReader) bool {
		if res.Error() != nil {
			result.Error = res.Error().Error()
			return false
		} else {
			_, pkeys, err := res.GetEntries()
			if err != nil {
				result.Error = err.Error()
				return false
			}

			result.Rows += uint64(len(pkeys))
		}

		return true
	}

	startTime := time.Now()
loop:
	for i := 0; i < spec.Repeat+1; i++ {
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
			result.Error = err.Error()
			break loop
		}
	}

	dur := time.Now().Sub(startTime)
	result.Duration = dur.Nanoseconds()

	return &result
}

func RunCommands(cluster string, cfg *Config) (*Result, error) {
	var result Result

	config := c.SystemConfig.SectionConfig("queryport.client.", true)
	client, err := qclient.NewGsiClient(cluster, config)
	if err != nil {
		return nil, err
	}

	indexes, err := client.Refresh()
	if err != nil {
		return nil, err
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

		res := RunScan(client, spec)
		result.ScanResults = append(result.ScanResults, res)
	}

	client.Close()

	return &result, err
}
