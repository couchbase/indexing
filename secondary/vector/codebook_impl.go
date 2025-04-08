// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package vector

import (
	"errors"
	"os"
	"runtime"
	"strconv"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	codebook "github.com/couchbase/indexing/secondary/vector/codebook"
	faiss "github.com/couchbase/indexing/secondary/vector/faiss"
)

func init() {
	faiss.SetOMPThreads(defaultOMPThreads)
	var numCores = runtime.GOMAXPROCS(-1)
	os.Setenv("OMP_THREAD_LIMIT", strconv.Itoa(numCores))
	os.Setenv("OMP_WAIT_POLICY", defaultWaitPolicy)
}

type CodebookVer int

const (
	CodebookVer1 = iota
)

func RecoverCodebook(data []byte, qType string) (codebook.Codebook, error) {

	switch qType {
	case "PQ":
		return recoverCodebookIVFPQ(data)
	case "SQ":
		return recoverCodebookIVFSQ(data)
	}

	return nil, codebook.ErrUnknownType
}

func convertToFaissMetric(metric codebook.MetricType) int {

	switch metric {
	case codebook.METRIC_L2:
		return faiss.MetricL2
	case codebook.METRIC_INNER_PRODUCT:
		return faiss.MetricInnerProduct

	}
	//default to L2
	return faiss.MetricL2
}

func NewCodebook(vectorMeta *common.VectorMetadata, nlist int) (cb codebook.Codebook, err error) {
	metric, useCosine := codebook.ConvertSimilarityToMetric(vectorMeta.Similarity)
	switch vectorMeta.Quantizer.Type {
	case common.PQ:
		nsub := vectorMeta.Quantizer.SubQuantizers
		if vectorMeta.Quantizer.FastScan {
			nsub = vectorMeta.Quantizer.BlockSize
		}
		cb, err = NewCodebookIVFPQ(vectorMeta.Dimension, nsub, vectorMeta.Quantizer.Nbits,
			nlist, metric, useCosine, vectorMeta.Quantizer.FastScan)
		if err != nil {
			return nil, err
		}
		logging.Infof("NewCodebookIVFPQ: Initialized codebook with dimension: %v, subquantizers: %v, "+
			"nbits: %v, nlist: %v, fastScan: %v, metric: %v, useCosine: %v", vectorMeta.Dimension, nsub,
			vectorMeta.Quantizer.Nbits, nlist, vectorMeta.Quantizer.FastScan, metric, useCosine)

	case common.SQ:
		cb, err = NewCodebookIVFSQ(vectorMeta.Dimension, nlist, vectorMeta.Quantizer.SQRange, metric, useCosine)
		if err != nil {
			return nil, err
		}
		logging.Infof("NewCodebookIVFSQ: Initialized codebook with dimension: %v, range: %v, nlist: %v, "+
			"metric: %v, useCosine: %v",
			vectorMeta.Dimension, vectorMeta.Quantizer.SQRange, nlist, metric, useCosine)

	default:
		return nil, errors.New("Unsupported quantisation type")
	}

	return cb, nil
}
