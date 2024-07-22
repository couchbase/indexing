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

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	codebook "github.com/couchbase/indexing/secondary/vector/codebook"
	faiss "github.com/couchbase/indexing/secondary/vector/faiss"
)

type MetricType int

const (
	METRIC_L2 MetricType = iota
	METRIC_INNER_PRODUCT
)

func (m MetricType) String() string {

	switch m {

	case METRIC_L2:
		return "L2"
	case METRIC_INNER_PRODUCT:
		return "INNER_PRODUCT"
	}

	return ""
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

func convertToFaissMetric(metric MetricType) int {

	switch metric {
	case METRIC_L2:
		return faiss.MetricL2
	case METRIC_INNER_PRODUCT:
		return faiss.MetricInnerProduct

	}
	//default to L2
	return faiss.MetricL2
}

func convertSimilarityToMetric(similarity common.VectorSimilarity) (MetricType, bool) {
	switch similarity {
	case common.EUCLIDEAN_SQUARED, common.L2_SQUARED, common.EUCLIDEAN, common.L2:
		return METRIC_L2, false // Default to L2
	case common.DOT:
		return METRIC_INNER_PRODUCT, false
	case common.COSINE:
		return METRIC_INNER_PRODUCT, true
	}
	return METRIC_L2, false // Always default to L2
}

func NewCodebook(vectorMeta *common.VectorMetadata, nlist int) (codebook codebook.Codebook, err error) {
	metric, useCosine := convertSimilarityToMetric(vectorMeta.Similarity)
	switch vectorMeta.Quantizer.Type {
	case common.PQ:
		nsub :=  vectorMeta.Quantizer.SubQuantizers
		if vectorMeta.Quantizer.FastScan {
			nsub =  vectorMeta.Quantizer.BlockSize
		}
		codebook, err = NewCodebookIVFPQ(vectorMeta.Dimension, nsub, vectorMeta.Quantizer.Nbits,
			nlist, metric, useCosine, vectorMeta.Quantizer.FastScan)
		if err != nil {
			return nil, err
		}
		logging.Infof("NewCodebookIVFPQ: Initialized codebook with dimension: %v, subquantizers: %v, "+
			"nbits: %v, nlist: %v, fastScan: %v, metric: %v, useCosine: %v", vectorMeta.Dimension, nsub,
			vectorMeta.Quantizer.Nbits, nlist, vectorMeta.Quantizer.FastScan, metric, useCosine)

	case common.SQ:
		codebook, err = NewCodebookIVFSQ(vectorMeta.Dimension, nlist, vectorMeta.Quantizer.SQRange, metric, useCosine)
		if err != nil {
			return nil, err
		}
		logging.Infof("NewCodebookIVFSQ: Initialized codebook with dimension: %v, range: %v, nlist: %v, "+
		 	"metric: %v, useCosine: %v",
			vectorMeta.Dimension, vectorMeta.Quantizer.SQRange, nlist, metric, useCosine)

	default:
		return nil, errors.New("Unsupported quantisation type")
	}

	return codebook, nil
}
