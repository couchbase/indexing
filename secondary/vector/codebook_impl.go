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

func convertSimilarityToMetric(similarity common.VectorSimilarity) MetricType {
	switch similarity {
	case common.EUCLIDEAN_SQUARED, common.L2_SQUARED, common.EUCLIDEAN, common.L2:
		return METRIC_L2 // Default to L2
	case common.DOT, common.COSINE:
		return METRIC_INNER_PRODUCT
	}
	return METRIC_L2 // Always default to L2
}

func NewCodebook(vectorMeta *common.VectorMetadata, nlist int) (codebook.Codebook, error) {
	metric := convertSimilarityToMetric(vectorMeta.Similarity)
	switch vectorMeta.Quantizer.Type {
	case common.PQ:

		codebook, err := NewCodebookIVFPQ(vectorMeta.Dimension, vectorMeta.Quantizer.SubQuantizers,
			vectorMeta.Quantizer.Nbits, nlist, metric)
		if err != nil {
			return nil, err
		}
		logging.Infof("NewCodebookIVFPQ: Initialized codebook with dimension: %v, subquantizers: %v, "+
			"nbits: %v, nlist: %v, metric: %v", vectorMeta.Dimension, vectorMeta.Quantizer.SubQuantizers,
			vectorMeta.Quantizer.Nbits, nlist, metric)
		return codebook, nil
	case common.SQ:
		return nil, errors.New("SQ interface is not yet supported")
	}
	return nil, errors.New("Unsupported quantisation type")
}
