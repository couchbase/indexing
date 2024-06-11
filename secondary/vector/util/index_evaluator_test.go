package vectorutil

import (
	"testing"

	"github.com/couchbase/indexing/secondary/common"
	c "github.com/couchbase/indexing/secondary/common"
)

func SkipTestFetchSampleVectorsForIndexes(t *testing.T) {

	cluster := "127.0.0.1:8091"
	pool := "default"
	bucket := "default"
	scope := "_default"
	collection := "_default"
	cid := "0"

	meta := &c.VectorMetadata{
		IsCompositeIndex: true,
		Dimension:        128,
	}

	idxDefn := c.IndexDefn{
		DefnId:        c.IndexDefnId(200),
		Name:          "index_evaluator",
		Using:         common.PlasmaDB,
		Bucket:        bucket,
		IsPrimary:     false,
		SecExprs:      []string{"description"},
		ExprType:      c.N1QL,
		IsVectorIndex: true,
		HasVectorAttr: []bool{true},
		Scope:         scope,
		Collection:    collection,
		VectorMeta:    meta,
	}

	indexInst := &common.IndexInst{
		InstId: c.IndexInstId(300),
		Defn:   idxDefn,
	}

	indexInsts := []*c.IndexInst{indexInst}

	vectors, err := FetchSampleVectorsForIndexes(cluster, pool, bucket, scope, collection, cid, indexInsts, 800)

	t.Logf("Vectors %v, Err %v", len(vectors[0]), err)

	t.Logf("Sample Vector %v", vectors[0][0])
}
