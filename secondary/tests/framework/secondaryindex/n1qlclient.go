package secondaryindex

import (
	nclient "github.com/couchbase/indexing/secondary/queryport/n1ql"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/parser"
	"log"
)

// Creates an index and waits for it to become active
func N1QLCreateSecondaryIndex(
	indexName, bucketName, server, whereExpr string, indexFields []string, isPrimary bool, with []byte,
	skipIfExists bool, indexActiveTimeoutSeconds int64) error {

	log.Printf("N1QLCreateSecondaryIndex :: server = %v", server)
	nc, err := nclient.NewGSIIndexer(server, "default", bucketName)
	requestId := "12345"
	exprs := make(expression.Expressions, 0, len(indexFields))
	for _, exprS := range indexFields {
		expr, _ := parser.Parse(exprS)
		exprs = append(exprs, expr)
	}
	rangeKey := exprs

	_, err = nc.CreateIndex(requestId, indexName, nil, rangeKey, nil, nil)
	if err != nil {
		return err
	}
	return nil
}
