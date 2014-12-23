package secondaryindex

import (
	"fmt"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbaselabs/query/expression"
	"github.com/couchbaselabs/query/parser/n1ql"
)

func CreateSecondaryIndex(indexName, bucketName, server string, indexFields []string, skipIfExists bool) error {
	indexExists := IndexExists(indexName, bucketName, server)
	if skipIfExists == true && indexExists == true {
		return nil
	}
	client := qc.NewClusterClient(server)
	var secExprs []string

	for _, indexField := range indexFields {
		expr, err := n1ql.ParseExpression(indexField)
		if err != nil {
			fmt.Printf("Creating index %v. Error while parsing the expression (%v) : %v\n", indexName, indexField, err)
		}

		secExprs = append(secExprs, expression.NewStringer().Visit(expr))
	}

	using := "lsm"
	exprType := "N1QL"
	partnExp := ""
	where := ""
	isPrimary := false

	_, err := client.CreateIndex(indexName, bucketName, using, exprType, partnExp, where, secExprs, isPrimary)
	if err == nil {
		fmt.Printf("Created the secondary index %v\n", indexName)
	}
	return err
}

func IndexExists(indexName, bucketName, server string) bool {
	client := qc.NewClusterClient(server)
	infos, err := client.List()
	tc.HandleError(err, "Error while listing the secondary indexes")
	for _, info := range infos {
		if info.Name == indexName {
			fmt.Printf("Index found:  %v\n", indexName)
			return true
		}
	}
	return false
}

func DropSecondaryIndex(indexName, bucketName, server string) error {
	fmt.Println("Dropping the secondary index ", indexName)
	client := qc.NewClusterClient(server)
	infos, err := client.List()
	tc.HandleError(err, "Error while listing the secondary indexes")

	for _, info := range infos {
		if (info.Name == indexName) && (info.Bucket == bucketName) {
			e := client.DropIndex(info.DefnID)
			if e == nil {
				fmt.Println("Index dropped")
			} else {
				return e
			}
		}
	}
	return nil
}

func DropSecondaryIndexByID(indexDefnID, server string) error {
	fmt.Println("Dropping the secondary index ", indexDefnID)
	client := qc.NewClusterClient(server)
	e := client.DropIndex(indexDefnID)
	if e != nil {
		return e
	}
	fmt.Println("Index dropped")
	return nil
}

func DropAllSecondaryIndexes(server string) {
	fmt.Println("In DropAllSecondaryIndexes()")
	client := qc.NewClusterClient(server)
	infos, err := client.List()
	tc.HandleError(err, "Error while listing the secondary indexes")

	for _, info := range infos {
		e := client.DropIndex(info.DefnID)
		tc.HandleError(e, "Error dropping the index "+info.Name)
		fmt.Println("Dropped index ", info.Name)
	}
}
