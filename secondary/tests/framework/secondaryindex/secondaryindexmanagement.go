package secondaryindex

import (
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbaselabs/query/expression"
	"github.com/couchbaselabs/query/parser/n1ql"
)

func CreateClient(server string) *qc.GsiClient {
	config := c.SystemConfig.SectionConfig("queryport.client.", true)
	client, err := qc.NewGsiClient(server, "2itest", config)
	tc.HandleError(err, "Error while creating gsi client")
	return client
}

func GetDefnID(client *qc.GsiClient, bucket, indexName string) (defnID c.IndexDefnId, ok bool) {
	indexes, err := client.Refresh()
	tc.HandleError(err, "Error while listing the indexes")
	for _, index := range indexes {
		defn := index.Definition
		if defn.Bucket == bucket && defn.Name == indexName {
			return index.Definition.DefnId, true
		}
	}
	return c.IndexDefnId(0), false
}

func CreateSecondaryIndex(indexName, bucketName, server string, indexFields []string, skipIfExists bool) error {
	indexExists := IndexExists(indexName, bucketName, server)
	if skipIfExists == true && indexExists == true {
		return nil
	}
	client := CreateClient(server)
	var secExprs []string

	for _, indexField := range indexFields {
		expr, err := n1ql.ParseExpression(indexField)
		if err != nil {
			fmt.Printf("Creating index %v. Error while parsing the expression (%v) : %v\n", indexName, indexField, err)
		}

		secExprs = append(secExprs, expression.NewStringer().Visit(expr))
	}

	using := "gsi"
	exprType := "N1QL"
	partnExp := ""
	where := ""
	isPrimary := false

	_, err := client.CreateIndex(indexName, bucketName, using, exprType, partnExp, where, secExprs, isPrimary)
	if err == nil {
		fmt.Printf("Created the secondary index %v\n", indexName)
	}

	client.Close()
	return err
}

func IndexExists(indexName, bucketName, server string) bool {
	client := CreateClient(server)
	indexes, err := client.Refresh()
	client.Close()
	tc.HandleError(err, "Error while listing the secondary indexes")
	for _, index := range indexes {
		defn := index.Definition
		if defn.Name == indexName {
			fmt.Printf("Index found:  %v\n", indexName)
			return true
		}
	}
	return false
}

func DropSecondaryIndex(indexName, bucketName, server string) error {
	fmt.Println("Dropping the secondary index ", indexName)
	client := CreateClient(server)
	indexes, err := client.Refresh()
	tc.HandleError(err, "Error while listing the secondary indexes")
	for _, index := range indexes {
		defn := index.Definition
		if (defn.Name == indexName) && (defn.Bucket == bucketName) {
			e := client.DropIndex(defn.DefnId)
			if e == nil {
				fmt.Println("Index dropped")
			} else {
				client.Close()
				return e
			}
		}
	}
	client.Close()
	return nil
}

func DropAllSecondaryIndexes(server string) {
	fmt.Println("In DropAllSecondaryIndexes()")
	client := CreateClient(server)
	indexes, err := client.Refresh()
	tc.HandleError(err, "Error while listing the secondary indexes")
	for _, index := range indexes {
		defn := index.Definition
		e := client.DropIndex(defn.DefnId)
		tc.HandleError(e, "Error dropping the index "+defn.Name)
		fmt.Println("Dropped index ", defn.Name)
	}
	client.Close()
}

func DropSecondaryIndexByID(indexDefnID uint64, server string) error {
	fmt.Println("Dropping the secondary index ", indexDefnID)
	client := CreateClient(server)
	e := client.DropIndex(c.IndexDefnId(indexDefnID))
	if e != nil {
		return e
	}
	client.Close()
	fmt.Println("Index dropped")
	return nil
}
