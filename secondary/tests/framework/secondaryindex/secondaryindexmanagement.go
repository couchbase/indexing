package secondaryindex

import (
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbaselabs/query/expression"
	"github.com/couchbaselabs/query/parser/n1ql"
)

func CreateClient(server, serviceAddr string) *qc.GsiClient {
	config := c.SystemConfig.SectionConfig("queryport.client.", true)
	client, err := qc.NewGsiClient(server, serviceAddr, config)
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

func CreatePrimaryIndex(indexName, bucketName, server string, skipIfExists bool) error {
	indexExists := IndexExists(indexName, bucketName, server)
	if skipIfExists == true && indexExists == true {
		return nil
	}
	client := CreateClient(server, "2itest")
	var secExprs []string

	using := "gsi"
	exprType := "N1QL"
	partnExp := ""
	where := ""
	isPrimary := true

	_, err := client.CreateIndex(indexName, bucketName, using, exprType, partnExp, where, secExprs, isPrimary, nil)
	if err == nil {
		fmt.Printf("Created the gsi primary index %v\n", indexName)
	}

	client.Close()
	return err
}

func CreateSecondaryIndex(indexName, bucketName, server string, indexFields []string, skipIfExists bool) error {
	indexExists := IndexExists(indexName, bucketName, server)
	if skipIfExists == true && indexExists == true {
		return nil
	}
	client := CreateClient(server, "2itest")
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

	_, err := client.CreateIndex(indexName, bucketName, using, exprType, partnExp, where, secExprs, isPrimary, nil)
	if err == nil {
		fmt.Printf("Created the secondary index %v\n", indexName)
	}

	client.Close()
	return err
}

func CreateSecondaryIndexWithClient(indexName, bucketName, server string, indexFields []string, skipIfExists bool, client *qc.GsiClient) error {
	indexExists := IndexExistsWithClient(indexName, bucketName, server, client)
	if skipIfExists == true && indexExists == true {
		return nil
	}
	
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

	_, err := client.CreateIndex(indexName, bucketName, using, exprType, partnExp, where, secExprs, isPrimary, nil)
	if err == nil {
		fmt.Printf("Created the secondary index %v\n", indexName)
	}
	return err
}

func IndexExists(indexName, bucketName, server string) bool {
	client := CreateClient(server, "2itest")
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

func IndexExistsWithClient(indexName, bucketName, server string, client *qc.GsiClient) bool {
	indexes, err := client.Refresh()
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
	client := CreateClient(server, "2itest")
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

func DropSecondaryIndexWithClient(indexName, bucketName, server string, client *qc.GsiClient) error {
	fmt.Println("Dropping the secondary index ", indexName)
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
	return nil
}

func DropAllSecondaryIndexes(server string) {
	fmt.Println("In DropAllSecondaryIndexes()")
	client := CreateClient(server, "2itest")
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
	client := CreateClient(server, "2itest")
	e := client.DropIndex(c.IndexDefnId(indexDefnID))
	if e != nil {
		return e
	}
	client.Close()
	fmt.Println("Index dropped")
	return nil
}
