package secondaryindex

import (
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/parser/n1ql"
	"log"
	"time"
)

func CreateClient(server, serviceAddr string) (*qc.GsiClient, error) {
	config := c.SystemConfig.SectionConfig("queryport.client.", true)
	client, err := qc.NewGsiClient(server, config)
	if err != nil {
		log.Println("Error while creating gsi client: ", err)
		return nil, err
	}

	return client, nil
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
	indexExists, e := IndexExists(indexName, bucketName, server)
	if e != nil {
		return e
	}

	if skipIfExists == true && indexExists == true {
		return nil
	}
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return e
	}

	var secExprs []string

	using := "gsi"
	exprType := "N1QL"
	partnExp := ""
	where := ""
	isPrimary := true

	defnID, err := client.CreateIndex(indexName, bucketName, using, exprType, partnExp, where, secExprs, isPrimary, nil)
	if err == nil {
		fmt.Printf("Created the gsi primary index %v\n", indexName)
		return WaitTillIndexActive(defnID, client)
	}

	client.Close()
	return err
}

func CreateSecondaryIndex(indexName, bucketName, server string, indexFields []string, skipIfExists bool) error {
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return e
	}

	defer client.Close()
	return CreateSecondaryIndexWithClient(indexName, bucketName, server, indexFields, skipIfExists, client)
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

	defnID, err := client.CreateIndex(indexName, bucketName, using, exprType, partnExp, where, secExprs, isPrimary, nil)
	if err == nil {
		fmt.Printf("Created the secondary index %v\n", indexName)
		return WaitTillIndexActive(defnID, client)
	}

	return err
}

func WaitTillIndexActive(defnID uint64, client *qc.GsiClient) error {
	for {
		state, e := client.IndexState(defnID)
		if e != nil {
			fmt.Println("Error while fetching index state for defnID ", defnID)
			return e
		}

		if state == c.INDEX_STATE_ACTIVE {
			return nil
		} else {
			time.Sleep(1 * time.Second)
		}
	}
}

func IndexExists(indexName, bucketName, server string) (bool, error) {
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return false, e
	}

	indexes, err := client.Refresh()
	client.Close()
	tc.HandleError(err, "Error while listing the secondary indexes")
	for _, index := range indexes {
		defn := index.Definition
		if defn.Name == indexName {
			fmt.Printf("Index found:  %v\n", indexName)
			return true, nil
		}
	}
	return false, nil
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
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return e
	}

	indexes, err := client.Refresh()
	tc.HandleError(err, "Error while listing the secondary indexes")
	for _, index := range indexes {
		defn := index.Definition
		if (defn.Name == indexName) && (defn.Bucket == bucketName) {
			e := client.DropIndex(uint64(defn.DefnId))
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
			e := client.DropIndex(uint64(defn.DefnId))
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

func DropAllSecondaryIndexes(server string) error {
	fmt.Println("In DropAllSecondaryIndexes()")
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return e
	}

	indexes, err := client.Refresh()
	tc.HandleError(err, "Error while listing the secondary indexes")
	for _, index := range indexes {
		defn := index.Definition
		e := client.DropIndex(uint64(defn.DefnId))
		if e != nil {
			return e
		}
		fmt.Println("Dropped index ", defn.Name)
	}
	client.Close()
	return nil
}

func DropSecondaryIndexByID(indexDefnID uint64, server string) error {
	fmt.Println("Dropping the secondary index ", indexDefnID)
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return e
	}

	e = client.DropIndex(indexDefnID)
	if e != nil {
		return e
	}
	client.Close()
	fmt.Println("Index dropped")
	return nil
}
