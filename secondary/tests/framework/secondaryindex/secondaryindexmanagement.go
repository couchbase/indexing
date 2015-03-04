package secondaryindex

import (
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
		log.Printf("Error while creating gsi client: ", err)
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
		log.Printf("Created the gsi primary index %v", indexName)
		return WaitTillIndexActive(defnID, client, 900)
	}

	client.Close()
	return err
}

func CreateSecondaryIndex(indexName, bucketName, server string, indexFields []string, skipIfExists bool, indexActiveTimeoutSeconds int64) error {
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return e
	}
	
	defer client.Close()
	return CreateSecondaryIndexWithClient(indexName, bucketName, server, indexFields, skipIfExists, client, indexActiveTimeoutSeconds)
}

func CreateSecondaryIndexWithClient(indexName, bucketName, server string, indexFields []string, skipIfExists bool, client *qc.GsiClient, indexActiveTimeoutSeconds int64) error {
	indexExists := IndexExistsWithClient(indexName, bucketName, server, client)
	if skipIfExists == true && indexExists == true {
		return nil
	}

	var secExprs []string
	for _, indexField := range indexFields {
		expr, err := n1ql.ParseExpression(indexField)
		if err != nil {
			log.Printf("Creating index %v. Error while parsing the expression (%v) : %v", indexName, indexField, err)
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
		log.Printf("Created the secondary index %v", indexName)
		return WaitTillIndexActive(defnID, client, indexActiveTimeoutSeconds)
	}

	return err
}

func WaitTillIndexActive(defnID uint64, client *qc.GsiClient, indexActiveTimeoutSeconds int64) error {
	start := time.Now()
	for {
		elapsed := time.Since(start)
		if elapsed.Seconds() >= float64(indexActiveTimeoutSeconds) {
			break
		}
		state, e := client.IndexState(defnID)
		if e != nil {
			log.Printf("Error while fetching index state for defnID %v", defnID)
			return e
		}

		if state == c.INDEX_STATE_ACTIVE {
			return nil
		} else {
			time.Sleep(1 * time.Second)
		}
	}
	return nil
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
			log.Printf("Index found:  %v", indexName)
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
			log.Printf("Index found:  %v", indexName)
			return true
		}
	}
	return false
}

func DropSecondaryIndex(indexName, bucketName, server string) error {
	log.Printf("Dropping the secondary index %v", indexName)
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
				log.Printf("Index dropped")
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
	log.Printf("Dropping the secondary index %v", indexName)
	indexes, err := client.Refresh()
	tc.HandleError(err, "Error while listing the secondary indexes")
	for _, index := range indexes {
		defn := index.Definition
		if (defn.Name == indexName) && (defn.Bucket == bucketName) {
			e := client.DropIndex(uint64(defn.DefnId))
			if e == nil {
				log.Printf("Index dropped")
			} else {
				client.Close()
				return e
			}
		}
	}
	return nil
}

func DropAllSecondaryIndexes(server string) error {
	log.Printf("In DropAllSecondaryIndexes()")
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
		log.Printf("Dropped index %v", defn.Name)
	}
	client.Close()
	return nil
}

func DropSecondaryIndexByID(indexDefnID uint64, server string) error {
	log.Printf("Dropping the secondary index %v", indexDefnID)
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return e
	}

	e = client.DropIndex(indexDefnID)
	if e != nil {
		return e
	}
	client.Close()
	log.Printf("Index dropped")
	return nil
}
