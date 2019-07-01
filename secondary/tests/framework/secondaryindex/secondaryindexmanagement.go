package secondaryindex

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"path"
	"path/filepath"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/natsort"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/parser/n1ql"
)

var IndexUsing = "gsi"

func CreateClient(server, serviceAddr string) (*qc.GsiClient, error) {
	config := c.SystemConfig.SectionConfig("queryport.client.", true)
	client, err := qc.NewGsiClient(server, config)
	if err != nil {
		log.Printf("Error while creating gsi client: ", err)
		return nil, err
	}

	return client, nil
}

func GetDefnID(client *qc.GsiClient, bucket, indexName string) (defnID uint64, ok bool) {
	indexes, _, _, err := client.Refresh()
	tc.HandleError(err, "Error while listing the indexes")
	for _, index := range indexes {
		defn := index.Definition
		if defn.Bucket == bucket && defn.Name == indexName {
			return uint64(index.Definition.DefnId), true
		}
	}
	return uint64(c.IndexDefnId(0)), false
}

// Creates an index and waits for it to become active
func CreateSecondaryIndex(
	indexName, bucketName, server, whereExpr string, indexFields []string, isPrimary bool, with []byte,
	skipIfExists bool, indexActiveTimeoutSeconds int64, client *qc.GsiClient) error {

	if client == nil {
		c, e := CreateClient(server, "2itest")
		if e != nil {
			return e
		}
		client = c
		defer client.Close()
	}

	indexExists := IndexExistsWithClient(indexName, bucketName, server, client)
	if skipIfExists == true && indexExists == true {
		return nil
	}
	var secExprs []string
	if isPrimary == false {
		for _, indexField := range indexFields {
			expr, err := n1ql.ParseExpression(indexField)
			if err != nil {
				log.Printf("Creating index %v. Error while parsing the expression (%v) : %v", indexName, indexField, err)
			}

			secExprs = append(secExprs, expression.NewStringer().Visit(expr))
		}
	}
	exprType := "N1QL"
	partnExp := ""

	start := time.Now()
	defnID, err := client.CreateIndex(indexName, bucketName, IndexUsing, exprType, partnExp, whereExpr, secExprs, isPrimary, with)
	if err == nil {
		log.Printf("Created the secondary index %v. Waiting for it become active", indexName)
		e := WaitTillIndexActive(defnID, client, indexActiveTimeoutSeconds)
		if e != nil {
			return e
		} else {
			elapsed := time.Since(start)
			tc.LogPerfStat("CreateAndBuildIndex", elapsed)
			return nil
		}
	}

	return err
}

// Creates an index and waits for it to become active
func CreateSecondaryIndex2(
	indexName, bucketName, server, whereExpr string, indexFields []string, desc []bool, isPrimary bool, with []byte,
	partnScheme c.PartitionScheme, partnKeys []string, skipIfExists bool, indexActiveTimeoutSeconds int64,
	client *qc.GsiClient) error {

	if client == nil {
		c, e := CreateClient(server, "2itest")
		if e != nil {
			return e
		}
		client = c
		defer client.Close()
	}

	indexExists := IndexExistsWithClient(indexName, bucketName, server, client)
	if skipIfExists == true && indexExists == true {
		return nil
	}
	var secExprs []string
	if isPrimary == false {
		for _, indexField := range indexFields {
			expr, err := n1ql.ParseExpression(indexField)
			if err != nil {
				log.Printf("Creating index %v. Error while parsing the expression (%v) : %v", indexName, indexField, err)
			}

			secExprs = append(secExprs, expression.NewStringer().Visit(expr))
		}
	}
	exprType := "N1QL"

	start := time.Now()
	defnID, err := client.CreateIndex3(indexName, bucketName, IndexUsing, exprType, whereExpr, secExprs, desc, isPrimary,
		partnScheme, partnKeys, with)
	if err == nil {
		log.Printf("Created the secondary index %v. Waiting for it become active", indexName)
		e := WaitTillIndexActive(defnID, client, indexActiveTimeoutSeconds)
		if e != nil {
			return e
		} else {
			elapsed := time.Since(start)
			tc.LogPerfStat("CreateAndBuildIndex", elapsed)
			return nil
		}
	}

	return err
}

// Creates an index and DOES NOT wait for it to become active
func CreateSecondaryIndexAsync(
	indexName, bucketName, server, whereExpr string, indexFields []string, isPrimary bool, with []byte,
	skipIfExists bool, client *qc.GsiClient) error {

	if client == nil {
		c, e := CreateClient(server, "2itest")
		if e != nil {
			return e
		}
		client = c
		defer client.Close()
	}

	indexExists := IndexExistsWithClient(indexName, bucketName, server, client)
	if skipIfExists == true && indexExists == true {
		return nil
	}
	var secExprs []string
	if isPrimary == false {
		for _, indexField := range indexFields {
			expr, err := n1ql.ParseExpression(indexField)
			if err != nil {
				log.Printf("Creating index %v. Error while parsing the expression (%v) : %v", indexName, indexField, err)
			}

			secExprs = append(secExprs, expression.NewStringer().Visit(expr))
		}
	}
	exprType := "N1QL"
	partnExp := ""

	_, err := client.CreateIndex(indexName, bucketName, IndexUsing, exprType, partnExp, whereExpr, secExprs, isPrimary, with)
	if err == nil {
		log.Printf("Created the secondary index %v", indexName)
		return nil
	}
	return err
}

// Todo: Remove this function and update functional tests to use BuildIndexes
func BuildIndex(indexName, bucketName, server string, indexActiveTimeoutSeconds int64) error {
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return e
	}
	defer client.Close()

	defnID, _ := GetDefnID(client, bucketName, indexName)

	start := time.Now()
	err := client.BuildIndexes([]uint64{defnID})
	time.Sleep(2 * time.Second) // This wait is required for index state to get updated from error to initial, for example

	if err == nil {
		log.Printf("Build the deferred index %v. Waiting for the index to become active", indexName)
		e := WaitTillIndexActive(defnID, client, indexActiveTimeoutSeconds)
		if e != nil {
			return e
		} else {
			elapsed := time.Since(start)
			tc.LogPerfStat("BuildIndex", elapsed)
			return nil
		}
	}

	return err
}

func BuildIndexes(indexNames []string, bucketName, server string, indexActiveTimeoutSeconds int64) error {
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return e
	}
	defer client.Close()
	defnIds := make([]uint64, len(indexNames))
	for i := range indexNames {
		defnIds[i], _ = GetDefnID(client, bucketName, indexNames[i])
	}
	err := client.BuildIndexes(defnIds)
	log.Printf("Build command issued for the deferred indexes %v", indexNames)

	if err == nil {
		for i := range indexNames {
			log.Printf("Waiting for the index %v to become active", indexNames[i])
			e := WaitTillIndexActive(defnIds[i], client, indexActiveTimeoutSeconds)
			if e != nil {
				return e
			}
		}
	}
	return err
}

func BuildIndexesAsync(defnIds []uint64, server string, indexActiveTimeoutSeconds int64) error {
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return e
	}
	defer client.Close()

	err := client.BuildIndexes(defnIds)
	log.Printf("Build command issued for the deferred indexes %v", defnIds)
	return err
}

func WaitTillIndexActive(defnID uint64, client *qc.GsiClient, indexActiveTimeoutSeconds int64) error {
	start := time.Now()
	for {
		elapsed := time.Since(start)
		if elapsed.Seconds() >= float64(indexActiveTimeoutSeconds) {
			err := errors.New(fmt.Sprintf("Index did not become active after %d seconds", indexActiveTimeoutSeconds))
			return err
		}
		state, _ := client.IndexState(defnID)

		if state == c.INDEX_STATE_ACTIVE {
			log.Printf("Index is now active")
			return nil
		} else {
			log.Printf("Waiting for index to go active ...")
			time.Sleep(1 * time.Second)
		}
	}
	return nil
}

func WaitTillAllIndexNodesActive(server string, indexerActiveTimeoutSeconds int64) error {
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return e
	}

	start := time.Now()
	for {
		elapsed := time.Since(start)
		if elapsed.Seconds() >= float64(indexerActiveTimeoutSeconds) {
			err := errors.New(fmt.Sprintf("Indexer(s) did not become online after %d seconds", indexerActiveTimeoutSeconds))
			return err
		}
		indexers, e := client.Nodes()
		if e != nil {
			log.Printf("Error while fetching Nodes() %v", e)
			return e
		}

		allIndexersActive := true
		for _, indexer := range indexers {
			if indexer.Status != "online" {
				allIndexersActive = false
			}
		}

		if allIndexersActive == true {
			log.Printf("All indexers are active")
			return nil
		}
	}
	return nil
}

func IndexState(indexName, bucketName, server string) (string, error) {
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return "", e
	}
	defer client.Close()

	defnID, _ := GetDefnID(client, bucketName, indexName)
	state, e := client.IndexState(defnID)
	if e != nil {
		log.Printf("Error while fetching index state for defnID %v", defnID)
		return "", e
	}

	return state.String(), nil
}

func IndexExists(indexName, bucketName, server string) (bool, error) {
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return false, e
	}
	defer client.Close()

	indexes, _, _, err := client.Refresh()
	tc.HandleError(err, "Error while listing the secondary indexes")
	for _, index := range indexes {
		defn := index.Definition
		if defn.Name == indexName && defn.Bucket == bucketName {
			log.Printf("Index found:  %v", indexName)
			return true, nil
		}
	}
	return false, nil
}

func IndexExistsWithClient(indexName, bucketName, server string, client *qc.GsiClient) bool {
	indexes, _, _, err := client.Refresh()
	tc.HandleError(err, "Error while listing the secondary indexes")
	for _, index := range indexes {
		defn := index.Definition
		if defn.Name == indexName && defn.Bucket == bucketName {
			log.Printf("Index found:  %v", indexName)
			return true
		}
	}
	return false
}

func ListAllSecondaryIndexes(header string, client *qc.GsiClient) error {
	indexes, _, _, err := client.Refresh()
	if err != nil {
		log.Printf("ListAllSecondaryIndexes() %v: Error from client.Refresh(): %v ", header, err)
		return err
	}

	for _, index := range indexes {
		defn := index.Definition
		log.Printf("ListAllSecondaryIndexes() for %v: Index %v Bucket %v", header, defn.Name, defn.Bucket)
	}
	return nil
}

func DropSecondaryIndex(indexName, bucketName, server string) error {
	log.Printf("Dropping the secondary index %v", indexName)
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return e
	}
	defer client.Close()

	indexes, _, _, err := client.Refresh()
	tc.HandleError(err, "Error while listing the secondary indexes")
	for _, index := range indexes {
		defn := index.Definition
		if (defn.Name == indexName) && (defn.Bucket == bucketName) {
			start := time.Now()
			e := client.DropIndex(uint64(defn.DefnId))
			elapsed := time.Since(start)
			if e == nil {
				log.Printf("Index dropped")
				tc.LogPerfStat("DropIndex", elapsed)
			} else {
				return e
			}
		}
	}
	return nil
}

func DropSecondaryIndexWithClient(indexName, bucketName, server string, client *qc.GsiClient) error {
	log.Printf("Dropping the secondary index %v", indexName)
	indexes, _, _, err := client.Refresh()
	tc.HandleError(err, "Error while listing the secondary indexes")
	for _, index := range indexes {
		defn := index.Definition
		if (defn.Name == indexName) && (defn.Bucket == bucketName) {
			start := time.Now()
			e := client.DropIndex(uint64(defn.DefnId))
			elapsed := time.Since(start)
			if e == nil {
				log.Printf("Index dropped")
				tc.LogPerfStat("DropIndex", elapsed)
			} else {
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
	defer client.Close()

	indexes, _, _, err := client.Refresh()
	tc.HandleError(err, "Error while listing the secondary indexes")
	for _, index := range indexes {
		defn := index.Definition
		exists := IndexExistsWithClient(defn.Name, defn.Bucket, server, client)
		if exists {
			e := client.DropIndex(uint64(defn.DefnId))
			if e != nil {
				return e
			}
			log.Printf("Dropped index %v", defn.Name)
		}
	}
	return nil
}

func DropSecondaryIndexByID(indexDefnID uint64, server string) error {
	log.Printf("Dropping the secondary index %v", indexDefnID)
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return e
	}
	defer client.Close()

	e = client.DropIndex(indexDefnID)
	if e != nil {
		return e
	}
	log.Printf("Index dropped")
	return nil
}

func BuildAllSecondaryIndexes(server string, indexActiveTimeoutSeconds int64) error {
	log.Printf("In BuildAllSecondaryIndexes()")
	client, e := CreateClient(server, "2itest")
	if e != nil {
		return e
	}
	defer client.Close()

	indexes, _, _, err := client.Refresh()
	tc.HandleError(err, "Error while listing the secondary indexes")
	for i, index := range indexes {
		defn := index.Definition
		log.Printf("Building index %v %v", i, defn.Name)
		state, _ := client.IndexState(uint64(defn.DefnId))
		if state == c.INDEX_STATE_ACTIVE {
			continue
		}
		err = BuildIndex(defn.Name, defn.Bucket, server, indexActiveTimeoutSeconds)
		log.Printf("Built index %v %v", i, defn.Name)
	}

	return err
}

func CorruptIndex(indexName, bucketName, dirPath, indexUsing string, partnId c.PartitionId) error {
	if indexUsing == "forestdb" {
		return corruptForestdbIndex(indexName, bucketName, dirPath, partnId)
	} else if indexUsing == "plasma" {
		return corruptPlasmaIndex(indexName, bucketName, dirPath, partnId)
	} else if indexUsing == "memory_optimized" {
		return corruptMOIIndex(indexName, bucketName, dirPath, partnId)
	} else {
		msg := fmt.Sprintf("Unknown indexUsing %v", indexUsing)
		return errors.New(msg)
	}
}

func corruptPlasmaIndex(indexName, bucketName, dirPath string, partnId c.PartitionId) error {
	var err error
	var slicePath string
	slicePath, err = tc.GetIndexSlicePath(indexName, bucketName, dirPath, partnId)
	if err != nil {
		return err
	}

	log.Printf("Corrupting index %v slicePath %v", indexName, slicePath)
	// Corrupt only main index
	mainIndexFilePath := filepath.Join(slicePath, "mainIndex")
	mainIndexErrFilePath := path.Join(mainIndexFilePath, "error")
	log.Printf("Corrupting index %v mainIndexErrFilePath %v", indexName, mainIndexErrFilePath)
	err = ioutil.WriteFile(mainIndexErrFilePath, []byte("Fatal error: Automation Induced Storage Corruption"), 0755)
	if err != nil {
		return err
	}
	return nil
}

// This method will corrupt all available snapshots.
func corruptMOIIndex(indexName, bucketName, dirPath string, partnId c.PartitionId) error {
	var err error
	var slicePath string
	slicePath, err = tc.GetIndexSlicePath(indexName, bucketName, dirPath, partnId)
	if err != nil {
		return err
	}

	log.Printf("Corrupting index %v slicePath %v", indexName, slicePath)

	infos, err := tc.GetMemDBSnapshots(slicePath)
	if err != nil {
		return err
	}

	snapInfoContainer := tc.NewSnapshotInfoContainer(infos)
	allSnapshots := snapInfoContainer.List()

	if len(allSnapshots) == 0 {
		return errors.New("Latest Snapshot not found")
	}

	for _, snapInfo := range allSnapshots {
		fmt.Println("snapshot datapath = ", snapInfo.DataPath)
		datadir := filepath.Join(snapInfo.DataPath, "data")

		// corrupt checksums.json. This ensures corruption even in case of empty partition.
		// TODO: Don't assume 8 shards.
		ioutil.WriteFile(filepath.Join(datadir, "checksums.json"), []byte("[1,1,1,1,1,1,1,1]"), 0755)
	}

	return nil
}

func corruptForestdbIndex(indexName, bucketName, dirPath string, partnId c.PartitionId) error {
	var err error
	var slicePath string
	slicePath, err = tc.GetIndexSlicePath(indexName, bucketName, dirPath, partnId)
	if err != nil {
		return err
	}

	log.Printf("Corrupting index %v slicePath %v", indexName, slicePath)

	pattern := fmt.Sprintf("data.fdb.*")
	files, _ := filepath.Glob(filepath.Join(slicePath, pattern))
	natsort.Strings(files)

	if len(files) <= 0 {
		return errors.New("No forestdb file found")
	}

	fpath := files[len(files)-1]
	log.Printf("Corrupting index %v slicePath %v filepath %v", indexName, slicePath, fpath)

	fm, err := NewFDBFilemgr(fpath)
	if err != nil {
		return err
	}

	err = fm.CorruptForestdbFile()
	if err != nil {
		return err
	}

	return nil
}

func CorruptMOIIndexLatestSnapshot(indexName, bucketName, dirPath, indexUsing string, partnId c.PartitionId) error {
	if indexUsing != "memory_optimized" {
		msg := fmt.Sprintf("Unexpected indexUsing %v", indexUsing)
		return errors.New(msg)
	}

	var err error
	var slicePath string
	slicePath, err = tc.GetIndexSlicePath(indexName, bucketName, dirPath, partnId)
	if err != nil {
		return err
	}

	log.Printf("Corrupting index %v slicePath %v", indexName, slicePath)

	infos, err := tc.GetMemDBSnapshots(slicePath)
	if err != nil {
		return err
	}

	snapInfoContainer := tc.NewSnapshotInfoContainer(infos)
	latestSnapshotInfo := snapInfoContainer.GetLatest()

	if latestSnapshotInfo == nil {
		return errors.New("Latest Snapshot not found")
	}

	CorruptMOIIndexBySnapshotPath(latestSnapshotInfo.DataPath)
	return nil
}

func CorruptMOIIndexBySnapshotPath(snapPath string) {
	fmt.Println("snapshot datapath = ", snapPath)

	datadir := filepath.Join(snapPath, "data")

	// corrupt checksums.json. This ensures corruption even in case of empty partition.
	// TODO: Don't assume 8 shards.
	ioutil.WriteFile(filepath.Join(datadir, "checksums.json"), []byte("[1,1,1,1,1,1,1,1]"), 0755)
}
