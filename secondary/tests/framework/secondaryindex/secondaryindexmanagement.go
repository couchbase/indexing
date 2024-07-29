package secondaryindex

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/common/queryutil"
	mclient "github.com/couchbase/indexing/secondary/manager/client"
	"github.com/couchbase/indexing/secondary/natsort"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/parser/n1ql"
)

var IndexUsing = "gsi"

var gsiClient *qc.GsiClient

func GetOrCreateClient(server, serviceAddr string) (*qc.GsiClient, error) {
	var err error
	if gsiClient == nil {
		gsiClient, err = CreateClient(server, serviceAddr)
		if err != nil {
			return nil, err
		}
	}

	gsiClient.Refresh()
	return gsiClient, nil
}

func RemoveClientForBucket(server, bucketName string) {
	if UseClient == "n1ql" {
		RemoveN1QLClientForBucket(server, bucketName)
	}
}

func CreateClient(server, serviceAddr string) (*qc.GsiClient, error) {
	config := c.SystemConfig.SectionConfig("queryport.client.", true)
	client, err := qc.NewGsiClient(server, config)
	if err != nil {
		log.Printf("Error while creating gsi client: ", err)
		return nil, err
	}

	return client, nil
}

func CreateClientWithConfig(server string, givenConfig c.Config) (*qc.GsiClient, error) {
	config := givenConfig.SectionConfig("queryport.client.", true)
	client, err := qc.NewGsiClient(server, config)
	if err != nil {
		log.Printf("Error while creating gsi client: ", err)
		return nil, err
	}

	return client, nil
}

func GetDefnID(client *qc.GsiClient, bucket, indexName string) (defnID uint64, ok bool) {

	return GetDefnID2(client, bucket, c.DEFAULT_SCOPE, c.DEFAULT_COLLECTION, indexName)
}

func GetDefnID2(client *qc.GsiClient, bucket, scopeName,
	collectionName, indexName string) (defnID uint64, ok bool) {

	indexes, _, _, _, err := client.Refresh()
	tc.HandleError(err, "Error while listing the indexes")
	for _, index := range indexes {
		defn := index.Definition
		if defn.Bucket == bucket &&
			defn.Scope == scopeName &&
			defn.Collection == collectionName &&
			defn.Name == indexName {
			return uint64(index.Definition.DefnId), true
		}
	}
	return uint64(c.IndexDefnId(0)), false
}

func GetDefnIdsDefault(client *qc.GsiClient, bucket string, indexNames []string) map[string]uint64 {
	return GetDefnIds(client, bucket, c.DEFAULT_SCOPE, c.DEFAULT_COLLECTION, indexNames)
}

func GetDefnIds(client *qc.GsiClient, bucket, scopeName, collectionName string,
	indexNames []string) map[string]uint64 {

	defnIds := make(map[string]uint64, len(indexNames))
	for _, i := range indexNames {
		defnIds[i] = 0
	}

	indexes, _, _, _, err := client.Refresh()
	tc.HandleError(err, "Error while listing the indexes")
	for _, index := range indexes {
		defn := index.Definition
		if defn.Bucket != bucket || defn.Scope != scopeName || defn.Collection != collectionName {
			continue
		}
		_, ok := defnIds[defn.Name]
		if ok {
			defnIds[defn.Name] = uint64(index.Definition.DefnId)
		}
	}
	return defnIds
}

// Creates an index and waits for it to become active
func CreateSecondaryIndex(
	indexName, bucketName, server, whereExpr string, indexFields []string, isPrimary bool, with []byte,
	skipIfExists bool, indexActiveTimeoutSeconds int64, client *qc.GsiClient) error {

	return CreateSecondaryIndex2(indexName, bucketName, server, whereExpr, indexFields, nil, isPrimary, with,
		c.SINGLE, nil, skipIfExists, indexActiveTimeoutSeconds, client)

}

// Creates an index and waits for it to become active
func CreateSecondaryIndex2(
	indexName, bucketName, server, whereExpr string, indexFields []string, desc []bool, isPrimary bool, with []byte,
	partnScheme c.PartitionScheme, partnKeys []string, skipIfExists bool, indexActiveTimeoutSeconds int64,
	client *qc.GsiClient) error {

	return CreateSecondaryIndex3(indexName, bucketName, c.DEFAULT_SCOPE, c.DEFAULT_COLLECTION, server,
		whereExpr, indexFields, desc, isPrimary, with, partnScheme, partnKeys,
		skipIfExists, indexActiveTimeoutSeconds, client)
}

// Creates an index and waits for it to become active
func CreateSecondaryIndex3(
	indexName, bucketName, scopeName, collectionName, server, whereExpr string, indexFields []string,
	desc []bool, isPrimary bool, with []byte, partnScheme c.PartitionScheme, partnKeys []string,
	skipIfExists bool, indexActiveTimeoutSeconds int64, client *qc.GsiClient) error {

	if client == nil {
		c, e := GetOrCreateClient(server, "2itest")
		if e != nil {
			return e
		}
		client = c
	}

	indexExists := IndexExistsWithClient(indexName, bucketName, scopeName, collectionName, server, client)
	if skipIfExists == true && indexExists == true {
		return nil
	}
	var secExprs []string
	if isPrimary == false {
		for _, indexField := range indexFields {
			expr, err := n1ql.ParseExpression(indexField)
			if err != nil {
				log.Printf("Creating index %v. Error while parsing the expression (%v) : %v", indexName, indexField, err)
				return err
			}

			isArray, _, isFlatten, err := queryutil.IsArrayExpression(indexField)
			if err != nil {
				log.Printf("Creating index %v. Error while checking array expression (%v) : %v", indexName, indexField, err)
				return err
			}
			if isArray && isFlatten {
				// Explode secExprs
				numFlattenKeys, err := queryutil.NumFlattenKeys(indexField)
				if err != nil {
					log.Printf("Creating index %v. Error while parsing the expression (%v) : %v, during the extraction of numFlattenKeys", indexName, indexField, err)
					return err
				}
				for i := 0; i < numFlattenKeys; i++ {
					secExprs = append(secExprs, expression.NewStringer().Visit(expr))
				}

			} else {
				secExprs = append(secExprs, expression.NewStringer().Visit(expr))
			}
		}
	}
	exprType := "N1QL"

	start := time.Now()
	// TODO: Add a way to pass indexMissingLeadingKey attribute here for functional tests
	defnID, err := client.CreateIndex4(indexName, bucketName, scopeName, collectionName, IndexUsing, exprType,
		whereExpr, secExprs, desc, false, isPrimary, partnScheme, partnKeys, with)

	if indexActiveTimeoutSeconds != 0 && err == nil {
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

	return CreateSecondaryIndex(indexName, bucketName, server, whereExpr, indexFields, isPrimary, with,
		skipIfExists, 0 /*timeout*/, client)
}

func CreateIndexesConcurrently(bucketName, server string,
	indexNameToFieldMap map[string][]string, indexNameToWhereExp map[string]string,
	indexNameToIsPrimary map[string]bool, indexNameToWith map[string][]byte,
	skipIfExists bool, client *qc.GsiClient) []error {

	var wg sync.WaitGroup
	errList := make([]error, len(indexNameToFieldMap))

	createAsync := func(in string, fields []string, errListIndex int, wg *sync.WaitGroup) {
		defer wg.Done()

		var where string
		var with []byte
		var isPrimary bool
		if indexNameToWhereExp != nil {
			where = indexNameToWhereExp[in]
		}
		if indexNameToWith != nil {
			with = indexNameToWith[in]
		}
		if indexNameToIsPrimary != nil {
			isPrimary = indexNameToIsPrimary[in]
		}

		err := CreateSecondaryIndexAsync(in, bucketName, server, where, fields, isPrimary, with, skipIfExists, nil)
		errList[errListIndex] = err
	}

	errListIndex := 0
	for indexName, indexFields := range indexNameToFieldMap {
		wg.Add(1)
		go createAsync(indexName, indexFields, errListIndex, &wg)
		errListIndex++
	}

	wg.Wait()

	return errList
}

// Todo: Remove this function and update functional tests to use BuildIndexes
func BuildIndex(indexName, bucketName, server string, indexActiveTimeoutSeconds int64) error {
	client, e := GetOrCreateClient(server, "2itest")
	if e != nil {
		return e
	}

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

	return BuildIndexes2(indexNames, bucketName, c.DEFAULT_SCOPE, c.DEFAULT_COLLECTION, server, indexActiveTimeoutSeconds)
}

func BuildIndexes2(indexNames []string, bucketName, scopeName, collectionName, server string, indexActiveTimeoutSeconds int64) error {
	client, e := GetOrCreateClient(server, "2itest")
	if e != nil {
		return e
	}

	defnIds := make([]uint64, len(indexNames))
	for i := range indexNames {
		defnIds[i], _ = GetDefnID2(client, bucketName, scopeName, collectionName, indexNames[i])
	}
	err := client.BuildIndexes(defnIds)
	log.Printf("Build command issued for the deferred indexes %v, bucket: %v, scope: %v, coll: %v",
		indexNames, bucketName, scopeName, collectionName)

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
	client, e := GetOrCreateClient(server, "2itest")
	if e != nil {
		return e
	}

	err := client.BuildIndexes(defnIds)
	log.Printf("Build command issued for the deferred indexes %v", defnIds)
	return err
}

func WaitTillIndexActive(defnID uint64, client *qc.GsiClient, indexActiveTimeoutSeconds int64) error {
	start := time.Now()
	interval := 1 * time.Second
	logAt := 1 * time.Second

	for {
		elapsed := time.Since(start)
		if elapsed.Seconds() >= float64(indexActiveTimeoutSeconds) {
			err := errors.New(fmt.Sprintf("Index did not become active after %d seconds", indexActiveTimeoutSeconds))
			return err
		}
		state, _ := client.IndexState(defnID)

		if state == c.INDEX_STATE_ACTIVE {
			log.Printf("Index is %d now active", defnID)
			return nil
		} else {
			if elapsed >= logAt {
				log.Printf("Waiting for index %d to go active ...", defnID)
				logAt += interval
				if interval < time.Duration(indexActiveTimeoutSeconds/10)*time.Second {
					interval = interval * time.Duration(2)
				}
			}
			time.Sleep(1 * time.Second)
		}
	}
}

func GetStatusOfAllIndexes(server string) (indexStateMap map[string]c.IndexState, err error) {
	var client *qc.GsiClient
	client, err = GetOrCreateClient(server, "2itest")
	if err != nil {
		log.Printf("PrintStatusOfAllIndexes(): Error from GetOrCreateClient: %v ", err)
		return
	}

	indexes, _, _, _, err := client.Refresh()
	if err != nil {
		log.Printf("PrintStatusOfAllIndexes(): Error from client.Refresh(): %v ", err)
		return
	}

	indexStateMap = make(map[string]c.IndexState, len(indexes))
	for _, index := range indexes {
		defn := index.Definition
		indexStateMap[defn.Name] = index.State
	}

	return
}

func WaitTillAllIndexesActive(defnIDs []uint64, client *qc.GsiClient, indexActiveTimeoutSeconds int64) error {
	start := time.Now()
	activeMap := make(map[uint64]bool, len(defnIDs))
	for {
		elapsed := time.Since(start)
		if elapsed.Seconds() >= float64(indexActiveTimeoutSeconds) {
			err := errors.New(fmt.Sprintf("Index did not become active after %d seconds", indexActiveTimeoutSeconds))
			return err
		}

		for _, defnID := range defnIDs {
			if active, ok := activeMap[defnID]; ok && active {
				continue
			}
			state, _ := client.IndexState(defnID)

			if state == c.INDEX_STATE_ACTIVE {
				log.Printf("Index %d is now active", defnID)
				activeMap[defnID] = true
			} else {
				log.Printf("Waiting for index %d in state %v to go active ...", defnID, state)
				activeMap[defnID] = false
			}
		}

		allActive := true
		for _, active := range activeMap {
			if !active {
				allActive = false
				break
			}
		}

		if allActive {
			return nil
		}

		time.Sleep(3 * time.Second)
	}
}

func WaitTillAllIndexNodesActive(server string, indexerActiveTimeoutSeconds int64) error {
	client, e := GetOrCreateClient(server, "2itest")
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
	client, e := GetOrCreateClient(server, "2itest")
	if e != nil {
		return "", e
	}

	defnID, _ := GetDefnID(client, bucketName, indexName)
	state, e := client.IndexState(defnID)
	if e != nil {
		log.Printf("Error while fetching index state for defnID %v", defnID)
		return "", e
	}

	return state.String(), nil
}

func IndexExists(indexName, bucketName, server string) (bool, error) {
	client, e := GetOrCreateClient(server, "2itest")
	if e != nil {
		return false, e
	}

	indexes, _, _, _, err := client.Refresh()
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

func IndexExistsWithClient(indexName, bucketName, scopeName, collectionName,
	server string, client *qc.GsiClient) bool {
	indexes, _, _, _, err := client.Refresh()
	tc.HandleError(err, "Error while listing the secondary indexes")
	for _, index := range indexes {
		defn := index.Definition
		if defn.Name == indexName && defn.Bucket == bucketName &&
			defn.Scope == scopeName && defn.Collection == collectionName {
			log.Printf("Index found:  %v", indexName)
			return true
		}
	}
	return false
}

func ListAllSecondaryIndexes(header string, client *qc.GsiClient) error {
	indexes, _, _, _, err := client.Refresh()
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

	return DropSecondaryIndex2(indexName, bucketName, c.DEFAULT_SCOPE, c.DEFAULT_COLLECTION, server)

}

func DropSecondaryIndex2(indexName, bucketName, scopeName, collectionName, server string) error {
	log.Printf("Dropping the secondary index %v", indexName)
	client, e := GetOrCreateClient(server, "2itest")
	if e != nil {
		return e
	}

	indexes, _, _, _, err := client.Refresh()
	tc.HandleError(err, "Error while listing the secondary indexes")
	for _, index := range indexes {
		defn := index.Definition
		if (defn.Name == indexName) && (defn.Bucket == bucketName) &&
			defn.Scope == scopeName && defn.Collection == collectionName {
			start := time.Now()
			e := client.DropIndex(uint64(defn.DefnId), defn.Bucket)
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
	indexes, _, _, _, err := client.Refresh()
	tc.HandleError(err, "Error while listing the secondary indexes")
	for _, index := range indexes {
		defn := index.Definition
		if (defn.Name == indexName) && (defn.Bucket == bucketName) {
			start := time.Now()
			e := client.DropIndex(uint64(defn.DefnId), defn.Bucket)
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
	client, e := GetOrCreateClient(server, "2itest")
	if e != nil {
		return e
	}

	indexes, _, _, _, err := client.Refresh()
	tc.HandleError(err, "Error while listing the secondary indexes")
	for _, index := range indexes {
		defn := index.Definition
		exists := IndexExistsWithClient(defn.Name, defn.Bucket, defn.Scope, defn.Collection, server, client)
		if exists {
			e := client.DropIndex(uint64(defn.DefnId), defn.Bucket)
			if e != nil {
				return e
			}
			log.Printf("Dropped index %v", defn.Name)
		}
	}
	return nil
}

func DropAllNonSystemIndexes(server string) error {
	log.Printf("In DropAllNonSystemIndexes()")
	client, e := GetOrCreateClient(server, "2itest")
	if e != nil {
		return e
	}

	indexes, _, _, _, err := client.Refresh()
	tc.HandleError(err, "Error while listing the secondary indexes")
	for _, index := range indexes {
		defn := index.Definition
		isSystemIndex := strings.Contains(defn.Scope, "system")
		exists := IndexExistsWithClient(defn.Name, defn.Bucket, defn.Scope, defn.Collection, server, client)
		if exists && !isSystemIndex {
			e := client.DropIndex(uint64(defn.DefnId), defn.Bucket)
			if e != nil {
				if !strings.Contains(e.Error(), "cleanup will happen in the background") {
					log.Printf("Issued drop for %v index. Will get cleaned up in background", defn.Name)
				}
				return e
			}
			log.Printf("Dropped index %v", defn.Name)
		}
	}
	return nil
}

func DropSecondaryIndexByID(indexDefnID uint64, server string) error {
	log.Printf("Dropping the secondary index %v", indexDefnID)
	client, e := GetOrCreateClient(server, "2itest")
	if e != nil {
		return e
	}

	indexes, _, _, _, err := client.Refresh()
	tc.HandleError(err, "Error while listing the secondary indexes")
	var bucketName string
	for _, index := range indexes {
		if defn := index.Definition; defn != nil && defn.DefnId == c.IndexDefnId(indexDefnID) {
			bucketName = defn.Bucket
		}
	}
	e = client.DropIndex(indexDefnID, bucketName)
	if e != nil {
		return e
	}
	log.Printf("Index dropped")
	return nil
}

func BuildAllSecondaryIndexes(server string, indexActiveTimeoutSeconds int64) error {
	log.Printf("In BuildAllSecondaryIndexes()")
	client, e := GetOrCreateClient(server, "2itest")
	if e != nil {
		return e
	}

	indexes, _, _, _, err := client.Refresh()
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

	infos, err := tc.GetMemDBSnapshots(slicePath, true)
	if err != nil {
		return err
	}

	snapInfoContainer := tc.NewSnapshotInfoContainer(infos)
	allSnapshots := snapInfoContainer.List()

	if len(allSnapshots) == 0 {
		return errors.New("Latest Snapshot not found")
	}

	// Allow indexer to persist the checksums.json before overwriting it.
	time.Sleep(5 * time.Second)

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

	infos, err := tc.GetMemDBSnapshots(slicePath, true)
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

func WaitForSystemIndices(server string, timeout time.Duration) error {
	now := time.Now()
	if timeout == 0 {
		timeout = time.Minute * 5
	}
	qclient, e := GetOrCreateClient(server, "2itest")
	if e != nil {
		return e
	}

	indices, _, _, _, err := qclient.Refresh()
	if err != nil {
		return err
	}
	waitForIndices := make([]*mclient.IndexMetadata, 0)
	for _, index := range indices {
		if strings.Contains(index.Definition.Scope, "system") &&
			index.State != c.INDEX_STATE_ACTIVE ||
			(index.Definition.Deferred && index.State != c.INDEX_STATE_READY) ||
			index.Scheduled {
			waitForIndices = append(waitForIndices, index)
		}
	}
	log.Printf("Waiting for _system indices %v", waitForIndices)

	for time.Since(now) < timeout {
		time.Sleep(1 * time.Second)
		for i, indexToDrop := range waitForIndices {
			state, err := qclient.IndexState(uint64(indexToDrop.Definition.DefnId))
			if err != nil {
				continue
			}
			switch state {
			case c.INDEX_STATE_ACTIVE:
				log.Printf("Index %v is now active", indexToDrop.Definition.Name)
				waitForIndices = append(waitForIndices[:i], waitForIndices[i+1:]...)
			case c.INDEX_STATE_READY:
				if indexToDrop.Definition.Deferred {
					log.Printf("Index %v is now active", indexToDrop.Definition.Name)
					waitForIndices = append(waitForIndices[:i], waitForIndices[i+1:]...)
				}
			default:
			}
		}
		if len(waitForIndices) == 0 {
			return nil
		}
	}
	return fmt.Errorf("indices %v not active even after %v duration", waitForIndices, timeout)
}
