package functionaltests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/couchbase/gocb.v1"
	c "github.com/couchbase/indexing/secondary/common"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	"github.com/couchbase/indexing/secondary/tools/randdocs"
)

// copied from gocbcryto/writer.go
const FILEHDR_MAGIC = "\x00Couchbase Encrypted\x00" // file is encrypted
const FILEHDR_SZ = 84

func GetFileEncryptionKeyId(filepath string) (string, error) {
	fd, err := os.Open(filepath)
	if err != nil {
		return "", err
	}
	defer fd.Close()

	bs := make([]byte, FILEHDR_SZ)
	if _, err = io.ReadFull(fd, bs); err != nil {
		return "", err
	}

	idLen := int(bs[27])
	if 28+idLen > len(bs) {
		return "", fmt.Errorf("file %v has invalid key id length %v in header, "+
			"it is most likely not encrypted", filepath, idLen)
	}
	keyId := bs[28 : 28+idLen]
	return string(keyId), nil
}

// copied from gocbcryto/writer.go
func IsFileEncrypted(filepath string) (bool, error) {
	fd, err := os.Open(filepath)
	if err != nil {
		return false, err
	}
	defer fd.Close()

	// Read the magic bytes (first 21 bytes of header)
	magicBuf := make([]byte, len(FILEHDR_MAGIC))
	n, err := fd.Read(magicBuf)
	if err != nil {
		if err == io.EOF {
			// File too small to be encrypted
			return false, nil
		}
		return false, err
	}

	// Check if we read enough bytes and if they match the magic signature
	if n < len(FILEHDR_MAGIC) {
		return false, nil
	}

	return bytes.Equal(magicBuf, []byte(FILEHDR_MAGIC)), nil
}

func getBucketEncryptionInfo(bucketName string, nodeIndex int) (map[string]interface{}, error) {
	if nodeIndex < 0 || nodeIndex >= len(clusterconfig.Nodes) {
		return nil, fmt.Errorf("invalid node index %d", nodeIndex)
	}

	hostaddress := clusterconfig.Nodes[nodeIndex]
	serverUserName := clusterconfig.Username
	serverPassword := clusterconfig.Password

	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/buckets/" + url.PathEscape(bucketName)

	req, err := http.NewRequest("GET", address, nil)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return nil, fmt.Errorf("request to bucket endpoint %v failed with status code %d", address, resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, err
	}

	return response, nil
}

func skipIfNotMOI(t *testing.T) {
	if clusterconfig.IndexUsing != "memory_optimized" {
		t.Skipf("Test %s is only valid with memory_optimized storage", t.Name())
		return
	}
}

func skipIfForestdb(t *testing.T) {
	if clusterconfig.IndexUsing == "forestdb" {
		t.Skipf("Test %s is invalid with forestdb storage", t.Name())
		return
	}
}

func getBackfillTempDir(t *testing.T) string {

	var strIndexStorageDir string
	workspace := os.Getenv("WORKSPACE")
	if workspace == "" {
		workspace = "../../../../../../../../"
	}

	if strings.HasSuffix(workspace, "/") == false {
		workspace += "/"
	}

	strIndexStorageDir = workspace + "ns_server" + "/tmp"

	absBackfillTempDir, err1 := filepath.Abs(strIndexStorageDir)
	FailTestIfError(err1, "Error while finding absolute path", t)
	return absBackfillTempDir
}

func getIndexStorageDirOnNode(nodeAddr string, t *testing.T) string {

	var strIndexStorageDir string
	workspace := os.Getenv("WORKSPACE")
	if workspace == "" {
		workspace = "../../../../../../../../"
	}

	if strings.HasSuffix(workspace, "/") == false {
		workspace += "/"
	}

	switch nodeAddr {
	case clusterconfig.Nodes[0]:
		strIndexStorageDir = workspace + "ns_server" + "/data/n_0/data/@2i/"
	case clusterconfig.Nodes[1]:
		strIndexStorageDir = workspace + "ns_server" + "/data/n_1/data/@2i/"
	case clusterconfig.Nodes[2]:
		strIndexStorageDir = workspace + "ns_server" + "/data/n_2/data/@2i/"
	case clusterconfig.Nodes[3]:
		strIndexStorageDir = workspace + "ns_server" + "/data/n_3/data/@2i/"
	case clusterconfig.Nodes[4]:
		strIndexStorageDir = workspace + "ns_server" + "/data/n_4/data/@2i/"
	case clusterconfig.Nodes[5]:
		strIndexStorageDir = workspace + "ns_server" + "/data/n_5/data/@2i/"
	case clusterconfig.Nodes[6]:
		strIndexStorageDir = workspace + "ns_server" + "/data/n_6/data/@2i/"

	}

	absIndexStorageDir, err1 := filepath.Abs(strIndexStorageDir)
	FailTestIfError(err1, "Error while finding absolute path", t)
	return absIndexStorageDir
}

// Add bucket encryption key
// Modify settings to trigger encryption of newly created persistent snapshot within minutes
// Create index
// Revert moi persistent snapshot settings
// Verify index encryption
// Delete index
// Delete bucket encryption key
func TestIndexEncryptionMOI(t *testing.T) {

	skipIfNotMOI(t)

	// Bucket is residing on node n_0 thus bucket encryption info should be fetched from n_0
	bucketName := "default"
	nodeKv := 0

	kvutility.DeleteBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.CreateBucket(bucketName, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
	time.Sleep(2 * time.Second)

	docs := generateDocs(1000, "users.prod")
	kvutility.SetKeyValues(docs, bucketName, "", clusterconfig.KVAddress)

	info, err := getBucketEncryptionInfo(bucketName, nodeKv)
	if err != nil {
		t.Fatalf("Failed to get bucket encryption info: %v", err)
	}

	log.Printf("encryptionAtRestInfo: %v", info["encryptionAtRestInfo"])

	dataStatus, err := getDataStatus(info)
	if err != nil {
		t.Fatalf("Failed to get dataStatus: %v", err)
	}
	log.Printf("Extracted dataStatus: %v", dataStatus)

	dekNumber, err := getDekNumber(info)
	if err != nil {
		t.Fatalf("Failed to get dekNumber: %v", err)
	}
	log.Printf("Extracted dekNumber: %v", dekNumber)

	keyId, err := getEncryptionAtRestKeyId(info)
	if err != nil {
		t.Fatalf("Failed to get encryptionAtRestKeyId: %v", err)
	}
	log.Printf("Current encryptionAtRestKeyId: %v", keyId)

	addBucketEncryptionKey(nodeKv, "default", "key1", 30)

	nodeIndex := 1
	resp, err := getAllEncryptionKeys(nodeIndex)
	log.Printf("All encryption keys: %v", resp)
	FailTestIfError(err, "Error in getAllEncryptionKeys", t)

	keyId = 0
	for _, keymap := range resp {
		usageIfc, ok := keymap["usage"].([]interface{})
		if !ok {
			t.Fatalf("Failed to get usage: %v", keymap["usage"])
		}

		var usage []string
		for _, u := range usageIfc {
			if str, ok := u.(string); ok {
				usage = append(usage, str)
			}
		}
		// verify that the key can be used for encryption of bucket
		if hasBucketEncryptionUsage(bucketName, usage) {
			log.Printf("keyId: %v, usage: %v", keymap["id"], usage)
			keyId = max(keyId, int(math.Round(keymap["id"].(float64))))
		}
	}
	keyIdStr := strconv.Itoa(keyId)
	err = updateBucketEncryptionKey(bucketName, nodeIndex, keyIdStr)
	FailTestIfError(err, "Error in updateBucketEncryptionKey", t)

	// Key Rotation not required for basic test
	// setBypassEncrCfgRestrictions(nodeKv)
	// setDekRotationInterval("default", nodeKv, 60)
	// setDekLifetime("default", nodeKv, 90)

	// Modify settings to trigger encryption of newly created persistent snapshot within minutes
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot_init_build.moi.interval", float64(45), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot.moi.interval", float64(45), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	node := clusterconfig.Nodes[nodeIndex]
	with := "{\"nodes\": [\"" + node + "\"]}"
	// Create index
	indexName := "idx_encr_age"
	err = secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress,
		"", []string{"age"}, false, []byte(with), false, 60, nil)
	FailTestIfError(err, "Error in creating the index", t)

	time.Sleep(1 * time.Minute)

	// Revert moi persistent snapshot settings
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot_init_build.moi.interval", float64(60000), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot.moi.interval", float64(60000), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	storageDir := getIndexStorageDirOnNode(clusterconfig.Nodes[nodeIndex], t)
	bucketUUID, err := c.GetBucketUUID(kvaddress, bucketName)
	tc.HandleError(err, "failed to get bucket UUID")
	indexDirPrefix := filepath.Join(storageDir, bucketUUID+"_")
	indexDir, err := getDirWithPrefix(indexDirPrefix)
	FailTestIfError(err, "Failed to get index directory", t)
	log.Printf("Index directory: %s", indexDir)

	snapshotDirs, err := getSnapshotDirs(indexDir)
	FailTestIfError(err, "Failed to get snapshot directories", t)
	log.Printf("Snapshot directories: %v", snapshotDirs)

	if len(snapshotDirs) == 0 {
		t.Fatalf("No snapshot directories found")
	}

	// Verify index encryption for last snapshot
	snapshotDir := snapshotDirs[len(snapshotDirs)-1]
	verifyMOISnapshotEncryption(snapshotDir, t)

	triggerIndexerCrash(nodeIndex)
	time.Sleep(15 * time.Second) // wait for indexer to recover
	secondaryindex.WaitForIndexerActive(clusterconfig.Username, clusterconfig.Password, kvaddress)

	verifyMOISnapshotEncryption(snapshotDir, t)

	// Disable encryption for bucket
	err = updateBucketEncryptionKey(bucketName, nodeIndex, "-1")
	FailTestIfError(err, "Error in updateBucketEncryptionKey", t)

	// Delete index
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	// Delete bucket
	kvutility.DeleteBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.CreateBucket(bucketName, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
	time.Sleep(2 * time.Second)

	// Delete bucket encryption key
	err = deleteBucketEncryptionKey(nodeIndex, keyIdStr)
	FailTestIfError(err, "Error in deleteBucketEncryptionKey", t)

}

// Add bucket encryption key
// Modify persistent snapshot settings
// Create index
// Revert persistent snapshot settings
// Verify index encryption
// Delete index
// Delete bucket encryption key
// For plasma log files, there will not be encryption header at start of the file and only keyId can be present at starting of the blocks
func TestIndexEncryptionPlasma(t *testing.T) {

	skipIfNotPlasma(t)

	// Bucket is residing on node n_0 thus bucket encryption info should be fetched from n_0
	bucketName := "default"
	nodeKv := 0

	kvutility.DeleteBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.CreateBucket(bucketName, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
	time.Sleep(2 * time.Second)

	docs := generateDocs(1000, "users.prod")
	kvutility.SetKeyValues(docs, bucketName, "", clusterconfig.KVAddress)

	info, err := getBucketEncryptionInfo(bucketName, nodeKv)
	if err != nil {
		t.Fatalf("Failed to get bucket encryption info: %v", err)
	}

	log.Printf("encryptionAtRestInfo: %v", info["encryptionAtRestInfo"])

	dataStatus, err := getDataStatus(info)
	if err != nil {
		t.Fatalf("Failed to get dataStatus: %v", err)
	}
	log.Printf("Extracted dataStatus: %v", dataStatus)

	dekNumber, err := getDekNumber(info)
	if err != nil {
		t.Fatalf("Failed to get dekNumber: %v", err)
	}
	log.Printf("Extracted dekNumber: %v", dekNumber)

	addBucketEncryptionKey(nodeKv, "default", "key1", 30)
	keyId, err := getEncryptionAtRestKeyId(info)
	if err != nil {
		t.Fatalf("Failed to get encryptionAtRestKeyId: %v", err)
	}
	log.Printf("Current encryptionAtRestKeyId: %v", keyId)

	nodeIndex := 1
	resp, err := getAllEncryptionKeys(nodeIndex)
	log.Printf("All encryption keys: %v", resp)
	FailTestIfError(err, "Error in getAllEncryptionKeys", t)

	keyId = 0
	for _, keymap := range resp {
		usageIfc, ok := keymap["usage"].([]interface{})
		if !ok {
			t.Fatalf("Failed to get usage: %v", keymap["usage"])
		}

		var usage []string
		for _, u := range usageIfc {
			if str, ok := u.(string); ok {
				usage = append(usage, str)
			}
		}
		// verify that the key can be used for encryption of bucket
		if hasBucketEncryptionUsage(bucketName, usage) {
			log.Printf("keyId: %v, usage: %v", keymap["id"], usage)
			keyId = max(keyId, int(math.Round(keymap["id"].(float64))))
		}
	}
	keyIdStr := strconv.Itoa(keyId)
	err = updateBucketEncryptionKey(bucketName, nodeIndex, keyIdStr)
	FailTestIfError(err, "Error in updateBucketEncryptionKey", t)

	// Key Rotation not required for basic test
	// setBypassEncrCfgRestrictions(nodeKv)
	// setDekRotationInterval("default", nodeKv, 60)
	// setDekLifetime("default", nodeKv, 90)

	// Modify settings to trigger encryption of newly created persistent snapshot within minutes
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot_init_build.interval", float64(15), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot.interval", float64(15), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	time.Sleep(7 * time.Second)

	node := clusterconfig.Nodes[nodeIndex]
	with := "{\"nodes\": [\"" + node + "\"]}"
	// Create index
	indexName := "idx_encr_age"
	err = secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress,
		"", []string{"age"}, false, []byte(with), false, 60, nil)
	FailTestIfError(err, "Error in creating the index", t)

	time.Sleep(30 * time.Second)

	// Revert moi persistent snapshot settings
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot_init_build.interval", float64(60000), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot.interval", float64(60000), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	bucketUUID, err := c.GetBucketUUID(kvaddress, bucketName)
	tc.HandleError(err, "failed to get bucket UUID")

	storageDir := getIndexStorageDirOnNode(clusterconfig.Nodes[nodeIndex], t)
	indexDirPrefix := filepath.Join(storageDir, bucketUUID+"_")
	indexDir, err := getDirWithPrefix(indexDirPrefix)
	FailTestIfError(err, "Failed to get index directory", t)
	log.Printf("Index directory: %s", indexDir)

	ekeyIds, err := getInUseKeyIds(nodeIndex, "service_bucket", bucketUUID)
	tc.HandleError(err, "failed to get in use key ids")

	ekeyId, err := filterNonEmptyKeyId(ekeyIds)
	tc.HandleError(err, "failed to filter non empty key id")

	plasmaEncrypted := verifyPlasmaEncryption(indexDir, ekeyId, t)
	if !plasmaEncrypted {
		t.Errorf("Plasma files are NOT encrypted")
	}

	triggerIndexerCrash(nodeIndex)
	time.Sleep(15 * time.Second) // wait for indexer to recover
	secondaryindex.WaitForIndexerActive(clusterconfig.Username, clusterconfig.Password, kvaddress)

	// Verify encryption after recovery
	plasmaEncrypted = verifyPlasmaEncryption(indexDir, ekeyId, t)
	if !plasmaEncrypted {
		t.Errorf("Plasma files are NOT encrypted after recovery")
	}

	// Disable encryption for bucket
	err = updateBucketEncryptionKey(bucketName, nodeIndex, "-1")
	FailTestIfError(err, "Error in updateBucketEncryptionKey", t)

	// Delete index
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	// Delete bucket
	kvutility.DeleteBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.CreateBucket(bucketName, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
	time.Sleep(2 * time.Second)

	// Delete bucket encryption key
	err = deleteBucketEncryptionKey(nodeIndex, keyIdStr)
	FailTestIfError(err, "Error in deleteBucketEncryptionKey", t)

}

func verifyMOISnapshotEncryption(snapshotDir string, t *testing.T) {
	dirsToCheck := []string{
		filepath.Join(snapshotDir, "data"),
		filepath.Join(snapshotDir, "delta"),
	}

	for _, dir := range dirsToCheck {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			continue
		}
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() && filepath.Ext(path) != ".json" {
				encrypted, err := IsFileEncrypted(path)
				if err != nil {
					t.Errorf("Error checking if file %s is encrypted: %v", path, err)
				} else if !encrypted {
					t.Errorf("File %s is NOT encrypted", path)
				} else {
					log.Printf("File %s is encrypted", path)
				}
			}
			return nil
		})
		if err != nil {
			t.Errorf("Error walking directory %s: %v", dir, err)
		}
	}

	manifestPath := filepath.Join(snapshotDir, "manifest.json")
	if _, err := os.Stat(manifestPath); err == nil {
		encrypted, err := IsFileEncrypted(manifestPath)
		if err != nil {
			t.Errorf("Error checking if manifest %s is encrypted: %v", manifestPath, err)
		} else if !encrypted {
			t.Errorf("Manifest %s is NOT encrypted", manifestPath)
		} else {
			log.Printf("Manifest %s is encrypted", manifestPath)
		}
	}
}

func verifyPlasmaEncryption(indexDir string, keyId string, t *testing.T) bool {
	dirsToCheck := []string{
		filepath.Join(indexDir, "mainIndex"),
		filepath.Join(indexDir, "docIndex"),
		filepath.Join(indexDir, "mainIndex", "recovery"),
		filepath.Join(indexDir, "docIndex", "recovery"),
	}

	plasmaEncrypted := true
	for _, dir := range dirsToCheck {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			continue
		}
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() {
				return nil
			}

			matched, _ := filepath.Match("log.*.data", filepath.Base(path))
			if !matched {
				return nil
			}

			hasKey, err := FileHasKey(path, keyId)
			if err != nil {
				t.Errorf("Error checking if file %s has key %s: %v", path, keyId, err)
				plasmaEncrypted = false
			} else if !hasKey {
				t.Errorf("File %s does NOT have expected keyId: %s", path, keyId)
				plasmaEncrypted = false
			} else {
				log.Printf("File %s is encrypted with keyId: %s", path, keyId)
			}

			return nil
		})
		if err != nil {
			t.Errorf("Error walking directory %s: %v", dir, err)
		}
	}
	return plasmaEncrypted
}

func verifyBhiveEncryption(indexDir string, keyId string, t *testing.T) bool {
	dirsToCheck := []string{
		filepath.Join(indexDir, "mainIndex"),
		filepath.Join(indexDir, "docIndex"),
		// filepath.Join(indexDir, "mainIndex", "recovery"),
		// filepath.Join(indexDir, "docIndex", "recovery"),
	}

	bhiveEncrypted := true
	for _, dir := range dirsToCheck {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			continue
		}
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() && info.Name() == "recovery" {
				return filepath.SkipDir
			}

			matched, _ := filepath.Match("log.*.data", filepath.Base(path))
			if !matched {
				return nil
			}

			hasKey, err := FileHasKey(path, keyId)
			if err != nil {
				t.Errorf("Error checking if file %s has key %s: %v", path, keyId, err)
				bhiveEncrypted = false
			} else if !hasKey {
				t.Errorf("File %s does NOT have expected keyId: %s", path, keyId)
				bhiveEncrypted = false
			} else {
				log.Printf("File %s is encrypted with keyId: %s", path, keyId)
			}

			return nil
		})
		if err != nil {
			t.Errorf("Error walking directory %s: %v", dir, err)
		}
	}
	return bhiveEncrypted
}

// Check if dropped key is still in use for indexDir files
func verifyPlasmaEncryptionWithDroppedKey(indexDir string, dropKeyId string, t *testing.T) bool {
	dirsToCheck := []string{
		filepath.Join(indexDir, "mainIndex"),
		filepath.Join(indexDir, "docIndex"),
		filepath.Join(indexDir, "mainIndex", "recovery"),
		filepath.Join(indexDir, "docIndex", "recovery"),
	}

	encryptedWithDroppedKey := false
	for _, dir := range dirsToCheck {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			continue
		}
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() {
				return nil
			}

			matched, _ := filepath.Match("log.*.data", filepath.Base(path))
			if !matched {
				return nil
			}

			hasKey, err := FileHasKey(path, dropKeyId)
			if err != nil {
				t.Errorf("Error checking if file %s has key %s: %v", path, dropKeyId, err)
			} else if hasKey {
				encryptedWithDroppedKey = true
				log.Printf("File %s is encrypted with dropped KeyId: %s", path, dropKeyId)
			}

			return nil
		})
		if err != nil {
			t.Errorf("Error walking directory %s: %v", dir, err)
		}
	}
	return encryptedWithDroppedKey
}

func FileHasKey(path string, keyId string) (bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}
	count := strings.Count(string(data), keyId)
	log.Printf("File %s has %d occurrences of key %s", path, count, keyId)
	return count > 0, nil
}

func getDataStatus(info map[string]interface{}) (string, error) {
	encInfo, ok := info["encryptionAtRestInfo"].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("encryptionAtRestInfo not found or not a map")
	}

	dataStatus, ok := encInfo["dataStatus"].(string)
	if !ok {
		return "", fmt.Errorf("dataStatus not found or not a string")
	}

	return dataStatus, nil
}

func getDekNumber(info map[string]interface{}) (int, error) {
	encInfo, ok := info["encryptionAtRestInfo"].(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("encryptionAtRestInfo not found or not a map")
	}

	dekNumFloat, ok := encInfo["dekNumber"].(float64)
	if !ok {
		return 0, fmt.Errorf("dekNumber not found or not a number")
	}

	return int(dekNumFloat), nil
}

func getEncryptionAtRestKeyId(info map[string]interface{}) (int, error) {
	keyIdFloat, ok := info["encryptionAtRestKeyId"].(float64)
	if !ok {
		return 0, fmt.Errorf("encryptionAtRestKeyId not found or not a number")
	}

	return int(keyIdFloat), nil
}

func setBypassEncrCfgRestrictions(nodeIndex int) error {
	if nodeIndex < 0 || nodeIndex >= len(clusterconfig.Nodes) {
		return fmt.Errorf("invalid node index %d", nodeIndex)
	}

	hostaddress := clusterconfig.Nodes[nodeIndex]
	serverUserName := clusterconfig.Username
	serverPassword := clusterconfig.Password

	client := &http.Client{}
	address := "http://" + hostaddress + "/diag/eval"
	bodyData := "ns_config:set(test_bypass_encr_cfg_restrictions, true)."

	req, err := http.NewRequest("POST", address, strings.NewReader(bodyData))
	if err != nil {
		return err
	}

	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("request to %s failed with status code %d", address, resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}

	log.Printf("Response for test_bypass_encr_cfg_restrictions on %s status:%d, body:%s", address, resp.StatusCode, string(body))

	return nil
}

func setDekRotationInterval(bucketName string, nodeIndex int, interval int) error {
	if nodeIndex < 0 || nodeIndex >= len(clusterconfig.Nodes) {
		return fmt.Errorf("invalid node index %d", nodeIndex)
	}

	hostaddress := clusterconfig.Nodes[nodeIndex]
	serverUserName := clusterconfig.Username
	serverPassword := clusterconfig.Password

	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/buckets/" + url.PathEscape(bucketName)

	data := url.Values{}
	data.Set("encryptionAtRestDekRotationInterval", fmt.Sprintf("%d", interval))

	req, err := http.NewRequest("POST", address, strings.NewReader(data.Encode()))
	if err != nil {
		return err
	}

	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("request to update DekRotationInterval %s failed with status code %d", address, resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}

	log.Printf("Response from setting DekRotationInterval for bucket %s on %s: status:%d, body:%s", bucketName, hostaddress, resp.StatusCode, string(body))

	return nil
}

func setDekLifetime(bucketName string, nodeIndex int, lifetime int) error {
	if nodeIndex < 0 || nodeIndex >= len(clusterconfig.Nodes) {
		return fmt.Errorf("invalid node index %d", nodeIndex)
	}

	hostaddress := clusterconfig.Nodes[nodeIndex]
	serverUserName := clusterconfig.Username
	serverPassword := clusterconfig.Password

	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/buckets/" + url.PathEscape(bucketName)

	data := url.Values{}
	data.Set("encryptionAtRestDekLifetime", fmt.Sprintf("%d", lifetime))

	req, err := http.NewRequest("POST", address, strings.NewReader(data.Encode()))
	if err != nil {
		return err
	}

	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("request to update DekLifetime %s failed with status code %d", address, resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}

	log.Printf("Response from setting DekLifetime for bucket %s on %s status:%d, body:%s", bucketName, hostaddress, resp.StatusCode, string(body))

	return nil
}

func addBucketEncryptionKey(nodeIndex int, bucketName string, keyName string, rotationIntervalDays int) error {
	if nodeIndex < 0 || nodeIndex >= len(clusterconfig.Nodes) {
		return fmt.Errorf("invalid node index %d", nodeIndex)
	}

	hostaddress := clusterconfig.Nodes[nodeIndex]
	serverUserName := clusterconfig.Username
	serverPassword := clusterconfig.Password

	client := &http.Client{}
	address := "http://" + hostaddress + "/settings/encryptionKeys"

	nextRotationTime := time.Now().UTC().Add(3 * 24 * time.Hour).Format("2006-01-02T15:04:05Z")

	usage := []string{"bucket-encryption"}
	if bucketName != "" {
		usage = append(usage, fmt.Sprintf("bucket-encryption-%s", bucketName))
	}

	//ENCRYPT_TODO: usage not being used by ns_server
	payload := map[string]interface{}{
		"name":  keyName,
		"type":  "cb-server-managed-aes-key-256",
		"usage": usage,
		"data": map[string]interface{}{
			"rotationIntervalInDays": rotationIntervalDays,
			"nextRotationTime":       nextRotationTime,
		},
	}

	bodyBytes, err := json.Marshal(payload)

	if err != nil {
		return fmt.Errorf("failed to marshal JSON payload: %v", err)
	}

	req, err := http.NewRequest("POST", address, bytes.NewReader(bodyBytes))
	if err != nil {
		return err
	}

	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("request to add encryption key %s failed with status code %d", address, resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}

	log.Printf("Response from adding encryption key for bucket %s on %s: %s", bucketName, hostaddress, string(body))

	return nil
}

func forceEncryptionAtRest(bucketName string, nodeIndex int) error {
	if nodeIndex < 0 || nodeIndex >= len(clusterconfig.Nodes) {
		return fmt.Errorf("invalid node index %d", nodeIndex)
	}

	hostaddress := clusterconfig.Nodes[nodeIndex]
	serverUserName := clusterconfig.Username
	serverPassword := clusterconfig.Password

	client := &http.Client{}
	address := "http://" + hostaddress + "/controller/forceEncryptionAtRest/bucket/" + url.PathEscape(bucketName)

	req, err := http.NewRequest("POST", address, nil)
	if err != nil {
		return err
	}

	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("request to forceEncryptionAtRest failed with status code %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}

	log.Printf("Response from forcing encryption at rest for bucket %s on %s: %s", bucketName, hostaddress, string(body))

	return nil
}

// dropEncryptionAtRestDeks asks ns_server to drop the bucket DEKs. Every service
// then rewrites the data encrypted with them using the key that is active now,
// which is plaintext once encryption has been disabled for the bucket.
func dropEncryptionAtRestDeks(bucketName string, nodeIndex int) error {
	if nodeIndex < 0 || nodeIndex >= len(clusterconfig.Nodes) {
		return fmt.Errorf("invalid node index %d", nodeIndex)
	}

	hostaddress := clusterconfig.Nodes[nodeIndex]
	serverUserName := clusterconfig.Username
	serverPassword := clusterconfig.Password

	client := &http.Client{}
	address := "http://" + hostaddress + "/controller/dropEncryptionAtRestDeks/bucket/" + url.PathEscape(bucketName)

	req, err := http.NewRequest("POST", address, nil)
	if err != nil {
		return err
	}

	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("request to dropEncryptionAtRestDeks failed with status code %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}

	log.Printf("Response from dropping encryption at rest deks for bucket %s on %s: %s", bucketName, hostaddress, string(body))

	return nil
}

func updateBucketEncryptionKey(bucketName string, nodeIndex int, keyId string) error {
	if nodeIndex < 0 || nodeIndex >= len(clusterconfig.Nodes) {
		return fmt.Errorf("invalid node index %d", nodeIndex)
	}

	hostaddress := clusterconfig.Nodes[nodeIndex]
	serverUserName := clusterconfig.Username
	serverPassword := clusterconfig.Password

	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/buckets/" + url.PathEscape(bucketName)

	payload := strings.NewReader("encryptionAtRestKeyId=" + keyId)

	req, err := http.NewRequest("POST", address, payload)
	if err != nil {
		return err
	}

	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("request to update bucket encryption key adress: %v keyId: %v failed with status code %d resp: %v", address, keyId, resp.StatusCode, resp.Body)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}

	log.Printf("Response from updating encryption keyId:%v for bucket %s on %s: %s", keyId, bucketName, hostaddress, string(body))

	return nil
}

func getAllEncryptionKeys(nodeIndex int) ([]map[string]interface{}, error) {
	if nodeIndex < 0 || nodeIndex >= len(clusterconfig.Nodes) {
		return nil, fmt.Errorf("invalid node index %d", nodeIndex)
	}

	hostaddress := clusterconfig.Nodes[nodeIndex]
	serverUserName := clusterconfig.Username
	serverPassword := clusterconfig.Password

	client := &http.Client{}
	address := "http://" + hostaddress + "/settings/encryptionKeys"

	req, err := http.NewRequest("GET", address, nil)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return nil, fmt.Errorf("request to get encryption keys failed with status code %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	var response []map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v, raw body: %s", err, string(body))
	}

	log.Printf("Successfully fetched %d encryption keys from %s", len(response), hostaddress)

	return response, nil
}

func getDirWithPrefix(prefix string) (string, error) {
	dir := filepath.Dir(prefix)
	basePrefix := filepath.Base(prefix)

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return "", err
	}

	for _, f := range files {
		if f.IsDir() && strings.HasPrefix(f.Name(), basePrefix) {
			return filepath.Join(dir, f.Name()), nil
		}
	}

	return "", fmt.Errorf("no directory found with prefix %s", prefix)
}

func getSnapshotDirs(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var dirs []string
	for _, f := range files {
		if f.IsDir() && strings.HasPrefix(f.Name(), "snapshot.") {
			dirs = append(dirs, filepath.Join(dir, f.Name()))
		}
	}

	sort.Strings(dirs)
	return dirs, nil
}

func hasBucketEncryptionUsage(bucketName string, usageSlice []string) bool {
	target := "bucket-encryption-" + bucketName
	for _, val := range usageSlice {
		if val == "bucket-encryption" || val == target {
			return true
		}
	}
	return false
}

func deleteBucketEncryptionKey(nodeIndex int, keyId string) error {
	if nodeIndex < 0 || nodeIndex >= len(clusterconfig.Nodes) {
		return fmt.Errorf("invalid node index %d", nodeIndex)
	}

	hostaddress := clusterconfig.Nodes[nodeIndex]
	serverUserName := clusterconfig.Username
	serverPassword := clusterconfig.Password

	client := &http.Client{}
	address := "http://" + hostaddress + "/settings/encryptionKeys/" + url.PathEscape(keyId)

	req, err := http.NewRequest("DELETE", address, nil)
	if err != nil {
		return err
	}

	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("request to delete encryption key %s failed with status code %d", keyId, resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}

	log.Printf("Response from deleting encryption key %s on %s: %s", keyId, hostaddress, string(body))

	return nil
}

func getInUseKeyIds(nodeIndex int, keyType string, bucketUUID string) ([]string, error) {
	if nodeIndex < 0 || nodeIndex >= len(clusterconfig.Nodes) {
		return nil, fmt.Errorf("invalid node index %d", nodeIndex)
	}

	hostaddress := clusterconfig.Nodes[nodeIndex]
	serverUserName := clusterconfig.Username
	serverPassword := clusterconfig.Password

	// Retrieve the actual Indexer HTTP address (e.g. host:9102)
	indexerAddr := secondaryindex.GetIndexHttpAddrOnNode(serverUserName, serverPassword, hostaddress)
	if indexerAddr == "" {
		return nil, fmt.Errorf("failed to get indexer HTTP address for %s", hostaddress)
	}

	client := &http.Client{}
	address := "http://" + indexerAddr + "/encryption/GetInUseKeys"

	req, err := http.NewRequest("GET", address, nil)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(serverUserName, serverPassword)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request to %s failed with status code %d", address, resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	var kdtMap map[string][]string
	if err := json.Unmarshal(body, &kdtMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %v, body: %s", err, string(body))
	}

	// The key format is "{keyType uuid}" as per the indexer's string representation of KeyDataType
	targetKey := fmt.Sprintf("{%s %s}", keyType, bucketUUID)
	keys, ok := kdtMap[targetKey]
	if !ok {
		return nil, fmt.Errorf("key data type %s not found in response", targetKey)
	}

	return keys, nil
}

func filterNonEmptyKeyId(keyIds []string) (string, error) {
	for _, k := range keyIds {
		if k != "" {
			return k, nil
		}
	}
	return "", fmt.Errorf("no non-empty keyid found")
}

func filterKeyId(keyIds []string, excludeKeyIds []string) (string, error) {

	excludeMap := make(map[string]bool)
	for _, keyId := range excludeKeyIds {
		excludeMap[keyId] = true
	}
	for _, k := range keyIds {
		_, ok := excludeMap[k]
		if !ok {
			return k, nil
		}
	}
	return "", fmt.Errorf("no new keyid found")
}

func triggerIndexerCrash(nodeIndex int) {
	hostaddress := clusterconfig.Nodes[nodeIndex]
	log.Printf("Triggering indexer crash on node %s", hostaddress)

	tc.KillIndexer()
}

func TestIndexEncryptionPlasmaMultiBucket(t *testing.T) {

	skipIfNotPlasma(t)

	bucket1 := "default"
	bucket2 := "bucket2"
	nodeKv := 0
	nodeIndex := 1

	// Setup buckets
	for _, b := range []string{bucket1, bucket2} {
		kvutility.DeleteBucket(b, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
		kvutility.CreateBucket(b, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
		time.Sleep(2 * time.Second)
		docs := generateDocs(1000, "users.prod")
		kvutility.SetKeyValues(docs, b, "", clusterconfig.KVAddress)
	}

	// Add encryption keys for both buckets
	addBucketEncryptionKey(nodeKv, bucket1, "key1", 30)
	addBucketEncryptionKey(nodeKv, bucket2, "key2", 30)

	// Function to get the latest key for a bucket from all keys
	getLatestKeyIdForBucket := func(bName string) string {
		resp, _ := getAllEncryptionKeys(nodeIndex)
		var kId int
		for _, keymap := range resp {
			usageIfc := keymap["usage"].([]interface{})
			var usage []string
			for _, u := range usageIfc {
				usage = append(usage, u.(string))
			}
			if hasBucketEncryptionUsage(bName, usage) {
				kId = max(kId, int(math.Round(keymap["id"].(float64))))
			}
		}
		return strconv.Itoa(kId)
	}

	keyId1Str := getLatestKeyIdForBucket(bucket1)
	keyId2Str := getLatestKeyIdForBucket(bucket2)

	updateBucketEncryptionKey(bucket1, nodeIndex, keyId1Str)
	updateBucketEncryptionKey(bucket2, nodeIndex, keyId2Str)

	// Trigger quick snapshots
	secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot_init_build.interval", float64(15), clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot.interval", float64(15), clusterconfig.Username, clusterconfig.Password, kvaddress)
	time.Sleep(7 * time.Second)

	node := clusterconfig.Nodes[nodeIndex]
	with := "{\"nodes\": [\"" + node + "\"]}"
	idx1 := "idx1"
	idx2 := "idx2"

	secondaryindex.CreateSecondaryIndex(idx1, bucket1, indexManagementAddress, "", []string{"age"}, false, []byte(with), false, 60, nil)
	secondaryindex.CreateSecondaryIndex(idx2, bucket2, indexManagementAddress, "", []string{"age"}, false, []byte(with), false, 60, nil)

	time.Sleep(45 * time.Second)

	// Revert snapshot settings
	secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot_init_build.interval", float64(60000), clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot.interval", float64(60000), clusterconfig.Username, clusterconfig.Password, kvaddress)

	verifyAndPostCrash := func() {
		for _, b := range []string{bucket1, bucket2} {
			idxName := idx1
			if b == bucket2 {
				idxName = idx2
			}
			storageDir := getIndexStorageDirOnNode(clusterconfig.Nodes[nodeIndex], t)
			indexDir, _ := getDirWithPrefix(filepath.Join(storageDir, b+"_"+idxName))
			uuid, _ := c.GetBucketUUID(kvaddress, b)
			ids, _ := getInUseKeyIds(nodeIndex, "service_bucket", uuid)
			eid, _ := filterNonEmptyKeyId(ids)

			if !verifyPlasmaEncryption(indexDir, eid, t) {
				t.Errorf("Encryption failed for bucket %s index %s", b, idxName)
			}
		}
	}

	log.Printf("Verifying encryption before crash")
	verifyAndPostCrash()

	triggerIndexerCrash(nodeIndex)
	time.Sleep(15 * time.Second)
	secondaryindex.WaitForIndexerActive(clusterconfig.Username, clusterconfig.Password, kvaddress)

	log.Printf("Verifying encryption after recovery")
	verifyAndPostCrash()

	// Cleanup
	secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	for _, b := range []string{bucket1, bucket2} {
		updateBucketEncryptionKey(b, nodeIndex, "-1")
		kvutility.DeleteBucket(b, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	}
	deleteBucketEncryptionKey(nodeIndex, keyId1Str)
	deleteBucketEncryptionKey(nodeIndex, keyId2Str)
}

func TestIndexEncryptionPlasmaRotationDrop(t *testing.T) {

	skipIfNotPlasma(t)

	// Bucket is residing on node n_0 thus bucket encryption info should be fetched from n_0
	bucketName := "default"
	nodeKv := 0

	kvutility.DeleteBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.CreateBucket(bucketName, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
	time.Sleep(2 * time.Second)

	docs := generateDocs(1000, "users.prod")
	kvutility.SetKeyValues(docs, bucketName, "", clusterconfig.KVAddress)

	info, err := getBucketEncryptionInfo(bucketName, nodeKv)
	if err != nil {
		t.Fatalf("Failed to get bucket encryption info: %v", err)
	}

	log.Printf("encryptionAtRestInfo: %v", info["encryptionAtRestInfo"])

	dataStatus, err := getDataStatus(info)
	if err != nil {
		t.Fatalf("Failed to get dataStatus: %v", err)
	}
	log.Printf("Extracted dataStatus: %v", dataStatus)

	dekNumber, err := getDekNumber(info)
	if err != nil {
		t.Fatalf("Failed to get dekNumber: %v", err)
	}
	log.Printf("Extracted dekNumber: %v", dekNumber)

	addBucketEncryptionKey(nodeKv, "default", "key1", 30)
	keyId, err := getEncryptionAtRestKeyId(info)
	if err != nil {
		t.Fatalf("Failed to get encryptionAtRestKeyId: %v", err)
	}
	log.Printf("Current encryptionAtRestKeyId: %v", keyId)

	nodeIndex := 1
	resp, err := getAllEncryptionKeys(nodeIndex)
	log.Printf("All encryption keys: %v", resp)
	FailTestIfError(err, "Error in getAllEncryptionKeys", t)

	keyId = 0
	for _, keymap := range resp {
		usageIfc, ok := keymap["usage"].([]interface{})
		if !ok {
			t.Fatalf("Failed to get usage: %v", keymap["usage"])
		}

		var usage []string
		for _, u := range usageIfc {
			if str, ok := u.(string); ok {
				usage = append(usage, str)
			}
		}
		// verify that the key can be used for encryption of bucket
		if hasBucketEncryptionUsage(bucketName, usage) {
			log.Printf("keyId: %v, usage: %v", keymap["id"], usage)
			keyId = max(keyId, int(math.Round(keymap["id"].(float64))))
		}
	}
	keyIdStr := strconv.Itoa(keyId)
	err = updateBucketEncryptionKey(bucketName, nodeIndex, keyIdStr)
	FailTestIfError(err, "Error in updateBucketEncryptionKey", t)

	// Modify settings to trigger encryption of newly created persistent snapshot within minutes
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot_init_build.interval", float64(15), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot.interval", float64(15), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	time.Sleep(7 * time.Second)

	node := clusterconfig.Nodes[nodeIndex]
	with := "{\"nodes\": [\"" + node + "\"]}"
	// Create index
	indexName := "idx_encr_age"
	err = secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress,
		"", []string{"age"}, false, []byte(with), false, 60, nil)
	FailTestIfError(err, "Error in creating the index", t)

	time.Sleep(30 * time.Second)

	// Revert moi persistent snapshot settings
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot_init_build.interval", float64(60000), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot.interval", float64(60000), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	bucketUUID, err := c.GetBucketUUID(kvaddress, bucketName)
	tc.HandleError(err, "failed to get bucket UUID")

	storageDir := getIndexStorageDirOnNode(clusterconfig.Nodes[nodeIndex], t)
	indexDirPrefix := filepath.Join(storageDir, bucketUUID+"_")
	indexDir, err := getDirWithPrefix(indexDirPrefix)
	FailTestIfError(err, "Failed to get index directory", t)
	log.Printf("Index directory: %s", indexDir)

	ekeyIds, err := getInUseKeyIds(nodeIndex, "service_bucket", bucketUUID)
	tc.HandleError(err, "failed to get in use key ids")

	ekeyId, err := filterNonEmptyKeyId(ekeyIds)
	tc.HandleError(err, "failed to filter non empty key id")

	plasmaEncrypted := verifyPlasmaEncryption(indexDir, ekeyId, t)
	if !plasmaEncrypted {
		t.Errorf("Plasma files are NOT encrypted")
	}

	// Key Rotation test, plasma must have received newer keys & data should be encrypted using newer keys.
	setBypassEncrCfgRestrictions(nodeKv)
	setDekRotationInterval("default", nodeKv, 25)
	setDekLifetime("default", nodeKv, 40)

	time.Sleep(30 * time.Second)
	ekeyIds2, err := getInUseKeyIds(nodeIndex, "service_bucket", bucketUUID)
	tc.HandleError(err, "failed to get in use key ids")

	//Set to higher interval as one rotation should have happened
	setDekRotationInterval("default", nodeKv, 86400)
	setDekLifetime("default", nodeKv, 86400)

	excludeKeyIds := []string{"", ekeyId}
	ekeyId2, err := filterKeyId(ekeyIds2, excludeKeyIds)
	tc.HandleError(err, "failed to filter key id")

	plasmaEncrypted = verifyPlasmaEncryption(indexDir, ekeyId2, t)
	if !plasmaEncrypted {
		t.Errorf("Plasma files are NOT encrypted with correct key")
	}

	time.Sleep(11 * time.Second)
	// Skip this check: MB-71420
	// ekeyId should have been dropped by now.
	// encryptedWithDroppedKey := verifyPlasmaEncryptionWithDroppedKey(indexDir, ekeyId, t)
	// if encryptedWithDroppedKey {
	// 	t.Errorf("Plasma files are encrypted with dropped key")
	// }

	triggerIndexerCrash(nodeIndex)
	time.Sleep(10 * time.Second) // wait for indexer to recover
	secondaryindex.WaitForIndexerActive(clusterconfig.Username, clusterconfig.Password, kvaddress)
	plasmaEncrypted = verifyPlasmaEncryption(indexDir, ekeyId2, t)
	if !plasmaEncrypted {
		t.Errorf("Plasma files are NOT encrypted with correct key after recovery")
	}

	// Skip this check: MB-71420
	// encryptedWithDroppedKey = verifyPlasmaEncryptionWithDroppedKey(indexDir, ekeyId, t)
	// if encryptedWithDroppedKey {
	// 	t.Errorf("Plasma files are encrypted with dropped key")
	// }

	// Disable encryption for bucket
	err = updateBucketEncryptionKey(bucketName, nodeIndex, "-1")
	FailTestIfError(err, "Error in updateBucketEncryptionKey", t)

	// Delete index
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	// Delete bucket
	kvutility.DeleteBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.CreateBucket(bucketName, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
	time.Sleep(2 * time.Second)

	// Delete bucket encryption key
	err = deleteBucketEncryptionKey(nodeIndex, keyIdStr)
	FailTestIfError(err, "Error in deleteBucketEncryptionKey", t)

}

// codebookEncryptionTimeout bounds how long the codebook may take to follow an
// encryption key change.  The codebook is not rewritten when the active key
// changes, only when its current key is dropped (see MB-72618 and
// StorageMgr::handleEncryptionUpdateKey), so it converges some time after the
// key change rather than along with it.
const codebookEncryptionTimeout = 3 * time.Minute

// codebookEncryptionPoll is the gap between two checks while waiting for the
// codebook to converge.
const codebookEncryptionPoll = 5 * time.Second

// waitForCodebookEncryption waits for every codebook under indexDir to be
// encrypted with keyid, and returns the last failure if that does not happen
// within codebookEncryptionTimeout.
func waitForCodebookEncryption(indexDir string, keyid string) error {
	deadline := time.Now().Add(codebookEncryptionTimeout)
	for {
		err := verifyCodebookEncryption(indexDir, keyid)
		if err == nil || time.Now().After(deadline) {
			return err
		}
		time.Sleep(codebookEncryptionPoll)
	}
}

// verifyCodebookDecryption returns an error while any codebook under indexDir
// is still encrypted.
func verifyCodebookDecryption(indexDir string) error {
	codebookDir := filepath.Join(indexDir, tc.CODEBOOK_DIR)
	if _, err := os.Stat(codebookDir); os.IsNotExist(err) {
		log.Printf("Codebook directory does not exist: %s", codebookDir)
		return nil
	}

	return filepath.Walk(codebookDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			encrypted, err := IsFileEncrypted(path)
			if err != nil {
				return fmt.Errorf("error checking if file %s is encrypted: %v", path, err)
			} else if encrypted {
				return fmt.Errorf("codebook file %s is encrypted", path)
			}
			log.Printf("Codebook file %s is NOT encrypted", path)
		}
		return nil
	})
}

// waitForCodebookDecryption waits for every codebook under indexDir to be
// unencrypted, and returns the last failure if that does not happen within
// codebookEncryptionTimeout.
func waitForCodebookDecryption(indexDir string) error {
	deadline := time.Now().Add(codebookEncryptionTimeout)
	for {
		err := verifyCodebookDecryption(indexDir)
		if err == nil || time.Now().After(deadline) {
			return err
		}
		time.Sleep(codebookEncryptionPoll)
	}
}

func verifyCodebookEncryption(indexDir string, keyid string) error {
	codebookDir := filepath.Join(indexDir, tc.CODEBOOK_DIR)
	if _, err := os.Stat(codebookDir); os.IsNotExist(err) {
		log.Printf("Codebook directory does not exist: %s", codebookDir)
		return nil
	}

	err := filepath.Walk(codebookDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			encrypted, err := IsFileEncrypted(path)
			if err != nil {
				return fmt.Errorf("error checking if file %s is encrypted: %v", path, err)
			}
			if !encrypted {
				return fmt.Errorf("codebook file %s is NOT encrypted", path)
			}
			keyid2, err := GetFileEncryptionKeyId(path)
			if err != nil {
				return fmt.Errorf("codebook file %s failed to get keyid: %v", path, err)
			}
			log.Printf("Codebook file %s is encrypted using key:%v expected key:%v", path, keyid2, keyid)
			if keyid != keyid2 {
				return fmt.Errorf("codebook file %s using incorrect keyid: got %v, expected %v", path, keyid2, keyid)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("error walking codebook directory %s: %v", codebookDir, err)
	}
	return nil
}

// TestPlasmaCodebookEncryption tests that codebook files for vector indexes
// are encrypted when bucket encryption is enabled.
// Steps:
// 1. Add bucket encryption key
// 2. Create vector index with training documents
// 3. Build the index
// 4. Wait for some time
// 5. Verify codebook directory encryption
// 6. Crash indexer and reverify codebook directory encryption
// 7. Rotate key and drop older key, check newer key being used and older key not being used
// 8. Disable encryption verify decryption of codebook
func TestPlasmaCodebookEncryption(t *testing.T) {
	skipIfNotPlasma(t)

	bucketName := "default"
	nodeKv := 0
	nodeIndex := 1

	kvutility.DeleteBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.CreateBucket(bucketName, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
	time.Sleep(2 * time.Second)

	// Load vector documents for training
	cfg := randdocs.Config{
		ClusterAddr:    clusterconfig.KVAddress,
		Bucket:         bucketName,
		NumDocs:        10000,
		Iterations:     1,
		Threads:        8,
		OpsPerSec:      100000,
		UseSIFTSmall:   true,
		SkipNormalData: true,
		SIFTFVecsFile:  "../../tools/randdocs/siftsmall/siftsmall_base.fvecs",
	}
	err := randdocs.Run(cfg)
	FailTestIfError(err, "Error loading vector data", t)

	info, err := getBucketEncryptionInfo(bucketName, nodeKv)
	if err != nil {
		t.Fatalf("Failed to get bucket encryption info: %v", err)
	}
	log.Printf("encryptionAtRestInfo: %v", info["encryptionAtRestInfo"])

	addBucketEncryptionKey(nodeKv, bucketName, "key1_vec", 30)

	resp, err := getAllEncryptionKeys(nodeIndex)
	log.Printf("All encryption keys: %v", resp)
	FailTestIfError(err, "Error in getAllEncryptionKeys", t)

	keyId := 0
	for _, keymap := range resp {
		usageIfc, ok := keymap["usage"].([]interface{})
		if !ok {
			t.Fatalf("Failed to get usage: %v", keymap["usage"])
		}

		var usage []string
		for _, u := range usageIfc {
			if str, ok := u.(string); ok {
				usage = append(usage, str)
			}
		}
		if hasBucketEncryptionUsage(bucketName, usage) {
			log.Printf("keyId: %v, usage: %v", keymap["id"], usage)
			keyId = max(keyId, int(math.Round(keymap["id"].(float64))))
		}
	}
	keyIdStr := strconv.Itoa(keyId)
	err = updateBucketEncryptionKey(bucketName, nodeIndex, keyIdStr)
	FailTestIfError(err, "Error in updateBucketEncryptionKey", t)
	keyUpdatedTime := time.Now()

	node := clusterconfig.Nodes[nodeIndex]
	// Create vector index
	indexName := "idx_vec_encr"
	stmt := fmt.Sprintf("CREATE INDEX %v ON `%v`(sift VECTOR) WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"nodes\":[\"%v\"], \"defer_build\":true};", indexName, bucketName, node)

	err = createWithDeferAndBuild(indexName, BUCKET, "", "", stmt, defaultIndexActiveTimeout*2)
	FailTestIfError(err, "Error in creating idx_sift10k", t)

	// Build the index
	err = secondaryindex.BuildIndexes2([]string{indexName}, bucketName, "_default", "_default", indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in building vector index", t)

	time.Sleep(15 * time.Second)

	// Verify codebook encryption
	storageDir := getIndexStorageDirOnNode(clusterconfig.Nodes[nodeIndex], t)
	bucketUUID, err := c.GetBucketUUID(kvaddress, bucketName)
	tc.HandleError(err, "failed to get bucket UUID")
	indexDirPrefix := filepath.Join(storageDir, bucketUUID+"_")
	indexDir, err := getDirWithPrefix(indexDirPrefix)
	FailTestIfError(err, "Failed to get index directory", t)
	log.Printf("Index directory: %s", indexDir)

	ekeyIds, err := getInUseKeyIds(nodeIndex, "service_bucket", bucketUUID)
	tc.HandleError(err, "failed to get in use key ids")

	ekeyId, err := filterNonEmptyKeyId(ekeyIds)
	tc.HandleError(err, "failed to filter non empty key id")

	log.Printf("Basic persistCodebookToDisk...")
	err = waitForCodebookEncryption(indexDir, ekeyId)
	FailTestIfError(err, "Error in verifyCodebookEncryption", t)

	// Crash indexer
	triggerIndexerCrash(nodeIndex)
	time.Sleep(15 * time.Second) // wait for indexer to recover
	secondaryindex.WaitForIndexerActive(clusterconfig.Username, clusterconfig.Password, kvaddress)

	log.Printf("Encrypted codebook crash recovery")
	err = waitForCodebookEncryption(indexDir, ekeyId)
	FailTestIfError(err, "Error in verifyCodebookEncryption", t)

	// Rotate Key & Drop Key
	diffInSeconds := int(time.Since(keyUpdatedTime).Seconds())
	setBypassEncrCfgRestrictions(nodeKv)
	log.Printf("diffInSeconds:%d", diffInSeconds)
	setDekRotationInterval("default", nodeKv, diffInSeconds+10)
	setDekLifetime("default", nodeKv, diffInSeconds+30)

	time.Sleep(15 * time.Second)
	ekeyIds2, err := getInUseKeyIds(nodeIndex, "service_bucket", bucketUUID)
	tc.HandleError(err, "failed to get in use key ids")

	excludeKeyIds := []string{"", ekeyId}
	ekeyId2, err := filterKeyId(ekeyIds2, excludeKeyIds)
	tc.HandleError(err, "failed to filter key id")

	time.Sleep(15 * time.Second)
	log.Printf("Encrypted codebook rotation")
	// Make sure that ekeyId is not being used & ekeyId2 is being used
	err = waitForCodebookEncryption(indexDir, ekeyId2)
	FailTestIfError(err, "Error in verifyCodebookEncryption", t)

	// Set to higher interval as one rotation should have happened
	setDekRotationInterval("default", nodeKv, 86400)
	setDekLifetime("default", nodeKv, 86400)

	// Disable encryption for bucket
	err = updateBucketEncryptionKey(bucketName, nodeIndex, "-1")
	FailTestIfError(err, "Error in updateBucketEncryptionKey", t)

	time.Sleep(10 * time.Second) // wait for key update

	// Disabling encryption only stops future writes from being encrypted.  The
	// codebook is rewritten as plaintext when the DEKs that encrypt it are
	// dropped, so ask for that explicitly.
	err = dropEncryptionAtRestDeks(bucketName, nodeKv)
	FailTestIfError(err, "Error in dropEncryptionAtRestDeks", t)

	err = waitForCodebookDecryption(indexDir)
	FailTestIfError(err, "Error in verifyCodebookDecryption", t)

	// Crash indexer
	triggerIndexerCrash(nodeIndex)
	time.Sleep(15 * time.Second) // wait for indexer to recover
	secondaryindex.WaitForIndexerActive(clusterconfig.Username, clusterconfig.Password, kvaddress)

	err = waitForCodebookDecryption(indexDir)
	FailTestIfError(err, "Error in verifyCodebookDecryption", t)

	// Delete index
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	// Delete bucket
	kvutility.DeleteBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.CreateBucket(bucketName, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
	time.Sleep(2 * time.Second)

	// Delete bucket encryption key
	err = deleteBucketEncryptionKey(nodeIndex, keyIdStr)
	FailTestIfError(err, "Error in deleteBucketEncryptionKey", t)
}

// This test creates index with encryption disabled & later enabled.
// 1. Create vector index with training documents
// 2. Build the index (persisted codebook will not be encrypted)
// 3. Add bucket encryption key
// 4. Wait for some time
// 5. Verify codebook directory encryption
// 6. Cleanup: delete index, disable encryption, delete encryption key
func TestPlasmaCodebookEncryption2(t *testing.T) {
	skipIfNotPlasma(t)

	bucketName := "default"
	nodeKv := 0
	nodeIndex := 1

	kvutility.DeleteBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.CreateBucket(bucketName, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
	time.Sleep(2 * time.Second)

	// Load vector documents for training
	cfg := randdocs.Config{
		ClusterAddr:    clusterconfig.KVAddress,
		Bucket:         bucketName,
		NumDocs:        10000,
		Iterations:     1,
		Threads:        8,
		OpsPerSec:      100000,
		UseSIFTSmall:   true,
		SkipNormalData: true,
		SIFTFVecsFile:  "../../tools/randdocs/siftsmall/siftsmall_base.fvecs",
	}
	err := randdocs.Run(cfg)
	FailTestIfError(err, "Error loading vector data", t)

	node := clusterconfig.Nodes[nodeIndex]
	// Create vector index
	indexName := "idx_vec_encr"
	stmt := fmt.Sprintf("CREATE INDEX %v ON `%v`(sift VECTOR) WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"nodes\":[\"%v\"], \"defer_build\":true};", indexName, bucketName, node)

	err = createWithDeferAndBuild(indexName, BUCKET, "", "", stmt, defaultIndexActiveTimeout*2)
	FailTestIfError(err, "Error in creating idx_sift10k", t)

	// Build the index
	err = secondaryindex.BuildIndexes2([]string{indexName}, bucketName, "_default", "_default", indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in building vector index", t)

	info, err := getBucketEncryptionInfo(bucketName, nodeKv)
	if err != nil {
		t.Fatalf("Failed to get bucket encryption info: %v", err)
	}
	log.Printf("encryptionAtRestInfo: %v", info["encryptionAtRestInfo"])

	addBucketEncryptionKey(nodeKv, bucketName, "key1_vec", 30)

	resp, err := getAllEncryptionKeys(nodeIndex)
	log.Printf("All encryption keys: %v", resp)
	FailTestIfError(err, "Error in getAllEncryptionKeys", t)

	keyId := 0
	for _, keymap := range resp {
		usageIfc, ok := keymap["usage"].([]interface{})
		if !ok {
			t.Fatalf("Failed to get usage: %v", keymap["usage"])
		}

		var usage []string
		for _, u := range usageIfc {
			if str, ok := u.(string); ok {
				usage = append(usage, str)
			}
		}
		if hasBucketEncryptionUsage(bucketName, usage) {
			log.Printf("keyId: %v, usage: %v", keymap["id"], usage)
			keyId = max(keyId, int(math.Round(keymap["id"].(float64))))
		}
	}
	keyIdStr := strconv.Itoa(keyId)
	err = updateBucketEncryptionKey(bucketName, nodeIndex, keyIdStr)
	FailTestIfError(err, "Error in updateBucketEncryptionKey", t)
	time.Sleep(10 * time.Second) // wait for key update

	// The codebook was written before encryption was enabled.  Enabling only
	// encrypts future writes, so ask ns_server to convert what is already on
	// disk, which it does by dropping the null key.
	err = forceEncryptionAtRest(bucketName, nodeKv)
	FailTestIfError(err, "Error in forceEncryptionAtRest", t)

	// Verify codebook encryption
	storageDir := getIndexStorageDirOnNode(clusterconfig.Nodes[nodeIndex], t)
	bucketUUID, err := c.GetBucketUUID(kvaddress, bucketName)
	tc.HandleError(err, "failed to get bucket UUID")
	indexDirPrefix := filepath.Join(storageDir, bucketUUID+"_")
	indexDir, err := getDirWithPrefix(indexDirPrefix)
	FailTestIfError(err, "Failed to get index directory", t)
	log.Printf("Index directory: %s", indexDir)

	ekeyIds, err := getInUseKeyIds(nodeIndex, "service_bucket", bucketUUID)
	tc.HandleError(err, "failed to get in use key ids")

	ekeyId, err := filterNonEmptyKeyId(ekeyIds)
	tc.HandleError(err, "failed to filter non empty key id")

	err = waitForCodebookEncryption(indexDir, ekeyId)
	FailTestIfError(err, "Error in verifyCodebookEncryption", t)
}

// TestVectorIndexCodebookEncryption tests that codebook files for vector indexes
// are encrypted when bucket encryption is enabled.
// Steps:
// 1. Add bucket encryption key
// 2. Create vector index with training documents
// 3. Build the index
// 4. Wait for some time
// 5. Verify codebook directory encryption
// 6. Crash indexer and reverify codebook directory encryption
// 7. Rotate key and drop older key, check newer key being used and older key not being used
// 8. Disable encryption verify decryption of codebook
func TestBhiveCodebookEncryption(t *testing.T) {
	skipIfNotPlasma(t)

	bucketName := "default"
	nodeKv := 0
	nodeIndex := 1

	kvutility.DeleteBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.CreateBucket(bucketName, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
	time.Sleep(2 * time.Second)

	vectorSetup(t, bucketName, "", "", numDocs)
	idx_bhive := "idx_bhive"

	// Drop all indexes from earlier tests
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	info, err := getBucketEncryptionInfo(bucketName, nodeKv)
	if err != nil {
		t.Fatalf("Failed to get bucket encryption info: %v", err)
	}
	log.Printf("encryptionAtRestInfo: %v", info["encryptionAtRestInfo"])

	addBucketEncryptionKey(nodeKv, bucketName, "key1_vec", 30)

	resp, err := getAllEncryptionKeys(nodeIndex)
	log.Printf("All encryption keys: %v", resp)
	FailTestIfError(err, "Error in getAllEncryptionKeys", t)

	keyId := 0
	for _, keymap := range resp {
		usageIfc, ok := keymap["usage"].([]interface{})
		if !ok {
			t.Fatalf("Failed to get usage: %v", keymap["usage"])
		}

		var usage []string
		for _, u := range usageIfc {
			if str, ok := u.(string); ok {
				usage = append(usage, str)
			}
		}
		if hasBucketEncryptionUsage(bucketName, usage) {
			log.Printf("keyId: %v, usage: %v", keymap["id"], usage)
			keyId = max(keyId, int(math.Round(keymap["id"].(float64))))
		}
	}
	keyIdStr := strconv.Itoa(keyId)
	err = updateBucketEncryptionKey(bucketName, nodeIndex, keyIdStr)
	FailTestIfError(err, "Error in updateBucketEncryptionKey", t)
	keyUpdatedTime := time.Now()

	node := clusterconfig.Nodes[nodeIndex]
	// Create vector index
	stmt := fmt.Sprintf("CREATE VECTOR INDEX "+idx_bhive+
		" ON default(sift VECTOR)"+
		" WITH { \"dimension\":128, \"description\": \"IVF,SQ8\", \"similarity\":\"L2_SQUARED\", \"nodes\":[\"%v\"], \"defer_build\":true};", node)
	err = createWithDeferAndBuild(idx_bhive, bucket, "", "", stmt, defaultIndexActiveTimeout*2)
	FailTestIfError(err, "Error in creating idx_sift10k", t)

	time.Sleep(15 * time.Second)

	// Verify codebook encryption
	storageDir := getIndexStorageDirOnNode(clusterconfig.Nodes[nodeIndex], t)
	storageDir = filepath.Join(storageDir, "@bhive")
	bucketUUID, err := c.GetBucketUUID(kvaddress, bucketName)
	tc.HandleError(err, "failed to get bucket UUID")
	indexDirPrefix := filepath.Join(storageDir, bucketUUID+"_")
	indexDir, err := getDirWithPrefix(indexDirPrefix)
	FailTestIfError(err, "Failed to get index directory", t)
	log.Printf("Index directory: %s", indexDir)

	ekeyIds, err := getInUseKeyIds(nodeIndex, "service_bucket", bucketUUID)
	tc.HandleError(err, "failed to get in use key ids")

	ekeyId, err := filterNonEmptyKeyId(ekeyIds)
	tc.HandleError(err, "failed to filter non empty key id")

	log.Printf("Basic persistCodebookToDisk...")
	err = waitForCodebookEncryption(indexDir, ekeyId)
	FailTestIfError(err, "Error in verifyCodebookEncryption", t)

	// Crash indexer
	triggerIndexerCrash(nodeIndex)
	time.Sleep(15 * time.Second) // wait for indexer to recover
	secondaryindex.WaitForIndexerActive(clusterconfig.Username, clusterconfig.Password, kvaddress)

	log.Printf("Encrypted codebook crash recovery")
	err = waitForCodebookEncryption(indexDir, ekeyId)
	FailTestIfError(err, "Error in verifyCodebookEncryption", t)

	// Rotate Key & Drop Key
	diffInSeconds := int(time.Since(keyUpdatedTime).Seconds())
	setBypassEncrCfgRestrictions(nodeKv)
	log.Printf("diffInSeconds:%d", diffInSeconds)
	setDekRotationInterval("default", nodeKv, diffInSeconds+10)
	setDekLifetime("default", nodeKv, diffInSeconds+30)

	time.Sleep(15 * time.Second)
	ekeyIds2, err := getInUseKeyIds(nodeIndex, "service_bucket", bucketUUID)
	tc.HandleError(err, "failed to get in use key ids")

	excludeKeyIds := []string{"", ekeyId}
	ekeyId2, err := filterKeyId(ekeyIds2, excludeKeyIds)
	tc.HandleError(err, "failed to filter key id")

	time.Sleep(15 * time.Second)
	log.Printf("Encrypted codebook rotation")
	// Make sure that ekeyId is not being used & ekeyId2 is being used
	err = waitForCodebookEncryption(indexDir, ekeyId2)
	FailTestIfError(err, "Error in verifyCodebookEncryption", t)

	// Set to higher interval as one rotation should have happened
	setDekRotationInterval("default", nodeKv, 86400)
	setDekLifetime("default", nodeKv, 86400)

	// Disable encryption for bucket
	err = updateBucketEncryptionKey(bucketName, nodeIndex, "-1")
	FailTestIfError(err, "Error in updateBucketEncryptionKey", t)

	time.Sleep(10 * time.Second) // wait for key update

	// Disabling encryption only stops future writes from being encrypted.  The
	// codebook is rewritten as plaintext when the DEKs that encrypt it are
	// dropped, so ask for that explicitly.
	err = dropEncryptionAtRestDeks(bucketName, nodeKv)
	FailTestIfError(err, "Error in dropEncryptionAtRestDeks", t)

	err = waitForCodebookDecryption(indexDir)
	FailTestIfError(err, "Error in verifyCodebookDecryption", t)

	// Crash indexer
	triggerIndexerCrash(nodeIndex)
	time.Sleep(15 * time.Second) // wait for indexer to recover
	secondaryindex.WaitForIndexerActive(clusterconfig.Username, clusterconfig.Password, kvaddress)

	err = waitForCodebookDecryption(indexDir)
	FailTestIfError(err, "Error in verifyCodebookDecryption", t)

	// Delete index
	err = secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(err, "Error in DropAllSecondaryIndexes", t)

	// Delete bucket
	kvutility.DeleteBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.CreateBucket(bucketName, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
	time.Sleep(2 * time.Second)

	// Delete bucket encryption key
	err = deleteBucketEncryptionKey(nodeIndex, keyIdStr)
	FailTestIfError(err, "Error in deleteBucketEncryptionKey", t)
}

// This test creates index with encryption disabled & later enabled.
// 1. Create vector index with training documents
// 2. Build the index (persisted codebook will not be encrypted)
// 3. Add bucket encryption key
// 4. Wait for some time
// 5. Verify codebook directory encryption
// 6. Cleanup: delete index, disable encryption, delete encryption key
func TestBhiveCodebookEncryption2(t *testing.T) {
	skipIfNotPlasma(t)

	bucketName := "default"
	nodeKv := 0
	nodeIndex := 1

	kvutility.DeleteBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.CreateBucket(bucketName, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
	time.Sleep(2 * time.Second)

	vectorSetup(t, bucketName, "", "", numDocs)
	idx_bhive := "idx_bhive"

	// Drop all indexes from earlier tests
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	node := clusterconfig.Nodes[nodeIndex]
	// Create vector index
	stmt := fmt.Sprintf("CREATE VECTOR INDEX "+idx_bhive+
		" ON default(sift VECTOR)"+
		" WITH { \"dimension\":128, \"description\": \"IVF,SQ8\", \"similarity\":\"L2_SQUARED\", \"nodes\":[\"%v\"], \"defer_build\":true};", node)
	err := createWithDeferAndBuild(idx_bhive, bucket, "", "", stmt, defaultIndexActiveTimeout*2)
	FailTestIfError(err, "Error in creating idx_sift10k", t)

	info, err := getBucketEncryptionInfo(bucketName, nodeKv)
	if err != nil {
		t.Fatalf("Failed to get bucket encryption info: %v", err)
	}
	log.Printf("encryptionAtRestInfo: %v", info["encryptionAtRestInfo"])

	addBucketEncryptionKey(nodeKv, bucketName, "key1_vec", 30)

	resp, err := getAllEncryptionKeys(nodeIndex)
	log.Printf("All encryption keys: %v", resp)
	FailTestIfError(err, "Error in getAllEncryptionKeys", t)

	keyId := 0
	for _, keymap := range resp {
		usageIfc, ok := keymap["usage"].([]interface{})
		if !ok {
			t.Fatalf("Failed to get usage: %v", keymap["usage"])
		}

		var usage []string
		for _, u := range usageIfc {
			if str, ok := u.(string); ok {
				usage = append(usage, str)
			}
		}
		if hasBucketEncryptionUsage(bucketName, usage) {
			log.Printf("keyId: %v, usage: %v", keymap["id"], usage)
			keyId = max(keyId, int(math.Round(keymap["id"].(float64))))
		}
	}
	keyIdStr := strconv.Itoa(keyId)
	err = updateBucketEncryptionKey(bucketName, nodeIndex, keyIdStr)
	FailTestIfError(err, "Error in updateBucketEncryptionKey", t)
	time.Sleep(10 * time.Second) // wait for key update

	// The codebook was written before encryption was enabled.  Enabling only
	// encrypts future writes, so ask ns_server to convert what is already on
	// disk, which it does by dropping the null key.
	err = forceEncryptionAtRest(bucketName, nodeKv)
	FailTestIfError(err, "Error in forceEncryptionAtRest", t)

	// Verify codebook encryption
	storageDir := getIndexStorageDirOnNode(clusterconfig.Nodes[nodeIndex], t)
	storageDir = filepath.Join(storageDir, "@bhive")
	bucketUUID, err := c.GetBucketUUID(kvaddress, bucketName)
	tc.HandleError(err, "failed to get bucket UUID")
	indexDirPrefix := filepath.Join(storageDir, bucketUUID+"_")
	indexDir, err := getDirWithPrefix(indexDirPrefix)
	FailTestIfError(err, "Failed to get index directory", t)
	log.Printf("Index directory: %s", indexDir)

	ekeyIds, err := getInUseKeyIds(nodeIndex, "service_bucket", bucketUUID)
	tc.HandleError(err, "failed to get in use key ids")

	ekeyId, err := filterNonEmptyKeyId(ekeyIds)
	tc.HandleError(err, "failed to filter non empty key id")

	err = waitForCodebookEncryption(indexDir, ekeyId)
	FailTestIfError(err, "Error in verifyCodebookEncryption", t)
}

func findScanResultFile(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	for _, e := range entries {
		if e.Type().IsRegular() && strings.HasPrefix(e.Name(), "scan-result") {
			return filepath.Join(dir, e.Name()), nil
		}
	}
	return "", fmt.Errorf("no scan-result file found in %s", dir)
}

// Add bucket encryption key
// Modify persistent snapshot settings
// Create index
// Revert persistent snapshot settings
// Verify index encryption
// Delete index
// Delete bucket encryption key
// For plasma log files, there will not be encryption header at start of the file and only keyId can be present at starting of the blocks
func TestIndexEncryptionBhive(t *testing.T) {

	t.Skipf("Test %s can be added after bhive bug fixed", t.Name())
	skipIfNotPlasma(t)

	// Bucket is residing on node n_0 thus bucket encryption info should be fetched from n_0
	bucketName := "default"
	nodeKv := 0

	kvutility.DeleteBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.CreateBucket(bucketName, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
	time.Sleep(2 * time.Second)

	vectorSetup(t, bucketName, "", "", numDocs)

	info, err := getBucketEncryptionInfo(bucketName, nodeKv)
	if err != nil {
		t.Fatalf("Failed to get bucket encryption info: %v", err)
	}

	log.Printf("encryptionAtRestInfo: %v", info["encryptionAtRestInfo"])

	dataStatus, err := getDataStatus(info)
	if err != nil {
		t.Fatalf("Failed to get dataStatus: %v", err)
	}
	log.Printf("Extracted dataStatus: %v", dataStatus)

	dekNumber, err := getDekNumber(info)
	if err != nil {
		t.Fatalf("Failed to get dekNumber: %v", err)
	}
	log.Printf("Extracted dekNumber: %v", dekNumber)

	addBucketEncryptionKey(nodeKv, "default", "key1", 30)
	keyId, err := getEncryptionAtRestKeyId(info)
	if err != nil {
		t.Fatalf("Failed to get encryptionAtRestKeyId: %v", err)
	}
	log.Printf("Current encryptionAtRestKeyId: %v", keyId)

	nodeIndex := 1
	resp, err := getAllEncryptionKeys(nodeIndex)
	log.Printf("All encryption keys: %v", resp)
	FailTestIfError(err, "Error in getAllEncryptionKeys", t)

	keyId = 0
	for _, keymap := range resp {
		usageIfc, ok := keymap["usage"].([]interface{})
		if !ok {
			t.Fatalf("Failed to get usage: %v", keymap["usage"])
		}

		var usage []string
		for _, u := range usageIfc {
			if str, ok := u.(string); ok {
				usage = append(usage, str)
			}
		}
		// verify that the key can be used for encryption of bucket
		if hasBucketEncryptionUsage(bucketName, usage) {
			log.Printf("keyId: %v, usage: %v", keymap["id"], usage)
			keyId = max(keyId, int(math.Round(keymap["id"].(float64))))
		}
	}
	keyIdStr := strconv.Itoa(keyId)
	err = updateBucketEncryptionKey(bucketName, nodeIndex, keyIdStr)
	FailTestIfError(err, "Error in updateBucketEncryptionKey", t)

	// Key Rotation not required for basic test
	// setBypassEncrCfgRestrictions(nodeKv)
	// setDekRotationInterval("default", nodeKv, 60)
	// setDekLifetime("default", nodeKv, 90)

	// Modify settings to trigger encryption of newly created persistent snapshot within minutes
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot_init_build.interval", float64(15), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot.interval", float64(15), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	time.Sleep(7 * time.Second)

	idx_bhive := "idx_bhive"
	node := clusterconfig.Nodes[nodeIndex]
	// Create vector index
	stmt := fmt.Sprintf("CREATE VECTOR INDEX "+idx_bhive+
		" ON default(sift VECTOR)"+
		" WITH { \"dimension\":128, \"description\": \"IVF,SQ8\", \"similarity\":\"L2_SQUARED\", \"nodes\":[\"%v\"], \"defer_build\":true};", node)
	err = createWithDeferAndBuild(idx_bhive, bucket, "", "", stmt, defaultIndexActiveTimeout*2)
	FailTestIfError(err, "Error in creating idx_sift10k", t)

	time.Sleep(15 * time.Second)

	// Revert persistent snapshot settings
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot_init_build.interval", float64(60000), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot.interval", float64(60000), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	bucketUUID, err := c.GetBucketUUID(kvaddress, bucketName)
	tc.HandleError(err, "failed to get bucket UUID")

	storageDir := getIndexStorageDirOnNode(clusterconfig.Nodes[nodeIndex], t)
	storageDir = filepath.Join(storageDir, "@bhive")
	indexDirPrefix := filepath.Join(storageDir, bucketUUID+"_")
	indexDir, err := getDirWithPrefix(indexDirPrefix)
	FailTestIfError(err, "Failed to get index directory", t)
	log.Printf("Index directory: %s", indexDir)

	ekeyIds, err := getInUseKeyIds(nodeIndex, "service_bucket", bucketUUID)
	tc.HandleError(err, "failed to get in use key ids")

	ekeyId, err := filterNonEmptyKeyId(ekeyIds)
	tc.HandleError(err, "failed to filter non empty key id")

	time.Sleep(2*time.Second)
	bhiveEncrypted := verifyBhiveEncryption(indexDir, ekeyId, t)
	if !bhiveEncrypted {
		t.Errorf("Bhive files are NOT encrypted")
	}

	triggerIndexerCrash(nodeIndex)
	time.Sleep(15 * time.Second) // wait for indexer to recover
	secondaryindex.WaitForIndexerActive(clusterconfig.Username, clusterconfig.Password, kvaddress)

	// Verify encryption after recovery
	bhiveEncrypted = verifyBhiveEncryption(indexDir, ekeyId, t)
	if !bhiveEncrypted {
		t.Errorf("Bhive files are NOT encrypted after recovery")
	}

	// Disable encryption for bucket
	err = updateBucketEncryptionKey(bucketName, nodeIndex, "-1")
	FailTestIfError(err, "Error in updateBucketEncryptionKey", t)

	// Delete index
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	// Delete bucket
	kvutility.DeleteBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.CreateBucket(bucketName, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
	time.Sleep(2 * time.Second)

	// Delete bucket encryption key
	err = deleteBucketEncryptionKey(nodeIndex, keyIdStr)
	FailTestIfError(err, "Error in deleteBucketEncryptionKey", t)

}

func TestIndexEncryptionBhiveMultiBucket(t *testing.T) {

	skipIfNotPlasma(t)

	bucket1 := "default"
	bucket2 := "bucket2"
	nodeKv := 0
	nodeIndex := 1

	// Setup buckets
	for _, b := range []string{bucket1, bucket2} {
		kvutility.DeleteBucket(b, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
		kvutility.CreateBucket(b, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
		time.Sleep(2 * time.Second)
		
		kvutility.FlushBucket(b, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
		e := loadVectorData(t, b, "", "", numDocs)
		FailTestIfError(e, "Error in loading vector data", t)	
	}

	defer resetVectorDataSetupFlags()

	// Add encryption keys for both buckets
	addBucketEncryptionKey(nodeKv, bucket1, "key1", 30)
	addBucketEncryptionKey(nodeKv, bucket2, "key2", 30)

	// Function to get the latest key for a bucket from all keys
	getLatestKeyIdForBucket := func(bName string) string {
		resp, _ := getAllEncryptionKeys(nodeIndex)
		var kId int
		for _, keymap := range resp {
			usageIfc := keymap["usage"].([]interface{})
			var usage []string
			for _, u := range usageIfc {
				usage = append(usage, u.(string))
			}
			if hasBucketEncryptionUsage(bName, usage) {
				kId = max(kId, int(math.Round(keymap["id"].(float64))))
			}
		}
		return strconv.Itoa(kId)
	}

	keyId1Str := getLatestKeyIdForBucket(bucket1)
	keyId2Str := getLatestKeyIdForBucket(bucket2)

	updateBucketEncryptionKey(bucket1, nodeIndex, keyId1Str)
	updateBucketEncryptionKey(bucket2, nodeIndex, keyId2Str)

	// Trigger quick snapshots
	secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot_init_build.interval", float64(15), clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot.interval", float64(15), clusterconfig.Username, clusterconfig.Password, kvaddress)
	time.Sleep(7 * time.Second)

	node := clusterconfig.Nodes[nodeIndex]
	idx1 := "idx1_bhive"
	idx2 := "idx2_bhive"

	// Create vector index
	stmt := fmt.Sprintf("CREATE VECTOR INDEX "+idx1+
	" ON default(sift VECTOR)"+
	" WITH { \"dimension\":128, \"description\": \"IVF,SQ8\", \"similarity\":\"L2_SQUARED\", \"nodes\":[\"%v\"], \"defer_build\":true};", node)
	err := createWithDeferAndBuild(idx1, bucket1, "", "", stmt, defaultIndexActiveTimeout*2)
	FailTestIfError(err, "Error in creating idx_sift10k", t)

	time.Sleep(3*time.Second)

	stmt = fmt.Sprintf("CREATE VECTOR INDEX "+idx2+
	" ON default(sift VECTOR)"+
	" WITH { \"dimension\":128, \"description\": \"IVF,SQ8\", \"similarity\":\"L2_SQUARED\", \"nodes\":[\"%v\"], \"defer_build\":false};", node)
	
	_, err = execN1QL(bucket, stmt)
	// err = createWithDeferAndBuild(idx2, bucket2, "", "", stmt, defaultIndexActiveTimeout*2)
	FailTestIfError(err, "Error in creating idx_sift10k", t)

	time.Sleep(45 * time.Second)

	// Revert snapshot settings
	secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot_init_build.interval", float64(60000), clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot.interval", float64(60000), clusterconfig.Username, clusterconfig.Password, kvaddress)

	verifyAndPostCrash := func() {
		for _, b := range []string{bucket1, bucket2} {
			idxName := idx1
			if b == bucket2 {
				idxName = idx2
			}
			storageDir := getIndexStorageDirOnNode(clusterconfig.Nodes[nodeIndex], t)
			storageDir = filepath.Join(storageDir, "@bhive")
			indexDir, _ := getDirWithPrefix(filepath.Join(storageDir, b+"_"+idxName))
			uuid, _ := c.GetBucketUUID(kvaddress, b)
			ids, _ := getInUseKeyIds(nodeIndex, "service_bucket", uuid)
			eid, _ := filterNonEmptyKeyId(ids)

			if !verifyBhiveEncryption(indexDir, eid, t) {
				t.Errorf("Encryption failed for bucket %s index %s", b, idxName)
			}
		}
	}

	log.Printf("Verifying encryption before crash")
	verifyAndPostCrash()

	triggerIndexerCrash(nodeIndex)
	time.Sleep(15 * time.Second)
	secondaryindex.WaitForIndexerActive(clusterconfig.Username, clusterconfig.Password, kvaddress)

	log.Printf("Verifying encryption after recovery")
	verifyAndPostCrash()

	// Cleanup
	secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	for _, b := range []string{bucket1, bucket2} {
		updateBucketEncryptionKey(b, nodeIndex, "-1")
		kvutility.DeleteBucket(b, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	}
	deleteBucketEncryptionKey(nodeIndex, keyId1Str)
	deleteBucketEncryptionKey(nodeIndex, keyId2Str)
}

func TestIndexEncryptionBhiveRotationDrop(t *testing.T) {

	t.Skipf("Test %s can be added after bhive drop key bug fixed", t.Name())

	skipIfNotPlasma(t)

	// Bucket is residing on node n_0 thus bucket encryption info should be fetched from n_0
	bucketName := "default"
	nodeKv := 0

	kvutility.DeleteBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.CreateBucket(bucketName, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
	time.Sleep(2 * time.Second)

	vectorSetup(t, bucketName, "", "", numDocs)

	info, err := getBucketEncryptionInfo(bucketName, nodeKv)
	if err != nil {
		t.Fatalf("Failed to get bucket encryption info: %v", err)
	}

	log.Printf("encryptionAtRestInfo: %v", info["encryptionAtRestInfo"])

	dataStatus, err := getDataStatus(info)
	if err != nil {
		t.Fatalf("Failed to get dataStatus: %v", err)
	}
	log.Printf("Extracted dataStatus: %v", dataStatus)

	dekNumber, err := getDekNumber(info)
	if err != nil {
		t.Fatalf("Failed to get dekNumber: %v", err)
	}
	log.Printf("Extracted dekNumber: %v", dekNumber)

	addBucketEncryptionKey(nodeKv, "default", "key1", 30)
	keyUpdatedTime := time.Now()
	keyId, err := getEncryptionAtRestKeyId(info)
	if err != nil {
		t.Fatalf("Failed to get encryptionAtRestKeyId: %v", err)
	}
	log.Printf("Current encryptionAtRestKeyId: %v", keyId)

	nodeIndex := 1
	resp, err := getAllEncryptionKeys(nodeIndex)
	log.Printf("All encryption keys: %v", resp)
	FailTestIfError(err, "Error in getAllEncryptionKeys", t)

	keyId = 0
	for _, keymap := range resp {
		usageIfc, ok := keymap["usage"].([]interface{})
		if !ok {
			t.Fatalf("Failed to get usage: %v", keymap["usage"])
		}

		var usage []string
		for _, u := range usageIfc {
			if str, ok := u.(string); ok {
				usage = append(usage, str)
			}
		}
		// verify that the key can be used for encryption of bucket
		if hasBucketEncryptionUsage(bucketName, usage) {
			log.Printf("keyId: %v, usage: %v", keymap["id"], usage)
			keyId = max(keyId, int(math.Round(keymap["id"].(float64))))
		}
	}
	keyIdStr := strconv.Itoa(keyId)
	err = updateBucketEncryptionKey(bucketName, nodeIndex, keyIdStr)
	FailTestIfError(err, "Error in updateBucketEncryptionKey", t)

	// Modify settings to trigger encryption of newly created persistent snapshot within minutes
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot_init_build.interval", float64(15), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot.interval", float64(15), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	time.Sleep(7 * time.Second)

	node := clusterconfig.Nodes[nodeIndex]

	idx1 := "idx_bhive_erd"
	// Create vector index
	stmt := fmt.Sprintf("CREATE VECTOR INDEX "+idx1+
	" ON default(sift VECTOR)"+
	" WITH { \"dimension\":128, \"description\": \"IVF,SQ8\", \"similarity\":\"L2_SQUARED\", \"nodes\":[\"%v\"], \"defer_build\":true};", node)
	err = createWithDeferAndBuild(idx1, bucketName, "", "", stmt, defaultIndexActiveTimeout*2)
	FailTestIfError(err, "Error in creating idx_sift10k", t)
	
	time.Sleep(30 * time.Second)

	// Revert moi persistent snapshot settings
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot_init_build.interval", float64(60000), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot.interval", float64(60000), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	bucketUUID, err := c.GetBucketUUID(kvaddress, bucketName)
	tc.HandleError(err, "failed to get bucket UUID")

	storageDir := getIndexStorageDirOnNode(clusterconfig.Nodes[nodeIndex], t)
	storageDir = filepath.Join(storageDir, "@bhive")
	indexDirPrefix := filepath.Join(storageDir, bucketUUID+"_")
	indexDir, err := getDirWithPrefix(indexDirPrefix)
	FailTestIfError(err, "Failed to get index directory", t)
	log.Printf("Index directory: %s", indexDir)

	ekeyIds, err := getInUseKeyIds(nodeIndex, "service_bucket", bucketUUID)
	tc.HandleError(err, "failed to get in use key ids")

	ekeyId, err := filterNonEmptyKeyId(ekeyIds)
	tc.HandleError(err, "failed to filter non empty key id")

	bhiveEncrypted := verifyBhiveEncryption(indexDir, ekeyId, t)
	if !bhiveEncrypted {
		t.Errorf("Bhive files are NOT encrypted")
	}

	// Key Rotation test, plasma must have received newer keys & data should be encrypted using newer keys.
	diffInSeconds := int(time.Since(keyUpdatedTime).Seconds())
	setBypassEncrCfgRestrictions(nodeKv)
	log.Printf("diffInSeconds:%d", diffInSeconds)
	setDekRotationInterval("default", nodeKv, diffInSeconds+5)
	setDekLifetime("default", nodeKv, diffInSeconds+20)

	time.Sleep(6 * time.Second)
	ekeyIds2, err := getInUseKeyIds(nodeIndex, "service_bucket", bucketUUID)
	tc.HandleError(err, "failed to get in use key ids")

	//Set to higher interval as one rotation should have happened
	setDekRotationInterval("default", nodeKv, 86400)
	setDekLifetime("default", nodeKv, 86400)

	excludeKeyIds := []string{"", ekeyId}
	ekeyId2, err := filterKeyId(ekeyIds2, excludeKeyIds)
	tc.HandleError(err, "failed to filter key id")

	bhiveEncrypted = verifyBhiveEncryption(indexDir, ekeyId2, t)
	if !bhiveEncrypted {
		t.Errorf("Bhive files are NOT encrypted with correct key")
	}

	time.Sleep(11 * time.Second)
	// Skip this check: MB-71420
	// ekeyId should have been dropped by now.
	// encryptedWithDroppedKey := verifyBhiveEncryptionWithDroppedKey(indexDir, ekeyId, t)
	// if encryptedWithDroppedKey {
	// 	t.Errorf("Plasma files are encrypted with dropped key")
	// }

	triggerIndexerCrash(nodeIndex)
	time.Sleep(10 * time.Second) // wait for indexer to recover
	secondaryindex.WaitForIndexerActive(clusterconfig.Username, clusterconfig.Password, kvaddress)
	bhiveEncrypted = verifyBhiveEncryption(indexDir, ekeyId2, t)
	if !bhiveEncrypted {
		t.Errorf("Bhive files are NOT encrypted with correct key after recovery")
	}

	// Skip this check: MB-71420
	// encryptedWithDroppedKey = verifyBhiveEncryptionWithDroppedKey(indexDir, ekeyId, t)
	// if encryptedWithDroppedKey {
	// 	t.Errorf("Plasma files are encrypted with dropped key")
	// }

	// Disable encryption for bucket
	err = updateBucketEncryptionKey(bucketName, nodeIndex, "-1")
	FailTestIfError(err, "Error in updateBucketEncryptionKey", t)

	// Delete index
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	// Delete bucket
	kvutility.DeleteBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.CreateBucket(bucketName, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
	time.Sleep(2 * time.Second)

	// Delete bucket encryption key
	err = deleteBucketEncryptionKey(nodeIndex, keyIdStr)
	FailTestIfError(err, "Error in deleteBucketEncryptionKey", t)

}

func TestBackfillEncryption(t *testing.T) {

	skipIfForestdb(t)
	nodeKv := 0
	bucketName := "default"
	kvutility.DeleteBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.CreateBucket(bucketName, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
	time.Sleep(4 * time.Second)

	docs := 10000 // # docs to create
	log.Printf("%v Creating %v documents", "TestBackfillEncryption", docs)
	CreateDocs(docs)
	log.Printf("%v %v documents created", "TestBackfillEncryption", docs)

	addBucketEncryptionKey(nodeKv, "default", "key1", 30)

	nodeIndex := 1
	resp, err := getAllEncryptionKeys(nodeIndex)
	log.Printf("All encryption keys: %v", resp)
	FailTestIfError(err, "Error in getAllEncryptionKeys", t)

	keyId := 0
	for _, keymap := range resp {
		usageIfc, ok := keymap["usage"].([]interface{})
		if !ok {
			t.Fatalf("Failed to get usage: %v", keymap["usage"])
		}

		var usage []string
		for _, u := range usageIfc {
			if str, ok := u.(string); ok {
				usage = append(usage, str)
			}
		}
		// verify that the key can be used for encryption of bucket
		if hasBucketEncryptionUsage(bucketName, usage) {
			log.Printf("keyId: %v, usage: %v", keymap["id"], usage)
			keyId = max(keyId, int(math.Round(keymap["id"].(float64))))
		}
	}
	keyIdStr := strconv.Itoa(keyId)
	err = updateBucketEncryptionKey(bucketName, nodeIndex, keyIdStr)
	FailTestIfError(err, "Error in updateBucketEncryptionKey", t)

	node := clusterconfig.Nodes[nodeIndex]
	with := "{\"nodes\": [\"" + node + "\"]}"
	// Create index
	indexName := "idx_backfill_age"
	err = secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress,
		"", []string{"age"}, false, []byte(with), false, 60, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.ChangeIndexerSettings("indexer.queryport.backfill_pause_test_duration", 10, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")
	defer func() {
		err = secondaryindex.ChangeIndexerSettings("indexer.queryport.backfill_pause_test_duration", 0, clusterconfig.Username, clusterconfig.Password, kvaddress)
		tc.HandleError(err, "Error in ChangeIndexerSettings")
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	respCh := make(chan error, 1)
	go func(respCh chan error) {
		defer wg.Done()
		time.Sleep(4 * time.Second)
		backfillTempDir := getBackfillTempDir(t)
		path, err := findScanResultFile(backfillTempDir)
		if err != nil {
			err = fmt.Errorf("error finding backfill file path:%v err:%v", path, err)
			respCh <- err
			return
		}
		encrypted, err := IsFileEncrypted(path)
		if err != nil {
			err = fmt.Errorf("error checking if file %s is encrypted: %v", path, err)
			respCh <- err
			return
		}
		log.Printf("Backfill encryption:%v expected:true", encrypted)
		if !encrypted {
			err = fmt.Errorf("backfill file path:%s backfillTempDir:%v NOT encrypted", path, backfillTempDir)
			respCh <- err
			return
		}
		respCh <- err
	}(respCh)
	// With backfill encryption
	n1qlstatement := "select * from " + bucketName + " where age > 10 and age < 90"
	scanResults1, err := tc.ExecuteN1QLStatement(clusterconfig.KVAddress, clusterconfig.Username, clusterconfig.Password, bucketName, n1qlstatement, false, gocb.RequestPlus)
	FailTestIfError(err, "Error in query execution", t)
	log.Printf("Results n1ql count with encryption:%v", len(scanResults1))

	wg.Wait()
	err = <-respCh
	//skip check if no file found
	if err != nil && !strings.HasPrefix(err.Error(), "error finding backfill file") {
		FailTestIfError(err, "Error in scan result validation with encryption", t)
	}

	time.Sleep(10 * time.Second)

	wg.Add(1)
	// Without backfill encryption
	// Delete bucket encryption key
	err = updateBucketEncryptionKey(bucketName, nodeIndex, "-1")
	time.Sleep(3 * time.Second)
	FailTestIfError(err, "Error in updateBucketEncryptionKey", t)
	go func(respCh chan error) {
		defer wg.Done()
		time.Sleep(4 * time.Second)
		backfillTempDir := getBackfillTempDir(t)
		path, err := findScanResultFile(backfillTempDir)
		if err != nil {
			err = fmt.Errorf("error finding backfill file path:%v err:%v", path, err)
			respCh <- err
			return
		}
		encrypted, err := IsFileEncrypted(path)
		if err != nil {
			err = fmt.Errorf("error checking if file %s is encrypted: %v", path, err)
			respCh <- err
			return
		}
		log.Printf("Backfill encryption:%v expected:false", encrypted)
		if encrypted {
			err = fmt.Errorf("backfill file path:%s backfillTempDir:%v encrypted", path, backfillTempDir)
			respCh <- err
			return
		}
		respCh <- err
	}(respCh)
	n1qlstatement = "select * from " + bucketName + " where age > 10 and age < 90"
	scanResults2, err := tc.ExecuteN1QLStatement(clusterconfig.KVAddress, clusterconfig.Username, clusterconfig.Password, bucketName, n1qlstatement, false, gocb.RequestPlus)
	FailTestIfError(err, "Error in scan without encryption", t)
	log.Printf("Results n1ql count without encryption:%v", len(scanResults2))

	wg.Wait()
	err = <-respCh
	//skip check if no file found
	if err != nil && !strings.HasPrefix(err.Error(), "error finding backfill file") {
		FailTestIfError(err, "Error in scan result validation without encryption", t)
	}
	if len(scanResults1) != len(scanResults2) {
		FailTestIfError(err, "Error in scan result counts", t)
	}

	// Disable encryption for bucket
	err = updateBucketEncryptionKey(bucketName, nodeIndex, "-1")
	FailTestIfError(err, "Error in updateBucketEncryptionKey", t)

	kvutility.DeleteBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.CreateBucket(bucketName, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
	time.Sleep(4 * time.Second)
	vectorsLoaded = false
}

// Every rotated indexer_stats.log slot stays encrypted with whatever log DEK was
// active when it was written. Those DEKs must stay reported as in-use, since
// ns_server deletes any key no service claims and the slot then cannot be read.

const logDekKeyType = "log"

const indexerStatsLogName = "indexer_stats.log"

func getLogDirOnNode(nodeIndex int, t *testing.T) string {
	if nodeIndex < 0 || nodeIndex >= len(clusterconfig.Nodes) {
		t.Fatalf("invalid node index %d", nodeIndex)
	}

	workspace := os.Getenv("WORKSPACE")
	if workspace == "" {
		workspace = "../../../../../../../../"
	}
	if !strings.HasSuffix(workspace, "/") {
		workspace += "/"
	}

	dir, err := filepath.Abs(fmt.Sprintf("%sns_server/logs/n_%d", workspace, nodeIndex))
	FailTestIfError(err, "Error resolving log dir", t)
	return dir
}

// statsLogKeyIdsOnDisk returns the keys the stats log files need: the active
// file plus every rotated slot. An unencrypted slot contributes "" (NULL_DEK).
func statsLogKeyIdsOnDisk(logDir, baseName string, t *testing.T) map[string]bool {
	slots, err := filepath.Glob(filepath.Join(logDir, baseName+"*"))
	FailTestIfError(err, "Error listing stats log slots", t)

	ids := make(map[string]bool)
	for _, path := range slots {
		if strings.HasSuffix(path, ".tmp") {
			continue
		}
		// Anything shorter than a full header carries no key id. Skipping keeps
		// the scan off the short read window just after a file is created.
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			t.Fatalf("cannot stat stats log slot %s: %v", path, err)
		}
		if info.Size() < int64(FILEHDR_SZ) {
			continue
		}
		encrypted, err := IsFileEncrypted(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			t.Fatalf("cannot read stats log slot %s: %v", path, err)
		}
		if !encrypted {
			ids[""] = true
			continue
		}
		keyId, err := GetFileEncryptionKeyId(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			t.Fatalf("cannot read key id from stats log slot %s: %v", path, err)
		}
		ids[keyId] = true
	}
	return ids
}

func postEncrAtRestSettings(nodeIndex int, data url.Values) error {
	if nodeIndex < 0 || nodeIndex >= len(clusterconfig.Nodes) {
		return fmt.Errorf("invalid node index %d", nodeIndex)
	}

	hostaddress := clusterconfig.Nodes[nodeIndex]
	client := &http.Client{}
	address := "http://" + hostaddress + "/settings/security/encryptionAtRest"

	req, err := http.NewRequest("POST", address, strings.NewReader(data.Encode()))
	if err != nil {
		return err
	}
	req.SetBasicAuth(clusterconfig.Username, clusterconfig.Password)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("request to %s failed with status code %d, body:%s",
			address, resp.StatusCode, string(body))
	}
	return nil
}

func enableLogEncryption(nodeIndex int) error {
	data := url.Values{}
	data.Set("log.encryptionMethod", "nodeSecretManager")
	return postEncrAtRestSettings(nodeIndex, data)
}

func disableLogEncryption(nodeIndex int) error {
	data := url.Values{}
	data.Set("log.encryptionMethod", "disabled")
	return postEncrAtRestSettings(nodeIndex, data)
}

// setLogDekRotation needs setBypassEncrCfgRestrictions, as these intervals are
// well below the minimums ns_server enforces.
func setLogDekRotation(nodeIndex int, intervalSec, lifetimeSec int) error {
	data := url.Values{}
	data.Set("log.dekLifetime", fmt.Sprintf("%d", lifetimeSec))
	data.Set("log.dekRotationInterval", fmt.Sprintf("%d", intervalSec))
	return postEncrAtRestSettings(nodeIndex, data)
}

func waitForDistinctStatsLogKeys(logDir string, want int, timeout time.Duration, t *testing.T) map[string]bool {
	deadline := time.Now().Add(timeout)
	var onDisk map[string]bool
	for time.Now().Before(deadline) {
		onDisk = statsLogKeyIdsOnDisk(logDir, indexerStatsLogName, t)
		if len(onDisk) >= want {
			return onDisk
		}
		time.Sleep(5 * time.Second)
	}
	t.Fatalf("timed out waiting for %d distinct stats log keys in %s; got %v",
		want, logDir, keySetToSlice(onDisk))
	return nil
}

func keySetToSlice(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// assertInUseCoversDisk checks the reported set is a superset of what is on
// disk. Extra keys only delay reclaiming them; a missing one gets collected and
// takes the slot with it.
func assertInUseCoversDisk(nodeIndex int, logDir, stage string, t *testing.T) {
	onDisk := statsLogKeyIdsOnDisk(logDir, indexerStatsLogName, t)
	if len(onDisk) == 0 {
		t.Fatalf("[%s] no stats log files found under %s", stage, logDir)
	}

	reported, err := getInUseKeyIds(nodeIndex, logDekKeyType, "")
	FailTestIfError(err, fmt.Sprintf("[%s] Error in getInUseKeyIds for log DEKs", stage), t)

	reportedSet := make(map[string]bool, len(reported))
	for _, k := range reported {
		reportedSet[k] = true
	}

	var missing []string
	for k := range onDisk {
		if !reportedSet[k] {
			missing = append(missing, k)
		}
	}
	sort.Strings(missing)

	log.Printf("[%s] stats log keys on disk: %v", stage, keySetToSlice(onDisk))
	log.Printf("[%s] keys reported in use  : %v", stage, reported)

	if len(missing) > 0 {
		t.Fatalf("[%s] %d key(s) held by stats log files are not reported in use: %v. "+
			"ns_server will garbage collect these and the files become undecryptable. "+
			"on disk=%v reported=%v",
			stage, len(missing), missing, keySetToSlice(onDisk), reported)
	}
}

// setupStatsLogKeyRotation enables log encryption with a short DEK rotation
// interval and restores both on cleanup.
func setupStatsLogKeyRotation(nodeIndex int, t *testing.T) string {
	err := setBypassEncrCfgRestrictions(nodeIndex)
	FailTestIfError(err, "Error in setBypassEncrCfgRestrictions", t)

	err = enableLogEncryption(nodeIndex)
	FailTestIfError(err, "Error enabling log encryption", t)

	err = setLogDekRotation(nodeIndex, 30, 3600)
	FailTestIfError(err, "Error in setLogDekRotation", t)

	t.Cleanup(func() {
		if err := setLogDekRotation(nodeIndex, 0, 0); err != nil {
			log.Printf("Warning: could not reset log dek rotation: %v", err)
		}
		if err := disableLogEncryption(nodeIndex); err != nil {
			log.Printf("Warning: could not disable log encryption: %v", err)
		}
	})

	return getLogDirOnNode(nodeIndex, t)
}

// Keys held by rotated slots must be reported, not just the key backing the
// file currently being written.
func TestStatsLogInUseKeysRotatedSlots(t *testing.T) {
	nodeIndex := 1

	logDir := setupStatsLogKeyRotation(nodeIndex, t)

	// Three keys means at least two rotated slots hold something other than the
	// active key.
	onDisk := waitForDistinctStatsLogKeys(logDir, 3, 5*time.Minute, t)
	log.Printf("Stats log files are spread across %d keys: %v", len(onDisk), keySetToSlice(onDisk))

	assertInUseCoversDisk(nodeIndex, logDir, "after rotation", t)
}

// recoverInUseKeys asks for the in-use set once at startup and treats the answer
// as final, and the restart itself triggers a GC, so a short answer here is when
// the keys actually get deleted.
func TestStatsLogInUseKeysAfterIndexerRestart(t *testing.T) {
	nodeIndex := 1

	logDir := setupStatsLogKeyRotation(nodeIndex, t)
	waitForDistinctStatsLogKeys(logDir, 3, 5*time.Minute, t)
	assertInUseCoversDisk(nodeIndex, logDir, "before restart", t)

	beforeRestart := statsLogKeyIdsOnDisk(logDir, indexerStatsLogName, t)

	log.Printf("Restarting indexer on node %s", clusterconfig.Nodes[nodeIndex])
	// forceKillIndexer settles for 20s; WaitForIndexerActive panics instead of
	// retrying if the indexer is not listening yet.
	forceKillIndexer()

	err := secondaryindex.WaitForIndexerActive(clusterconfig.Username,
		clusterconfig.Password, clusterconfig.Nodes[nodeIndex])
	FailTestIfError(err, "Indexer did not come back up after restart", t)

	assertInUseCoversDisk(nodeIndex, logDir, "after restart", t)

	stillOnDisk := statsLogKeyIdsOnDisk(logDir, indexerStatsLogName, t)
	reported, err := getInUseKeyIds(nodeIndex, logDekKeyType, "")
	FailTestIfError(err, "Error in getInUseKeyIds after restart", t)

	reportedSet := make(map[string]bool, len(reported))
	for _, k := range reported {
		reportedSet[k] = true
	}
	for k := range beforeRestart {
		if !stillOnDisk[k] {
			continue // slot aged out, key is releasable
		}
		if !reportedSet[k] {
			t.Fatalf("key %q survived the restart on disk but was dropped from the "+
				"in-use set; recovery forgot it. on disk=%v reported=%v",
				k, keySetToSlice(stillOnDisk), reported)
		}
	}
}

// getNumCommits returns the num_commits stat for an index. num_commits is
// incremented once per snapshot that is committed to disk, so it goes up by one
// for every recovery point created for a bhive index.
func getNumCommits(t *testing.T, indexName, bucketName string) int64 {
	stats := secondaryindex.GetIndexStats(indexName, bucketName,
		clusterconfig.Username, clusterconfig.Password, kvaddress)

	statKey := bucketName + ":" + indexName + ":num_commits"
	val, ok := stats[statKey]
	if !ok {
		t.Fatalf("stat %v not found for index %v", statKey, indexName)
	}

	numCommits, ok := val.(float64)
	if !ok {
		t.Fatalf("stat %v has unexpected type %T", statKey, val)
	}
	return int64(numCommits)
}

// bhiveKeyHeaders holds the number of bhive files found per encryption key id,
// counted separately for the live data files and for the files of the recovery
// points. The recovery points are the ones the drop key flow waits to be
// recreated, so they are the interesting half after a drop.
type bhiveKeyHeaders struct {
	data     map[string]int
	recovery map[string]int
}

// logBhiveKeyHeaders walks the bhive files of an index and logs the encryption
// key id present in each file header. Unlike verifyBhiveEncryption the recovery
// directories are walked as well, so that a caller can assert that a dropped
// key is gone from the recovery points and not only from the live data.
func logBhiveKeyHeaders(indexDir, label string, t *testing.T) bhiveKeyHeaders {

	dirsToCheck := []string{
		filepath.Join(indexDir, "mainIndex"),
		filepath.Join(indexDir, "docIndex"),
	}

	headers := bhiveKeyHeaders{
		data:     make(map[string]int),
		recovery: make(map[string]int),
	}

	for _, dir := range dirsToCheck {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			continue
		}

		// Walk recurses into <dir>/recovery, so the recovery points are covered
		// without listing them separately and without counting a file twice.
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			if matched, _ := filepath.Match("log.*.data", filepath.Base(path)); !matched {
				return nil
			}

			// An unencrypted file has no key id header, reading one out of it
			// gives a garbage length. Counted under "" as elsewhere in this file.
			encrypted, err := IsFileEncrypted(path)
			if err != nil {
				t.Errorf("Error checking if %v is encrypted: %v", path, err)
				return nil
			}

			keyId := ""
			if encrypted {
				keyId, err = GetFileEncryptionKeyId(path)
				if err != nil {
					t.Errorf("Error reading key header of %v: %v", path, err)
					return nil
				}
			}

			fileType := "data"
			if strings.Contains(path, string(os.PathSeparator)+"recovery"+string(os.PathSeparator)) {
				fileType = "recovery"
				headers.recovery[keyId]++
			} else {
				headers.data[keyId]++
			}

			if encrypted {
				log.Printf("%v: %v file %v header keyId %q", label, fileType, path, keyId)
			} else {
				log.Printf("%v: %v file %v is NOT encrypted", label, fileType, path)
			}
			return nil
		})
		if err != nil {
			t.Errorf("Error walking directory %v: %v", dir, err)
		}
	}

	log.Printf("%v: key header summary(keyId -> num files) data %v recovery %v",
		label, headers.data, headers.recovery)
	return headers
}

// findFilesWithKey returns the bhive files, live data and recovery points both,
// whose contents mention keyId anywhere. logBhiveKeyHeaders only reads the key
// the file is encrypted with, this is the stronger check that no trace of a
// dropped key is left behind in any record.
//
// A file which is not encrypted cannot be holding the dropped key, and the
// content match is a plain substring search which could hit the key id by
// chance in plaintext, so such files are counted as having no trace.
func findFilesWithKey(indexDir, keyId string, t *testing.T) []string {

	dirsToCheck := []string{
		filepath.Join(indexDir, "mainIndex"),
		filepath.Join(indexDir, "docIndex"),
	}

	var found []string
	for _, dir := range dirsToCheck {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			continue
		}

		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			if matched, _ := filepath.Match("log.*.data", filepath.Base(path)); !matched {
				return nil
			}

			encrypted, err := IsFileEncrypted(path)
			if err != nil {
				t.Errorf("Error checking if %v is encrypted: %v", path, err)
				return nil
			}
			if !encrypted {
				log.Printf("File %v is not encrypted, no trace of key %v", path, keyId)
				return nil
			}

			hasKey, err := FileHasKey(path, keyId)
			if err != nil {
				t.Errorf("Error checking if %v has key %v: %v", path, keyId, err)
				return nil
			}
			if hasKey {
				found = append(found, path)
			}
			return nil
		})
		if err != nil {
			t.Errorf("Error walking directory %v: %v", dir, err)
		}
	}
	return found
}

// TestIndexEncryptionBhiveDropKeyIdleBucket verifies that a drop key completes
// for a bucket with a bhive index which is not receiving any mutations.
//
// After DropKeys, storage manager waits for 3 successive recovery points per
// bhive slice so that the recovery points holding the dropped key are cleaned
// up. A keyspace with no incoming mutations does not generate a stability TS
// and hence creates no disk snapshot(and no recovery point) on its own, so this
// wait used to never finish. Timekeeper now forces a commit for such a keyspace
// every indexer.timekeeper.forceCommitInterval while the drop key is waiting.
//
// The test sets the forced commit interval to its minimum of 2 minutes and the
// persisted snapshot interval well beyond the test duration, so every recovery
// point observed here can only have come from a forced commit.
func TestIndexEncryptionBhiveDropKeyIdleBucket(t *testing.T) {

	skipIfNotPlasma(t)

	const forceCommitInterval = 2 * time.Minute // MIN_FORCE_COMMIT_INTERVAL
	const numRPsAwaited = 3                     // RPs storage manager waits for post DropKeys
	const persistInterval = 600000              // 10 minutes, must outlast the test

	// numRPsAwaited forced commits take numRPsAwaited*forceCommitInterval as the
	// first one is due one interval after the last persist. 15 seconds of slack
	// is allowed on top for the drop key to reach storage manager and for the
	// stats of the last commit to be published.
	const rpWaitTimeout = numRPsAwaited*forceCommitInterval + 15*time.Second

	// time allowed for the rotated out key to expire and be dropped
	const dekDropWait = 45 * time.Second

	// settle time after the last recovery point, so that its cleanup of the
	// older recovery points is on disk before the files are checked
	const postRPSettle = 40 * time.Second

	bucketName := "default"
	nodeKv := 0
	nodeIndex := 1
	idx1 := "idx_bhive_idle_dropkey"

	kvutility.DeleteBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.CreateBucket(bucketName, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
	time.Sleep(2 * time.Second)

	vectorSetup(t, bucketName, "", "", numDocs)

	addBucketEncryptionKey(nodeKv, bucketName, "key1", 30)
	keyUpdatedTime := time.Now()

	resp, err := getAllEncryptionKeys(nodeIndex)
	FailTestIfError(err, "Error in getAllEncryptionKeys", t)

	keyId := 0
	for _, keymap := range resp {
		usageIfc, ok := keymap["usage"].([]interface{})
		if !ok {
			t.Fatalf("Failed to get usage: %v", keymap["usage"])
		}

		var usage []string
		for _, u := range usageIfc {
			if str, ok := u.(string); ok {
				usage = append(usage, str)
			}
		}
		if hasBucketEncryptionUsage(bucketName, usage) {
			keyId = max(keyId, int(math.Round(keymap["id"].(float64))))
		}
	}
	keyIdStr := strconv.Itoa(keyId)
	err = updateBucketEncryptionKey(bucketName, nodeIndex, keyIdStr)
	FailTestIfError(err, "Error in updateBucketEncryptionKey", t)

	defer func() {
		err := updateBucketEncryptionKey(bucketName, nodeIndex, "-1")
		tc.HandleError(err, "Error in reverting bucket encryption key")

		e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
		tc.HandleError(e, "Error in DropAllSecondaryIndexes")

		kvutility.DeleteBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
		kvutility.CreateBucket(bucketName, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
		time.Sleep(2 * time.Second)

		err = deleteBucketEncryptionKey(nodeIndex, keyIdStr)
		tc.HandleError(err, "Error in deleteBucketEncryptionKey")
	}()

	node := clusterconfig.Nodes[nodeIndex]
	stmt := fmt.Sprintf("CREATE VECTOR INDEX "+idx1+
		" ON default(sift VECTOR)"+
		" WITH { \"dimension\":128, \"description\": \"IVF,SQ8\", \"similarity\":\"L2_SQUARED\", \"nodes\":[\"%v\"], \"defer_build\":true};", node)
	err = createWithDeferAndBuild(idx1, bucketName, "", "", stmt, defaultIndexActiveTimeout*2)
	FailTestIfError(err, "Error in creating "+idx1, t)

	// From here on no more documents are loaded, so the keyspace is idle.

	// The persist interval has to outlast the test so that a recovery point can
	// only come from a forced commit. For plasma/bhive the moi key is the one
	// which is read, indexer.settings.persisted_snapshot.interval is not mapped
	// to it.
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot.moi.interval",
		float64(persistInterval), clusterconfig.Username, clusterconfig.Password, kvaddress)
	FailTestIfError(err, "Error in setting persisted_snapshot.moi.interval", t)

	err = secondaryindex.ChangeIndexerSettings("indexer.timekeeper.forceCommitInterval",
		float64(forceCommitInterval/time.Millisecond), clusterconfig.Username, clusterconfig.Password, kvaddress)
	FailTestIfError(err, "Error in setting timekeeper.forceCommitInterval", t)

	defer func() {
		err := secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot.moi.interval",
			float64(600000), clusterconfig.Username, clusterconfig.Password, kvaddress)
		tc.HandleError(err, "Error in reverting persisted_snapshot.moi.interval")

		err = secondaryindex.ChangeIndexerSettings("indexer.timekeeper.forceCommitInterval",
			float64(300000), clusterconfig.Username, clusterconfig.Password, kvaddress)
		tc.HandleError(err, "Error in reverting timekeeper.forceCommitInterval")
	}()

	// Let the snapshots of the build settle before taking the baseline.
	time.Sleep(60 * time.Second)

	// Encryption was enabled before the index was created, so everything the
	// build wrote has to be encrypted with the in use key.
	bucketUUID, err := c.GetBucketUUID(kvaddress, bucketName)
	FailTestIfError(err, "Failed to get bucket UUID", t)

	storageDir := getIndexStorageDirOnNode(clusterconfig.Nodes[nodeIndex], t)
	indexDir, err := getDirWithPrefix(filepath.Join(storageDir, "@bhive", bucketUUID+"_"))
	FailTestIfError(err, "Failed to get index directory", t)
	log.Printf("%v: index directory %v", t.Name(), indexDir)

	ekeyIds, err := getInUseKeyIds(nodeIndex, "service_bucket", bucketUUID)
	FailTestIfError(err, "Failed to get in use key ids", t)

	keyBeforeDrop, err := filterNonEmptyKeyId(ekeyIds)
	FailTestIfError(err, "Failed to filter non empty key id", t)
	log.Printf("%v: index built with in use key %q", t.Name(), keyBeforeDrop)

	if !verifyBhiveEncryption(indexDir, keyBeforeDrop, t) {
		t.Fatalf("Bhive index data is NOT encrypted with key %v after the build",
			keyBeforeDrop)
	}

	baseline := getNumCommits(t, idx1, bucketName)
	log.Printf("%v: num_commits after build is %v", t.Name(), baseline)

	// Control check - with no drop key in progress an idle keyspace must not
	// commit at all. Anything committed here would make the counts below
	// meaningless.
	time.Sleep(forceCommitInterval + 30*time.Second)
	idleCommits := getNumCommits(t, idx1, bucketName)
	if idleCommits != baseline {
		t.Fatalf("Idle keyspace committed without a drop key in progress. "+
			"num_commits went from %v to %v", baseline, idleCommits)
	}
	log.Printf("%v: num_commits still %v after an idle interval", t.Name(), idleCommits)

	// Drop the DEK the index data is encrypted with. Rotate first as the active
	// key cannot be dropped, then expire the rotated out key.
	diffInSeconds := int(time.Since(keyUpdatedTime).Seconds())
	setBypassEncrCfgRestrictions(nodeKv)
	setDekRotationInterval(bucketName, nodeKv, diffInSeconds+5)
	setDekLifetime(bucketName, nodeKv, diffInSeconds+20)

	defer func() {
		setDekRotationInterval(bucketName, nodeKv, 86400)
		setDekLifetime(bucketName, nodeKv, 86400)
	}()

	// Give the rotated out key time to expire so that the drop key is issued to
	// the indexer. rpWaitTimeout below covers only the recovery point wait and
	// not this, so it is waited out separately.
	log.Printf("%v: waiting %v for the dropped key to reach the indexer",
		t.Name(), dekDropWait)
	time.Sleep(dekDropWait)

	dropKeyStart := time.Now()
	deadline := dropKeyStart.Add(rpWaitTimeout)
	log.Printf("%v: waiting up to %v for %v recovery points at %v intervals",
		t.Name(), rpWaitTimeout, numRPsAwaited, forceCommitInterval)

	// Every forced commit creates one recovery point per bhive slice, so
	// num_commits has to go up by numRPsAwaited.
	lastCommits := baseline
	lastCommitTime := dropKeyStart

	for lastCommits-baseline < numRPsAwaited && time.Now().Before(deadline) {
		time.Sleep(10 * time.Second)

		currCommits := getNumCommits(t, idx1, bucketName)
		if currCommits == lastCommits {
			continue
		}

		log.Printf("%v: num_commits %v -> %v after %v", t.Name(), lastCommits,
			currCommits, time.Since(lastCommitTime).Round(time.Second))
		lastCommits = currCommits
		lastCommitTime = time.Now()
	}

	numForced := lastCommits - baseline
	elapsed := time.Since(dropKeyStart).Round(time.Second)

	if numForced < numRPsAwaited {
		t.Fatalf("Only %v of %v recovery points created in %v after dropKey. An idle "+
			"keyspace is not being forced to commit, dropKey would wait indefinitely.",
			numForced, numRPsAwaited, elapsed)
	}

	log.Printf("%v: %v recovery points created in %v after dropKey", t.Name(),
		numForced, elapsed)

	time.Sleep(postRPSettle)

	// The forced commits re-encrypted the data with the new key and gave
	// cleanupOldRecoveryPoints the 3 rounds it needs to purge the recovery
	// points holding the dropped key. Log what every file is encrypted with now
	// and require that nothing mentions the dropped key any more, in the live
	// data or in the recovery points.
	logBhiveKeyHeaders(indexDir, "after drop", t)

	if leftOver := findFilesWithKey(indexDir, keyBeforeDrop, t); len(leftOver) != 0 {
		t.Errorf("Dropped key %v is still present in %v bhive files after %v recovery "+
			"points: %v", keyBeforeDrop, len(leftOver), numForced, leftOver)
	} else {
		log.Printf("%v: no trace of the dropped key %v is left in %v", t.Name(),
			keyBeforeDrop, indexDir)
	}
}
