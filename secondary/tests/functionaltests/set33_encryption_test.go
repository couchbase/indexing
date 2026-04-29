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
	"testing"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
)

// copied from gocbcryto/writer.go
const FILEHDR_MAGIC = "\x00Couchbase Encrypted\x00" // file is encrypted

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
	bucketUUID, err := c.GetBucketUUID(kvaddress, bucket)
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

	bucketUUID, err := c.GetBucketUUID(kvaddress, bucket)
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

	bucketUUID, err := c.GetBucketUUID(kvaddress, bucket)
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
