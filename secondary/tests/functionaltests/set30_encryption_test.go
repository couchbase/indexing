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
func TestIndexEncryption(t *testing.T) {

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
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot_init_build.moi.interval", float64(45), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot.moi.interval", float64(45), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	node := clusterconfig.Nodes[nodeIndex]
	with := "{\"nodes\": [\"" + node + "\"]}"
	// Create index
	indexName := "idx_encr_age"
	err = secondaryindex.CreateSecondaryIndex(indexName, "default", indexManagementAddress,
		"", []string{"age"}, false, []byte(with), false, 60, nil)
	FailTestIfError(err, "Error in creating the index", t)

	time.Sleep(1 * time.Minute)

	// Revert moi persistent snapshot settings
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot_init_build.moi.interval", float64(60000), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.persisted_snapshot.moi.interval", float64(60000), clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	storageDir := getIndexStorageDirOnNode(clusterconfig.Nodes[nodeIndex], t)
	indexDirPrefix := filepath.Join(storageDir, bucketName+"_"+indexName)
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
	verifySnapshotEncryption(snapshotDir, t)

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

func verifySnapshotEncryption(snapshotDir string, t *testing.T) {
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
