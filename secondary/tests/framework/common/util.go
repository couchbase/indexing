package common

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	"gopkg.in/couchbase/gocb.v1"
)

var LogPerformanceStat = false

// ToDo: Point out the exact difference between two responses
func PrintScanResults(results ScanResponse, resultType string) {
	log.Printf("Count of %v is %d\n", resultType, len(results))
	for key, value := range results {
		log.Printf("Key: %v  Value: %v", key, value)
	}
}

func PrintArrayScanResults(results ArrayIndexScanResponse, resultType string) {
	log.Printf("Count of %v is %d\n", resultType, len(results))
	for key, value := range results {
		log.Printf("Key: %v  Value: %v", key, value)
	}
}

func PrintGroupAggrResults(results GroupAggrScanResponse, resultType string) {
	log.Printf("Count of %v is %d\n", resultType, len(results))
	for _, value := range results {
		log.Printf("Value: %v", value)
	}
}

func PrintScanResultsActual(results ScanResponseActual, resultType string) {
	log.Printf("Count of %v is %d\n", resultType, len(results))
	for key, value := range results {
		log.Printf("Key: %T %v  Value: %T %v %v", key, key, value, value, value == nil)
	}
}

func PrintArrayScanResultsActual(results ArrayIndexScanResponseActual, resultType string) {
	log.Printf("Count of %v is %d\n", resultType, len(results))
	for key, value := range results {
		log.Printf("Key: %v  Value: %v", key, value)
	}
}

func PrintGroupAggrResultsActual(results GroupAggrScanResponseActual, resultType string) {
	log.Printf("Count of %v is %d\n", resultType, len(results))
	for _, value := range results {
		log.Printf("Value: %v", value)
	}
}

func PrintDocs(docs KeyValues) {
	log.Printf("Count of %v is %d\n", "docs", len(docs))
	for key, value := range docs {
		log.Printf("Key: %v  Value: %v", key, value)
	}
}

func HandleError(err error, msg string) {
	if err != nil {
		log.Panicf("%v: %v\n", msg, err)
	}
}

// ReadFileToString reads the contents of a file into a string.
func ReadFileToString(filePath string) (string, error) {
	const _ReadFileToString = "util.go::ReadFileToString:"

	fileHandle, err := os.Open(filePath)
	if err != nil {
		log.Printf("%v os.Open(%v) returned error: %v", _ReadFileToString, filePath, err)
		return "", err
	}
	defer fileHandle.Close()

	var result strings.Builder
	buffer := make([]byte, 64*1024)
	var bytesRead int            // avoid bytesRead, err := in loop shadowing loop condition err
	for err = nil; err == nil; { // err == io.EOF terminates loop; other errors return from it
		bytesRead, err = fileHandle.Read(buffer)
		result.Write(buffer[0:bytesRead]) // process bytesRead before err
		if err != nil && err != io.EOF {
			log.Printf("%v fileHandle.Read(%v) returned error: %v", _ReadFileToString,
				filePath, err)
			return "", err
		}
	}
	return result.String(), nil
}

// Read a .gz file
func ReadCompressedFile(filePath string) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func FileExists(path string) bool {
	if _, err := os.Stat(path); err == nil {
		return true
	} else {
		return false
	}
}

// Creates a directory if it not exists, else skips
func CreateDirectory(dirpath string) {
	if FileExists(dirpath) == true {
		return
	}
	err := os.Mkdir(dirpath, 0777)
	HandleError(err, "Error creating directory: "+dirpath)
	log.Printf("Directory created: %v", dirpath)
}

// Download a remote file over HTTP
func DownloadDataFile(sourceDataFile, destinationFilePath string, skipIfFileExists bool) {
	dataFileExists := FileExists(destinationFilePath)
	if skipIfFileExists == true && dataFileExists == true {
		log.Printf("Data file exists. Skipping download")
		return
	}

	CreateDirectory(filepath.Dir(destinationFilePath))
	log.Printf("Downloading data file to: %v", destinationFilePath)
	f, err := os.Create(destinationFilePath)
	HandleError(err, "Error downloading datafile "+destinationFilePath)
	defer f.Close()

	c := http.Client{
		CheckRedirect: func(r *http.Request, via []*http.Request) error {
			r.URL.Opaque = r.URL.Path
			return nil
		},
	}

	url := sourceDataFile
	r, err := c.Get(url)
	HandleError(err, "Error downloading datafile "+destinationFilePath)
	defer r.Body.Close()
	log.Printf("%v", r.Status)

	n, err := io.Copy(f, r.Body)
	HandleError(err, "Error downloading datafile "+destinationFilePath)
	log.Printf("%d Data file downloaded", n)
}

func GetClusterConfFromFile(filepath string) ClusterConfiguration {
	file, e := os.Open(filepath)
	HandleError(e, "Error in creating config file handle")
	decoder := json.NewDecoder(file)
	configuration := ClusterConfiguration{}
	err := decoder.Decode(&configuration)
	HandleError(err, "Error in decoding cluster configuration")
	return configuration
}

// Returns paths of Prod and Bags dir
func FetchMonsterToolPath() (string, string) {
	// Resolve monster bags and prods paths
	gopath := os.Getenv("GOPATH")
	for _, dir := range strings.Split(gopath, ":") {
		file := filepath.Join(dir, "src/github.com/prataprc/monster")
		if FileExists(file) {
			proddir := filepath.Join(file, "prods")
			bagdir := filepath.Join(file, "bags")
			return proddir, bagdir
		}
	}

	return "", ""
}

func ClearMap(docs KeyValues) {
	for k := range docs {
		delete(docs, k)
	}
}

func KillMemcacheD() {
	out, err := exec.Command("pkill", "memcached").CombinedOutput()
	if err != nil {
		log.Printf("%v", err)
	} else {
		log.Printf("%v", out)
	}
}

func KillIndexer() {
	out, err := exec.Command("pkill", "indexer").CombinedOutput()
	if err != nil {
		log.Printf("%v", err)
	} else {
		log.Printf("%v", out)
	}
}

func KillProjector() {
	out, err := exec.Command("pkill", "projector").CombinedOutput()
	if err != nil {
		log.Printf("%v", err)
	} else {
		log.Printf("%v", out)
	}
}

func LogPerfStat(apiName string, elapsed time.Duration) {
	if LogPerformanceStat {
		log.Printf("PERFSTAT %v %.4f seconds\n", apiName, elapsed.Seconds())
	}
}

func ExecuteN1QLStatement(clusterAddr, username, password, bucketName,
	statement string, disableAggrPushdown bool, consistency interface{}) ([]interface{}, error) {

	clusterAddr = "http://" + clusterAddr
	cluster, err := gocb.Connect(clusterAddr)
	if err != nil {
		return nil, err
	}
	err = cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: username,
		Password: password,
	})
	if err != nil {
		return nil, err
	}
	bucket, err := cluster.OpenBucket(bucketName, "")
	if err != nil {
		return nil, err
	}

	query := gocb.NewN1qlQuery(statement)
	if consistency != nil {
		query.Consistency((consistency).(gocb.ConsistencyMode))
	}

	if disableAggrPushdown {
		query.Custom("n1ql_feat_ctrl", "1")
	}

	rows, err := bucket.ExecuteN1qlQuery(query, []interface{}{})
	if err != nil {
		log.Printf("Error in executing N1QL query, err: %v", err)
		return nil, err
	}

	var row interface{}
	results := make([]interface{}, 0)
	for rows.Next(&row) {
		results = append(results, row)
	}
	return results, nil
}

func GetIndexSlicePath(indexName, bucketName, dirPath string, partnId c.PartitionId) (string, error) {
	p := []string{bucketName, indexName, ""}
	prefix := strings.Join(p, "_")
	var files []string
	indexSuffix := fmt.Sprintf("_%d.index", partnId)

	walkFn := func(pth string, finfo os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if finfo == nil {
			return fmt.Errorf("Nil finifo for path %v", pth)
		}

		if !finfo.IsDir() {
			return nil
		}

		if finfo.IsDir() && finfo.Name() == ".tmp" {
			return filepath.SkipDir
		}

		if len(pth) <= len(dirPath) {
			return nil
		}

		dirName := pth[len(dirPath)+len(string(os.PathSeparator)):]
		if strings.Contains(dirName, string(os.PathSeparator)) {
			return nil
		}

		if !strings.HasPrefix(dirName, prefix) {
			return nil
		}

		if !strings.HasSuffix(dirName, indexSuffix) {
			return nil
		}

		rem := dirName[len(prefix):]

		rem = rem[:len(rem)-len(indexSuffix)]
		if strings.Contains(rem, "_") {
			return nil
		}

		files = append(files, pth)
		return nil
	}

	err := filepath.Walk(dirPath, walkFn)
	if err != nil {
		msg := fmt.Sprintf("Error %v during directory walk", err)
		return "", errors.New(msg)
	}

	if len(files) != 1 {
		msg := fmt.Sprintf("Unexpected number of slice paths found %v", files)
		return "", errors.New(msg)
	}

	return files[0], nil
}

func GetMOILatestSnapshotPath(indexName, bucketName, dirPath string,
	partnId c.PartitionId) (string, error) {
	slicePath, err := GetIndexSlicePath(indexName, bucketName, dirPath, partnId)
	if err != nil {
		return "", err
	}

	pattern := "*"
	files, errGlob := filepath.Glob(filepath.Join(slicePath, pattern))
	if errGlob != nil {
		return "", errGlob
	}

	return files[len(files)-1], nil
}

func GetIndexerSetting(indexerAddr, setting, username, password string) (interface{}, error) {
	var err error

	addr := fmt.Sprintf("http://%v/settings?internal=ok", indexerAddr)
	req, errNR := http.NewRequest("GET", addr, nil)
	if errNR != nil {
		return nil, errNR
	}

	req.SetBasicAuth(username, password)

	resp, errResp := http.DefaultClient.Do(req)
	if errResp != nil {
		return nil, errResp
	}

	defer resp.Body.Close()

	r := make(map[string]interface{})

	var p []byte
	p, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(p, &r)
	if err != nil {
		return nil, err
	}

	val, ok := r[setting]
	if !ok {
		return nil, errors.New("Setting not found.")
	}

	return val, nil
}
