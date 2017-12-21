package common

import (
	"compress/gzip"
	"encoding/json"
	"github.com/couchbase/gocb"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

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
	log.Printf("PERFSTAT %v %.4f seconds\n", apiName, elapsed.Seconds())
}

func ExecuteN1QLStatement(clusterAddr, username, password, bucketName,
	statement string) ([]interface{}, error) {

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
	rows, err := bucket.ExecuteN1qlQuery(query, []interface{}{})
	if err != nil {
		return nil, err
	}

	var row interface{}
	results := make([]interface{}, 0)
	for rows.Next(&row) {
		results = append(results, row)
	}
	return results, nil
}
