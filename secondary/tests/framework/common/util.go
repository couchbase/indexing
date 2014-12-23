package common

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

// ToDo: Point out the exact difference between two responses
func PrintScanResults(results ScanResponse, resultType string) {
	fmt.Printf("Count of %v is %d\n", resultType, len(results))
	for key, value := range results {
		fmt.Println("Key:", key, "Value:", value)
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

// Download a remote file over HTTP
func DownloadDataFile(sourceDataFile, destinationFilePath string) {
	fmt.Println("Downloading file...")

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
	fmt.Println(r.Status)

	n, err := io.Copy(f, r.Body)
	HandleError(err, "Error downloading datafile "+destinationFilePath)
	fmt.Println(n, "Data file downloaded")
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
