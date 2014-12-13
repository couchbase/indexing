package common

import (
	"fmt"
	"os"
	"log"
	"io/ioutil"
	"compress/gzip"
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