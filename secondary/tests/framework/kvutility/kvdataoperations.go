package kvutility

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"gopkg.in/couchbase/gocb.v1"
)

// ToDo: Refactor Code
func Set(key string, v interface{}, bucketName string, password string, hostaddress string) {
	url := "http://" + url.PathEscape(bucketName) + ":" + password + "@" + hostaddress

	b, err := c.ConnectBucket(url, "default", bucketName)
	tc.HandleError(err, "bucket")
	defer b.Close()

	err = b.Set(key, 0, v)
	tc.HandleError(err, "set")
}

func SetKeyValues(keyValues tc.KeyValues, bucketName string, password string, hostaddress string) {
	url := "http://" + url.PathEscape(bucketName) + ":" + password + "@" + hostaddress

	b, err := c.ConnectBucket(url, "default", bucketName)
	tc.HandleError(err, "bucket")
	defer b.Close()

	for key, value := range keyValues {
		err = b.Set(key, 0, value)
		tc.HandleError(err, "set")
	}
}

func GetBytes(key interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(key)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func SetBinaryValuesWithXattrs(keyValues tc.KeyValues, bucketName string, password string, hostaddress, serverUsername, serverPassword string) {
	cluster, _ := gocb.Connect(hostaddress)
	cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: serverUsername,
		Password: serverPassword,
	})

	bucket, err := cluster.OpenBucket(bucketName, "")

	for key, value := range keyValues {
		if bytes, err := GetBytes(value); err == nil {
			bucket.Insert(key, &bytes, 0)
			bucket.MutateIn(key, 0, 0).UpsertEx("_sync1", "1000", gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).Execute()
		}
		tc.HandleError(err, "setRaw")
	}
}

func SetValuesWithXattrs(keyValues tc.KeyValues, bucketName string, password string, hostaddress, serverUsername, serverPassword string, xattrs map[string]string) {
	cluster, _ := gocb.Connect(hostaddress)
	cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: serverUsername,
		Password: serverPassword,
	})

	bucket, err := cluster.OpenBucket(bucketName, "")
	tc.HandleError(err, "Error while opening bucket")

	for key, value := range keyValues {
		bucket.Insert(key, value, 0)
		for xattr, xattrVal := range xattrs {
			bucket.MutateIn(key, 0, 0).UpsertEx(xattr, xattrVal, gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).Execute()
		}
	}
}

func SetBinaryValues(keyValues tc.KeyValues, bucketName string, password string, hostaddress string) {
	url := "http://" + url.PathEscape(bucketName) + ":" + password + "@" + hostaddress

	b, err := c.ConnectBucket(url, "default", bucketName)
	tc.HandleError(err, "bucket")
	defer b.Close()

	for key, value := range keyValues {
		if bytes, err := GetBytes(value); err == nil {
			err = b.SetRaw(key, 0, bytes)
		}
		tc.HandleError(err, "setRaw")
	}
}

func Get(key string, rv interface{}, bucketName string, password string, hostaddress string) {
	url := "http://" + url.PathEscape(bucketName) + ":" + password + "@" + hostaddress

	b, err := c.ConnectBucket(url, "default", bucketName)
	tc.HandleError(err, "bucket")
	defer b.Close()

	err = b.Get(key, &rv)
	tc.HandleError(err, "get")
}

func Delete(key string, bucketName string, password string, hostaddress string) {
	url := "http://" + url.PathEscape(bucketName) + ":" + password + "@" + hostaddress

	b, err := c.ConnectBucket(url, "default", bucketName)
	tc.HandleError(err, "bucket")
	defer b.Close()

	err = b.Delete(key)
	tc.HandleError(err, "delete")
}

func DeleteKeys(keyValues tc.KeyValues, bucketName string, password string, hostaddress string) {
	url := "http://" + url.PathEscape(bucketName) + ":" + password + "@" + hostaddress

	b, err := c.ConnectBucket(url, "default", bucketName)
	tc.HandleError(err, "bucket")
	defer b.Close()

	for key, _ := range keyValues {
		err = b.Delete(key)
		tc.HandleError(err, "delete")
	}
}

func CreateBucket(bucketName, authenticationType, saslBucketPassword, serverUserName, serverPassword, hostaddress, bucketRamQuota, proxyPort string) {
	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/buckets"
	data := url.Values{"name": {bucketName}, "ramQuotaMB": {bucketRamQuota}, "flushEnabled": {"1"}, "replicaNumber": {"1"}}
	req, _ := http.NewRequest("POST", address, strings.NewReader(data.Encode()))
	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	resp, err := client.Do(req)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		log.Printf(address)
		log.Printf("%v", req)
		log.Printf("%v", resp)
		log.Printf("CreateBucket failed for bucket %v \n", bucketName)
	}
	// todo : error out if response is error
	tc.HandleError(err, "Create Bucket")
	defer resp.Body.Close()
	responseBody, _ := ioutil.ReadAll(resp.Body)
	log.Printf("Created bucket %v, responseBody: %s", bucketName, responseBody)
}

func CreateBucketOfType(bucketName, serverUserName, serverPassword, hostaddress, bucketRamQuota, bucketType string) {
	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/buckets"
	data := url.Values{"name": {bucketName}, "ramQuotaMB": {bucketRamQuota}, "flushEnabled": {"1"}, "replicaNumber": {"1"}, "bucketType": {bucketType}}
	req, _ := http.NewRequest("POST", address, strings.NewReader(data.Encode()))
	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	resp, err := client.Do(req)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		log.Printf(address)
		log.Printf("%v", req)
		log.Printf("%v", resp)
		log.Printf("CreateBucket failed for bucket %v \n", bucketName)
	}
	// todo : error out if response is error
	tc.HandleError(err, "Create Bucket")
	defer resp.Body.Close()
	responseBody, _ := ioutil.ReadAll(resp.Body)
	log.Printf("Created bucket %v, responseBody: %s", bucketName, responseBody)
}

func DeleteBucket(bucketName, bucketPassword, serverUserName, serverPassword, hostaddress string) {
	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/buckets/" + url.PathEscape(bucketName)
	req, _ := http.NewRequest("DELETE", address, nil)
	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	resp, err := client.Do(req)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		log.Printf(address)
		log.Printf("%v", req)
		log.Printf("%v", resp)
		log.Printf("DeleteBucket failed for bucket %v \n", bucketName)
	}
	// todo : error out if response is error
	tc.HandleError(err, "Delete Bucket "+address)
	defer resp.Body.Close()
	responseBody, _ := ioutil.ReadAll(resp.Body)
	log.Printf("Deleted bucket %v, responseBody: %s", bucketName, responseBody)
}

func EnableBucketFlush(bucketName, bucketPassword, serverUserName, serverPassword, hostaddress string) {
	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/buckets/" + url.PathEscape(bucketName)
	data := url.Values{"name": {bucketName}, "flushEnabled": {"1"}}

	req, _ := http.NewRequest("POST", address, strings.NewReader(data.Encode()))
	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	resp, err := client.Do(req)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		log.Printf(address)
		log.Printf("%v", req)
		log.Printf("%v", resp)
		log.Printf("EnableBucketFlush failed for bucket %v \n", bucketName)
	}
	// todo : error out if response is error
	tc.HandleError(err, "Enable Bucket")
	defer resp.Body.Close()
	responseBody, _ := ioutil.ReadAll(resp.Body)
	time.Sleep(3 * time.Second)
	log.Printf("Flush Enabled on bucket %v, responseBody: %s", bucketName, responseBody)
}

func FlushBucket(bucketName, bucketPassword, serverUserName, serverPassword, hostaddress string) {
	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/buckets/" + url.PathEscape(bucketName) + "/controller/doFlush"
	req, _ := http.NewRequest("POST", address, nil)
	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	resp, err := client.Do(req)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		log.Printf(address)
		log.Printf("%v", req)
		log.Printf("%v", resp)
		log.Printf("Flush Bucket failed for bucket %v \n", bucketName)
	}
	// todo : error out if response is error
	tc.HandleError(err, "Flush Bucket "+address)
	defer resp.Body.Close()
	responseBody, _ := ioutil.ReadAll(resp.Body)
	time.Sleep(30 * time.Second)
	log.Printf("Flushed the bucket %v, Response body: %s", bucketName, responseBody)
}

func EditBucket(bucketName, bucketPassword, serverUserName, serverPassword, hostaddress, bucketRamQuota string) {
	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/buckets/" + url.PathEscape(bucketName)
	data := url.Values{"name": {bucketName}, "ramQuotaMB": {bucketRamQuota}}

	req, _ := http.NewRequest("POST", address, strings.NewReader(data.Encode()))
	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	resp, err := client.Do(req)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		log.Printf(address)
		log.Printf("%v", req)
		log.Printf("%v", resp)
		log.Printf("EditBucket failed for bucket %v \n", bucketName)
	}
	// todo : error out if response is error
	tc.HandleError(err, "Edit Bucket")
	defer resp.Body.Close()
	responseBody, _ := ioutil.ReadAll(resp.Body)
	time.Sleep(3 * time.Second)
	log.Printf("Modified parameters of bucket %v, responseBody: %s", bucketName, responseBody)
}

func GetItemCountInBucket(bucketName, bucketPassword, serverUserName, serverPassword, hostaddress string) int {
	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/buckets/" + url.PathEscape(bucketName)

	req, _ := http.NewRequest("GET", address, nil)
	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	resp, err := client.Do(req)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		log.Printf(address)
		log.Printf("%v", req)
		log.Printf("%v", resp)
		log.Printf("Get Bucket failed for bucket %v \n", bucketName)
	}
	// todo : error out if response is error
	tc.HandleError(err, "Get Bucket")
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)

	response := make(map[string]interface{})
	err = json.Unmarshal(body, &response)
	if err != nil {
		tc.HandleError(err, "Get Bucket :: Unmarshal of response body")
	}

	var itemcount float64
	basicstats := response["basicStats"].(map[string]interface{})
	itemcount = basicstats["itemCount"].(float64)
	return int(itemcount)
}

func WaitForBucketCreation(bucketName, serverUserName, serverPassword string, hostaddresses []string) {

	for _, hostaddress := range hostaddresses {
		log.Printf("WaitForBucketCreation: Checking bucket(%v) creation for host: %v", bucketName, hostaddress)
		created := false
		for j := 0; j < 30; j++ {
			client := &http.Client{}
			address := "http://" + hostaddress + "/pools/default/buckets/" + url.PathEscape(bucketName)

			req, _ := http.NewRequest("GET", address, nil)
			req.SetBasicAuth(serverUserName, serverPassword)
			req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
			resp, err := client.Do(req)
			if resp.StatusCode != http.StatusOK || err != nil {
				func() {
					log.Printf(address)
					log.Printf("%v", req)
					log.Printf("%v", resp)
					log.Printf("Waiting for bucket creation %v, err: %v \n", bucketName, err)

					defer resp.Body.Close()
					ioutil.ReadAll(resp.Body)

				}()
			} else { // Bucket creation is successful
				func() {
					defer resp.Body.Close()
					body, _ := ioutil.ReadAll(resp.Body)

					response := make(map[string]interface{})
					err = json.Unmarshal(body, &response)
					if err != nil {
						tc.HandleError(err, "Get Bucket :: Unmarshal of response body")
					}
					vBucketServerMap := response["vBucketServerMap"].(map[string]interface{})
					numVBuckets := response["numVBuckets"].(float64)
					vBucketMap := vBucketServerMap["vBucketMap"].([]interface{})
					if numVBuckets == float64(len(vBucketMap)) {
						created = true
					}
				}()
			}
			if created {
				break
			}
			time.Sleep(1 * time.Second)
		}
	}
}
