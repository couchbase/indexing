package kvutility

import (
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/go-couchbase"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ToDo: Refactor Code
func Set(key string, v interface{}, bucketName string, password string, hostaddress string) {
	url := "http://" + bucketName + ":" + password + "@" + hostaddress

	c, err := couchbase.Connect(url)
	tc.HandleError(err, "connect - "+url)

	p, err := c.GetPool("default")
	tc.HandleError(err, "pool")

	b, err := p.GetBucket(bucketName)
	tc.HandleError(err, "bucket")

	err = b.Set(key, 0, v)
	tc.HandleError(err, "set")
	b.Close()
}

func SetKeyValues(keyValues tc.KeyValues, bucketName string, password string, hostaddress string) {
	url := "http://" + bucketName + ":" + password + "@" + hostaddress
	c, err := couchbase.Connect(url)
	tc.HandleError(err, "connect - "+url)

	p, err := c.GetPool("default")
	tc.HandleError(err, "pool")

	b, err := p.GetBucket(bucketName)
	tc.HandleError(err, "bucket")

	for key, value := range keyValues {
		err = b.Set(key, 0, value)
		tc.HandleError(err, "set")
	}
	b.Close()
}

func Get(key string, rv interface{}, bucketName string, password string, hostaddress string) {

	url := "http://" + bucketName + ":" + password + "@" + hostaddress

	c, err := couchbase.Connect(url)
	tc.HandleError(err, "connect - "+url)

	p, err := c.GetPool("default")
	tc.HandleError(err, "pool")

	b, err := p.GetBucket("test")
	tc.HandleError(err, "bucket")

	err = b.Get(key, &rv)
	tc.HandleError(err, "get")
}

func Delete(key string, bucketName string, password string, hostaddress string) {

	url := "http://" + bucketName + ":" + password + "@" + hostaddress

	c, err := couchbase.Connect(url)
	tc.HandleError(err, "connect - "+url)

	p, err := c.GetPool("default")
	tc.HandleError(err, "pool")

	b, err := p.GetBucket(bucketName)
	tc.HandleError(err, "bucket")

	err = b.Delete(key)
	tc.HandleError(err, "delete")
	b.Close()
}

func DeleteKeys(keyValues tc.KeyValues, bucketName string, password string, hostaddress string) {

	url := "http://" + bucketName + ":" + password + "@" + hostaddress

	c, err := couchbase.Connect(url)
	tc.HandleError(err, "connect - "+url)

	p, err := c.GetPool("default")
	tc.HandleError(err, "pool")

	b, err := p.GetBucket(bucketName)
	tc.HandleError(err, "bucket")

	for key, _ := range keyValues {
		err = b.Delete(key)
		// tc.HandleError(err, "delete")
	}
	b.Close()
}

func CreateBucket(bucketName, authenticationType, saslBucketPassword, serverUserName, serverPassword, hostaddress, bucketRamQuota, proxyPort string) {
	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/buckets"
	data := url.Values{"name": {bucketName}, "ramQuotaMB": {bucketRamQuota}, "authType": {authenticationType}, "saslPassword": {saslBucketPassword}, "flushEnabled": {"1"}, "replicaNumber": {"1"}, "proxyPort": {proxyPort}}
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
	time.Sleep(30 * time.Second)
	log.Printf("Created bucket %v", bucketName)
}

func DeleteBucket(bucketName, bucketPassword, serverUserName, serverPassword, hostaddress string) {
	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/buckets/" + bucketName
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
	time.Sleep(30 * time.Second)
	log.Printf("Deleted bucket %v", bucketName)
}

func EnableBucketFlush(bucketName, bucketPassword, serverUserName, serverPassword, hostaddress string) {
	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/buckets/" + bucketName
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
	time.Sleep(3 * time.Second)
	log.Printf("Flush Enabled on bucket %v", bucketName)
}

func FlushBucket(bucketName, bucketPassword, serverUserName, serverPassword, hostaddress string) {
	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/buckets/" + bucketName + "/controller/doFlush"
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
	tc.HandleError(err, "Delete Bucket "+address)
	time.Sleep(3 * time.Second)
	log.Printf("Flushed the bucket %v", bucketName)
}

func EditBucket(bucketName, bucketPassword, serverUserName, serverPassword, hostaddress, bucketRamQuota string) {
	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/buckets/" + bucketName
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
	time.Sleep(3 * time.Second)
	log.Printf("Modified parameters of bucket %v", bucketName)
}
