package kvutility

import (
	"fmt"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbaselabs/go-couchbase"
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
		fmt.Println(address)
		fmt.Println(req)
		fmt.Println(resp)
		fmt.Println("CreateBucket failed for bucket: ", bucketName)
	}
	// todo : error out if response is error
	tc.HandleError(err, "Create Bucket")
	time.Sleep(30 * time.Second)
	fmt.Println("Created bucket ", bucketName)
}

func DeleteBucket(bucketName, bucketPassword, serverUserName, serverPassword, hostaddress string) {
	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/buckets/" + bucketName
	req, _ := http.NewRequest("DELETE", address, nil)
	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	resp, err := client.Do(req)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		fmt.Println(address)
		fmt.Println(req)
		fmt.Println(resp)
		fmt.Println("DeleteBucket failed for bucket: ", bucketName)
	}
	// todo : error out if response is error
	tc.HandleError(err, "Delete Bucket "+address)
	time.Sleep(30 * time.Second)
	fmt.Println("Deleted bucket ", bucketName)
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
		fmt.Println(address)
		fmt.Println(req)
		fmt.Println(resp)
		fmt.Println("EnableBucketFlush failed for bucket: ", bucketName)
	}
	// todo : error out if response is error
	tc.HandleError(err, "Enable Bucket")
	time.Sleep(3 * time.Second)
	fmt.Println("Flush Enabled on bucket ", bucketName)
}

func FlushBucket(bucketName, bucketPassword, serverUserName, serverPassword, hostaddress string) {
	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/buckets/" + bucketName + "/controller/doFlush"
	req, _ := http.NewRequest("POST", address, nil)
	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	resp, err := client.Do(req)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		fmt.Println(address)
		fmt.Println(req)
		fmt.Println(resp)
		fmt.Println("Flush Bucket failed for bucket: ", bucketName)
	}
	// todo : error out if response is error
	tc.HandleError(err, "Delete Bucket "+address)
	time.Sleep(3 * time.Second)
	fmt.Println("Flushed the bucket ", bucketName)
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
		fmt.Println(address)
		fmt.Println(req)
		fmt.Println(resp)
		fmt.Println("EditBucket failed for bucket: ", bucketName)
	}
	// todo : error out if response is error
	tc.HandleError(err, "Edit Bucket")
	time.Sleep(3 * time.Second)
	fmt.Println("Modified parameters of bucket: ", bucketName)
}
