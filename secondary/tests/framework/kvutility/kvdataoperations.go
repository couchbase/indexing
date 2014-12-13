package kvutility

import (
	"fmt"
	"github.com/couchbaselabs/go-couchbase"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
)

// Move to common
type KeyValue struct {
	Key       string
	JsonValue map[string]interface{}
}

// ToDo: Refactor Code
func Set(key string, v interface{}, bucketName string, password string, hostname string) {
	url := "http://" + bucketName + ":" + password + "@" + hostname + ":9000"

	c, err := couchbase.Connect(url)
	tc.HandleError(err, "connect - "+url)

	p, err := c.GetPool("default")
	tc.HandleError(err, "pool")

	b, err := p.GetBucket(bucketName)
	tc.HandleError(err, "bucket")

	err = b.Set(key, 0, v)
	tc.HandleError(err, "set")
}

func SetKeyValue(keyValue KeyValue, bucketName string, password string, hostname string) {
	Set(keyValue.Key, keyValue.JsonValue, bucketName, password, hostname)
}

func SetKeyValues(keyValues []KeyValue, bucketName string, password string, hostname string) {
	for _, value := range keyValues {
		SetKeyValue(value, bucketName, password, hostname)
	}
}

func Get(key string, rv interface{}, bucketName string, password string, hostname string) {

	url := "http://" + bucketName + ":" + password + "@" + hostname + ":9000"

	c, err := couchbase.Connect(url)
	tc.HandleError(err, "connect - "+url)

	p, err := c.GetPool("default")
	tc.HandleError(err, "pool")

	b, err := p.GetBucket("test")
	tc.HandleError(err, "bucket")

	err = b.Get(key, &rv)
	tc.HandleError(err, "get")
}

func Delete(key string, bucketName string, password string, hostname string) {

	url := "http://" + bucketName + ":" + password + "@" + hostname + ":9000"

	c, err := couchbase.Connect(url)
	tc.HandleError(err, "connect - "+url)

	p, err := c.GetPool("default")
	tc.HandleError(err, "pool")

	b, err := p.GetBucket(bucketName)
	tc.HandleError(err, "bucket")

	fmt.Printf("Setting key %v", key)

	err = b.Delete(key)
	tc.HandleError(err, "set")
}
