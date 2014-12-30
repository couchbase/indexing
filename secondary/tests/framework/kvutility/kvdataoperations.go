package kvutility

import (
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbaselabs/go-couchbase"
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
		tc.HandleError(err, "delete")
	}
	b.Close()
}
