package client

import "testing"

import c "github.com/couchbase/indexing/secondary/common"

func TestRetry100_5(t *testing.T) {
	//c.SetLogLevel(c.LogLevelDebug)
	adminport := "localhost:9999"
	maxvbs := c.SystemConfig["maxVbuckets"].Int()
	config := c.SystemConfig.SectionConfig("indexer.projectorclient.", true)
	config.SetValue("retryInterval", 100)
	config.SetValue("maxRetries", 5)
	client := NewClient(adminport, maxvbs, config)
	client.GetVbmap("default", "default", []string{"localhost:9000"})
}

func TestRetry0_5(t *testing.T) {
	//c.SetLogLevel(c.LogLevelDebug)
	adminport := "localhost:9999"
	maxvbs := c.SystemConfig["maxVbuckets"].Int()
	config := c.SystemConfig.SectionConfig("indexer.projectorclient.", true)
	config.SetValue("retryInterval", 0)
	config.SetValue("maxRetries", 5)
	client := NewClient(adminport, maxvbs, config)
	client.GetVbmap("default", "default", []string{"localhost:9000"})
}

//func TestRetry100_0(t *testing.T) {
//    c.SetLogLevel(c.LogLevelDebug)
//    adminport := "localhost:9999"
//    config := c.SystemConfig.SectionConfig("indexer.projectorclient", true)
//    config.SetValue("retryInterval", 100)
//    config.SetValue("maxRetries", 0)
//    client := NewClient(adminport, config)
//    client.GetVbmap("default", "default", []string{"localhost:9000"})
//}
