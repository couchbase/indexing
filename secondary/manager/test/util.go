package test

import (
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/manager"
)

type testDefaultClientFactory struct {
}

type testDefaultClientEnv struct {
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// testDefaultClientFactory
////////////////////////////////////////////////////////////////////////////////////////////////////

func (p *testDefaultClientFactory) GetClientForNode(server string) manager.ProjectorStreamClient  {

	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// testDefaultClientEnv 
////////////////////////////////////////////////////////////////////////////////////////////////////

func (p *testDefaultClientEnv) GetNodeList(buckets []string) (map[string]string, error) {

	common.Infof("testDefaultClientEnv.GetNodeList() ")
	nodes := make(map[string]string)
	nodes["127.0.0.1"] = "127.0.0.1"
	return nodes, nil
}