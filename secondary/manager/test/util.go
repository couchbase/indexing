package test

import (
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/manager"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
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

func (p *testDefaultClientEnv) GetNodeListForBuckets(buckets []string) (map[string]string, error) {

	common.Infof("testDefaultClientEnv.GetNodeListForBuckets() ")
	nodes := make(map[string]string)
	nodes["127.0.0.1"] = "127.0.0.1"
	return nodes, nil
}

func (p *testDefaultClientEnv) GetNodeListForTimestamps(timestamps []*common.TsVbuuid) (map[string][]*protobuf.TsVbuuid, error) {

	common.Infof("testDefaultClientEnv.GetNodeListForTimestamps() ")
	
	nodes := make(map[string][]*protobuf.TsVbuuid)
	nodes["127.0.0.1"] = nil 
		
	newTs := protobuf.NewTsVbuuid("default", "Default", 1)
	for i, _ := range timestamps[0].Seqnos {
		newTs.Append(uint16(i), timestamps[0].Seqnos[i], timestamps[0].Vbuuids[i],
			timestamps[0].Snapshots[i][0], timestamps[0].Snapshots[i][1])
	}
		
	nodes["127.0.0.1"] = append(nodes["127.0.0.1"], newTs)	
	return nodes, nil
}
