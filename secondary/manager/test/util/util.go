package test

import (
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/manager"
	"github.com/couchbase/indexing/secondary/dataport"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
	"github.com/couchbase/indexing/secondary/transport"
	"net"
	"testing"
)

type TestDefaultClientFactory struct {
}

type TestDefaultClientEnv struct {
}

type fakeProjector struct {
	Client *dataport.Client
}

var TT *testing.T

////////////////////////////////////////////////////////////////////////////////////////////////////
// testDefaultClientFactory
////////////////////////////////////////////////////////////////////////////////////////////////////

func (p *TestDefaultClientFactory) GetClientForNode(server string) manager.ProjectorStreamClient  {

	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// testDefaultClientEnv 
////////////////////////////////////////////////////////////////////////////////////////////////////

func (p *TestDefaultClientEnv) GetNodeListForBuckets(buckets []string) (map[string]string, error) {

	common.Infof("testDefaultClientEnv.GetNodeListForBuckets() ")
	nodes := make(map[string]string)
	nodes["127.0.0.1"] = "127.0.0.1"
	return nodes, nil
}

func (p *TestDefaultClientEnv) GetNodeListForTimestamps(timestamps []*common.TsVbuuid) (map[string][]*protobuf.TsVbuuid, error) {

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

func (p *TestDefaultClientEnv) FilterTimestampsForNode(timestamps []*protobuf.TsVbuuid, node string) ([]*protobuf.TsVbuuid, error) {
	return timestamps, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// fakeProjector
////////////////////////////////////////////////////////////////////////////////////////////////////

func NewFakeProjector(port string) *fakeProjector {

	p := new(fakeProjector)

	addr := net.JoinHostPort("127.0.0.1", port)
	prefix := "projector.dataport.client."
    config := common.SystemConfig.SectionConfig(prefix, true /*trim*/)
    maxvbs := common.SystemConfig["maxVbuckets"].Int()
    flag := transport.TransportFlag(0).SetProtobuf()
   
	var err error
	p.Client, err = dataport.NewClient("unit-test", "mutation topic", addr, flag, maxvbs, config)
	if err != nil {
		TT.Fatal(err)
	}

	return p
}

func (p *fakeProjector) Run(donech chan bool) {

	<-donech
	p.Client.Close()

	common.Infof("fakeProjector: done")
}