//go:build nolint

package test

import (
	"net"
	"testing"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dataport"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
	"github.com/couchbase/indexing/secondary/transport"
)

type TestDefaultClientFactory struct {
}

type TestDefaultClientEnv struct {
}

type fakeProjector struct {
	Client *dataport.Client
}

type fakeAddressProvider struct {
	adminport string
	httpport  string
}

var TT *testing.T

////////////////////////////////////////////////////////////////////////////////////////////////////
// testDefaultClientFactory
////////////////////////////////////////////////////////////////////////////////////////////////////

func (p *TestDefaultClientFactory) GetClientForNode(server string) manager.ProjectorStreamClient {

	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// testDefaultClientEnv
////////////////////////////////////////////////////////////////////////////////////////////////////

func (p *TestDefaultClientEnv) GetNodeListForBuckets(buckets []string) (map[string]string, error) {

	logging.Infof("testDefaultClientEnv.GetNodeListForBuckets() ")
	nodes := make(map[string]string)
	nodes["127.0.0.1"] = "127.0.0.1"
	return nodes, nil
}

func (p *TestDefaultClientEnv) GetNodeListForTimestamps(timestamps []*common.TsVbuuid) (map[string][]*protobuf.TsVbuuid, error) {

	logging.Infof("testDefaultClientEnv.GetNodeListForTimestamps() ")

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
	prefix := "projector.dataport."
	config := common.SystemConfig.SectionConfig(prefix, true)
	maxvbs := common.SystemConfig["maxVbuckets"].Int()
	flag := transport.TransportFlag(0).SetProtobuf()
	config.Set("mutationChanSize", common.ConfigValue{10000, "channel size of projector-dataport-client's data path routine", 10000})
	config.Set("parConnections", common.ConfigValue{1, "number of parallel connections to open with remote", 1})

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

	logging.Infof("fakeProjector: done")
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// fakeAdderssProvider
////////////////////////////////////////////////////////////////////////////////////////////////////

func NewFakeAddressProvider(adminport string, httpport string) common.ServiceAddressProvider {
	return &fakeAddressProvider{adminport: adminport, httpport: httpport}
}

func (p *fakeAddressProvider) GetLocalServiceAddress(srvc string) (string, error) {
	if srvc == common.INDEX_ADMIN_SERVICE {
		return p.adminport, nil
	}

	if srvc == common.INDEX_HTTP_SERVICE {
		return p.httpport, nil
	}
	return "", nil
}

func (p *fakeAddressProvider) GetLocalServicePort(srvc string) (string, error) {
	if srvc == common.INDEX_ADMIN_SERVICE {
		_, port, err := net.SplitHostPort(p.adminport)
		if err != nil {
			return "", err
		}
		return net.JoinHostPort("", port), nil
	}

	if srvc == common.INDEX_HTTP_SERVICE {
		_, port, err := net.SplitHostPort(p.httpport)
		if err != nil {
			return "", err
		}
		return net.JoinHostPort("", port), nil
	}

	return "", nil
}

func (p *fakeAddressProvider) GetLocalServiceHost(srvc string) (string, error) {
	if srvc == common.INDEX_ADMIN_SERVICE {
		h, _, err := net.SplitHostPort(p.adminport)
		if err != nil {
			return "", err
		}
		return h, nil
	}

	if srvc == common.INDEX_HTTP_SERVICE {
		h, _, err := net.SplitHostPort(p.httpport)
		if err != nil {
			return "", err
		}
		return h, nil
	}

	return "", nil
}

func (p *fakeAddressProvider) GetLocalHostAddress() (string, error) {
	return p.adminport, nil
}
