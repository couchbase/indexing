package protobuf

import "github.com/golang/protobuf/proto"
import mc "github.com/couchbase/indexing/secondary/dcp/transport/client"

// NewTestParitition return a new partition instance,
// initialized with a list of endpoint nodes excluding coordinator.
func NewTestParitition(endpoints []string) *TestPartition {
	return &TestPartition{Endpoints: endpoints}
}

// AddEndpoint add a host to list of endpoints.
func (p *TestPartition) AddEndpoint(endpoint string) *TestPartition {
	p.Endpoints = append(p.Endpoints, endpoint)
	return p
}

// AddEndpoints add a list of hosts to endpoints.
func (p *TestPartition) AddEndpoints(endpoints []string) *TestPartition {
	for _, e := range endpoints {
		p.AddEndpoint(e)
	}
	return p
}

// SetCoordinatorEndpoint will set coordinator endpoint, that is different
// from other endpoints.
func (p *TestPartition) SetCoordinatorEndpoint(endpoint string) *TestPartition {
	p.CoordEndpoint = proto.String(endpoint)
	return p
}

// Hosts implements Partition{} interface.
func (p *TestPartition) Hosts(inst *IndexInst) []string {
	endpoints := p.GetEndpoints()
	endpoints = append(endpoints, p.GetCoordEndpoint())
	return endpoints
}

// UpsertEndpoints implements Partition{} interface.
// Full broadcast.
func (p *TestPartition) UpsertEndpoints(
	inst *IndexInst, m *mc.DcpEvent, partKey, key, oldKey []byte) []string {
	return p.Hosts(inst)
}

// UpsertDeletionEndpoints implements Partition{} interface.
// Full broadcast.
func (p *TestPartition) UpsertDeletionEndpoints(
	inst *IndexInst, m *mc.DcpEvent, oldPartKey, key, oldKey []byte) []string {
	return p.Hosts(inst)
}

// DeletionEndpoints implements Partition{} interface.
// Full broadcast.
func (p *TestPartition) DeletionEndpoints(
	inst *IndexInst, m *mc.DcpEvent, oldPartKey, oldKey []byte) []string {
	return p.Hosts(inst)
}
