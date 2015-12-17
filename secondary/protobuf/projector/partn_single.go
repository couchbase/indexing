package protobuf

import "github.com/golang/protobuf/proto"
import mc "github.com/couchbase/indexing/secondary/dcp/transport/client"

// NewSinglePartition return a new partition instance,
// initialized with a list of endpoint hosts.
func NewSinglePartition(endpoints []string) *SinglePartition {
	return &SinglePartition{Endpoints: endpoints}
}

// AddEndpoint add a host to list of endpoints.
func (p *SinglePartition) AddEndpoint(endpoint string) *SinglePartition {
	p.Endpoints = append(p.Endpoints, endpoint)
	return p
}

// AddEndpoints add a list of hosts to endpoints.
func (p *SinglePartition) AddEndpoints(endpoints []string) *SinglePartition {
	for _, e := range endpoints {
		p.AddEndpoint(e)
	}
	return p
}

// SetCoordinatorEndpoint will set coordinator endpoint, that is different
// from other endpoints.
func (p *SinglePartition) SetCoordinatorEndpoint(endpoint string) *SinglePartition {
	p.CoordEndpoint = proto.String(endpoint)
	return p
}

// Hosts implements Partition{} interface.
func (p *SinglePartition) Hosts(inst *IndexInst) []string {
	endpoints := make([]string, 0)
	for _, endpoint := range p.GetEndpoints() {
		endpoints = append(endpoints, endpoint)
	}
	if p.GetCoordEndpoint() != "" {
		endpoints = append(endpoints, p.GetCoordEndpoint())
	}
	return endpoints
}

// UpsertEndpoints implements Partition{} interface.
// - sent only if where clause is true.
// - UpsertDeletion is implied for every UpsertEndpoint.
// - if `key` is empty downstream shall consider Upsert as NOOP
//   and only apply UpsertDeletion.
// - `partnKey` is ignored.
// - for now, `oldKey` is ignored.
func (p *SinglePartition) UpsertEndpoints(
	inst *IndexInst, m *mc.DcpEvent, partKey, key, oldKey []byte) []string {

	return p.GetEndpoints()
}

// UpsertDeletionEndpoints implements Partition{} interface.
// - sent only if where clause is false.
// - downstream can use immutable flag to opimtimize back-index lookup.
// - `key` is always nil
// - `partnKey` is ignored.
// - for now, `oldKey` is ignored.
func (p *SinglePartition) UpsertDeletionEndpoints(
	inst *IndexInst, m *mc.DcpEvent, oldPartKey, key, oldKey []byte) []string {

	return p.GetEndpoints()
}

// DeletionEndpoints implements Partition{} interface.
// - not sent to coordinator-endpoint
// - `oldPartKey` is ignored.
// - for now, `oldKey` is ignored.
func (p *SinglePartition) DeletionEndpoints(
	inst *IndexInst, m *mc.DcpEvent, oldPartKey, oldKey []byte) []string {

	return p.GetEndpoints()
}
