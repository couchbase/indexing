package protobuf

import mc "github.com/couchbase/indexing/secondary/dcp/transport/client"
import "github.com/couchbase/indexing/secondary/common"
import "github.com/golang/protobuf/proto"

// NewKeyPartition return a new partition instance,
// initialized with a list of endpoint hosts.
func NewKeyPartition(numPartition uint64, endpoints []string, partitions []uint64) *KeyPartition {
	return &KeyPartition{Partitions: partitions, NumPartition: proto.Uint64(numPartition), Endpoints: endpoints}
}

func (p *KeyPartition) AddPartitions(partitions []uint64) {
	p.Partitions = append(p.Partitions, partitions...)
}

// Hosts implements Partition{} interface.
func (p *KeyPartition) Hosts(inst *IndexInst) []string {
	return p.getAllEndpoints()
}

// UpsertEndpoints implements Partition{} interface.
// - sent only if where clause is true.
// - UpsertDeletion is implied for every UpsertEndpoint.
// - if `key` is empty downstream shall consider Upsert as NOOP
//   and only apply UpsertDeletion.
// - `partnKey` is ignored.
// - for now, `oldKey` is ignored.
func (p *KeyPartition) UpsertEndpoints(
	inst *IndexInst, m *mc.DcpEvent, partKey, key, oldKey []byte) []string {

	return p.getPartitionEndpoint(partKey, inst.GetDefinition().GetHashScheme())
}

// UpsertDeletionEndpoints implements Partition{} interface.
// - sent only if where clause is false.
// - downstream can use immutable flag to opimtimize back-index lookup.
// - `key` is always nil
// - `partnKey` is ignored.
// - for now, `oldKey` is ignored.
func (p *KeyPartition) UpsertDeletionEndpoints(
	inst *IndexInst, m *mc.DcpEvent, partKey, key, oldKey []byte) []string {

	return p.getAllEndpoints()
}

// DeletionEndpoints implements Partition{} interface.
// - not sent to coordinator-endpoint
// - `oldPartKey` is ignored.
// - for now, `oldKey` is ignored.
func (p *KeyPartition) DeletionEndpoints(
	inst *IndexInst, m *mc.DcpEvent, oldPartKey, oldKey []byte) []string {

	return p.getAllEndpoints()
}

//
// Get endpoint of a specific partition
//
func (p *KeyPartition) getPartitionEndpoint(partKey []byte, scheme HashScheme) []string {

	partitionId := uint64(common.HashKeyPartition(partKey, int(p.GetNumPartition()), common.HashScheme(scheme)))
	for _, partnId := range p.Partitions {
		if partnId == partitionId {
			return p.GetEndpoints()
		}
	}
	return nil
}

//
// Get all endpoints
//
func (p *KeyPartition) getAllEndpoints() []string {
	return p.GetEndpoints()
}
