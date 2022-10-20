package dataport

import (
	"net"
	"strconv"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/transport"

	dcpTransport "github.com/couchbase/indexing/secondary/dcp/transport"
)

type endpointBuffers struct {
	raddr       string
	vbs         map[string]*c.VbKeyVersions // uuid -> VbKeyVersions
	oso         map[string]bool             // uuid -> osoSnapshot
	lastAvgSent int64
}

func newEndpointBuffers(raddr string) *endpointBuffers {
	vbs := make(map[string]*c.VbKeyVersions)
	oso := make(map[string]bool)
	b := &endpointBuffers{raddr: raddr, vbs: vbs, oso: oso}
	return b
}

// addKeyVersions, add a mutation's keyversions to buffer.
func (b *endpointBuffers) addKeyVersions(
	keyspaceId string,
	vbno uint16,
	vbuuid uint64,
	opaque2 uint64,
	oso bool,
	kv *c.KeyVersions,
	endpoint *RouterEndpoint) {

	if kv != nil && kv.Length() > 0 {
		uuid := c.StreamID(keyspaceId, vbno, opaque2)
		if _, ok := b.vbs[uuid]; !ok {
			nMuts := 16 // to avoid reallocs.
			b.vbs[uuid] = c.NewVbKeyVersions(keyspaceId, vbno,
				vbuuid, opaque2, nMuts)
			b.oso[uuid] = oso
		}
		b.vbs[uuid].AddKeyVersions(kv)
		// update statistics.
		for _, cmd := range kv.Commands {
			switch cmd {
			case c.Upsert:
				endpoint.stats.upsertCount.Add(1)
			case c.Deletion:
				endpoint.stats.deleteCount.Add(1)
			case c.UpsertDeletion:
				endpoint.stats.upsdelCount.Add(1)
			case c.Sync:
				endpoint.stats.syncCount.Add(1)
			case c.StreamBegin:
				if _, ok := endpoint.keyspaceIdVBMap[keyspaceId]; !ok {
					endpoint.keyspaceIdVBMap[keyspaceId] = make(map[uint16]bool)
				}
				endpoint.keyspaceIdVBMap[keyspaceId][vbno] = true

				endpoint.stats.beginCount.Add(1)
			case c.StreamEnd:

				delete(endpoint.keyspaceIdVBMap[keyspaceId], vbno)
				if len(endpoint.keyspaceIdVBMap[keyspaceId]) == 0 {
					delete(endpoint.keyspaceIdVBMap, keyspaceId)
				}

				endpoint.stats.endCount.Add(1)
			case c.Snapshot:
				endpoint.stats.snapCount.Add(1)
			case c.CollectionCreate:
				endpoint.stats.collectionCreate.Add(1)
			case c.CollectionDrop:
				endpoint.stats.collectionCreate.Add(1)
			case c.CollectionFlush:
				endpoint.stats.collectionFlush.Add(1)
			case c.ScopeCreate:
				endpoint.stats.scopeCreate.Add(1)
			case c.ScopeDrop:
				endpoint.stats.scopeDrop.Add(1)
			case c.CollectionChanged:
				endpoint.stats.collectionChanged.Add(1)
			case c.UpdateSeqno:
				endpoint.stats.updateSeqno.Add(1)
			case c.SeqnoAdvanced:
				endpoint.stats.seqnoAdvanced.Add(1)
			case c.OSOSnapshotStart:
				endpoint.stats.osoSnapshotStart.Add(1)
			case c.OSOSnapshotEnd:
				endpoint.stats.osoSnapshotEnd.Add(1)
			}
		}
		endpoint.stats.mutCount.Add(1)
	}
}

// flush the buffers to the other end.
func (b *endpointBuffers) flushBuffers(
	endpoint *RouterEndpoint,
	conn net.Conn,
	pkt *transport.TransportPacket) error {

	getKey := func(keyspace string, vb uint16) string {
		return keyspace + ":" + strconv.FormatUint(uint64(vb), 10)
	}

	vbs := make([]*c.VbKeyVersions, 0, len(b.vbs))
	for uuid, vb := range b.vbs {
		vbs = append(vbs, vb)
		key := getKey(vb.KeyspaceId, vb.Vbucket)
		for _, kv := range vb.Kvs {
			if kv.Ctime > 0 {
				now := time.Now().UnixNano()
				endpoint.stats.prjLatency.Add(now - kv.Ctime)

				// Send moving average latency to indexer only once every second
				if now-b.lastAvgSent > int64(time.Second) {
					b.lastAvgSent = now
					// Populate Ctime with moving average
					kv.Ctime = endpoint.stats.prjLatency.MovingAvg()
				} else {
					kv.Ctime = 0 // Clear kv.Ctime so that we do not send it to indexer
				}
			}

			if !b.oso[uuid] {
				b.checkSeqOrder(kv, endpoint, key)
			}
		}
	}
	b.vbs = make(map[string]*c.VbKeyVersions)
	b.oso = make(map[string]bool)

	if err := pkt.Send(conn, vbs); err != nil {
		return err
	}
	return nil
}

func (b *endpointBuffers) checkSeqOrder(kv *c.KeyVersions, endpoint *RouterEndpoint, key string) {

	// If there are multiple indexes on the endpoint, kv can have multiple
	// commands. Command for one index can be different from command for
	// any other index. Logically, the commands for each index should belong
	// to one of the categories.
	//
	// Categories:
	// 1. Mutation: Upsert, UpsertDeletion, Deletion
	// 2. StreamBegin
	// 3. StreamEnd
	// 4. Snapshot
	//
	// Assuming that the commands don't span across multiple categories for
	// same kv, checking only for the first command.

	if len(kv.Commands) < 1 {
		return
	}

	switch kv.Commands[0] {

	case c.StreamBegin:
		endpoint.seqOrders[key] = dcpTransport.NewSeqOrderState()

	case c.StreamEnd:
		if s, ok := endpoint.seqOrders[key]; ok && s != nil && s.GetErrCount() != 0 {
			logging.Fatalf("%v error count for sequence number ordering is %v", endpoint.logPrefix, s.GetErrCount())
		}

		endpoint.seqOrders[key] = nil

	case c.Snapshot:
		if s, ok := endpoint.seqOrders[key]; ok && s != nil {
			_, start, end := kv.GetSnapshot()
			if snapInfo, correctSnapOrder := s.ProcessSnapshot(start, end); !correctSnapOrder {
				logging.Fatalf("%v seq order violation for snapshot message for vb = %v, command = %v, "+
					"orderState = %v, snapStart: %v, snapEnd: %v, mutation = %v", endpoint.logPrefix, key, kv.Commands[0],
					snapInfo, start, end, kv.GetDebugInfo())
			}
		}

	case c.Upsert, c.Deletion, c.UpsertDeletion, c.UpdateSeqno, c.SeqnoAdvanced,
		c.CollectionCreate, c.CollectionDrop, c.CollectionChanged,
		c.CollectionFlush, c.ScopeCreate, c.ScopeDrop:
		if s, ok := endpoint.seqOrders[key]; ok && s != nil {
			if !s.ProcessSeqno(kv.Seqno) {
				logging.Fatalf("%v seq order violation for vb = %v, seq = %v, command = %v, "+
					"orderState = %v, mutation = %v", endpoint.logPrefix, key, kv.Seqno,
					kv.Commands[0], s.GetInfo(), kv.GetDebugInfo())
			}
		}
	}
}
