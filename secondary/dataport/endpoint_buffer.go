package dataport

import "net"
import "time"

import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/transport"

type endpointBuffers struct {
	raddr       string
	vbs         map[string]*c.VbKeyVersions // uuid -> VbKeyVersions
	lastAvgSent int64
}

func newEndpointBuffers(raddr string) *endpointBuffers {
	vbs := make(map[string]*c.VbKeyVersions)
	b := &endpointBuffers{raddr: raddr, vbs: vbs}
	return b
}

// addKeyVersions, add a mutation's keyversions to buffer.
func (b *endpointBuffers) addKeyVersions(
	keyspaceId string,
	vbno uint16,
	vbuuid uint64,
	opaque2 uint64,
	kv *c.KeyVersions,
	endpoint *RouterEndpoint) {

	if kv != nil && kv.Length() > 0 {
		uuid := c.StreamID(keyspaceId, vbno)
		if _, ok := b.vbs[uuid]; !ok {
			nMuts := 16 // to avoid reallocs.
			b.vbs[uuid] = c.NewVbKeyVersions(keyspaceId, vbno,
				vbuuid, opaque2, nMuts)
		}
		b.vbs[uuid].AddKeyVersions(kv)
		// update statistics
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
				endpoint.stats.beginCount.Add(1)
			case c.StreamEnd:
				endpoint.stats.endCount.Add(1)
			case c.Snapshot:
				endpoint.stats.snapCount.Add(1)
				// TODO (Collections): Add collection event specific stats
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

	vbs := make([]*c.VbKeyVersions, 0, len(b.vbs))
	for _, vb := range b.vbs {
		vbs = append(vbs, vb)
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
		}
	}
	b.vbs = make(map[string]*c.VbKeyVersions)

	if err := pkt.Send(conn, vbs); err != nil {
		return err
	}
	return nil
}
