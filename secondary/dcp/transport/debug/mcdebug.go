// Package mcdebug provides memcached client op statistics via expvar.
package mcdebug

import (
	"encoding/json"
	"expvar"
	"github.com/couchbase/indexing/secondary/platform"

	"github.com/couchbase/indexing/secondary/dcp/transport"
	"github.com/couchbase/indexing/secondary/dcp/transport/client"
)

type mcops struct {
	moved, success, errored [257]platform.AlignedUint64
}

func addToMap(m map[string]uint64, i int, counters [257]platform.AlignedUint64) {
	v := platform.LoadUint64(&counters[i])
	if v > 0 {
		k := "unknown"
		if i < 256 {
			k = transport.CommandCode(i).String()
		}
		m[k] = v
		m["total"] += v
	}
}

func (m *mcops) String() string {
	bytes := map[string]platform.AlignedUint64{}
	ops := map[string]platform.AlignedUint64{}
	errs := map[string]platform.AlignedUint64{}
	for i := range m.moved {
		addToMap(bytes, i, m.moved)
		addToMap(ops, i, m.success)
		addToMap(errs, i, m.errored)
	}
	j := map[string]interface{}{"bytes": bytes, "ops": ops, "errs": errs}
	b, err := json.Marshal(j)
	if err != nil {
		panic(err) // shouldn't be possible
	}
	return string(b)
}

func (m *mcops) count(i, n int, err error) {
	if n < 2 {
		// Too short to actually know the opcode
		i = 256
	}
	if err == nil {
		platform.AddUint64(&m.success[i], 1)
	} else {
		platform.AddUint64(&m.errored[i], 1)
	}
	platform.AddUint64(&m.moved[i], uint64(n))
}

func (m *mcops) countReq(req *transport.MCRequest, n int, err error) {
	i := 256
	if req != nil {
		i = int(req.Opcode)
	}
	m.count(i, n, err)
}

func (m *mcops) countRes(res *transport.MCResponse, n int, err error) {
	i := 256
	if res != nil {
		i = int(res.Opcode)
	}
	m.count(i, n, err)
}

func newMCops() *mcops {
	return &mcops{
		moved:   platform.NewAlignedUint64(0),
		success: platform.NewAlignedUint64(0),
		errored: platform.NewAlignedUint64(0),
	}

}

func init() {
	mcSent := newMCops()
	mcRecvd := newMCops()
	tapRecvd := newMCops()

	memcached.TransmitHook = mcSent.countReq
	memcached.ReceiveHook = mcRecvd.countRes
	memcached.TapRecvHook = tapRecvd.countReq

	mcStats := expvar.NewMap("mc")
	mcStats.Set("xmit", mcSent)
	mcStats.Set("recv", mcRecvd)
	mcStats.Set("tap", tapRecvd)
}
