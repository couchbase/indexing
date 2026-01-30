//go:build nolint

// Package mcdebug provides memcached client op statistics via expvar.
package mcdebug

import (
	"encoding/json"
	"expvar"
	"sync/atomic"

	"github.com/couchbase/indexing/secondary/dcp/transport"
	memcached "github.com/couchbase/indexing/secondary/dcp/transport/client"
)

type mcops struct {
	moved, success, errored [257]uint64
}

func addToMap(m map[string]uint64, i int, counters [257]uint64) {
	v := atomic.LoadUint64(&counters[i])
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
	bytes := map[string]uint64{}
	ops := map[string]uint64{}
	errs := map[string]uint64{}
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
		atomic.AddUint64(&m.success[i], 1)
	} else {
		atomic.AddUint64(&m.errored[i], 1)
	}
	atomic.AddUint64(&m.moved[i], uint64(n))
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
		moved:   0,
		success: 0,
		errored: 0,
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
