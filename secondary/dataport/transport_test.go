package dataport

import "fmt"
import "net"
import "testing"

import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/protobuf"
import "github.com/couchbase/indexing/secondary/transport"

func TestPktKeyVersions(t *testing.T) {
	seqno, nVbs, nMuts, nIndexes := 1, 20, 5, 5
	vbsRef := constructVbKeyVersions("default", seqno, nVbs, nMuts, nIndexes)
	tc := newTestConnection()
	tc.reset()
	flags := transport.TransportFlag(0).SetProtobuf()
	pkt := transport.NewTransportPacket(c.MaxDataportPayload, flags)
	pkt.SetEncoder(transport.EncodingProtobuf, protobufEncode)
	pkt.SetDecoder(transport.EncodingProtobuf, protobufDecode)

	if err := pkt.Send(tc, vbsRef); err != nil { // Send reference
		t.Fatal(err)
	}
	if payload, err := pkt.Receive(tc); err != nil { // Receive reference
		t.Fatal(err)
	} else { // compare both
		val := payload.([]*protobuf.VbKeyVersions)
		vbs := protobuf2VbKeyVersions(val)
		if len(vbsRef) != len(vbs) {
			t.Fatal("Mismatch in length")
		}
		for i, vb := range vbs {
			if vb.Equal(vbsRef[i]) == false {
				t.Fatal("Mismatch in VbKeyVersions")
			}
		}
	}
}

func TestPktVbmap(t *testing.T) {
	vbmapRef := &c.VbConnectionMap{
		Bucket:   "default",
		Vbuckets: []uint16{1, 2, 3, 4},
		Vbuuids:  []uint64{10, 20, 30, 40},
	}
	tc := newTestConnection()
	tc.reset()
	flags := transport.TransportFlag(0).SetProtobuf()
	pkt := transport.NewTransportPacket(c.MaxDataportPayload, flags)
	pkt.SetEncoder(transport.EncodingProtobuf, protobufEncode)
	pkt.SetDecoder(transport.EncodingProtobuf, protobufDecode)

	if err := pkt.Send(tc, vbmapRef); err != nil { // send reference
		t.Fatal(err)
	}
	if payload, err := pkt.Receive(tc); err != nil { // receive reference
		t.Fatal(err)
	} else { // Compare both
		vbmap := protobuf2Vbmap(payload.(*protobuf.VbConnectionMap))
		vbmap.Equal(vbmapRef)
	}
}

func BenchmarkSendVbKeyVersions(b *testing.B) {
	seqno, nVbs, nMuts, nIndexes := 1, 20, 5, 5
	vbs := constructVbKeyVersions("default", seqno, nVbs, nMuts, nIndexes)
	tc := newTestConnection()
	flags := transport.TransportFlag(0).SetProtobuf()
	pkt := transport.NewTransportPacket(c.MaxDataportPayload, flags)
	pkt.SetEncoder(transport.EncodingProtobuf, protobufEncode)
	pkt.SetDecoder(transport.EncodingProtobuf, protobufDecode)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tc.reset()
		pkt.Send(tc, vbs)
	}
}

func BenchmarkReceiveKeyVersions(b *testing.B) {
	seqno, nVbs, nMuts, nIndexes := 1, 20, 5, 5
	vbs := constructVbKeyVersions("default", seqno, nVbs, nMuts, nIndexes)
	tc := newTestConnection()
	flags := transport.TransportFlag(0).SetProtobuf()
	pkt := transport.NewTransportPacket(c.MaxDataportPayload, flags)
	pkt.SetEncoder(transport.EncodingProtobuf, protobufEncode)
	pkt.SetDecoder(transport.EncodingProtobuf, protobufDecode)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tc.reset()
		pkt.Send(tc, vbs)
		pkt.Receive(tc)
	}
}

func BenchmarkSendVbmap(b *testing.B) {
	vbmap := &c.VbConnectionMap{
		Bucket:   "default",
		Vbuckets: []uint16{1, 2, 3, 4},
		Vbuuids:  []uint64{10, 20, 30, 40},
	}
	tc := newTestConnection()
	flags := transport.TransportFlag(0).SetProtobuf()
	pkt := transport.NewTransportPacket(c.MaxDataportPayload, flags)
	pkt.SetEncoder(transport.EncodingProtobuf, protobufEncode)
	pkt.SetDecoder(transport.EncodingProtobuf, protobufDecode)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tc.reset()
		pkt.Send(tc, vbmap)
	}
}

func BenchmarkReceiveVbmap(b *testing.B) {
	vbmap := &c.VbConnectionMap{
		Bucket:   "default",
		Vbuckets: []uint16{1, 2, 3, 4},
		Vbuuids:  []uint64{10, 20, 30, 40},
	}
	tc := newTestConnection()
	flags := transport.TransportFlag(0).SetProtobuf()
	pkt := transport.NewTransportPacket(c.MaxDataportPayload, flags)
	pkt.SetEncoder(transport.EncodingProtobuf, protobufEncode)
	pkt.SetDecoder(transport.EncodingProtobuf, protobufDecode)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pkt.Send(tc, vbmap)
		tc.reset()
		pkt.Receive(tc)
	}
}

func constructVbKeyVersions(bucket string, seqno, nVbs, nMuts, nIndexes int) []*c.VbKeyVersions {
	vbs := make([]*c.VbKeyVersions, 0, nVbs)

	for i := 0; i < nVbs; i++ { // for N vbuckets
		vbno, vbuuid := uint16(i), uint64(i*10)
		vb := c.NewVbKeyVersions(bucket, vbno, vbuuid, nMuts)
		for j := 0; j < nMuts; j++ {
			kv := c.NewKeyVersions(uint64(seqno+j), []byte("Bourne"), nIndexes)
			for k := 0; k < nIndexes; k++ {
				key := fmt.Sprintf("bangalore%v", k)
				oldkey := fmt.Sprintf("varanasi%v", k)
				kv.AddUpsert(uint64(k), []byte(key), []byte(oldkey))
			}
			vb.AddKeyVersions(kv)
		}
		vbs = append(vbs, vb)
	}
	return vbs
}

type testConnection struct {
	roff  int
	woff  int
	buf   []byte
	laddr netAddr
	raddr netAddr
}

func newTestConnection() *testConnection {
	return &testConnection{
		buf:   make([]byte, 100000),
		laddr: netAddr("127.0.0.1:9998"),
		raddr: netAddr("127.0.0.1:9999"),
	}
}

func (tc *testConnection) Write(b []byte) (n int, err error) {
	newoff := tc.woff + len(b)
	copy(tc.buf[tc.woff:newoff], b)
	tc.woff = newoff
	return len(b), nil
}

func (tc *testConnection) Read(b []byte) (n int, err error) {
	newoff := tc.roff + len(b)
	copy(b, tc.buf[tc.roff:newoff])
	tc.roff = newoff
	return len(b), nil
}

func (tc *testConnection) LocalAddr() net.Addr {
	return tc.laddr
}

func (tc *testConnection) RemoteAddr() net.Addr {
	return tc.raddr
}

func (tc *testConnection) reset() {
	tc.woff, tc.roff = 0, 0
}

type netAddr string

func (addr netAddr) Network() string {
	return "tcp"
}

func (addr netAddr) String() string {
	return string(addr)
}
