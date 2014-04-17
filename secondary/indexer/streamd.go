// Daemon listens for new connections and spawns, indirectly, a reader routine
// for each new connection.

package indexer

import (
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	"log"
	"net"
	"time"
)

// go-routine to listen for new connections, if this routine goes down -
// server is shutdown and reason notified back to application.
func streamListen(laddr string, lis net.Listener, serverch chan streamServerMessage) {
	defer func() {
		log.Println("listener panic:", recover())
		serverch <- streamServerMessage{cmd: streamgError, err: StreamDaemonExit}
	}()
	for {
		// TODO: handle `err` for lis.Close() and avoid log.Panicln()
		if conn, err := lis.Accept(); err != nil {
			panic(err)
		} else {
			serverch <- streamServerMessage{
				cmd:  streamgNewConnection,
				args: []interface{}{conn},
			}
		}
	}
}

// go-routine to just read VbConnectionMap map message, that provides a list
// of vbuckets that be expected on this connection.
//
// routine exits after receiving the intended payload.
func doReceiveVbmap(conn net.Conn, serverch chan streamServerMessage, timeout time.Duration) {
	flags := streamTransportFlag(0).setProtobuf()
	pkt := NewStreamTransportPacket(GetMaxStreamDataLen(), flags)
	msg := streamServerMessage{raddr: conn.RemoteAddr().String()}

	conn.SetReadDeadline(time.Now().Add(timeout))
	if payload, err := pkt.Receive(conn); err != nil {
		msg.cmd, msg.err = streamgError, err
	} else if vbmap, ok := payload.(*protobuf.VbConnectionMap); ok == false {
		msg.cmd, msg.err = streamgError, StreamPayloadError
	} else {
		msg.cmd, msg.args = streamgVbmap, []interface{}{vbmap}
	}
	serverch <- msg
}

// per connection go-routine to read KeyVersions and control messages like
// DropData, StreamBegin and StreamEnd.
//
// go-routine will exit in case of error, or upon receiving control messages,
// or upon receiving kill-command.
func doReceiveKeyVersions(conn net.Conn, channels []interface{}, timeout time.Duration) {
	mutch := channels[0].(chan<- interface{})
	serverch := channels[1].(chan streamServerMessage)
	killch := channels[2].(chan bool)

	flags := streamTransportFlag(0).setProtobuf()
	pkt := NewStreamTransportPacket(GetMaxStreamDataLen(), flags)
	msg := streamServerMessage{raddr: conn.RemoteAddr().String()}

loop:
	for {
		msg.cmd, msg.err, msg.args = 0, nil, nil
		conn.SetReadDeadline(time.Now().Add(timeout))
		if payload, err := pkt.Receive(conn); err != nil {
			msg.cmd, msg.err = streamgError, err
			serverch <- msg
			break loop
		} else if isControlMessage(payload) {
			kvs := payload.([]*protobuf.KeyVersions)
			msg.cmd, msg.args = streamgVbcontrol, []interface{}{kvs[0]}
			serverch <- msg
			break loop
		} else {
			kvs := payload.([]*protobuf.KeyVersions)
			select {
			case mutch <- kvs:
			case <-killch:
				msg.cmd, msg.err = streamgError, StreamWorkerKilled
				serverch <- msg
				break loop
			}
		}
	}
}

func isControlMessage(payload interface{}) bool {
	if kvs, ok := payload.([]*protobuf.KeyVersions); ok && len(kvs) == 1 {
		c := byte(kvs[0].GetCommand())
		if (c == common.DropData) || (c == common.StreamBegin) ||
			(c == common.StreamEnd) {
			return true
		}
	}
	return false
}
