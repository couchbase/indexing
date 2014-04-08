// transporting a mutation packet.
//  { uint32(packetlen), uint16(flags), []byte(mutation) }
//
// `flags` can be used for specifying encoding format of mutation, type of
// compression etc.
//
// packetlen == len(mutation)
//
// concurrency model:        *---------------*------------------*
//                           | error channel | mutation channel |
//      NewStreamServer()    *---------------*------------------*
//              |                   ^  ^ .. ^
//              |                   |  |    | (mutations & errors back to appl.)
//              V                   |  |    |
//        listener routine --*----> doReceive() per connection routine
//                           |
//                           *----> doReceive() per connection routine
//                           ...
//                           *----> doReceive() per connection routine

package indexer

import (
    "encoding/binary"
    "errors"
    "fmt"
    "github.com/couchbase/indexing/secondary/common"
    "io"
    "log"
    "net"
    "sync"
)

const MAX_STREAM_DATA_LEN = 10 * 1024 // Maximum packet length
var   StreamServerClosed  = errors.New("StreamServerClosed")

// StreamServer contains as active server instance
type StreamServer struct {
    mu    sync.Mutex
    laddr string                  // address to listen
    mutch chan<- *common.Mutation // application channel to send mutations
    errch chan<- error            // application channel to send error
    ln    net.Listener
}

// NewStreamServer creates a new stream server to receive stream mutations
func NewStreamServer(laddr string, mutch chan<- *common.Mutation, errch chan<- error) (s *StreamServer, err error) {
    killchs := make([]chan bool, 0) // kill switch for each connection

    s = &StreamServer{laddr: laddr, mutch: mutch, errch: errch}
    if s.ln, err = net.Listen("tcp", laddr); err != nil {
        return nil, err
    }

    // Server routine
    go func() {
        for {
            conn, err := s.ln.Accept() // wait for new client connection
            if err != nil {
                log.Printf("server %v quiting due to %v ...", laddr, err)
                errch <- StreamServerClosed
                break
            }
            // for a new client connection
            killch := make(chan bool)
            go s.doReceive(conn, killch)
            killchs = append(killchs, killch)
        }

        s.mu.Lock()
        defer s.mu.Unlock()

        // when server exits kill all active connections.
        s.ln = nil
        for _, killch := range killchs {
            close(killch)
        }
    }()
    return s, nil
}

// Stop an active stream server. All active connections will be shutdown.
func (s *StreamServer) Stop() {
    s.mu.Lock()
    defer s.mu.Unlock()
    if s.ln != nil {
        log.Printf("Stopping server %v", s.laddr)
        s.ln.Close() // server routine will exit because of this
        s.ln = nil
    }
}

func (s *StreamServer) doReceive(conn net.Conn, killSwitch chan bool) {
    var m *common.Mutation
    var err error

    // reuse the buffer to receive packet
    connBuf := make([]byte, MAX_STREAM_DATA_LEN)

loop:
    for {
        if m, err = readMutation(conn, connBuf); err != nil {
            select {
            case s.errch <- err:
            case <-killSwitch:
                break loop
            }
            if err == io.EOF {
                break
            }
            continue
        }
        select {
        case s.mutch <- m:
        case <-killSwitch:
            break loop
        }
    }
    log.Println("Connection %v closing for server %v", conn.RemoteAddr(), s.laddr)
}

// read a full mutation packet and decode it
func readMutation(conn net.Conn, buf []byte) (*common.Mutation, error) {
    var pktlen uint32
    //var flags  uint16
    var err error

    if pktlen, err = readPacketLen(conn, buf[:4]); err != nil {
        return nil, err
    }
    if _, err = readPacketFlag(conn, buf[:2]); err != nil { // TODO flags is unused
        return nil, err
    }
    if pktlen > MAX_STREAM_DATA_LEN {
        err = fmt.Errorf("packet length is greater than %v", MAX_STREAM_DATA_LEN)
        return nil, err
    }

    if err = readPacketData(conn, buf[:pktlen]); err != nil {
        return nil, err
    }

    m := &common.Mutation{}
    err = m.Decode(buf[:pktlen])
    return m, nil
}

func readPacketLen(conn net.Conn, buf []byte) (pktlen uint32, err error) {
    var n int

    if n, err = conn.Read(buf); err != nil {
        log.Printf("%v reading from %v: %v", conn.RemoteAddr(), conn.LocalAddr(), err)
        return
    } else if n != len(buf) {
        err = fmt.Errorf("partially read %v, expected to read %v", n, len(buf))
        return
    }
    pktlen = uint32(binary.BigEndian.Uint32(buf))
    return
}

func readPacketFlag(conn net.Conn, buf []byte) (flags uint16, err error) {
    var n int

    if n, err = conn.Read(buf); err != nil {
        log.Printf("%v reading from %v: %v", conn.RemoteAddr(), conn.LocalAddr(), err)
        return
    } else if n != len(buf) {
        err = fmt.Errorf("partially read %v, expected to read %v", n, len(buf))
        return
    }
    flags = uint16(binary.BigEndian.Uint16(buf))
    return
}

func readPacketData(conn net.Conn, buf []byte) (err error) {
    var n int
    if n, err = conn.Read(buf); err != nil {
        log.Printf("%v reading from %v: %v", conn.RemoteAddr(), conn.LocalAddr(), err)
        return
    } else if n != len(buf) {
        err = fmt.Errorf("partially read %v, expected to read %v", n, len(buf))
        return
    }
    return
}
