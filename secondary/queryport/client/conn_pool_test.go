package client

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/indexing/secondary/transport"
)

func testMkConn(h string) (*connection, error) {
	flags := transport.TransportFlag(0).SetProtobuf()
	pkt := transport.NewTransportPacket(1024*1024, flags)
	conn, err := net.Dial("tcp", h)
	if err != nil {
		fmt.Printf("Error %v during connection\n", err)
	}
	return &connection{conn, pkt, false}, err
}

type testServer struct {
	ln net.Listener
}

func (ts *testServer) initServer(h string, stopCh chan bool) error {
	ipport := strings.Split(h, ":")
	ln, err := net.Listen("tcp", ":"+ipport[1])
	ts.ln = ln
	if err != nil {
		msg := fmt.Sprintf("Error %v during Listen", err)
		return errors.New(msg)
	}
	for {
		_, err := ln.Accept()
		if err != nil {
			select {
			case <-stopCh:
				return nil

			default:
				msg := fmt.Sprintf("Error %v during Accept", err)
				return errors.New(msg)
			}
		}
	}
}

func TestConnPoolBasicSanity(t *testing.T) {
	var err error
	var sc *connection

	readDeadline := time.Duration(30)
	writeDeadline := time.Duration(40)

	ts := &testServer{}
	tsStopCh := make(chan bool, 1)

	host := "127.0.0.1:15151"
	go ts.initServer(host, tsStopCh)
	time.Sleep(1 * time.Second)

	var needsAuth uint32
	cp := newConnectionPool(host, 3, 6, 1024*1024, readDeadline, writeDeadline, 3, 1, 1, "", &needsAuth, true)
	cp.mkConn = testMkConn

	seenClients := map[*connection]bool{}

	// assert overflowLimit
	if cp.overflowLimit != 9 {
		t.Errorf("Expected value of overflowLimit = 9, actual = %v", cp.overflowLimit)
	}

	// assert active connections closing flag
	if !cp.closeActiveConnections {
		t.Errorf("Expected closeActiveConnections flag to be true")
	}

	// build some connections
	for i := 0; i < 5; i++ {
		sc, err = cp.Get()
		if err != nil {
			t.Fatalf("Error getting connection from pool: %v", err)
		}
		seenClients[sc] = true
	}

	// wait for consumer to finish
	time.Sleep(10 * time.Millisecond)

	if len(cp.activeConnections) != 5 {
		t.Errorf("Incorrect number of active connections. Expected %v, actual %v", 5, len(cp.activeConnections))
	}

	// return them
	for k := range seenClients {
		cp.Return(k, true)
	}

	// wait for consumer to finish
	time.Sleep(10 * time.Millisecond)

	if len(cp.activeConnections) != 0 {
		t.Errorf("Incorrect number of active connections. Expected %v, actual %v", 0, len(cp.activeConnections))
	}

	err = cp.Close()
	if err != nil {
		t.Errorf("Expected clean close, got %v", err)
	}

	cp.Close()
	time.Sleep(2 * time.Second)

	tsStopCh <- true
	ts.ln.Close()
	time.Sleep(1 * time.Second)
}

func createPoolAndStartServer(closeActiveConnections bool) (*testServer, *connectionPool, chan bool) {
	readDeadline := time.Duration(30)
	writeDeadline := time.Duration(40)

	ts := &testServer{}
	tsStopCh := make(chan bool, 1)

	host := "127.0.0.1:15151"
	go ts.initServer(host, tsStopCh)
	time.Sleep(1 * time.Second)

	var needsAuth uint32
	cp := newConnectionPool(host, 3, 6, 1024*1024, readDeadline, writeDeadline, 3, 1, 1, "", &needsAuth, closeActiveConnections)
	cp.mkConn = testMkConn

	return ts, cp, tsStopCh
}

func closePoolAndServer(cp *connectionPool, tsStopCh chan bool, ts *testServer) {
	// close the pool
	cp.Close()
	time.Sleep(2 * time.Second)

	tsStopCh <- true
	ts.ln.Close()
	time.Sleep(1 * time.Second)
}

func addToActiveConnections(count int, cp *connectionPool, t *testing.T) {
	var err error
	// Add to active connections
	for i := 0; i < count; i++ {
		_, err = cp.Get()
		if err != nil {
			t.Fatalf("Error getting connection from pool: %v", err)
		}
	}
}

func isConnClosed(conn net.Conn, t *testing.T) bool {
	// Set a small read deadline to unblock
	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	buffer := make([]byte, 1)

	// Try to read from the connection
	_, err := conn.Read(buffer)

	if err != nil && strings.Contains(strings.ToLower(err.Error()), "use of closed network connection") {
		return true
	}

	return false
}

func TestCloseActiveConnectionsUnsetFlag(t *testing.T) {

	ts, cp, tsStopCh := createPoolAndStartServer(false)

	addToActiveConnections(5, cp, t)

	// wait for consumer to finish
	time.Sleep(10 * time.Millisecond)

	// assert size of active connections
	if len(cp.activeConnections) != 5 {
		t.Errorf("Incorrect number of active connections. Expected %v, actual %v", 5, len(cp.activeConnections))
	}

	activeConnections := map[*connection]struct{}{}
	for conn := range cp.activeConnections {
		activeConnections[conn] = struct{}{}
	}

	closePoolAndServer(cp, tsStopCh, ts)

	// assert that the active connections are removed
	if len(cp.activeConnections) != 0 {
		t.Errorf("Active connections should be removed from the pool")
	}

	for conn := range activeConnections {
		if isConnClosed(conn.conn, t) {
			t.Errorf("Active connection should not be closed")
		}
	}

	// release resources
	for conn := range activeConnections {
		conn.conn.Close()
	}
}

func TestCloseActiveConnectionsSetFlag(t *testing.T) {
	ts, cp, tsStopCh := createPoolAndStartServer(true)

	addToActiveConnections(5, cp, t)

	// wait for consumer to finish
	time.Sleep(10 * time.Millisecond)

	// assert size of active connections
	if len(cp.activeConnections) != 5 {
		t.Errorf("Incorrect number of active connections. Expected %v, actual %v", 5, len(cp.activeConnections))
	}

	activeConnections := map[*connection]struct{}{}
	for conn := range cp.activeConnections {
		activeConnections[conn] = struct{}{}
	}

	closePoolAndServer(cp, tsStopCh, ts)

	// assert that the active connections are removed
	if len(cp.activeConnections) != 0 {
		t.Errorf("Active connections should be removed from the pool")
	}

	for conn := range activeConnections {
		if !isConnClosed(conn.conn, t) {
			t.Errorf("Active connection should be closed")
		}
	}

	// release resources
	for conn := range activeConnections {
		// no-op if already closed
		conn.conn.Close()
	}
}

func TestMaintainActiveConnections(t *testing.T) {
	ts, cp, tsStopCh := createPoolAndStartServer(true)

	addToActiveConnections(5, cp, t)

	// wait for consumer to finish
	time.Sleep(1 * time.Millisecond)

	if len(cp.activeConnections) != 5 {
		t.Errorf("Incorrect number of active connections. Expected %v, actual %v", 5, len(cp.activeConnections))
	}

	activeConnections := map[*connection]struct{}{}
	for conn := range cp.activeConnections {
		activeConnections[conn] = struct{}{}
	}

	i := 0
	// remove some connections
	for conn := range cp.activeConnections {
		if i == 3 {
			break
		}
		i++

		delete(activeConnections, conn)
		err := cp.pushActiveConnectionUpdates(&ActiveConnOperation{Operation: Delete, conn: conn})
		if err != nil {
			t.Fatalf("Error while removing connection from active connections %v", err)
		}
	}

	// wait for consumer to finish
	time.Sleep(1 * time.Millisecond)

	if len(cp.activeConnections) != 2 {
		t.Errorf("Incorrect number of active connections. Expected %v, actual %v", 2, len(cp.activeConnections))
	}

	// check if the correct connections are removed
	for conn := range cp.activeConnections {
		if _, ok := activeConnections[conn]; !ok {
			t.Errorf("Deleted incorrect active connection")
		}
	}

	// add back connections
	addToActiveConnections(3, cp, t)

	// wait for consumer to finish
	time.Sleep(1 * time.Millisecond)

	if len(cp.activeConnections) != 5 {
		t.Errorf("Incorrect number of active connections. Expected %v, actual %v", 5, len(cp.activeConnections))
	}

	// delete
	i = 0
	// remove some connections
	for conn := range cp.activeConnections {
		if i == 3 {
			break
		}
		i++

		err1 := cp.pushActiveConnectionUpdates(&ActiveConnOperation{Operation: Delete, conn: conn})
		// should be idempotent
		err2 := cp.pushActiveConnectionUpdates(&ActiveConnOperation{Operation: Delete, conn: conn})
		if err1 != nil || err2 != nil {
			t.Fatalf("Error while removing connection from active connections %v || %v", err1, err2)
		}
	}

	// wait for consumer to finish
	time.Sleep(1 * time.Millisecond)

	if len(cp.activeConnections) != 2 {
		t.Errorf("Incorrect number of active connections. Expected %v, actual %v", 2, len(cp.activeConnections))
	}

	closePoolAndServer(cp, tsStopCh, ts)

	// Should panic if we update after closing the channel
	for conn := range activeConnections {
		err := cp.pushActiveConnectionUpdates(&ActiveConnOperation{Operation: Delete, conn: conn})
		if err == nil || err != ErrorClosedPool {
			t.Fatalf("Active connections channel should have been closed")
		}
	}

	_, err := cp.Get()
	if !errors.Is(err, ErrorClosedPool) {
		t.Fatalf("The connection pool should have been closed: %v", err)
	}
}

func TestConnectionPoolRenew(t *testing.T) {

	ts, cp, tsStopCh := createPoolAndStartServer(true)

	addToActiveConnections(5, cp, t)

	// wait for consumer to finish
	time.Sleep(1 * time.Millisecond)

	if len(cp.activeConnections) != 5 {
		t.Errorf("Incorrect number of active connections. Expected %v, actual %v", 5, len(cp.activeConnections))
	}

	activeConnections := map[*connection]struct{}{}
	for conn := range cp.activeConnections {
		activeConnections[conn] = struct{}{}
	}

	renewedActiveConnections := map[*connection]struct{}{}

	// Renew all connections
	for conn := range activeConnections {
		c, err, poolClosed := cp.Renew(conn)

		if err != nil {
			t.Fatalf("Error renewing the connection %v", err)
		}

		if poolClosed {
			t.Fatalf("Pool closed")
		}

		if _, ok := activeConnections[c]; ok {
			t.Errorf("Connection %v is not renewed", c)
		}

		renewedActiveConnections[c] = struct{}{}
	}

	// wait for consumer to finish
	time.Sleep(1 * time.Millisecond)

	if len(cp.activeConnections) != 5 {
		t.Errorf("Incorrect number of active connections. Expected %v, actual %v", 5, len(cp.activeConnections))
	}

	for conn := range renewedActiveConnections {
		if _, ok := cp.activeConnections[conn]; !ok {
			t.Errorf("Renewed connection %v not added to active connections", conn)
		}
	}

	// close the pool
	cp.Close()
	time.Sleep(2 * time.Second)

	if len(cp.activeConnections) != 0 {
		t.Errorf("Incorrect number of active connections. Expected %v, actual %v", 0, len(cp.activeConnections))
	}

	// Try to renew a connection
	for conn := range renewedActiveConnections {
		_, err, poolClosed := cp.Renew(conn)

		if err != nil {
			t.Fatalf("Error renewing the connection %v", err)
		}

		if !poolClosed {
			t.Fatalf("Pool should have been closed")
		}

		if !isConnClosed(conn.conn, t) {
			t.Errorf("Active connection should be closed")
		}

		break
	}

	// close server
	tsStopCh <- true
	ts.ln.Close()
	time.Sleep(1 * time.Second)

}

func TestConnRelease(t *testing.T) {
	readDeadline := time.Duration(30)
	writeDeadline := time.Duration(40)

	ts := &testServer{}
	tsStopCh := make(chan bool, 1)

	host := "127.0.0.1:15151"
	go ts.initServer(host, tsStopCh)
	time.Sleep(1 * time.Second)

	var needsAuth uint32
	cp := newConnectionPool(host, 500, 10, 1024*1024, readDeadline, writeDeadline, 40, 10, 1, "", &needsAuth, true)
	cp.mkConn = testMkConn

	seenClients := map[*connection]bool{}

	// Get 240 Connections.

	for i := 0; i < 240; i++ {
		sc, err := cp.Get()
		if err != nil {
			t.Fatalf("Error getting connection from pool: %v", err)
		}
		seenClients[sc] = true
	}

	// Return 220 of them
	j := 0
	for k := range seenClients {
		cp.Return(k, true)
		delete(seenClients, k)
		j++
		if j >= 220 {
			break
		}
	}

	// time.Sleep(time.Millisecond)
	if cp.freeConns != 220 {
		t.Errorf("Warning! cp.freeConns is not 220, its %d", cp.freeConns)
	}

	fmt.Println("Waiting for connections to get released")
	time.Sleep(CONN_RELEASE_INTERVAL * 2 * time.Second)
	if cp.freeConns != 200 {
		t.Errorf("Warning! cp.freeConns is not 200, its %d", cp.freeConns)
	}

	fmt.Println("Waiting for more connections to get released")
	time.Sleep(CONN_RELEASE_INTERVAL * 2 * time.Second)
	if cp.freeConns != 180 {
		t.Errorf("Warning! cp.freeConns is not 180, its %d", cp.freeConns)
	}

	fmt.Println("Waiting for further more connections to get released")
	time.Sleep(CONN_RELEASE_INTERVAL * 2 * time.Second)
	if cp.freeConns != 160 {
		t.Errorf("Warning! cp.freeConns is not 160, its %d", cp.freeConns)
	}

	for l := range seenClients {
		cp.Return(l, true)
		delete(seenClients, l)
	}

	cp.Close()
	time.Sleep(2 * time.Second)

	tsStopCh <- true
	ts.ln.Close()
	time.Sleep(1 * time.Second)
}

func TestLongevity(t *testing.T) {
	readDeadline := time.Duration(30)
	writeDeadline := time.Duration(40)

	ts := &testServer{}
	tsStopCh := make(chan bool, 1)

	host := "127.0.0.1:15151"
	go ts.initServer(host, tsStopCh)
	time.Sleep(1 * time.Second)

	var needsAuth uint32
	cp := newConnectionPool(host, 500, 10, 1024*1024, readDeadline, writeDeadline, 40, 10, 1, "", &needsAuth, true)
	cp.mkConn = testMkConn

	// Get 240 Connections.

	seenClients := map[*connection]bool{}

	for i := 0; i < 200; i++ {
		sc, err := cp.Get()
		if err != nil {
			t.Fatalf("Error getting connection from pool: %v", err)
		}
		seenClients[sc] = true
	}

	for i := 0; i < 30; i++ {
		time.Sleep(time.Second)
		num := rand.Intn(5)
		j := 0
		if i%2 == 0 {
			fmt.Printf("Releasing %d conns.\n", num)
			for k := range seenClients {
				cp.Return(k, true)
				delete(seenClients, k)
				j++
				if j >= num {
					break
				}
			}
		} else {
			fmt.Printf("Getting %d conns.\n", num)
			for k := 0; k < num; k++ {
				sc, err := cp.Get()
				if err != nil {
					t.Fatalf("Error getting connection from pool: %v", err)
				}
				seenClients[sc] = true
			}
		}

		// Use some safe number to verify.
		if cp.freeConns > 20 {
			t.Errorf("Warning! cp.freeConns is greater than 20, its %d", cp.freeConns)
		}
	}

	for l := range seenClients {
		cp.Return(l, true)
		delete(seenClients, l)
	}

	cp.Close()
	time.Sleep(2 * time.Second)

	tsStopCh <- true
	ts.ln.Close()
	time.Sleep(1 * time.Second)
}

func startAllocatorRoutine(cp *connectionPool, ch chan *connection, stopCh chan bool) {
	for {
		time.Sleep(500 * time.Millisecond)
		var num int

		select {
		case <-stopCh:
			fmt.Println("Retuning from startAllocatorRoutine")
			return

		default:
			if cp.curActConns < 200 {
				num = rand.Intn(25)
			} else {
				num = rand.Intn(5)
			}
			fmt.Println("Allocating", num, "Connections")
			for i := 0; i < num; i++ {
				conn, err := cp.Get()
				if err != nil {
					errmsg := fmt.Sprintf("ERROR %v: CONNECTION GET FAILED", err)
					panic(errmsg)
				}
				ch <- conn
			}
			if cp.curActConns > 250 {
				errmsg := fmt.Sprintf("ERROR: TOO MANY ACTIVE CONNS %v", cp.curActConns)
				panic(errmsg)
			}
		}
	}
}

func startDeallocatorRoutine(cp *connectionPool, ch chan *connection, stopCh chan bool) {
	for {
		time.Sleep(500 * time.Millisecond)

		select {
		case <-stopCh:
			fmt.Println("Retuning from startDeallocatorRoutine")
			return

		default:
			num := rand.Intn(5)
			fmt.Println("Returning", num, "Connections")
			for i := 0; i < num; i++ {
				conn := <-ch
				cp.Return(conn, true)
			}
		}
	}
}

func TestSustainedHighConns(t *testing.T) {
	readDeadline := time.Duration(30)
	writeDeadline := time.Duration(40)

	ts := &testServer{}
	tsStopCh := make(chan bool, 1)

	host := "127.0.0.1:15151"
	go ts.initServer(host, tsStopCh)
	time.Sleep(1 * time.Second)

	var needsAuth uint32
	cp := newConnectionPool(host, 500, 10, 1024*1024, readDeadline, writeDeadline, 40, 10, 1, "", &needsAuth, true)
	cp.mkConn = testMkConn

	ch := make(chan *connection, 1000)

	stopCh := make(chan bool, 2)

	go startAllocatorRoutine(cp, ch, stopCh)
	go startDeallocatorRoutine(cp, ch, stopCh)

	for i := 0; i < 100; i++ {
		time.Sleep(500 * time.Millisecond)
		fmt.Println("cp.curActConns =", cp.curActConns)
	}

	stopCh <- true
	stopCh <- true

	time.Sleep(5 * time.Second)
	close(ch)
	for connectn := range ch {
		cp.Return(connectn, true)
	}

	cp.Close()
	time.Sleep(2 * time.Second)

	tsStopCh <- true
	ts.ln.Close()
	time.Sleep(1 * time.Second)
}

func TestLowWM(t *testing.T) {
	readDeadline := time.Duration(30)
	writeDeadline := time.Duration(40)

	ts := &testServer{}
	tsStopCh := make(chan bool, 1)

	host := "127.0.0.1:15151"
	go ts.initServer(host, tsStopCh)
	time.Sleep(1 * time.Second)

	var needsAuth uint32
	cp := newConnectionPool(host, 20, 5, 1024*1024, readDeadline, writeDeadline, 10, 2, 1, "", &needsAuth, true)
	cp.mkConn = testMkConn

	seenClients := map[*connection]bool{}

	for i := 0; i < 12; i++ {
		sc, err := cp.Get()
		if err != nil {
			t.Fatalf("Error getting connection from pool: %v", err)
		}
		seenClients[sc] = true
	}

	time.Sleep(CONN_RELEASE_INTERVAL * time.Second)
	for k := range seenClients {
		cp.Return(k, true)
	}

	if cp.freeConns != 12 {
		t.Errorf("Expected value fo freeConns = 12, actual = %v", cp.freeConns)
	}

	// Wait 5 mins. Make sure that freeConns never get below 10.
	for i := 0; i < 24; i++ {
		time.Sleep(CONN_RELEASE_INTERVAL * time.Second)
		if cp.freeConns < 10 {
			msg := fmt.Sprintf("freeConns (%v) went below low WM", cp.freeConns)
			panic(msg)
		}
	}

	for l := range seenClients {
		cp.Return(l, true)
		delete(seenClients, l)
	}

	cp.Close()
	time.Sleep(2 * time.Second)

	tsStopCh <- true
	ts.ln.Close()
	time.Sleep(1 * time.Second)
}

func TestTotalConns(t *testing.T) {
	readDeadline := time.Duration(30)
	writeDeadline := time.Duration(40)

	ts := &testServer{}
	tsStopCh := make(chan bool, 1)

	host := "127.0.0.1:15151"
	go ts.initServer(host, tsStopCh)
	time.Sleep(1 * time.Second)

	var needsAuth uint32
	cp := newConnectionPool(host, 120, 5, 1024*1024, readDeadline, writeDeadline, 10, 10, 1, "", &needsAuth, true)
	cp.mkConn = testMkConn

	seenClients := map[*connection]bool{}

	// Get 100 connections.

	for i := 0; i < 100; i++ {
		sc, err := cp.Get()
		if err != nil {
			t.Fatalf("Error getting connection from pool: %v", err)
		}
		seenClients[sc] = true
	}

	// Return 20 of them
	i := 0
	for k := range seenClients {
		cp.Return(k, true)
		i++
		if i >= 20 {
			break
		}
	}

	if cp.freeConns != 20 {
		t.Errorf("Expected value for freeConns = 20, actual = %v", cp.freeConns)
	}

	if cp.curActConns != 80 {
		t.Errorf("Expected value fo curActConns = 80, actual = %v", cp.curActConns)
	}

	time.Sleep(10 * time.Millisecond)

	if len(cp.activeConnections) != 80 {
		t.Errorf("Incorrect number of active connections. Expected %v, actual %v", 80, len(cp.activeConnections))
	}

	// Sleep for an interval. Avg will be 80. Expect 10 conns getting freed.
	time.Sleep(CONN_RELEASE_INTERVAL * time.Second)

	if cp.freeConns != 10 {
		t.Errorf("Expected value for freeConns = 10, actual = %v", cp.freeConns)
	}

	// Release 20 more conns.
	j := 0
	for k := range seenClients {
		cp.Return(k, true)
		j++
		if j >= 20 {
			break
		}
	}

	if cp.freeConns != 30 {
		t.Errorf("Expected value for freeConns = 30, actual = %v", cp.freeConns)
	}

	if cp.curActConns != 60 {
		t.Errorf("Expected value fo curActConns = 60, actual = %v", cp.curActConns)
	}

	time.Sleep(10 * time.Millisecond)

	if len(cp.activeConnections) != 60 {
		t.Errorf("Incorrect number of active connections. Expected %v, actual %v", 60, len(cp.activeConnections))
	}

	// Sleep for an interval. Avg will be 80. Expect 10 conns getting freed.
	time.Sleep(CONN_RELEASE_INTERVAL * time.Second)

	if cp.freeConns != 20 {
		t.Errorf("Expected value for freeConns = 20, actual = %v", cp.freeConns)
	}

	cp.Close()
	time.Sleep(2 * time.Second)

	tsStopCh <- true
	ts.ln.Close()
	time.Sleep(1 * time.Second)
}

func TestUpdateTickRate(t *testing.T) {
	readDeadline := time.Duration(30)
	writeDeadline := time.Duration(40)

	ts := &testServer{}
	tsStopCh := make(chan bool, 1)

	host := "127.0.0.1:15151"
	go ts.initServer(host, tsStopCh)
	time.Sleep(1 * time.Second)

	var needsAuth uint32
	cp := newConnectionPool(host, 40, 5, 1024*1024, readDeadline, writeDeadline, 2, 2, 1, "", &needsAuth, true)
	cp.mkConn = testMkConn

	seenClients := map[*connection]bool{}

	// Allocate 20 conns per seconds for 10 seconds. Return all connections after 1 second.

	for i := 0; i < 10; i++ {
		for j := 0; j < 20; j++ {
			sc, err := cp.Get()
			if err != nil {
				t.Fatalf("Error getting connection from pool: %v", err)
			}
			seenClients[sc] = true
		}
		time.Sleep(1 * time.Second)
		for k := range seenClients {
			cp.Return(k, true)
			delete(seenClients, k)
		}
	}

	// Make sure that numConnsToRetain returns false and 20.
	numRetConns, needToFreeConns := cp.numConnsToRetain()
	if needToFreeConns != false {
		t.Errorf("needToFreeConns was expected to be false. But it is not")
		fmt.Printf("freeConns = %v, curActConns = %v, rate = %v\n", cp.freeConns, cp.curActConns, cp.ewma.Rate())
	}

	if numRetConns != 20 {
		t.Errorf("numRetConns was expected to be 20, Actual = %v", numRetConns)
		fmt.Printf("freeConns = %v, curActConns = %v, rate = %v\n", cp.freeConns, cp.curActConns, cp.ewma.Rate())
	}

	// Allocate 10 conns per seconds for 10 seconds. Return all connections after 1 second.
	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			sc, err := cp.Get()
			if err != nil {
				t.Fatalf("Error getting connection from pool: %v", err)
			}
			seenClients[sc] = true
		}
		time.Sleep(1 * time.Second)
		for k := range seenClients {
			cp.Return(k, true)
			delete(seenClients, k)
		}
	}

	// Make sure that numConnsToRetain still returns false and 20.
	numRetConns, needToFreeConns = cp.numConnsToRetain()
	if needToFreeConns != false {
		t.Errorf("needToFreeConns was expected to be false. But it is not")
		fmt.Printf("freeConns = %v, curActConns = %v, rate = %v\n", cp.freeConns, cp.curActConns, cp.ewma.Rate())
	}

	if numRetConns != 20 {
		t.Errorf("numRetConns was expected to be 20, Actual = %v", numRetConns)
		fmt.Printf("freeConns = %v, curActConns = %v, rate = %v\n", cp.freeConns, cp.curActConns, cp.ewma.Rate())
	}

	cp.Close()
	time.Sleep(2 * time.Second)

	tsStopCh <- true
	ts.ln.Close()
	time.Sleep(1 * time.Second)
}
