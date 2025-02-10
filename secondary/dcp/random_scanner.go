package couchbase

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	crypt "crypto/rand"

	"github.com/couchbase/indexing/secondary/dcp/transport"
	memcached "github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
)

type RandomScanner interface {
	StartRandomScan() (chan *memcached.DcpEvent, chan error, error) //TODO change channel to outch type
	StopScan() error
}

var debugId = uint32(1)

const KV_CPU_MULTIPLIER = 1
const DEFAULT_FETCH_SIZE = 1000
const RESULT_CHANNEL_SIZE = 2000

type randomScan struct {
	b *Bucket //bucket pointer

	id uint32 //debug identifier

	collId          string //collection ID
	sampleSizePerVb int64  //intended sample size per VB
	concurrency     int    //number of vbs to be sampled concurrently

	//book-keeping
	numVbs int //number of vbuckets in the bucket

	vbsByServer map[string][]int //list of vbs per kv node

	errch   chan error //err reported by vb scan workers
	abortch chan bool  //abort per vb scan
	stopch  chan bool  //channel used by the caller to stop scan

	datach chan *memcached.DcpEvent //channel to return sample docs

	appErr chan error //error channel for the caller
}

// NewRandomScanner create a new random scanner for the given collection Id for this bucket
// and the requested sample size(i.e. number of documents to scan). Bucket object cannot
// be used concurrently when using random scanner.
func (b *Bucket) NewRandomScanner(collId string, sampleSize int64) (RandomScanner, error) {

	rs := &randomScan{
		b:      b,
		collId: collId,
	}

	rs.id = atomic.AddUint32(&debugId, 1)
	rs.numVbs = b.NumVBuckets

	if sampleSize <= int64(rs.numVbs) {
		rs.sampleSizePerVb = int64(1)
	} else {
		rs.sampleSizePerVb = int64(math.Ceil(float64(sampleSize) / float64(rs.numVbs)))
	}

	logging.Infof("NewRandomScanner: created new scanner %v for bucket %v", rs, b.Name)
	return rs, nil
}

func (rs *randomScan) StartRandomScan() (chan *memcached.DcpEvent, chan error, error) {

	var errStr string

	// get all kv-nodes
	if err := rs.b.Refresh(); err != nil {
		logging.Errorf("RandomScanner:StartRandomScan %v, Err %v", rs, err)
		return nil, nil, err
	}

	smap := rs.b.VBServerMap()
	if smap == nil {
		errStr = fmt.Sprintf("No VB map for bucket %v", rs.b.Name)
		logging.Errorf("RandomScanner:StartRandomScan %v, Err %v", rs, errStr)
		return nil, nil, errors.New(errStr)
	}
	vblist := smap.VBucketMap
	if len(vblist) == 0 {
		errStr = fmt.Sprintf("Invalid VB map for bucket %v - no v-buckets", rs.b.Name)
		logging.Errorf("RandomScanner:StartRandomScan %v, Err %v", rs, errStr)
		return nil, nil, errors.New(errStr)
	}

	numServers := len(smap.ServerList)
	if numServers < 1 {
		errStr = fmt.Sprintf("Invalid VB map for bucket %v - no server list", rs.b.Name)
		logging.Errorf("RandomScanner:StartRandomScan %v, Err %v", rs, errStr)
		return nil, nil, errors.New(errStr)
	}

	//init book-keeping
	rs.init()

	err := rs.computeVbsByServer()
	if err != nil {
		return nil, nil, err
	}

	rs.setConcurrency()

	go rs.doScan()

	return rs.datach, rs.appErr, nil
}

// StopScan cancels the in-flight random scans for all vbs
func (rs *randomScan) StopScan() error {

	if rs.stopch != nil {
		close(rs.stopch)
		rs.stopch = nil
	}

	//wait for datach to be closed indicating all scan workers are done
	for _ = range rs.datach {
		//nothing to do
	}

	return nil
}

// compute the list of vbs per server
func (rs *randomScan) computeVbsByServer() error {

	var errStr string

	smap := rs.b.VBServerMap()
	vblist := smap.VBucketMap
	numServers := len(smap.ServerList)

	for vb := 0; vb < len(vblist); vb++ {
		server := 0
		if len(vblist[vb]) > 0 {
			// first server (>=0) that's in the list
			for n := 0; n < len(vblist[vb]); n++ {
				server = vblist[vb][n]
				if server >= 0 && server < numServers {
					break
				}
			}
			if server >= numServers || server < 0 {
				errStr = fmt.Sprintf("Invalid server for VB (%d): %d (max valid: %d)", vb, server, numServers-1)
				logging.Errorf("RandomScanner: ##%x, %v", rs.id, errStr)
				return errors.New(errStr)
			}
		} else {
			errStr = fmt.Sprintf("No servers for VB (%d)", vb)
			logging.Errorf("RandomScanner: ##%x, %v", rs.id, errStr)
			return errors.New(errStr)
		}
		addr := smap.ServerList[server]
		//add to the vb list for the server
		if vblist, ok := rs.vbsByServer[addr]; !ok || vblist == nil {
			vblist = make([]int, 0, rs.numVbs)
			rs.vbsByServer[addr] = vblist
		}
		rs.vbsByServer[addr] = append(rs.vbsByServer[addr], vb)
	}

	logging.Tracef("RandomScanner::vbsByServer %v", rs.vbsByServer)

	return nil
}

func (rs *randomScan) init() {

	rs.vbsByServer = make(map[string][]int)
	rs.datach = make(chan *memcached.DcpEvent, RESULT_CHANNEL_SIZE)
	rs.errch = make(chan error)
	rs.appErr = make(chan error)

	rs.stopch = make(chan bool)
	rs.abortch = make(chan bool)

	return
}

func (rs *randomScan) setConcurrency() {

	sc := 0
	maxCC := rs.numVbs
	nodes := rs.b.Nodes()
	for _, n := range nodes {
		found := false
		for _, s := range n.Services {
			if s == "kv" {
				found = true
				break
			}
		}
		if !found {
			continue
		}
		sc++
		var cc int
		ccnt := n.CpuCount
		if ccnt != 0 {
			cc = int(ccnt / float64(len(n.Services)) * KV_CPU_MULTIPLIER)
		} else {
			cc = 1
		}
		if cc < maxCC {
			maxCC = cc
		}
	}
	if sc == 0 {
		logging.Warnf("RandomScanner: Unable find any KV nodes")
	}
	if maxCC < 1 {
		maxCC = 1
	}

	rs.concurrency = maxCC

	logging.Infof("RandomScanner: Concurrency per server set to: %v", rs.concurrency)
}

func (rs *randomScan) doScan() {

	defer func() {
		//panic safe
		if r := recover(); r != nil {
			logging.Errorf("RandomScanner::doScan crashed %v", rs)
			logging.Errorf("%s", logging.StackTrace())
		}

		close(rs.datach)
	}()

	var wg sync.WaitGroup
	for addr, vblist := range rs.vbsByServer {
		wg.Add(1)
		go rs.doScanPerServer(addr, vblist, &wg)
	}

	donech := make(chan bool)

	//wait for all workers to finish
	go func() {
		logging.Tracef("RandomScanner::doScan waiting for workers to finish %s", rs)
		wg.Wait()
		logging.Tracef("RandomScanner::doScan all workers finished %s", rs)
		close(donech)
	}()

	for {
		select {

		case <-rs.stopch:
			//stop channel closed by the caller, abort all vb scans
			close(rs.abortch)
			//wait for all workers to be done
			<-donech

		case err := <-rs.errch:
			logging.Errorf("RandomScanner::doScan err %v", err)
			//Even if some vbs err out, let others continue.
			//Caller can decide to use the partial sample or abort.
			rs.appErr <- err

		case <-donech:
			return
		}
	}

}

func (rs *randomScan) doScanPerServer(addr string, vblist []int, wg *sync.WaitGroup) {

	defer func() {
		//panic safe
		if r := recover(); r != nil {
			logging.Errorf("RandomScanner::doScanPerServer crashed %v KV:%v", rs, addr)
			logging.Errorf("%s", logging.StackTrace())
			logging.Errorf("recover: %v", r)
		}

		wg.Done()
		logging.Tracef("RandomScanner::doScanPerServer finished server scan for KV %v", addr)

	}()

	logging.Tracef("RandomScanner::doScanPerServer started server scan for KV %v", addr)

	//based on the concurrency per server, start the scan
	for len(vblist) > 0 {
		var currVblist []int
		if len(vblist) > rs.concurrency {
			currVblist = vblist[:rs.concurrency]
			vblist = vblist[rs.concurrency:]
		} else {
			currVblist = vblist
			vblist = vblist[:0]
		}

		var wg2 sync.WaitGroup
		for _, vb := range currVblist {
			wg2.Add(1)
			go rs.doSingleVbScan(addr, vb, &wg2)

		}

		donech := make(chan bool)

		//wait for all workers to finish
		go func() {
			logging.Tracef("RandomScanner::doScanPerServer waiting for workers to finish for KV %v", addr)
			wg2.Wait()
			//send signal on channel to indicate all vbs have finished
			logging.Tracef("RandomScanner::doScanPerServer all workers finished for KV %v", addr)
			close(donech)
		}()

		//wait for all workers to signal done or caller to abort
		select {
		case <-rs.abortch:
			//wait for all workers to abort
			<-donech
			logging.Tracef("RandomScanner::doScanPerServer stopped all workers for KV %v", addr)
			return

			//wait for notification of all workers finishing
		case <-donech:
			break

		}
	}
}

func (rs *randomScan) doSingleVbScan(addr string, vb int, wg *sync.WaitGroup) {

	var conn *memcached.Client
	var err error

	defer func() {
		//panic safe
		if r := recover(); r != nil {
			logging.Errorf("RandomScanner::doSingleVbScan crashed %v vb:%v KV:%v", rs, vb, addr)
			logging.Errorf("%s", logging.StackTrace())
			logging.Errorf("recover: %s", r)
		}

		rs.b.Return(conn, addr)
		wg.Done()
		logging.Tracef("RandomScanner::doSingleVbScan finished vb:%v KV:%v", vb, addr)
	}()

	logging.Tracef("RandomScanner::doSingleVbScan started vb:%v KV:%v", vb, addr)

	conn, err = rs.b.GetMcConn(addr)
	if err != nil {
		logging.Errorf("RandomScanner::doSingleVbScan Error during connecting to %v. Err %v", addr, err)
		rs.errch <- err
		return
	}

	err = tryEnableRangeScanFeatures(conn)
	if err != nil {
		logging.Errorf("RandomScanner::doSingleVbScan Error enable collections %v. Err %v", addr, err)
		rs.errch <- err
		return
	}

	resp, err := conn.CreateRandomScan(uint16(vb), rs.collId, rs.sampleSizePerVb) //TODO divide into vbs
	if err != nil || len(resp.Body) < 16 {
		logging.Errorf("RandomScanner::doSingleVbScan Error during creating random scan from %v. Err %v", addr, err)
		rs.errch <- err
		return
	}
	uuid := make([]byte, 16) //KV always responds with 16 bytes uuid on success
	copy(uuid, resp.Body[0:16])

	skipCancel := false

	defer func() {
		if !skipCancel {
			conn.CancelRangeScan(uint16(vb), uuid)
		}

	}()

	for {
		err = conn.ContinueRangeScan(uint16(vb), uuid, DEFAULT_FETCH_SIZE, 0, 0)
		if resp, ok := err.(*transport.MCResponse); ok {
			if resp.Status != transport.SUCCESS &&
				resp.Status != transport.RANGE_SCAN_COMPLETE &&
				resp.Status != transport.RANGE_SCAN_MORE {
				//rest of the response codes are treated as error
				logging.Errorf("RandomScanner::doSingleVbScan Error during continue range scan from %v. Err %v", addr, err)
				rs.errch <- err
				return
			}
		} else if err != nil {
			logging.Errorf("RandomScanner::doSingleVbScan Error during continue range scan from %v. Err %v", addr, err)
			rs.errch <- err
			return
		}

	receiver:
		for {

			select {

			case <-rs.abortch:
				return

			default:
				conn.SetMcdMutationReadDeadline()
				resp, err := conn.Receive()
				if resp, ok := err.(*transport.MCResponse); ok {
					if resp.Status != transport.SUCCESS &&
						resp.Status != transport.RANGE_SCAN_COMPLETE &&
						resp.Status != transport.RANGE_SCAN_MORE {
						//rest of the response codes are treated as error
						logging.Errorf("RandomScanner::doSingleVbScan Error during continue range scan from %v. Err %v", addr, err)
						rs.errch <- err
						return
					}
				} else if err != nil {
					logging.Errorf("RandomScanner::doSingleVbScan Error during continue range scan from %v. Err %v", addr, err)
					rs.errch <- err
					return
				}

				logging.Tracef("RandomScanner::doSingleVbScan Key %v, Extra %v, Body %v", len(resp.Key), len(resp.Extras), len(resp.Body))

				rs.createAndSendDcpEvent(resp.Body)

				if resp.Status == transport.RANGE_SCAN_COMPLETE {
					//scan is complete
					logging.Tracef("RandomScanner::doSingleVbScan vb:%v scan complete", vb)
					skipCancel = true
					return
				}

				if resp.Status == transport.SUCCESS {
					//keep reading more responses
					logging.Tracef("RandomScanner::doSingleVbScan vb:%v scan success", vb)
					continue
				}

				if resp.Status == transport.RANGE_SCAN_MORE {
					//issue another continue
					logging.Tracef("RandomScanner::doSingleVbScan vb:%v scan more", vb)
					break receiver
				}
			}
		}
	}

}

// create a DcpEvent from the response body and send on the data channel
func (rs *randomScan) createAndSendDcpEvent(respBody []byte) {

	/*
		Multiple documents follow the below format:
		1. document1 {fixed meta}{varint, key}{varint, value}
		2. document2 {fixed meta}{varint, key}{varint, value}

			Fixed metadata (25 bytes)
			32-bit flags
			32-bit expiry
			64-bit seqno
			64-bit cas
			8-bit datatype
	*/

	if len(respBody) > 0 {

		num_docs := 0
		for i := 0; i < len(respBody); {
			event := &memcached.DcpEvent{}

			event.Opcode = transport.DCP_MUTATION
			event.Flags = binary.BigEndian.Uint32(respBody)
			event.Expiry = binary.BigEndian.Uint32(respBody[i+4:])
			event.Seqno = binary.BigEndian.Uint64(respBody[i+8:])
			event.Cas = binary.BigEndian.Uint64(respBody[i+16:])
			event.Datatype = uint8(respBody[i+24])

			isXATTR := event.HasXATTR()
			i += 25 //increment by fixed 25 bytes metadata length

			readLen := func() uint32 {
				var l, p uint32
				// read a length leb128 format
				l = uint32(0)
				for shift := 0; i < len(respBody); {
					p = uint32(respBody[i])
					i++
					l |= (p & uint32(0x7f)) << shift
					if p&uint32(0x80) == 0 {
						break
					}
					shift += 7
				}
				if i+int(l) > len(respBody) {
					l = uint32(len(respBody) - int(i))
				}
				return l
			}

			//read key
			l := readLen()
			logging.Tracef("RandomScanner::doSingleVbScan start %v end %v, key %s", i, i+int(l), respBody[i:i+int(l)])
			event.Key = make([]byte, int(l))
			copy(event.Key, respBody[i:i+int(l)])

			i += int(l)

			//read value
			l = readLen()
			logging.Tracef("RandomScanner::doSingleVbScan start %v end %v, value %s", i, i+int(l), respBody[i:i+int(l)])
			if isXATTR {
				xattrLen := int(binary.BigEndian.Uint32(respBody[i:]))
				xattrData := respBody[i+4 : i+4+xattrLen]
				event.RawXATTR = make(map[string][]byte)
				for len(xattrData) > 0 {
					pairLen := binary.BigEndian.Uint32(xattrData[0:])
					xattrData = xattrData[4:]
					binaryPair := xattrData[:pairLen-1]
					xattrData = xattrData[pairLen:]
					kvPair := bytes.Split(binaryPair, []byte{0x00})
					event.RawXATTR[string(kvPair[0])] = kvPair[1]
				}
				event.Value = make([]byte, len(respBody)-i-(4+xattrLen))
				copy(event.Value, respBody[i+4+xattrLen:])
			} else {
				event.Value = make([]byte, int(l))
				copy(event.Value, respBody[i:i+int(l)])
			}
			i += int(l)
			rs.datach <- event
			num_docs++
		}
		logging.Tracef("RandomScanner::doSingleVbScan doc count %v", num_docs)
	}
}

func (rs *randomScan) String() string {
	var b strings.Builder
	b.WriteString("ID:")
	b.WriteString(fmt.Sprintf("##%x", rs.id))
	b.WriteString(", collId:")
	b.WriteString(fmt.Sprintf("%v", rs.collId))
	b.WriteString(", sampleSizePerVb:")
	b.WriteString(fmt.Sprintf("%v", rs.sampleSizePerVb))
	b.WriteString(", concurrency:")
	b.WriteString(fmt.Sprintf("%v", rs.concurrency))
	return b.String()
}

// This step enables Collections, JSON, XATTR & RangeScanIncludeXattr in single HELO call.
func tryEnableRangeScanFeatures(conn *memcached.Client) error {

	connName, err := getConnName()
	if err != nil {
		return err
	}

	conn.SetMcdConnectionDeadline()
	defer conn.ResetMcdConnectionDeadline()

	err = conn.EnableRangeScanIncludeXattr(connName)
	if err != nil {
		return err
	}

	return nil
}

func getConnName() (string, error) {
	uuid, _ := NewUUID()
	name := uuid.Str()
	if name == "" {
		err := fmt.Errorf("getConnName: invalid uuid.")

		// probably not a good idea to fail if uuid
		// based name fails. Can return const string
		return "", err
	}
	connName := "secidx:randomsample" + name
	return connName, nil
}

type UUID []byte

func NewUUID() (UUID, error) {
	uuid := make([]byte, 8)
	n, err := io.ReadFull(crypt.Reader, uuid)
	if n != len(uuid) || err != nil {
		return UUID(nil), err
	}
	return UUID(uuid), nil
}

func (u UUID) Uint64() uint64 {
	return binary.LittleEndian.Uint64(([]byte)(u))
}

func (u UUID) Str() string {
	var buf bytes.Buffer
	for i := 0; i < len(u); i++ {
		if i > 0 {
			buf.WriteString(":")
		}
		buf.WriteString(strconv.FormatUint(uint64(u[i]), 16))
	}
	return buf.String()
}
