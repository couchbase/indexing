// Package queryport provides a simple library to spawn a queryport and access
// queryport via passive client API.
//
// ---> Request                 ---> Request
//      <--- Response                <--- Response
//      <--- Response                <--- Response
//      ...                     ---> EndStreamRequest
//      <--- StreamEndResponse       <--- Response (residue)
//                                   <--- StreamEndResponse

package client

import (
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"sync/atomic"

	"github.com/couchbase/indexing/secondary/common"
	json "github.com/couchbase/indexing/secondary/common/json"
	"github.com/couchbase/indexing/secondary/logging"

	protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
	"github.com/couchbase/indexing/secondary/transport"
	"github.com/golang/protobuf/proto"
)

// GsiScanClient for scan operations.
type GsiScanClient struct {
	queryport string
	pool      *connectionPool
	// config params
	maxPayload             int // TODO: what if it exceeds ?
	readDeadline           time.Duration
	writeDeadline          time.Duration
	poolSize               int
	poolOverflow           int
	cpTimeout              time.Duration
	cpAvailWaitTimeout     time.Duration
	logPrefix              string
	minPoolSizeWM          int32
	relConnBatchSize       int32
	closeActiveConnections bool

	serverVersion uint32
	closed        uint32
	needsAuth     *uint32
}

func NewGsiScanClient(queryport, cluster string, config common.Config, needsAuth *uint32) (*GsiScanClient, error) {
	t := time.Duration(config["connPoolAvailWaitTimeout"].Int())

	closeActiveConnections := false

	if val, ok := config["settings.closeActiveConnections"]; ok {
		closeActiveConnections = val.Bool()
	}

	c := &GsiScanClient{
		queryport:              queryport,
		maxPayload:             config["maxPayload"].Int(),
		readDeadline:           time.Duration(config["readDeadline"].Int()),
		writeDeadline:          time.Duration(config["writeDeadline"].Int()),
		poolSize:               config["settings.poolSize"].Int(),
		poolOverflow:           config["settings.poolOverflow"].Int(),
		cpTimeout:              time.Duration(config["connPoolTimeout"].Int()),
		cpAvailWaitTimeout:     t,
		logPrefix:              fmt.Sprintf("[GsiScanClient:%q]", queryport),
		minPoolSizeWM:          int32(config["settings.minPoolSizeWM"].Int()),
		relConnBatchSize:       int32(config["settings.relConnBatchSize"].Int()),
		needsAuth:              needsAuth,
		closeActiveConnections: closeActiveConnections,
	}
	c.pool = newConnectionPool(
		queryport, c.poolSize, c.poolOverflow, c.maxPayload, c.cpTimeout,
		c.cpAvailWaitTimeout, c.minPoolSizeWM, c.relConnBatchSize, config["keepAliveInterval"].Int(),
		cluster, needsAuth, closeActiveConnections)
	logging.Infof("%v started ...\n", c.logPrefix)

	if version, err := c.Helo(); err == nil || err == io.EOF {
		atomic.StoreUint32(&c.serverVersion, version)
	} else {
		c.pool.Close()
		return nil, fmt.Errorf("%s: unable to obtain server version. Error = %v", queryport, err)
	}

	return c, nil
}

func (c *GsiScanClient) RefreshServerVersion() {
	// refresh the version ONLY IF there is no error, so we absolutely
	// know we have right version.
	if version, err := c.Helo(); err == nil {
		if version != atomic.LoadUint32(&c.serverVersion) {
			atomic.StoreUint32(&c.serverVersion, version)
		}
	}
}

func (c *GsiScanClient) NeedSessionConsVector() bool {
	return atomic.LoadUint32(&c.serverVersion) == 0
}

func (c *GsiScanClient) Helo() (uint32, error) {
	req := &protobuf.HeloRequest{
		Version: proto.Uint32(uint32(protobuf.ProtobufVersion())),
	}

	resp, _, err := c.doRequestResponse(req, "", true)
	if err != nil {
		return 0, err
	}
	heloResp := resp.(*protobuf.HeloResponse)
	return heloResp.GetVersion(), nil
}

// LookupStatistics for a single secondary-key.
func (c *GsiScanClient) LookupStatistics(
	defnID uint64, value common.SecondaryKey) (common.IndexStatistics, error) {

	// serialize lookup value.
	val, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	req := &protobuf.StatisticsRequest{
		DefnID: proto.Uint64(defnID),
		Span:   &protobuf.Span{Equals: [][]byte{val}},
	}
	resp, _, err := c.doRequestResponse(req, "", true)
	if err != nil {
		return nil, err
	}
	statResp := resp.(*protobuf.StatisticsResponse)
	if statResp.GetErr() != nil {
		err = errors.New(statResp.GetErr().GetError())
		return nil, err
	}
	return statResp.GetStats(), nil
}

// RangeStatistics for index range.
func (c *GsiScanClient) RangeStatistics(
	defnID uint64, low, high common.SecondaryKey,
	inclusion Inclusion) (common.IndexStatistics, error) {

	// serialize low and high values.
	l, err := json.Marshal(low)
	if err != nil {
		return nil, err
	}
	h, err := json.Marshal(high)
	if err != nil {
		return nil, err
	}

	req := &protobuf.StatisticsRequest{
		DefnID: proto.Uint64(defnID),
		Span: &protobuf.Span{
			Range: &protobuf.Range{
				Low: l, High: h, Inclusion: proto.Uint32(uint32(inclusion)),
			},
		},
	}
	resp, _, err := c.doRequestResponse(req, "", true)
	if err != nil {
		return nil, err
	}
	statResp := resp.(*protobuf.StatisticsResponse)
	if statResp.GetErr() != nil {
		err = errors.New(statResp.GetErr().GetError())
		return nil, err
	}
	return statResp.GetStats(), nil
}

// Lookup scan index between low and high.
func (c *GsiScanClient) Lookup(
	defnID uint64, requestId string, values []common.SecondaryKey,
	distinct bool, limit int64,
	cons common.Consistency, tsvector *TsConsistency,
	callb ResponseHandler,
	rollbackTime int64,
	partitions []common.PartitionId,
	dataEncFmt common.DataEncodingFormat,
	retry bool, scanParams map[string]interface{}) (error, bool) {

	// serialize lookup value.
	equals := make([][]byte, 0, len(values))
	for _, value := range values {
		val, err := json.Marshal(value)
		if err != nil {
			return err, false
		}
		equals = append(equals, val)
	}

	partnIds := make([]uint64, len(partitions))
	for i, partnId := range partitions {
		partnIds[i] = uint64(partnId)
	}

	req := &protobuf.ScanRequest{
		DefnID:           proto.Uint64(defnID),
		RequestId:        proto.String(requestId),
		Span:             &protobuf.Span{Equals: equals},
		Distinct:         proto.Bool(distinct),
		Limit:            proto.Int64(limit),
		Cons:             proto.Uint32(uint32(cons)),
		RollbackTime:     proto.Int64(rollbackTime),
		PartitionIds:     partnIds,
		Sorted:           proto.Bool(true),
		DataEncFmt:       proto.Uint32(uint32(dataEncFmt)),
		SkipReadMetering: proto.Bool(scanParams["skipReadMetering"].(bool)),
		User:             proto.String(scanParams["user"].(string)),
	}
	if tsvector != nil {
		req.TsVector = protobuf.NewTsConsistency(
			tsvector.Vbnos, tsvector.Seqnos, tsvector.Vbuuids, tsvector.Crc64)
	}

	return c.doStreamingWithRetry(requestId, req, callb, "Lookup", retry)
}

func (c *GsiScanClient) doStreamingWithRetry(requestId string, req interface{}, callb ResponseHandler,
	caller string, retry bool) (error, bool /*partial*/) {

	partial, healthy, closeStream := false, true, false

	poolClosed := false

	connectn, err := c.pool.Get()
	if err != nil {
		return err, false
	}

	defer func() {
		go func() {
			if healthy && closeStream {
				conn, pkt := connectn.conn, connectn.pkt
				_, healthy = c.closeStream(conn, pkt, requestId)
			}
			c.pool.Return(connectn, healthy)
		}()
	}()

	renew := func() bool {
		var err1 error
		connectn, err1, poolClosed = c.pool.Renew(connectn)

		if connectn != nil && err1 == nil {
			logging.Verbosef("%v GsiScanClient::doStreamingWithRetry req(%v) renew connection from %v to %v", c.logPrefix, requestId, connectn.conn.LocalAddr(), connectn.conn.RemoteAddr())
		}

		return err1 == nil
	}

	var authRetryOnce bool

STREAM_RETRY:

	if poolClosed {
		return err, partial
	}

	conn, pkt := connectn.conn, connectn.pkt

	// ---> protobuf.ScanRequest
	err = c.sendRequest(conn, pkt, req)
	if isgone(err) && retry && renew() {
		retry, healthy, closeStream = false, true, false
		goto STREAM_RETRY
	}
	if err != nil {
		fmsg := "%v %s request transport failed `%v`\n"
		logging.Errorf(fmsg, c.logPrefix, requestId, err)
		healthy = false
		return err, partial
	}

	cont := true
	for cont {
		// <--- protobuf.ResponseStream
		var authRetry bool
		cont, healthy, err, closeStream, authRetry = c.streamResponse(conn, pkt, callb, requestId, authRetryOnce)
		if authRetry {
			healthy, closeStream, authRetryOnce = false, false, true
			renew()
			goto STREAM_RETRY
		}

		if isgone(err) && !partial && retry && renew() {
			retry, healthy, closeStream = false, true, false
			goto STREAM_RETRY
		}
		if err != nil { // if err, cont should have been set to false
			fmsg := "%v %v(%v) response failed `%v`\n"
			logging.Errorf(fmsg, c.logPrefix, caller, requestId, err)
		} else { // partially succeeded
			partial = true
		}
	}

	return err, partial
}

// Range scan index between low and high.
func (c *GsiScanClient) Range(
	defnID uint64, requestId string, low, high common.SecondaryKey, inclusion Inclusion,
	distinct bool, limit int64, cons common.Consistency, tsvector *TsConsistency,
	callb ResponseHandler, rollbackTime int64, partitions []common.PartitionId,
	dataEncFmt common.DataEncodingFormat, retry bool, scanParams map[string]interface{}) (error, bool) {

	// serialize low and high values.
	l, err := json.Marshal(low)
	if err != nil {
		return err, false
	}
	h, err := json.Marshal(high)
	if err != nil {
		return err, false
	}

	partnIds := make([]uint64, len(partitions))
	for i, partnId := range partitions {
		partnIds[i] = uint64(partnId)
	}

	req := &protobuf.ScanRequest{
		DefnID:    proto.Uint64(defnID),
		RequestId: proto.String(requestId),
		Span: &protobuf.Span{
			Range: &protobuf.Range{
				Low: l, High: h, Inclusion: proto.Uint32(uint32(inclusion)),
			},
		},
		Distinct:         proto.Bool(distinct),
		Limit:            proto.Int64(limit),
		Cons:             proto.Uint32(uint32(cons)),
		RollbackTime:     proto.Int64(rollbackTime),
		PartitionIds:     partnIds,
		Sorted:           proto.Bool(true),
		DataEncFmt:       proto.Uint32(uint32(dataEncFmt)),
		SkipReadMetering: proto.Bool(scanParams["skipReadMetering"].(bool)),
		User:             proto.String(scanParams["user"].(string)),
	}
	if tsvector != nil {
		req.TsVector = protobuf.NewTsConsistency(
			tsvector.Vbnos, tsvector.Seqnos, tsvector.Vbuuids, tsvector.Crc64)
	}

	return c.doStreamingWithRetry(requestId, req, callb, "Range", retry)
}

// Range scan index between low and high.
func (c *GsiScanClient) RangePrimary(
	defnID uint64, requestId string, low, high []byte, inclusion Inclusion,
	distinct bool, limit int64, cons common.Consistency, tsvector *TsConsistency,
	callb ResponseHandler, rollbackTime int64, partitions []common.PartitionId,
	dataEncFmt common.DataEncodingFormat, retry bool, scanParams map[string]interface{}) (error, bool) {

	partnIds := make([]uint64, len(partitions))
	for i, partnId := range partitions {
		partnIds[i] = uint64(partnId)
	}

	req := &protobuf.ScanRequest{
		DefnID:    proto.Uint64(defnID),
		RequestId: proto.String(requestId),
		Span: &protobuf.Span{
			Range: &protobuf.Range{
				Low: low, High: high,
				Inclusion: proto.Uint32(uint32(inclusion)),
			},
		},
		Distinct:         proto.Bool(distinct),
		Limit:            proto.Int64(limit),
		Cons:             proto.Uint32(uint32(cons)),
		RollbackTime:     proto.Int64(rollbackTime),
		PartitionIds:     partnIds,
		Sorted:           proto.Bool(true),
		DataEncFmt:       proto.Uint32(uint32(dataEncFmt)),
		SkipReadMetering: proto.Bool(scanParams["skipReadMetering"].(bool)),
		User:             proto.String(scanParams["user"].(string)),
	}
	if tsvector != nil {
		req.TsVector = protobuf.NewTsConsistency(
			tsvector.Vbnos, tsvector.Seqnos, tsvector.Vbuuids, tsvector.Crc64)
	}

	return c.doStreamingWithRetry(requestId, req, callb, "RangePrimary", retry)
}

// ScanAll for full table scan.
func (c *GsiScanClient) ScanAll(
	defnID uint64, requestId string, limit int64,
	cons common.Consistency, tsvector *TsConsistency,
	callb ResponseHandler, rollbackTime int64, partitions []common.PartitionId,
	dataEncFmt common.DataEncodingFormat, retry bool, scanParams map[string]interface{}) (error, bool) {

	partnIds := make([]uint64, len(partitions))
	for i, partnId := range partitions {
		partnIds[i] = uint64(partnId)
	}

	req := &protobuf.ScanAllRequest{
		DefnID:           proto.Uint64(defnID),
		RequestId:        proto.String(requestId),
		Limit:            proto.Int64(limit),
		Cons:             proto.Uint32(uint32(cons)),
		RollbackTime:     proto.Int64(rollbackTime),
		PartitionIds:     partnIds,
		DataEncFmt:       proto.Uint32(uint32(dataEncFmt)),
		SkipReadMetering: proto.Bool(scanParams["skipReadMetering"].(bool)),
		User:             proto.String(scanParams["user"].(string)),
	}
	if tsvector != nil {
		req.TsVector = protobuf.NewTsConsistency(
			tsvector.Vbnos, tsvector.Seqnos, tsvector.Vbuuids, tsvector.Crc64)
	}

	return c.doStreamingWithRetry(requestId, req, callb, "ScanAll", retry)
}

func (c *GsiScanClient) MultiScan(
	defnID uint64, requestId string, scans Scans,
	reverse, distinct bool, projection *IndexProjection, offset, limit int64,
	cons common.Consistency, tsvector *TsConsistency,
	callb ResponseHandler, rollbackTime int64, partitions []common.PartitionId,
	dataEncFmt common.DataEncodingFormat, retry bool, scanParams map[string]interface{}) (error, bool) {

	// serialize scans
	protoScans := make([]*protobuf.Scan, len(scans))
	for i, scan := range scans {
		if scan != nil {
			var equals [][]byte
			var filters []*protobuf.CompositeElementFilter

			// If Seek is there, then do not marshall Range
			if len(scan.Seek) > 0 {
				equals = make([][]byte, len(scan.Seek))
				for i, seek := range scan.Seek {
					s, err := json.Marshal(seek)
					if err != nil {
						return err, false
					}
					equals[i] = s
				}
			} else {
				filters = make([]*protobuf.CompositeElementFilter, len(scan.Filter))
				if scan.Filter != nil {
					for j, f := range scan.Filter {
						var l, h []byte
						var err error
						if f.Low != common.MinUnbounded { // Do not encode if unbounded
							l, err = json.Marshal(f.Low)
							if err != nil {
								return err, false
							}
						}

						if f.High != common.MaxUnbounded { // Do not encode if unbounded
							h, err = json.Marshal(f.High)
							if err != nil {
								return err, false
							}
						}

						fl := &protobuf.CompositeElementFilter{
							Low: l, High: h, Inclusion: proto.Uint32(uint32(f.Inclusion)),
						}

						filters[j] = fl
					}
				}
			}
			s := &protobuf.Scan{
				Filters: filters,
				Equals:  equals,
			}
			protoScans[i] = s
		}
	}

	//IndexProjection
	var protoProjection *protobuf.IndexProjection
	if projection != nil {
		protoProjection = &protobuf.IndexProjection{
			EntryKeys:  projection.EntryKeys,
			PrimaryKey: proto.Bool(projection.PrimaryKey),
		}
	}

	partnIds := make([]uint64, len(partitions))
	for i, partnId := range partitions {
		partnIds[i] = uint64(partnId)
	}

	req := &protobuf.ScanRequest{
		DefnID: proto.Uint64(defnID),
		Span: &protobuf.Span{
			Range: nil,
		},
		RequestId:        proto.String(requestId),
		Distinct:         proto.Bool(distinct),
		Limit:            proto.Int64(limit),
		Cons:             proto.Uint32(uint32(cons)),
		Scans:            protoScans,
		Indexprojection:  protoProjection,
		Reverse:          proto.Bool(reverse),
		Offset:           proto.Int64(offset),
		RollbackTime:     proto.Int64(rollbackTime),
		PartitionIds:     partnIds,
		Sorted:           proto.Bool(true),
		DataEncFmt:       proto.Uint32(uint32(dataEncFmt)),
		SkipReadMetering: proto.Bool(scanParams["skipReadMetering"].(bool)),
		User:             proto.String(scanParams["user"].(string)),
	}
	if tsvector != nil {
		req.TsVector = protobuf.NewTsConsistency(
			tsvector.Vbnos, tsvector.Seqnos, tsvector.Vbuuids, tsvector.Crc64)
	}

	return c.doStreamingWithRetry(requestId, req, callb, "MultiScan", retry)
}

func (c *GsiScanClient) MultiScanPrimary(
	defnID uint64, requestId string, scans Scans,
	reverse, distinct bool, projection *IndexProjection, offset, limit int64,
	cons common.Consistency, tsvector *TsConsistency,
	callb ResponseHandler, rollbackTime int64, partitions []common.PartitionId,
	dataEncFmt common.DataEncodingFormat, retry bool, scanParams map[string]interface{}) (error, bool) {

	var what string
	// serialize scans
	protoScans := make([]*protobuf.Scan, 0)
	for _, scan := range scans {
		if scan != nil {
			var equals [][]byte
			var filters []*protobuf.CompositeElementFilter

			// If Seek is there, then ignore Range
			if len(scan.Seek) > 0 {
				var k []byte
				key := scan.Seek[0]
				if k, what = curePrimaryKey(key); what == "after" {
					continue
				}
				equals = [][]byte{k}
			} else {
				filters = make([]*protobuf.CompositeElementFilter, 0)
				skip := false
				if scan.Filter != nil {
					for _, f := range scan.Filter {
						var l, h []byte
						if f.Low != common.MinUnbounded { // Ignore if unbounded
							if l, what = curePrimaryKey(f.Low); what == "after" {
								skip = true
								break
							}
						}
						if f.High != common.MaxUnbounded { // Ignore if unbounded
							if h, what = curePrimaryKey(f.High); what == "before" {
								skip = true
								break
							}
						}

						fl := &protobuf.CompositeElementFilter{
							Low: l, High: h, Inclusion: proto.Uint32(uint32(f.Inclusion)),
						}

						filters = append(filters, fl)
					}
					if skip {
						continue
					}
				}
			}
			s := &protobuf.Scan{
				Filters: filters,
				Equals:  equals,
			}
			protoScans = append(protoScans, s)
		}
	}

	if len(protoScans) == 0 {
		return nil, true
	}

	//IndexProjection
	var protoProjection *protobuf.IndexProjection
	if projection != nil {
		protoProjection = &protobuf.IndexProjection{
			EntryKeys:  projection.EntryKeys,
			PrimaryKey: proto.Bool(projection.PrimaryKey),
		}
	}

	partnIds := make([]uint64, len(partitions))
	for i, partnId := range partitions {
		partnIds[i] = uint64(partnId)
	}

	req := &protobuf.ScanRequest{
		DefnID: proto.Uint64(defnID),
		Span: &protobuf.Span{
			Range: nil,
		},
		RequestId:        proto.String(requestId),
		Distinct:         proto.Bool(distinct),
		Limit:            proto.Int64(limit),
		Cons:             proto.Uint32(uint32(cons)),
		Scans:            protoScans,
		Indexprojection:  protoProjection,
		Reverse:          proto.Bool(reverse),
		Offset:           proto.Int64(offset),
		RollbackTime:     proto.Int64(rollbackTime),
		PartitionIds:     partnIds,
		Sorted:           proto.Bool(true),
		DataEncFmt:       proto.Uint32(uint32(dataEncFmt)),
		SkipReadMetering: proto.Bool(scanParams["skipReadMetering"].(bool)),
		User:             proto.String(scanParams["user"].(string)),
	}
	if tsvector != nil {
		req.TsVector = protobuf.NewTsConsistency(
			tsvector.Vbnos, tsvector.Seqnos, tsvector.Vbuuids, tsvector.Crc64)
	}

	return c.doStreamingWithRetry(requestId, req, callb, "MultiScanPrimary", retry)
}

// CountLookup to count number entries for given set of keys.
func (c *GsiScanClient) CountLookup(
	defnID uint64, requestId string, values []common.SecondaryKey,
	cons common.Consistency, tsvector *TsConsistency, rollbackTime int64, partitions []common.PartitionId, retry bool) (int64, error) {

	// serialize match value.
	equals := make([][]byte, 0, len(values))
	for _, value := range values {
		val, err := json.Marshal(value)
		if err != nil {
			return 0, err
		}
		equals = append(equals, val)
	}

	partnIds := make([]uint64, len(partitions))
	for i, partnId := range partitions {
		partnIds[i] = uint64(partnId)
	}

	req := &protobuf.CountRequest{
		DefnID:       proto.Uint64(defnID),
		RequestId:    proto.String(requestId),
		Span:         &protobuf.Span{Equals: equals},
		Cons:         proto.Uint32(uint32(cons)),
		RollbackTime: proto.Int64(rollbackTime),
		PartitionIds: partnIds,
	}
	if tsvector != nil {
		req.TsVector = protobuf.NewTsConsistency(
			tsvector.Vbnos, tsvector.Seqnos, tsvector.Vbuuids, tsvector.Crc64)
	}
	resp, _, err := c.doRequestResponse(req, requestId, retry)
	if err != nil {
		return 0, err
	}
	countResp := resp.(*protobuf.CountResponse)
	if countResp.GetErr() != nil {
		err = errors.New(countResp.GetErr().GetError())
		return 0, err
	}
	return countResp.GetCount(), nil
}

// CountLookup to count number entries for given set of keys for primary index
func (c *GsiScanClient) CountLookupPrimary(
	defnID uint64, requestId string, values [][]byte,
	cons common.Consistency, tsvector *TsConsistency, rollbackTime int64, partitions []common.PartitionId, retry bool) (int64, error) {

	partnIds := make([]uint64, len(partitions))
	for i, partnId := range partitions {
		partnIds[i] = uint64(partnId)
	}

	req := &protobuf.CountRequest{
		DefnID:       proto.Uint64(defnID),
		RequestId:    proto.String(requestId),
		Span:         &protobuf.Span{Equals: values},
		Cons:         proto.Uint32(uint32(cons)),
		RollbackTime: proto.Int64(rollbackTime),
		PartitionIds: partnIds,
	}
	if tsvector != nil {
		req.TsVector = protobuf.NewTsConsistency(
			tsvector.Vbnos, tsvector.Seqnos, tsvector.Vbuuids, tsvector.Crc64)
	}
	resp, _, err := c.doRequestResponse(req, requestId, retry)
	if err != nil {
		return 0, err
	}
	countResp := resp.(*protobuf.CountResponse)
	if countResp.GetErr() != nil {
		err = errors.New(countResp.GetErr().GetError())
		return 0, err
	}
	return countResp.GetCount(), nil
}

// CountRange to count number entries in the given range.
func (c *GsiScanClient) CountRange(
	defnID uint64, requestId string, low, high common.SecondaryKey, inclusion Inclusion,
	cons common.Consistency, tsvector *TsConsistency, rollbackTime int64, partitions []common.PartitionId, retry bool) (int64, error) {

	// serialize low and high values.
	l, err := json.Marshal(low)
	if err != nil {
		return 0, err
	}
	h, err := json.Marshal(high)
	if err != nil {
		return 0, err
	}

	partnIds := make([]uint64, len(partitions))
	for i, partnId := range partitions {
		partnIds[i] = uint64(partnId)
	}

	req := &protobuf.CountRequest{
		DefnID:    proto.Uint64(defnID),
		RequestId: proto.String(requestId),
		Span: &protobuf.Span{
			Range: &protobuf.Range{
				Low: l, High: h, Inclusion: proto.Uint32(uint32(inclusion)),
			},
		},
		Cons:         proto.Uint32(uint32(cons)),
		RollbackTime: proto.Int64(rollbackTime),
		PartitionIds: partnIds,
	}
	if tsvector != nil {
		req.TsVector = protobuf.NewTsConsistency(
			tsvector.Vbnos, tsvector.Seqnos, tsvector.Vbuuids, tsvector.Crc64)
	}

	resp, _, err := c.doRequestResponse(req, requestId, retry)
	if err != nil {
		return 0, err
	}
	countResp := resp.(*protobuf.CountResponse)
	if countResp.GetErr() != nil {
		err = errors.New(countResp.GetErr().GetError())
		return 0, err
	}
	return countResp.GetCount(), nil
}

// CountRange to count number entries in the given range for primary index
func (c *GsiScanClient) CountRangePrimary(
	defnID uint64, requestId string, low, high []byte, inclusion Inclusion,
	cons common.Consistency, tsvector *TsConsistency, rollbackTime int64, partitions []common.PartitionId, retry bool) (int64, error) {

	partnIds := make([]uint64, len(partitions))
	for i, partnId := range partitions {
		partnIds[i] = uint64(partnId)
	}

	req := &protobuf.CountRequest{
		DefnID:    proto.Uint64(defnID),
		RequestId: proto.String(requestId),
		Span: &protobuf.Span{
			Range: &protobuf.Range{
				Low: low, High: high, Inclusion: proto.Uint32(uint32(inclusion)),
			},
		},
		Cons:         proto.Uint32(uint32(cons)),
		RollbackTime: proto.Int64(rollbackTime),
		PartitionIds: partnIds,
	}
	if tsvector != nil {
		req.TsVector = protobuf.NewTsConsistency(
			tsvector.Vbnos, tsvector.Seqnos, tsvector.Vbuuids, tsvector.Crc64)
	}

	resp, _, err := c.doRequestResponse(req, requestId, retry)
	if err != nil {
		return 0, err
	}
	countResp := resp.(*protobuf.CountResponse)
	if countResp.GetErr() != nil {
		err = errors.New(countResp.GetErr().GetError())
		return 0, err
	}
	return countResp.GetCount(), nil
}

func (c *GsiScanClient) MultiScanCount(
	defnID uint64, requestId string, scans Scans, distinct bool,
	cons common.Consistency, tsvector *TsConsistency, rollbackTime int64,
	partitions []common.PartitionId, retry bool, scanParams map[string]interface{}) (int64, uint64, error) {

	// serialize scans
	protoScans := make([]*protobuf.Scan, len(scans))
	for i, scan := range scans {
		if scan != nil {
			var equals [][]byte
			var filters []*protobuf.CompositeElementFilter

			// If Seek is there, then do not marshall Range
			if len(scan.Seek) > 0 {
				equals = make([][]byte, len(scan.Seek))
				for i, seek := range scan.Seek {
					s, err := json.Marshal(seek)
					if err != nil {
						return 0, 0, err
					}
					equals[i] = s
				}
			} else {
				filters = make([]*protobuf.CompositeElementFilter, len(scan.Filter))
				if scan.Filter != nil {
					for j, f := range scan.Filter {
						var l, h []byte
						var err error
						if f.Low != common.MinUnbounded { // Do not encode if unbounded
							l, err = json.Marshal(f.Low)
							if err != nil {
								return 0, 0, err
							}
						}
						if f.High != common.MaxUnbounded { // Do not encode if unbounded
							h, err = json.Marshal(f.High)
							if err != nil {
								return 0, 0, err
							}
						}

						fl := &protobuf.CompositeElementFilter{
							Low: l, High: h, Inclusion: proto.Uint32(uint32(f.Inclusion)),
						}

						filters[j] = fl
					}
				}
			}
			s := &protobuf.Scan{
				Filters: filters,
				Equals:  equals,
			}
			protoScans[i] = s
		}
	}

	partnIds := make([]uint64, len(partitions))
	for i, partnId := range partitions {
		partnIds[i] = uint64(partnId)
	}

	req := &protobuf.CountRequest{
		DefnID:    proto.Uint64(defnID),
		RequestId: proto.String(requestId),
		Span: &protobuf.Span{
			Range: nil,
		},
		Distinct:         proto.Bool(distinct),
		Scans:            protoScans,
		Cons:             proto.Uint32(uint32(cons)),
		RollbackTime:     proto.Int64(rollbackTime),
		PartitionIds:     partnIds,
		SkipReadMetering: proto.Bool(scanParams["skipReadMetering"].(bool)),
		User:             proto.String(scanParams["user"].(string)),
	}

	if tsvector != nil {
		req.TsVector = protobuf.NewTsConsistency(
			tsvector.Vbnos, tsvector.Seqnos, tsvector.Vbuuids, tsvector.Crc64)
	}

	resp, ru, err := c.doRequestResponse(req, requestId, retry)
	if err != nil {
		return 0, 0, err
	}
	countResp := resp.(*protobuf.CountResponse)
	if countResp.GetErr() != nil {
		err = errors.New(countResp.GetErr().GetError())
		return 0, 0, err
	}
	return countResp.GetCount(), ru, nil
}

func (c *GsiScanClient) MultiScanCountPrimary(
	defnID uint64, requestId string, scans Scans, distinct bool,
	cons common.Consistency, tsvector *TsConsistency, rollbackTime int64,
	partitions []common.PartitionId, retry bool, scanParams map[string]interface{}) (int64, uint64, error) {

	var what string
	// serialize scans
	protoScans := make([]*protobuf.Scan, 0)
	for _, scan := range scans {
		if scan != nil {
			var equals [][]byte
			var filters []*protobuf.CompositeElementFilter

			// If Seek is there, then ignore Range
			if len(scan.Seek) > 0 {
				var k []byte
				key := scan.Seek[0]
				if k, what = curePrimaryKey(key); what == "after" {
					continue
				}
				equals = [][]byte{k}
			} else {
				filters = make([]*protobuf.CompositeElementFilter, 0)
				skip := false
				if scan.Filter != nil {
					for _, f := range scan.Filter {
						var l, h []byte
						if f.Low != common.MinUnbounded { // Ignore if unbounded
							if l, what = curePrimaryKey(f.Low); what == "after" {
								skip = true
								break
							}
						}
						if f.High != common.MaxUnbounded { // Ignore if unbounded
							if h, what = curePrimaryKey(f.High); what == "before" {
								skip = true
								break
							}
						}

						fl := &protobuf.CompositeElementFilter{
							Low: l, High: h, Inclusion: proto.Uint32(uint32(f.Inclusion)),
						}

						filters = append(filters, fl)
					}

					if skip {
						continue
					}
				}
			}
			s := &protobuf.Scan{
				Filters: filters,
				Equals:  equals,
			}
			protoScans = append(protoScans, s)
		}
	}

	if len(protoScans) == 0 {
		return 0, 0, nil
	}

	partnIds := make([]uint64, len(partitions))
	for i, partnId := range partitions {
		partnIds[i] = uint64(partnId)
	}

	req := &protobuf.CountRequest{
		DefnID:    proto.Uint64(defnID),
		RequestId: proto.String(requestId),
		Span: &protobuf.Span{
			Range: nil,
		},
		Distinct:         proto.Bool(distinct),
		Scans:            protoScans,
		Cons:             proto.Uint32(uint32(cons)),
		RollbackTime:     proto.Int64(rollbackTime),
		PartitionIds:     partnIds,
		SkipReadMetering: proto.Bool(scanParams["skipReadMetering"].(bool)),
		User:             proto.String(scanParams["user"].(string)),
	}

	if tsvector != nil {
		req.TsVector = protobuf.NewTsConsistency(
			tsvector.Vbnos, tsvector.Seqnos, tsvector.Vbuuids, tsvector.Crc64)
	}

	resp, ru, err := c.doRequestResponse(req, requestId, retry)
	if err != nil {
		return 0, 0, err
	}
	countResp := resp.(*protobuf.CountResponse)
	if countResp.GetErr() != nil {
		err = errors.New(countResp.GetErr().GetError())
		return 0, 0, err
	}
	return countResp.GetCount(), ru, nil
}

func (c *GsiScanClient) Scan(
	defnID uint64, requestId string, scans Scans,
	reverse, distinct bool, projection *IndexProjection, offset, limit int64,
	groupAggr *GroupAggr, sorted bool,
	cons common.Consistency, tsvector *TsConsistency,
	callb ResponseHandler, rollbackTime int64, partitions []common.PartitionId,
	dataEncFmt common.DataEncodingFormat, retry bool, scanParams map[string]interface{},
	indexVector *IndexVector) (error, bool) {

	// serialize scans
	protoScans := make([]*protobuf.Scan, len(scans))
	for i, scan := range scans {
		if scan != nil {
			var equals [][]byte
			var filters []*protobuf.CompositeElementFilter

			// If Seek is there, then do not marshall Range
			if len(scan.Seek) > 0 {
				equals = make([][]byte, len(scan.Seek))
				for i, seek := range scan.Seek {
					s, err := json.Marshal(seek)
					if err != nil {
						return err, false
					}
					equals[i] = s
				}
			} else {
				filters = make([]*protobuf.CompositeElementFilter, len(scan.Filter))
				if scan.Filter != nil {
					for j, f := range scan.Filter {
						var l, h []byte
						var err error
						if f.Low != common.MinUnbounded { // Do not encode if unbounded
							l, err = json.Marshal(f.Low)
							if err != nil {
								return err, false
							}
						}
						if f.High != common.MaxUnbounded { // Do not encode if unbounded
							h, err = json.Marshal(f.High)
							if err != nil {
								return err, false
							}
						}

						fl := &protobuf.CompositeElementFilter{
							Low: l, High: h, Inclusion: proto.Uint32(uint32(f.Inclusion)),
						}

						filters[j] = fl
					}
				}
			}
			s := &protobuf.Scan{
				Filters: filters,
				Equals:  equals,
			}
			protoScans[i] = s
		}
	}

	//IndexProjection
	var protoProjection *protobuf.IndexProjection
	if projection != nil {
		protoProjection = &protobuf.IndexProjection{
			EntryKeys:  projection.EntryKeys,
			PrimaryKey: proto.Bool(projection.PrimaryKey),
		}
	}

	// Groups and Aggregates
	var protoGroupAggr *protobuf.GroupAggr
	if groupAggr != nil {
		// GroupKeys
		protoGroupKeys := make([]*protobuf.GroupKey, len(groupAggr.Group))
		for i, grp := range groupAggr.Group {
			gk := &protobuf.GroupKey{
				EntryKeyId: proto.Int32(grp.EntryKeyId),
				KeyPos:     proto.Int32(grp.KeyPos),
				Expr:       []byte(grp.Expr),
			}
			protoGroupKeys[i] = gk
		}
		// Aggregates
		protoAggregates := make([]*protobuf.Aggregate, len(groupAggr.Aggrs))
		for i, aggr := range groupAggr.Aggrs {
			ag := &protobuf.Aggregate{
				AggrFunc:   proto.Uint32(uint32(aggr.AggrFunc)),
				EntryKeyId: proto.Int32(aggr.EntryKeyId),
				KeyPos:     proto.Int32(aggr.KeyPos),
				Expr:       []byte(aggr.Expr),
				Distinct:   proto.Bool(aggr.Distinct),
			}
			protoAggregates[i] = ag
		}
		protoIndexKeyNames := make([][]byte, len(groupAggr.IndexKeyNames))
		for i, keyName := range groupAggr.IndexKeyNames {
			protoIndexKeyNames[i] = []byte(keyName)
		}
		protoGroupAggr = &protobuf.GroupAggr{
			Name:               []byte(groupAggr.Name),
			GroupKeys:          protoGroupKeys,
			Aggrs:              protoAggregates,
			DependsOnIndexKeys: groupAggr.DependsOnIndexKeys,
			IndexKeyNames:      protoIndexKeyNames,
			AllowPartialAggr:   proto.Bool(groupAggr.AllowPartialAggr),
			OnePerPrimaryKey:   proto.Bool(groupAggr.OnePerPrimaryKey),
		}
	}

	partnIds := make([]uint64, len(partitions))
	for i, partnId := range partitions {
		partnIds[i] = uint64(partnId)
	}

	// IndexVector
	var protoIndexVector *protobuf.IndexVector
	if indexVector != nil {
		protoIndexVector = &protobuf.IndexVector{
			QueryVector:  make([]float32, len(indexVector.QueryVector)),
			IndexKeyPos:  proto.Int32(int32(indexVector.IndexKeyPos)),
			Probes:       proto.Int32(int32(indexVector.Probes)),
			ActualVector: proto.Bool(indexVector.ActualVector),
		}
		copy(protoIndexVector.QueryVector, indexVector.QueryVector)
	}

	req := &protobuf.ScanRequest{
		DefnID: proto.Uint64(defnID),
		Span: &protobuf.Span{
			Range: nil,
		},
		RequestId:        proto.String(requestId),
		Distinct:         proto.Bool(distinct),
		Limit:            proto.Int64(limit),
		Cons:             proto.Uint32(uint32(cons)),
		Scans:            protoScans,
		Indexprojection:  protoProjection,
		Reverse:          proto.Bool(reverse),
		Offset:           proto.Int64(offset),
		RollbackTime:     proto.Int64(rollbackTime),
		PartitionIds:     partnIds,
		GroupAggr:        protoGroupAggr,
		Sorted:           proto.Bool(sorted),
		DataEncFmt:       proto.Uint32(uint32(dataEncFmt)),
		SkipReadMetering: proto.Bool(scanParams["skipReadMetering"].(bool)),
		User:             proto.String(scanParams["user"].(string)),
		IndexVector:      protoIndexVector,
	}
	if tsvector != nil {
		req.TsVector = protobuf.NewTsConsistency(
			tsvector.Vbnos, tsvector.Seqnos, tsvector.Vbuuids, tsvector.Crc64)
	}

	return c.doStreamingWithRetry(requestId, req, callb, "Scan", retry)
}

func (c *GsiScanClient) ScanPrimary(
	defnID uint64, requestId string, scans Scans,
	reverse, distinct bool, projection *IndexProjection, offset, limit int64,
	groupAggr *GroupAggr, sorted bool,
	cons common.Consistency, tsvector *TsConsistency,
	callb ResponseHandler, rollbackTime int64, partitions []common.PartitionId,
	dataEncFmt common.DataEncodingFormat, retry bool, scanParams map[string]interface{},
	indexVector *IndexVector) (error, bool) {

	var what string
	// serialize scans
	protoScans := make([]*protobuf.Scan, 0)
	for _, scan := range scans {
		if scan != nil {
			var equals [][]byte
			var filters []*protobuf.CompositeElementFilter

			// If Seek is there, then ignore Range
			if len(scan.Seek) > 0 {
				var k []byte
				key := scan.Seek[0]
				if k, what = curePrimaryKey(key); what == "after" {
					continue
				}
				equals = [][]byte{k}
			} else {
				filters = make([]*protobuf.CompositeElementFilter, 0)
				skip := false
				if scan.Filter != nil {
					for _, f := range scan.Filter {
						var l, h []byte
						if f.Low != common.MinUnbounded { // Ignore if unbounded
							if l, what = curePrimaryKey(f.Low); what == "after" {
								skip = true
								break
							}
						}
						if f.High != common.MaxUnbounded { // Ignore if unbounded
							if h, what = curePrimaryKey(f.High); what == "before" {
								skip = true
								break
							}
						}

						fl := &protobuf.CompositeElementFilter{
							Low: l, High: h, Inclusion: proto.Uint32(uint32(f.Inclusion)),
						}

						filters = append(filters, fl)
					}
					if skip {
						continue
					}
				}
			}
			s := &protobuf.Scan{
				Filters: filters,
				Equals:  equals,
			}
			protoScans = append(protoScans, s)
		}
	}

	if len(protoScans) == 0 {
		protoScans = append(protoScans, getEmptySpanForPrimary())
	}

	//IndexProjection
	var protoProjection *protobuf.IndexProjection
	if projection != nil {
		protoProjection = &protobuf.IndexProjection{
			EntryKeys:  projection.EntryKeys,
			PrimaryKey: proto.Bool(projection.PrimaryKey),
		}
	}

	// Groups and Aggregates
	var protoGroupAggr *protobuf.GroupAggr
	if groupAggr != nil {
		// GroupKeys
		protoGroupKeys := make([]*protobuf.GroupKey, len(groupAggr.Group))
		for i, grp := range groupAggr.Group {
			gk := &protobuf.GroupKey{
				EntryKeyId: proto.Int32(grp.EntryKeyId),
				KeyPos:     proto.Int32(grp.KeyPos),
				Expr:       []byte(grp.Expr),
			}
			protoGroupKeys[i] = gk
		}
		// Aggregates
		protoAggregates := make([]*protobuf.Aggregate, len(groupAggr.Aggrs))
		for i, aggr := range groupAggr.Aggrs {
			ag := &protobuf.Aggregate{
				AggrFunc:   proto.Uint32(uint32(aggr.AggrFunc)),
				EntryKeyId: proto.Int32(aggr.EntryKeyId),
				KeyPos:     proto.Int32(aggr.KeyPos),
				Expr:       []byte(aggr.Expr),
				Distinct:   proto.Bool(aggr.Distinct),
			}
			protoAggregates[i] = ag
		}
		protoIndexKeyNames := make([][]byte, len(groupAggr.IndexKeyNames))
		for i, keyName := range groupAggr.IndexKeyNames {
			protoIndexKeyNames[i] = []byte(keyName)
		}
		protoGroupAggr = &protobuf.GroupAggr{
			Name:               []byte(groupAggr.Name),
			GroupKeys:          protoGroupKeys,
			Aggrs:              protoAggregates,
			DependsOnIndexKeys: groupAggr.DependsOnIndexKeys,
			IndexKeyNames:      protoIndexKeyNames,
		}
	}

	partnIds := make([]uint64, len(partitions))
	for i, partnId := range partitions {
		partnIds[i] = uint64(partnId)
	}

	// IndexVector
	var protoIndexVector *protobuf.IndexVector
	if indexVector != nil {
		protoIndexVector = &protobuf.IndexVector{
			QueryVector:  make([]float32, len(indexVector.QueryVector)),
			IndexKeyPos:  proto.Int32(int32(indexVector.IndexKeyPos)),
			Probes:       proto.Int32(int32(indexVector.Probes)),
			ActualVector: proto.Bool(indexVector.ActualVector),
		}
		copy(protoIndexVector.QueryVector, indexVector.QueryVector)
	}

	req := &protobuf.ScanRequest{
		DefnID: proto.Uint64(defnID),
		Span: &protobuf.Span{
			Range: nil,
		},
		RequestId:        proto.String(requestId),
		Distinct:         proto.Bool(distinct),
		Limit:            proto.Int64(limit),
		Cons:             proto.Uint32(uint32(cons)),
		Scans:            protoScans,
		Indexprojection:  protoProjection,
		Reverse:          proto.Bool(reverse),
		Offset:           proto.Int64(offset),
		RollbackTime:     proto.Int64(rollbackTime),
		PartitionIds:     partnIds,
		GroupAggr:        protoGroupAggr,
		Sorted:           proto.Bool(sorted),
		DataEncFmt:       proto.Uint32(uint32(dataEncFmt)),
		SkipReadMetering: proto.Bool(scanParams["skipReadMetering"].(bool)),
		User:             proto.String(scanParams["user"].(string)),
		IndexVector:      protoIndexVector,
	}
	if tsvector != nil {
		req.TsVector = protobuf.NewTsConsistency(
			tsvector.Vbnos, tsvector.Seqnos, tsvector.Vbuuids, tsvector.Crc64)
	}

	return c.doStreamingWithRetry(requestId, req, callb, "ScanPrimary", retry)
}

func (c *GsiScanClient) Close() error {
	atomic.StoreUint32(&c.closed, 1)
	return c.pool.Close()
}

func (c *GsiScanClient) IsClosed() bool {
	return atomic.LoadUint32(&c.closed) == uint32(1)
}

func (c *GsiScanClient) doRequestResponse(req interface{}, requestId string,
	retry bool) (interface{}, uint64, error) {

	poolClosed := false

	connectn, err := c.pool.Get()
	if err != nil {
		return nil, 0, err
	}
	healthy := true
	defer func() {
		c.pool.Return(connectn, healthy)
	}()

	renew := func() bool {
		logging.Verbosef("%v GsiScanClient::doRequestResponse req(%v) renew connection %v", c.logPrefix, requestId, c.pool.host)

		var err1 error
		connectn, err1, poolClosed = c.pool.Renew(connectn)
		return err1 == nil
	}

	var authRetry bool

REQUEST_RESPONSE_RETRY:

	if poolClosed {
		return nil, 0, err
	}

	conn, pkt := connectn.conn, connectn.pkt

	// ---> protobuf.*Request
	err = c.sendRequest(conn, pkt, req)
	if isgone(err) && retry && renew() {
		retry = false
		goto REQUEST_RESPONSE_RETRY
	}
	if err != nil {
		fmsg := "%v GsiScanClient::doRequestResponse req(%v) %T request transport failed `%v`\n"
		arg1 := logging.TagUD(req)
		logging.Errorf(fmsg, c.logPrefix, requestId, arg1, err)
		healthy = false
		return nil, 0, err
	}

	laddr := conn.LocalAddr()
	c.trySetDeadline(conn, c.readDeadline)
	// <--- protobuf.*Response
	resp, err := pkt.Receive(conn)
	if resp != nil {
		if rsp, ok := resp.(*protobuf.AuthResponse); ok {

			// When the cluster upgrade completes and the queryport starts supporting
			// auth. The upgrade is handled as follows:
			// 1. Once all the nodes in the cluster get upgraded, all new connections
			//    will require auth. Existing connections will continue to exist.
			// 2. Server will reject the unauthenticated request, and will close the
			//    connection. In such cases, client needs to retry with a new connection.
			// 3. Once the TCP connection is estabished, the first packet sent will
			//    always be AuthRequest.
			// 4. The upgraded server will always respond to AuthRequest packet
			//    with AuthResponse.
			// 5. If client makes an  unauthenticated request and server returns
			//    AuthResponse with code AUTH_MISSING, then server has already observed
			//    the upgraded cluster version. From this point onwards, the client will
			//    start using auth for all new connections.

			atomic.StoreUint32(c.needsAuth, uint32(1))

			if rsp.GetCode() == transport.AUTH_MISSING && !authRetry {
				// Do not count this as a "retry"
				logging.Infof("%v GsiScanClient::doRequestResponse req(%v) server needs authentication information. Retrying "+
					"request with auth", c.logPrefix, requestId)
				renew()
				authRetry = true
				goto REQUEST_RESPONSE_RETRY
			}
		}
	}

	if isgone(err) && retry && renew() {
		retry = false
		goto REQUEST_RESPONSE_RETRY
	}
	if err != nil {
		fmsg := "%v GsiScanClient::doRequestResponse req(%v) connection %v response %T transport failed `%v`\n"
		arg1 := logging.TagUD(req)
		logging.Errorf(fmsg, c.logPrefix, requestId, laddr, arg1, err)
		healthy = false
		return nil, 0, err
	}

	c.trySetDeadline(conn, c.readDeadline)
	// <--- protobuf.StreamEndResponse (skipped) TODO: knock this off.
	var readUnits uint64
	endResp, err := pkt.Receive(conn)
	if isgone(err) && retry && renew() {
		retry = false
		goto REQUEST_RESPONSE_RETRY
	}
	if err != nil {
		fmsg := "%v GsiScanClient::doRequestResponse req(%v) connection %v response %T transport failed `%v`\n"
		arg1 := logging.TagUD(req)
		logging.Errorf(fmsg, c.logPrefix, requestId, laddr, arg1, err)
		healthy = false
		return nil, 0, err
	} else if endResp != nil {
		if ser, ok := endResp.(*protobuf.StreamEndResponse); !ok {
			healthy = false
			return nil, 0, ErrorProtocol
		} else {
			readUnits = ser.GetReadUnits()
		}
	}
	return resp, readUnits, nil
}

func (c *GsiScanClient) sendRequest(
	conn net.Conn, pkt *transport.TransportPacket, req interface{}) (err error) {

	c.trySetDeadline(conn, c.writeDeadline)
	return pkt.Send(conn, req)
}

func (c *GsiScanClient) streamResponse(
	conn net.Conn,
	pkt *transport.TransportPacket,
	callb ResponseHandler, requestId string,
	authRetryOnce bool) (cont bool, healthy bool, err error, closeStream bool, authRetry bool) {

	var resp interface{}
	var finish bool

	closeStream = false
	laddr := conn.LocalAddr()
	c.trySetDeadline(conn, c.readDeadline)
	if resp, err = pkt.Receive(conn); err != nil {
		//resp := &protobuf.ResponseStream{
		//    Err: &protobuf.Error{Error: proto.String(err.Error())},
		//}
		//callb(resp) // callback with error
		cont, healthy = false, false
		if err == io.EOF {
			fmsg := "%v GsiScanClient::streamResponse req(%v) connection %q closed `%v` \n"
			logging.Errorf(fmsg, c.logPrefix, requestId, laddr, err)
		} else {
			fmsg := "%v GsiScanClient::streamResponse req(%v) connection %q response transport failed `%v`\n"
			logging.Errorf(fmsg, c.logPrefix, requestId, laddr, err)
		}

	} else if resp == nil {
		finish = true
		logging.Tracef("%v GsiScanClient::streamResponse req(%v) connection %q received StreamEndResponse", c.logPrefix, requestId, laddr)
		callb(&protobuf.StreamEndResponse{}) // callback most likely return true
		cont, healthy = false, true

	} else if ar, ok := resp.(*protobuf.AuthResponse); ok {
		// When the cluster upgrade completes and the queryport starts supporting
		// auth. See doRequestResponse for more details.

		atomic.StoreUint32(c.needsAuth, uint32(1))

		if ar.GetCode() == transport.AUTH_MISSING {
			// Do not count this as a "retry"
			logging.Infof("%v GsiScanClient::streamResponse req(%v) server needs authentication information. Retrying "+
				"request with auth", c.logPrefix, requestId)
			// TODO: Update cluster version

			// Perform authRetry only once.
			if !authRetryOnce {
				authRetry = true
			}

			return
		}
	} else if streamResp, ok := resp.(*protobuf.ResponseStream); ok {
		if err = streamResp.Error(); err == nil {
			cont = callb(streamResp)
		}
		healthy = true
	} else if ser, ok := resp.(*protobuf.StreamEndResponse); ok {
		finish = true
		logging.Tracef("%v req(%v) connection %q received StreamEndResponse", c.logPrefix, requestId, laddr)
		callb(ser) // callback most likely return true
		cont, healthy = false, true
	} else {
		cont, healthy = false, false
		err = ErrorProtocol
	}

	if cont == false && healthy == true && finish == false {
		closeStream = true
	}
	return
}

func (c *GsiScanClient) closeStream(
	conn net.Conn, pkt *transport.TransportPacket,
	requestId string) (err error, healthy bool) {

	var resp interface{}
	laddr := conn.LocalAddr()
	healthy = true
	// request server to end the stream.
	err = c.sendRequest(conn, pkt, &protobuf.EndStreamRequest{})
	if err != nil {
		fmsg := "%v GsiScanClient::closeStream req(%v) request transport failed `%v`\n"
		logging.Errorf(fmsg, c.logPrefix, requestId, err)
		healthy = false
		return
	}

	logging.Tracef("%v GsiScanClient::closeStream req(%v) connection %q transmitted protobuf.EndStreamRequest", c.logPrefix, requestId, laddr)

	// flush the connection until stream has ended.
	for true {
		c.trySetDeadline(conn, c.readDeadline)
		resp, err = pkt.Receive(conn)
		if err != nil {
			healthy = false
			if err == io.EOF {
				fmsg := "%v GsiScanClient::closeStream req(%v) connection %q closed `%v`\n"
				logging.Errorf(fmsg, c.logPrefix, requestId, laddr, err)
				return
			}
			fmsg := "%v GsiScanClient::closeStream req(%v) connection %q response transport failed `%v`\n"
			logging.Errorf(fmsg, c.logPrefix, requestId, laddr, err)
			return

		} else if resp == nil { // End of stream marker
			return
		} else if _, ok := resp.(*protobuf.StreamEndResponse); ok { // New End of stream marker
			return
		}
	}
	return
}

func (c *GsiScanClient) trySetDeadline(conn net.Conn, deadline time.Duration) {
	if deadline > time.Duration(0) {
		timeoutMs := deadline * time.Millisecond
		conn.SetReadDeadline(time.Now().Add(timeoutMs))
	}
}

func getEmptySpanForPrimary() *protobuf.Scan {
	fl := &protobuf.CompositeElementFilter{
		Low: []byte(""), High: []byte(""), Inclusion: proto.Uint32(uint32(0)),
	}
	return &protobuf.Scan{Filters: []*protobuf.CompositeElementFilter{fl}}
}
