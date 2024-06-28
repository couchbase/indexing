// Copyright 2015-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"encoding/binary"
	"net"

	"github.com/couchbase/indexing/secondary/common"
	p "github.com/couchbase/indexing/secondary/pipeline"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
	"github.com/golang/protobuf/proto"
)

type ScanResponseWriter interface {
	Error(err error) error
	Stats(rows, unique uint64, min, max []byte) error
	Count(count uint64) error
	RawBytes([]byte) error
	Row(pk, sk []byte) error
	Done(readUnits uint64, clientVersion uint32) error
	Helo() error
}

type protoResponseWriter struct {
	scanType   ScanReqType
	conn       net.Conn
	encBuf     *[]byte
	rowBuf     *[]byte
	rowEntries []*protobuf.IndexEntry
	rowSize    int
}

func NewProtoWriter(t ScanReqType, conn net.Conn) *protoResponseWriter {
	return &protoResponseWriter{
		scanType: t,
		conn:     conn,
		encBuf:   p.GetBlock(),
		rowBuf:   p.GetBlock(),
	}
}

func (w *protoResponseWriter) writeLen(l int) error {
	binary.LittleEndian.PutUint16((*w.encBuf)[:2], uint16(l))
	_, err := w.conn.Write((*w.rowBuf)[:2])
	return err
}

func (w *protoResponseWriter) Error(err error) error {
	var res interface{}
	protoErr := &protobuf.Error{Error: proto.String(err.Error())}

	// Drop all collected rows
	w.rowEntries = nil
	w.rowSize = 0

	switch w.scanType {
	case StatsReq:
		res = &protobuf.StatisticsResponse{
			Err: protoErr,
		}
	case CountReq, MultiScanCountReq:
		res = &protobuf.CountResponse{
			Count: proto.Int64(0), Err: protoErr,
		}
	case ScanAllReq, ScanReq, FastCountReq:
		res = &protobuf.ResponseStream{
			Err: protoErr,
		}
	}

	return protobuf.EncodeAndWrite(w.conn, *w.encBuf, res)
}

func (w *protoResponseWriter) Stats(rows, unique uint64, min, max []byte) error {
	res := &protobuf.StatisticsResponse{
		Stats: &protobuf.IndexStatistics{
			KeysCount:       proto.Uint64(rows),
			UniqueKeysCount: proto.Uint64(unique),
			KeyMin:          min,
			KeyMax:          max,
		},
	}

	return protobuf.EncodeAndWrite(w.conn, *w.encBuf, res)
}

func (w *protoResponseWriter) Helo() error {
	res := &protobuf.HeloResponse{
		Version: proto.Uint32(common.INDEXER_CUR_VERSION),
	}

	return protobuf.EncodeAndWrite(w.conn, *w.encBuf, res)
}

func (w *protoResponseWriter) Count(c uint64) error {
	res := &protobuf.CountResponse{
		Count: proto.Int64(int64(c)),
	}

	return protobuf.EncodeAndWrite(w.conn, *w.encBuf, res)
}

func (w *protoResponseWriter) RawBytes(b []byte) error {
	err := w.writeLen(len(b))
	if err != nil {
		return err
	}

	_, err = w.conn.Write(b)
	return err
}

func (w *protoResponseWriter) Row(pk, sk []byte) error {

	if w.rowSize != 0 && w.rowSize+len(pk)+len(sk) > len(*w.rowBuf) {
		res := &protobuf.ResponseStream{IndexEntries: w.rowEntries}
		err := protobuf.EncodeAndWrite(w.conn, *w.encBuf, res)
		if err != nil {
			return err
		}

		w.rowSize = 0
		w.rowEntries = nil
	}

	if w.rowSize == 0 && len(pk)+len(sk) > cap(*w.rowBuf) {
		newSize := (len(pk) + len(sk))
		(*w.rowBuf) = make([]byte, newSize, newSize)
	}

	pkCopy := (*w.rowBuf)[w.rowSize : w.rowSize+len(pk)]
	w.rowSize += len(pk)
	skCopy := (*w.rowBuf)[w.rowSize : w.rowSize+len(sk)]
	w.rowSize += len(sk)

	copy(pkCopy, pk)
	copy(skCopy, sk)
	row := &protobuf.IndexEntry{
		EntryKey:   skCopy,
		PrimaryKey: pkCopy,
	}

	// TODO: remove below line
	w.rowSize += len(sk) + len(pk)
	w.rowEntries = append(w.rowEntries, row)
	return nil
}

func (w *protoResponseWriter) Done(readUnits uint64, clientVersion uint32) error {
	defer p.PutBlock(w.encBuf)
	defer p.PutBlock(w.rowBuf)

	if (w.scanType == ScanReq || w.scanType == ScanAllReq || w.scanType == FastCountReq || w.scanType == VectorScanReq) && w.rowSize > 0 {
		res := &protobuf.ResponseStream{IndexEntries: w.rowEntries}
		err := protobuf.EncodeAndWrite(w.conn, *w.encBuf, res)
		if err != nil {
			return err
		}
	}

	if clientVersion >= common.INDEXER_76_VERSION {
		res := &protobuf.StreamEndResponse{
			ReadUnits: proto.Uint64(readUnits),
		}

		return protobuf.EncodeAndWrite(w.conn, *w.encBuf, res)
	}

	return nil
}
