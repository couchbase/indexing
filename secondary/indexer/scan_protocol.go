// Copyright (c) 2015 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package indexer

import (
	"code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	p "github.com/couchbase/indexing/secondary/pipeline"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
	"io"
)

const rowBufSize = 8 * 1024

type ScanResponseWriter interface {
	Error(err error) error
	Stats(rows, unique uint64, min, max []byte) error
	Count(count uint64) error
	RawBytes([]byte) error
	Row(pk, sk []byte) error
	Done() error
}

type protoResponseWriter struct {
	scanType   ScanReqType
	wr         io.Writer
	buf        *[]byte
	rowEntries []*protobuf.IndexEntry
	rowSize    int
}

func newProtoWriter(t ScanReqType, w io.Writer) *protoResponseWriter {
	return &protoResponseWriter{
		scanType: t,
		wr:       w,
		buf:      p.GetBlock(),
	}
}

func (w *protoResponseWriter) writeLen(l int) error {
	binary.LittleEndian.PutUint16((*w.buf)[:2], uint16(l))
	_, err := w.wr.Write((*w.buf)[:2])
	return err
}

func (w *protoResponseWriter) Error(err error) error {
	var res interface{}
	protoErr := &protobuf.Error{Error: proto.String(err.Error())}

	switch w.scanType {
	case StatsReq:
		res = &protobuf.StatisticsResponse{
			Err: protoErr,
		}
	case CountReq:
		res = &protobuf.CountResponse{
			Count: proto.Int64(0), Err: protoErr,
		}
	case ScanAllReq, ScanReq:
		res = &protobuf.ResponseStream{
			Err: protoErr,
		}
	}

	return protobuf.EncodeAndWrite(w.wr, *w.buf, res)
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

	return protobuf.EncodeAndWrite(w.wr, *w.buf, res)
}

func (w *protoResponseWriter) Count(c uint64) error {
	res := &protobuf.CountResponse{
		Count: proto.Int64(int64(c)),
	}

	return protobuf.EncodeAndWrite(w.wr, *w.buf, res)
}

func (w *protoResponseWriter) RawBytes(b []byte) error {
	err := w.writeLen(len(b))
	if err != nil {
		return err
	}

	_, err = w.wr.Write(b)
	return err
}

func (w *protoResponseWriter) Row(pk, sk []byte) error {
	row := &protobuf.IndexEntry{
		EntryKey:   append([]byte(nil), sk...),
		PrimaryKey: append([]byte(nil), pk...),
	}

	w.rowSize += len(sk) + len(pk)
	w.rowEntries = append(w.rowEntries, row)
	if w.rowSize >= rowBufSize {
		res := &protobuf.ResponseStream{IndexEntries: w.rowEntries}
		err := protobuf.EncodeAndWrite(w.wr, *w.buf, res)
		if err != nil {
			return err
		}

		w.rowSize = 0
		w.rowEntries = nil
	}
	return nil
}

func (w *protoResponseWriter) Done() error {
	defer p.PutBlock(w.buf)

	if (w.scanType == ScanReq || w.scanType == ScanAllReq) && w.rowSize > 0 {
		res := &protobuf.ResponseStream{IndexEntries: w.rowEntries}
		err := protobuf.EncodeAndWrite(w.wr, *w.buf, res)
		if err != nil {
			return err
		}
	}

	return nil
}
