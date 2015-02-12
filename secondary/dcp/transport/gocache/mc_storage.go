package main

import (
	"encoding/binary"

	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/dcp/transport"
)

type storage struct {
	data map[string]transport.MCItem
	cas  uint64
}

type handler func(req *transport.MCRequest, s *storage) *transport.MCResponse

var handlers = map[transport.CommandCode]handler{
	transport.SET:    handleSet,
	transport.GET:    handleGet,
	transport.DELETE: handleDelete,
	transport.FLUSH:  handleFlush,
}

// RunServer runs the cache server.
func RunServer(input chan chanReq) {
	var s storage
	s.data = make(map[string]transport.MCItem)
	for {
		req := <-input
		logging.Warnf("Got a request: %s", req.req)
		req.res <- dispatch(req.req, &s)
	}
}

func dispatch(req *transport.MCRequest, s *storage) (rv *transport.MCResponse) {
	if h, ok := handlers[req.Opcode]; ok {
		rv = h(req, s)
	} else {
		return notFound(req, s)
	}
	return
}

func notFound(req *transport.MCRequest, s *storage) *transport.MCResponse {
	var response transport.MCResponse
	response.Status = transport.UNKNOWN_COMMAND
	return &response
}

func handleSet(req *transport.MCRequest, s *storage) (ret *transport.MCResponse) {
	ret = &transport.MCResponse{}
	var item transport.MCItem

	item.Flags = binary.BigEndian.Uint32(req.Extras)
	item.Expiration = binary.BigEndian.Uint32(req.Extras[4:])
	item.Data = req.Body
	ret.Status = transport.SUCCESS
	s.cas++
	item.Cas = s.cas
	ret.Cas = s.cas

	s.data[string(req.Key)] = item
	return
}

func handleGet(req *transport.MCRequest, s *storage) (ret *transport.MCResponse) {
	ret = &transport.MCResponse{}
	if item, ok := s.data[string(req.Key)]; ok {
		ret.Status = transport.SUCCESS
		ret.Extras = make([]byte, 4)
		binary.BigEndian.PutUint32(ret.Extras, item.Flags)
		ret.Cas = item.Cas
		ret.Body = item.Data
	} else {
		ret.Status = transport.KEY_ENOENT
	}
	return
}

func handleFlush(req *transport.MCRequest, s *storage) (ret *transport.MCResponse) {
	ret = &transport.MCResponse{}
	delay := binary.BigEndian.Uint32(req.Extras)
	if delay > 0 {
		logging.Warnf("Delay not supported (got %d)", delay)
	}
	s.data = make(map[string]transport.MCItem)
	return
}

func handleDelete(req *transport.MCRequest, s *storage) (ret *transport.MCResponse) {
	ret = &transport.MCResponse{}
	delete(s.data, string(req.Key))
	return
}
