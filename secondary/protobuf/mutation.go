package protobuf

import (
	"code.google.com/p/goprotobuf/proto"
)

func (m *Mutation) Name() string {
	return "Mutation"
}

func (m *Mutation) Encode() (data []byte, err error) {
	data, err = proto.Marshal(m)
	return
}

func (m *Mutation) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, m)
}
