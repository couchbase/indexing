// Protobuf encoding scheme for payload

package indexer

import (
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
)

// protobufEncode Mutation structure into protobuf array of bytes. Returned
// `data` can be transported to the other end and decoded back to Mutation
// message.
func protobufEncode(payload interface{}) (data []byte, err error) {
	version := ProtobufVersion()
	mp := protobuf.Mutation{Version: proto.Uint32(uint32(version))}

	switch val := payload.(type) {
	case []*common.KeyVersions:
		mp.Keys = make([]*protobuf.KeyVersions, 0, len(val))
		if len(val) == 0 {
			err = fmt.Errorf("empty mutation")
			break
		}
		for _, k := range val {
			kp := &protobuf.KeyVersions{
				Command: proto.Uint32(uint32(k.Command)),
				Vbucket: proto.Uint32(uint32(k.Vbucket)),
				Vbuuid:  proto.Uint64(uint64(k.Vbuuid)),
			}
			if k.Docid != nil && len(k.Docid) > 0 {
				kp.Docid = k.Docid
			}
			if k.Seqno > 0 {
				kp.Seqno = proto.Uint64(k.Seqno)
			}
			if k.Keys != nil {
				kp.Keys = k.Keys
				kp.Oldkeys = k.Oldkeys
				kp.Indexids = k.Indexids
			}
			mp.Keys = append(mp.Keys, kp)
		}

	case common.VbConnectionMap:
		mp.Vbuckets = &protobuf.VbConnectionMap{
			Vbuuids: val.Vbuuids, Vbuckets: vbno16to32(val.Vbuckets),
		}
	}

	if err == nil {
		data, err = proto.Marshal(&mp)
	}
	return
}

// protobufDecode complements protobufEncode() API. `data` returned by encode
// is converted back to *protobuf.VbConnectionMap, or []*protobuf.KeyVersions
// and returns back the payload
func protobufDecode(data []byte) (payload interface{}, err error) {
	mp := protobuf.Mutation{}
	if err = proto.Unmarshal(data, &mp); err != nil {
		return nil, err
	}
	if ver := byte(mp.GetVersion()); ver != ProtobufVersion() {
		return nil, fmt.Errorf("mismatch in transport version %v", ver)
	}

	if vbmap := mp.GetVbuckets(); vbmap != nil {
		return vbmap, nil
	} else if keys := mp.GetKeys(); keys != nil {
		return keys, nil
	}
	return nil, fmt.Errorf("mutation does not have payload")
}

func vbno32to16(vbnos []uint32) []uint16 {
	vbnos16 := make([]uint16, 0, len(vbnos))
	for _, vb := range vbnos {
		vbnos16 = append(vbnos16, uint16(vb))
	}
	return vbnos16
}

func vbno16to32(vbnos []uint16) []uint32 {
	vbnos32 := make([]uint32, 0, len(vbnos))
	for _, vb := range vbnos {
		vbnos32 = append(vbnos32, uint32(vb))
	}
	return vbnos32
}

func protobuf2KeyVersions(keys []*protobuf.KeyVersions) []*common.KeyVersions {
	kvs := make([]*common.KeyVersions, 0, len(keys))
	for _, key := range keys {
		kv := &common.KeyVersions{
			Command:  byte(key.GetCommand()),
			Vbucket:  uint16(key.GetVbucket()),
			Seqno:    key.GetSeqno(),
			Vbuuid:   key.GetVbuuid(),
			Docid:    key.GetDocid(),
			Keys:     key.GetKeys(),
			Oldkeys:  key.GetOldkeys(),
			Indexids: key.GetIndexids(),
		}
		kvs = append(kvs, kv)
	}
	return kvs
}

// TBD: Yet to be defined. Just a place holder for now.
func ProtobufVersion() byte {
	return 1
}
