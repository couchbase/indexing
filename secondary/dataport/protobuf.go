// Protobuf encoding scheme for payload

package dataport

import (
	"errors"

	c "github.com/couchbase/indexing/secondary/common"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/data"
	"github.com/golang/protobuf/proto"
)

// ErrorTransportVersion
var ErrorTransportVersion = errors.New("dataport.transportVersion")

// ErrorMissingPayload
var ErrorMissingPayload = errors.New("dataport.missingPlayload")

// protobufEncode encode payload message into protobuf array of bytes. Return
// `data` can be transported to the other end and decoded back to Payload
// message.
func protobufEncode(payload interface{}) (data []byte, err error) {
	pl := protobuf.Payload{
		Version: proto.Uint32(uint32(ProtobufVersion())),
	}

	switch val := payload.(type) {
	case []*c.VbKeyVersions:
		pl.Vbkeys = make([]*protobuf.VbKeyVersions, 0, len(val))
		for _, vb := range val { // for each VbKeyVersions
			pvb := &protobuf.VbKeyVersions{
				KeyspaceId: proto.String(vb.KeyspaceId),
				Vbucket:    proto.Uint32(uint32(vb.Vbucket)),
				Vbuuid:     proto.Uint64(vb.Vbuuid),
				ProjVer:    protobuf.ProjectorVersion(int32(vb.ProjVer)).Enum(),
				Opaque2:    proto.Uint64(vb.Opaque2),
			}
			pvb.Kvs = make([]*protobuf.KeyVersions, 0, len(vb.Kvs))
			for _, kv := range vb.Kvs { // for each mutation
				pkv := &protobuf.KeyVersions{
					Seqno: proto.Uint64(kv.Seqno),
				}
				if kv.Docid != nil && len(kv.Docid) > 0 {
					pkv.Docid = kv.Docid
				}
				if kv.Ctime > 0 {
					// Send moving average value of mutation processing latency in projector
					pkv.PrjMovingAvg = proto.Int64(kv.Ctime)
				}
				if len(kv.Uuids) == 0 {
					continue
				}
				l := len(kv.Uuids)
				pkv.Uuids = make([]uint64, 0, l)
				pkv.Commands = make([]uint32, 0, l)
				pkv.Keys = make([][]byte, 0, l)
				pkv.Oldkeys = make([][]byte, 0, l)
				pkv.Partnkeys = make([][]byte, 0, l)
				pkv.IncludeColumns = make([][]byte, 0, l)
				pkv.Vectors = make([]*protobuf.Vectors, 0, l)
				pkv.CentroidPos = make([]*protobuf.FieldPos, 0, l)
				for i, uuid := range kv.Uuids { // for each key-version
					pkv.Uuids = append(pkv.Uuids, uuid)
					pkv.Commands = append(pkv.Commands, uint32(kv.Commands[i]))
					pkv.Keys = append(pkv.Keys, kv.Keys[i])
					pkv.Oldkeys = append(pkv.Oldkeys, kv.Oldkeys[i])
					pkv.Partnkeys = append(pkv.Partnkeys, kv.Partnkeys[i])
					pkv.IncludeColumns = append(pkv.IncludeColumns, kv.IncludeColumn[i])
					if len(kv.Vectors[i]) > 0 {
						vectors := &protobuf.Vectors{}

						vectors.Vectors = make([]*protobuf.Vector, len(kv.Vectors[i]))
						for j := range kv.Vectors[i] {
							vectors.Vectors[j] = &protobuf.Vector{Vector: kv.Vectors[i][j]}
						}

						pkv.Vectors = append(pkv.Vectors, vectors)
					} else {
						pkv.Vectors = append(pkv.Vectors, nil)
					}

					if len(kv.CentroidPos[i]) > 0 {
						centroidPos := &protobuf.FieldPos{}
						centroidPos.FieldPos = kv.CentroidPos[i]
						pkv.CentroidPos = append(pkv.CentroidPos, centroidPos)
					} else {
						pkv.CentroidPos = append(pkv.CentroidPos, nil)
					}
				}
				pvb.Kvs = append(pvb.Kvs, pkv)
			}
			pl.Vbkeys = append(pl.Vbkeys, pvb)
		}

	case *c.VbConnectionMap:
		pl.Vbmap = &protobuf.VbConnectionMap{
			KeyspaceId: proto.String(val.KeyspaceId),
			Vbuuids:    val.Vbuuids,
			Vbuckets:   c.Vbno16to32(val.Vbuckets),
		}

	case *protobuf.AuthRequest:
		pl.AuthRequest = &protobuf.AuthRequest{
			User: proto.String(*val.User),
			Pass: proto.String(*val.Pass),
		}
	}

	if err == nil {
		data, err = proto.Marshal(&pl)
	}
	return
}

// protobufDecode complements protobufEncode() API. `data` returned by encode
// is converted back to *protobuf.VbConnectionMap, or []*protobuf.VbKeyVersions
// and returns back the value inside the payload
func protobufDecode(data []byte) (value interface{}, err error) {
	pl := &protobuf.Payload{}
	if err = proto.Unmarshal(data, pl); err != nil {
		return nil, err
	}
	currVer := ProtobufVersion()
	if ver := byte(pl.GetVersion()); ver == currVer {
		// do nothing
	} else if ver > currVer {
		return nil, ErrorTransportVersion
	} else {
		pl = protoMsgConvertor[ver](pl)
	}

	if value = pl.Value(); value == nil {
		return nil, ErrorMissingPayload
	}
	return value, nil
}

func protobuf2Vbmap(vbmap *protobuf.VbConnectionMap) *c.VbConnectionMap {
	return &c.VbConnectionMap{
		KeyspaceId: vbmap.GetKeyspaceId(),
		Vbuckets:   c.Vbno32to16(vbmap.GetVbuckets()),
		Vbuuids:    vbmap.GetVbuuids(),
	}
}

func protobuf2KeyVersions(keys []*protobuf.KeyVersions) []*c.KeyVersions {
	kvs := make([]*c.KeyVersions, 0, len(keys))
	size := 4 // To avoid reallocs
	for _, key := range keys {
		kv := &c.KeyVersions{
			Seqno:     key.GetSeqno(),
			Docid:     key.GetDocid(),
			Uuids:     make([]uint64, 0, size),
			Commands:  make([]byte, 0, size),
			Keys:      make([][]byte, 0, size),
			Oldkeys:   make([][]byte, 0, size),
			Partnkeys: make([][]byte, 0, size),
		}
		commands := key.GetCommands()
		newkeys := key.GetKeys()
		oldkeys := key.GetOldkeys()
		partnkeys := key.GetPartnkeys()
		for i, uuid := range key.GetUuids() {
			kv.Uuids = append(kv.Uuids, uuid)
			kv.Commands = append(kv.Commands, byte(commands[i]))
			kv.Keys = append(kv.Keys, newkeys[i])
			kv.Oldkeys = append(kv.Oldkeys, oldkeys[i])

			// For backward compatibility, projector may not send partnkeys pre-5.1.
			if len(partnkeys) != 0 {
				kv.Partnkeys = append(kv.Partnkeys, partnkeys[i])
			}
		}
		kvs = append(kvs, kv)
	}
	return kvs
}

func protobuf2VbKeyVersions(protovbs []*protobuf.VbKeyVersions) []*c.VbKeyVersions {
	vbs := make([]*c.VbKeyVersions, 0, len(protovbs))
	for _, protovb := range protovbs {
		vb := &c.VbKeyVersions{
			KeyspaceId: protovb.GetKeyspaceId(),
			Vbucket:    uint16(protovb.GetVbucket()),
			Vbuuid:     protovb.GetVbuuid(),
			Kvs:        protobuf2KeyVersions(protovb.GetKvs()),
		}
		vbs = append(vbs, vb)
	}
	return vbs
}

// ProtobufVersion return version of protobuf schema used in packet transport.
func ProtobufVersion() byte {
	return (c.ProtobufDataPathMajorNum << 4) | c.ProtobufDataPathMinorNum
}

var protoMsgConvertor = map[byte]func(*protobuf.Payload) *protobuf.Payload{}
