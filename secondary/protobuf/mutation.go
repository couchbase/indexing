package protobuf

import (
	"encoding/binary"

	c "github.com/couchbase/indexing/secondary/common"
)

func (pl *Payload) Value() interface{} {
	if pl.Vbmap != nil {
		return pl.Vbmap
	} else if pl.Vbkeys != nil {
		return pl.Vbkeys
	}
	return nil
}

func (kv *KeyVersions) Snapshot() (typ uint32, start, end uint64) {
	uuids := kv.GetUuids()
	keys := kv.GetKeys()
	oldkeys := kv.GetOldkeys()
	for i, cmd := range kv.GetCommands() {
		if byte(cmd) == c.Snapshot {
			typ = uint32(uuids[i])
			start = binary.BigEndian.Uint64(keys[i])
			end = binary.BigEndian.Uint64(oldkeys[i])
		}
	}
	return
}
