package projector

import (
	"fmt"

	c "github.com/couchbase/indexing/secondary/common"
	mc "github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"

	mcd "github.com/couchbase/indexing/secondary/dcp/transport"
)

// Vbucket is immutable structure defined for each vbucket.
type Vbucket struct {
	bucket      string // immutable
	keyspaceId  string // immutable
	opaque      uint16 // immutable
	vbno        uint16 // immutable
	vbuuid      uint64 // immutable
	seqno       uint64
	logPrefix   string // immutable
	opaque2     uint64 // immutable
	osoSnapshot bool   // immutable
	// stats
	sshotCount    uint64
	mutationCount uint64
	syncCount     uint64
}

// NewVbucket creates a new routine to handle this vbucket stream.
func NewVbucket(
	cluster, topic, bucket, keyspaceId string,
	opaque, vbno uint16, vbuuid, startSeqno uint64,
	config c.Config, opaque2 uint64, osoSnapshot bool) *Vbucket {

	v := &Vbucket{
		bucket:      bucket,
		keyspaceId:  keyspaceId,
		opaque:      opaque,
		vbno:        vbno,
		vbuuid:      vbuuid,
		seqno:       startSeqno,
		opaque2:     opaque2,
		osoSnapshot: osoSnapshot,
	}
	fmsg := "VBRT[<-%v<-%v<-%v #%v]"
	v.logPrefix = fmt.Sprintf(fmsg, vbno, keyspaceId, cluster, topic)
	logging.Infof("%v ##%x ##%v created\n", v.logPrefix, opaque, opaque2)
	return v
}

func (v *Vbucket) makeStreamBeginData(
	engines map[uint32]EngineMap, status byte, code byte) (data interface{}) {

	defer func() {
		if r := recover(); r != nil {
			fmsg := "%v ##%x stream-begin crashed: %v\n"
			logging.Fatalf(fmsg, v.logPrefix, v.opaque, r)
			logging.Errorf("%s", logging.StackTrace())
		} else if data == nil {
			fmsg := "%v ##%x StreamBeginData NOT PUBLISHED\n"
			logging.Errorf(fmsg, v.logPrefix, v.opaque)
		} else {
			logging.Infof("%v ##%x ##%v StreamBegin\n", v.logPrefix,
				v.opaque, v.opaque2)
		}
	}()

	if len(engines) == 0 {
		return nil
	}
	// using the first engine that is capable of it.
	for _, enginesPerColl := range engines {
		for _, engine := range enginesPerColl {
			data := engine.StreamBeginData(v.vbno, v.vbuuid,
				v.seqno, status, code, v.opaque2, v.osoSnapshot)
			if data != nil {
				return data
			}
		}
	}
	return nil
}

func (v *Vbucket) makeSyncData(engines map[uint32]EngineMap) (data interface{}) {
	defer func() {
		if r := recover(); r != nil {
			fmsg := "%v ##%x sync crashed: %v\n"
			logging.Fatalf(fmsg, v.logPrefix, v.opaque, r)
			logging.Errorf("%s", logging.StackTrace())

		} else if data == nil {
			fmsg := "%v ##%x Sync NOT PUBLISHED\n"
			logging.Errorf(fmsg, v.logPrefix, v.opaque)
		}
	}()

	if len(engines) == 0 {
		return
	}
	// using the first engine that is capable of it.
	for _, enginesPerColl := range engines {
		for _, engine := range enginesPerColl {
			data = engine.SyncData(v.vbno, v.vbuuid, v.seqno, v.opaque2)
			if data != nil {
				return data
			}
		}
	}
	return
}

var ssFormat = "%v ##%x received snapshot %v %v (type %x)\n"

func (v *Vbucket) makeSnapshotData(
	m *mc.DcpEvent, engines map[uint32]EngineMap) (data interface{}) {

	defer func() {
		if r := recover(); r != nil {
			fmsg := "%v ##%x snapshot crashed: %v\n"
			logging.Fatalf(fmsg, v.logPrefix, v.opaque, r)
			logging.Errorf("%s", logging.StackTrace())

		} else if data == nil {
			fmsg := "%v ##%x Snapshot NOT PUBLISHED\n"
			logging.Errorf(fmsg, v.logPrefix, m.Opaque)

		} else {
			typ, start, end := m.SnapshotType, m.SnapstartSeq, m.SnapendSeq
			logging.Debugf(ssFormat, v.logPrefix, m.Opaque, start, end, typ)
		}
	}()

	if len(engines) == 0 {
		return nil
	}
	// using the first engine that is capable of it.
	for _, enginesPerColl := range engines {
		for _, engine := range enginesPerColl {
			data := engine.SnapshotData(m, v.vbno, v.vbuuid,
				v.seqno, v.opaque2, v.osoSnapshot)
			if data != nil {
				return data
			}
		}
	}
	return nil
}

var seFormat = "%v ##%x received system event %v %v %v (type %v)\n"

func (v *Vbucket) makeSystemEventData(m *mc.DcpEvent, engines map[uint32]EngineMap) (data interface{}) {
	defer func() {
		if r := recover(); r != nil {
			fmsg := "%v ##%x system event crashed: %v\n"
			logging.Fatalf(fmsg, v.logPrefix, v.opaque, r)
			logging.Errorf("%s", logging.StackTrace())

		} else if data == nil {
			fmsg := "%v ##%x SystemEvent NOT PUBLISHED\n"
			logging.Errorf(fmsg, v.logPrefix, m.Opaque)

		} else {
			manifestUID, scopeID, collectionID, eventType := m.ManifestUID, m.ScopeID, m.CollectionID, m.EventType
			logging.Debugf(seFormat, v.logPrefix, m.Opaque, manifestUID, scopeID, collectionID, eventType)
		}
	}()

	if len(engines) == 0 {
		return nil
	}
	// using the first engine that is capable of it.
	for _, enginesPerColl := range engines {
		for _, engine := range enginesPerColl {
			data := engine.SystemEventData(m, v.vbno, v.vbuuid, v.seqno, v.opaque2)
			if data != nil {
				return data
			}
		}
	}
	return nil
}

var usFormat = "%v ##%x UpdateSeqno %v %v\n"

func (v *Vbucket) makeUpdateSeqnoData(m *mc.DcpEvent, engines map[uint32]EngineMap) (data interface{}) {
	defer func() {
		if r := recover(); r != nil {
			fmsg := "%v ##%x UpdateSeqno crashed: %v\n"
			logging.Fatalf(fmsg, v.logPrefix, v.opaque, r)
			logging.Errorf("%s", logging.StackTrace())

		} else if data == nil {
			fmsg := "%v ##%x UpdateSeqno NOT PUBLISHED\n"
			logging.Errorf(fmsg, v.logPrefix, m.Opaque)

		} else {
			seqno, collectionID := m.Seqno, m.CollectionID
			logging.Debugf(usFormat, v.logPrefix, m.Opaque, seqno, collectionID)
		}
	}()

	if len(engines) == 0 {
		return nil
	}
	// using the first engine that is capable of it.
	for _, enginesPerColl := range engines {
		for _, engine := range enginesPerColl {
			data := engine.UpdateSeqnoData(m, v.vbno, v.vbuuid, v.seqno, v.opaque2)
			if data != nil {
				return data
			}
		}
	}
	return nil
}

var seqnoFormat = "%v ##%x received SeqnoAdvanced %v %v %v\n"

func (v *Vbucket) makeSeqnoAdvancedEvent(m *mc.DcpEvent, engines map[uint32]EngineMap) (data interface{}) {
	defer func() {
		if r := recover(); r != nil {
			fmsg := "%v ##%x SeqnoAdvanced crashed: %v\n"
			logging.Fatalf(fmsg, v.logPrefix, v.opaque, r)
			logging.Errorf("%s", logging.StackTrace())

		} else if data == nil {
			fmsg := "%v ##%x SeqnoAdvanced NOT PUBLISHED\n"
			logging.Errorf(fmsg, v.logPrefix, m.Opaque)

		} else {
			logging.Debugf(seqnoFormat, v.logPrefix, m.Opaque, m.VBucket, m.VBuuid, m.Seqno)
		}
	}()

	if len(engines) == 0 {
		return nil
	}
	// using the first engine that is capable of it.
	for _, enginesPerColl := range engines {
		for _, engine := range enginesPerColl {
			data := engine.SeqnoAdvancedData(m, v.vbno, v.vbuuid, v.seqno, v.opaque2)
			if data != nil {
				return data
			}
		}
	}
	return nil
}

var osoFormat = "%v ##%x received OSOSnapshot %v %v %v\n"

func (v *Vbucket) makeOSOSnapshotEvent(m *mc.DcpEvent, engines map[uint32]EngineMap) (data interface{}) {
	defer func() {
		if r := recover(); r != nil {
			fmsg := "%v ##%x OSOSnapshot crashed: %v\n"
			logging.Fatalf(fmsg, v.logPrefix, v.opaque, r)
			logging.Errorf("%s", logging.StackTrace())

		} else if data == nil {
			fmsg := "%v ##%x OSOSnapshot NOT PUBLISHED\n"
			logging.Errorf(fmsg, v.logPrefix, m.Opaque)

		} else {
			logging.Debugf(osoFormat, v.logPrefix, m.Opaque, m.VBucket, m.VBuuid)
		}
	}()

	if len(engines) == 0 {
		return nil
	}
	// using the first engine that is capable of it.
	for _, enginesPerColl := range engines {
		for _, engine := range enginesPerColl {
			data := engine.OSOSnapshotData(m, v.vbno, v.vbuuid, v.opaque2)
			if data != nil {
				return data
			}
		}
	}
	return nil
}

func (v *Vbucket) makeStreamEndData(
	engines map[uint32]EngineMap) (data interface{}) {

	defer func() {
		if r := recover(); r != nil {
			fmsg := "%v stream-end crashed: %v\n"
			logging.Fatalf(fmsg, v.logPrefix, v.opaque, r)
			logging.Errorf("%s", logging.StackTrace())
		} else if data == nil {
			fmsg := "%v ##%x StreamEnd NOT PUBLISHED\n"
			logging.Errorf(fmsg, v.logPrefix, v.opaque)
		} else {
			fmsg := "%v ##%x ##%v StreamEnd\n"
			logging.Infof(fmsg, v.logPrefix, v.opaque, v.opaque2)
		}
	}()

	if len(engines) == 0 {
		return nil
	}

	// using the first engine that is capable of it.
	for _, enginesPerColl := range engines {
		for _, engine := range enginesPerColl {
			data := engine.StreamEndData(v.vbno, v.vbuuid,
				v.seqno, v.opaque2, v.osoSnapshot)
			if data != nil {
				return data
			}
		}
	}
	return nil
}

func (v *Vbucket) mcStatus2StreamStatus(s mcd.Status) c.StreamStatus {

	if s == mcd.SUCCESS {
		return c.STREAM_SUCCESS
	}

	if s == mcd.ROLLBACK {
		return c.STREAM_ROLLBACK
	}

	if s == mcd.UNKNOWN_COLLECTION {
		return c.STREAM_UNKNOWN_COLLECTION
	}

	if s == mcd.UNKNOWN_SCOPE {
		return c.STREAM_UNKNOWN_SCOPE
	}

	return c.STREAM_FAIL
}
