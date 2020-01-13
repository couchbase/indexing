package projector

import "fmt"

import mc "github.com/couchbase/indexing/secondary/dcp/transport/client"
import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/logging"
import mcd "github.com/couchbase/indexing/secondary/dcp/transport"

// Vbucket is immutable structure defined for each vbucket.
type Vbucket struct {
	bucket     string // immutable
	keyspaceId string // immutable
	opaque     uint16 // immutable
	vbno       uint16 // immutable
	vbuuid     uint64 // immutable
	seqno      uint64
	logPrefix  string // immutable
	opaque2    uint64 // immutable
	// stats
	sshotCount    uint64
	mutationCount uint64
	syncCount     uint64
}

// NewVbucket creates a new routine to handle this vbucket stream.
func NewVbucket(
	cluster, topic, bucket, keyspaceId string, opaque, vbno uint16,
	vbuuid, startSeqno uint64, config c.Config, opaque2 uint64) *Vbucket {

	v := &Vbucket{
		bucket:     bucket,
		keyspaceId: keyspaceId,
		opaque:     opaque,
		vbno:       vbno,
		vbuuid:     vbuuid,
		seqno:      startSeqno,
		opaque2:    opaque2,
	}
	fmsg := "VBRT[<-%v<-%v<-%v #%v]"
	v.logPrefix = fmt.Sprintf(fmsg, vbno, keyspaceId, cluster, topic)
	logging.Infof("%v ##%x ##%v created\n", v.logPrefix, opaque, opaque2)
	return v
}

func (v *Vbucket) makeStreamBeginData(
	engines map[uint64]*Engine, status byte, code byte) (data interface{}) {

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
	for _, engine := range engines {
		data := engine.StreamBeginData(v.vbno, v.vbuuid,
			v.seqno, status, code, v.opaque2)
		if data != nil {
			return data
		}
	}
	return nil
}

func (v *Vbucket) makeSyncData(engines map[uint64]*Engine) (data interface{}) {
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
	for _, engine := range engines {
		data = engine.SyncData(v.vbno, v.vbuuid, v.seqno, v.opaque2)
		if data != nil {
			return data
		}
	}
	return
}

var ssFormat = "%v ##%x received snapshot %v %v (type %x)\n"

func (v *Vbucket) makeSnapshotData(
	m *mc.DcpEvent, engines map[uint64]*Engine) (data interface{}) {

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
	for _, engine := range engines {
		data := engine.SnapshotData(m, v.vbno, v.vbuuid, v.seqno, v.opaque2)
		if data != nil {
			return data
		}
	}
	return nil
}

func (v *Vbucket) makeStreamEndData(
	engines map[uint64]*Engine) (data interface{}) {

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
	for _, engine := range engines {
		data := engine.StreamEndData(v.vbno, v.vbuuid, v.seqno, v.opaque2)
		if data != nil {
			return data
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

	return c.STREAM_FAIL
}
