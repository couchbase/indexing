package indexer

import (
	"bytes"
	"errors"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/memdb"
	"sync"
)

type memDBSlice struct {
	sync.Mutex
	id        SliceId
	instId    c.IndexInstId
	indDefId  c.IndexDefnId
	status    SliceStatus
	isActive  bool
	isPrimary bool
	conf      c.Config

	main memdb.MemDB
	back memdb.MemDB

	ts *c.TsVbuuid
}

type memDBSnapInfo struct {
	db        memdb.MemDB
	ts        *c.TsVbuuid
	committed bool
}

func (i *memDBSnapInfo) Timestamp() *c.TsVbuuid {
	return i.ts
}

func (i *memDBSnapInfo) IsCommitted() bool {
	return i.committed
}

type KV struct {
	k []byte
	v []byte
}

func (kv *KV) Less(that memdb.Item) bool {
	thatKV := that.(*KV)
	return bytes.Compare(kv.k, thatKV.k) == -1
}

func NewMemDBSlice(id SliceId, defId c.IndexDefnId, instId c.IndexInstId, isPrimary bool, conf c.Config) (*memDBSlice, error) {

	slice := &memDBSlice{
		id:        id,
		instId:    instId,
		indDefId:  defId,
		isPrimary: isPrimary,
		conf:      conf,
		main:      memdb.New(),
		back:      memdb.New(),
	}

	return slice, nil
}

func (s *memDBSlice) Id() SliceId {
	return s.id
}

func (s *memDBSlice) Path() string {
	return "not-implemented"
}

func (s *memDBSlice) Status() SliceStatus {
	return SLICE_STATUS_ACTIVE
}

func (s *memDBSlice) IndexInstId() c.IndexInstId {
	return s.instId
}

func (s *memDBSlice) IndexDefnId() c.IndexDefnId {
	return s.indDefId
}

func (s *memDBSlice) IsActive() bool {
	return s.status == SLICE_STATUS_ACTIVE
}

func (s *memDBSlice) SetActive(b bool) {
	s.isActive = b

}

func (s *memDBSlice) SetStatus(ss SliceStatus) {
	s.status = ss
}

func (s *memDBSlice) IsDirty() bool {
	return true
}

func (s *memDBSlice) UpdateConfig(cfg c.Config) {
	s.conf = cfg
}

func (s *memDBSlice) Insert(k []byte, docid []byte) error {
	s.Lock()
	defer s.Unlock()

	mainItm := &KV{
		k: k,
	}

	if s.isPrimary {
		exists := s.main.Get(mainItm)
		if exists == nil {
			s.main.InsertNoReplace(mainItm)
		}

		return nil
	}

	backItm := &KV{
		k: docid,
		v: k,
	}

	oldkey := s.back.Get(backItm)
	if oldkey != nil {
		s.main.Delete(oldkey)
		s.back.Delete(backItm)
	}

	if mainItm.k == nil {
		return nil
	}

	s.back.InsertNoReplace(backItm)
	s.main.InsertNoReplace(mainItm)
	return nil
}

func (s *memDBSlice) Delete(d []byte) error {
	s.Lock()
	defer s.Unlock()
	backItm := &KV{
		k: d,
	}

	if s.isPrimary {
		s.main.Delete(backItm)
		return nil
	}

	oldkey := s.back.Get(backItm)
	if oldkey != nil {
		s.main.Delete(oldkey)
		s.back.Delete(backItm)
	}

	return nil
}

func (s *memDBSlice) NewSnapshot(ts *c.TsVbuuid, commit bool) (SnapshotInfo, error) {
	s.Lock()
	defer s.Unlock()
	si := &memDBSnapInfo{
		ts:        ts,
		committed: commit,
		db:        s.main.Clone(),
	}

	return si, nil
}

func (s *memDBSlice) OpenSnapshot(info SnapshotInfo) (Snapshot, error) {
	s.Lock()
	defer s.Unlock()
	msi := info.(*memDBSnapInfo)
	snap := &memDBSnapshot{
		db:   msi.db,
		ts:   msi.ts,
		info: info.(*memDBSnapInfo),
	}

	return snap, nil
}

func (s *memDBSlice) GetSnapshots() ([]SnapshotInfo, error) {
	s.Lock()
	defer s.Unlock()
	return []SnapshotInfo{}, nil
}

func (s *memDBSlice) Rollback(info SnapshotInfo) error {
	return errors.New("not-implemented")
}

func (s *memDBSlice) RollbackToZero() error {
	s.main = memdb.New()
	s.back = memdb.New()
	return nil
}

func (s *memDBSlice) Close() {
	s.Lock()
	defer s.Unlock()
	s.main = nil
	s.back = nil
}

func (s *memDBSlice) Destroy() {
	s.Lock()
	defer s.Unlock()
	s.main = nil
	s.back = nil
}

func (s *memDBSlice) SetTimestamp(ts *c.TsVbuuid) error {
	s.ts = ts
	return nil
}

func (s *memDBSlice) Timestamp() *c.TsVbuuid {
	return s.ts
}

func (s *memDBSlice) IncrRef() {
}

func (s *memDBSlice) DecrRef() {
}

func (s *memDBSlice) Compact() error {
	return nil
}

func (s *memDBSlice) Statistics() (StorageStatistics, error) {
	return StorageStatistics{}, nil
}

type memDBSnapshot struct {
	db   memdb.MemDB
	ts   *c.TsVbuuid
	info *memDBSnapInfo
}

func (s *memDBSnapshot) CountTotal(stopch StopChannel) (uint64, error) {
	return uint64(s.db.Len()), nil
}

func (s *memDBSnapshot) StatCountTotal() (uint64, error) {
	return uint64(s.db.Len()), nil
}

func (s *memDBSnapshot) Exists(key IndexKey, stopch StopChannel) (bool, error) {
	itm := &KV{
		k: key.Bytes(),
	}
	return s.db.Get(itm) != nil, nil
}

func (s *memDBSnapshot) Lookup(key IndexKey, callb EntryCallback) error {
	return nil
}

func (s *memDBSnapshot) All(callb EntryCallback) error {
	cb := func(i memdb.Item) bool {
		kv := i.(*KV)
		if callb(kv.k) != nil {
			return false
		}

		return true
	}

	nilK := &KV{
		k: []byte(nil),
	}

	s.db.AscendGreaterOrEqual(nilK, cb)

	return nil
}

func (s *memDBSnapshot) Range(low, high IndexKey, inclusion Inclusion,
	callb EntryCallback) error {

	stK := &KV{
		k: low.Bytes(),
	}

	endK := &KV{
		k: high.Bytes(),
	}
	cb := func(i memdb.Item) bool {
		kv := i.(*KV)
		if len(high.Bytes()) > 0 && !kv.Less(endK) {
			return false
		}

		if callb(kv.k) != nil {
			return false
		}

		return true
	}

	s.db.AscendGreaterOrEqual(stK, cb)

	return nil
}

func (s *memDBSnapshot) CountRange(low IndexKey, high IndexKey, inclusion Inclusion,
	stopch StopChannel) (uint64, error) {
	count := uint64(0)
	return count, nil
}

func (s *memDBSnapshot) CountLookup(keys []IndexKey, stopch StopChannel) (uint64, error) {
	count := uint64(0)
	return count, nil
}

func (s *memDBSnapshot) Open() error {
	return nil
}

func (s *memDBSnapshot) Close() error {
	return nil
}

func (s *memDBSnapshot) IsOpen() bool {
	return true
}

func (s *memDBSnapshot) Id() SliceId {
	return SliceId(0)
}

func (s *memDBSnapshot) IndexInstId() c.IndexInstId {
	return c.IndexInstId(0)
}

func (s *memDBSnapshot) IndexDefnId() c.IndexDefnId {
	return c.IndexDefnId(0)
}

func (s *memDBSnapshot) Timestamp() *c.TsVbuuid {
	return s.ts
}

func (s *memDBSnapshot) SetTimestamp(ts *c.TsVbuuid) {
	s.ts = ts
}

func (s *memDBSnapshot) Info() SnapshotInfo {
	return s.info
}
