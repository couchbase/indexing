//go:build ignore
// +build ignore

package indexer

import (
	"sync"

	c "github.com/couchbase/indexing/secondary/common"
)

// Implements mock indexer data structures required to test scan coordinator
type scannerTestHarness struct {
	scanner    *scanCoordinator
	indexCount int
	scanTS     *c.TsVbuuid

	cmdch, msgch MsgChannel
}

// A feeder function that provides scan results through mock interface
// When a range query is requested on a slice snapshot, this function will
// be executed to provide range query results from the mock indexer.
// This callback function can be used to simulate indexer behaviors such
// as index errors during snapshot read, normal scan, empty index, etc.
type snapshotFeeder func(keych chan Key, valch chan Value, errch chan error)

// Create a mock index that uses a feeder function to provide query results.
// Creates an index with single partition, single slice with a snapshot.
func (s *scannerTestHarness) createIndex(name, bucket string, feeder snapshotFeeder) c.IndexDefnId {
	s.indexCount++

	pc := c.NewKeyPartitionContainer()
	pId := c.PartitionId(0)
	endpt := c.Endpoint("localhost:1000")
	pDef := c.KeyPartitionDefn{Id: pId, Endpts: []c.Endpoint{endpt}}
	pc.AddPartition(pId, pDef)

	instId := c.IndexInstId(s.indexCount)
	defnId := c.IndexDefnId(0xABBA)
	indDefn := c.IndexDefn{Name: name, Bucket: bucket, DefnId: defnId}
	indInst := c.IndexInst{InstId: instId, State: c.INDEX_STATE_ACTIVE,
		Defn: indDefn, Pc: pc,
	}
	// TODO: Use cmdch to update map
	s.scanner.indexInstMap[instId] = indInst

	sc := NewHashedSliceContainer()
	partInst := PartitionInst{Defn: pDef, Sc: sc}
	partInstMap := PartitionInstMap{pId: partInst}

	snap := &mockSnapshot{feeder: feeder}
	snap.SetTimestamp(s.scanTS)

	slice := &mockSlice{}
	slId := SliceId(0)
	sc.AddSlice(slId, slice)
	// TODO: Use cmdch to update map
	s.scanner.indexPartnMap[instId] = partInstMap
	return defnId
}

func newScannerTestHarness() (*scannerTestHarness, error) {
	// TODO Set NUM_VBUCKETS = 8 in config
	h := new(scannerTestHarness)
	h.cmdch = make(chan Message)
	h.msgch = make(chan Message)
	si, errMsg := NewScanCoordinator(h.cmdch, h.msgch, c.SystemConfig.SectionConfig("indexer.", true))
	h.scanner = si.(*scanCoordinator)
	if errMsg.GetMsgType() != MSG_SUCCESS {
		return nil, (errMsg.(*MsgError)).GetError().cause
	}

	h.scanner.indexInstMap = make(c.IndexInstMap)
	h.scanner.indexPartnMap = make(IndexPartnMap)

	// FIXME:
	// This is hack to comply with existing timestamp datastructure
	// We need to come up with right timestamp datastructrue to be
	// used to index queries.
	h.scanTS = c.NewTsVbuuid("default", 8)
	h.scanTS.Snapshots = [][2]uint64{
		[2]uint64{0, 1},
		[2]uint64{0, 2},
		[2]uint64{0, 3},
		[2]uint64{0, 4},
		[2]uint64{0, 5},
		[2]uint64{0, 6},
		[2]uint64{0, 7},
		[2]uint64{0, 8},
	}

	go h.handleScanTimestamps()
	return h, nil
}

func (s *scannerTestHarness) handleScanTimestamps() {
loop:
	for {
		select {
		case msg, ok := <-s.msgch:
			if !ok {
				break loop
			}

			if msg.GetMsgType() == STORAGE_INDEX_SNAP_REQUEST {
				req := msg.(*MsgIndexSnapRequest)
				ch := req.GetReplyChannel()
				// TODO: Fix tests
				ch <- nil
			}
		}
	}
}

// Cleanup test harness resources
func (s *scannerTestHarness) Shutdown() {
	s.cmdch <- &MsgGeneral{mType: SCAN_COORD_SHUTDOWN}
	<-s.cmdch

	close(s.cmdch)
	close(s.msgch)
}

type mockSlice struct {
	id       SliceId
	instId   c.IndexInstId
	indDefId c.IndexDefnId
	snap     Snapshot
	err      error
	ts       *c.TsVbuuid
}

func (s *mockSlice) Id() SliceId {
	return s.id
}

func (s *mockSlice) Path() string {
	return "/tmp/mockslice/"
}

func (s *mockSlice) Status() SliceStatus {
	return SLICE_STATUS_ACTIVE
}

func (s *mockSlice) IndexInstId() c.IndexInstId {
	return s.instId
}

func (s *mockSlice) IndexDefnId() c.IndexDefnId {
	return s.indDefId
}

func (s *mockSlice) IsActive() bool {
	return true
}

func (s *mockSlice) SetActive(b bool) {
}

func (s *mockSlice) SetStatus(ss SliceStatus) {
}

func (s *mockSlice) Insert(k []byte, docid []byte) error {
	return s.err
}

func (s *mockSlice) Delete(d []byte) error {
	return s.err
}

func (s *mockSlice) NewSnapshot(ts *c.TsVbuuid, commit bool) (SnapshotInfo, error) {
	return &mockSnapshotInfo{}, s.err
}

func (s *mockSlice) OpenSnapshot(info SnapshotInfo, logOncePerBucket *sync.Once) (Snapshot, error) {
	return s.snap, s.err
}

func (s *mockSlice) GetSnapshots() ([]SnapshotInfo, error) {
	return nil, s.err
}

func (s *mockSlice) Rollback(info SnapshotInfo) error {
	return s.err
}

func (s *mockSlice) RollbackToZero() error {
	return s.err
}

func (s *mockSlice) Close() {
}

func (s *mockSlice) Destroy() {
}

func (s *mockSlice) SetTimestamp(ts *c.TsVbuuid) error {
	s.ts = ts
	return nil
}

func (s *mockSlice) Timestamp() *c.TsVbuuid {
	return s.ts
}

func (s *mockSlice) IncrRef() {
}

func (s *mockSlice) DecrRef() {
}

func (s *mockSlice) Compact() error {
	return nil
}

func (s *mockSlice) Statistics() (StorageStatistics, error) {
	return StorageStatistics{}, nil
}

type mockSnapshot struct {
	id        SliceId
	indInstid c.IndexInstId
	indDefId  c.IndexDefnId
	ts        *c.TsVbuuid

	count  uint64
	exists bool
	err    error
	valch  chan Value
	keych  chan Key
	errch  chan error
	order  SortOrder

	feeder snapshotFeeder
}

func (s *mockSnapshot) CountTotal(stopch StopChannel) (uint64, error) {
	return s.count, s.err
}

func (s *mockSnapshot) Exists(key Key, stopch StopChannel) (bool, error) {
	return s.exists, s.err
}

func (s *mockSnapshot) Lookup(key Key, stopch StopChannel) (chan Value, chan error) {
	return s.valch, s.errch
}

func (s *mockSnapshot) KeySet(stopch StopChannel) (chan Key, chan error) {
	s.keych = make(chan Key)
	s.errch = make(chan error)
	go s.feeder(s.keych, nil, s.errch)
	return s.keych, s.errch
}

func (s *mockSnapshot) ValueSet(stopch StopChannel) (chan Value, chan error) {
	return s.valch, s.errch
}

func (s *mockSnapshot) KeyRange(low, high Key, inclusion Inclusion,
	stopch StopChannel) (chan Key, chan error, SortOrder) {

	s.keych = make(chan Key)
	s.errch = make(chan error)
	go s.feeder(s.keych, nil, s.errch)
	return s.keych, s.errch, s.order
}

func (s *mockSnapshot) ValueRange(low, high Key, inclusion Inclusion,
	stopch StopChannel) (chan Value, chan error, SortOrder) {
	s.valch = make(chan Value)
	s.errch = make(chan error)
	go s.feeder(nil, s.valch, s.errch)
	return s.valch, s.errch, s.order
}

func (s *mockSnapshot) GetKeySetForKeyRange(low Key, high Key,
	inclusion Inclusion, chkey chan Key, cherr chan error, stopch StopChannel) {
	panic("not implemented")
}

func (s *mockSnapshot) GetValueSetForKeyRange(low Key, high Key,
	inclusion Inclusion, chval chan Value, cherr chan error, stopch StopChannel) {
	panic("not implemented")
}

func (s *mockSnapshot) CountRange(low Key, high Key, inclusion Inclusion,
	stopch StopChannel) (uint64, error) {
	s.keych = make(chan Key)
	s.errch = make(chan error)
	s.count = 0
	go s.feeder(s.keych, nil, s.errch)

loop:
	for {
		select {
		case s.err, _ = <-s.errch:
			break loop
		case _, ok := <-s.keych:
			if !ok {
				break loop
			}
			s.count++
		}
	}

	return s.count, s.err
}

func (s *mockSnapshot) Open() error {
	return nil
}

func (s *mockSnapshot) Close() error {
	return nil
}

func (s *mockSnapshot) IsOpen() bool {
	return true
}

func (s *mockSnapshot) Id() SliceId {
	return s.id
}

func (s *mockSnapshot) IndexInstId() c.IndexInstId {
	return s.indInstid
}

func (s *mockSnapshot) IndexDefnId() c.IndexDefnId {
	return s.indDefId
}

func (s *mockSnapshot) Timestamp() *c.TsVbuuid {
	return s.ts
}

func (s *mockSnapshot) SetTimestamp(ts *c.TsVbuuid) {
	s.ts = ts
}

func (s *mockSnapshot) Info() SnapshotInfo {
	return &mockSnapshotInfo{}
}

type mockSnapshotInfo struct {
}

func (info *mockSnapshotInfo) Timestamp() *c.TsVbuuid {
	return nil
}

func (info *mockSnapshotInfo) IsCommitted() bool {
	return true
}
