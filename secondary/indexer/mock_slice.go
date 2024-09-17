package indexer

import (
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

type snapshotFeeder func(datach chan Row, errch chan error)

// -----
// Slice
// -----

type mockSlice struct {
	ID          SliceId
	PathStr     string
	SliceStatus SliceStatus

	IdxDefn c.IndexDefn

	InstID   c.IndexInstId
	PartnID  c.PartitionId
	DefnID   c.IndexDefnId
	ShardIDs []c.ShardId

	IsActiveFlag        bool
	IsDirtyFlag         bool
	CleanupDoneFlag     bool
	RecoveryDoneFlag    bool
	BuildDoneFlag       bool
	RebalRunningFlag    bool
	PersistorActiveFlag bool
	FlushDoneFlag       bool

	WriteUnits    uint64
	StopWUBilling bool

	cfg c.Config

	snap    Snapshot
	testErr error
	ts      *c.TsVbuuid
}

func (ms *mockSlice) Id() SliceId {
	return ms.ID
}

func (ms *mockSlice) Path() string {
	ms.PathStr = "/tmp/mockslice/"
	return ms.PathStr
}

func (ms *mockSlice) Status() SliceStatus {
	ms.SliceStatus = SLICE_STATUS_ACTIVE
	return ms.SliceStatus
}

func (ms *mockSlice) IndexInstId() c.IndexInstId {
	return ms.InstID
}
func (ms *mockSlice) IndexPartnId() c.PartitionId {
	return ms.PartnID
}

func (ms *mockSlice) IndexDefnId() c.IndexDefnId {
	return ms.DefnID
}

func (ms *mockSlice) IsActive() bool {
	ms.IsActiveFlag = true
	return ms.IsActiveFlag
}

func (ms *mockSlice) IsDirty() bool {
	return ms.IsDirtyFlag
}

func (ms *mockSlice) IsCleanupDone() bool {
	return ms.CleanupDoneFlag
}

func (ms *mockSlice) SetActive(active bool) {
	ms.IsActiveFlag = active
}

func (ms *mockSlice) SetStatus(ss SliceStatus) {
	ms.SliceStatus = ss
}

func (ms *mockSlice) UpdateConfig(cfg c.Config) {
	ms.cfg = cfg
}

func (ms *mockSlice) RecoveryDone() {
	ms.RecoveryDoneFlag = true
}

func (ms *mockSlice) BuildDone() {
	ms.BuildDoneFlag = true
}

func (ms *mockSlice) ClearRebalRunning() {
	ms.RebalRunningFlag = false
}

func (ms *mockSlice) SetRebalRunning() {
	ms.RebalRunningFlag = true
}

func (ms *mockSlice) IsPersistanceActive() bool {
	return ms.PersistorActiveFlag
}

func (ms *mockSlice) GetWriteUnits() uint64 {
	return ms.WriteUnits
}

func (ms *mockSlice) SetStopWriteUnitBilling(disableBilling bool) {
	ms.StopWUBilling = disableBilling
}

func (ms *mockSlice) Insert(key []byte, docid []byte, meta *MutationMeta) error {
	if ms.testErr != nil {
		return ms.testErr
	}
	return nil
}

func (ms *mockSlice) Delete(docid []byte, meta *MutationMeta) error {
	if ms.testErr != nil {
		return ms.testErr
	}
	return nil
}

func (ms *mockSlice) GetShardIds() []c.ShardId {
	return ms.ShardIDs
}

func (ms *mockSlice) GetAlternateShardId(c.PartitionId) string {
	return ""
}

func (ms *mockSlice) FlushDone() {
	ms.FlushDoneFlag = true
}

func (ms *mockSlice) Rollback(s SnapshotInfo) error {
	if ms.testErr != nil {
		return ms.testErr
	}

	return nil
}

func (ms *mockSlice) RollbackToZero(bool) error {
	if ms.testErr != nil {
		return ms.testErr
	}

	return nil
}

func (ms *mockSlice) LastRollbackTs() *c.TsVbuuid {
	return nil
}

func (ms *mockSlice) SetLastRollbackTs(ts *c.TsVbuuid) {
}

func (ms *mockSlice) Statistics(consumerFilter uint64) (StorageStatistics, error) {
	return StorageStatistics{}, nil
}

func (ms *mockSlice) PrepareStats() {
}

func (ms *mockSlice) ShardStatistics(c.PartitionId) *c.ShardStats {
	return nil
}

func (ms *mockSlice) GetTenantDiskSize() (int64, error) {
	return 0, nil
}

func (ms *mockSlice) IncrRef() {
}

func (ms *mockSlice) DecrRef() {
}

func (ms *mockSlice) CheckAndIncrRef() bool {
	return true
}

func (ms *mockSlice) Compact(abortTime time.Time, minFrag int) error {
	return nil
}

func (ms *mockSlice) Close() {
}

func (ms *mockSlice) Destroy() {
}

func (ms *mockSlice) GetReaderContext(user string, skipReadMetering bool) IndexReaderContext {
	return nil
}

func (ms *mockSlice) NewSnapshot(*c.TsVbuuid, bool) (SnapshotInfo, error) {
	return nil, nil
}

func (ms *mockSlice) OpenSnapshot(SnapshotInfo) (Snapshot, error) {
	if ms.testErr != nil {
		return nil, ms.testErr
	}

	m := &mockSnapshot{
		slice: ms,
	}
	return m, nil
}

func (ms *mockSlice) GetSnapshots() ([]SnapshotInfo, error) {
	if ms.testErr != nil {
		return nil, ms.testErr
	}

	return nil, nil
}

func (ms *mockSlice) SetNlist(int)                                       {}
func (ms *mockSlice) InitCodebook() error                                { return nil }
func (ms *mockSlice) ResetCodebook() error                               { return nil }
func (ms *mockSlice) Train([]float32) error                              { return nil }
func (ms *mockSlice) InitCodebookFromSerialized(content []byte) error { return nil }

// -------------
// SliceSnapshot
// -------------

type mockSliceSnapshot struct {
	mockSlice *mockSlice
	snap      *mockSnapshot
}

func (mss *mockSliceSnapshot) SliceId() SliceId {
	return mss.mockSlice.ID
}

func (mss *mockSliceSnapshot) Snapshot() Snapshot {
	if mss.snap != nil {
		return mss.snap
	}
	return nil
}

// ------------------
// IndexReaderContext
// ------------------
type mockReaderContext struct {
}

func (mrc *mockReaderContext) Init(chan bool) bool            { return true }
func (mrc *mockReaderContext) Done()                          {}
func (mrc *mockReaderContext) SetCursorKey(cur *[]byte)       {}
func (mrc *mockReaderContext) GetCursorKey() *[]byte          { return nil }
func (mrc *mockReaderContext) User() string                   { return "" }
func (mrc *mockReaderContext) ReadUnits() uint64              { return 0 }
func (mrc *mockReaderContext) RecordReadUnits(byteLen uint64) {}
func (mrc *mockReaderContext) SkipReadMetering() bool         { return true }

// ------------
// SnapshotInfo
// ------------
type mockSnapshotInfo struct {
}

func (msi *mockSnapshotInfo) Timestamp() *c.TsVbuuid        { return nil }
func (msi *mockSnapshotInfo) IsCommitted() bool             { return true }
func (msi *mockSnapshotInfo) IsOSOSnap() bool               { return false }
func (msi *mockSnapshotInfo) Stats() map[string]interface{} { return nil }

// --------
// Snapshot
// --------
type mockSnapshot struct {
	slice       *mockSlice
	snapVersion int

	id        SliceId
	indInstid c.IndexInstId
	indDefId  c.IndexDefnId
	ts        *c.TsVbuuid

	count  uint64
	exists bool
	err    error

	datach chan Row
	errch  chan error
	order  SortOrder

	feeder snapshotFeeder
}

func (ms *mockSnapshot) Open() error                             { return nil }
func (ms *mockSnapshot) Close() error                            { return nil }
func (ms *mockSnapshot) IsOpen() bool                            { return true }
func (ms *mockSnapshot) Id() SliceId                             { return ms.id }
func (ms *mockSnapshot) IndexInstId() c.IndexInstId              { return ms.indInstid }
func (ms *mockSnapshot) IndexDefnId() c.IndexDefnId              { return ms.indDefId }
func (ms *mockSnapshot) Timestamp() *c.TsVbuuid                  { return ms.ts }
func (ms *mockSnapshot) Info() SnapshotInfo                      { return &mockSnapshotInfo{} }
func (ms *mockSnapshot) DecodeMeta(meta []byte) (uint64, []byte) { return 0, nil }

// -----------
// IndexReader
// -----------

func (ms *mockSnapshot) Range(ctx IndexReaderContext, start IndexKey, end IndexKey, incl Inclusion,
	cb EntryCallback) error {

	if ms.feeder != nil {
		ms.datach = make(chan Row)
		ms.errch = make(chan error)
		go ms.feeder(ms.datach, ms.errch)
		for {
			select {
			case err, _ := <-ms.errch:
				logging.Infof("[mockSnapshot::Range] got error from feeder: %v", err)
				return err
			case d, ok := <-ms.datach:
				if !ok {
					return nil
				}
				err := cb(d.key, d.value)
				if err != nil {
					logging.Infof("[mockSnapshot::Range] got error from callback: %v", err)
					return err
				}
			}
		}
	}
	return nil
}

func (ms *mockSnapshot) CountTotal(ctx IndexReaderContext, stopch StopChannel) (uint64, error) {
	return ms.count, ms.err
}
func (ms *mockSnapshot) StatCountTotal() (uint64, error) { return 0, nil }
func (ms *mockSnapshot) Exists(ctx IndexReaderContext, Indexkey IndexKey, stopch StopChannel) (bool, error) {
	return ms.exists, ms.err
}
func (ms *mockSnapshot) Lookup(IndexReaderContext, IndexKey, EntryCallback) error { return nil }

func (ms *mockSnapshot) CountRange(ctx IndexReaderContext, low, high IndexKey, inclusion Inclusion,
	stopch StopChannel) (uint64, error) {
	ms.datach = make(chan Row)
	ms.errch = make(chan error)
	ms.count = 0
	go ms.feeder(ms.datach, ms.errch)

loop:
	for {
		select {
		case ms.err, _ = <-ms.errch:
			break loop
		case _, ok := <-ms.datach:
			if !ok {
				break loop
			}
			ms.count++
		}
	}

	return ms.count, ms.err
}

func (ms *mockSnapshot) CountLookup(ctx IndexReaderContext, keys []IndexKey, stopch StopChannel) (uint64, error) {
	return 0, nil
}

func (ms *mockSnapshot) MultiScanCount(ctx IndexReaderContext, low, high IndexKey, inclusion Inclusion, scan Scan,
	distinct bool, stopch StopChannel) (uint64, error) {
	return 0, nil
}

func (ms *mockSnapshot) All(IndexReaderContext, EntryCallback) error { return nil }

func (ms *mockSnapshot) FindNearestCentroids(vec []float32, k int64) ([]int64, error) {
	return nil, nil
}

func (ms *mockSnapshot) ComputeDistanceTable(vec []float32) ([][]float32, error) {
	return nil, nil
}

func (ms *mockSnapshot) ComputeDistanceWithDT(code []byte, dtable [][]float32) float32 {
	return 0
}

func (ms *mockSnapshot) ComputeDistance(qvec []float32, fvecs []float32, dist []float32) error {
	return nil
}

func (ms *mockSnapshot) DecodeVector(code []byte, vec []float32) error {
	return nil
}

func (ms *mockSnapshot) DecodeVectors(n int, codes []byte, vecs []float32) error {
	return nil
}

// ----------------------
// Other Setter functions
// ----------------------

func (s *mockSlice) SetTimestamp(ts *c.TsVbuuid) error {
	s.ts = ts
	return nil
}

func (s *mockSlice) Timestamp() *c.TsVbuuid {
	return s.ts
}

func (s *mockSnapshot) SetTimestamp(ts *c.TsVbuuid) {
	s.ts = ts
}
