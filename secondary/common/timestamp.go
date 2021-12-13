// defines timestamp types to interface with go-coubhbase and also provides
// functions for set-operations on time-stamps.

package common

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/couchbase/indexing/secondary/common/collections"
	"github.com/couchbase/indexing/secondary/logging"
)

// TsVb is logical clock for a subset of vbuckets.
type TsVb struct {
	Bucket string
	Vbnos  []uint16
	Seqnos []uint64
}

// TsVbFull is logical clock for full set of vbuckets.
type TsVbFull struct {
	Bucket string
	Seqnos []uint64
}

// TsVbuuid is logical clock for full set of vbuckets along with branch value
// and last seen snapshot.
type TsVbuuid struct {
	Bucket       string
	ScopeId      string
	CollectionId string
	Seqnos       []uint64
	Vbuuids      []uint64
	ManifestUIDs []string
	OSOCount     []uint64 `json:"-"` //excluded from json marshal
	Crc64        uint64
	Snapshots    [][2]uint64
	SnapType     IndexSnapType
	LargeSnap    bool
	SnapAligned  bool
	DisableAlign bool
	OpenOSOSnap  bool
}

// NewTsVbuuid returns reference to new instance of TsVbuuid.
// `numVbuckets` is same as `maxVbuckets`.
func NewTsVbuuid(bucket string, numVbuckets int) *TsVbuuid {
	ts := &TsVbuuid{
		Bucket:       bucket,
		Seqnos:       make([]uint64, numVbuckets),
		Vbuuids:      make([]uint64, numVbuckets),
		ManifestUIDs: make([]string, numVbuckets),
		Snapshots:    make([][2]uint64, numVbuckets),
	}
	ts.SetEpochManifestUIDIfEmpty()
	return ts
}

// NewTsVbuuid returns reference to new instance of TsVbuuid.
func NewTsVbuuid2(bucket string, seqnos, vbuuids []uint64) *TsVbuuid {
	return &TsVbuuid{
		Bucket:  bucket,
		Seqnos:  seqnos,
		Vbuuids: vbuuids,
	}
}

func newTsVbuuid() interface{} {
	ts := &TsVbuuid{
		Bucket:       "",
		Seqnos:       make([]uint64, NUM_VBUCKETS),
		Vbuuids:      make([]uint64, NUM_VBUCKETS),
		ManifestUIDs: make([]string, NUM_VBUCKETS),
		Snapshots:    make([][2]uint64, NUM_VBUCKETS),
	}
	ts.SetEpochManifestUIDIfEmpty()
	return ts
}

var tsVbuuidPool = sync.Pool{New: newTsVbuuid}
var NUM_VBUCKETS int

func NewTsVbuuidCached(bucket string, numVbuckets int) *TsVbuuid {

	NUM_VBUCKETS = numVbuckets

	ts := tsVbuuidPool.Get().(*TsVbuuid)

	//re-init
	for i, _ := range ts.Vbuuids {
		ts.Seqnos[i] = 0
		ts.Vbuuids[i] = 0
		ts.ManifestUIDs[i] = collections.MANIFEST_UID_EPOCH
		ts.Snapshots[i][0] = 0
		ts.Snapshots[i][1] = 0
		ts.Crc64 = 0
	}
	ts.Bucket = bucket
	ts.ScopeId = ""
	ts.CollectionId = ""
	ts.OpenOSOSnap = false
	return ts
}

func (ts *TsVbuuid) Free() {
	tsVbuuidPool.Put(ts)
}

// GetVbnos will return the list of all vbnos.
func (ts *TsVbuuid) GetVbnos() []uint16 {
	var vbnos []uint16
	for i := 0; i < len(ts.Vbuuids); i++ {
		if ts.Vbuuids[i] != 0 { //if vbuuid is valid
			vbnos = append(vbnos, uint16(i))
		}
	}
	return vbnos
}

func (ts *TsVbuuid) SetEpochManifestUIDIfEmpty() {

	if ts.ManifestUIDs == nil {
		ts.ManifestUIDs = make([]string, len(ts.Vbuuids))
	}

	for i := 0; i < len(ts.Vbuuids); i++ {
		if ts.ManifestUIDs[i] == "" {
			ts.ManifestUIDs[i] = collections.MANIFEST_UID_EPOCH
		}
	}
}

// CompareVbuuids will compare two timestamps for its bucket and vbuuids
func (ts *TsVbuuid) CompareVbuuids(other *TsVbuuid) bool {
	if ts == nil || other == nil {
		return false
	}
	if ts.Bucket != other.Bucket || ts.Len() != other.Len() {
		return false
	}
	for i, vbuuid := range ts.Vbuuids {
		if vbuuid != other.Vbuuids[i] {
			return false
		}
	}
	return true
}

func (ts *TsVbuuid) IsEpoch() bool {
	for _, seqno := range ts.Seqnos {
		if seqno != 0 {
			return false
		}
	}
	return true
}

func (ts *TsVbuuid) HasZeroSeqNum() bool {
	for _, seqno := range ts.Seqnos {
		if seqno == 0 {
			return true
		}
	}
	return false
}

// CheckVbuuids will check whether vbuuids in timestamp `ts` is same
// as that of `other`.
func (ts *TsVbuuid) CheckCrc64(other *TsVbuuid) bool {
	if ts == nil || other == nil {
		return false
	}

	if ts.Bucket != other.Bucket {
		return false
	}

	return ts.Crc64 == 0 || other.Crc64 == 0 || ts.Crc64 == other.Crc64
}

// AsRecent will check whether timestamp `ts` is atleast as recent as
// timestamp `other`.
func (ts *TsVbuuid) AsRecent(other *TsVbuuid) bool {
	if ts == nil || other == nil {
		return false
	}
	if ts.Bucket != other.Bucket {
		return false
	}

	if len(ts.Vbuuids) > len(other.Vbuuids) {
		return false
	}
	for i, vbuuid := range ts.Vbuuids {
		//skip comparing the vbucket if "other" ts has vbuuid 0
		if other.Vbuuids[i] == 0 {
			continue
		}
		if vbuuid != other.Vbuuids[i] || ts.Seqnos[i] < other.Seqnos[i] {
			return false
		}
	}
	return true
}

func (ts *TsVbuuid) Union(other *TsVbuuid) *TsVbuuid {

	if ts == nil {
		return other
	}

	if other == nil {
		return ts
	}

	if ts.Bucket != other.Bucket {
		return nil
	}

	if len(ts.Vbuuids) != len(other.Vbuuids) {
		return nil
	}

	result := other.Clone()
	result.Crc64 = 0
	for i, _ := range result.Vbuuids {
		if ts.Vbuuids[i] != 0 && result.Vbuuids[i] == 0 {
			result.Seqnos[i] = ts.Seqnos[i]
			result.Vbuuids[i] = ts.Vbuuids[i]
			result.ManifestUIDs[i] = ts.ManifestUIDs[i]
			result.Snapshots[i][0] = ts.Snapshots[i][0]
			result.Snapshots[i][1] = ts.Snapshots[i][1]
		}
	}

	return result
}

// AsRecentTs will check whether timestamp `ts` is atleast as recent as
// timestamp `other`.
func (ts *TsVbuuid) AsRecentTs(other *TsVbuuid) bool {
	if ts == nil || other == nil {
		return false
	}
	if ts.Bucket != other.Bucket {
		return false
	}
	if len(ts.Seqnos) > len(other.Seqnos) {
		return false
	}
	for i, seqno := range ts.Seqnos {
		if seqno < other.Seqnos[i] {
			return false
		}
	}
	return true
}

// Len return number of entries in the timestamp.
func (ts *TsVbuuid) Len() int {
	length := 0
	for i := 0; i < len(ts.Vbuuids); i++ {
		if ts.Vbuuids[i] != 0 { //if vbuuid is valid
			length++
		}
	}
	return length
}

//Persisted returns the value of persisted flag
func (ts *TsVbuuid) GetSnapType() IndexSnapType {
	return ts.SnapType
}

//Persisted sets the persisted flag
func (ts *TsVbuuid) SetSnapType(typ IndexSnapType) {
	ts.SnapType = typ
}

//HasLargeSnapshot returns the value of largeSnap flag
func (ts *TsVbuuid) HasLargeSnapshot() bool {
	return ts.LargeSnap
}

//SetLargeSnapshot sets the largeSnap flag
func (ts *TsVbuuid) SetLargeSnapshot(largeSnap bool) {
	ts.LargeSnap = largeSnap
}

func (ts *TsVbuuid) HasDisableAlign() bool {
	return ts.DisableAlign
}

func (ts *TsVbuuid) SetDisableAlign(disable bool) {
	ts.DisableAlign = disable
}

func (ts *TsVbuuid) HasOpenOSOSnap() bool {

	if ts == nil {
		return false
	}

	return ts.OpenOSOSnap
}

func (ts *TsVbuuid) SetOpenOSOSnap(open bool) {
	ts.OpenOSOSnap = open
}

func (ts *TsVbuuid) GetCrc64() uint64 {

	if ts == nil {
		return 0
	}

	return ts.Crc64
}

// Copy will return a clone of this timestamp.
func (ts *TsVbuuid) Copy() *TsVbuuid {
	newTs := NewTsVbuuid(ts.Bucket, len(ts.Seqnos))
	copy(newTs.Seqnos, ts.Seqnos)
	copy(newTs.Vbuuids, ts.Vbuuids)
	copy(newTs.ManifestUIDs, ts.ManifestUIDs)
	copy(newTs.Snapshots, ts.Snapshots)
	copy(newTs.OSOCount, ts.OSOCount)
	newTs.SnapType = ts.SnapType
	newTs.LargeSnap = ts.LargeSnap
	newTs.SnapAligned = ts.SnapAligned
	newTs.Crc64 = ts.Crc64

	newTs.ScopeId = ts.ScopeId
	newTs.CollectionId = ts.CollectionId
	newTs.OpenOSOSnap = ts.OpenOSOSnap
	return newTs
}

func (ts *TsVbuuid) CopyFrom(src *TsVbuuid) {
	copy(ts.Seqnos, src.Seqnos)
	copy(ts.Vbuuids, src.Vbuuids)
	copy(ts.ManifestUIDs, src.ManifestUIDs)
	copy(ts.Snapshots, src.Snapshots)
	copy(ts.OSOCount, src.OSOCount)
	ts.SnapType = src.SnapType
	ts.LargeSnap = src.LargeSnap
	ts.SnapAligned = src.SnapAligned
	ts.Crc64 = src.Crc64
	ts.ScopeId = src.ScopeId
	ts.CollectionId = src.CollectionId
	ts.OpenOSOSnap = src.OpenOSOSnap
}

// Equal returns whether `ts` and `other` compare equal.
func (ts *TsVbuuid) Equal(other *TsVbuuid) bool {
	return ts.Equal2(other, true)
}

// Equal returns whether `ts` and `other` compare equal.
func (ts *TsVbuuid) Equal2(other *TsVbuuid, compareSnapshot bool) bool {
	if ts != nil && other == nil ||
		ts == nil && other != nil {
		return false
	}

	if ts == nil && other == nil {
		return true
	}

	if len(ts.Seqnos) != len(other.Seqnos) {
		return false
	}

	for i, seqno := range ts.Seqnos {
		if other.Seqnos[i] != seqno {
			return false
		}
	}

	for i, vbuuid := range ts.Vbuuids {
		if other.Vbuuids[i] != vbuuid {
			return false
		}
	}

	if compareSnapshot {
		for i, sn := range ts.Snapshots {
			if other.Snapshots[i][0] != sn[0] {
				return false
			}

			if other.Snapshots[i][1] != sn[1] {
				return false
			}
		}
	}

	return true
}

// Equal returns whether `ts` is equal or greater than `other`
func (ts *TsVbuuid) EqualOrGreater(other *TsVbuuid, compareVbuuidForZeroSeqnos bool) bool {
	if ts != nil && other == nil ||
		ts == nil && other != nil {
		return false
	}

	if ts == nil && other == nil {
		return true
	}

	if len(ts.Seqnos) != len(other.Seqnos) {
		return false
	}

	for i, seqno := range ts.Seqnos {
		if other.Seqnos[i] > seqno {
			return false
		}
	}

	for i, vbuuid := range ts.Vbuuids {
		if other.Seqnos[i] != 0 || compareVbuuidForZeroSeqnos {
			if other.Vbuuids[i] != vbuuid {
				return false
			}
		}
	}

	return true
}

// Clone of TsVbuuid
func (ts *TsVbuuid) Clone() *TsVbuuid {

	other := NewTsVbuuid(ts.Bucket, len(ts.Seqnos))
	for i, seqno := range ts.Seqnos {
		other.Seqnos[i] = seqno
	}

	for i, vbuuid := range ts.Vbuuids {
		other.Vbuuids[i] = vbuuid
	}

	for i, sn := range ts.Snapshots {
		other.Snapshots[i][0] = sn[0]
		other.Snapshots[i][1] = sn[1]
	}

	for i, m := range ts.ManifestUIDs {
		other.ManifestUIDs[i] = m
	}

	other.Crc64 = ts.Crc64
	other.ScopeId = ts.ScopeId
	other.CollectionId = ts.CollectionId
	other.SnapAligned = ts.SnapAligned
	other.OpenOSOSnap = ts.OpenOSOSnap

	return other
}

// Convert into a human readable format
func (ts *TsVbuuid) String() string {
	var buf bytes.Buffer
	vbnos := ts.GetVbnos()
	fmsg := "bucket: %v, scopeId: %v, collectionId: %v, vbuckets: %v Crc64: %v snapType %v -\n"
	buf.WriteString(fmt.Sprintf(fmsg, ts.Bucket, ts.ScopeId, ts.CollectionId,
		len(vbnos), ts.Crc64, ts.SnapType))

	if ts.OSOCount == nil {
		fmsg = "    {vbno, vbuuid, manifest, seqno, snapshot-start, snapshot-end}\n"
		buf.WriteString(fmt.Sprintf(fmsg))
		for _, v := range vbnos {
			start, end := ts.Snapshots[v][0], ts.Snapshots[v][1]
			buf.WriteString(fmt.Sprintf("    {%5d %16x %v %10d %10d %10d}\n",
				v, ts.Vbuuids[v], ts.ManifestUIDs[v], ts.Seqnos[v], start, end))
		}
	} else {
		fmsg = "    {vbno, vbuuid, manifest, seqno, oscount, snapshot-start, snapshot-end}\n"
		buf.WriteString(fmt.Sprintf(fmsg))
		for _, v := range vbnos {
			start, end := ts.Snapshots[v][0], ts.Snapshots[v][1]
			buf.WriteString(fmt.Sprintf("    {%5d %16x %v %10d %10d %10d %10d}\n",
				v, ts.Vbuuids[v], ts.ManifestUIDs[v], ts.Seqnos[v], ts.OSOCount[v], start, end))
		}

	}
	return buf.String()
}

// Convert the difference between two timestamps to human readable format
func (ts *TsVbuuid) Diff(other *TsVbuuid) string {

	var buf bytes.Buffer
	if ts.Equal(other) {
		buf.WriteString("Timestamps are equal\n")
		return buf.String()
	}

	if other == nil {
		buf.WriteString("This timestamp:\n")
		buf.WriteString(ts.String())
		buf.WriteString("Other timestamp is nil\n")
		return buf.String()
	}

	if len(other.Seqnos) != len(ts.Seqnos) {
		logging.Debugf("Two timestamps contain different number of vbuckets\n")
		buf.WriteString("This timestamp:\n")
		buf.WriteString(ts.String())
		buf.WriteString("Other timestamp:\n")
		buf.WriteString(other.String())
		return buf.String()
	}

	for i := range ts.Seqnos {
		if ts.Seqnos[i] != other.Seqnos[i] || ts.Vbuuids[i] != other.Vbuuids[i] ||
			ts.Snapshots[i][0] != other.Snapshots[i][0] || ts.Snapshots[i][1] != other.Snapshots[i][1] ||
			ts.ManifestUIDs[i] != other.ManifestUIDs[i] {
			buf.WriteString(fmt.Sprintf("This timestamp: bucket %s, vb = %d, vbuuid = %d, manifestuid = %v, seqno = %d, snapshot[0] = %d, snapshot[1] = %d\n",
				ts.Bucket, i, ts.Vbuuids[i], ts.ManifestUIDs[i], ts.Seqnos[i], ts.Snapshots[0], ts.Snapshots[1]))
			buf.WriteString(fmt.Sprintf("Other timestamp: bucket %s, vb = %d, vbuuid = %d, manifestuid = %v,  seqno = %d, snapshot[0] = %d, snapshot[1] = %d\n",
				other.Bucket, i, other.Vbuuids[i], other.ManifestUIDs[i], other.Seqnos[i], other.Snapshots[0], other.Snapshots[1]))
		}
	}

	return buf.String()
}

//check if seqnum of all vbuckets are aligned with the snapshot end
func (ts *TsVbuuid) CheckSnapAligned() bool {

	// Nil timestamp can be considered equivalent to all vbs with seqno=0 (empty bucket)
	if ts == nil {
		return true
	}

	for i, s := range ts.Snapshots {
		if ts.Seqnos[i] != s[1] {
			return false
		}
	}
	return true

}

//IsSnapAligned returns the value of SnapAligned flag
func (ts *TsVbuuid) IsSnapAligned() bool {

	if ts == nil {
		return true
	}

	return ts.SnapAligned
}

//SetSnapAligned sets the SnapAligned flag
func (ts *TsVbuuid) SetSnapAligned(snapAligned bool) {
	ts.SnapAligned = snapAligned
}
