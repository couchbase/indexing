// defines Timestamp type to interface with go-coubhbase and also provides
// functions for set-operations on time-stamps.

// TODO: move file to go-couchbase, if go-couchbase can only accept
// Timestamp type definition, the we may need to do some rework to avoid
// namespace issues.

package common

import (
	"sort"
)

// Timestamp is logical clock to coordinate secondary index cluster.
type Timestamp struct {
	Bucket    string
	Vbnos     []uint16
	Seqnos    []uint64
	Vbuuids   []uint64
	Snapshots [][2]uint64
}

// NewTimestamp returns reference to new instance of Timestamp.
func NewTimestamp(bucket string, maxVbuckets int) *Timestamp {
	return &Timestamp{
		Bucket:    bucket,
		Vbnos:     make([]uint16, 0, maxVbuckets),
		Seqnos:    make([]uint64, 0, maxVbuckets),
		Vbuuids:   make([]uint64, 0, maxVbuckets),
		Snapshots: make([][2]uint64, 0, maxVbuckets),
	}
}

// Append adds a new set of vbno, seqno, vbuuid
func (ts *Timestamp) Append(vbno uint16, vbuuid, seqno, start, end uint64) *Timestamp {
	ts.Vbnos = append(ts.Vbnos, vbno)
	ts.Vbuuids = append(ts.Vbuuids, vbuuid)
	ts.Seqnos = append(ts.Seqnos, seqno)
	ts.Snapshots = append(ts.Snapshots, [2]uint64{start, end})
	return ts
}

// SelectByVbuckets will select vbuckets from `ts` for a subset of `vbuckets`,
// both `ts` and `vbuckets` are expected to be pre-sorted.
func (ts *Timestamp) SelectByVbuckets(vbuckets []uint16) *Timestamp {
	if ts == nil || vbuckets == nil {
		return ts
	}

	newts := NewTimestamp(ts.Bucket, 1024) // TODO: avoid magic numbers
	if len(ts.Vbnos) == 0 {
		return newts
	}

	cache := [MaxVbuckets]byte{} // TODO: optimize for GC
	for _, vbno := range vbuckets {
		cache[vbno] = 1
	}
	for i, vbno := range ts.Vbnos {
		if cache[vbno] == 1 {
			newts.Vbnos = append(newts.Vbnos, vbno)
			newts.Vbuuids = append(newts.Vbuuids, ts.Vbuuids[i])
			newts.Seqnos = append(newts.Seqnos, ts.Seqnos[i])
			newts.Snapshots = append(newts.Snapshots, ts.Snapshots[i])
		}
	}
	return newts
}

// FilterByVbuckets will exclude `vbuckets` from `ts`, both `ts` and `vbuckets`
// are expected to be pre-sorted. TODO: Write unit test case.
func (ts *Timestamp) FilterByVbuckets(vbuckets []uint16) *Timestamp {
	if ts == nil || vbuckets == nil {
		return ts
	}

	newts := NewTimestamp(ts.Bucket, MaxVbuckets) // TODO: avoid magic numbers
	if len(ts.Vbnos) == 0 {
		return newts
	}

	cache := [MaxVbuckets]byte{}
	for _, vbno := range vbuckets {
		cache[vbno] = 1
	}
	for i, vbno := range ts.Vbnos {
		if cache[vbno] == 1 {
			continue
		}
		newts.Vbnos = append(newts.Vbnos, vbno)
		newts.Seqnos = append(newts.Seqnos, ts.Seqnos[i])
		newts.Vbuuids = append(newts.Vbuuids, ts.Vbuuids[i])
		newts.Snapshots = append(newts.Snapshots, ts.Snapshots[i])
	}
	return newts
}

// CompareVbuckets will compare two timestamps for its bucket and vbuckets
func (ts *Timestamp) CompareVbuckets(other *Timestamp) bool {
	if ts == nil || other == nil {
		return false
	}
	sort.Sort(ts)
	sort.Sort(other)
	if ts.Bucket != other.Bucket || ts.Len() != other.Len() {
		return false
	}
	for i, vbno := range ts.Vbnos {
		if vbno != other.Vbnos[i] {
			return false
		}
	}
	return true
}

// CompareVbuuids will compare two timestamps for its bucket and vbuuids
func (ts *Timestamp) CompareVbuuids(other *Timestamp) bool {
	if ts == nil || other == nil {
		return false
	}
	sort.Sort(ts)
	sort.Sort(other)
	if ts.Bucket != other.Bucket || ts.Len() != other.Len() {
		return false
	}
	for i, vbuuid := range ts.Vbuuids {
		if (ts.Vbnos[i] != other.Vbnos[i]) || (vbuuid != other.Vbuuids[i]) ||
			(ts.Snapshots[i][0] != other.Snapshots[i][0]) ||
			(ts.Snapshots[i][1] != other.Snapshots[i][1]) {
			return false
		}
	}
	return true
}

// AsRecent will check whether timestamp `ts` is atleast as recent as
// timestamp `other`.
func (ts *Timestamp) AsRecent(other *Timestamp) bool {
	if ts == nil || other == nil {
		return false
	}
	sort.Sort(ts)
	sort.Sort(other)
	if ts.Bucket != other.Bucket || ts.Len() != other.Len() {
		return false
	}
	for i, vbuuid := range ts.Vbuuids {
		if vbuuid != other.Vbuuids[i] || ts.Seqnos[i] < other.Seqnos[i] {
			return false
		}
	}
	return true
}

// Union will return a union set of timestamps based on Vbuckets. Duplicate
// vbucket entries in `other` timestamp will be skipped.
func (ts *Timestamp) Union(other *Timestamp) *Timestamp {
	if ts == nil || other == nil {
		return ts
	}
	newts := NewTimestamp(ts.Bucket, MaxVbuckets)

	// copy from other
	newts.Vbnos = append(newts.Vbnos, other.Vbnos...)
	newts.Seqnos = append(newts.Seqnos, other.Seqnos...)
	newts.Vbuuids = append(newts.Vbuuids, other.Vbuuids...)
	newts.Snapshots = append(newts.Snapshots, other.Snapshots...)

	cache := [MaxVbuckets]byte{}
	for _, vbno := range other.Vbnos {
		cache[vbno] = 1
	}

	// deduplicate this
	for i, vbno := range ts.Vbnos {
		if cache[vbno] == 1 {
			continue
		}
		newts.Vbnos = append(newts.Vbnos, vbno)
		newts.Seqnos = append(newts.Seqnos, ts.Seqnos[i])
		newts.Vbuuids = append(newts.Vbuuids, ts.Vbuuids[i])
		newts.Snapshots = append(newts.Snapshots, ts.Snapshots[i])
	}
	sort.Sort(newts)
	return newts
}

// Unions will return a union set of all timestamps arguments. First vbucket
// entry from the list of timestamps will be picked and while rest are skipped.
func (ts *Timestamp) Unions(timestamps ...*Timestamp) *Timestamp {
	for _, other := range timestamps {
		ts = ts.Union(other)
	}
	return ts
}

// sort timestamp
func (ts *Timestamp) Len() int {
	return len(ts.Vbnos)
}

func (ts *Timestamp) Less(i, j int) bool {
	return ts.Vbnos[i] < ts.Vbnos[j]
}

func (ts *Timestamp) Swap(i, j int) {
	ts.Vbnos[i], ts.Vbnos[j] = ts.Vbnos[j], ts.Vbnos[i]
	ts.Seqnos[i], ts.Seqnos[j] = ts.Seqnos[j], ts.Seqnos[i]
	ts.Vbuuids[i], ts.Vbuuids[j] = ts.Vbuuids[j], ts.Vbuuids[i]
	ts.Snapshots[i], ts.Snapshots[j] = ts.Snapshots[j], ts.Snapshots[i]
}
