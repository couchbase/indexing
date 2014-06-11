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
	Bucket  string
	Vbnos   []uint16
	Seqnos  []uint64
	Vbuuids []uint64
}

// NewTimestamp returns reference to new instance of Timestamp.
func NewTimestamp(bucket string, maxVbuckets int) *Timestamp {
	return &Timestamp{
		Bucket:  bucket,
		Vbnos:   make([]uint16, 0, maxVbuckets),
		Seqnos:  make([]uint64, 0, maxVbuckets),
		Vbuuids: make([]uint64, 0, maxVbuckets),
	}
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

	cache := make(map[uint16]bool)
	for _, vbno := range vbuckets {
		cache[vbno] = true
	}
	for i, vbno := range ts.Vbnos {
		if _, ok := cache[vbno]; ok {
			newts.Vbnos = append(newts.Vbnos, vbno)
			newts.Seqnos = append(newts.Seqnos, ts.Seqnos[i])
			newts.Vbuuids = append(newts.Vbuuids, ts.Vbuuids[i])
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

	cache := make(map[uint16]bool)
	for _, vbno := range vbuckets {
		cache[vbno] = true
	}
	for i, vbno := range ts.Vbnos {
		if _, ok := cache[vbno]; ok {
			continue
		}
		newts.Vbnos = append(newts.Vbnos, vbno)
		newts.Seqnos = append(newts.Seqnos, ts.Seqnos[i])
		newts.Vbuuids = append(newts.Vbuuids, ts.Vbuuids[i])
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
		if (ts.Vbnos[i] != other.Vbnos[i]) || (vbuuid != other.Vbuuids[i]) {
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
	cache := make(map[uint16]bool)
	newts := NewTimestamp(ts.Bucket, 1024) // TODO: no magic numbers
	// copy from other
	newts.Vbnos = append(newts.Vbnos, other.Vbnos...)
	newts.Seqnos = append(newts.Seqnos, other.Seqnos...)
	newts.Vbuuids = append(newts.Vbuuids, other.Vbuuids...)
	for _, vbno := range other.Vbnos {
		cache[vbno] = true
	}
	// deduplicate this
	for i, vbno := range ts.Vbnos {
		if _, ok := cache[vbno]; ok {
			continue
		}
		newts.Vbnos = append(newts.Vbnos, vbno)
		newts.Seqnos = append(newts.Seqnos, ts.Seqnos[i])
		newts.Vbuuids = append(newts.Vbuuids, ts.Vbuuids[i])
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
}
