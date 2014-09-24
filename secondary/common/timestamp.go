// defines timestamp types to interface with go-coubhbase and also provides
// functions for set-operations on time-stamps.

// TODO: move file to go-couchbase, if go-couchbase can only accept
// Timestamp type definition, the we may need to do some rework to avoid
// namespace issues.

package common

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
	Bucket    string
	Seqnos    []uint64
	Vbuuids   []uint64
	Snapshots [][2]uint64
}

// NewTsVbuuid returns reference to new instance of TsVbuuid.
func NewTsVbuuid(bucket string, numVbuckets int) *TsVbuuid {
	return &TsVbuuid{
		Bucket:    bucket,
		Seqnos:    make([]uint64, numVbuckets),
		Vbuuids:   make([]uint64, numVbuckets),
		Snapshots: make([][2]uint64, numVbuckets),
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
		if (vbuuid != other.Vbuuids[i]) ||
			(ts.Snapshots[i][0] != other.Snapshots[i][0]) ||
			(ts.Snapshots[i][1] != other.Snapshots[i][1]) {
			return false
		}
	}
	return true
}

// AsRecent will check whether timestamp `ts` is atleast as recent as
// timestamp `other`.
func (ts *TsVbuuid) AsRecent(other *TsVbuuid) bool {
	if ts == nil || other == nil {
		return false
	}
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

func (ts *TsVbuuid) Len() int {
	return len(ts.Seqnos)
}

//TODO: As TsVbuuid acts like a array now, the below helper functions are
//no longer required. These can be deleted, once we are sure these are not
//going to required.

/*
// SelectByVbuckets will select vbuckets from `ts` for a subset of `vbuckets`,
// both `ts` and `vbuckets` are expected to be pre-sorted.
func (ts *TsVbuuid) SelectByVbuckets(vbuckets []uint16) *TsVbuuid {
	if ts == nil || vbuckets == nil {
		return ts
	}

	newts := NewTsVbuuid(ts.Bucket, 1024) // TODO: avoid magic numbers
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
func (ts *TsVbuuid) FilterByVbuckets(vbuckets []uint16) *TsVbuuid {
	if ts == nil || vbuckets == nil {
		return ts
	}

	newts := NewTsVbuuid(ts.Bucket, MaxVbuckets) // TODO: avoid magic numbers
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

// Union will return a union set of timestamps based on Vbuckets. Duplicate
// vbucket entries in `other` timestamp will be skipped.
func (ts *TsVbuuid) Union(other *TsVbuuid) *TsVbuuid {
	if ts == nil || other == nil {
		return ts
	}
	newts := NewTsVbuuid(ts.Bucket, MaxVbuckets)

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
func (ts *TsVbuuid) Unions(timestamps ...*TsVbuuid) *TsVbuuid {
	for _, other := range timestamps {
		ts = ts.Union(other)
	}
	return ts
}

// CompareVbuckets will compare two timestamps for its bucket and vbuckets
func (ts *TsVbuuid) CompareVbuckets(other *TsVbuuid) bool {
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

func (ts *TsVbuuid) Less(i, j int) bool {
	return ts.Vbnos[i] < ts.Vbnos[j]
}

func (ts *TsVbuuid) Swap(i, j int) {
	ts.Vbnos[i], ts.Vbnos[j] = ts.Vbnos[j], ts.Vbnos[i]
	ts.Seqnos[i], ts.Seqnos[j] = ts.Seqnos[j], ts.Seqnos[i]
	ts.Vbuuids[i], ts.Vbuuids[j] = ts.Vbuuids[j], ts.Vbuuids[i]
	ts.Snapshots[i], ts.Snapshots[j] = ts.Snapshots[j], ts.Snapshots[i]
}

*/
