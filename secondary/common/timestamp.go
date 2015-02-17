// defines timestamp types to interface with go-coubhbase and also provides
// functions for set-operations on time-stamps.

package common

import (
	"fmt"
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
	Bucket    string
	Seqnos    []uint64
	Vbuuids   []uint64
	Snapshots [][2]uint64
	Persisted bool
}

// NewTsVbuuid returns reference to new instance of TsVbuuid.
// `numVbuckets` is same as `maxVbuckets`.
func NewTsVbuuid(bucket string, numVbuckets int) *TsVbuuid {
	return &TsVbuuid{
		Bucket:    bucket,
		Seqnos:    make([]uint64, numVbuckets),
		Vbuuids:   make([]uint64, numVbuckets),
		Snapshots: make([][2]uint64, numVbuckets),
	}
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
	if ts.Bucket != other.Bucket {
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
func (ts *TsVbuuid) IsPersisted() bool {
	return ts.Persisted
}

//Persisted sets the persisted flag
func (ts *TsVbuuid) SetPersisted(persist bool) {
	ts.Persisted = persist
}

// Copy will return a clone of this timestamp.
func (ts *TsVbuuid) Copy() *TsVbuuid {
	newTs := NewTsVbuuid(ts.Bucket, len(ts.Seqnos))
	copy(newTs.Seqnos, ts.Seqnos)
	copy(newTs.Vbuuids, ts.Vbuuids)
	copy(newTs.Snapshots, ts.Snapshots)
	return newTs
}

// Equal returns whether `ts` and `other` compare equal.
func (ts *TsVbuuid) Equal(other *TsVbuuid) bool {
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

	for i, sn := range ts.Snapshots {
		if other.Snapshots[i][0] != sn[0] {
			return false
		}

		if other.Snapshots[i][1] != sn[1] {
			return false
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

	return other
}

func (ts *TsVbuuid) String() string {
	vbnos := ts.GetVbnos()
	s := fmt.Sprintf("bucket: %v, vbuckets: %v -\n",
		ts.Bucket, len(vbnos))
	s += fmt.Sprintf("    vbno, vbuuid, seqno, snapshot-start, snapshot-end\n")
	for _, v := range vbnos {
		start, end := ts.Snapshots[v][0], ts.Snapshots[v][1]
		s += fmt.Sprintf("    {%5d %16x %10d %10d %10d}\n",
			v, ts.Vbuuids[v], ts.Seqnos[v], start, end)
	}
	return s
}

// DebugPrint to log this timestamp using logging.Debugf.
func (ts *TsVbuuid) DebugPrint() {

	logging.Debugf("TsVbuuid : bucket = %s", ts.Bucket)
	for i, seqno := range ts.Seqnos {
		logging.Debugf("TsVbuuid : vb = %d, vbuuid = %d, seqno = %d, snapshot[0] = %d, snapshot[1] = %d",
			i, ts.Vbuuids[i], seqno, ts.Snapshots[0], ts.Snapshots[1])
	}
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

    maxVbuckets := len(ts.Seqnos)
    newts := NewTsVbuuid(ts.Bucket, maxVbuckets)
    if len(ts.Vbnos) == 0 {
        return newts
    }

    cache := [maxVbuckets]byte{}
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

    maxVbuckets := len(ts.Seqnos)
    newts := NewTsVbuuid(ts.Bucket, maxVbuckets)
    if len(ts.Vbnos) == 0 {
        return newts
    }

    cache := [maxVbuckets]byte{}
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

    maxVbuckets := len(ts.Seqnos)
    newts := NewTsVbuuid(ts.Bucket, maxVbuckets)

    // copy from other
    newts.Vbnos = append(newts.Vbnos, other.Vbnos...)
    newts.Seqnos = append(newts.Seqnos, other.Seqnos...)
    newts.Vbuuids = append(newts.Vbuuids, other.Vbuuids...)
    newts.Snapshots = append(newts.Snapshots, other.Snapshots...)

    cache := [maxVbuckets]byte{}
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
