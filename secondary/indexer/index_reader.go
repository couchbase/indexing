// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

// Inclusion controls how the boundaries values of a range are treated
type Inclusion int

const (
	Neither Inclusion = iota
	Low
	High
	Both
)

// SortOrder characterizes if the algorithm emits keys in a predictable order
type SortOrder string

const (
	Unsorted SortOrder = "none"
	Asc                = "asc"
	Desc               = "desc"
)

// Counter is a class of algorithms that return total node count efficiently
type Counter interface {
	CountTotal(ctx IndexReaderContext, stopch StopChannel) (uint64, error)
	// Approximate count
	StatCountTotal() (uint64, error)
}

// Exister is a class of algorithms that allow testing if a key exists in the
// index
type Exister interface {
	Exists(ctx IndexReaderContext, Indexkey IndexKey, stopch StopChannel) (bool, error)
}

// Looker is a class of algorithms that allow looking up a key in an index.
// Usually, being able to look up a key means we can iterate through all keys
// too, and so that is introduced here as well.
type Looker interface {
	Exister
	Lookup(IndexReaderContext, IndexKey, EntryCallback, FinishCallback) error
	All(IndexReaderContext, EntryCallback, FinishCallback) error
}

// Ranger is a class of algorithms that can extract a range of keys from the
// index.
type Ranger interface {
	Looker
	Range(IndexReaderContext, IndexKey, IndexKey, Inclusion, EntryCallback, FinishCallback) error
}

// RangeCounter is a class of algorithms that can count a range efficiently
type RangeCounter interface {
	CountRange(ctx IndexReaderContext, low, high IndexKey, inclusion Inclusion, stopch StopChannel) (
		uint64, error)
	CountLookup(ctx IndexReaderContext, keys []IndexKey, stopch StopChannel) (uint64, error)
	MultiScanCount(ctx IndexReaderContext, low, high IndexKey, inclusion Inclusion,
		scan Scan, distinct bool, stopch StopChannel) (
		uint64, error)
}

type IndexReader interface {
	Counter
	Ranger
	RangeCounter

	DecodeMeta([]byte) (uint64, []byte)                                    // Used only for BHIVE storage currently
	FetchValue(IndexReaderContext, uint64, []byte, []byte) ([]byte, error) // Used only for BHIVE storage
}

// Abstract context implemented by storage subsystem
type IndexReaderContext interface {
	Init(chan bool) bool
	Done()
	SetCursorKey(cur *[]byte)
	GetCursorKey() *[]byte
	User() string
	ReadUnits() uint64
	RecordReadUnits(byteLen uint64)
	SkipReadMetering() bool
}
