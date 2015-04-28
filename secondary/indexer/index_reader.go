// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

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
	CountTotal(stopch StopChannel) (uint64, error)
	// Approximate count
	StatCountTotal() (uint64, error)
}

// Exister is a class of algorithms that allow testing if a key exists in the
// index
type Exister interface {
	Exists(key IndexKey, stopch StopChannel) (bool, error)
}

// Looker is a class of algorithms that allow looking up a key in an index.
// Usually, being able to look up a key means we can iterate through all keys
// too, and so that is introduced here as well.
//
type Looker interface {
	Exister
	Lookup(IndexKey, EntryCallback) error
	All(EntryCallback) error
}

// Ranger is a class of algorithms that can extract a range of keys from the
// index.
type Ranger interface {
	Looker
	Range(IndexKey, IndexKey, Inclusion, EntryCallback) error
}

// RangeCounter is a class of algorithms that can count a range efficiently
type RangeCounter interface {
	CountRange(low, high IndexKey, inclusion Inclusion, stopch StopChannel) (
		uint64, error)
	CountLookup(keys []IndexKey, stopch StopChannel) (uint64, error)
}

type IndexReader interface {
	Counter
	Ranger
	RangeCounter
}
