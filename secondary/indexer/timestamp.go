//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package indexer

import "fmt"

//list of seqno per vbucket
type Timestamp []Seqno

//Stability Timestamp
type StabilityTimestamp Timestamp

func NewTimestamp(numVbuckets int) Timestamp {
	ts := make([]Seqno, numVbuckets)
	return ts
}

func CopyTimestamp(ts Timestamp) Timestamp {
	newTs := make([]Seqno, len(ts))
	copy(newTs, ts)
	return newTs
}

//Equals returns true if both timestamps match, false otherwise
func (ts Timestamp) Equals(ts1 Timestamp) bool {

	//if length is not equal, no need to compare
	if len(ts) != len(ts1) {
		return false
	}

	//each individual value should match
	for i, t := range ts {
		if t != ts1[i] {
			return false
		}
	}
	return true
}

//GreaterThanEqual returns true if the given timestamp is matching or
//greater
func (ts Timestamp) GreaterThanEqual(ts1 Timestamp) bool {

	//if length is not equal, no need to compare
	if len(ts) != len(ts1) {
		return false
	}

	//each individual seqno should be matching or greater
	for i, t := range ts {
		if t < ts1[i] {
			return false
		}
	}
	return true
}

//GreaterThan returns true if the timestamp is greater
//than given timestamp
func (ts Timestamp) GreaterThan(ts1 Timestamp) bool {

	//if length is not equal, no need to compare
	if len(ts) != len(ts1) {
		return false
	}

	//atleast one seqno should be greater and
	//none should be less
	var foundGreaterSeqNo bool
	for i, t := range ts {
		if t > ts1[i] {
			foundGreaterSeqNo = true
		} else if t < ts1[i] {
			return false
		}
	}

	if foundGreaterSeqNo {
		return true
	} else {
		return false
	}
}

//IsZeroTs return true if all seqno in TS are zero
func (ts Timestamp) IsZeroTs() bool {

	for _, t := range ts {
		if t != Seqno(0) {
			return false
		}
	}
	return true
}

func (ts Timestamp) String() string {

	str := "["
	if ts != nil {
		for i, s := range ts {
			if i > 0 {
				str += ","
			}
			str += fmt.Sprintf("%d=%d", i, s)
		}
	}
	str += "]"
	return str
}
