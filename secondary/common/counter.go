// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package common

import (
	"encoding/json"
	"fmt"
)

// convergent counter
// After the counter is updated in one node, any client can read the latest counter value by
// polling the counter from every node.  The latest value of the counter can be re-computed from
// these eventual-consistent counters retrieved from all the nodes without having to use timestamp
// to determine the latest counter value. To ensure the counter is not lost after update due to
// failure, the client should do quorum-write.
type Counter struct {
	HasValue bool
	Base     uint32
	Incr     uint32
	Decr     uint32
}

func (c *Counter) MergeWith(other Counter) (Counter, bool, error) {

	var changed bool
	result := *c

	if !other.HasValue {
		return result, changed, nil
	}

	if result.HasValue && other.Base != result.Base {
		return result, changed, fmt.Errorf("Cannot merge counter with different base values")
	}

	if !result.HasValue {
		result.Base = other.Base
		result.HasValue = true
		changed = true
	}

	if result.Incr < other.Incr {
		result.Incr = other.Incr
		result.HasValue = true
		changed = true
	}

	if result.Decr < other.Decr {
		result.Decr = other.Decr
		result.HasValue = true
		changed = true
	}

	return result, changed, nil
}

func (c *Counter) Value() (uint32, bool) {

	if !c.HasValue {
		return 0, false
	}

	return c.Base + c.Incr - c.Decr, true
}

func (c *Counter) Initialize(base uint32) {
	c.Base = base
	c.HasValue = true
}

func (c *Counter) InitializeAndIncrement(base uint32, increment uint32) {

	if !c.HasValue {
		c.Initialize(base)
	}

	c.Increment(increment)
}

func (c *Counter) InitializeAndDecrement(base uint32, decrement uint32) {

	if !c.HasValue {
		c.Initialize(base)
	}

	c.Decrement(decrement)
}

func (c *Counter) Decrement(decrement uint32) {
	c.Decr = c.Decr + decrement
}

func (c *Counter) Increment(increment uint32) {
	c.Incr = c.Incr + increment
}

func (c *Counter) NeedMergeWith(other Counter) (bool, error) {
	_, merged, err := c.MergeWith(other)
	if err != nil {
		return false, err
	}
	return merged, nil
}

func (c *Counter) IsValid() bool {
	return c.HasValue
}

func UnmarshallCounter(data []byte) (*Counter, error) {

	counter := new(Counter)
	if err := json.Unmarshal(data, counter); err != nil {
		return nil, err
	}

	return counter, nil
}

func MarshallCounter(counter *Counter) ([]byte, error) {

	buf, err := json.Marshal(&counter)
	if err != nil {
		return nil, err
	}

	return buf, nil
}
