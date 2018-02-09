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
	"bytes"
	"fmt"
	"github.com/couchbase/indexing/secondary/collatejson"
	"github.com/couchbase/query/value"
)

type AggrFuncType uint32

const (
	AGG_MIN AggrFuncType = iota
	AGG_MAX
	AGG_SUM
	AGG_COUNT
	AGG_COUNTN
	AGG_INVALID
)

func (a AggrFuncType) String() string {

	switch a {
	case AGG_MIN:
		return "MIN"
	case AGG_MAX:
		return "MAX"
	case AGG_SUM:
		return "SUM"
	case AGG_COUNT:
		return "COUNT"
	case AGG_COUNTN:
		return "COUNTN"
	default:
		return "AGG_UNKNOWN"
	}
}

type AggrFunc interface {
	Type() AggrFuncType
	AddDelta(delta interface{})
	Value() interface{}
	Distinct() bool
}

var (
	encodedNull = []byte{2, 0}
)

func NewAggrFunc(typ AggrFuncType, val interface{}, distinct bool) AggrFunc {

	var agg AggrFunc

	switch typ {

	case AGG_SUM:
		agg = &AggrFuncSum{typ: AGG_SUM, distinct: distinct}
	case AGG_COUNT:
		agg = &AggrFuncCount{typ: AGG_COUNT, distinct: distinct}
	case AGG_COUNTN:
		agg = &AggrFuncCountN{typ: AGG_COUNTN, distinct: distinct}
	case AGG_MIN:
		agg = &AggrFuncMin{typ: AGG_MIN, distinct: distinct}
	case AGG_MAX:
		agg = &AggrFuncMax{typ: AGG_MAX, distinct: distinct}
	default:
		return nil
	}

	agg.AddDelta(val)

	return agg
}

type AggrFuncSum struct {
	typ      AggrFuncType
	val      float64
	validVal bool
	distinct bool
	lastVal  float64
}

func (a AggrFuncSum) Type() AggrFuncType {
	return AGG_SUM
}

func (a AggrFuncSum) Value() interface{} {
	if !a.validVal {
		return nil
	}
	return a.val
}

func (a AggrFuncSum) Distinct() bool {
	return a.distinct
}

//Only numeric values are considered.
//null/missing/non-numeric are ignored.
func (a *AggrFuncSum) AddDelta(delta interface{}) {

	var actual interface{}

	if val, ok := delta.(value.Value); ok {
		actual = val.ActualForIndex()
	} else {
		actual = delta
	}

	switch v := actual.(type) {

	case float64:
		a.validVal = true
		if a.distinct {
			if !a.checkDistinct(v) {
				return
			}
		}
		a.val += v

	case int64:
		a.validVal = true
		if a.distinct {
			if !a.checkDistinct(float64(v)) {
				return
			}
		}
		a.val += float64(v) // TODO: Do not convert. Support SUM for both int64 and float64

	case nil, bool, []interface{}, map[string]interface{}, string:
		//ignored

	default:
		//ignored
	}
}

func (a *AggrFuncSum) checkDistinct(newVal float64) bool {

	if a.lastVal == newVal {
		return false
	}

	a.lastVal = newVal
	return true

}

func (a AggrFuncSum) String() string {
	return fmt.Sprintf("Type %v Value %v Distinct %v LastVal %v", a.typ, a.val, a.distinct, a.lastVal)
}

type AggrFuncCount struct {
	typ       AggrFuncType
	val       int64
	distinct  bool
	lastEntry interface{}
}

func (a AggrFuncCount) Type() AggrFuncType {
	return AGG_COUNT
}

func (a AggrFuncCount) Value() interface{} {
	return a.val
}

func (a AggrFuncCount) Distinct() bool {
	return a.distinct
}

//null/missing are ignored.
//add 1 for all other types
func (a *AggrFuncCount) AddDelta(delta interface{}) {

	//ignore if null or missing
	if isNullOrMissing(delta) {
		return
	}

	if a.distinct {
		if !a.checkDistinct(delta) {
			return
		}
	}
	a.val += 1

}

func (a *AggrFuncCount) checkDistinct(newval interface{}) bool {

	switch v := newval.(type) {

	case []byte:
		if a.lastEntry == nil {
			a.lastEntry = append([]byte(nil), v...)
		} else {
			if bytes.Compare(v, a.lastEntry.([]byte)) == 0 {
				return false
			}
			a.lastEntry = append(a.lastEntry.([]byte)[:0], v...)
		}

	case value.Value:
		if a.lastEntry != nil && v.EquivalentTo(a.lastEntry.(value.Value)) {
			return false
		}
		a.lastEntry = newval
	}

	return true

}

func (a AggrFuncCount) String() string {
	return fmt.Sprintf("Type %v Value %v Distinct %v LastVal %v", a.typ, a.val, a.distinct, a.lastEntry)
}

type AggrFuncCountN struct {
	typ       AggrFuncType
	val       int64
	distinct  bool
	lastEntry interface{}
}

func (a AggrFuncCountN) Type() AggrFuncType {
	return AGG_COUNTN
}

func (a AggrFuncCountN) Value() interface{} {
	return a.val
}

func (a AggrFuncCountN) Distinct() bool {
	return a.distinct
}

//null/missing are ignored.
//add 1 for all other numeric types(non-numeric are ignored)
func (a *AggrFuncCountN) AddDelta(delta interface{}) {

	//ignore if null or missing
	if isNullOrMissing(delta) {
		return
	}

	if isNumeric(delta) {
		if a.distinct {
			if !a.checkDistinct(delta) {
				return
			}
		}
		a.val += 1
	}

}

func (a *AggrFuncCountN) checkDistinct(newval interface{}) bool {

	switch v := newval.(type) {

	case []byte:
		if a.lastEntry == nil {
			a.lastEntry = append([]byte(nil), v...)
		} else {
			if bytes.Compare(v, a.lastEntry.([]byte)) == 0 {
				return false
			}
			a.lastEntry = append(a.lastEntry.([]byte)[:0], v...)
		}

	case value.Value:
		if a.lastEntry != nil && v.EquivalentTo(a.lastEntry.(value.Value)) {
			return false
		}
		a.lastEntry = newval
	}

	return true

}

func (a AggrFuncCountN) String() string {
	return fmt.Sprintf("Type %v Value %v Distinct %v LastVal %v", a.typ, a.val, a.distinct, a.lastEntry)
}

type AggrFuncMin struct {
	typ      AggrFuncType
	val      interface{}
	validVal bool
	distinct bool //ignored
}

func (a AggrFuncMin) Type() AggrFuncType {
	return AGG_MIN
}

func (a AggrFuncMin) Value() interface{} {
	if !a.validVal {
		return encodedNull
	}
	return a.val
}

func (a AggrFuncMin) Distinct() bool {
	return a.distinct
}

func (a *AggrFuncMin) AddDelta(delta interface{}) {

	//ignore if null or missing
	if isNullOrMissing(delta) {
		return
	}

	switch v := delta.(type) {

	case []byte:

		if !a.validVal {
			a.val = append([]byte(nil), v...)
			a.validVal = true
			return
		} else {
			if bytes.Compare(a.val.([]byte), v) > 0 {
				a.val = append(a.val.([]byte)[:0], v...)
			}
		}

	case value.Value:

		if !a.validVal {
			a.val = v
			a.validVal = true
			return
		} else {
			val := a.val.(value.Value)
			if val.Collate(v) > 0 {
				a.val = v
			}
		}

	default:
		//ignored
	}
}

func (a AggrFuncMin) String() string {
	return fmt.Sprintf("Type %v Value %v", a.typ, a.val)
}

type AggrFuncMax struct {
	typ      AggrFuncType
	val      interface{}
	validVal bool
	distinct bool //ignored
}

func (a AggrFuncMax) Type() AggrFuncType {
	return AGG_MAX
}

func (a AggrFuncMax) Value() interface{} {
	if !a.validVal {
		return encodedNull
	}
	return a.val
}

func (a AggrFuncMax) Distinct() bool {
	return a.distinct
}

func (a *AggrFuncMax) AddDelta(delta interface{}) {

	//ignore if null or missing
	if isNullOrMissing(delta) {
		return
	}

	switch v := delta.(type) {

	case []byte:

		if !a.validVal {
			a.val = append([]byte(nil), v...)
			a.validVal = true
			return
		} else {
			if bytes.Compare(a.val.([]byte), v) < 0 {
				a.val = append(a.val.([]byte)[:0], v...)
			}
		}

	case value.Value:

		if !a.validVal {
			a.val = v
			a.validVal = true
			return
		} else {
			val := a.val.(value.Value)
			if val.Collate(v) < 0 {
				a.val = v
			}
		}

	default:
		//ignored
	}
}

func (a AggrFuncMax) String() string {
	return fmt.Sprintf("Type %v Value %v", a.typ, a.val)
}

func isNullOrMissing(val interface{}) bool {

	switch v := val.(type) {

	case []byte:

		//ignore if null or missing
		if v[0] == collatejson.TypeMissing || v[0] == collatejson.TypeNull {
			return true
		}

	case value.Value:

		if v.Type() == value.MISSING || v.Type() == value.NULL {
			return true
		}

	}
	return false
}

func isNumeric(val interface{}) bool {

	switch v := val.(type) {

	case []byte:

		if v[0] == collatejson.TypeNumber {
			return true
		}

	case value.Value:

		if v.Type() == value.NUMBER {
			return true
		}

	}
	return false
}
