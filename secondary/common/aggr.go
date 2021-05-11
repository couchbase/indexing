// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

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
	AddDeltaObj(delta value.Value)
	AddDeltaRaw(delta []byte)
	Value() interface{}
	Distinct() bool
	IsValid() bool
}

var (
	encodedNull = []byte{2, 0}
)

func NewAggrFunc(typ AggrFuncType, val interface{}, distinct bool, n1qlValue bool) AggrFunc {

	var agg AggrFunc

	switch typ {

	case AGG_SUM:
		agg = &AggrFuncSum{typ: AGG_SUM, distinct: distinct, n1qlValue: n1qlValue}
	case AGG_COUNT:
		agg = &AggrFuncCount{typ: AGG_COUNT, distinct: distinct, n1qlValue: n1qlValue}
	case AGG_COUNTN:
		agg = &AggrFuncCountN{typ: AGG_COUNTN, distinct: distinct, n1qlValue: n1qlValue}
	case AGG_MIN:
		agg = &AggrFuncMin{typ: AGG_MIN, distinct: distinct, n1qlValue: n1qlValue}
	case AGG_MAX:
		agg = &AggrFuncMax{typ: AGG_MAX, distinct: distinct, n1qlValue: n1qlValue}
	default:
		return nil
	}

	if n1qlValue {
		agg.AddDeltaObj(val.(value.Value))
	} else {
		if typ == AGG_SUM {
			agg.AddDelta(val)
		} else {
			agg.AddDeltaRaw(val.([]byte))
		}
	}

	return agg
}

type AggrFuncSum struct {
	typ AggrFuncType

	fval     float64
	fLastVal float64
	ival     int64
	iLastVal int64

	validVal  bool
	distinct  bool
	n1qlValue bool
	isInt64   bool
}

func (a AggrFuncSum) Type() AggrFuncType {
	return AGG_SUM
}

func (a AggrFuncSum) Value() interface{} {
	if !a.validVal {
		return nil
	}

	if a.isInt64 {
		return a.ival
	} else {
		return a.fval
	}
}

func (a AggrFuncSum) Distinct() bool {
	return a.distinct
}

func (a AggrFuncSum) IsValid() bool {
	return a.validVal
}

//Only numeric values are considered.
//null/missing/non-numeric are ignored.
func (a *AggrFuncSum) AddDeltaObj(delta value.Value) {

	actual := delta.ActualForIndex()
	a.AddDelta(actual)

}

//Only numeric values are considered.
//null/missing/non-numeric are ignored.
func (a *AggrFuncSum) AddDelta(delta interface{}) {

	switch v := delta.(type) {

	case float64:
		a.validVal = true

		if a.isInt64 {
			if a.distinct {
				if !a.checkDistinctFloat(v) {
					return
				}
			}
			a.fval = float64(a.ival) + v
		} else {
			if a.distinct {
				if !a.checkDistinctFloat(v) {
					return
				}
			}
			a.fval += v
		}
		a.isInt64 = false

	case int64:

		if !a.isInt64 && a.validVal {
			a.validVal = true
			if a.distinct {
				if !a.checkDistinctInt(v) {
					return
				}
			}
			a.fval += float64(v)
		} else {
			a.validVal = true
			if a.distinct {
				if !a.checkDistinctInt(v) {
					return
				}
			}
			temp := a.ival + v

			//handle overflow (https://blog.regehr.org/archives/1139)
			if (a.ival >= 0 && v >= 0 && temp < 0) ||
				(a.ival < 0 && v < 0 && temp >= 0) {
				a.fval = float64(a.ival) + float64(v)
				a.isInt64 = false
			} else {
				a.ival = temp
				a.isInt64 = true
			}
		}

	case nil, bool, []interface{}, map[string]interface{}, string:
		//ignored

	default:
		//ignored
	}
}

func (a *AggrFuncSum) AddDeltaRaw(delta []byte) {
	//not implemented
}

func (a *AggrFuncSum) checkDistinctFloat(newVal float64) bool {

	if a.fLastVal == newVal {
		return false
	}

	a.fLastVal = newVal
	return true

}

func (a *AggrFuncSum) checkDistinctInt(newVal int64) bool {

	if a.iLastVal == newVal {
		return false
	}

	a.iLastVal = newVal
	return true

}

func (a AggrFuncSum) String() string {
	if a.isInt64 {
		return fmt.Sprintf("Type %v Value %v Distinct %v LastVal %v", a.typ, a.ival, a.distinct, a.iLastVal)
	} else {
		return fmt.Sprintf("Type %v Value %v Distinct %v LastVal %v", a.typ, a.fval, a.distinct, a.fLastVal)
	}
}

type AggrFuncCount struct {
	typ      AggrFuncType
	val      int64
	distinct bool
	lastRaw  []byte
	lastObj  value.Value

	n1qlValue bool
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

func (a AggrFuncCount) IsValid() bool {
	return true
}

func (a *AggrFuncCount) AddDelta(delta interface{}) {
	//not implemented
}

//null/missing are ignored.
//add 1 for all other types
func (a *AggrFuncCount) AddDeltaObj(delta value.Value) {

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

//null/missing are ignored.
//add 1 for all other types
func (a *AggrFuncCount) AddDeltaRaw(delta []byte) {

	//ignore if null or missing
	if isNullOrMissingRaw(delta) {
		return
	}

	if a.distinct {
		if !a.checkDistinctRaw(delta) {
			return
		}
	}
	a.val += 1

}

func (a *AggrFuncCount) checkDistinct(newObj value.Value) bool {

	if a.lastObj != nil && newObj.EquivalentTo(a.lastObj) {
		return false
	}
	a.lastObj = newObj

	return true

}

func (a *AggrFuncCount) checkDistinctRaw(newRaw []byte) bool {

	if a.lastRaw == nil {
		a.lastRaw = append([]byte(nil), newRaw...)
	} else {
		if bytes.Compare(newRaw, a.lastRaw) == 0 {
			return false
		}
		a.lastRaw = append(a.lastRaw[:0], newRaw...)
	}

	return true

}

func (a AggrFuncCount) String() string {
	if a.n1qlValue {
		return fmt.Sprintf("Type %v Value %v Distinct %v LastVal %v", a.typ, a.val, a.distinct, a.lastRaw)
	} else {
		return fmt.Sprintf("Type %v Value %v Distinct %v LastVal %v", a.typ, a.val, a.distinct, a.lastObj)
	}
}

type AggrFuncCountN struct {
	typ      AggrFuncType
	val      int64
	distinct bool
	lastRaw  []byte
	lastObj  value.Value

	n1qlValue bool
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

func (a AggrFuncCountN) IsValid() bool {
	return true
}

func (a *AggrFuncCountN) AddDelta(delta interface{}) {
	//not implemented
}

//null/missing are ignored.
//add 1 for all other numeric types(non-numeric are ignored)
func (a *AggrFuncCountN) AddDeltaObj(delta value.Value) {

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

//null/missing are ignored.
//add 1 for all other numeric types(non-numeric are ignored)
func (a *AggrFuncCountN) AddDeltaRaw(delta []byte) {

	//ignore if null or missing
	if isNullOrMissingRaw(delta) {
		return
	}

	if isNumericRaw(delta) {
		if a.distinct {
			if !a.checkDistinctRaw(delta) {
				return
			}
		}
		a.val += 1
	}

}

func (a *AggrFuncCountN) checkDistinct(newObj value.Value) bool {

	if a.lastObj != nil && newObj.EquivalentTo(a.lastObj) {
		return false
	}
	a.lastObj = newObj

	return true

}

func (a *AggrFuncCountN) checkDistinctRaw(newRaw []byte) bool {

	if a.lastRaw == nil {
		a.lastRaw = append([]byte(nil), newRaw...)
	} else {
		if bytes.Compare(newRaw, a.lastRaw) == 0 {
			return false
		}
		a.lastRaw = append(a.lastRaw[:0], newRaw...)
	}

	return true

}

func (a AggrFuncCountN) String() string {
	if a.n1qlValue {
		return fmt.Sprintf("Type %v Value %v Distinct %v LastVal %v", a.typ, a.val, a.distinct, a.lastRaw)
	} else {
		return fmt.Sprintf("Type %v Value %v Distinct %v LastVal %v", a.typ, a.val, a.distinct, a.lastObj)
	}
}

type AggrFuncMin struct {
	typ AggrFuncType
	raw []byte
	obj value.Value

	validVal  bool
	distinct  bool //ignored
	n1qlValue bool
}

func (a AggrFuncMin) Type() AggrFuncType {
	return AGG_MIN
}

func (a AggrFuncMin) Value() interface{} {
	if !a.validVal {
		return encodedNull
	}

	if a.n1qlValue {
		return a.obj
	} else {
		return a.raw
	}
}

func (a AggrFuncMin) Distinct() bool {
	return a.distinct
}

func (a AggrFuncMin) IsValid() bool {
	return a.validVal
}

func (a *AggrFuncMin) AddDelta(delta interface{}) {
	//not implemented
}

func (a *AggrFuncMin) AddDeltaObj(delta value.Value) {

	//ignore if null or missing
	if isNullOrMissing(delta) {
		return
	}

	if !a.validVal {
		a.obj = delta
		a.validVal = true
		return
	} else {
		if a.obj.Collate(delta) > 0 {
			a.obj = delta
		}
	}

}

func (a *AggrFuncMin) AddDeltaRaw(delta []byte) {

	//ignore if null or missing
	if isNullOrMissingRaw(delta) {
		return
	}

	if !a.validVal {
		a.raw = append([]byte(nil), delta...)
		a.validVal = true
		return
	} else {
		if bytes.Compare(a.raw, delta) > 0 {
			a.raw = append(a.raw[:0], delta...)
		}
	}

}

func (a AggrFuncMin) String() string {
	if a.n1qlValue {
		return fmt.Sprintf("Type %v Value %v", a.typ, a.obj)
	} else {
		return fmt.Sprintf("Type %v Value %v", a.typ, a.raw)
	}
}

type AggrFuncMax struct {
	typ AggrFuncType
	raw []byte
	obj value.Value

	validVal  bool
	distinct  bool //ignored
	n1qlValue bool
}

func (a AggrFuncMax) Type() AggrFuncType {
	return AGG_MAX
}

func (a AggrFuncMax) Value() interface{} {
	if !a.validVal {
		return encodedNull
	}
	if a.n1qlValue {
		return a.obj
	} else {
		return a.raw
	}
}

func (a AggrFuncMax) Distinct() bool {
	return a.distinct
}

func (a AggrFuncMax) IsValid() bool {
	return a.validVal
}

func (a *AggrFuncMax) AddDelta(delta interface{}) {
	//not implemented
}

func (a *AggrFuncMax) AddDeltaObj(delta value.Value) {

	//ignore if null or missing
	if isNullOrMissing(delta) {
		return
	}

	if !a.validVal {
		a.obj = delta
		a.validVal = true
		return
	} else {
		if a.obj.Collate(delta) < 0 {
			a.obj = delta
		}
	}

}

func (a *AggrFuncMax) AddDeltaRaw(delta []byte) {

	//ignore if null or missing
	if isNullOrMissingRaw(delta) {
		return
	}

	if !a.validVal {
		a.raw = append([]byte(nil), delta...)
		a.validVal = true
		return
	} else {
		if bytes.Compare(a.raw, delta) < 0 {
			a.raw = append(a.raw[:0], delta...)
		}
	}

}

func (a AggrFuncMax) String() string {
	if a.n1qlValue {
		return fmt.Sprintf("Type %v Value %v", a.typ, a.obj)
	} else {
		return fmt.Sprintf("Type %v Value %v", a.typ, a.raw)
	}
}

func isNullOrMissing(val value.Value) bool {

	if val.Type() == value.MISSING || val.Type() == value.NULL {
		return true
	}

	return false
}

func isNullOrMissingRaw(val []byte) bool {

	//ignore if null or missing
	if val[0] == collatejson.TypeMissing || val[0] == collatejson.TypeNull {
		return true
	}

	return false
}

func isNumeric(val value.Value) bool {

	if val.Type() == value.NUMBER {
		return true
	}

	return false
}

func isNumericRaw(val []byte) bool {

	if val[0] == collatejson.TypeNumber {
		return true
	}

	return false
}
