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
	"fmt"
	"github.com/couchbase/indexing/secondary/collatejson"
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
}

func NewAggrFunc(typ AggrFuncType, val interface{}) AggrFunc {

	var agg AggrFunc

	switch typ {

	case AGG_SUM:
		agg = &AggrFuncSum{typ: AGG_SUM}
	case AGG_COUNT:
		agg = &AggrFuncCount{typ: AGG_COUNT}
	case AGG_MIN:
		agg = &AggrFuncMin{typ: AGG_MIN}
	case AGG_MAX:
		agg = &AggrFuncMax{typ: AGG_MAX}
	default:
		return nil
	}

	agg.AddDelta(val)

	return agg
}

type AggrFuncSum struct {
	typ AggrFuncType
	val float64
}

func (a AggrFuncSum) Type() AggrFuncType {
	return AGG_SUM
}

func (a AggrFuncSum) Value() interface{} {
	return a.val
}

func (a *AggrFuncSum) AddDelta(delta interface{}) {

	switch v := delta.(type) {

	case float64:
		a.val += v

	case int64:
		a.val += float64(v) // TODO: Do not convert. Support SUM for both int64 and float64

	case nil:
		//no-op

	case string:

		var m collatejson.Missing
		if m.Equal(v) {
			//no-op
		} else {
			//ignore
		}

	default:
		//ignore
	}
}

func (a AggrFuncSum) String() string {
	return fmt.Sprintf("Type %v Value %v", a.typ, a.val)
}

type AggrFuncCount struct {
	typ AggrFuncType
	val float64
}

func (a AggrFuncCount) Type() AggrFuncType {
	return AGG_COUNT
}

func (a AggrFuncCount) Value() interface{} {
	return a.val
}

func (a *AggrFuncCount) AddDelta(delta interface{}) {

	switch v := delta.(type) {

	case float64:
		a.val += 1

	case nil:
		//no-op

	case string:

		var m collatejson.Missing
		if m.Equal(v) {
			//no-op
		} else {
			a.val += 1
		}

	default:
		//ignore
	}
}

func (a AggrFuncCount) String() string {
	return fmt.Sprintf("Type %v Value %v", a.typ, a.val)
}

type AggrFuncMin struct {
	typ AggrFuncType
	val float64
}

func (a AggrFuncMin) Type() AggrFuncType {
	return AGG_MIN
}

func (a AggrFuncMin) Value() interface{} {
	return a.val
}

func (a *AggrFuncMin) AddDelta(delta interface{}) {

	switch v := delta.(type) {

	case float64:
		if v < a.val {
			a.val = v
		}

	case nil:
		//no-op

	case string:

		var m collatejson.Missing
		if m.Equal(v) {
			//no-op
		} else {
			//ignore
		}

	default:
		//ignore
	}
}

func (a AggrFuncMin) String() string {
	return fmt.Sprintf("Type %v Value %v", a.typ, a.val)
}

type AggrFuncMax struct {
	typ AggrFuncType
	val float64
}

func (a AggrFuncMax) Type() AggrFuncType {
	return AGG_MAX
}

func (a AggrFuncMax) Value() interface{} {
	return a.val
}

func (a *AggrFuncMax) AddDelta(delta interface{}) {

	switch v := delta.(type) {

	case float64:
		if v > a.val {
			a.val = v
		}

	case nil:
		//no-op

	case string:

		var m collatejson.Missing
		if m.Equal(v) {
			//no-op
		} else {
			//ignore
		}

	default:
		//ignore
	}
}

func (a AggrFuncMax) String() string {
	return fmt.Sprintf("Type %v Value %v", a.typ, a.val)
}
