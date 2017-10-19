// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package common

type AggrFunc uint32

const (
	AGG_MIN AggrFunc = iota
	AGG_MAX
	AGG_SUM
	AGG_COUNT
	AGG_COUNTN
	AGG_INVALID
)

func (a AggrFunc) String() string {

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
