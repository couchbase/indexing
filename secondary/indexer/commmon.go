//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package indexer

const MAX_NUM_VBUCKETS = 1024

//Stream represents the possible mutation streams
type StreamId uint16

const (
	MAINT_STREAM StreamId = iota
	MAINT_CATCHUP_STREAM
	BACKFILL_STREAM
	BACKFILL_CATCHUP_STREAM
	MAX_STREAMS
)

// a generic channel which can be closed when you
// want someone to stop doing something
type StopChannel chan bool

// a generic channel which can be closed when you
// want to indicate the caller that you are done
type DoneChannel chan bool

type MsgChannel chan Message
