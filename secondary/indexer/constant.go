//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package indexer

//Max number of vbuckets supported in the system
const MAX_NUM_VBUCKETS = 1024

//Supervisor's channel capacity to buffer requests
//from workers
const WORKER_MSG_QUEUE_LEN = 1000000

// Intermediate message buffer channel size
const WORKER_RECV_QUEUE_LEN = 10000

// Default cluster address
const DEFAULT_CLUSTER_ENDPOINT = "127.0.0.1:9000"

const LOCALHOST = "127.0.0.1"

//Maintenance Topic Name
const MAINT_TOPIC = "MAINT_STREAM_TOPIC"

//Catchup Topic Name
const CATCHUP_TOPIC = "CATCHUP_STREAM_TOPIC"

//Initial Stream Topic Name
const INIT_TOPIC = "INIT_STREAM_TOPIC"

//Default Pool Name
const DEFAULT_POOL = "default"

//Default Number of Workers started by a stream reader
//to processed incoming mutation. Max can be upto the
//number of vbuckets and minimum must be 1
const DEFAULT_NUM_STREAM_READER_WORKERS = 32

//Max number of snapshot to be retained per index.
//Older snapshots are deleted.
const MAX_SNAPSHOTS_PER_INDEX = 5

//Slab Manager Specific constants
const DEFAULT_START_CHUNK_SIZE = 256
const DEFAULT_SLAB_SIZE = DEFAULT_START_CHUNK_SIZE * 1024
const DEFAULT_MAX_SLAB_MEMORY = DEFAULT_SLAB_SIZE * 1024

//Internal Buffer Size for Each Slice to store incoming
//requests
const SLICE_COMMAND_BUFFER_SIZE = 20000

//Max Length of Secondary Key
const MAX_SEC_KEY_LEN = 4096

const MAX_DOCID_LEN = 256

//Buffer Length for encoded Sec Key
const MAX_SEC_KEY_BUFFER_LEN = MAX_SEC_KEY_LEN * 3

const INDEXER_ID_KEY = "IndexerId"

const INDEXER_STATE_KEY = "IndexerState"

const INDEXER_NODE_UUID = "IndexerNodeUUID"

const MAX_KVWARMUP_RETRIES = 120

const MAX_METAKV_RETRIES = 100
