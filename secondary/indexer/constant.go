//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package indexer

//Max number of vbuckets supported in the system
const MAX_NUM_VBUCKETS = 1024

//Supervisor's channel capacity to buffer requests
//from workers
const WORKER_MSG_QUEUE_LEN = 1000000

// Intermediate message buffer channel size
const WORKER_RECV_QUEUE_LEN = 10000

// Default cluster address
// This can ONLY be used for indexer main as default value for command line argument.
const DEFAULT_CLUSTER_ENDPOINT = "127.0.0.1:9000"

//Maintenance Topic Name
const MAINT_TOPIC = "MAINT_STREAM_TOPIC"

//Catchup Topic Name
const CATCHUP_TOPIC = "CATCHUP_STREAM_TOPIC"

//Initial Stream Topic Name
const INIT_TOPIC = "INIT_STREAM_TOPIC"

//Default Pool Name
const DEFAULT_POOL = "default"

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

const MAX_DOCID_LEN = 256

const ENCODE_BUF_SAFE_PAD = 512

// Constants for unlimiting entry size
const DEFAULT_MAX_SEC_KEY_LEN = 512
const DEFAULT_MAX_SEC_KEY_LEN_SCAN = 4608
const DEFAULT_MAX_ARRAY_KEY_SIZE = 1024
const MAX_KEY_EXTRABYTES_LEN = MAX_DOCID_LEN + 2
const RESIZE_PAD = 1024

const INDEXER_ID_KEY = "IndexerId"

const INDEXER_STATE_KEY = "IndexerState"

const INDEXER_NODE_UUID = "IndexerNodeUUID"

const MAX_KVWARMUP_RETRIES = 120

const MAX_METAKV_RETRIES = 100

const PLASMA_MEMQUOTA_FRAC = 0.9

const SCAN_ROLLBACK_ERROR_BATCHSIZE = 1000

const MAX_PROJ_RETRY = 20

// Constants for stats persistence in snapshot meta
const SNAPSHOT_META_VERSION_MOI_1 = 1
const SNAPSHOT_META_VERSION_PLASMA_1 = 1
const SNAP_STATS_KEY_SIZES = "key_size_dist"
const SNAP_STATS_ARRKEY_SIZES = "arrkey_size_dist"
const SNAP_STATS_KEY_SIZES_SINCE = "key_size_stats_since"
const SNAP_STATS_RAW_DATA_SIZE = "raw_data_size"
const SNAP_STATS_BACKSTORE_RAW_DATA_SIZE = "backstore_raw_data_size"
const SNAP_STATS_ARR_ITEMS_COUNT = "arr_items_count"
