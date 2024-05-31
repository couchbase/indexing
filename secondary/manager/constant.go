// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package manager

import (
	"time"
)

// ///////////////////////////////////////////
// Constant for Testing
// ///////////////////////////////////////////
const TESTING = true

/////////////////////////////////////////////
// Configurable/Tuning Parameter
/////////////////////////////////////////////

// Common
var NUM_VB = 1024

const DEFAULT_BUCKET_NAME = "default"
const DEFAULT_POOL_NAME = "default"

// Coordinator
const COORD_MAINT_STREAM_PORT = ":9334"
const COORD_INIT_STREAM_PORT = ":9335"

// Stream Manager
const COUCHBASE_INTERNAL_BUCKET_URL = "http://localhost:11209/"
const KV_DCP_PORT = "11210"
const KV_DCP_PORT_CLUSTER_RUN = "12000"
const PROJECTOR_PORT = "9999"

// Timer (2s)
var TIME_INTERVAL = time.Duration(2000) * time.Millisecond

// Stream Monitor (2m)
var MONITOR_INTERVAL = time.Duration(120000) * time.Millisecond

/////////////////////////////////////////////
// Constant
/////////////////////////////////////////////

// Common
const HTTP_PREFIX = "http://"

// Coordinator
const COORDINATOR_CONFIG_STORE = "IndexCoordinatorConfigStore"

// Event Manager
const DEFAULT_EVT_QUEUE_SIZE = 20
const DEFAULT_NOTIFIER_QUEUE_SIZE = 5

// Stream Manager
const MAINT_TOPIC = "MAINT"
const CATCHUP_TOPIC = "CATCHUP"
const INIT_TOPIC = "INIT"

const MAX_PROJECTOR_RETRY_ELAPSED_TIME = int64(time.Minute) * 5

// Timer
const TIMESTAMP_HISTORY_COUNT = 10
const TIMESTAMP_CHANNEL_SIZE = 30
const TIMESTAMP_NOTIFY_CH_SIZE = 100
const TIMESTAMP_PERSIST_INTERVAL = uint64(time.Minute)

// Index Definition
const INDEX_INSTANCE_ID = "IndexInstanceId"
const INDEX_PARTITION_ID = "IndexPartitionId"
