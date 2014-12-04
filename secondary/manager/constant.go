// Copyright (c) 2014 Couchbase, Inc.

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package manager

import (
	"time"
)

/////////////////////////////////////////////
// Constant for Testing
/////////////////////////////////////////////
const TESTING = true

/////////////////////////////////////////////
// Configurable/Tuning Parameter
/////////////////////////////////////////////

// Common
const NUM_VB = 1024
const DEFAULT_BUCKET_NAME = "Default"
const DEFAULT_POOL_NAME = "default"

// Coordinator
const COORD_MAINT_STREAM_PORT = "9334"
const COORD_INIT_STREAM_PORT = "9335"

// Request Handler
const INDEX_DDL_HTTP_ADDR = ":9102"

// Stream Manager
const COUCHBASE_INTERNAL_BUCKET_URL = "http://localhost:11209/"
const LOCALHOST = "127.0.0.1"
const KV_DCP_PORT = "11210"
const KV_DCP_PORT_CLUSTER_RUN = "12000"
const PROJECTOR_PORT = "9999"

// Timer
const TIME_INTERVAL = time.Duration(2000) * time.Millisecond

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
const MAINT_TOPIC = "MAINT_STREAM_TOPIC"
const CATCHUP_TOPIC = "CATCHUP_STREAM_TOPIC"
const INIT_TOPIC = "INIT_STREAM_TOPIC"

const MAX_PROJECTOR_RETRY_ELAPSED_TIME = int64(time.Minute) * 5

// Timer
const TIMESTAMP_HISTORY_COUNT = 10
const TIMESTAMP_CHANNEL_SIZE = 30
const TIMESTAMP_NOTIFY_CH_SIZE = 100
const TIMESTAMP_PERSIST_INTERVAL = uint64(time.Minute)

// Index Definition
const INDEX_INSTANCE_ID = "IndexInstanceId"
const INDEX_PARTITION_ID = "IndexPartitionId"
