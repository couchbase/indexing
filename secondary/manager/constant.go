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

// common
const NUM_VB = 1024

// Coordinator configurable parameter
const COORDINATOR_CONFIG_STORE = "IndexCoordinatorConfigStore"
const COORD_MAINT_STREAM_PORT = "5334"
const COORD_INIT_STREAM_PORT = "5335"

// Event Manager configurable parameter
const DEFAULT_EVT_QUEUE_SIZE = 20
const DEFAULT_NOTIFIER_QUEUE_SIZE = 5

// Request Handler configurable parameter
const INDEX_DDL_HTTP_ADDR = ":9102"

// Stream Proxy configurable parameter
const HTTP_PREFIX = "http://"
const COUCHBASE_INTERNAL_BUCKET_URL = "http://localhost:11209/"
const COUCHBASE_DEFAULT_POOL_NAME = "default"

const LOCALHOST = "127.0.0.1"
const KV_DCP_PORT = "11210"
const KV_DCP_PORT_CLUSTER_RUN = "12000"
const PROJECTOR_PORT = "9999"

const MAINT_TOPIC = "MAINT_STREAM_TOPIC"
const CATCHUP_TOPIC = "CATCHUP_STREAM_TOPIC"
const INIT_TOPIC = "INIT_STREAM_TOPIC"

const MAX_TOPIC_REQUEST_RETRY_COUNT = 3

// Timer configurable parameter
const TIMESTAMP_HISTORY_COUNT = 10
const TIME_INTERVAL = time.Duration(2000) * time.Millisecond
const TIMESTAMP_CHANNEL_SIZE = 30
const TIMESTAMP_NOTIFY_CH_SIZE = 100
