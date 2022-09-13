// Config is key, value map for system level and component configuration.
// Key is a string and represents a config parameter, and corresponding
// value is an interface{} that can be consumed using accessor methods
// based on the context of config-value.
//
// Config maps are immutable and newer versions can be created using accessor
// methods.
//
// Shape of config-parameter, the key string, is sequence of alpha-numeric
// characters separated by one or more '.' , eg,
//      "projector.adminport.readtimeout"

package common

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/indexing/secondary/logging"
)

// NOTE:
// following settings are related to each other.
//      "projector.adminport.readTimeout",
//      "projector.dataport.harakiriTimeout",
//      "indexer.dataport.tcpReadDeadline",
//
// configurations for underprovisioned nodes,
//		"projector.feedWaitStreamReqTimeout": 300 * 1000,
//		"projector.feedWaitStreamEndTimeout": 300 * 1000,
//		"projector.dataport.harakiriTimeout": 300 * 1000,
//		"indexer.dataport.tcpReadDeadline": 300 * 1000

// formula to compute the default CPU allocation for projector.
var projector_maxCpuPercent = int(math.Max(400.0, float64(runtime.GOMAXPROCS(0))*100.0*0.25))
var Plasma_minNumShard = uint64(math.Max(2.0, float64(runtime.GOMAXPROCS(0))*0.25))

// Threadsafe config holder object
type ConfigHolder struct {
	ptr unsafe.Pointer
}

func (h *ConfigHolder) Store(conf Config) {
	atomic.StorePointer(&h.ptr, unsafe.Pointer(&conf))
}

func (h *ConfigHolder) Load() Config {
	confptr := atomic.LoadPointer(&h.ptr)
	return *(*Config)(confptr)
}

// Config is a key, value map with key always being a string
// represents a config-parameter.
type Config map[string]ConfigValue

// ConfigValue for each parameter.
type ConfigValue struct {
	Value         interface{}
	Help          string
	DefaultVal    interface{}
	Immutable     bool
	Casesensitive bool
}

// SystemConfig is default configuration for system and components.
// configuration parameters follow flat namespacing like,
//      "maxVbuckets"  for system-level config parameter
// 	Example:
// 		"maxVbuckets": ConfigValue{
//			1024,
//			"number of vbuckets configured in KV",
//			1024,
//			true,  // immutable
//			false, // case-insensitive
//		},
//      "projector.xxx" for projector component.
//      "projector.adminport.xxx" for adminport under projector component.
// etc...
var SystemConfig = Config{
	// system parameters
	// security parameters
	// Note: If we set this to false and set node to node encryption level to all we will
	// need both TLS and NonTLS versions for ports that are accessed both from local host
	// and remote host. Eg: Adminport connections from local host might fail as they will
	// not use TLS where as server will be configured to use TLS connections.
	"security.encryption.encryptLocalhost": ConfigValue{
		true,
		"enable encryption on local host",
		true,
		true,  // immutable
		false, // case-insensitive
	},
	// projector parameters
	"projector.name": ConfigValue{
		"projector",
		"human readable name for this projector",
		"projector",
		true,  // immutable
		false, // case-insensitive
	},
	"projector.clusterAddr": ConfigValue{

		"localhost:9000",
		"KV cluster's address to be used by projector",
		"localhost:9000",
		true,  // immutable
		false, // case-insensitive
	},
	"projector.maxCpuPercent": ConfigValue{
		projector_maxCpuPercent,
		"Maximum percent of CPU that projector can use. " +
			"EG, 200% in 4-core (400%) machine would set indexer to " +
			"use 2 cores",
		projector_maxCpuPercent,
		false, // mutable
		false, // case-insensitive
	},
	"projector.memstatTick": ConfigValue{
		1 * 60 * 1000, // in milli-second, 1 minute
		"in milli-second, periodically log runtime memory-stats for projector.",
		1 * 60 * 1000,
		false, // mutable
		false, // case-insensitive
	},
	// Projector feed settings
	"projector.routerEndpointFactory": ConfigValue{
		RouterEndpointFactory(nil),
		"RouterEndpointFactory callback to generate endpoint instances " +
			"to push data to downstream",
		RouterEndpointFactory(nil),
		true,  // immutable
		false, // case-insensitive
	},
	"projector.feedWaitStreamReqTimeout": ConfigValue{
		300 * 1000,
		"timeout, in milliseconds, to await a response for StreamRequest",
		300 * 1000, // 300s
		false,      // mutable
		false,      // case-insensitive
	},
	"projector.feedWaitStreamEndTimeout": ConfigValue{
		300 * 1000,
		"timeout, in milliseconds, to await a response for StreamEnd",
		300 * 1000, // 300s
		false,      // mutable
		false,      // case-insensitive
	},
	"projector.mutationChanSize": ConfigValue{
		150,
		"channel size of projector's vbucket workers, " +
			"changing this value does not affect existing feeds.",
		150,
		false, // mutable
		false, // case-insensitive
	},
	"projector.encodeBufSize": ConfigValue{
		2 * 1024, // 2KB
		"Collatejson encode buffer size",
		2 * 1024, // 2KB
		false,    // mutable
		false,    // case-insensitive
	},
	"projector.encodeBufResizeInterval": ConfigValue{
		60, // 60 Minutes
		"Period (in Minutes) with which projector would resize its encodeBuf",
		60,    // 60 Minutes,
		false, // mutable
		false, // case-insensitive
	},
	"projector.feedChanSize": ConfigValue{
		100,
		"channel size for feed's control path, " +
			"changing this value does not affect existing feeds.",
		100,
		false, // mutable
		false, // case-insensitive
	},
	"projector.backChanSize": ConfigValue{
		50000,
		"channel size of projector feed's back-channel, " +
			"changing this value does not affect existing feeds.",
		50000,
		false, // mutable
		false, // case-insensitive
	},
	"projector.vbucketWorkers": ConfigValue{
		64,
		"number of workers handling the vbuckets",
		64,
		false, // mutable
		false, // case-insensitive
	},
	"projector.syncTimeout": ConfigValue{
		20000,
		"timeout, in milliseconds, for sending periodic Sync messages, " +
			"changing this value does not affect existing feeds.",
		20000,
		false, // mutable
		false, // case-insensitive
	},
	"projector.watchInterval": ConfigValue{
		5 * 60 * 1000, // 5 minutes
		"periodic tick, in milli-seconds to check for stale feeds, " +
			"a feed is considered stale when all its endpoint go stale.",
		5 * 60 * 1000,
		true,  // immutable
		false, // case-insensitive
	},
	"projector.staleTimeout": ConfigValue{
		5 * 60 * 1000, // 5 minutes
		"timeout, in milli-seconds to wait for response for feed's genserver" +
			"feed will be force-shutdown if timeout expires",
		5 * 60 * 1000,
		true,  // immutable
		false, // case-insensitive
	},
	"projector.cpuProfDir": ConfigValue{
		"",
		"Directory at which cpu_profile will be generated for projector." +
			"Name of the generated cpu_profile file: projector_cpu.pprof",
		"",
		false, // mutable
		true,  // case-sensitive
	},
	"projector.cpuProfile": ConfigValue{
		false,
		"boolean indicate whether to start or stop projector cpu profiling.",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"projector.memProfDir": ConfigValue{
		"",
		"Directory at which mem-profile will be generated for projector." +
			"Name of the generated mem_profile file: projector_mem.pprof",
		"",
		false, // mutable
		true,  // case-sensitive
	},
	"projector.memProfile": ConfigValue{
		false,
		"boolean to take current mem profile from projector.",
		false,
		false, // mutable
		false, // case-insensitive
	},
	// projector dcp parameters
	"projector.dcp.genChanSize": ConfigValue{
		2048,
		"channel size for DCP's gen-server routine, " +
			"changing this value does not affect existing feeds.",
		2048,
		false, // mutable
		false, // case-insensitive
	},
	"projector.dcp.dataChanSize": ConfigValue{
		1000,
		"channel size for DCP's data path routines, " +
			"changing this value does not affect existing feeds.",
		1000,
		false, // mutable
		false, // case-insensitive
	},
	"projector.dcp.numConnections": ConfigValue{
		4,
		"connect with N concurrent DCP connection with KV",
		4,
		false, // mutable
		false, // case-insensitive
	},
	"projector.dcp.latencyTick": ConfigValue{
		1 * 60 * 1000, // 1 minute
		"in milliseconds, periodically log cumulative stats of dcp latency",
		1 * 60 * 1000, // 1 minute
		false,         // mutable
		false,         // case-insensitive
	},
	"projector.dcp.activeVbOnly": ConfigValue{
		true,
		"request dcp to process active vbuckets only",
		true,
		false, // mutable
		false, // case-insensitive
	},
	// projector adminport parameters
	"projector.adminport.name": ConfigValue{
		"projector.adminport",
		"human readable name for this adminport, must be supplied",
		"projector.adminport",
		true,  // immutable
		false, // case-insensitive
	},
	"projector.adminport.listenAddr": ConfigValue{
		"",
		"projector's adminport address listen for request.",
		"",
		true,  // immutable
		false, // case-insensitive
	},
	"projector.adminport.urlPrefix": ConfigValue{
		"/adminport/",
		"url prefix (script-path) for adminport used by projector",
		"/adminport/",
		true,  // immutable
		false, // case-insensitive
	},
	"projector.adminport.readTimeout": ConfigValue{
		30 * 1000,
		"timeout in milliseconds, is http server's read timeout, " +
			"also refer to projector.dataport.harakiriTimeout and " +
			"indexer.dataport.tcpReadDeadline",
		30 * 1000,
		true,  // immutable
		false, // case-insensitive
	},
	"projector.adminport.readHeaderTimeout": ConfigValue{
		5 * 1000,
		"timeout in milliseconds, is http server's read header timeout, " +
			"also refer to projector.dataport.harakiriTimeout, " +
			"projector.adminport.readTimeout and indexer.dataport.tcpReadDeadline",
		5 * 1000,
		true,  // immutable
		false, // case-insensitive
	},
	"projector.adminport.writeTimeout": ConfigValue{
		0,
		"timeout in milliseconds, is http server's write timeout",
		0,
		true,  // immutable
		false, // case-insensitive
	},
	"projector.adminport.maxHeaderBytes": ConfigValue{
		1 << 20, // 1 MegaByte
		"in bytes, is max. length of adminport http header",
		1 << 20, // 1 MegaByte
		true,    // immutable
		false,   // case-insensitive
	},
	// projector dataport client parameters
	"projector.dataport.remoteBlock": ConfigValue{
		true,
		"should dataport endpoint block when remote is slow, " +
			"does not affect existing feeds.",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"projector.dataport.keyChanSize": ConfigValue{
		5000,
		"channel size of dataport endpoints data input, " +
			"does not affect existing feeds.",
		5000,
		false, // mutable
		false, // case-insensitive
	},
	"projector.dataport.bufferSize": ConfigValue{
		100,
		"number of entries to buffer before flushing it, where each entry " +
			"is for a vbucket's set of mutations that was flushed, " +
			"by the endpoint, does not affect existing feeds.",
		100,
		false, // mutable
		false, // case-insensitive
	},
	"projector.dataport.bufferTimeout": ConfigValue{
		1, // 1ms
		"timeout in milliseconds, to flush vbucket-mutations from, " +
			"endpoint, does not affect existing feeds.",
		1,     // 1ms
		false, // mutable
		false, // case-insensitive
	},
	"projector.dataport.harakiriTimeout": ConfigValue{
		300 * 1000,
		"timeout in milliseconds, after which endpoint will commit harakiri " +
			"if not active, does not affect existing feeds, " +
			"also refer to projector.adminport.readTimeout and " +
			"indexer.dataport.tcpReadDeadline.",
		300 * 1000, //300s
		false,      // mutable
		false,      // case-insensitive
	},
	"projector.dataport.maxPayload": ConfigValue{
		1024 * 1024,
		"maximum payload length, in bytes, for transmission data from " +
			"router to downstream client, does not affect eixting feeds.",
		1024 * 1024, // 1MB
		true,        // immutable
		false,       // case-insensitive
	},
	"projector.statsLogDumpInterval": ConfigValue{
		60, // 1 minute
		"in seconds, periodically log stats of all projector components",
		60,    // 1 minute
		false, // mutable
		false, // case-insensitive
	},
	"projector.vbseqnosLogIntervalMultiplier": ConfigValue{
		5, // 5 * statsLogDumpInterval
		"Number of cycles (each cycle with statsLogDumpInterval seconds) to wait before logging vbseqnos",
		5,     // 5 * statsLogDumpInterval
		false, // mutable
		false, // case-insensitive
	},
	"projector.evalStatLoggingThreshold": ConfigValue{
		200, // 200 microseconds
		"Threshold after which index evaluator stats will be logged in projector logs (In microseconds)",
		200,   // 200 microseconds
		false, // mutable
		false, // case-insensitive
	},
	"projector.systemStatsCollectionInterval": ConfigValue{
		5, // 5 seconds
		"The period with which projector updates the system level stats",
		5,     // 5 seconds
		false, // mutable
		false, // case-insensitive
	},
	"projector.usedMemThreshold": ConfigValue{
		0.5, // 0.5 or 50%
		"Projector starts to take memory management decisions if " +
			"the overall used memory (across all processes) in the system " +
			"goes above this fraction of total availble memory in the system",
		0.5,   // 0.5 or 50%
		false, // mutable
		false, // case-insensitive
	},
	"projector.forceGCOnThreshold": ConfigValue{
		true,
		"When set to true, projector forces a GC if RSS > 16% of memTotal and " +
			" heapIdle is significant when compared to heapReleased and " +
			"heapIdle-heapReleased > 30% of RSS",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"projector.rssThreshold": ConfigValue{
		0.1, // 0.1 or 10%
		"Projector starts to take memory management decisions if " +
			"the process RSS is greater than this fraction of  " +
			"total availble memory in the system",
		0.1,   // 0.1 or 10%
		false, // mutable
		false, // case-insensitive
	},
	"projector.relaxGCThreshold": ConfigValue{
		0.01, // 0.01 or 1%
		"Projector will relax GC percent if RSS is below this value. " +
			"Set to '0' to disable relaxing GC percentage",
		0.01,  // 0.01 or 1%
		false, // mutable
		false, // case-insensitive
	},
	"projector.memThrottle": ConfigValue{
		true,
		"Slows down ingestion from DCP feed if the projector " +
			"RSS goes beyond projector.rssThreshold value. Throttles " +
			"both INIT and MAINT streams by default",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"projector.memThrottle.init_build.start_level": ConfigValue{
		0,
		"Ranges from 0-10.  if value is >=10, defaulted to 10. Defaulted to 0, if value <= 0. " +
			"Projector initiates throttling for this stream when the throttle level " +
			"computed by memManager reaches this value",
		0,
		false, // mutable
		false, // case-insensitive
	},
	"projector.memThrottle.incr_build.start_level": ConfigValue{
		0,
		"Ranges from 0-10.  if value is >=10, defaulted to 10. Defaulted to 0, if value <= 0. " +
			"Projector initiates throttling for this stream when the throttle level " +
			"computed by memManager reaches this value",
		0,
		false, // mutable
		false, // case-insensitive
	},
	"projector.maintStreamMemThrottle": ConfigValue{
		true,
		"When set to false, disables the throttling on MAINT_STREAM",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"projector.gogc": ConfigValue{
		100, // 100 percent
		"set GOGC percent",
		100,
		false, // mutable
		false, // case-insensitive
	},
	"projector.memcachedTimeout": ConfigValue{
		120, // In Seconds
		"Timeout for projector to memcached communication (In Seconds)",
		120,
		false, // mutable
		false, // case-insensitive
	},
	// projector's adminport client, can be used by manager
	"manager.projectorclient.retryInterval": ConfigValue{
		16,
		"retryInterval, in milliseconds when connection refused by server",
		16,
		true,  // immutable
		false, // case-insensitive
	},
	"manager.projectorclient.maxRetries": ConfigValue{
		5,
		"maximum number of times to retry",
		5,
		true,  // immutable
		false, // case-insensitive
	},
	"manager.projectorclient.exponentialBackoff": ConfigValue{
		2,
		"multiplying factor on retryInterval for every attempt with server",
		2,
		true,  // immutable
		false, // case-insensitive
	},
	"manager.projectorclient.urlPrefix": ConfigValue{
		"/adminport/",
		"url prefix (script-path) for adminport used by projector",
		"/adminport/",
		true,  // immutable
		false, // case-insensitive
	},
	// indexer dataport parameters
	"indexer.dataport.genServerChanSize": ConfigValue{
		1024,
		"request channel size of indexer dataport's gen-server routine",
		1024,
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.dataport.dataChanSize": ConfigValue{
		250,
		"request channel size of indexer dataport's gen-server routine",
		250,
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.dataport.maxPayload": ConfigValue{
		1000 * 1024,
		"maximum payload length, in bytes, for receiving data from router",
		1000 * 1024, // bytes
		true,        // immutable
		false,       // case-insensitive
	},
	"indexer.dataport.tcpReadDeadline": ConfigValue{
		300 * 1000,
		"timeout, in milliseconds, while reading from socket, " +
			"also refer to projector.adminport.readTimeout and " +
			"projector.dataport.harakiriTimeout.",
		300 * 1000, // 300s
		false,      // mutable
		false,      // case-insensitive
	},
	"indexer.dataport.enableAuth": ConfigValue{
		true,
		"force authentication for dataport server",
		true,
		false, // mutable
		false, // case-insensitive
	},
	// indexer queryport configuration
	"indexer.queryport.maxPayload": ConfigValue{
		64 * 1024,
		"maximum payload, in bytes, for receiving data from client",
		64 * 1024,
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.queryport.readDeadline": ConfigValue{
		4000,
		"timeout, in milliseconds, is timeout while reading from socket",
		4000,
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.queryport.writeDeadline": ConfigValue{
		4000,
		"timeout, in milliseconds, is timeout while writing to socket",
		4000,
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.queryport.pageSize": ConfigValue{
		1,
		"number of index-entries that shall be returned as single payload",
		1,
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.queryport.streamChanSize": ConfigValue{
		16,
		"size of the buffered channels used to stream request and response.",
		16,
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.queryport.keepAliveInterval": ConfigValue{
		1,
		"keep alive interval to set on query port connections",
		1,
		false, // immutable
		false, // case-insensitive
	},
	// queryport client configuration
	"queryport.client.maxPayload": ConfigValue{
		1000 * 1024,
		"maximum payload, in bytes, for receiving data from server",
		1000 * 1024,
		true,  // immutable
		false, // case-insensitive
	},
	"queryport.client.readDeadline": ConfigValue{
		120000,
		"timeout, in milliseconds, is timeout while reading from socket",
		120000,
		true,  // immutable
		false, // case-insensitive
	},
	"queryport.client.writeDeadline": ConfigValue{
		4000,
		"timeout, in milliseconds, is timeout while writing to socket",
		4000,
		true,  // immutable
		false, // case-insensitive
	},
	"queryport.client.settings.poolSize": ConfigValue{
		5000,
		"number of simultaneous active connections in a pool",
		5000,
		true,  // immutable
		false, // case-insensitive
	},
	"queryport.client.settings.minPoolSizeWM": ConfigValue{
		1000,
		"Minimum number of simultaneous active connections in a pool." +
			" Once reached, never gets reduced below this value.",
		1000,
		true,  // immutable
		false, // case-insensitive
	},
	"queryport.client.settings.relConnBatchSize": ConfigValue{
		100,
		"Number of connection to be released in a single iteration",
		100,
		true,  // immutable
		false, // case-insensitive
	},
	"queryport.client.settings.poolOverflow": ConfigValue{
		30,
		"maximum number of connections in a pool",
		30,
		true,  // immutable
		false, // case-insensitive
	},
	"queryport.client.connPoolTimeout": ConfigValue{
		1000,
		"timeout, in milliseconds, is timeout for retrieving a connection " +
			"from the pool",
		1000,
		true,  // immutable
		false, // case-insensitive
	},
	"queryport.client.connPoolAvailWaitTimeout": ConfigValue{
		1,
		"timeout, in milliseconds, to wait for an existing connection " +
			"from the pool before considering the creation of a new one",
		1,
		true,  // immutable
		false, // case-insensitive
	},
	"queryport.client.retryScanPort": ConfigValue{
		2,
		"number of times to retry when scanport is not detectable",
		2,
		true,  // immutable
		false, // case-insensitive
	},
	"queryport.client.retryIntervalScanport": ConfigValue{
		10,
		"wait, in milliseconds, before re-trying for a scanport",
		10,
		true,  // immutable
		false, // case-insensitive
	},
	"queryport.client.servicesNotifierRetryTm": ConfigValue{
		1000,
		"wait, in milliseconds, before restarting the ServicesNotifier",
		1000,
		true,  // immutable
		false, // case-insensitive
	},
	"queryport.client.logtick": ConfigValue{
		60 * 1000, // 1 minutes
		"tick, in milliseconds, to log queryport client's statistics",
		60 * 1000,
		true,  // immutable
		false, // case-insensitive
	},
	"queryport.client.load.randomWeight": ConfigValue{
		0.9,
		"random weightage between [0, 1.0) for random load-balancing, " +
			"lower the value less likely for random load-balancing",
		0.9,
		true,  // immutable
		false, // case-insensitive
	},
	"queryport.client.load.equivalenceFactor": ConfigValue{
		0.1,
		"normalization factor on replica's avg-load to group them with " +
			"least loaded replica.",
		0.1,
		true,  // immutable
		false, // case-insensitive
	},
	"queryport.client.settings.backfillLimit": ConfigValue{
		5 * 1024, // 5GB
		"limit in mega-bytes to cap n1ql side backfilling, if ZERO backfill " +
			"will be disabled.",
		5 * 1024, // 5GB
		false,    // mutable
		false,    // case-insensitive
	},
	"queryport.client.scanLagPercent": ConfigValue{
		0.2,
		"allowed threshold on mutation lag from fastest replica during scan, " +
			"representing as a percentage of pending mutations from fastest replica,",
		0.2,
		false, // immutable
		false, // case-insensitive
	},
	"queryport.client.scanLagItem": ConfigValue{
		100000,
		"allowed threshold on mutation lag from fastest replica during scan, " +
			"representing as a number of pending mutations from fastest replica,",
		100000,
		false, // immutable
		false, // case-insensitive
	},
	"queryport.client.disable_prune_replica": ConfigValue{
		false,
		"disable client to filter our replica based on stats",
		false,
		false, // immutable
		false, // case-insensitive
	},
	"queryport.client.scan.queue_size": ConfigValue{
		0,
		"When performing scan scattering in client, specify the queue size for the scatterer.",
		0,
		false, // mutable
		false, // case-insensitive
	},
	"queryport.client.log_level": ConfigValue{
		"info", // keep in sync with index_settings_manager.erl
		"GsiClient logging level",
		"info",
		false, // mutable
		false, // case-insensitive
	},
	"queryport.client.scan.max_concurrency": ConfigValue{
		0,
		"When performing query on partitioned index, specify maximum concurrency allowed. Use 0 to disable.",
		0,
		false, // mutable
		false, // case-insensitive
	},
	"queryport.client.allowCJsonScanFormat": ConfigValue{
		true,
		"Allow collatejson as data format between queryport client and indexer.",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"queryport.client.usePlanner": ConfigValue{
		true,
		"boolean flag to direct whether to use planner for non-partitioned index",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"queryport.client.listSchedIndexes": ConfigValue{
		true,
		"When true, queryport client will return the list of indexes scheduled for creation.",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"queryport.client.restRequestTimeout": ConfigValue{
		120, // value
		"timeout, in seconds, is timeout for REST calls in GetWithCbauth Function of Index Planner.", //help
		120,   // default
		false, // mutable
		false, // case-insensitive
	},
	"queryport.client.keepAliveInterval": ConfigValue{
		1, // value
		"client side keepalive interval, in seconds, to fast close the scan client conection on failover",
		1,     // default
		false, // mutable
		false, // case-insensitive
	},
	"queryport.client.waitForScheduledIndex": ConfigValue{
		true,
		"Do not return the index creation request until the scheduled index is created",
		true,
		false,
		false,
	},
	"indexer.allowPartialQuorum": ConfigValue{
		false,
		"This boolean flag, when set, allows index creation with partial quorum. " +
			"This will be allowed only when \"with nodes\" is specified during " +
			"index creation. By default, this flag is false and will require full " +
			"quorum i.e. all indexer nodes in the cluster should allow index creation.",
		false,
		false, // mutable
		false, // case-insensitive
	},
	// projector's adminport client, can be used by indexer.
	"indexer.projectorclient.retryInterval": ConfigValue{
		16,
		"retryInterval, in milliseconds when connection refused by server",
		16,
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.projectorclient.maxRetries": ConfigValue{
		5,
		"maximum number of times to retry",
		5,
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.projectorclient.exponentialBackoff": ConfigValue{
		2,
		"multiplying factor on retryInterval for every attempt with server",
		2,
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.projectorclient.urlPrefix": ConfigValue{
		"/adminport/",
		"url prefix (script-path) for adminport used by projector",
		"/adminport/",
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.adminPort": ConfigValue{
		"9100",
		"port for index ddl and status operations",
		"9100",
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.scanPort": ConfigValue{
		"9101",
		"port for index scan operations",
		"9101",
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.httpPort": ConfigValue{
		"9102",
		"http port for external stats amd settings",
		"9102",
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.httpsPort": ConfigValue{
		"",
		"https port for external stats and settings",
		"",
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.certFile": ConfigValue{
		"",
		"X.509 certificate PEM string for accepting external TLS connections",
		"",
		true, // immutable
		true, // case-sensitive
	},
	"indexer.keyFile": ConfigValue{
		"",
		"X.509 private key PEM string for accepting external TLS connections",
		"",
		true, // immutable
		true, // case-sensitive
	},
	"indexer.clientCertFile": ConfigValue{
		"",
		"X.509 certificate PEM string for making internal TLS connections",
		"",
		true, // immutable
		true, // case-sensitive
	},
	"indexer.clientKeyFile": ConfigValue{
		"",
		"X.509 private key PEM string for making internal TLS connections",
		"",
		true, // immutable
		true, // case-sensitive
	},
	"indexer.caFile": ConfigValue{
		"",
		"ssl ca file",
		"",
		true, // immutable
		true, // case-sensitive
	},
	"indexer.isEnterprise": ConfigValue{
		true,
		"enterprise edition",
		true,
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.deploymentModel": ConfigValue{ // Storing for storage config
		"",
		"deployment model [serverless|default]",
		"default",
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.isIPv6": ConfigValue{
		false,
		"is cluster in IPv6 mode",
		false,
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.streamInitPort": ConfigValue{
		"9103",
		"port for inital build stream",
		"9103",
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.streamCatchupPort": ConfigValue{
		"9104",
		"port for catchup stream",
		"9104",
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.streamMaintPort": ConfigValue{
		"9105",
		"port for maintenance stream",
		"9105",
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.clusterAddr": ConfigValue{
		"127.0.0.1:8091",
		"Local cluster manager address",
		"127.0.0.1:8091",
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.numPartitions": ConfigValue{
		8,
		"Number of vbuckets",
		8,
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.numSnapshotWorkers": ConfigValue{
		runtime.GOMAXPROCS(0) * 10,
		"Number of workers each keyspaceId in a stream will spawn to create snapshots",
		runtime.GOMAXPROCS(0) * 10,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.enableManager": ConfigValue{
		false,
		"Enable index manager",
		false,
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.storage_dir": ConfigValue{
		"./",
		"Index file storage directory",
		"./",
		true, // immutable
		true, // case-sensitive
	},
	"indexer.diagnostics_dir": ConfigValue{
		"./",
		"Index diagnostics information directory",
		"./",
		true, // immutable
		true, // case-sensitive
	},
	"indexer.log_dir": ConfigValue{
		"",
		"Index log directory",
		"",
		true, // immutable
		true, // case-sensitive
	},
	"indexer.nodeuuid": ConfigValue{
		"",
		"Indexer node UUID",
		"",
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.numSliceWriters": ConfigValue{
		runtime.GOMAXPROCS(0),
		"Number of Writer Threads for a Slice",
		runtime.GOMAXPROCS(0),
		true,  // immutable
		false, // case-insensitive
	},

	"indexer.sync_period": ConfigValue{
		uint64(2000),
		"Stream message sync interval in millis",
		uint64(2000),
		true,  // immutable
		false, // case-insensitive
	},

	"indexer.client_stats_refresh_interval": ConfigValue{
		uint64(5000),
		"Periodic interval (in milliseconds) at which indexer broadcasts stats to GSI client",
		uint64(5000),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.stats_cache_timeout": ConfigValue{
		uint64(30000),
		"Stats cache ttl in millis",
		uint64(30000),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.statsPersistenceInterval": ConfigValue{
		uint64(900),
		"Periodic stats persistence interval in seconds. Value of 0 disables persistence",
		uint64(900),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.statsPersistenceChunkSize": ConfigValue{
		20 * 1024 * 1024,
		"Size of individual chunk while persisting stats in bytes",
		20 * 1024 * 1024,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.memstats_cache_timeout": ConfigValue{
		uint64(60000),
		"Memstats cache ttl in millis",
		uint64(60000),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.max_parallel_collection_builds": ConfigValue{
		10,
		"Maximum number of collections that can be built simultaneously." +
			"Note: This setting does not limit the number of indexes that are being " +
			"built in each collection",
		10,
		false, // mutable
		false, // case-insensitive
	},

	//fdb specific config
	"indexer.stream_reader.fdb.syncBatchInterval": ConfigValue{
		uint64(40),
		"Batching Interval for sync messages generated by " +
			"stream reader in millis",
		uint64(40),
		false, // mutable
		false, // case-insensitive
	},

	"indexer.stream_reader.fdb.workerBuffer": ConfigValue{
		uint64(500),
		"Buffer Size for stream reader worker to hold mutations " +
			"before being enqueued in mutation queue",
		uint64(500),
		false, // mutable
		false, // case-insensitive
	},

	"indexer.stream_reader.fdb.mutationBuffer": ConfigValue{
		uint64(250),
		"Buffer Size to hold incoming mutations from dataport",
		uint64(250),
		false, // mutable
		false, // case-insensitive
	},

	"indexer.stream_reader.fdb.numWorkers": ConfigValue{
		1,
		"Number of stream reader workers to read from dataport",
		1,
		false, // mutable
		false, // case-insensitive
	},

	"indexer.storage.fdb.commitPollInterval": ConfigValue{
		uint64(10),
		"Time in milliseconds for a slice to poll for " +
			"any outstanding writes before commit",
		uint64(10),
		false, // mutable
		false, // case-insensitive
	},

	"indexer.mutation_queue.fdb.allocPollInterval": ConfigValue{
		uint64(30),
		"time in milliseconds to try for new alloc " +
			"if mutation queue is full.",
		uint64(30),
		false, // mutable
		false, // case-insensitive
	},

	//moi specific config
	"indexer.stream_reader.moi.syncBatchInterval": ConfigValue{
		uint64(3),
		"Batching Interval for sync messages generated by " +
			"stream reader in millis",
		uint64(3),
		false, // mutable
		false, // case-insensitive
	},

	"indexer.stream_reader.moi.workerBuffer": ConfigValue{
		uint64(500),
		"Buffer Size for stream reader worker to hold mutations " +
			"before being enqueued in mutation queue",
		uint64(500),
		false, // mutable
		false, // case-insensitive
	},

	"indexer.stream_reader.moi.mutationBuffer": ConfigValue{
		uint64(250),
		"Buffer Size to hold incoming mutations from dataport",
		uint64(250),
		false, // mutable
		false, // case-insensitive
	},

	"indexer.stream_reader.moi.numWorkers": ConfigValue{
		32,
		"Number of stream reader workers to read from dataport",
		32,
		false, // mutable
		false, // case-insensitive
	},

	"indexer.storage.moi.commitPollInterval": ConfigValue{
		uint64(1),
		"Time in milliseconds for a slice to poll for " +
			"any outstanding writes before commit",
		uint64(1),
		false, // mutable
		false, // case-insensitive
	},

	"indexer.mutation_queue.moi.allocPollInterval": ConfigValue{
		uint64(1),
		"time in milliseconds to try for new alloc " +
			"if mutation queue is full.",
		uint64(1),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.moi.useMemMgmt": ConfigValue{
		true,
		"Use jemalloc based manual memory management",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.moi.useDeltaInterleaving": ConfigValue{
		true,
		"Use delta interleaving mode for on-disk snapshots",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.moi.exposeItemCopy": ConfigValue{
		false,
		"Expose item copy from storage to GSI during scans and mutations",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.useMutationSyncPool": ConfigValue{
		false,
		"Use sync pool for mutations",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.disablePersistence": ConfigValue{
		false,
		"Disable persistence",
		false,
		false,
		false,
	},
	"indexer.plasma.enforceKeyRange": ConfigValue{
		true,
		"Enforce key range check when a page is mutated or compacted",
		true,
		false,
		false,
	},
	"indexer.plasma.flushBufferSize": ConfigValue{
		1024 * 1024,
		"Flush buffer size for dedicated instance data log",
		1024 * 1024,
		false,
		false,
	},
	"indexer.plasma.recoveryFlushBufferSize": ConfigValue{
		256 * 1024,
		"Flush buffer size for dedicated instance recovery log",
		256 * 1024,
		false,
		false,
	},
	"indexer.plasma.sharedFlushBufferSize": ConfigValue{
		1024 * 1024,
		"Flush buffer size for shared instance data log",
		1024 * 1024,
		false,
		false,
	},
	"indexer.plasma.sharedRecoveryFlushBufferSize": ConfigValue{
		1024 * 1024,
		"Flush buffer size for shared instance recovery log",
		1024 * 1024,
		false,
		false,
	},
	"indexer.plasma.useMemMgmt": ConfigValue{
		true,
		"Use jemalloc based manual memory management",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.numReaders": ConfigValue{
		runtime.GOMAXPROCS(0) * 3,
		"Numbers of readers for plasma",
		runtime.GOMAXPROCS(0) * 3,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.useCompression": ConfigValue{
		true,
		"Enable compression for plasma",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.compression": ConfigValue{
		"snappy",
		"Compression algorithm",
		"snappy",
		false, // mutable
		true,  // case-sensitive
	},
	"indexer.plasma.inMemoryCompression": ConfigValue{
		"zstd",
		"Compression algorithm for in memory compression",
		"zstd",
		false, // mutable
		true,  // case-sensitive
	},
	"indexer.plasma.persistenceCPUPercent": ConfigValue{
		50,
		"Percentage of cpu used for persistence",
		50,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.evictionCPUPercent": ConfigValue{
		50,
		"Percentage of cpu used for plasma evictions",
		50,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.memFragThreshold": ConfigValue{
		float64(0.15),
		"Percentage of memory fragmentation",
		float64(0.15),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.LSSSegmentFileSize": ConfigValue{
		0,
		"LSS log segment maxsize per file",
		0,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.LSSReclaimBlockSize": ConfigValue{
		64 * 1024 * 1024,
		"Space reclaim granularity for LSS log",
		64 * 1024 * 1024,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.LSSCleanerConcurrency": ConfigValue{
		4,
		"Number of concurrent threads used by the cleaner",
		4,
		false, // mutable,
		false, // case-insensitive
	},
	"indexer.plasma.AutoTuneLSSCleaner": ConfigValue{
		false,
		"Enable auto tuning of lss cleaning thresholds based on available free space",
		false,
		false, // mutable,
		false, // case-insensitive
	},
	"indexer.plasma.AutoTuneDiskQuota": ConfigValue{
		uint64(0),
		"Disk Quota for LSS frag ratio tuning",
		uint64(0),
		false, // mutable,
		false, // case-insensitive
	},
	"indexer.plasma.AutoTuneCleanerTargetFragRatio": ConfigValue{
		50,
		"Target LSS Cleaner fragmentation ratio for auto tuning",
		50,
		false, // mutable,
		false, // case-insensitive
	},
	"indexer.plasma.AutoTuneCleanerMinBandwidthRatio": ConfigValue{
		float64(0.1),
		"Minimum bandwidth (percentage) allocated for LSS cleaning with auto tuning",
		float64(0.1),
		false, // mutable,
		false, // case-insensitive
	},
	"indexer.plasma.AutoTuneDiskFullTimeLimit": ConfigValue{
		3600,
		"time allowance (in second) before disk is full",
		3600,
		false, // mutable,
		false, // case-insensitive
	},
	"indexer.plasma.AutoTuneAvailDiskLimit": ConfigValue{
		float64(0.9),
		"percentage of available disk space reserved for plasma",
		float64(0.9),
		false, // mutable,
		false, // case-insensitive
	},
	"indexer.plasma.MaxSMRWorkerPerCore": ConfigValue{
		uint64(2),
		"Max number of SMR Worker per core per context",
		uint64(2),
		false,
		false,
	},
	"indexer.plasma.MaxSMRInstPerCtx": ConfigValue{
		uint64(100),
		"max number of instances per SMR context",
		uint64(100),
		false,
		false,
	},
	"indexer.plasma.BufMemQuotaRatio": ConfigValue{
		float64(0.3),
		"max limit of memory quota used for buffer",
		float64(0.3),
		false, // mutable,
		false, // case-insensitive
	},
	"indexer.plasma.MaxPageSize": ConfigValue{
		192 * 1024,
		"Used with AutoTuneLSSCleaner; target page size limit",
		192 * 1024,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.useMmapReads": ConfigValue{
		false,
		"Use mmap for reads",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.useDirectIO": ConfigValue{
		false,
		"Use direct io mode",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.EnableContainerSupport": ConfigValue{
		true,
		"Use sigar for getting memory stats",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.maxNumPageDeltas": ConfigValue{
		200,
		"Maximum number of page deltas",
		200,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.pageSplitThreshold": ConfigValue{
		400,
		"Threshold for triggering page split",
		400,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.pageMergeThreshold": ConfigValue{
		25,
		"Threshold for triggering page merge",
		25,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.maxLSSPageSegments": ConfigValue{
		4,
		"Maximum number of page segments on LSS for a page",
		4,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.maxLSSFragmentation": ConfigValue{
		80,
		"Desired max LSS fragmentation percent",
		80,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.LSSFragmentation": ConfigValue{
		30,
		"Desired LSS fragmentation percent",
		30,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.enablePeriodicEvict": ConfigValue{
		true,
		"Enable Periodic Eviction",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.evictMinThreshold": ConfigValue{
		0.5,
		"Minimum memory use for periodic eviction to run. When memory use over min threshold," +
			" eviction will not run if plasma estimates all indexes can fit into quota.",
		0.5,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.evictMaxThreshold": ConfigValue{
		0.9,
		"Maximum memory use for periodic eviction to run.  Once reach max threshold," +
			" periodic eviction will run regardless if it is going to run into DGM or not.",
		0.9,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.evictDirtyOnPersistRatio": ConfigValue{
		1.2,
		"Evict dirty page during persistence when memory usage is over given ratio. " +
			"This allows write-only working set to be evicted before reaching quota. " +
			"Note that persist interval is usually large (10 min), so it should not " +
			"cause any performance impact even for hot wrrite-only working set.",
		1.2,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.evictDirtyPercent": ConfigValue{
		0.95,
		"Memory usage relative to quota for enabling periodic evict to purge dirty page",
		0.95,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.evictSweepInterval": ConfigValue{
		300,
		"Time interval to sweep through all pages in an index (in sec)",
		300,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.evictSweepIntervalIncrementDuration": ConfigValue{
		0,
		"Time interval for sweep interval to be incremented to max value (in sec)",
		0,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.evictRunInterval": ConfigValue{
		100,
		"Minimum elapsed time between each run for swapper to sweep pages (in millisecond)",
		100,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.evictUseMemEstimate": ConfigValue{
		true,
		"enable eviction to estimate if index memory can fit into quota",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.enableInMemoryCompression": ConfigValue{
		false,
		"Enable compression of memory resident items",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.enableCompressDuringBurst": ConfigValue{
		false,
		"Enable compression of memory resident items during burst eviction also",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.enableDecompressDuringSwapin": ConfigValue{
		false,
		"Enable decompression of compressed items during swapin",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.compressBeforeEvictPercent": ConfigValue{
		0,
		"Percent of compressible items to compress before eviction",
		0,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.enableCompressAfterSwapin": ConfigValue{
		true,
		"Compress items that are read from disk into memory",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.compressMemoryThresholdPercent": ConfigValue{
		95,
		"Percent of quota that memory usage should be greater than to compress after swapin",
		95,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.enableCompressFullMarshal": ConfigValue{
		true,
		"Compress page after full marshal",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.enablePageBloomFilter": ConfigValue{
		false,
		"Enable maintenance and use of bloom filter for lookup of swapped out items",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.bloomFilterFalsePositiveRate": ConfigValue{
		0.15,
		"The target false positive rate for bloom filter. A smaller fpRate will make bloom filters consume more memory.",
		0.15,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.mainIndex.bloomFilterExpectedMaxItems": ConfigValue{
		uint64(220),
		"The maximum number of items we expect to insert into each bloom filter. This is based on MaxPageItems and MaxDeltaChainLen.",
		uint64(220),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.maxNumPageDeltas": ConfigValue{
		30,
		"Maximum number of page deltas",
		30,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.pageSplitThreshold": ConfigValue{
		300,
		"Threshold for triggering page split",
		300,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.pageMergeThreshold": ConfigValue{
		5,
		"Threshold for triggering page merge",
		5,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.maxLSSPageSegments": ConfigValue{
		4,
		"Maximum number of page segments on LSS for a page",
		4,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.maxLSSFragmentation": ConfigValue{
		80,
		"Desired max LSS fragmentation percent",
		80,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.LSSFragmentation": ConfigValue{
		30,
		"Desired LSS fragmentation percent",
		30,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.enablePeriodicEvict": ConfigValue{
		true,
		"Enable Periodic Eviction",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.evictMinThreshold": ConfigValue{
		0.5,
		"See indexer.plasma.mainIndex.evictMinThreshold",
		0.5,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.evictMaxThreshold": ConfigValue{
		0.9,
		"See indexer.plasma.mainIndex.evictMaxThreshold",
		0.9,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.evictDirtyOnPersistRatio": ConfigValue{
		1.2,
		"see indexer.plasma.mainIndex.evictDirtyOnPersistRatio",
		1.2,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.evictDirtyPercent": ConfigValue{
		0.95,
		"Memory usage relative to quota for enabling periodic evict to purge dirty page",
		0.95,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.evictSweepInterval": ConfigValue{
		300,
		"Time interval to sweep through all pages in an index (in sec)",
		300,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.evictSweepIntervalIncrementDuration": ConfigValue{
		0,
		"Time interval for sweep interval to be incremented to max value (in sec)",
		0,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.evictRunInterval": ConfigValue{
		100,
		"Minimum elapsed time between each run for swapper to sweep pages (in millisecond)",
		100,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.evictUseMemEstimate": ConfigValue{
		true,
		"enable eviction to estimate if index memory can fit into quota",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.enableInMemoryCompression": ConfigValue{
		false,
		"Enable compression of memory resident items",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.enableCompressDuringBurst": ConfigValue{
		false,
		"Enable compression of memory resident items during burst eviction also",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.enableDecompressDuringSwapin": ConfigValue{
		false,
		"Enable decompression of compressed items during swapin",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.compressBeforeEvictPercent": ConfigValue{
		0,
		"Percent of compressible items to compress before eviction",
		0,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.enableCompressAfterSwapin": ConfigValue{
		true,
		"Compress items that are read from disk into memory",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.compressMemoryThresholdPercent": ConfigValue{
		95,
		"Percent of quota that memory usage should be greater than to compress after swapin",
		95,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.enableCompressFullMarshal": ConfigValue{
		true,
		"Compress page after full marshal",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.enable_page_bloom_filter": ConfigValue{
		false, // keep in sync with index_settings_manager.erl and indexer.plasma.backIndex.enablePageBloomFilter
		"Enable maintenance and use of bloom filter for lookup of swapped out items",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.enablePageBloomFilter": ConfigValue{
		false,
		"Enable maintenance and use of bloom filter for lookup of swapped out items",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.bloomFilterFalsePositiveRate": ConfigValue{
		0.15,
		"The target false positive rate for bloom filter. A smaller fpRate will make bloom filters consume more memory.",
		0.15,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.backIndex.bloomFilterExpectedMaxItems": ConfigValue{
		uint64(220),
		"The maximum number of items we expect to insert into each bloom filter. This is based on MaxPageItems and MaxDeltaChainLen.",
		uint64(220),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.disableReadCaching": ConfigValue{
		false,
		"Disable read caching",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.UseQuotaTuner": ConfigValue{
		true,
		"Enable memquota tuner",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.writer.tuning.enable": ConfigValue{
		false,
		"Tune number of writers dynamically",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.writer.tuning.adjust.interval": ConfigValue{
		100,
		"Interval to check if writers needs to be adjusted (millis)",
		100,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.writer.tuning.sampling.interval": ConfigValue{
		20,
		"Sampling interval for stats (millis)",
		20,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.writer.tuning.sampling.window": ConfigValue{
		5000,
		"Duration for which stats is kept for sampling (millis)",
		5000,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.writer.tuning.throughput.scalingFactor": ConfigValue{
		float64(0.2),
		"Scaling factoring for minimum percentage increase on drain rate after expanding writer",
		float64(0.2),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.writer.tuning.throttling.threshold": ConfigValue{
		10,
		"Number of misses on minimum drain rate before throttling writers",
		10,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.memtuner.maxFreeMemory": ConfigValue{
		1024 * 1024 * 1024 * 8,
		"Max free memory",
		1024 * 1024 * 1024 * 8,
		false,
		false,
	},
	"indexer.plasma.memtuner.minFreeRatio": ConfigValue{
		float64(0.10),
		"Minimum free memory ratio",
		float64(0.10),
		false,
		false,
	},
	"indexer.plasma.memtuner.overshootRatio": ConfigValue{
		float64(0.15),
		"Allow RSS to overshoot quota by this ratio",
		float64(0.15),
		false,
		false,
	},
	"indexer.plasma.memtuner.trimDownRatio": ConfigValue{
		float64(0.10),
		"Memtuner trimdown ratio",
		float64(0.10),
		false,
		false,
	},
	"indexer.plasma.memtuner.incrementRatio": ConfigValue{
		float64(0.01),
		"Memtuner increment ratio",
		float64(0.01),
		false,
		false,
	},
	"indexer.plasma.memtuner.minQuotaRatio": ConfigValue{
		float64(0.20),
		"Memtuner min quota ratio",
		float64(0.20),
		false,
		false,
	},
	"indexer.plasma.memtuner.incrCeilPercent": ConfigValue{
		float64(3),
		"Memtuner increment ceiling percent",
		float64(3),
		false,
		false,
	},
	"indexer.plasma.memtuner.minQuota": ConfigValue{
		1024 * 1024 * 1024,
		"Memtuner minimum quota",
		1024 * 1024 * 1024,
		false,
		false,
	},
	"indexer.plasma.fbtuner.enable": ConfigValue{
		false,
		"Enable Auto LSS FlushBuffer Tuning",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.fbtuner.minQuotaRatio": ConfigValue{
		float64(0.10),
		"Minimum ratio between flush buffer memory to plasma current memory quota tuner tries to keep",
		float64(0.10),
		false,
		false,
	},
	"indexer.plasma.fbtuner.adjustRate": ConfigValue{
		float64(0.10),
		"used by tuner to compute increment/decrement sizes in a readjustment cycle",
		float64(0.10),
		false,
		false,
	},
	"indexer.plasma.fbtuner.adjustInterval": ConfigValue{
		270, // swapper eviction timeout is 300 secs
		"interval (in seconds) at which tuner performs readjustments",
		270,
		false,
		false,
	},
	"indexer.plasma.fbtuner.lssSampleInterval": ConfigValue{
		180,
		"interval (in seconds) at which LSS offsets are sampled by tuner for checking write activity",
		180,
		false,
		false,
	},
	"indexer.plasma.fbtuner.debug": ConfigValue{
		false,
		"enable tuner debug logging",
		false,
		false,
		false,
	},
	"indexer.plasma.purger.enabled": ConfigValue{
		true,
		"Enable mvcc page purger",
		true,
		false,
		false,
	},
	"indexer.plasma.purger.interval": ConfigValue{
		60,
		"Purger purge_ratio check interval in seconds",
		60,
		false,
		false,
	},
	"indexer.plasma.purger.highThreshold": ConfigValue{
		float64(10),
		"Purger high threshold",
		float64(10),
		false,
		false,
	},
	"indexer.plasma.purger.lowThreshold": ConfigValue{
		float64(7),
		"Purger low threshold",
		float64(7),
		false,
		false,
	},
	"indexer.plasma.purger.compactRatio": ConfigValue{
		float64(0.5),
		"Max ratio of pages to be scanned during a purge attempt",
		float64(0.5),
		false,
		false,
	},
	"indexer.plasma.reader.purge.enabled": ConfigValue{
		true,
		"Enable mvcc purge from reader",
		true,
		false,
		false,
	},
	"indexer.plasma.reader.purge.threshold": ConfigValue{
		float64(5),
		"Reader Purge Threshold.  Initialized to 5 (20% valid record per page).",
		float64(5),
		false,
		false,
	},
	"indexer.plasma.reader.purge.pageRatio": ConfigValue{
		float64(0.25),
		" number of items in a page relative to (active max page items + active delta chain length)",
		float64(0.25),
		false,
		false,
	},
	"indexer.plasma.reader.hole.minPages": ConfigValue{
		uint64(10),
		"minimum number of contiguous empty pages needed to be a hole",
		uint64(10),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.holecleaner.enabled": ConfigValue{
		true,
		"Enable hole cleaning activity",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.holecleaner.cpuPercent": ConfigValue{
		10,
		"Maximum percentage of cpu used for hole cleaning." +
			"eg, 10% in 80 core machine can use up to 8 cores",
		10,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.holecleaner.interval": ConfigValue{
		uint64(60),
		"interval in seconds to check if any hole cleaning activity is needed",
		uint64(60),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.holecleaner.maxPages": ConfigValue{
		uint64(128 * 1000),
		"upper limit on the number of empty pages processed in a single hole cleaning cycle",
		uint64(128 * 1000),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.enablePageChecksum": ConfigValue{
		true, // Value set
		"Checksum every page to enable corruption detection",
		true,  // Default value
		false, // Mutable but effective upon restart
		false, // Case-insensitive
	},
	"indexer.plasma.enableLSSPageSMO": ConfigValue{
		true,
		"Enable page structure modification in lss",
		true,
		false,
		false,
	},
	"indexer.plasma.PageStatsSamplePercent": ConfigValue{
		float64(0.1),
		"percentage of total pages used in page stats computation",
		float64(0.1),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.logReadAheadSize": ConfigValue{
		1024 * 1024,
		"Log read ahead size",
		1024 * 1024,
		false,
		false,
	},

	"indexer.plasma.checkpointInterval": ConfigValue{
		600,
		"Fast recovery checkpoint interval in seconds",
		600,
		false,
		false,
	},
	"indexer.plasma.maxInstancePerShard": ConfigValue{
		uint64(100),
		"Maximum number of instances per shard",
		uint64(100),
		false,
		false,
	},
	"indexer.plasma.maxDiskUsagePerShard": ConfigValue{
		uint64(250 * 1024 * 1024 * 1024),
		"Maximum disk usage per shard",
		uint64(250 * 1024 * 1024 * 1024),
		false,
		false,
	},
	"indexer.plasma.minNumShard": ConfigValue{
		Plasma_minNumShard,
		"Minimum number of shard",
		Plasma_minNumShard,
		false,
		false,
	},
	"indexer.plasma.stats.runInterval": ConfigValue{
		uint64(15 * 60),
		"stats logger run interval (second)",
		uint64(15 * 60),
		false,
		false,
	},
	"indexer.plasma.stats.logInterval": ConfigValue{
		uint64(0),
		"stats logger log interval (second)",
		uint64(0),
		false,
		false,
	},
	"indexer.plasma.stats.threshold.keySize": ConfigValue{
		uint64(1000),
		"logging threshold based on average key size",
		uint64(1000),
		false,
		false,
	},
	"indexer.plasma.stats.threshold.percentile": ConfigValue{
		float64(0.9),
		"logging threshold based on percentile on key stats",
		float64(0.9),
		false,
		false,
	},
	"indexer.plasma.stats.threshold.numInstances": ConfigValue{
		100,
		"minimum of instances to enable threshold based logging",
		100,
		false,
		false,
	},
	"indexer.plasma.stats.logger.fileName": ConfigValue{
		"plasma_stats.log",
		"name of the plasma stats file",
		"plasma_stats.log",
		false,
		false,
	},
	"indexer.plasma.stats.logger.fileSize": ConfigValue{
		uint64(32 * 1024 * 1024),
		"size of the plasma stats file",
		uint64(32 * 1024 * 1024),
		false,
		false,
	},
	"indexer.plasma.stats.logger.fileCount": ConfigValue{
		uint64(10),
		"number of the plasma stats file",
		uint64(10),
		false,
		false,
	},
	"indexer.plasma.recovery.checkpointInterval": ConfigValue{
		uint64(15 * time.Minute),
		"max interval for recovery log checkpoint",
		uint64(15 * time.Minute),
		false,
		false,
	},
	"indexer.plasma.recovery.enableFullReplayOnError": ConfigValue{
		true,
		"enable full data replay upon error",
		true,
		false,
		false,
	},

	"indexer.stream_reader.plasma.workerBuffer": ConfigValue{
		uint64(500),
		"Buffer Size for stream reader worker to hold mutations " +
			"before being enqueued in mutation queue",
		uint64(500),
		false, // mutable
		false, // case-insensitive
	},

	"indexer.stream_reader.plasma.mutationBuffer": ConfigValue{
		uint64(250),
		"Buffer Size to hold incoming mutations from dataport",
		uint64(250),
		false, // mutable
		false, // case-insensitive
	},

	"indexer.dataport.plasma.dataChanSize": ConfigValue{
		250,
		"request channel size of indexer dataport's gen-server routine",
		250,
		false, // mutable
		false, // case-insensitive
	},

	//end of plasma specific config

	"indexer.mutation_queue.dequeuePollInterval": ConfigValue{
		uint64(1),
		"time in milliseconds to wait before retrying the dequeue " +
			"if mutations are not available in queue.",
		uint64(1),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.mutation_queue.resultChanSize": ConfigValue{
		uint64(20),
		"size of buffered result channel returned by " +
			"mutation queue on dequeue",
		uint64(20),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.memstatTick": ConfigValue{
		60, // in second
		"in second, periodically log runtime memory-stats.",
		60,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.high_mem_mark": ConfigValue{
		0.95,
		"Fraction of memory_quota above which Indexer moves " +
			"to paused state",
		0.95,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.low_mem_mark": ConfigValue{
		0.8,
		"Once Indexer goes to Paused state, it becomes Active " +
			"only after mem_usage reaches below this fraction of memory_quota",
		0.8,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.pause_if_memory_full": ConfigValue{
		true,
		"Indexer goes to Paused when memory_quota is exhausted(moi only)",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.min_oom_memory": ConfigValue{
		uint64(256 * 1024 * 1024),
		"Minimum memory_quota below which Indexer doesn't go to Paused state",
		uint64(256 * 1024 * 1024),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.allow_scan_when_paused": ConfigValue{
		true,
		"stale=ok scans are allowed when Indexer is in Paused state",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.enable_session_consistency_strict": ConfigValue{
		false,
		"enable strict session consistency to handle rollback scenarios for consistent scans",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.strict_consistency_check_threshold": ConfigValue{
		5000,
		"Criterion to determine if there is significant KV rollback based on which strict consistency is turned on",
		5000,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.force_gc_mem_frac": ConfigValue{
		0.1,
		"Fraction of memory_quota left after which GC is forced " +
			"by Indexer. Only applies to moi.",
		0.1,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.mutation_manager.fdb.fracMutationQueueMem": ConfigValue{
		0.2,
		"Fraction of memory_quota allocated to Mutation Queue",
		0.2,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.mutation_manager.moi.fracMutationQueueMem": ConfigValue{
		0.1,
		"Fraction of memory_quota allocated to Mutation Queue",
		0.1,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.mutation_manager.maxQueueMem": ConfigValue{
		uint64(1 * 256 * 1024 * 1024),
		"Max memory used by the mutation queue",
		uint64(1 * 256 * 1024 * 1024),
		false,
		false,
	},
	"indexer.settings.gc_percent": ConfigValue{
		100,
		"(GOGC) Ratio of current heap size over heap size from last GC." +
			" Value must be positive integer.",
		100,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.mem_usage_check_interval": ConfigValue{
		10,
		"Time inteval in seconds after which Indexer will check " +
			"for memory_usage and do Pause/Unpause if required." +
			"This also determines how often GC is forced. Please see " +
			"force_gc_mem_frac setting. Only applies to moi.",
		10,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.timekeeper.monitor_flush": ConfigValue{
		true,
		"Debug option to enable monitoring flush in timekeeper." +
			"If a flush doesn't complete for 60secs, additional debug info " +
			"will be logged",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.enableAsyncOpenStream": ConfigValue{
		true,
		"Enable async stream open operation between indexer and projector",
		true,
		true,  // mutable
		false, // case-insensitive
	},
	"indexer.timekeeper.rollback.StreamBeginWaitTime": ConfigValue{
		30, // 30 minutes
		"Max wait time after the last received stream begin (in second) before rollback takes place during stream repair. ",
		30,
		true,  // mutable
		false, // case-insensitive
	},
	"indexer.timekeeper.escalate.StreamBeginWaitTime": ConfigValue{
		30 * 60, // 30 minutes
		"Max wait time after the last received stream begin (in second) escalate to the next repair action during stream repair. ",
		30 * 60,
		true,  // mutable
		false, // case-insensitive
	},
	"indexer.timekeeper.streamRepairWaitTime": ConfigValue{
		60, // 1 minute
		"Wait time between retrying stream repair (in second)",
		60,
		true,  // mutable
		false, // case-insensitive
	},
	"indexer.http.readTimeout": ConfigValue{
		30,
		"timeout in seconds, is indexer http server's read timeout",
		30,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.http.readHeaderTimeout": ConfigValue{
		5,
		"timeout in seconds, is indexer http server's read timeout to read request headers",
		5,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.http.writeTimeout": ConfigValue{
		1200,
		"timeout in seconds, is indexer http server's write timeout",
		1200,
		false, // mutable
		false, // case-insensitive
	},

	// Indexer dynamic settings
	"indexer.settings.compaction.check_period": ConfigValue{
		30,
		"Compaction poll interval in seconds",
		30,
		false, // mutable
		false, // case-insensitive
	},

	"indexer.settings.compaction.interval": ConfigValue{
		"00:00,00:00",
		"Compaction allowed interval",
		"00:00,00:00",
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.compaction.min_frag": ConfigValue{
		30,
		"Compaction fragmentation threshold percentage",
		30,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.compaction.min_size": ConfigValue{
		uint64(1024 * 1024 * 500),
		"Compaction min file size",
		uint64(1024 * 1024 * 500),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.compaction.compaction_mode": ConfigValue{
		"circular",
		"compaction mode (circular, full)",
		"circular",
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.compaction.days_of_week": ConfigValue{
		"",
		"Days of the week to run full compaction (Sunday, Monday, ...)",
		"",
		false, // mutable
		true,  // case-sensitive
	},
	"indexer.settings.compaction.abort_exceed_interval": ConfigValue{
		false,
		"Abort full compaction if exceeding compaction interval",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.compaction.plasma.manual": ConfigValue{
		false,
		"Enable plasma manual compaction",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.compaction.plasma.optional.min_frag": ConfigValue{
		20,
		"Compaction fragmentation threshold percentage for optional compaction",
		20,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.compaction.plasma.optional.decrement": ConfigValue{
		5,
		"Compaction fragmentation decrement percentage for optional compaction",
		5,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.compaction.plasma.optional.quota": ConfigValue{
		25,
		"Percentage of plasma instances eligible for optional compaction",
		25,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.persisted_snapshot.interval": ConfigValue{
		uint64(5000), // keep in sync with index_settings_manager.erl
		"Persisted snapshotting interval in milliseconds",
		uint64(5000),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.persisted_snapshot_init_build.interval": ConfigValue{
		uint64(5000),
		"Persisted snapshotting interval in milliseconds for initial build",
		uint64(5000),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.inmemory_snapshot.interval": ConfigValue{
		uint64(200), // keep in sync with index_settings_manager.erl
		"InMemory snapshotting interval in milliseconds",
		uint64(200),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.moi.recovery.max_rollbacks": ConfigValue{
		2,
		"Maximum number of committed rollback points",
		2,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.plasma.recovery.max_rollbacks": ConfigValue{
		2,
		"Maximum number of committed rollback points",
		2,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.recovery.max_rollbacks": ConfigValue{
		5, // keep in sync with index_settings_manager.erl
		"Maximum number of committed rollback points",
		5,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.memory_quota": ConfigValue{
		uint64(256 * 1024 * 1024),
		"Maximum memory used by the indexer buffercache",
		uint64(256 * 1024 * 1024),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.cgroup.memory_quota": ConfigValue{
		uint64(0),
		"Linux cgroup override of indexer.settings.memory_quota;" +
			" 0 if cgroups are not supported",
		uint64(0),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.max_cpu_percent": ConfigValue{
		0,
		"Maximum percent of CPU that indexer can use. " +
			"EG, 200% in 4-core (400%) machine would set indexer to " +
			"use 2 cores. 0 means use all available cores.",
		0,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.cgroup.max_cpu_percent": ConfigValue{
		0,
		"Linux cgroup override of indexer.settings.max_cpu_percent;" +
			" 0 if cgroups are not supported",
		0,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.log_level": ConfigValue{
		"info", // keep in sync with index_settings_manager.erl
		"Indexer logging level",
		"info",
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.scan_timeout": ConfigValue{
		120000,
		"timeout, in milliseconds, timeout for index scan processing",
		120000,
		true,  // immutable
		false, // case-insensitive
	},
	"indexer.settings.eTagPeriod": ConfigValue{
		240,
		"Average ETag expiration period in seconds",
		240,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.max_array_seckey_size": ConfigValue{
		10240,
		"Maximum size of secondary index key size for array index",
		10240,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.max_seckey_size": ConfigValue{
		4608,
		"Maximum size of secondary index key",
		4608,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.allow_large_keys": ConfigValue{
		true,
		"Allow indexing of large index items",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.send_buffer_size": ConfigValue{
		1024,
		"Buffer size for batching rows during scan result streaming",
		1024,
		true,  // immutable
		false, // case-insensitive
	},

	"indexer.settings.cpuProfDir": ConfigValue{
		"",
		"Directory at which cpu-profile will be generated for indexer." +
			"Name of the generated cpu_profile file: indexer_cpu.pprof",
		"",
		false, // mutable
		true,  // case-sensitive
	},
	"indexer.settings.cpuProfile": ConfigValue{
		false,
		"boolean indicate whether to start or stop indexer cpu profiling.",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.memProfDir": ConfigValue{
		"",
		"Directory at which mem-profile will be generated for indexer." +
			"Name of the generated mem_profile file: indexer_mem.pprof",
		"",
		false, // mutable
		true,  // case-sensitive
	},
	"indexer.settings.memProfile": ConfigValue{
		false,
		"boolean to take current mem profile from indexer.",
		false,
		false, // mutable
		false, // case-insensitive
	},

	"indexer.settings.maxVbQueueLength": ConfigValue{
		uint64(0),
		"Maximum Length of Mutation Queue Per Vbucket. This " +
			"allocation is done per bucket.",
		uint64(10000),
		false, // mutable
		false, // case-insensitive
	},

	"indexer.settings.minVbQueueLength": ConfigValue{
		uint64(250),
		"Minimum Length of Mutation Queue Per Vbucket. This " +
			"allocation is done per bucket. Must be greater " +
			"than smallSnapshotThreshold.",
		uint64(250),
		false, // mutable
		false, // case-insensitive
	},

	"indexer.settings.largeSnapshotThreshold": ConfigValue{
		uint64(200),
		"Threshold For Considering a DCP Snapshot as Large. " +
			"Must be less than maxVbQueueLength.",
		uint64(200),
		false, // mutable
		false, // case-insensitive
	},

	"indexer.settings.smallSnapshotThreshold": ConfigValue{
		uint64(30), //please see minVbQueueLength before changing this
		"Threshold For Considering a DCP Snapshot as Small. Must be" +
			"smaller than minVbQueueLength.",
		uint64(30),
		false, // mutable
		false, // case-insensitive
	},

	"indexer.init_stream.smallSnapshotThreshold": ConfigValue{
		uint64(100), //please see minVbQueueLength before changing this
		"Threshold For Considering a DCP Snapshot as Small for INIT_STREAM." +
			"Must be smaller than minVbQueueLength.",
		uint64(100),
		false, // mutable
		false, // case-insensitive
	},

	"indexer.settings.snapshotRequestWorkers": ConfigValue{
		(runtime.GOMAXPROCS(0) + 1) / 2,
		"Number of workers storage manager will spawn for listening " +
			"snapshot requests from scan coordinator",
		(runtime.GOMAXPROCS(0) + 1) / 2,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.snapshotListeners": ConfigValue{
		(runtime.GOMAXPROCS(0) + 1) / 2,
		"Number of workers scan coordinator will spawn for listening " +
			"snapshot notifications from storage manager",
		(runtime.GOMAXPROCS(0) + 1) / 2,
		false, // mutable
		false, // case-insensitive
	},

	"indexer.settings.sliceBufSize": ConfigValue{
		uint64(runtime.GOMAXPROCS(0) * 200),
		"Buffer for each slice to queue mutations before flush " +
			"to storage.",
		uint64(runtime.GOMAXPROCS(0) * 200),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.bufferPoolBlockSize": ConfigValue{
		16 * 1024,
		"Size of memory block in memory pool",
		16 * 1024,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.statsLogDumpInterval": ConfigValue{
		uint64(60),
		"Periodic stats dump logging interval in seconds",
		uint64(60),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.statsLogEnable": ConfigValue{
		true,
		"When enabled, indexer stats will be logged to a different log file.",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.statsLogFname": ConfigValue{
		"indexer_stats.log",
		"Name of the log file to log indexer stats.",
		"indexer_stats.log",
		false, // mutable
		false, // case-insensitive
	},
	"indexer.statsLogFcount": ConfigValue{
		10,
		"Number of log files (for log rotation) to log indexer stats.",
		10,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.statsLogFsize": ConfigValue{
		32 * 1024 * 1024,
		"Size of one log file to log indexer stats.",
		32 * 1024 * 1024,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.max_writer_lock_prob": ConfigValue{
		20,
		"Controls the write rate for compaction to catch up",
		20,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.wal_size": ConfigValue{
		uint64(4096),
		"WAL threshold size",
		uint64(4096),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.fast_flush_mode": ConfigValue{
		true,
		"Skips InMem Snapshots When Indexer Is Backed Up",
		true,
		false, // mutable
		false, // case-insensitive
	},

	//fdb specific settings
	"indexer.settings.persisted_snapshot.fdb.interval": ConfigValue{
		uint64(5000),
		"Persisted snapshotting interval in milliseconds",
		uint64(5000),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.persisted_snapshot_init_build.fdb.interval": ConfigValue{
		uint64(5000),
		"Persisted snapshotting interval in milliseconds for initial build",
		uint64(5000),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.inmemory_snapshot.fdb.interval": ConfigValue{
		uint64(200),
		"InMemory snapshotting interval in milliseconds",
		uint64(200),
		false, // mutable
		false, // case-insensitive
	},
	//end of fdb specific settings

	//moi specific settings
	"indexer.settings.persisted_snapshot.moi.interval": ConfigValue{
		uint64(600000), // keep in sync with index_settings_manager.erl
		"Persisted snapshotting interval in milliseconds",
		uint64(600000),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.persisted_snapshot_init_build.moi.interval": ConfigValue{
		uint64(600000),
		"Persisted snapshotting interval in milliseconds for initial build",
		uint64(600000),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.inmemory_snapshot.moi.interval": ConfigValue{
		uint64(10), // keep in sync with index_settings_manager.erl
		"InMemory snapshotting interval in milliseconds",
		uint64(10),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.moi.persistence_threads": ConfigValue{
		(runtime.GOMAXPROCS(0) + 1) / 2,
		"Number of concurrent threads scanning index for persistence",
		(runtime.GOMAXPROCS(0) + 1) / 2,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.moi.recovery_threads": ConfigValue{
		runtime.GOMAXPROCS(0),
		"Number of concurrent threads for rebuilding index from disk snapshot",
		runtime.GOMAXPROCS(0),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.moi.persistence.io_concurrency": ConfigValue{
		float64(0.7),
		"Number of concurrent disk operations during persistence. On linux, if it is smaller than 1, " +
			"this parameter specifies io concurrency as a percentage of max file descriptor limit.  On other " +
			"platforms, it is a percentage of default file descriptor limit.   If the value of this parameter is " +
			"greater than 1, it specifies the absolute value of io concurrency independent of system limit.",
		float64(0.7),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.storage_mode": ConfigValue{
		"",
		"Storage Type e.g. forestdb, memory_optimized",
		"",
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.scan_getseqnos_retries": ConfigValue{
		30,
		"Max retries for DCP request",
		30,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.use_bucket_seqnos": ConfigValue{
		false,
		"For session consistent scans, use BucketSeqnos " +
			"to avoid contention while retrieving seqnos",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.vbseqnos.workers_per_reader": ConfigValue{
		10,
		"Number of workers each vbSeqnosReader will spawn to " +
			"retrieve Seqnos from KV nodes. Changing this value" +
			"will close existing connections and new connections " +
			" will be established with latest number of workers",
		10,
		false, // mutable
		false, // case-insensitive
	},

	"indexer.settings.num_replica": ConfigValue{
		0,
		"Number of additional replica for each index.",
		0,
		false, // mutable
		false, // case-insensitive
	},
	"projector.settings.log_level": ConfigValue{
		"info",
		"Projector logging level",
		"info",
		false, // mutable
		false, // case-insensitive
	},
	"projector.diagnostics_dir": ConfigValue{
		"./",
		"Projector diagnostics information directory",
		"./",
		true, // immutable
		true, // case-sensitive
	},
	"projector.use_cinfo_lite": ConfigValue{
		true,
		"Use ClusterInfoCacheLite",
		true,
		false,
		false,
	},
	"projector.cinfo_lite.force_after": ConfigValue{
		5, // Minutes
		"Minutes after last update time to do a force fetch",
		5,
		false, // mutable
		false, // not case-sensitive
	},
	"projector.cinfo_lite.notifier_restart_sleep": ConfigValue{
		1000, // Milliseconds
		"Time to sleep before restarting notifier upon restart",
		1000,
		false, // mutable
		false, // not case-sensitive
	},
	"indexer.cinfo_lite.notifier_restart_sleep": ConfigValue{
		1000, // Milliseconds
		"Time to sleep before restarting notifier upon restart",
		1000,
		false, // mutable
		false, // not case-sensitive
	},
	"indexer.cinfo_lite.force_after": ConfigValue{
		5, // Minutes
		"Minutes after last update time to do a force fetch",
		5,
		false, // mutable
		false, // not case-sensitive
	},
	"indexer.use_cinfo_lite": ConfigValue{
		true,
		"Use ClusterInfoCacheLite",
		true,
		false,
		false,
	},
	"indexer.settings.moi.debug": ConfigValue{
		false,
		"Enable debug mode for moi storage engine",
		false,
		false, // mutable
		false, // case-interface
	},
	"indexer.rebalance.shard_aware_rebalance": ConfigValue{
		false,
		"use shard aware rebalance algorithm",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.rebalance.use_simple_planner": ConfigValue{
		false,
		"use simple round-robin planner for index placement." +
			"otherwise a sophisticated planner is used.",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.rebalance.node_eject_only": ConfigValue{
		true,
		"indexes are moved for only the nodes being ejected." +
			"If false, indexes will be moved to new nodes being added " +
			"to achieve a balanced distribution.",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.rebalance.disable_index_move": ConfigValue{
		false,
		"disable index movement on node add/remove",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.rebalance.maxRemainingBuildTime": ConfigValue{
		uint64(10),
		"max remaining build time(in seconds) before index state is switched to active",
		uint64(10),
		false, // mutable
		false, // case-insensitive
	},
	"indexer.rebalance.globalTokenWaitTimeout": ConfigValue{
		60,
		"wait time(in seconds) for global rebalance token to be observed by all nodes",
		60,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.rebalance.startPhaseBeginTimeout": ConfigValue{
		600,
		"wait time(in seconds) for Start Phase to begin after Prepare Phase",
		600,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.rebalance.disable_replica_repair": ConfigValue{
		false,
		"disable repairing replica",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.rebalance.httpTimeout": ConfigValue{
		1200,
		"timeout(in seconds) for http requests during rebalance",
		1200,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.rebalance.stream_update.interval": ConfigValue{
		600,
		"interval for indexer to update projector stream during rebalance (sec)",
		600,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.rebalance.drop_index.wait_time": ConfigValue{
		1,
		"wait time for rebalancer to start drop index after all indexes are built (sec)",
		1,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.rebalance.transferBatchSize": ConfigValue{
		3,
		"batch size of indexes transferred in one iteration during rebalance. 0 disables batching.",
		3,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.rebalance.redistribute_indexes": ConfigValue{
		false, // keep in sync with index_settings_manager.erl
		"redistribute indexes for optimal placement during rebalance." +
			"If false, indexes will only be moved from ejected nodes " +
			"or missing replicas will be repaired.",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.storage_mode.disable_upgrade": ConfigValue{
		false,
		"Disable upgrading storage mode. This is checked on every indexer restart, " +
			"independent if the cluster is under software upgrade or not.",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.build.batch_size": ConfigValue{
		5,
		"When performing background index build, specify the number of index to build in an iteration.  " +
			"Use -1 for no limit on batch size.",
		5,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.enable_corrupt_index_backup": ConfigValue{
		false,
		"When corrupted index is found, backup the corrupted index data files.",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.settings.corrupt_index_num_backups": ConfigValue{
		1,
		"Number of corrupted index backups to be retained, per index.",
		1,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.build.background.disable": ConfigValue{
		false,
		"Disable background index build, except during upgrade",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.build.enableOSO": ConfigValue{
		true,
		"Use OSO mode for Initial Index Build",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.scan.queue_size": ConfigValue{
		20,
		"When performing scan scattering in indexer, specify the queue size for the scatterer.",
		20,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.scan.notify_count": ConfigValue{
		5,
		"When performing scan scattering in indexer, specify the minimum item count in queue before notifying gatherer on new item arrival.",
		5,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.scan.partial_group_buffer_size": ConfigValue{
		50,
		"buffer size to hold partial group results. once the buffer is full, the results will be flushed",
		50,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.scan.enable_fast_count": ConfigValue{
		true,
		"enable fast count optimization for aggregate pushdown",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.planner.timeout": ConfigValue{
		300,
		"timeout (sec) on planner",
		300,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.planner.variationThreshold": ConfigValue{
		0.25,
		"acceptance threshold on resource variation. 0.25 means 25% variation from mean.",
		0.25,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.planner.cpuProfile": ConfigValue{
		false,
		"on/off for cpu profiling",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.planner.minResidentRatio": ConfigValue{
		0.2,
		"minimum resident ratio for index.  Use for enforcing minimum memory check. Set to 0 to disable memory check.",
		0.2,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.planner.internal.minIterPerTemp": ConfigValue{
		100,
		"Minimum number of iterations - per temperature - to be used by simulated annealing index planner.",
		100,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.planner.internal.maxIterPerTemp": ConfigValue{
		20000,
		"Maximum number of iterations - per temperature - to be used by simulated annealing index planner. " +
			"If set to a value < minIterPerTemp, then minIterPerTemp iterations will be executed.",
		20000,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.planner.useGreedyPlanner": ConfigValue{
		true,
		"Attempt to use greedy planner (instead of simulated annealing planner) for index creation, for faster placement.",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.stream_reader.markFirstSnap": ConfigValue{
		true,
		"Identify mutations from first DCP snapshot. Used for back index lookup optimization.",
		true,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.api.enableTestServer": ConfigValue{
		false,
		"Enable index QE REST Server",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.encoding.encode_compat_mode": ConfigValue{
		0,
		"enable indexer to re-encode keys from projector, to avoid MB-28956" +
			"0 - enable based on projector version, 1 - force enable, 2 - force disable",
		0,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.metadata.compaction.sleepDuration": ConfigValue{
		900,
		"sleep interval between metadata compaction",
		900,   // 15 min
		false, // mutable
		false, // case-insensitive
	},
	"indexer.metadata.compaction.threshold": ConfigValue{
		30,
		"compaction threshold percentage",
		30,    // 30%
		false, // mutable
		false, // case-insensitive
	},
	"indexer.metadata.compaction.minFileSize": ConfigValue{
		0,
		"minimum file size for compaction",
		0,     // default - 4M
		false, // mutable
		false, // case-insensitive
	},
	"indexer.memcachedTimeout": ConfigValue{
		120, // In Seconds
		"Timeout for indexer to memcached communication (In Seconds)",
		120,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.ddl.create.retryInterval": ConfigValue{
		300, // In Seconds
		"Interval to retry create index token (In Seconds)",
		300,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.allowScheduleCreate": ConfigValue{
		true,
		"Allow scheduling index creation in the background",
		true,
		false,
		false,
	},
	"indexer.allowScheduleCreateRebal": ConfigValue{
		false,
		"Allow scheduling of index creation in the background during rebalance",
		false,
		false,
		false,
	},
	"indexer.serverless.allowScheduleCreateRebal": ConfigValue{
		true,
		"Allow scheduling of index creation in the background during rebalance",
		true,
		false,
		false,
	},
	"indexer.scheduleCreateRetries": ConfigValue{
		1000,
		"Number of retries - per index - for background index creation.",
		1000,
		false,
		false,
	},
	"indexer.debug.enableBackgroundIndexCreation": ConfigValue{
		true,
		"This is an internal-use-only flag to enable/disable background index creation." +
			"If this flag is false, indexes scheduled for background creation " +
			"will not get created in the background. But the indexes will get " +
			"scheduled for background creation based on flags indexer.allowScheduleCreate" +
			"and indexer.allowScheduleCreateRebal.",
		true,
		false,
		false,
	},
	"indexer.debug.assertOnError": ConfigValue{
		false,
		"This flag is intended for use in test/debug setups. Certain " +
			"error conditions can be configured to cause panic to easily " +
			"catch issues. This should be disabled for production builds",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.debug.randomDelayInjection": ConfigValue{
		false,
		"This flag is intended for use in test/debug setups. Injects random " +
			"delay in generating async messages in indexer main loop to trigger " +
			"race conditions. This should be disabled for production builds",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.recovery.max_disksnaps": ConfigValue{
		4,
		"Maximum number of disk snapshots for recovery. If KV replica is behind active, " +
			"indexer would retain upto max_disksnaps for better safety against failover." +
			"This setting must be greater than or equal to max_rollbacks.",
		4,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.recovery.reset_index_on_rollback": ConfigValue{
		false,
		"This flag controls the rollback to 0 behavior of indexes in MAINT_STREAM. " +
			"If true, the indexes get reset on rollback to 0(flush, DCP rollback) i.e. the " +
			"state of indexes will be moved to Created(Ready for LifecyleManager) and rebuilt " +
			"by the builder in batches. If false, the indexes will rollback to 0 and rebuild in " +
			"MAINT_STREAM(pre 7.0 behavior).",
		false,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.cpu.throttle.target": ConfigValue{
		float64(1.00),
		"Target CPU usage in [0.50, 1.00] if CPU throttling is enabled.",
		float64(1.00),
		false, // mutable
		false, // case-insensitive
	},

	"indexer.settings.thresholds.mem_high": ConfigValue{
		80,
		"Percentage of memory_quota usage above which Indexer node " +
			"is considered to have maxed out the memory usage and needs " +
			"additional capacity.",
		80,
		false, // mutable
		false, // case-insensitive
	},

	"indexer.settings.thresholds.mem_low": ConfigValue{
		60,
		"Percentage of memory_quota usage above which Indexer node " +
			"doesn't accept new create index except for existing tenants. " +
			"The capacity between low and high threshold is for existing tenant growth.",
		60,
		false, // mutable
		false, // case-insensitive
	},

	"indexer.settings.serverless.indexLimit": ConfigValue{
		200,
		"Limit on the number of indexes that can be created per bucket in Serverless Mode.",
		200,
		false, // mutable
		false, // case-insensitive
	},

	"indexer.plasma.serverless.maxInstancePerShard": ConfigValue{
		uint64(400),
		"Maximum number of instances per shard for serverless",
		uint64(400),
		false,
		false,
	},
	"indexer.plasma.serverless.maxDiskUsagePerShard": ConfigValue{
		uint64(math.MaxInt64),
		"Maximum disk usage per shard for serverless",
		uint64(math.MaxInt64),
		false,
		false,
	},
	"indexer.plasma.serverless.minNumShard": ConfigValue{
		uint64(1),
		"Minimum number of shard",
		uint64(1),
		false,
		false,
	},
	"indexer.plasma.serverless.mainIndex.maxNumPageDeltas": ConfigValue{
		150,
		"Maximum number of page deltas",
		150,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.serverless.mainIndex.pageSplitThreshold": ConfigValue{
		150,
		"Threshold for triggering page split",
		150,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.serverless.backIndex.pageSplitThreshold": ConfigValue{
		100,
		"Threshold for triggering page split",
		100,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.serverless.recovery.requestQuoteIncrement": ConfigValue{
		1024 * 1024,
		"minimum amount of quota increment during recovery",
		1024 * 1024,
		false,
		false,
	},
	"indexer.plasma.serverless.targetResidentRatio": ConfigValue{
		float64(0.1),
		"Target resident ratio for index instance",
		float64(0.1),
		false,
		false,
	},
	"indexer.plasma.serverless.mutationRateLimit": ConfigValue{
		5000,
		"Mutation Rate Limit",
		5000,
		false,
		false,
	},
	"indexer.plasma.serverless.mainIndex.evictMinThreshold": ConfigValue{
		0.9,
		"Minimum memory use for periodic eviction to run. When memory use over min threshold," +
			" eviction will not run if plasma estimates all indexes can fit into quota.",
		0.9,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.serverless.backIndex.evictMinThreshold": ConfigValue{
		0.9,
		"See indexer.plasma.mainIndex.evictMinThreshold",
		0.9,
		false, // mutable
		false, // case-insensitive
	},
	"indexer.plasma.serverless.discretionaryQuotaThreshold": ConfigValue{
		0.6,
		"",
		0.6,
		false, // mutable
		false, // case-insensitive
	},
}

// NewConfig from another
// Config object or from map[string]interface{} object
// or from []byte slice, a byte-slice of JSON string.
func NewConfig(data interface{}) (Config, error) {
	config := make(Config)
	err := config.Update(data)
	return config, err
}

// Update config object with data, can be a Config, map[string]interface{},
// []byte.
func (config Config) Update(data interface{}) error {
	fmsg := "CONF[] skipping setting key %q value '%v': %v"
	switch v := data.(type) {
	case Config: // Clone
		for key, value := range v {
			config.Set(key, value)
		}

	case []byte: // parse JSON
		m := make(map[string]interface{})
		if err := json.Unmarshal(v, &m); err != nil {
			return err
		}
		config.Update(m)

	case map[string]interface{}: // transform
		for key, value := range v {
			if cv, ok := SystemConfig[key]; ok { // valid config.
				if _, ok := config[key]; !ok {
					config[key] = cv // copy by value
				}
				if err := config.SetValue(key, value); err != nil {
					logging.Warnf(fmsg, key, value, err)
				}

			} else {
				logging.Errorf("invalid config param %q", key)
			}
		}

	default:
		return nil
	}
	return nil
}

// Ensure these are valid configs
var configMap = map[string]string{
	"indexer.settings.enable_page_bloom_filter": "indexer.plasma.backIndex.enablePageBloomFilter",
}

func MapSettings(value []byte) ([]byte, bool, error) {
	newConfig, err := NewConfig(value)
	if err != nil {
		return value, false, err
	}

	logging.Infof("MapSettings: config before mapping: %v", string(newConfig.Json()))

	var dstKey string
	var srcVal interface{}
	var updated bool

	for key1, key2 := range configMap {
		val1, key1IsPresent := newConfig[key1]
		val2, key2IsPresent := newConfig[key2]

		// If both key1 and key2 are present, then prefer key1
		if key1IsPresent {
			// Either both key1 and key2 are present or only key1 is present
			// map key1 into key2
			dstKey = key2
			srcVal = val1.Value

		} else if key2IsPresent {
			// Only key2 is present
			// map key2 into key1
			dstKey = key1
			srcVal = val2.Value

		} else {
			// Both are not present, nothing to map
			continue
		}

		if cv, ok := SystemConfig[dstKey]; ok {

			if dstCV, ok := newConfig[dstKey]; !ok {
				// dstKey is not present, populate default value
				newConfig[dstKey] = cv
			} else if dstCV.Value == srcVal {
				// dstKey already has target value, skip mapping
				continue
			}

			if err := newConfig.SetValue(dstKey, srcVal); err == nil {
				updated = true
			} else {
				return value, false, fmt.Errorf("MapSettings: error during set : dstKey[%v] srcVal[%v] [%v]", dstKey, srcVal, err)
			}

		} else {
			return value, false, fmt.Errorf("MapSettings: invalid config param %q in configMap", dstKey)
		}
	}

	newValue := newConfig.Json()
	logging.Infof("MapSettings: config after mapping: %v", string(newValue))

	return newValue, updated, nil
}

// Clone a new config object.
func (config Config) Clone() Config {
	clone := make(Config)
	for key, value := range config {
		clone[key] = value
	}
	return clone
}

// Gives diff of two configs and returns different values in two Config objects
// first one has values from receiver and second has values from parameter
func (config Config) Diff(other Config) (Config, Config) {
	diffThis := make(Config)
	diffOther := make(Config)
	for key, _ := range other {
		if config[key].Immutable {
			continue
		}
		if config[key] != other[key] {
			diffThis[key] = config[key]
			diffOther[key] = other[key]
		}
	}
	return diffThis, diffOther
}

// Override will clone `config` object and update parameters with
// values from `others` instance. Will skip immutable fields.
func (config Config) Override(others ...Config) Config {
	newconfig := config.Clone()
	for _, other := range others {
		for key, cv := range other {
			if newconfig[key].Immutable { // skip immutables.
				continue
			}
			ocv, ok := newconfig[key]
			if !ok {
				ocv = cv
			} else {
				ocv.Value = cv.Value
			}
			newconfig[key] = ocv
		}
	}
	return newconfig
}

// OverrideForce will clone `config` object and update parameters with
// values from `others` instance. Will force override immutable fields
// as well.
func (config Config) OverrideForce(others ...Config) Config {
	newconfig := config.Clone()
	for _, other := range others {
		for key, cv := range other {
			ocv, ok := newconfig[key]
			if !ok {
				ocv = cv
			} else {
				ocv.Value = cv.Value
			}
			config[key] = ocv
		}
	}
	return config
}

// LogConfig will check wether a configuration parameter is
// mutable and log that information.
func (config Config) LogConfig(prefix string) {
	for key, cv := range config {
		if cv.Immutable {
			fmsg := "%v immutable settings %v cannot be update to `%v`\n"
			logging.Warnf(fmsg, prefix, key, cv.Value)
		} else {
			fmsg := "%v settings %v will updated to `%v`\n"
			logging.Infof(fmsg, prefix, key, cv.Value)
		}
	}
}

// SectionConfig will create a new config object with parameters
// starting with `prefix`. If `trim` is true, then config
// parameter will be trimmed with the prefix string.
func (config Config) SectionConfig(prefix string, trim bool) Config {
	section := make(Config)
	for key, value := range config {
		if strings.HasPrefix(key, prefix) {
			if trim {
				section[strings.TrimPrefix(key, prefix)] = value
			} else {
				section[key] = value
			}
		}
	}
	return section
}

func (config Config) FilterConfig(subs string) Config {
	newConfig := make(Config)
	for key, value := range config {
		if strings.Contains(key, subs) {
			newConfig[key] = value
		}
	}
	return newConfig
}

// Set ConfigValue for parameter. Mutates the config object.
func (config Config) Set(key string, cv ConfigValue) Config {
	config[key] = cv
	return config
}

// SetValue config parameter with value. Mutates the config object.
func (config Config) SetValue(key string, value interface{}) error {
	cv, ok := config[key]
	if !ok {
		return errors.New("invalid config parameter")
	}

	if value == nil {
		return errors.New("config value is nil")
	}

	defType := reflect.TypeOf(cv.DefaultVal)
	valType := reflect.TypeOf(value)

	if valType.ConvertibleTo(defType) {
		v := reflect.ValueOf(value)
		v = reflect.Indirect(v)
		value = v.Convert(defType).Interface()
		valType = defType
	}

	if valType.Kind() == reflect.String && cv.Casesensitive == false {
		value = strings.ToLower(value.(string))
	}

	if defType != reflect.TypeOf(value) {
		return fmt.Errorf("%v: Value type mismatch, %v != %v (%v)",
			key, valType, defType, value)
	}

	cv.Value = value
	config[key] = cv

	return nil
}

// Map will return key value map from the config
func (config Config) Map() map[string]interface{} {
	kvs := make(map[string]interface{})
	for key, value := range config {
		kvs[key] = value.Value
	}
	return kvs
}

// Json will marshal config into JSON string.
func (config Config) Json() []byte {
	kvs := make(map[string]interface{})
	for key, value := range config {
		kvs[key] = value.Value
	}

	bytes, _ := json.Marshal(kvs)
	return bytes
}

// getIndexerConfig gets an Indexer config value of any type from a config map that may or may not
// have the "indexer." prefix stripped from its keys. Caller provides the key with prefix stripped.
func (config Config) getIndexerConfig(strippedKey string) ConfigValue {
	value, exists := config[strippedKey]
	if !exists {
		value = config["indexer."+strippedKey]
	}
	return value
}

// getIndexerConfigInt gets an Indexer int config value from a config map that may or may not have
// the "indexer." prefix stripped from its keys. Caller provides the key with prefix stripped.
func (config Config) getIndexerConfigInt(strippedKey string) int {
	return config.getIndexerConfig(strippedKey).Int()
}

// getIndexerConfigString gets an Indexer string config value from a config map that may or may not
// have the "indexer." prefix stripped from its keys. Caller provides the key with prefix stripped.
func (config Config) getIndexerConfigString(strippedKey string) string {
	return config.getIndexerConfig(strippedKey).String()
}

// getIndexerConfigUint64 gets an Indexer uint64 config value from a config map that may or may not
// have the "indexer." prefix stripped from its keys. Caller provides the key with prefix stripped.
func (config Config) getIndexerConfigUint64(strippedKey string) uint64 {
	return config.getIndexerConfig(strippedKey).Uint64()
}

// GetIndexerMemoryQuota gets the Indexer's memory quota in bytes as logical
// min(indexer.settings.memory_quota, indexer.cgroup.memory_quota).
// The latter is from sigar memory_max and only included if cgroups are supported.
func (config Config) GetIndexerMemoryQuota() uint64 {
	gsiMemQuota := config.getIndexerConfigUint64("settings.memory_quota")
	cgroupMemQuota := config.getIndexerConfigUint64("cgroup.memory_quota")
	if cgroupMemQuota > 0 && cgroupMemQuota < gsiMemQuota {
		gsiMemQuota = cgroupMemQuota
	}
	return gsiMemQuota
}

// GetIndexerNumCpuPrc gets the Indexer's percentage of CPU to use (e.g. 400 means 4 cores). It is
// the logical minimum min(node, cgroup, GSI) * 100 available CPUs, where:
//   node  : # CPUs available on the node
//   cgroup: # CPUs the Indexer cgroup is allocated (if cgroups are supported, else 0);
//     indexer.cgroup.max_cpu_percent set from sigar num_cpu_prc
//   GSI   : indexer.settings.max_cpu_percent "Indexer Threads" UI config (if specified, else 0)
func (config Config) GetIndexerNumCpuPrc() int {
	const _GetIndexerNumCpuPrc = "Config::GetIndexerNumCpuPrc:"

	numCpuPrc := runtime.NumCPU() * 100 // node-level CPUs as a percent
	cgroupCpuPrc := config.getIndexerConfigInt("cgroup.max_cpu_percent")
	if cgroupCpuPrc > 0 && cgroupCpuPrc < numCpuPrc { // sigar gave a value and it is lower
		numCpuPrc = cgroupCpuPrc
	}
	gsiCpuPrc := config.getIndexerConfigInt("settings.max_cpu_percent") // GSI UI override
	if gsiCpuPrc > numCpuPrc {
		consoleMsg := fmt.Sprintf("Indexer Threads setting %v exceeds CPU cores"+
			" available %v. Using %v.", gsiCpuPrc/100, numCpuPrc/100, numCpuPrc/100)
		Console(config.getIndexerConfigString("clusterAddr"), consoleMsg)
		logging.Warnf("%v %v", _GetIndexerNumCpuPrc, consoleMsg)
	} else if gsiCpuPrc > 0 { // GSI overide is set; known at this point that it is not higher
		numCpuPrc = gsiCpuPrc
	}
	return numCpuPrc
}

func (config Config) String() string {
	return string(config.Json())
}

// Int assumes config value is an integer and returns the same.
func (cv ConfigValue) Int() int {
	if val, ok := cv.Value.(int); ok {
		return val
	} else if val, ok := cv.Value.(float64); ok {
		return int(val)
	}
	panic(fmt.Sprintf("not support Int() on %#v", cv))
}

// Float64 assumes config value integer or float64.
func (cv ConfigValue) Float64() float64 {
	if val, ok := cv.Value.(float64); ok {
		return val
	} else if val, ok := cv.Value.(float32); ok {
		return float64(val)
	} else if val, ok := cv.Value.(int); ok {
		return float64(val)
	}
	panic(fmt.Errorf("not support Float64() on %#v", cv))
}

// Uint64 assumes config value is 64-bit integer and returns the same.
func (cv ConfigValue) Uint64() uint64 {
	return cv.Value.(uint64)
}

// Uint32 assumes config value is 32-bit unsigned integer and returns the same.
func (cv ConfigValue) Uint32() uint32 {
	return uint32(cv.Int())
}

// String assumes config value is a string and returns the same.
func (cv ConfigValue) String() string {
	return cv.Value.(string)
}

// Strings assumes config value is comma separated string items.
func (cv ConfigValue) Strings() []string {
	ss := make([]string, 0)
	for _, s := range strings.Split(cv.Value.(string), ",") {
		s = strings.Trim(s, " \t\r\n")
		if len(s) > 0 {
			ss = append(ss, s)
		}
	}
	return ss
}

// Bool assumes config value is a Bool and returns the same.
func (cv ConfigValue) Bool() bool {
	return cv.Value.(bool)
}
