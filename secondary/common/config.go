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

import "encoding/json"
import "strings"
import "fmt"
import "reflect"
import "errors"
import "github.com/couchbase/indexing/secondary/logging"
import "github.com/couchbase/indexing/secondary/platform"
import "unsafe"
import "runtime"

// NOTE:
// following settings are related to each other.
//      "projector.adminport.readTimeout",
//      "projector.dataport.harakiriTimeout",
//      "indexer.dataport.tcpReadDeadline",
//

// formula to compute the default CPU allocation for projector.
var projector_maxCpuPercent = (1 + (runtime.NumCPU() / 6)) * 100

// Threadsafe config holder object
type ConfigHolder struct {
	ptr unsafe.Pointer
}

func (h *ConfigHolder) Store(conf Config) {
	platform.StorePointer(&h.ptr, unsafe.Pointer(&conf))
}

func (h *ConfigHolder) Load() Config {
	confptr := platform.LoadPointer(&h.ptr)
	return *(*Config)(confptr)
}

// Config is a key, value map with key always being a string
// represents a config-parameter.
type Config map[string]ConfigValue

// ConfigValue for each parameter.
type ConfigValue struct {
	Value      interface{}
	Help       string
	DefaultVal interface{}
	Immutable  bool
}

// SystemConfig is default configuration for system and components.
// configuration parameters follow flat namespacing like,
//      "maxVbuckets"  for system-level config parameter
//      "projector.xxx" for projector component.
//      "projector.adminport.xxx" for adminport under projector component.
// etc...
var SystemConfig = Config{
	// system parameters
	"maxVbuckets": ConfigValue{
		1024,
		"number of vbuckets configured in KV",
		1024,
		true, // immutable
	},
	// projector parameters
	"projector.name": ConfigValue{
		"projector",
		"human readable name for this projector",
		"projector",
		true, // immutable
	},
	"projector.clusterAddr": ConfigValue{
		"localhost:9000",
		"KV cluster's address to be used by projector",
		"localhost:9000",
		true, // immutable
	},
	"projector.maxCpuPercent": ConfigValue{
		projector_maxCpuPercent,
		"Maximum percent of CPU that projector can use. " +
			"EG, 200% in 4-core (400%) machine would set indexer to " +
			"use 2 cores",
		projector_maxCpuPercent,
		false, // mutable
	},
	// Projector feed settings
	"projector.routerEndpointFactory": ConfigValue{
		RouterEndpointFactory(nil),
		"RouterEndpointFactory callback to generate endpoint instances " +
			"to push data to downstream",
		RouterEndpointFactory(nil),
		true, // immutable
	},
	"projector.feedWaitStreamReqTimeout": ConfigValue{
		10 * 1000,
		"timeout, in milliseconds, to await a response for StreamRequest",
		10 * 1000,
		false, // mutable
	},
	"projector.feedWaitStreamEndTimeout": ConfigValue{
		10 * 1000,
		"timeout, in milliseconds, to await a response for StreamEnd",
		10 * 1000,
		false, // mutable
	},
	"projector.mutationChanSize": ConfigValue{
		100,
		"channel size of projector's vbucket workers, " +
			"changing this value does not affect existing feeds.",
		100,
		false, // mutable
	},
	"projector.feedChanSize": ConfigValue{
		100,
		"channel size for feed's control path, " +
			"changing this value does not affect existing feeds.",
		100,
		false, // mutable
	},
	"projector.backChanSize": ConfigValue{
		10000,
		"channel size of projector feed's back-channel, " +
			"changing this value does not affect existing feeds.",
		10000,
		false, // mutable
	},
	"projector.syncTimeout": ConfigValue{
		2000,
		"timeout, in milliseconds, for sending periodic Sync messages, " +
			"changing this value does not affect existing feeds.",
		2000,
		false, // mutable
	},
	"projector.watchInterval": ConfigValue{
		5 * 60 * 1000, // 5 minutes
		"periodic tick, in milli-seconds to check for stale feeds, " +
			"a feed is considered stale when all its endpoint go stale.",
		5 * 60 * 1000,
		true, // immutable
	},
	"projector.staleTimeout": ConfigValue{
		5 * 60 * 1000, // 5 minutes
		"timeout, in milli-seconds to wait for response for feed's genserver" +
			"feed will be force-shutdown if timeout expires",
		5 * 60 * 1000,
		true, // immutable
	},
	"projector.cpuProfFname": ConfigValue{
		"",
		"filename to dump cpu-profile for projector.",
		"",
		false, // mutable
	},
	"projector.cpuProfile": ConfigValue{
		false,
		"boolean indicate whether to start or stop projector cpu profiling.",
		false,
		false, // mutable
	},
	"projector.memProfFname": ConfigValue{
		"",
		"filename to dump mem-profile for projector.",
		"",
		false, // mutable
	},
	"projector.memProfile": ConfigValue{
		false,
		"boolean to take current mem profile from projector.",
		false,
		false, // mutable
	},
	// projector dcp parameters
	"projector.dcp.genChanSize": ConfigValue{
		2048,
		"channel size for DCP's gen-server routine, " +
			"changing this value does not affect existing feeds.",
		2048,
		false, // mutable
	},
	"projector.dcp.dataChanSize": ConfigValue{
		10000,
		"channel size for DCP's data path routines, " +
			"changing this value does not affect existing feeds.",
		10000,
		false, // mutable
	},
	// projector adminport parameters
	"projector.adminport.name": ConfigValue{
		"projector.adminport",
		"human readable name for this adminport, must be supplied",
		"projector.adminport",
		true, // immutable

	},
	"projector.adminport.listenAddr": ConfigValue{
		"",
		"projector's adminport address listen for request.",
		"",
		true, // immutable
	},
	"projector.adminport.urlPrefix": ConfigValue{
		"/adminport/",
		"url prefix (script-path) for adminport used by projector",
		"/adminport/",
		true, // immutable
	},
	"projector.adminport.readTimeout": ConfigValue{
		0,
		"timeout in milliseconds, is http server's read timeout, " +
			"also refer to projector.dataport.harakiriTimeout and " +
			"indexer.dataport.tcpReadDeadline",
		0,
		true, // immutable
	},
	"projector.adminport.writeTimeout": ConfigValue{
		0,
		"timeout in milliseconds, is http server's write timeout",
		0,
		true, // immutable
	},
	"projector.adminport.maxHeaderBytes": ConfigValue{
		1 << 20, // 1 MegaByte
		"in bytes, is max. length of adminport http header",
		1 << 20, // 1 MegaByte
		true,    // immutable
	},
	// projector dataport client parameters
	"projector.dataport.remoteBlock": ConfigValue{
		true,
		"should dataport endpoint block when remote is slow, " +
			"does not affect existing feeds.",
		true,
		false, // mutable
	},
	"projector.dataport.keyChanSize": ConfigValue{
		100000,
		"channel size of dataport endpoints data input, " +
			"does not affect existing feeds.",
		100000,
		true, // immutable
	},
	"projector.dataport.bufferSize": ConfigValue{
		100,
		"number of entries to buffer before flushing it, where each entry " +
			"is for a vbucket's set of mutations that was flushed, " +
			"by the endpoint, does not affect existing feeds.",
		100,
		false, // mutable
	},
	"projector.dataport.bufferTimeout": ConfigValue{
		25,
		"timeout in milliseconds, to flush vbucket-mutations from, " +
			"endpoint, does not affect existing feeds.",
		25,    // 25ms
		false, // mutable
	},
	"projector.dataport.harakiriTimeout": ConfigValue{
		30 * 1000,
		"timeout in milliseconds, after which endpoint will commit harakiri " +
			"if not active, does not affect existing feeds, " +
			"also refer to projector.adminport.readTimeout and " +
			"indexer.dataport.tcpReadDeadline.",
		30 * 1000, //10s
		false,     // mutable
	},
	"projector.dataport.maxPayload": ConfigValue{
		1024 * 1024,
		"maximum payload length, in bytes, for transmission data from " +
			"router to downstream client, does not affect eixting feeds.",
		1024 * 1024, // 1MB
		true,        // immutable
	},
	// projector's adminport client, can be used by manager
	"manager.projectorclient.retryInterval": ConfigValue{
		16,
		"retryInterval, in milliseconds when connection refused by server",
		16,
		true, // immutable
	},
	"manager.projectorclient.maxRetries": ConfigValue{
		5,
		"maximum number of times to retry",
		5,
		true, // immutable
	},
	"manager.projectorclient.exponentialBackoff": ConfigValue{
		2,
		"multiplying factor on retryInterval for every attempt with server",
		2,
		true, // immutable
	},
	"manager.projectorclient.urlPrefix": ConfigValue{
		"/adminport/",
		"url prefix (script-path) for adminport used by projector",
		"/adminport/",
		true, // immutable
	},
	// indexer dataport parameters
	"indexer.dataport.genServerChanSize": ConfigValue{
		10000,
		"request channel size of indexer dataport's gen-server routine",
		10000,
		true, // immutable
	},
	"indexer.dataport.maxPayload": ConfigValue{
		1000 * 1024,
		"maximum payload length, in bytes, for receiving data from router",
		1000 * 1024, // bytes
		true,        // immutable
	},
	"indexer.dataport.tcpReadDeadline": ConfigValue{
		30 * 1000,
		"timeout, in milliseconds, while reading from socket, " +
			"also refer to projector.adminport.readTimeout and " +
			"projector.dataport.harakiriTimeout.",
		30 * 1000, // 10s
		true,      // immutable
	},
	// indexer queryport configuration
	"indexer.queryport.maxPayload": ConfigValue{
		1000 * 1024,
		"maximum payload, in bytes, for receiving data from client",
		1000 * 1024,
		true, // immutable
	},
	"indexer.queryport.readDeadline": ConfigValue{
		4000,
		"timeout, in milliseconds, is timeout while reading from socket",
		4000,
		true, // immutable
	},
	"indexer.queryport.writeDeadline": ConfigValue{
		4000,
		"timeout, in milliseconds, is timeout while writing to socket",
		4000,
		true, // immutable
	},
	"indexer.queryport.pageSize": ConfigValue{
		1,
		"number of index-entries that shall be returned as single payload",
		1,
		true, // immutable
	},
	"indexer.queryport.streamChanSize": ConfigValue{
		16,
		"size of the buffered channels used to stream request and response.",
		16,
		true, // immutable
	},
	// queryport client configuration
	"queryport.client.maxPayload": ConfigValue{
		1000 * 1024,
		"maximum payload, in bytes, for receiving data from server",
		1000 * 1024,
		true, // immutable
	},
	"queryport.client.readDeadline": ConfigValue{
		300000,
		"timeout, in milliseconds, is timeout while reading from socket",
		300000,
		true, // immutable
	},
	"queryport.client.writeDeadline": ConfigValue{
		4000,
		"timeout, in milliseconds, is timeout while writing to socket",
		4000,
		true, // immutable
	},
	"queryport.client.settings.poolSize": ConfigValue{
		1000,
		"number simultaneous active connections connections in a pool",
		1000,
		true, // immutable
	},
	"queryport.client.settings.poolOverflow": ConfigValue{
		30,
		"maximum number of connections in a pool",
		30,
		true, // immutable
	},
	"queryport.client.connPoolTimeout": ConfigValue{
		1000,
		"timeout, in milliseconds, is timeout for retrieving a connection " +
			"from the pool",
		1000,
		true, // immutable
	},
	"queryport.client.connPoolAvailWaitTimeout": ConfigValue{
		1,
		"timeout, in milliseconds, to wait for an existing connection " +
			"from the pool before considering the creation of a new one",
		1,
		true, // immutable
	},
	"queryport.client.retryScanPort": ConfigValue{
		2,
		"number of times to retry when scanport is not detectable",
		2,
		true, // immutable
	},
	"queryport.client.retryIntervalScanport": ConfigValue{
		10,
		"wait, in milliseconds, before re-trying for a scanport",
		10,
		true, // immutable
	},
	"queryport.client.servicesNotifierRetryTm": ConfigValue{
		1000,
		"wait, in milliseconds, before restarting the ServicesNotifier",
		1000,
		true, // immutable
	},
	// projector's adminport client, can be used by indexer.
	"indexer.projectorclient.retryInterval": ConfigValue{
		16,
		"retryInterval, in milliseconds when connection refused by server",
		16,
		true, // immutable
	},
	"indexer.projectorclient.maxRetries": ConfigValue{
		5,
		"maximum number of times to retry",
		5,
		true, // immutable
	},
	"indexer.projectorclient.exponentialBackoff": ConfigValue{
		2,
		"multiplying factor on retryInterval for every attempt with server",
		2,
		true, // immutable
	},
	"indexer.projectorclient.urlPrefix": ConfigValue{
		"/adminport/",
		"url prefix (script-path) for adminport used by projector",
		"/adminport/",
		true, // immutable
	},
	"indexer.adminPort": ConfigValue{
		"9100",
		"port for index ddl and status operations",
		"9100",
		true, // immutable
	},
	"indexer.scanPort": ConfigValue{
		"9101",
		"port for index scan operations",
		"9101",
		true, // immutable
	},
	"indexer.httpPort": ConfigValue{
		"9102",
		"port for external stats amd settings",
		"9102",
		true, // immutable
	},
	"indexer.streamInitPort": ConfigValue{
		"9103",
		"port for inital build stream",
		"9103",
		true, // immutable
	},
	"indexer.streamCatchupPort": ConfigValue{
		"9104",
		"port for catchup stream",
		"9104",
		true, // immutable
	},
	"indexer.streamMaintPort": ConfigValue{
		"9105",
		"port for maintenance stream",
		"9105",
		true, // immutable
	},
	"indexer.clusterAddr": ConfigValue{
		"127.0.0.1:8091",
		"Local cluster manager address",
		"127.0.0.1:8091",
		true, // immutable
	},
	"indexer.numVbuckets": ConfigValue{
		1024,
		"Number of vbuckets",
		1024,
		true, // immutable
	},
	"indexer.enableManager": ConfigValue{
		false,
		"Enable index manager",
		false,
		true, // immutable
	},
	"indexer.storage_dir": ConfigValue{
		"./",
		"Index file storage directory",
		"./",
		true, // immutable
	},
	"indexer.numSliceWriters": ConfigValue{
		1,
		"Number of Writer Threads for a Slice",
		1,
		true, // immutable
	},

	"indexer.sync_period": ConfigValue{
		uint64(2000),
		"Stream message sync interval in millis",
		uint64(2000),
		true, // immutable
	},

	"indexer.stats_cache_timeout": ConfigValue{
		uint64(5000),
		"Stats cache ttl in millis",
		uint64(5000),
		true, // immutable
	},

	// Indexer dynamic settings
	"indexer.settings.compaction.check_period": ConfigValue{
		30,
		"Compaction poll interval in seconds",
		30,
		false, // mutable
	},
	"indexer.settings.compaction.interval": ConfigValue{
		"00:00,00:00",
		"Compaction allowed interval",
		"00:00,00:00",
		false, // mutable
	},
	"indexer.settings.compaction.min_frag": ConfigValue{
		30,
		"Compaction fragmentation threshold percentage",
		30,
		false, // mutable
	},
	"indexer.settings.compaction.min_size": ConfigValue{
		uint64(1024 * 1024 * 500),
		"Compaction min file size",
		uint64(1024 * 1024 * 500),
		false, // mutable
	},
	"indexer.settings.persisted_snapshot.interval": ConfigValue{
		uint64(5000), // keep in sync with index_settings_manager.erl
		"Persisted snapshotting interval in milliseconds",
		uint64(5000),
		false, // mutable
	},
	"indexer.settings.persisted_snapshot_init_build.interval": ConfigValue{
		uint64(5000),
		"Persisted snapshotting interval in milliseconds for initial build",
		uint64(5000),
		false, // mutable
	},
	"indexer.settings.inmemory_snapshot.interval": ConfigValue{
		uint64(200), // keep in sync with index_settings_manager.erl
		"InMemory snapshotting interval in milliseconds",
		uint64(200),
		false, // mutable
	},
	"indexer.settings.recovery.max_rollbacks": ConfigValue{
		5, // keep in sync with index_settings_manager.erl
		"Maximum number of committed rollback points",
		5,
		false, // mutable
	},
	"indexer.settings.memory_quota": ConfigValue{
		uint64(256 * 1024 * 1024),
		"Maximum memory used by the indexer buffercache",
		uint64(256 * 1024 * 1024),
		false, // mutable
	},
	"indexer.settings.max_cpu_percent": ConfigValue{
		400,
		"Maximum percent of CPU that indexer can use. " +
			"EG, 200% in 4-core (400%) machine would set indexer to " +
			"use 2 cores",
		400,
		false, // mutable
	},
	"indexer.settings.log_level": ConfigValue{
		"info", // keep in sync with index_settings_manager.erl
		"Indexer logging level",
		"info",
		false, // mutable
	},
	"indexer.settings.scan_timeout": ConfigValue{
		120000,
		"timeout, in milliseconds, timeout for index scan processing",
		120000,
		true, // immutable
	},

	"indexer.settings.send_buffer_size": ConfigValue{
		1024,
		"Buffer size for batching rows during scan result streaming",
		1024,
		true, // immutable
	},

	"indexer.settings.cpuProfFname": ConfigValue{
		"",
		"filename to dump cpu-profile for indexer.",
		"",
		false, // mutable
	},
	"indexer.settings.cpuProfile": ConfigValue{
		false,
		"boolean indicate whether to start or stop indexer cpu profiling.",
		false,
		false, // mutable
	},
	"indexer.settings.memProfFname": ConfigValue{
		"",
		"filename to dump mem-profile for indexer.",
		"",
		false, // mutable
	},
	"indexer.settings.memProfile": ConfigValue{
		false,
		"boolean to take current mem profile from indexer.",
		false,
		false, // mutable
	},

	"indexer.settings.maxVbQueueLength": ConfigValue{
		uint64(0),
		"Maximum Length of Mutation Queue Per Vbucket. This " +
			"allocation is done per bucket.",
		uint64(10000),
		false, // mutable
	},

	"indexer.settings.largeSnapshotThreshold": ConfigValue{
		uint64(200),
		"Threshold For Considering a DCP Snapshot as Large. " +
			"Must be less than maxVbQueueLength.",
		uint64(200),
		false, // mutable
	},

	"indexer.settings.smallSnapshotThreshold": ConfigValue{
		uint64(30),
		"Threshold For Considering a DCP Snapshot as Small.",
		uint64(30),
		false, // mutable
	},

	"indexer.settings.sliceBufSize": ConfigValue{
		uint64(50000),
		"Buffer for each slice to queue mutations before flush " +
			"to storage.",
		uint64(50000),
		false, // mutable
	},
	"indexer.settings.bufferPoolBlockSize": ConfigValue{
		16 * 1024,
		"Size of memory block in memory pool",
		16 * 1024,
		false,
	},
	"indexer.settings.statsLogDumpInterval": ConfigValue{
		uint64(60),
		"Periodic stats dump logging interval in seconds",
		uint64(60),
		false,
	},
	"indexer.settings.max_writer_lock_prob": ConfigValue{
		20,
		"Controls the write rate for compaction to catch up",
		20,
		false, // mutable
	},
	"indexer.settings.wal_size": ConfigValue{
		uint64(4096),
		"WAL threshold size",
		uint64(4096),
		false, // mutable
	},
	"indexer.settings.fast_flush_mode": ConfigValue{
		true,
		"Skips InMem Snapshots When Indexer Is Backed Up",
		true,
		false, // mutable
	},
	"projector.settings.log_level": ConfigValue{
		"info",
		"Projector logging level",
		"info",
		false, // mutable
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

// Clone a new config object.
func (config Config) Clone() Config {
	clone := make(Config)
	for key, value := range config {
		clone[key] = value
	}
	return clone
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
			config[key] = ocv
		}
	}
	return config
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

	defType := reflect.TypeOf(cv.DefaultVal)
	valType := reflect.TypeOf(value)

	if valType.ConvertibleTo(defType) {
		v := reflect.ValueOf(value)
		v = reflect.Indirect(v)
		value = v.Convert(defType).Interface()
		valType = defType
	}

	if valType.Kind() == reflect.String {
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

// Json will marshal config into JSON string.
func (config Config) Json() []byte {
	kvs := make(map[string]interface{})
	for key, value := range config {
		kvs[key] = value.Value
	}

	bytes, _ := json.Marshal(kvs)
	return bytes
}

// Int assumes config value is an integer and returns the same.
func (cv ConfigValue) Int() int {
	if val, ok := cv.Value.(int); ok {
		return val
	} else if val, ok := cv.Value.(float64); ok {
		return int(val)
	}
	panic(fmt.Errorf("not support Int() on %v", cv))
}

// Uint64 assumes config value is 64-bit integer and returns the same.
func (cv ConfigValue) Uint64() uint64 {
	return cv.Value.(uint64)
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
