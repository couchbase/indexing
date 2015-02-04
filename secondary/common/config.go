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

// Config is a key, value map with key always being a string
// represents a config-parameter.
type Config map[string]ConfigValue

// ConfigValue for each parameter.
type ConfigValue struct {
	Value      interface{}
	Help       string
	DefaultVal interface{}
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
	},
	// log parameters
	// TODO: add configuration for log file-name and other types of writer.
	"log.ignore": ConfigValue{
		false,
		"ignore all logging, irrespective of the log-level",
		false,
	},
	"log.level": ConfigValue{
		"info",
		"logging level for the system",
		"info",
	},
	"log.file": ConfigValue{
		"",
		"log messages to file",
		"",
	},
	// projector parameters
	"projector.name": ConfigValue{
		"projector",
		"human readable name for this projector",
		"projector",
	},
	"projector.clusterAddr": ConfigValue{
		"localhost:9000",
		"KV cluster's address to be used by projector",
		"localhost:9000",
	},
	"projector.kvAddrs": ConfigValue{
		"127.0.0.1:9000",
		"Comma separated list of KV-address to read mutations, this need to " +
			"exactly match with KV-node's configured address",
		"127.0.0.1:9000",
	},
	"projector.colocate": ConfigValue{
		true,
		"Whether projector will be colocated with KV. In which case " +
			"`kvaddrs` specified above will be discarded",
		true,
	},
	"projector.routerEndpointFactory": ConfigValue{
		RouterEndpointFactory(nil),
		"RouterEndpointFactory callback to generate endpoint instances " +
			"to push data to downstream",
		RouterEndpointFactory(nil),
	},
	"projector.feedWaitStreamReqTimeout": ConfigValue{
		10 * 1000,
		"timeout, in milliseconds, to await a response for StreamRequest",
		10 * 1000,
	},
	"projector.feedWaitStreamEndTimeout": ConfigValue{
		10 * 1000,
		"timeout, in milliseconds, to await a response for StreamEnd",
		10 * 1000,
	},
	"projector.mutationChanSize": ConfigValue{
		10000,
		"channel size of projector's vbucket workers",
		10000,
	},
	"projector.feedChanSize": ConfigValue{
		100,
		"channel size for feed's control path and its back-channel.",
		100,
	},
	"projector.vbucketSyncTimeout": ConfigValue{
		500,
		"timeout, in milliseconds, for sending periodic Sync messages.",
		500,
	},
	// projector adminport parameters
	"projector.adminport.name": ConfigValue{
		"projector.adminport",
		"human readable name for this adminport, must be supplied",
		"projector.adminport",
	},
	"projector.adminport.listenAddr": ConfigValue{
		"",
		"projector's adminport address listen for request.",
		"",
	},
	"projector.adminport.urlPrefix": ConfigValue{
		"/adminport/",
		"url prefix (script-path) for adminport used by projector",
		"/adminport/",
	},
	"projector.adminport.readTimeout": ConfigValue{
		0,
		"timeout in milliseconds, is http server's read timeout",
		0,
	},
	"projector.adminport.writeTimeout": ConfigValue{
		0,
		"timeout in milliseconds, is http server's write timeout",
		0,
	},
	"projector.adminport.maxHeaderBytes": ConfigValue{
		1 << 20, // 1 MegaByte
		"in bytes, is max. length of adminport http header",
		1 << 20, // 1 MegaByte
	},
	// projector dataport client parameters
	"endpoint.dataport.remoteBlock": ConfigValue{
		true,
		"should dataport endpoint block when remote is slow ?",
		true,
	},
	"endpoint.dataport.keyChanSize": ConfigValue{
		10000,
		"channel size of dataport endpoints data input",
		10000,
	},
	"endpoint.dataport.bufferSize": ConfigValue{
		100,
		"number of entries to buffer before flushing it, where each entry " +
			"is for a vbucket's set of mutations that was flushed by the endpoint.",
		100,
	},
	"endpoint.dataport.bufferTimeout": ConfigValue{
		1,
		"timeout in milliseconds, to flush vbucket-mutations from endpoint",
		1, // 1ms
	},
	"endpoint.dataport.harakiriTimeout": ConfigValue{
		10 * 1000,
		"timeout in milliseconds, after which endpoint will commit harakiri " +
			"if not activity",
		10 * 1000, //10s
	},
	"endpoint.dataport.maxPayload": ConfigValue{
		1000 * 1024,
		"maximum payload length, in bytes, for transmission data from " +
			"router to downstream client",
		1000 * 1024, // bytes
	},
	// projector's adminport client, can be used by manager
	"manager.projectorclient.retryInterval": ConfigValue{
		16,
		"retryInterval, in milliseconds when connection refused by server",
		16,
	},
	"manager.projectorclient.maxRetries": ConfigValue{
		5,
		"maximum number of times to retry",
		5,
	},
	"manager.projectorclient.exponentialBackoff": ConfigValue{
		2,
		"multiplying factor on retryInterval for every attempt with server",
		2,
	},
	"manager.projectorclient.urlPrefix": ConfigValue{
		"/adminport/",
		"url prefix (script-path) for adminport used by projector",
		"/adminport/",
	},
	// indexer dataport parameters
	"indexer.dataport.genServerChanSize": ConfigValue{
		64,
		"request channel size of indexer dataport's gen-server routine",
		64,
	},
	"indexer.dataport.maxPayload": ConfigValue{
		1000 * 1024,
		"maximum payload length, in bytes, for receiving data from router",
		1000 * 1024, // bytes
	},
	"indexer.dataport.tcpReadDeadline": ConfigValue{
		10 * 1000,
		"timeout, in milliseconds, while reading from socket",
		10 * 1000, // 10s
	},
	// indexer queryport configuration
	"indexer.queryport.maxPayload": ConfigValue{
		1000 * 1024,
		"maximum payload, in bytes, for receiving data from client",
		1000 * 1024,
	},
	"indexer.queryport.readDeadline": ConfigValue{
		4000,
		"timeout, in milliseconds, is timeout while reading from socket",
		4000,
	},
	"indexer.queryport.writeDeadline": ConfigValue{
		4000,
		"timeout, in milliseconds, is timeout while writing to socket",
		4000,
	},
	"indexer.queryport.pageSize": ConfigValue{
		1,
		"number of index-entries that shall be returned as single payload",
		1,
	},
	"indexer.queryport.streamChanSize": ConfigValue{
		16,
		"size of the buffered channels used to stream request and response.",
		16,
	},
	// queryport client configuration
	"queryport.client.maxPayload": ConfigValue{
		1000 * 1024,
		"maximum payload, in bytes, for receiving data from server",
		1000 * 1024,
	},
	"queryport.client.readDeadline": ConfigValue{
		300000,
		"timeout, in milliseconds, is timeout while reading from socket",
		300000,
	},
	"queryport.client.writeDeadline": ConfigValue{
		4000,
		"timeout, in milliseconds, is timeout while writing to socket",
		4000,
	},
	"queryport.client.poolSize": ConfigValue{
		2,
		"number simultaneous active connections connections in a pool",
		2,
	},
	"queryport.client.poolOverflow": ConfigValue{
		4,
		"maximum number of connections in a pool",
		4,
	},
	"queryport.client.connPoolTimeout": ConfigValue{
		1000,
		"timeout, in milliseconds, is timeout for retrieving a connection " +
			"from the pool",
		1000,
	},
	"queryport.client.connPoolAvailWaitTimeout": ConfigValue{
		1,
		"timeout, in milliseconds, to wait for an existing connection " +
			"from the pool before considering the creation of a new one",
		1,
	},
	// projector's adminport client, can be used by indexer.
	"indexer.projectorclient.retryInterval": ConfigValue{
		16,
		"retryInterval, in milliseconds when connection refused by server",
		16,
	},
	"indexer.projectorclient.maxRetries": ConfigValue{
		5,
		"maximum number of times to retry",
		5,
	},
	"indexer.projectorclient.exponentialBackoff": ConfigValue{
		2,
		"multiplying factor on retryInterval for every attempt with server",
		2,
	},
	"indexer.projectorclient.urlPrefix": ConfigValue{
		"/adminport/",
		"url prefix (script-path) for adminport used by projector",
		"/adminport/",
	},
	// indexer configuration
	"indexer.scanTimeout": ConfigValue{
		120000,
		"timeout, in milliseconds, timeout for index scan processing",
		120000,
	},
	"indexer.adminPort": ConfigValue{
		"9100",
		"port for index ddl and status operations",
		"9100",
	},
	"indexer.scanPort": ConfigValue{
		"9101",
		"port for index scan operations",
		"9101",
	},
	"indexer.httpPort": ConfigValue{
		"9102",
		"port for external stats amd settings",
		"9102",
	},
	"indexer.streamInitPort": ConfigValue{
		"9103",
		"port for inital build stream",
		"9103",
	},
	"indexer.streamCatchupPort": ConfigValue{
		"9104",
		"port for catchup stream",
		"9104",
	},
	"indexer.streamMaintPort": ConfigValue{
		"9105",
		"port for maintenance stream",
		"9105",
	},
	"indexer.clusterAddr": ConfigValue{
		"127.0.0.1:8091",
		"Local cluster manager address",
		"127.0.0.1:8091",
	},
	"indexer.numVbuckets": ConfigValue{
		1024,
		"Number of vbuckets",
		1024,
	},
	"indexer.enableManager": ConfigValue{
		false,
		"Enable index manager",
		false,
	},
	"indexer.storage_dir": ConfigValue{
		"./",
		"Index file storage directory",
		"./",
	},
	"indexer.numSliceWriters": ConfigValue{
		1,
		"Number of Writer Threads for a Slice",
		1,
	},

	"indexer.sync_period": ConfigValue{
		uint64(100),
		"Stream message sync interval in millis",
		uint64(100),
	},

	// Indexer dynamic settings
	"indexer.settings.compaction.check_period": ConfigValue{
		1200000,
		"Compaction poll interval in seconds",
		1200000,
	},
	"indexer.settings.compaction.interval": ConfigValue{
		"00:00,00:00",
		"Compaction allowed interval",
		"00:00,00:00",
	},
	"indexer.settings.compaction.min_frag": ConfigValue{
		30,
		"Compaction fragmentation threshold percentage",
		30,
	},
	"indexer.settings.compaction.min_size": ConfigValue{
		uint64(1024 * 1024),
		"Compaction min file size",
		uint64(1024 * 1024),
	},
	"indexer.settings.persisted_snapshot.interval": ConfigValue{
		uint64(30000),
		"Persisted snapshotting interval in milliseconds",
		uint64(30000),
	},
	"indexer.settings.inmemory_snapshot.interval": ConfigValue{
		uint64(200),
		"InMemory snapshotting interval in milliseconds",
		uint64(200),
	},
	"indexer.settings.recovery.max_rollbacks": ConfigValue{
		5,
		"Maximum number of committed rollback points",
		5,
	},
	"indexer.settings.memory_quota": ConfigValue{
		uint64(0),
		"Maximum memory used by the indexer buffercache",
		uint64(0),
	},
	"indexer.settings.max_cpu_percent": ConfigValue{
		400,
		"Maximum nCPUs percent used by the processes",
		400,
	},
}

// NewConfig from another
// Config object or from map[string]interface{} object
// or from []byte slice, a byte-slice of JSON string.
func NewConfig(data interface{}) (Config, error) {
	config := SystemConfig.Clone()
	err := config.Update(data)
	return config, err
}

// Update config object with data, can be a Config, map[string]interface{},
// []byte.
func (config Config) Update(data interface{}) error {
	switch v := data.(type) {
	case Config: // Clone
		for key, value := range v {
			config.Set(key, value)
		}

	case map[string]interface{}: // transform
		for key, value := range v {
			if err := config.SetValue(key, value); err != nil {
				Warnf("Skipping setting key '%v' value '%v' due to %v", key, value, err)
			}
		}

	case []byte: // parse JSON
		m := make(map[string]interface{})
		if err := json.Unmarshal(v, &m); err != nil {
			return err
		}
		for key, value := range m {
			if err := config.SetValue(key, value); err != nil {
				Warnf("Skipping setting key '%v' value '%v' due to %v", key, value, err)
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
// values from `others` instance.
func (config Config) Override(others ...Config) Config {
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
	return cv.Value.(int)
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
