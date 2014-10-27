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
		"ignores all logging, irrespective of the log-level",
		false,
	},
	"log.level": ConfigValue{
		"info",
		"logging level for the system",
		"info",
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
		"channel size of projector's data path routine",
		10000,
	},
	"projector.feedChanSize": ConfigValue{
		100,
		"channel size of projector's control path and feed back path",
		100,
	},
	"projector.vbucketSyncTimeout": ConfigValue{
		500,
		"timeout, in milliseconds, is for sending periodic Sync messages for",
		500,
	},
	// projector adminport parameters
	"projector.adminport.name": ConfigValue{
		"projector.adminport",
		"human readable name for this adminport, must be supplied",
		"projector.adminport",
	},
	"projector.adminport.listenAddr": ConfigValue{
		"localhost:8888",
		"http bind address for this projector's adminport",
		"localhost:8888",
	},
	"projector.adminport.urlPrefix": ConfigValue{
		"/adminport/",
		"url prefix (script-path) for adminport used by projector",
		"/adminport/",
	},
	"projector.adminport.readTimeout": ConfigValue{
		0,
		"timeout in milliseconds, is read timeout for adminport http server " +
			"used by projector",
		0,
	},
	"projector.adminport.writeTimeout": ConfigValue{
		0,
		"timeout in milliseconds, is write timeout for adminport http server " +
			"used by projector",
		0,
	},
	"projector.adminport.maxHeaderBytes": ConfigValue{
		1 << 20, // 1 MegaByte
		"in bytes, is max. length of adminport http header " +
			"used by projector",
		1 << 20, // 1 MegaByte
	},
	// projector's adminport client
	"projector.client.retryInterval": ConfigValue{
		16,
		"retryInterval, in milliseconds, when connection refused by server",
		16,
	},
	"projector.client.maxRetries": ConfigValue{
		5,
		"maximum number of timest to retry",
		5,
	},
	"projector.client.exponentialBackoff": ConfigValue{
		2,
		"multiplying factor on retryInterval for every attempt with server",
		2,
	},
	// projector dataport client parameters
	"projector.dataport.client.parConnections": ConfigValue{
		1,
		"number of parallel connections to open with remote",
		1,
	},
	"projector.dataport.client.noRemoteBlock": ConfigValue{
		false,
		"should dataport endpoint block when remote is slow ?",
		false,
	},
	"projector.dataport.client.genServerChanSize": ConfigValue{
		64,
		"request channel size of projector-dataport-client's gen-server " +
			"routine",
		64,
	},
	"projector.dataport.client.mutationChanSize": ConfigValue{
		10000,
		"channel size of projector-dataport-client's data path " +
			"routine",
		10000,
	},
	"projector.dataport.client.keyChanSize": ConfigValue{
		10000,
		"channel size of dataport endpoints data input",
		10000,
	},
	"projector.dataport.client.bufferSize": ConfigValue{
		100,
		"number of entries to buffer before flushing it, where each entry " +
			"is for a vbucket's set of mutations that was flushed by the endpoint.",
		100,
	},
	"projector.dataport.client.bufferTimeout": ConfigValue{
		1,
		"timeout in milliseconds, to flush vbucket-mutations from endpoint " +
			"buffer to dataport-client, again from dataport-client to socket.",
		1, // 1ms
	},
	"projector.dataport.client.harakiriTimeout": ConfigValue{
		10 * 1000,
		"timeout in milliseconds, after which endpoint will commit harakiri " +
			"if not activity",
		10 * 1000, //10s
	},
	"projector.dataport.client.maxPayload": ConfigValue{
		1000 * 1024,
		"maximum payload length, in bytes, for transmission data from " +
			"router to downstream client",
		1000 * 1024, // bytes
	},
	// indexer dataport parameters
	"projector.dataport.indexer.genServerChanSize": ConfigValue{
		64,
		"request channel size of indexer dataport's gen-server routine",
		64,
	},
	"projector.dataport.indexer.maxPayload": ConfigValue{
		1000 * 1024,
		"maximum payload length, in bytes, for receiving data from router",
		1000 * 1024, // bytes
	},
	"projector.dataport.indexer.tcpReadDeadline": ConfigValue{
		4000,
		"timeout, in milliseconds, while reading from socket",
		4000, // 4s
	},
	// indexer queryport configuration
	"queryport.indexer.maxPayload": ConfigValue{
		1000 * 1024,
		"maximum payload, in bytes, for receiving data from client",
		1000 * 1024,
	},
	"queryport.indexer.readDeadline": ConfigValue{
		4000,
		"timeout, in milliseconds, is timeout while reading from socket",
		4000,
	},
	"queryport.indexer.writeDeadline": ConfigValue{
		4000,
		"timeout, in milliseconds, is timeout while writing to socket",
		4000,
	},
	"queryport.indexer.pageSize": ConfigValue{
		1,
		"number of index-entries that shall be returned as single payload",
		1,
	},
	"queryport.indexer.streamChanSize": ConfigValue{
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
		4000,
		"timeout, in milliseconds, is timeout while reading from socket",
		4000,
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
}

// NewConfig from another Config object (clone) or from
// map[string]interface{} object or from []byte slice, a byte-slice
// will be interpreted as JSON string.
func NewConfig(data interface{}) (Config, error) {
	config := SystemConfig.Clone()
	switch v := data.(type) {
	case Config:
		for key, value := range v {
			config.Set(key, value)
		}

	case map[string]interface{}:
		for key, value := range v {
			config.SetValue(key, value)
		}

	case []byte: // assume this to be in JSON
		m := make(map[string]interface{})
		if err := json.Unmarshal(v, m); err != nil {
			return nil, err
		}
		for key, value := range m {
			config.SetValue(key, value)
		}

	default:
		return nil, nil
	}

	return config, nil
}

// Clone a new config object.
func (config Config) Clone() Config {
	clone := make(Config)
	for key, value := range config {
		clone[key] = value
	}
	return clone
}

// SectionConfig will gather only those parameters whose
// key starts with `prefix`. If `trim` is true, then config
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

// Set ConfigValue for parameter.
func (config Config) Set(key string, cv ConfigValue) Config {
	config[key] = cv
	return config
}

// SetValue config parameter with value.
func (config Config) SetValue(key string, value interface{}) Config {
	cv := config[key]
	cv.Value = value
	config[key] = cv
	return config
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
		ss = append(ss, strings.Trim(s, " \t\r\n"))
	}
	return ss
}

// Bool assumes config value is a Bool and returns the same.
func (cv ConfigValue) Bool() bool {
	return cv.Value.(bool)
}
