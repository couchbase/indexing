package systemevent

import (
	"net/http"
	"sync"
	"time"

	sel "github.com/couchbase/goutils/systemeventlog"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"
)

type SystemEventID sel.EventId

// Indexing's EventId range: 2048-3071
const (
	// ****
	// Setting Change events
	// ****
	// Below events are logged after the component gets a callback on settings
	// change and applies the settings
	EVENTID_INDEXER_SETTINGS_CHANGE SystemEventID = 2048 + iota
	EVENTID_PROJECTOR_SETTINGS_CHANGE
	EVENTID_QUERY_CLIENT_SETTINGS_CHANGE

	// ***
	// Crash events
	// ***
	EVENTID_INDEXER_CRASH
	EVENTID_PROJECTOR_CRASH
	EVENTID_QUERY_CLIENT_CRASH

	// ****
	// DDL Events
	// ****
	// Logged when index state changes to INDEX_STATE_READY
	EVENTID_INDEX_PARTITION_CREATED
	// Logged when index state changes to INDEX_STATE_INITIAL
	EVENTID_INDEX_PARTITION_BUILDING
	// Logged when index state changes to INDEX_STATE_ACTIVE
	EVENTID_INDEX_PARTITION_ONLINE
	// Logged when index or index partition is deleted or pruned
	EVENTID_INDEX_PARTITION_DROPPED
	// Logged when index partition is added during merge
	EVENTID_INDEX_PARTITION_MERGED
	// Logged when there is an Error in the system and some residue of that
	// index instance or partition in the system
	EVENTID_INDEX_PARTITION_ERROR
	// Logged when index is scheduled for background creation
	EVENTID_INDEX_SCHED_CREATE
	// Logged when index background creation of index fails
	EVENTID_INDEX_SCHED_CREATE_ERROR

	// Logged when 2 replicas of same index are diverging in items_count
	EVENID_DIVERGING_REPLICAS
	// *****
	// Note: Add events here. Don't add events above in between the Events.
	// EventID once assigned should not be changed.
	// *****

	EVENTID_UNDEFINED
	EVENTID_INDEXING_LIMIT SystemEventID = 3071
)

// Add description in the below map when new Event is added
var eventIDToDescriptionMap = map[SystemEventID]string{
	EVENTID_INDEXER_SETTINGS_CHANGE:      "Indexer Settings Changed",
	EVENTID_PROJECTOR_SETTINGS_CHANGE:    "Projector Settings Changed",
	EVENTID_QUERY_CLIENT_SETTINGS_CHANGE: "Query Client Settings Changed",
	EVENTID_INDEXER_CRASH:                "Indexer Process Crashed",
	EVENTID_PROJECTOR_CRASH:              "Projector Process Crashed",
	EVENTID_QUERY_CLIENT_CRASH:           "Query Client Crashed",
	EVENTID_INDEX_PARTITION_CREATED:      "Index Instance or Partition Created",
	EVENTID_INDEX_PARTITION_BUILDING:     "Index Instance or Partition Building",
	EVENTID_INDEX_PARTITION_ONLINE:       "Index Instance or Partition Online",
	EVENTID_INDEX_PARTITION_DROPPED:      "Index Instance or Partition Dropped",
	EVENTID_INDEX_PARTITION_MERGED:       "Index Partition Merged",
	EVENTID_INDEX_PARTITION_ERROR:        "Index Instance or Partition Error State Change",
	EVENTID_INDEX_SCHED_CREATE:           "Index Scheduled for Creation",
	EVENTID_INDEX_SCHED_CREATE_ERROR:     "Index Scheduled Creation Error",
	EVENID_DIVERGING_REPLICAS:            "Index replicas are diverging in items_count",
}

// Configuration values for SystemEventLogger
// TODO: Tune these parameters
const (
	SystemEventComponent = "indexing"
	MaxQueuedEvents      = 3000
	MaxTries             = 10
	MaxRetryIntervalSecs = 120
)

var systemEventLogger sel.SystemEventLogger
var selInitOnce sync.Once

func InitSystemEventLogger(clusterAddr string) (err error) {

	selInitOnce.Do(func() {
		clusterAddr = "http://" + clusterAddr

		config := sel.SystemEventLoggerConfig{
			QueueSize:            MaxQueuedEvents,
			MaxTries:             MaxTries,
			MaxRetryIntervalSecs: MaxRetryIntervalSecs,
		}

		client, err := getClient(clusterAddr,
			time.Duration(120)*time.Second)
		if err != nil {
			return
		}

		errorLoggerFunc := getErrLogger()

		systemEventLogger = sel.NewSystemEventLogger(config, clusterAddr,
			SystemEventComponent, *client, errorLoggerFunc)

		logging.Infof("InitSystemEventLogger: Started SystemEventLogger")
	})

	return
}

func getClient(url string, clientTimeout time.Duration) (*http.Client, error) {
	c, err := security.MakeClient(url)
	if err != nil {
		return nil, err
	}
	if clientTimeout != 0 {
		c.Timeout = clientTimeout
	}
	return c, nil
}

func getErrLogger() func(msg string) {
	return func(msg string) {
		logging.Errorf("%v", msg)
	}
}

func LogSystemEvent(subComponent string, eventId SystemEventID,
	severity sel.EventSeverity, extraAttributes interface{}) {

	if eventId < EVENTID_INDEXER_SETTINGS_CHANGE ||
		eventId >= EVENTID_UNDEFINED {
		logging.Errorf("LogSystemEvent: Unrecognized SystemEventId: %v",
			eventId)
		return
	}

	des, ok := eventIDToDescriptionMap[eventId]
	if !ok {
		logging.Errorf("LogSystemEvent: Description for EventId %v"+
			" not available in eventIDToDescriptionMap", eventId)
		return
	}

	seInfo := sel.SystemEventInfo{EventId: sel.EventId(eventId),
		Description: des}
	se := sel.NewSystemEvent(subComponent, seInfo, severity,
		extraAttributes)

	systemEventLogger.Log(se)
}

func InfoEvent(subComponent string, eventId SystemEventID,
	extraAttributes interface{}) {
	LogSystemEvent(subComponent, eventId, sel.SEInfo, extraAttributes)
}

func WarnEvent(subComponent string, eventId SystemEventID,
	extraAttributes interface{}) {
	LogSystemEvent(subComponent, eventId, sel.SEWarning, extraAttributes)
}

func ErrorEvent(subComponent string, eventId SystemEventID,
	extraAttributes interface{}) {
	LogSystemEvent(subComponent, eventId, sel.SEError, extraAttributes)
}

func FatalEvent(subComponent string, eventId SystemEventID,
	extraAttributes interface{}) {
	LogSystemEvent(subComponent, eventId, sel.SEFatal, extraAttributes)
}

//
// System Events
//

type ddlSystemEvent struct {
	Group          string             `json:"group"`
	Module         string             `json:"module"`
	DefinitionID   common.IndexDefnId `json:"definition_id"`
	InstanceID     common.IndexInstId `json:"instance_id"`
	ReplicaID      uint64             `json:"replica_id"`
	PartitionID    uint64             `json:"partition_id,omitempty"`
	RealInstanceID common.IndexInstId `json:"real_instance_id,omitempty"`
	IsProxyInst    bool               `json:"is_proxy_instance,omitempty"`
	ErrorString    string             `json:"error_string,omitempty"`

	// For Debug as the node name in event does not contain port number
	IndexerID string `json:"indexer_id,omitempty"`
}

func NewDDLSystemEvent(mod string, defnId common.IndexDefnId,
	instId common.IndexInstId, replicaId uint64, partnId uint64,
	realInstId common.IndexInstId, indexerId string, errorStr string) ddlSystemEvent {
	e := ddlSystemEvent{
		Group:          "DDL",
		Module:         mod,
		DefinitionID:   defnId,
		InstanceID:     instId,
		ReplicaID:      replicaId,
		PartitionID:    partnId,
		RealInstanceID: realInstId,
		IndexerID:      indexerId,
		IsProxyInst:    realInstId != 0,
		ErrorString:    errorStr,
	}
	return e
}

type divergingReplicasEvent struct {
	Group          string             `json:"group"`
	Module         string             `json:"module"`
	IndexName      string             `json:"index_name"`
	PartitionId    common.PartitionId `json:"partition_id"`
	ReplicaID_1    int                `json:"replica_id_1"`
	ReplicaID_2    int                `json:"replica_id_2"`
	ItemsCount_1   uint64             `json:"items_count_1"`
	ItemsCount_2   uint64             `json:"items_count_2"`
	TimestampMatch bool               `json:"timestamp_match"`

	PendingItems_1 int64 `json:"pending_items_1,omitempty"`
	PendingItems_2 int64 `json:"pending_items_2,omitempty"`
}

func NewDivergingReplicasEvent(mod string, indexName string,
	partnId common.PartitionId, replicaId_1 int, replicaId_2 int,
	timestampMatch bool, itemsCount_1, itemsCount_2 uint64,
	firstIndexPending, secondIndexPending int64) divergingReplicasEvent {

	e := divergingReplicasEvent{
		Group:          "ItemsCountCheck",
		Module:         mod,
		IndexName:      indexName,
		PartitionId:    partnId,
		ReplicaID_1:    replicaId_1,
		ReplicaID_2:    replicaId_2,
		ItemsCount_1:   itemsCount_1,
		ItemsCount_2:   itemsCount_2,
		TimestampMatch: timestampMatch,
		PendingItems_1: firstIndexPending,
		PendingItems_2: secondIndexPending,
	}
	return e
}

type settingsChangeEvent struct {
	Group       string                 `json:"group"`
	Module      string                 `json:"module"`
	OldSettings map[string]interface{} `json:"old_setting"`
	NewSettings map[string]interface{} `json:"new_setting"`
}

func NewSettingsChangeEvent(mod string, oldSetting,
	newSettings map[string]interface{}) settingsChangeEvent {
	e := settingsChangeEvent{
		Group:       "SettingsChange",
		Module:      mod,
		NewSettings: newSettings,
		OldSettings: oldSetting,
	}
	return e
}
