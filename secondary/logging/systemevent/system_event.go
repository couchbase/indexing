package systemevent

import (
	"net/http"
	"sync"
	"time"

	sel "github.com/couchbase/goutils/systemeventlog"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"
)

// Indexing's EventId range: 2048-3071
const (
	EVENTID_INDEXER_SETTINGS_CHANGE sel.EventId = 2048 + iota
	EVENTID_PROJECTOR_SETTINGS_CHANGE
	EVENTID_QUERY_CLIENT_SETTINGS_CHANGE
	EVENTID_INDEXER_CRASH
	EVENTID_PROJECTOR_CRASH
	EVENTID_QUERY_CLIENT_CRASH
	// Note: Add events here. Don't add events above in between the Events.
	// EventID once assigned should not be changed.
	EVENTID_UNDEFINED
	EVENTID_INDEXING_LIMIT sel.EventId = 3071
)

// Add description in the below map when new Event is added
var eventIDToDescriptionMap = map[sel.EventId]string{
	EVENTID_INDEXER_SETTINGS_CHANGE:      "Indexer Settings Changed",
	EVENTID_PROJECTOR_SETTINGS_CHANGE:    "Projector Settings Changed",
	EVENTID_QUERY_CLIENT_SETTINGS_CHANGE: "Query Client Settings Changed",
	EVENTID_INDEXER_CRASH:                "Indexer Process Crashed",
	EVENTID_PROJECTOR_CRASH:              "Projector Process Crashed",
	EVENTID_QUERY_CLIENT_CRASH:           "Query Client Crashed",
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

func LogSystemEvent(subComponent string, eventId sel.EventId,
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

	seInfo := sel.SystemEventInfo{EventId: eventId, Description: des}
	se := sel.NewSystemEvent(subComponent, seInfo, severity,
		extraAttributes)

	systemEventLogger.Log(se)
}

func InfoEvent(subComponent string, eventId sel.EventId,
	extraAttributes interface{}) {
	LogSystemEvent(subComponent, eventId, sel.SEInfo, extraAttributes)
}

func WarnEvent(subComponent string, eventId sel.EventId,
	extraAttributes interface{}) {
	LogSystemEvent(subComponent, eventId, sel.SEWarning, extraAttributes)
}

func ErrorEvent(subComponent string, eventId sel.EventId,
	extraAttributes interface{}) {
	LogSystemEvent(subComponent, eventId, sel.SEError, extraAttributes)
}

func FatalEvent(subComponent string, eventId sel.EventId,
	extraAttributes interface{}) {
	LogSystemEvent(subComponent, eventId, sel.SEFatal, extraAttributes)
}
