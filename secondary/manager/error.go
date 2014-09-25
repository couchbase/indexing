package manager

import (
	"fmt"
)

// The error handling has the same setup as the indexer. 
// This is to help with consolidation on a common error facility
// at a later point.

type errCode int16
const (
	ERROR_PANIC errCode = 0 
	
	// Event Manager (100)
	ERROR_EVT_DUPLICATE_NOTIFIER = 100
)

type errSeverity int16
const (
	FATAL errSeverity = iota
	NORMAL
)

type errCategory int16
const (
	COORDINATOR errCategory = iota
 	INDEX_MANAGER	
	METADATA_REPO	
	REQUEST_HANDLER	
	EVENT_MANAGER
	WATCHER	
)

type Error struct {
	code     errCode
	severity errSeverity
	category errCategory
	cause    error
	msg      string
}

func NewError(code errCode, severity errSeverity, category errCategory, cause error, msg string) Error {
	return Error{errCode: errCode,
				 severity : severity,
				 category : category,
				 cause : cause,
				 msg : msg}
}

func (e *Error) Error() string {
	return fmt.Printf("Error :: code= %d, severity= %s, category= %s, reason= %s, cause= %s, 
			e.errCode, severity(e.severity), category(e.category), e.msg, e.cause)	
}

func category(category errCategory) string {
	switch category {
		case COORDINATOR : return "Coordinator" 
		case INDEX_MANAGER : return "Index Manager" 
		case METADATA_REPO : return "Metadata Repo" 
		case REQUEST_HANDLER : return "Request Handler" 
		case EVENT_MANAGER : return "Event Manager" 
		case WATCHER : return "Watcher" 
	}
}

func severity(severity errSeverity) string {
	switch severity {
		case NORMAL: return "Normal" 
		case FATAL:  return "Fatal" 
	}
}
