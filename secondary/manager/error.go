// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package manager

import (
	"fmt"
)

// The error handling has the same setup as the indexer.
// This is to help with consolidation on a common error facility
// at a later point.

type errCode int16

const (
	// generic (0 - 50)
	ERROR_PANIC     errCode = 0
	ERROR_ARGUMENTS         = 1

	// MetadataRepo (51-100)
	ERROR_META_WRONG_KEY          = 51
	ERROR_META_IDX_DEFN_EXIST     = 52
	ERROR_META_IDX_DEFN_NOT_EXIST = 53
	ERROR_META_FAIL_TO_PARSE_INT  = 54

	// Event Manager (101-150)
	ERROR_EVT_DUPLICATE_NOTIFIER = 101

	// Index Manager (151-200)
	ERROR_MGR_DDL_CREATE_IDX = 151
	ERROR_MGR_DDL_DROP_IDX   = 152

	// Coordinator (201-250)
	ERROR_COOR_LISTENER_FAIL = 201
	ERROR_COOR_ELECTION_FAIL = 202

	// Watcher (251 - 300)
	ERROR_WATCH_NO_ADDR_AVAIL = 251

	// Stream (301-350)
	ERROR_STREAM_INVALID_ARGUMENT   = 301
	ERROR_STREAM_NOT_OPEN           = 302
	ERROR_STREAM_REQUEST_ERROR      = 303
	ERROR_STREAM_WRONG_VBUCKET      = 304
	ERROR_STREAM_INVALID_TIMESTAMP  = 305
	ERROR_STREAM_PROJECTOR_TIMEOUT  = 306
	ERROR_STREAM_INVALID_KVADDRS    = 307
	ERROR_STREAM_STREAM_END         = 308
	ERROR_STREAM_FEEDER             = 309
	ERROR_STREAM_INCONSISTENT_VBMAP = 310
)

type errSeverity int16

const (
	FATAL errSeverity = iota
	NORMAL
)

type errCategory int16

const (
	GENERIC errCategory = iota
	COORDINATOR
	INDEX_MANAGER
	METADATA_REPO
	REQUEST_HANDLER
	EVENT_MANAGER
	WATCHER
	STREAM
)

type Error struct {
	code     errCode
	severity errSeverity
	category errCategory
	cause    error
	msg      string
}

func NewError(code errCode, severity errSeverity, category errCategory, cause error, msg string) Error {
	return Error{code: code,
		severity: severity,
		category: category,
		cause:    cause,
		msg:      msg}
}

func NewError4(code errCode, severity errSeverity, category errCategory, msg string) Error {
	return Error{code: code,
		severity: severity,
		category: category,
		cause:    nil,
		msg:      msg}
}

func NewError3(code errCode, severity errSeverity, category errCategory) Error {
	return Error{code: code,
		severity: severity,
		category: category,
		cause:    nil,
		msg:      ""}
}

func NewError2(code errCode, category errCategory) Error {
	return Error{code: code,
		severity: NORMAL,
		category: category,
		cause:    nil,
		msg:      ""}
}

func (e Error) Error() string {
	return fmt.Sprintf("Error :: code= %d, severity= %s, category= %s, reason= %s, cause= %s",
		e.code, severity(e.severity), category(e.category), e.msg, e.cause)
}

func category(category errCategory) string {
	switch category {
	case GENERIC:
		return "Generic"
	case COORDINATOR:
		return "Coordinator"
	case INDEX_MANAGER:
		return "Index Manager"
	case METADATA_REPO:
		return "Metadata Repo"
	case REQUEST_HANDLER:
		return "Request Handler"
	case EVENT_MANAGER:
		return "Event Manager"
	case WATCHER:
		return "Watcher"
	case STREAM:
		return "Stream"
	}
	return ""
}

func severity(severity errSeverity) string {
	switch severity {
	case NORMAL:
		return "Normal"
	case FATAL:
		return "Fatal"
	}
	return ""
}
