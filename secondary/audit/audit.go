// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package audit

import (
	"fmt"
	"net/http"

	cbaudit "github.com/couchbase/goutils/go-cbaudit"
	"github.com/couchbase/indexing/secondary/logging"
)

///////////////////////////////////////////////////////////////////////////////
// audit.go provides GSI's integration with the Couchbase Audit Service (part
// of kv_engine). Its APIs are called statically, so no class or instantation.
///////////////////////////////////////////////////////////////////////////////

// auditService holds a reference to the (kv-owned) auditing service.
// This is initialized by InitAuditService (called by indexer.NewIndexer)
// and used by all GSI audit calls.
var auditService *cbaudit.AuditSvc

// AuditEvent represents all the information to be logged in audit.log for an
// auditable event. Fields are defined in audit/audit_descriptor.json, which
// the build integrates into build/kv_engine/auditd/audit_events.json, which
// gets installed to install/etc/security/audit_events.json
// (see https://github.com/couchbase/kv_engine/blob/a78ef664ba162267d777ddb2fc1a920757effb8e/auditd/README.md).
// GSI's event ID range is 49152-53247 (0xC000-0xCFFF). AuditEvent's fields
// must be capitalized so they are visible to go-cbaudit's audit package for
// marshaling (except fields in Common are visible to it even without
// capitalizing since their type is a member of that package, but it's less
// confusing to capitalize all fields).
type AuditEvent struct {
	cbaudit.CommonAuditFields        // timestamp, real_userid, local, remote
	Service                   string `json:"service"`           // will always be set to "Index"
	Method                    string `json:"method"`            // "Class::Method"
	Url                       string `json:"url"`               // string version of URL from request
	Message                   string `json:"message,omitempty"` // optional additional message
}

// InitAuditService initializes the singleton auditService.
func InitAuditService(address string) error {

	// clusterAddr is missing the protocol prefix
	address = "http://" + address
	logging.Infof("audit::InitAuditService using address %v", address)

	var err error // avoid shadowing auditService with :=
	auditService, err = cbaudit.NewAuditSvc(address)
	if err != nil {
		err2 := fmt.Errorf("audit::InitAuditService: NewAuditSvc(%v) failed with error %v",
			address, err)
		logging.Errorf("%v", err2)
		return err2
	}
	return nil
}

// Audit writes an entry in the audit.log for the event (if auditing is enabled). eventId is
// the "id" field of a GSI event in audit_descriptor.json. Common fields are pulled from req.
// method should be set to "Class::Method" of the calling method. If a string other than the
// true class name is used in other logging messages, that string should be used here too, e.g.
// request_handler.go uses "RequestHandler" but the true class name is "requestHandlerContext".
// If the caller is a function rather than a method (no receiver), the filename before ".go"
// should be passed as the class name, e.g. "request_handler" for funcs in request_handler.go.)
// msg is an optional additional message to log with the audit info.
func Audit(eventId uint32, req *http.Request, method string, msg string) error {
	// event is the full audit event to log
	event := AuditEvent{
		Service: "Index",
		Method:  method,
		Url:     fmt.Sprintf("%v", req.URL),
		Message: msg} // optional
	event.CommonAuditFields = cbaudit.GetCommonAuditFields(req)

	// Write the event to audit.log if auditing is enabled (else this is a no-op)
	err := auditService.Write(eventId, event)
	if err != nil {
		err2 := fmt.Errorf("audit::AuditEvent: Write failed with error %v", err)
		logging.Errorf("%v", err2)
		return err2
	}
	return nil
}
