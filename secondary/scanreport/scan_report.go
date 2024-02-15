// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package scanreport

import (
	"time"
)

type ScanReport struct {
	DefnID        uint64
	indexerReport map[string]*IndexerScanReport
	qcReport      map[string]*QueryClientScanReport
}

type IndexerScanReport struct {
	WaitDur         time.Duration
	GetSeqnosDur    time.Duration
	NumRowsReturned uint64
	NumRowsScanned  uint64
	ScanDur         time.Duration
}

type QueryClientScanReport struct {
}
