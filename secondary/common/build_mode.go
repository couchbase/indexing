//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package common

import (
	"sync"
)

type BuildMode byte

const (
	COMMUNITY = iota
	ENTERPRISE
)

func (b BuildMode) String() string {
	switch b {
	case COMMUNITY:
		return "Community"
	case ENTERPRISE:
		return "Enterprise"
	default:
		return "Invalid"
	}
}

//Global Build Mode
var gBuildMode BuildMode
var bLock sync.RWMutex

func GetBuildMode() BuildMode {

	bLock.RLock()
	defer bLock.RUnlock()
	return gBuildMode

}

func SetBuildMode(mode BuildMode) {

	bLock.Lock()
	defer bLock.Unlock()
	gBuildMode = mode

}
