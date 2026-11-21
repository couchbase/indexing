//go:build !community
// +build !community

// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package indexer

import (
	"runtime"
	"testing"
	"weak"

	"github.com/couchbase/bhive"
)

// bhive keeps a store's config in its shard, which outlives the slice that
// created the store. The encryption callbacks set on the config must therefore
// not keep the slice reachable, or a dropped index's stores stay in memory until
// its shard is destroyed.
func TestBhiveEncryptionCallbacksDoNotHoldSlice(t *testing.T) {
	slice := &bhiveSlice{sliceEncryptionCallbacks: EncrCbsTest}
	slice.idxDefn.BucketUUID = "c3b7d9e1"

	var cfg bhive.Config
	slice.setupEncryptionCallbacks(&cfg)
	if cfg.GetKeyById == nil || cfg.GetActiveKeyByPath == nil {
		t.Fatalf("encryption callbacks not set")
	}

	ws := weak.Make(slice)
	slice = nil
	runtime.GC()
	runtime.GC()
	if ws.Value() != nil {
		t.Fatalf("the config's encryption callbacks keep the slice reachable")
	}

	// the callbacks still work without the slice
	if _, _, cipher := cfg.GetKeyById(nil); cipher != "" {
		t.Fatalf("GetKeyById: cipher %q, want none", cipher)
	}
	runtime.KeepAlive(cfg)
}
