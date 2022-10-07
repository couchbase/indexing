//go:build !community

// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package common

import "fmt"

import "github.com/couchbase/indexing/secondary/logging"
import "github.com/couchbase/regulator"

func CheckIngressLockdown(bucket string) error {
	if regulator.Instance == nil {
		// This is not expected.
		// TODO: Should we panic here?
		logging.Errorf("Regulator is not initialised. Allowing ingress for bucket %v.", bucket)
		return nil
	}

	bctx := regulator.NewBucketCtx(bucket)
	acc, _, err := regulator.CheckAccess(bctx, true)
	if err != nil {
		return err
	}

	// TODO: Return per service disk usage in the error.

	if acc == regulator.AccessNoIngress {
		return ErrNoIngress
	}

	if acc == regulator.AccessError {
		return fmt.Errorf("AccessError")
	}

	return nil
}
