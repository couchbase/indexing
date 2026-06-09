// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package common

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

var errEARUnexpectedStatus = errors.New("unexpected HTTP status from encryption-at-rest endpoint")

// earTypeSettings holds the encryptionMethod field for one EAR category.
type earTypeSettings struct {
	EncryptionMethod string `json:"encryptionMethod"`
}

// earSettings mirrors the top-level JSON object returned by
// GET /settings/security/encryptionAtRest.
// config is intentionally excluded — it is almost always enabled on non-EAR clusters.
type earSettings struct {
	Log   earTypeSettings `json:"log"`
	Audit earTypeSettings `json:"audit"`
	Other earTypeSettings `json:"other"`
}

// earBucketInfo is the minimal subset of a bucket object from
// GET /pools/default/buckets needed to detect per-bucket EAR.
// A full Bucket struct lives in secondary/dcp/pools.go; that package
// cannot be imported here without creating a cycle.
type earBucketInfo struct {
	Name                  string `json:"name"`
	EncryptionAtRestKeyId *int   `json:"encryptionAtRestKeyId,omitempty"`
}

// earFetch performs an authenticated GET, decodes the JSON response into out,
// and returns (true, nil) on success. Returns (false, nil) when the server
// responds with 404 so callers can treat a missing endpoint as "disabled".
// All other non-200 status codes and decode errors are returned as errors.
func earFetch(url, username, password string, out any) (bool, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return false, fmt.Errorf("earFetch: build request: %w", err)
	}
	req.SetBasicAuth(username, password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("earFetch: GET %s: %w", url, err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("earFetch: GET %s returned %s: %w",
			url, resp.Status, errEARUnexpectedStatus)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return false, fmt.Errorf("earFetch: decode %s: %w", url, err)
	}
	return true, nil
}

// IsEncryptionAtRestEnabled queries the cluster management REST API and returns
// true if encryption at rest is active on any service category (log, audit,
// other) or on any bucket (encryptionAtRestKeyId >= 0).
//
// mgmtAddr is the host:port of the ns_server management endpoint.
// Returns (false, nil) when the cluster does not expose the EAR endpoint
// (older clusters that pre-date EAR support).
func IsEncryptionAtRestEnabled(mgmtAddr, username, password string) (bool, error) {
	base := "http://" + mgmtAddr

	// Service-level check: log, audit, other categories.
	var settings earSettings
	ok, err := earFetch(base+"/settings/security/encryptionAtRest", username, password, &settings)
	if err != nil {
		return false, err
	}
	if ok {
		isActive := func(m string) bool {
			return m == "nodeSecretManager" || m == "encryptionKey"
		}
		if isActive(settings.Log.EncryptionMethod) ||
			isActive(settings.Audit.EncryptionMethod) ||
			isActive(settings.Other.EncryptionMethod) {

			return true, nil
		}
	}

	// Bucket-level check: any bucket with encryptionAtRestKeyId >= 0 is encrypted.
	var buckets []earBucketInfo
	ok, err = earFetch(base+"/pools/default/buckets", username, password, &buckets)
	if err != nil {
		return false, err
	}
	if ok {
		for _, b := range buckets {
			if b.EncryptionAtRestKeyId != nil && *b.EncryptionAtRestKeyId >= 0 {
				return true, nil
			}
		}
	}

	return false, nil
}
