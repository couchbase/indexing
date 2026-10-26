package client

import (
	"sync/atomic"
	"testing"

	couchbase "github.com/couchbase/indexing/secondary/dcp"
)

func TestClusterCompat(t *testing.T) {
	defer atomic.StoreInt64(&rawClusterCompat, 0)

	if GetClusterCompat() != 0 {
		t.Fatalf("cluster compat %v before any observation, want 0", GetClusterCompat())
	}

	// No nodes: nothing recorded
	UpdateClusterCompat(nil)
	if GetClusterCompat() != 0 {
		t.Fatalf("cluster compat %v after empty pool, want 0", GetClusterCompat())
	}

	// ns_server reports [8,0] as 8*65536 + 0; the lowest node value is used
	UpdateClusterCompat([]couchbase.Node{
		{ClusterCompatibility: 8*65536 + 5},
		{ClusterCompatibility: 8 * 65536},
	})
	tests := []struct {
		major, minor uint32
		want         bool
	}{
		{7, 6, true},
		{7, 7, true},
		{8, 0, true},
		{8, 1, false},
		{8, 5, false},
		{9, 0, false},
	}
	for _, tt := range tests {
		if got := GetClusterCompat() >= ReleaseCompat(tt.major, tt.minor); got != tt.want {
			t.Errorf("compat 8.0 >= %d.%d = %v, want %v", tt.major, tt.minor, got, tt.want)
		}
	}

	// Compat never goes backwards
	UpdateClusterCompat([]couchbase.Node{{ClusterCompatibility: 7*65536 + 7}})
	if GetClusterCompat() != ReleaseCompat(8, 0) {
		t.Errorf("cluster compat moved back to %v", GetClusterCompat())
	}
}
