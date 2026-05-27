// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package common

import (
	"encoding/json"
	"math"
	"math/rand"
	"testing"
)

func TestWeightHistogram_EmptyReturnsZeroThreshold(t *testing.T) {
	h := NewWeightHistogram()
	tau, retained := h.ThresholdForL1Retention(0.95)
	if tau != 0 || retained != 1.0 {
		t.Fatalf("empty hist: want (0, 1.0), got (%v, %v)", tau, retained)
	}
}

func TestWeightHistogram_ObserveBasics(t *testing.T) {
	h := NewWeightHistogram()
	h.Observe(0.5)
	h.Observe(-0.5) // |v| folded
	h.Observe(0)    // ignored
	h.Observe(0.02)

	if h.TotalObs != 3 {
		t.Fatalf("TotalObs: want 3, got %d", h.TotalObs)
	}
	wantL1 := float64(0.5 + 0.5 + 0.02)
	if math.Abs(h.TotalL1-wantL1) > 1e-6 {
		t.Fatalf("TotalL1: want %v, got %v", wantL1, h.TotalL1)
	}
}

func TestWeightHistogram_ObserveConciseMatchesObserve(t *testing.T) {
	h1 := NewWeightHistogram()
	h2 := NewWeightHistogram()

	vals := []float32{0.5, -0.5, 0.02, 0.7}
	// Build concise: [N, idx..., val...]
	concise := []float32{float32(len(vals)), 10, 20, 30, 40}
	concise = append(concise, vals...)

	h1.ObserveConcise(concise)
	for _, v := range vals {
		h2.Observe(v)
	}

	if h1.TotalObs != h2.TotalObs {
		t.Fatalf("TotalObs mismatch: %d vs %d", h1.TotalObs, h2.TotalObs)
	}
	if math.Abs(h1.TotalL1-h2.TotalL1) > 1e-6 {
		t.Fatalf("TotalL1 mismatch: %v vs %v", h1.TotalL1, h2.TotalL1)
	}
	for i := range h1.Counts {
		if h1.Counts[i] != h2.Counts[i] {
			t.Fatalf("Counts[%d] mismatch: %d vs %d", i, h1.Counts[i], h2.Counts[i])
		}
	}
}

func TestWeightHistogram_ThresholdMonotonicity(t *testing.T) {
	// Stricter retention (higher p) must yield smaller or equal τ.
	h := NewWeightHistogram()
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 10000; i++ {
		// Pareto-like: heavy small values, occasional large ones.
		v := float32(rng.ExpFloat64() * 0.1)
		h.Observe(v)
	}

	tau90, _ := h.ThresholdForL1Retention(0.90)
	tau95, _ := h.ThresholdForL1Retention(0.95)
	tau98, _ := h.ThresholdForL1Retention(0.98)
	tau99, _ := h.ThresholdForL1Retention(0.99)

	if !(tau90 >= tau95 && tau95 >= tau98 && tau98 >= tau99) {
		t.Fatalf("thresholds not monotonic: 90=%v 95=%v 98=%v 99=%v",
			tau90, tau95, tau98, tau99)
	}
}

func TestWeightHistogram_ThresholdRetentionIsHonored(t *testing.T) {
	// At chosen τ, actual retained fraction must be >= requested p.
	h := NewWeightHistogram()
	for i := 0; i < 5000; i++ {
		h.Observe(0.01)
	}
	for i := 0; i < 1000; i++ {
		h.Observe(0.5)
	}
	for i := 0; i < 100; i++ {
		h.Observe(2.0)
	}

	for _, p := range []float64{0.50, 0.75, 0.90, 0.95, 0.98, 0.99} {
		_, retained := h.ThresholdForL1Retention(p)
		if retained < p-1e-9 {
			t.Errorf("retention violated at p=%v: got %v", p, retained)
		}
	}
}

// TestWeightHistogram_ThresholdInterpolatesWithinBucket constructs a
// pathological case where the target retention falls in the middle of a
// fat bucket: pure boundary-snapping would land far below target, but
// linear interpolation must land near it. Regression for the case where
// p=0.95 yielded τ=0.01 at 98.6% retained (target 95%).
func TestWeightHistogram_ThresholdInterpolatesWithinBucket(t *testing.T) {
	h := NewWeightHistogram()
	// Pile mass into the [0.01, 0.02) bucket — far more than the drop budget
	// can absorb if we have to take the whole bucket. Plus a small amount of
	// lower-bucket mass so we know boundary-snapping would settle at 0.01.
	for i := 0; i < 100; i++ {
		h.Observe(0.005) // bucket [0.005, 0.01)
	}
	for i := 0; i < 2000; i++ {
		h.Observe(0.015) // bucket [0.01, 0.02)
	}
	for i := 0; i < 50; i++ {
		h.Observe(1.0) // heavy tail so most mass stays above the threshold
	}

	tau, retained := h.ThresholdForL1Retention(0.95)
	if retained < 0.95-1e-6 {
		t.Fatalf("retention floor violated: target 0.95 got %v", retained)
	}
	// Without interpolation this would be 0.01. With interpolation it should
	// be strictly inside [0.01, 0.02).
	if tau <= 0.01 || tau >= 0.02 {
		t.Fatalf("expected interpolated τ in (0.01, 0.02), got %v", tau)
	}
	// Retained fraction should land very close to the target, not far above.
	if retained > 0.96 {
		t.Fatalf("interpolation should land near 0.95, got %v", retained)
	}
}

func TestWeightHistogram_ThresholdFor100PercentIsZero(t *testing.T) {
	h := NewWeightHistogram()
	for i := 0; i < 100; i++ {
		h.Observe(0.5)
	}
	tau, retained := h.ThresholdForL1Retention(1.0)
	if tau != 0 || retained != 1.0 {
		t.Fatalf("p=1: want (0, 1.0), got (%v, %v)", tau, retained)
	}
}

func TestWeightHistogram_ThresholdDoesNotReturnInf(t *testing.T) {
	// Even with extreme drop budgets, returned τ must be finite — otherwise
	// downstream PruneConciseByThreshold would prune everything.
	h := NewWeightHistogram()
	h.Observe(0.001)
	h.Observe(10.0)

	tau, _ := h.ThresholdForL1Retention(0.01) // drop ~99% of mass
	if math.IsInf(float64(tau), 0) {
		t.Fatalf("τ must be finite, got %v", tau)
	}
}

func TestWeightHistogram_JSONRoundTrip(t *testing.T) {
	h := NewWeightHistogram()
	for i := 0; i < 50; i++ {
		h.Observe(float32(i) * 0.01)
	}

	data, err := json.Marshal(h)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var h2 WeightHistogram
	if err := json.Unmarshal(data, &h2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if h.TotalObs != h2.TotalObs || h.TotalL1 != h2.TotalL1 {
		t.Fatalf("round-trip mismatch on totals")
	}
	for i := range h.Counts {
		if h.Counts[i] != h2.Counts[i] {
			t.Fatalf("round-trip mismatch on Counts[%d]", i)
		}
		if h.Sums[i] != h2.Sums[i] {
			t.Fatalf("round-trip mismatch on Sums[%d]", i)
		}
	}

	// And the recovered histogram must still derive the same τ.
	tau1, _ := h.ThresholdForL1Retention(0.95)
	tau2, _ := h2.ThresholdForL1Retention(0.95)
	if tau1 != tau2 {
		t.Fatalf("τ mismatch after round-trip: %v vs %v", tau1, tau2)
	}
}

func TestWeightHistogram_NilSafe(t *testing.T) {
	var h *WeightHistogram
	h.Observe(0.5)                                  // must not panic
	h.ObserveConcise([]float32{1, 10, 0.5})         // must not panic
	tau, retained := h.ThresholdForL1Retention(0.9) // must not panic
	if tau != 0 || retained != 1.0 {
		t.Fatalf("nil hist: want (0, 1.0)")
	}
	if s := h.Summary(); s == "" {
		t.Fatalf("Summary on nil should return non-empty placeholder")
	}
}

func TestWeightHistogram_CustomBounds(t *testing.T) {
	bounds := []float32{0, 1.0, 2.0, float32(math.Inf(1))}
	h := NewWeightHistogramWithBounds(bounds)
	h.Observe(0.5)
	h.Observe(1.5)
	h.Observe(3.0)

	if h.Counts[0] != 1 || h.Counts[1] != 1 || h.Counts[2] != 1 {
		t.Fatalf("custom bounds: counts=%v", h.Counts)
	}
}

func TestWeightHistogram_SummaryFormatStable(t *testing.T) {
	h := NewWeightHistogram()
	h.Observe(0.5)
	s := h.Summary()
	// Just sanity — contains expected anchors.
	for _, want := range []string{"weight histogram", "total_obs=1", "buckets:"} {
		if !contains(s, want) {
			t.Fatalf("Summary missing %q: %q", want, s)
		}
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
