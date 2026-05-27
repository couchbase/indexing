// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package common

import (
	"fmt"
	"math"
	"strings"
)

// DefaultSparseWeightHistogramBounds is a log-spaced bucket set tuned for
// SPLADE-style raw weights, which sit on a log(1+ReLU(x)) saturated scale.
// Resolution is dense near zero where prune/keep decisions live.
//
// Bucket i covers values in [Bounds[i], Bounds[i+1]). math.MaxFloat32 is used
// in lieu of +Inf as the final boundary because encoding/json cannot encode
// Inf, and the histogram is persisted as part of the codebook payload.
var DefaultSparseWeightHistogramBounds = []float32{
	0, 0.001, 0.005, 0.01, 0.02, 0.03, 0.05, 0.07, 0.10, 0.15,
	0.20, 0.30, 0.50, 0.70, 1.0, 2.0, 5.0,
	math.MaxFloat32,
}

// WeightHistogram is a fixed-bound histogram of sparse vector weights. Built
// at training time over the training set's nonzero values, it lets the
// codebook derive a data-driven prune threshold without sorting or storing
// every weight.
//
// Each bucket tracks both count and L1 mass (sum of |v|) so the threshold
// derivation can target either NNZ retention or L1-mass retention.
//
// Fields are exported to make JSON round-tripping (in codebook persistence)
// straightforward. Callers should treat the struct as opaque and use the
// Observe* / Threshold* / Summary methods.
type WeightHistogram struct {
	Bounds   []float32 `json:"bounds,omitempty"`
	Counts   []uint64  `json:"counts,omitempty"`
	Sums     []float64 `json:"sums,omitempty"`
	TotalObs uint64    `json:"total_obs,omitempty"`
	TotalL1  float64   `json:"total_l1,omitempty"`
}

// NewWeightHistogram returns a histogram with the default SPLADE-tuned bounds.
func NewWeightHistogram() *WeightHistogram {
	return NewWeightHistogramWithBounds(DefaultSparseWeightHistogramBounds)
}

// NewWeightHistogramWithBounds returns a histogram with custom ascending bucket
// boundaries. The last boundary should be +Inf to catch outliers. Provided
// primarily for tests and unusual encoders; production should use the default.
func NewWeightHistogramWithBounds(bounds []float32) *WeightHistogram {
	// Defensive copy so callers can't mutate our buckets after the fact.
	cb := make([]float32, len(bounds))
	copy(cb, bounds)
	return &WeightHistogram{
		Bounds: cb,
		Counts: make([]uint64, len(bounds)-1),
		Sums:   make([]float64, len(bounds)-1),
	}
}

// Observe records a single weight value. The sign is folded via |w|, since
// SPLADE weights are non-negative but generic sparse vectors may carry signs.
// Zero observations are ignored — the concise format only stores nonzero
// entries, so observing zero would just inflate the lowest bucket without
// reflecting reality.
func (h *WeightHistogram) Observe(w float32) {
	if h == nil {
		return
	}
	if w < 0 {
		w = -w
	}
	if w == 0 {
		return
	}
	bucket := h.locate(w)
	h.Counts[bucket]++
	h.Sums[bucket] += float64(w)
	h.TotalObs++
	h.TotalL1 += float64(w)
}

// ObserveConcise records every nonzero value from a concise sparse vector in
// one pass. Format reminder: [size, idx1, ..., idxN, val1, ..., valN].
func (h *WeightHistogram) ObserveConcise(v []float32) {
	if h == nil || len(v) == 0 {
		return
	}
	nnz := int(v[0])
	if nnz == 0 || len(v) < 1+2*nnz {
		return
	}
	values := v[1+nnz : 1+2*nnz]
	for _, w := range values {
		if w < 0 {
			w = -w
		}
		if w == 0 {
			continue
		}
		bucket := h.locate(w)
		h.Counts[bucket]++
		h.Sums[bucket] += float64(w)
		h.TotalObs++
		h.TotalL1 += float64(w)
	}
}

// locate returns the bucket index for w (assumes w > 0). Linear scan is fine
// for the default 17 buckets — branch predictor handles it well and avoids
// the index-arithmetic gotchas of a binary search when bounds aren't power-of-2.
func (h *WeightHistogram) locate(w float32) int {
	n := len(h.Counts)
	for i := 0; i < n; i++ {
		if w < h.Bounds[i+1] {
			return i
		}
	}
	return n - 1
}

// ThresholdForL1Retention returns the τ such that pruning entries with
// |value| < τ retains at least p * TotalL1 of the observed mass. The returned
// retained fraction will be ≈ p (to bucket-uniformity precision) rather than
// snapping to the next coarser bucket boundary.
//
// Implementation: walk buckets ascending, accumulating mass to be dropped.
// When the next bucket would overshoot the drop budget, linearly interpolate
// within that bucket assuming uniform weight distribution to land on the
// exact target. This gives sub-bucket precision and avoids the "stuck at
// 98.6% when target was 95%" failure mode of pure boundary-snapping.
//
// Returns (0, 1.0) when the histogram is empty or p <= 0; returns
// (0, 1.0) when p >= 1 (retain everything → only τ=0 works).
func (h *WeightHistogram) ThresholdForL1Retention(p float64) (float32, float64) {
	if h == nil || h.TotalObs == 0 || p <= 0 {
		return 0, 1.0
	}
	if p >= 1.0 {
		return 0, 1.0
	}

	dropBudget := (1.0 - p) * h.TotalL1
	var dropped float64
	chosen := 0
	for i := 0; i < len(h.Counts); i++ {
		// If we move τ all the way to Bounds[i+1], we drop the full
		// contents of bucket i.
		next := dropped + h.Sums[i]
		if next > dropBudget {
			// Bucket i would overshoot. Pick a τ inside [Bounds[i], Bounds[i+1])
			// that drops exactly the remaining budget. Skip when the bucket
			// is degenerate (zero width or the +Inf sentinel) — fall through
			// to boundary-snapping in that case.
			lo := h.Bounds[i]
			hi := h.Bounds[i+1]
			if h.Sums[i] > 0 && hi > lo && hi < math.MaxFloat32 {
				remaining := dropBudget - dropped
				frac := remaining / h.Sums[i]
				if frac < 0 {
					frac = 0
				} else if frac > 1 {
					frac = 1
				}
				tau := lo + float32(frac)*(hi-lo)
				retained := 1.0 - ((dropped + remaining) / h.TotalL1)
				return tau, retained
			}
			break
		}
		dropped = next
		chosen = i + 1
	}

	tau := h.Bounds[chosen]
	// Cap at the largest finite boundary < MaxFloat32 — using the sentinel
	// itself as τ would prune everything.
	if tau >= math.MaxFloat32 && len(h.Bounds) >= 2 {
		tau = h.Bounds[len(h.Bounds)-2]
	}
	retained := 1.0 - (dropped / h.TotalL1)
	return tau, retained
}

// Summary returns a multi-line human-readable description of the histogram
// suitable for logging at training completion. Format is stable and grep-able.
func (h *WeightHistogram) Summary() string {
	if h == nil || h.TotalObs == 0 {
		return "weight histogram: empty"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "weight histogram: total_obs=%d total_l1=%.3f buckets:",
		h.TotalObs, h.TotalL1)
	var cumCount uint64
	var cumL1 float64
	for i := 0; i < len(h.Counts); i++ {
		cumCount += h.Counts[i]
		cumL1 += h.Sums[i]
		hi := fmt.Sprintf("%.4g", h.Bounds[i+1])
		if h.Bounds[i+1] >= math.MaxFloat32 {
			hi = "Inf"
		}
		fmt.Fprintf(&b, "\n  [%.4g, %s) count=%d (cum %.1f%%) l1=%.3f (cum %.1f%%)",
			h.Bounds[i], hi, h.Counts[i],
			100.0*float64(cumCount)/float64(h.TotalObs),
			h.Sums[i],
			100.0*cumL1/h.TotalL1)
	}
	return b.String()
}
