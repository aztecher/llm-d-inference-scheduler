/*
Copyright 2025 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package metrics defines Prometheus metrics for the routing sidecar.
//
// These metrics make the sidecar's behaviour observable for PoC verification:
//   - prefill_fallback_total: silent fallback path is now visible
//   - prefill_requests_total: per-prefiller request accounting
//   - pd_protocol_total:      disaggregation vs passthrough breakdown
//
// Register the collectors returned by GetCollectors() with the default
// prometheus registerer at sidecar startup.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// SidecarSubsystem is the Prometheus metric subsystem prefix for the sidecar.
const SidecarSubsystem = "llm_d_sidecar"

// Reason label values for PrefillFallbackCount.
const (
	// FallbackReason5xx indicates the prefiller responded with a 5xx HTTP code.
	FallbackReason5xx = "5xx"
	// FallbackReasonClientError indicates a 4xx outside the silent-fallback range.
	// Currently shouldFallbackToDecode treats 4xx as terminal so this is unused
	// in practice, but the label is reserved for future cases.
	FallbackReasonClientError = "4xx"
	// FallbackReasonOther covers timeouts, connection refused, EOF, etc., which
	// shouldFallbackToDecode also routes to fallback.
	FallbackReasonOther = "other"
)

// Path label values for PDProtocolCount.
const (
	// PathDisagg indicates the request went through the full PD protocol
	// (i.e. x-prefiller-host-port was set and Prefiller was contacted).
	PathDisagg = "disagg"
	// PathPassthrough indicates the request was forwarded directly to the local
	// decoder without contacting any Prefiller.
	PathPassthrough = "passthrough"
	// PathFallback indicates the sidecar attempted PD but fell back to local
	// decode after a Prefiller error. This is the silent failure that was
	// previously invisible.
	PathFallback = "fallback"
)

// Status label values for PrefillRequestCount.
const (
	StatusOK      = "ok"
	StatusError   = "error"
	StatusTimeout = "timeout"
)

var (
	// PrefillFallbackCount counts how many times the sidecar's PD path silently
	// fell back to local decode because the Prefiller failed.
	//
	// IMPORTANT: A non-zero value here means the EPP saw a "prefill-decode"
	// decision but the actual processing was decode-only. It explains the
	// discrepancy between EPP's disagg_decision_total and observed Prefill load.
	PrefillFallbackCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: SidecarSubsystem,
			Name:      "prefill_fallback_total",
			Help: "Number of times the sidecar's PD path fell back to local decode " +
				"after a Prefiller failure. Non-zero values indicate silent disagg bypass.",
		},
		[]string{"reason"},
	)

	// PrefillRequestCount counts requests forwarded to a Prefiller, keyed by
	// the target host:port and outcome. Lets operators verify which Prefill pod
	// each request landed on.
	PrefillRequestCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: SidecarSubsystem,
			Name:      "prefill_requests_total",
			Help:      "Number of prefill stage requests forwarded to a Prefiller, broken down by target and status.",
		},
		[]string{"prefiller", "status"},
	)

	// PDProtocolCount tracks the high-level routing decision per request:
	// disagg (full PD protocol), passthrough (no x-prefiller-host-port header),
	// or fallback (PD attempted but the prefiller failed).
	PDProtocolCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: SidecarSubsystem,
			Name:      "pd_protocol_total",
			Help:      "Number of requests by PD routing path: disagg, passthrough, or fallback.",
		},
		[]string{"path"},
	)

	// PrefillDuration tracks how long each prefill stage took, end-to-end as
	// observed by the sidecar (network + remote prefill compute + KV register).
	PrefillDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: SidecarSubsystem,
			Name:      "prefill_duration_seconds",
			Help:      "Duration of the prefill stage as seen by the sidecar (network + Prefiller compute).",
			Buckets:   prometheus.ExponentialBuckets(0.005, 2, 12), // 5ms..~10s
		},
		[]string{"prefiller"},
	)
)

// GetCollectors returns all collectors that should be registered with the
// Prometheus registry on sidecar startup.
func GetCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		PrefillFallbackCount,
		PrefillRequestCount,
		PDProtocolCount,
		PrefillDuration,
	}
}

// RecordPrefillFallback increments the silent-fallback counter for the given reason.
func RecordPrefillFallback(reason string) {
	PrefillFallbackCount.WithLabelValues(reason).Inc()
}

// RecordPrefillRequest increments the per-prefiller request counter with the given status.
func RecordPrefillRequest(prefiller, status string) {
	PrefillRequestCount.WithLabelValues(prefiller, status).Inc()
}

// RecordPDProtocol increments the path counter for a routing decision.
func RecordPDProtocol(path string) {
	PDProtocolCount.WithLabelValues(path).Inc()
}

// ObservePrefillDuration records the prefill stage duration in seconds.
func ObservePrefillDuration(prefiller string, seconds float64) {
	PrefillDuration.WithLabelValues(prefiller).Observe(seconds)
}
