// Package metrics provides metrics registration for the epp.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	compbasemetrics "k8s.io/component-base/metrics"

	"github.com/llm-d/llm-d-inference-scheduler/pkg/common/observability/metrics"
)

const (
	// SchedulerSubsystem is the metric prefix of the package.
	SchedulerSubsystem = "llm_d_inference_scheduler"

	// DecisionTypeDecodeOnly is for requests that are routed to decode instance only.
	DecisionTypeDecodeOnly = "decode-only"
	// DecisionTypePrefillDecode is for requests that are gone through P/D or EP/D.
	DecisionTypePrefillDecode = "prefill-decode"
	// DecisionTypeEncodeDecode is for requests that are gone through E/PD.
	DecisionTypeEncodeDecode = "encode-decode"
	// DecisionTypeEncodePrefillDecode is for requests that are gone through E/P/D.
	DecisionTypeEncodePrefillDecode = "encode-prefill-decode"

	// FlexibleDecodeActionSelfDecode is recorded when FlexibleDecode handles both Prefill and Decode.
	FlexibleDecodeActionSelfDecode = "self_decode"
	// FlexibleDecodeActionPrefillAndForward is recorded when FlexibleDecode handles Prefill
	// and forwards KV cache to a regular Decode pod.
	FlexibleDecodeActionPrefillAndForward = "prefill_and_forward"
	// FlexibleDecodeActionNoEndpoints is recorded when FlexibleDecode was activated by the
	// SLOBasedFlexDecider but no FlexibleDecode pod was available; the request falls back to
	// decode-only routing.
	FlexibleDecodeActionNoEndpoints = "no_endpoints"
)

var (
	// SchedulerPDDecisionCount records request P/D decision.
	//
	// Deprecated: Use SchedulerDisaggDecisionCount instead.
	SchedulerPDDecisionCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "pd_decision_total",
			Help:      metrics.HelpMsgWithStability("Total number of P/D disaggregation decisions made", compbasemetrics.ALPHA),
		},
		[]string{"model_name", "decision_type"}, // "decode-only" or "prefill-decode"
	)

	// SchedulerDisaggDecisionCount records disaggregation routing decisions,
	// covering all stages: decode-only, prefill-decode, encode-decode, encode-prefill-decode.
	SchedulerDisaggDecisionCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "disagg_decision_total",
			Help:      metrics.HelpMsgWithStability("Total number of disaggregation routing decisions made", compbasemetrics.ALPHA),
		},
		[]string{"model_name", "decision_type"},
	)

	// SchedulerFlexibleDecodeActivationCount records FlexibleDecode activation events.
	// This counter is independent of disagg_decision_total and specifically tracks when
	// the SLOBasedFlexDecider triggered FlexibleDecode as a temporary Prefiller for SLO protection.
	SchedulerFlexibleDecodeActivationCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "flexible_decode_activation_total",
			Help:      metrics.HelpMsgWithStability("Total number of FlexibleDecode activations for SLO protection", compbasemetrics.ALPHA),
		},
		[]string{"model_name", "action"},
	)

	// SchedulerPoolSaturation records the most recent saturation value computed by a SaturationDetector for a given pool.
	SchedulerPoolSaturation = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "pool_saturation",
			Help:      metrics.HelpMsgWithStability("Most recent saturation value computed by a SaturationDetector for a pool", compbasemetrics.ALPHA),
		},
		[]string{"pool", "detector"},
	)

	// SchedulerPoolSaturationThreshold records the configured saturation threshold for a pool.
	// Constant per process lifetime, but exposed as a gauge so dashboards can plot the
	// threshold line alongside the live saturation value without hard-coding it in PromQL.
	SchedulerPoolSaturationThreshold = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "pool_saturation_threshold",
			Help:      metrics.HelpMsgWithStability("Configured saturation threshold above which the SLOBasedFlexDecider activates FlexibleDecode", compbasemetrics.ALPHA),
		},
		[]string{"pool", "detector"},
	)

	// SchedulerFlexibleDecodeActivationEvaluationCount records every SLOBasedFlexDecider evaluation, regardless of outcome.
	// Combined with flexible_decode_activation_total this lets operators compute
	// the rate at which saturation actually crossed the threshold
	SchedulerFlexibleDecodeActivationEvaluationCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "flexible_decode_activation_evaluations_total",
			Help:      metrics.HelpMsgWithStability("Total number of SLOBasedFlexDecider evaluations, broken down by outcome", compbasemetrics.ALPHA),
		},
		[]string{"decision"},
	)
)

// FlexibleDecodeActivation label values for SchedulerFlexibleDecodeActivationEvaluationCount.
const (
	FlexibleDecodeActivationAboveThreshold = "above_threshold"
	FlexibleDecodeActivationBelowThreshold = "below_threshold"
	FlexibleDecodeActivationNoEndpoints    = "no_endpoints"
	FlexibleDecodeActivationUnwired        = "unwired"
)

// GetCollectors returns all custom collectors for the llm-d-inference-scheduler.
func GetCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		SchedulerPDDecisionCount,
		SchedulerDisaggDecisionCount,
		SchedulerFlexibleDecodeActivationCount,
		SchedulerPoolSaturation,
		SchedulerPoolSaturationThreshold,
		SchedulerFlexibleDecodeActivationEvaluationCount,
	}
}

// RecordPoolSaturation updates the saturation gauge for a given pool.
func RecordPoolSaturation(pool, detector string, value float64) {
	SchedulerPoolSaturation.WithLabelValues(pool, detector).Set(value)
}

// RecordPoolSaturationThreshold sets the threshold gauge once per pool.
func RecordPoolSaturationThreshold(pool, detector string, threshold float64) {
	SchedulerPoolSaturationThreshold.WithLabelValues(pool, detector).Set(threshold)
}

// RecordFlexibleDecodeActivationEvaluation increments the SLO-risk evaluation counter.
func RecordFlexibleDecodeActivationEvaluation(decision string) {
	SchedulerFlexibleDecodeActivationEvaluationCount.WithLabelValues(decision).Inc()
}

// RecordFlexibleDecodeActivation increments the FlexibleDecode activation counter.
func RecordFlexibleDecodeActivation(modelName, action string) {
	if modelName == "" {
		modelName = "unknown"
	}
	SchedulerFlexibleDecodeActivationCount.WithLabelValues(modelName, action).Inc()
}

// RecordPDDecision increments the counter for a specific P/D routing decision.
//
// Deprecated: Use RecordDisaggDecision instead.
func RecordPDDecision(modelName, decisionType string) {
	if modelName == "" {
		modelName = "unknown"
	}
	SchedulerPDDecisionCount.WithLabelValues(modelName, decisionType).Inc()
}

// RecordDisaggDecision increments the counter for a disaggregation routing decision.
// The decisionType must be one of the DecisionType* constants (DecisionTypeDecodeOnly,
// DecisionTypePrefillDecode, DecisionTypeEncodeDecode, DecisionTypeEncodePrefillDecode).
// The model parameter should be the target model name; if empty, "unknown" is used.
func RecordDisaggDecision(modelName, decisionType string) {
	if modelName == "" {
		modelName = "unknown"
	}
	SchedulerDisaggDecisionCount.WithLabelValues(modelName, decisionType).Inc()
}

// DisaggDecisionType returns the DecisionType* constant corresponding to which
// disaggregation stages were used for a request.
func DisaggDecisionType(encodeUsed, prefillUsed bool) string {
	switch {
	case encodeUsed && prefillUsed:
		return DecisionTypeEncodePrefillDecode
	case encodeUsed:
		return DecisionTypeEncodeDecode
	case prefillUsed:
		return DecisionTypePrefillDecode
	default:
		return DecisionTypeDecodeOnly
	}
}
