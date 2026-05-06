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

	// FlexDecoderActionSelfDecode is recorded when FlexibleDecoder handles both Prefill and Decode.
	FlexDecoderActionSelfDecode = "self_decode"
	// FlexDecoderActionPrefillAndForward is recorded when FlexibleDecoder handles Prefill
	// and forwards KV cache to a regular Decode pod.
	FlexDecoderActionPrefillAndForward = "prefill_and_forward"
	// FlexDecoderActionNoEndpoints is recorded when FlexibleDecoder was activated by the
	// SLORiskDecider but no FlexibleDecoder pod was available; the request falls back to
	// decode-only routing.
	FlexDecoderActionNoEndpoints = "no_endpoints"
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

	// SchedulerFlexDecoderActivationCount records FlexibleDecoder activation events.
	// This counter is independent of disagg_decision_total and specifically tracks when
	// the SLORiskDecider triggered FlexibleDecoder as a temporary Prefiller for SLO protection.
	// The action label distinguishes the resulting routing mode:
	//   - self_decode:          FlexibleDecoder handled both Prefill and Decode.
	//   - prefill_and_forward:  FlexibleDecoder handled Prefill; a regular Decode pod handled Decode.
	//   - no_endpoints:         FlexibleDecoder was activated but no pod was available; fell back to decode-only.
	SchedulerFlexDecoderActivationCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "flex_decoder_activation_total",
			Help:      metrics.HelpMsgWithStability("Total number of FlexibleDecoder activations for SLO protection", compbasemetrics.ALPHA),
		},
		[]string{"model_name", "action"},
	)

	// SchedulerPoolSaturation records the most recent saturation value computed by a
	// SaturationDetector for a given pool.
	//
	// This is the value that SLORiskDecider compares against the configured threshold.
	// Exposing it directly lets operators verify the decision logic without having to
	// reproduce the calculation in PromQL (which cannot replicate the staleness fallback
	// nor any plugin-internal state).
	//
	// Labels:
	//   - pool:     "prefill" / "decode" / "flex" — which pool the saturation refers to
	//   - detector: name of the SaturationDetector plugin used (e.g. "utilization-detector")
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
			Help:      metrics.HelpMsgWithStability("Configured saturation threshold above which the SLORiskDecider activates FlexibleDecoder", compbasemetrics.ALPHA),
		},
		[]string{"pool", "detector"},
	)

	// SchedulerSLORiskEvaluationCount records every SLORiskDecider evaluation, regardless
	// of outcome. Combined with flex_decoder_activation_total this lets operators compute
	// the rate at which saturation actually crossed the threshold:
	//
	//   rate(slo_risk_evaluations_total{decision="above_threshold"}[5m])
	//     / rate(slo_risk_evaluations_total[5m])
	//
	// Labels:
	//   - decision: "above_threshold" / "below_threshold" / "no_endpoints" / "unwired"
	SchedulerSLORiskEvaluationCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: SchedulerSubsystem,
			Name:      "slo_risk_evaluations_total",
			Help:      metrics.HelpMsgWithStability("Total number of SLORiskDecider evaluations, broken down by outcome", compbasemetrics.ALPHA),
		},
		[]string{"decision"},
	)
)

// SLORiskDecision label values for SchedulerSLORiskEvaluationCount.
const (
	SLORiskDecisionAboveThreshold = "above_threshold"
	SLORiskDecisionBelowThreshold = "below_threshold"
	SLORiskDecisionNoEndpoints    = "no_endpoints"
	SLORiskDecisionUnwired        = "unwired"
)

// GetCollectors returns all custom collectors for the llm-d-inference-scheduler.
func GetCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		SchedulerPDDecisionCount,
		SchedulerDisaggDecisionCount,
		SchedulerFlexDecoderActivationCount,
		SchedulerPoolSaturation,
		SchedulerPoolSaturationThreshold,
		SchedulerSLORiskEvaluationCount,
	}
}

// RecordPoolSaturation updates the saturation gauge for a given pool.
// Call this after every SaturationDetector.Saturation() invocation so dashboards
// can observe the live value the SLORiskDecider is comparing against.
func RecordPoolSaturation(pool, detector string, value float64) {
	SchedulerPoolSaturation.WithLabelValues(pool, detector).Set(value)
}

// RecordPoolSaturationThreshold sets the threshold gauge once per pool.
// Typically called from the SLORiskDecider's constructor.
func RecordPoolSaturationThreshold(pool, detector string, threshold float64) {
	SchedulerPoolSaturationThreshold.WithLabelValues(pool, detector).Set(threshold)
}

// RecordSLORiskEvaluation increments the SLO-risk evaluation counter.
// decision must be one of SLORiskDecision* constants.
func RecordSLORiskEvaluation(decision string) {
	SchedulerSLORiskEvaluationCount.WithLabelValues(decision).Inc()
}

// RecordFlexDecoderActivation increments the FlexibleDecoder activation counter.
// The action must be one of FlexDecoderAction* constants.
func RecordFlexDecoderActivation(modelName, action string) {
	if modelName == "" {
		modelName = "unknown"
	}
	SchedulerFlexDecoderActivationCount.WithLabelValues(modelName, action).Inc()
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
