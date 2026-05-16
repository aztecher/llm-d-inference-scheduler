/*
Copyright 2025 The Kubernetes Authors.

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

// Package ttftsloviolation implements a SaturationDetector that returns the
// fraction of pool endpoints predicted to violate the TTFT SLO.
//
// SCOPE: This detector is designed EXCLUSIVELY for the SLOBasedFlexDecider
// activation path. The returned value's unit is "fraction of TTFT-violating
// usable endpoints", NOT "fraction of used capacity" as the SaturationDetector
// interface contract suggests. Plugging this detector into admission control,
// UsageLimitPolicy, or any other SaturationDetector consumer will produce
// semantically incorrect behaviour — use ConcurrencyDetector or
// UtilizationDetector for those use cases.
//
// See docs/poc-design/01-slo-based-flexd-decider.md for the FutureDiscussion
// item on splitting this out into a dedicated SLOPressureSignal interface.
package ttftsloviolation

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-inference-scheduler/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/flowcontrol"
	fwkplugin "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
	framework "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
	attrlatency "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/datalayer/attribute/latency"
)

const (
	// TTFTSLOViolationDetectorType is the type-name of the TTFTSLOViolationDetector plugin.
	TTFTSLOViolationDetectorType = "ttft-slo-violation-detector"
)

// TTFTSLOViolationDetectorFactory defines the factory function for creating
// a new instance of the TTFTSLOViolationDetector.
func TTFTSLOViolationDetectorFactory(
	name string,
	params json.RawMessage,
	handle fwkplugin.Handle,
) (fwkplugin.Plugin, error) {
	var apiCfg apiConfig
	if len(params) > 0 {
		if err := json.Unmarshal(params, &apiCfg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal ttft-slo-violation-detector config: %w", err)
		}
	}
	cfg := buildConfig(&apiCfg)
	return newDetector(name, *cfg, log.FromContext(handle.Context())), nil
}

var (
	_ flowcontrol.SaturationDetector = &detector{}
	_ fwkplugin.ConsumerPlugin       = &detector{}
	_ framework.Filter               = &detector{}
)

// detector reports the fraction of usable pool endpoints predicted to violate the TTFT SLO.
type detector struct {
	config    config
	typedName fwkplugin.TypedName
}

// newDetector initializes a detector and returns its pointer.
func newDetector(name string, cfg config, logger logr.Logger) *detector {
	typedName := fwkplugin.TypedName{
		Type: TTFTSLOViolationDetectorType,
		Name: name,
	}

	pluginLogger := logger.WithName(typedName.String())
	pluginLogger.V(logutil.DEFAULT).Info("Creating new TTFTSLOViolationDetector",
		"suppressOnTPOTViolation", cfg.suppressOnTPOTViolation)

	return &detector{
		config:    cfg,
		typedName: typedName,
	}
}

// TypedName returns the typed name of the plugin.
func (d *detector) TypedName() fwkplugin.TypedName {
	return d.typedName
}

// Consumes declares a dependency on the LatencyPredictionInfo attribute.
func (d *detector) Consumes() map[string]any {
	return map[string]any{
		attrlatency.LatencyPredictionInfoKey: attrlatency.LatencyPredictionInfo{},
	}
}

// Filter is a no-op pass-through. The detector implements framework.Filter so
// it is classified into the SchedulingLayer by DAG ordering, allowing it to
// consume LatencyPredictionInfo produced at the RequestControlLayer.
func (d *detector) Filter(_ context.Context, _ *framework.CycleState, _ *framework.InferenceRequest, endpoints []framework.Endpoint) []framework.Endpoint {
	return endpoints
}

// Saturation returns the fraction of usable pool endpoints predicted to
// violate the TTFT SLO.
func (d *detector) Saturation(ctx context.Context, endpoints []datalayer.Endpoint) float64 {
	logger := log.FromContext(ctx).V(logutil.DEBUG).WithName(TTFTSLOViolationDetectorType)

	if len(endpoints) == 0 {
		return 0
	}

	var usable, violating int
	for _, e := range endpoints {
		info, ok := getLatencyInfo(e)
		if !ok || !d.isUsable(info) {
			// endpoint without a usable prediction is excluded
			continue
		}
		usable++
		if d.isViolating(info) {
			violating++
		}
	}

	if usable == 0 {
		logger.Info(
			"Saturation computed: no usable predictions, any flexible decoders doesn't work as a prefiller",
			"poolSize", len(endpoints))
		return 0
	}

	saturation := float64(violating) / float64(usable)

	logger.Info("Saturation computed",
		"poolSize", len(endpoints),
		"usable", usable,
		"violating", violating,
		"saturation", saturation,
		"suppressOnTPOTViolation", d.config.suppressOnTPOTViolation)

	return saturation
}

// isUsable reports whether the prediction is eligible for aggregation under
// the configured policy.
func (d *detector) isUsable(info *attrlatency.LatencyPredictionInfo) bool {
	if info == nil {
		return false
	}
	// TTFT must always be reliable to judge a TTFT SLO violation.
	if info.TTFT() <= 0 {
		return false
	}
	// Cold-start fingerprint: when the SLO header has not been propagated,
	// the upstream predictor sets headroom = -predicted so that
	// predicted + headroom == 0. Treat as missing.
	if info.TTFT()+info.TTFTHeadroom() <= 0 {
		return false
	}
	if d.config.suppressOnTPOTViolation {
		// TPOT signal is required to apply the suppression rule.
		if info.TPOT() <= 0 {
			return false
		}
		if info.TPOT()+info.TPOTHeadroom() <= 0 {
			return false
		}
	}
	return true
}

// isViolating reports whether the endpoint should be counted as a TTFT SLO
// violator. When SuppressOnTPOTViolation is true and the endpoint also
// violates TPOT, the violation is suppressed.
func (d *detector) isViolating(info *attrlatency.LatencyPredictionInfo) bool {
	if info.TTFTValid() {
		// TTFT within SLO, not a violator.
		return false
	}
	if d.config.suppressOnTPOTViolation && !info.TPOTValid() {
		// Both TTFT and TPOT in violation, suppress to protect decode.
		return false
	}
	return true
}

// getLatencyInfo extracts LatencyPredictionInfo from an endpoint's AttributeMap.
func getLatencyInfo(e datalayer.Endpoint) (*attrlatency.LatencyPredictionInfo, bool) {
	if e == nil {
		return nil, false
	}
	attrs := e.GetAttributes()
	if attrs == nil {
		return nil, false
	}
	raw, ok := attrs.Get(attrlatency.LatencyPredictionInfoKey)
	if !ok {
		return nil, false
	}
	info, ok := raw.(*attrlatency.LatencyPredictionInfo)
	return info, ok
}
