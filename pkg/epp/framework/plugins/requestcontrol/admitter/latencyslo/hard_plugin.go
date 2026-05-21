/*
Copyright 2026 The Kubernetes Authors.

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

package latencyslo

import (
	"context"
	"encoding/json"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/log"

	errcommon "github.com/llm-d/llm-d-inference-scheduler/pkg/common/error"
	logutil "github.com/llm-d/llm-d-inference-scheduler/pkg/common/observability/logging"
	fwkplugin "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/requestcontrol"
	schedulingtypes "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
	attrlatency "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/datalayer/attribute/latency"
	requtil "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/util/request"
)

const HardSLOAdmissionPluginType = "hard-slo-admitter"

var _ requestcontrol.Admitter = &HardSLOAdmission{}

type HardSLOAdmissionConfig struct {
	RejectOnTTFTViolation       bool    `json:"rejectOnTTFTViolation"`
	RejectOnTPOTViolation       bool    `json:"rejectOnTPOTViolation"`
	OnlySheddable               bool    `json:"onlySheddable"`
	FailOpenOnMissingPrediction bool    `json:"failOpenOnMissingPrediction"`
	AllowIdleFallback           bool    `json:"allowIdleFallback"`
	AllowColdFallback           bool    `json:"allowColdFallback"`
	ColdKVCacheThreshold        float64 `json:"coldKVCacheThreshold"`
}

var HardSLOAdmissionDefaultConfig = HardSLOAdmissionConfig{
	RejectOnTTFTViolation:       true,
	RejectOnTPOTViolation:       true,
	OnlySheddable:               false,
	FailOpenOnMissingPrediction: true,
	AllowIdleFallback:           false,
	AllowColdFallback:           false,
	ColdKVCacheThreshold:        0.02,
}

// HardSLOAdmission rejects requests before scheduling when every candidate endpoint
// is predicted to violate the configured hard latency SLO.
type HardSLOAdmission struct {
	typedName fwkplugin.TypedName
	config    HardSLOAdmissionConfig
}

func HardSLOAdmissionFactory(name string, rawParameters json.RawMessage, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	config := HardSLOAdmissionDefaultConfig
	if len(rawParameters) > 0 {
		if err := json.Unmarshal(rawParameters, &config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal config for HardSLOAdmission: %w", err)
		}
	}
	if !config.RejectOnTTFTViolation && !config.RejectOnTPOTViolation {
		return nil, fmt.Errorf("at least one of rejectOnTTFTViolation or rejectOnTPOTViolation must be true")
	}
	if config.ColdKVCacheThreshold < 0 {
		return nil, fmt.Errorf("coldKVCacheThreshold must be >= 0, got %f", config.ColdKVCacheThreshold)
	}
	return NewHardSLOAdmission(config).WithName(name), nil
}

func NewHardSLOAdmission(config HardSLOAdmissionConfig) *HardSLOAdmission {
	return &HardSLOAdmission{
		typedName: fwkplugin.TypedName{Type: HardSLOAdmissionPluginType, Name: HardSLOAdmissionPluginType},
		config:    config,
	}
}

func (p *HardSLOAdmission) WithName(name string) *HardSLOAdmission {
	p.typedName.Name = name
	return p
}

func (p *HardSLOAdmission) TypedName() fwkplugin.TypedName {
	return p.typedName
}

func (p *HardSLOAdmission) Consumes() map[string]any {
	return map[string]any{
		attrlatency.LatencyPredictionInfoKey: attrlatency.LatencyPredictionInfo{},
	}
}

func (p *HardSLOAdmission) AdmitRequest(ctx context.Context, request *schedulingtypes.InferenceRequest, endpoints []schedulingtypes.Endpoint) error {
	if request == nil {
		return nil
	}
	if p.config.OnlySheddable && !requtil.IsSheddable(request.Objectives.Priority) {
		return nil
	}

	checkTTFT := p.config.RejectOnTTFTViolation && parseFloatHeaderValue(request.Headers[ttftSLOHeaderKey]) > 0
	checkTPOT := p.config.RejectOnTPOTViolation && parseFloatHeaderValue(request.Headers[tpotSLOHeaderKey]) > 0
	if !checkTTFT && !checkTPOT {
		return nil
	}

	hasPrediction := false
	for _, endpoint := range endpoints {
		if metrics := endpoint.GetMetrics(); metrics != nil {
			if p.config.AllowIdleFallback && metrics.RunningRequestsSize == 0 {
				return nil
			}
			if p.config.AllowColdFallback && metrics.KVCacheUsagePercent < p.config.ColdKVCacheThreshold {
				return nil
			}
		}

		latencyInfoRaw, ok := endpoint.Get(attrlatency.LatencyPredictionInfoKey)
		if !ok {
			continue
		}
		latencyInfo, ok := latencyInfoRaw.(*attrlatency.LatencyPredictionInfo)
		if !ok || latencyInfo == nil {
			continue
		}

		hasPrediction = true
		if endpointSatisfiesHardSLO(latencyInfo, checkTTFT, checkTPOT) {
			return nil
		}
	}

	if !hasPrediction && p.config.FailOpenOnMissingPrediction {
		return nil
	}

	log.FromContext(ctx).V(logutil.DEBUG).Info("HardSLOAdmission: rejecting request, no endpoint can satisfy hard SLO",
		"endpoints", len(endpoints), "checkTTFT", checkTTFT, "checkTPOT", checkTPOT)
	return errcommon.Error{
		Code: errcommon.ResourceExhausted,
		Msg:  "no endpoint can satisfy configured hard latency SLO",
	}
}

func endpointSatisfiesHardSLO(latencyInfo *attrlatency.LatencyPredictionInfo, checkTTFT, checkTPOT bool) bool {
	if checkTTFT && !latencyInfo.TTFTValid() {
		return false
	}
	if checkTPOT && !latencyInfo.TPOTValid() {
		return false
	}
	return true
}
