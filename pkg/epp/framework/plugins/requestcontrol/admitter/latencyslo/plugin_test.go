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

package latencyslo

import (
	"context"
	"errors"
	"testing"

	k8stypes "k8s.io/apimachinery/pkg/types"

	errcommon "github.com/llm-d/llm-d-inference-scheduler/pkg/common/error"
	fwkdl "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	schedulingtypes "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
	attrlatency "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/datalayer/attribute/latency"
)

func makeLatencyAdmissionEndpoint(name string, kvCache float64, runningRequests int) schedulingtypes.Endpoint {
	return schedulingtypes.NewEndpoint(
		&fwkdl.EndpointMetadata{NamespacedName: k8stypes.NamespacedName{Name: name}},
		&fwkdl.Metrics{
			KVCacheUsagePercent: kvCache,
			RunningRequestsSize: runningRequests,
		},
		nil,
	)
}

func makeSheddableRequest(ttftSLO, tpotSLO string) *schedulingtypes.InferenceRequest {
	return &schedulingtypes.InferenceRequest{
		Headers: map[string]string{
			ttftSLOHeaderKey: ttftSLO,
			tpotSLOHeaderKey: tpotSLO,
		},
		Objectives: schedulingtypes.RequestObjectives{Priority: -1},
	}
}

func makeNonSheddableRequest(ttftSLO, tpotSLO string) *schedulingtypes.InferenceRequest {
	return &schedulingtypes.InferenceRequest{
		Headers: map[string]string{
			ttftSLOHeaderKey: ttftSLO,
			tpotSLOHeaderKey: tpotSLO,
		},
		Objectives: schedulingtypes.RequestObjectives{Priority: 1},
	}
}

func TestAdmitRequest(t *testing.T) {
	plugin := NewLatencyAdmission(LatencyAdmissionDefaultConfig)

	tests := []struct {
		name      string
		request   *schedulingtypes.InferenceRequest
		endpoints []schedulingtypes.Endpoint
		setupFn   func(endpoints []schedulingtypes.Endpoint) // set endpoint attributes
		wantErr   bool
	}{
		{
			name:    "nil request — admit",
			request: nil,
			wantErr: false,
		},
		{
			name:    "non-sheddable request — always admit",
			request: makeNonSheddableRequest("100", "30"),
			endpoints: []schedulingtypes.Endpoint{
				makeLatencyAdmissionEndpoint("pod1", 0.5, 5),
			},
			setupFn: func(endpoints []schedulingtypes.Endpoint) {
				// All invalid predictions
				endpoints[0].Put(attrlatency.LatencyPredictionInfoKey,
					attrlatency.NewLatencyPredictionInfo(false, false, -50, -10, 150, 40, 0))
			},
			wantErr: false,
		},
		{
			name:    "no SLO headers — admit",
			request: makeSheddableRequest("", ""),
			endpoints: []schedulingtypes.Endpoint{
				makeLatencyAdmissionEndpoint("pod1", 0.5, 5),
			},
			wantErr: false,
		},
		{
			name:    "sheddable, all invalid, all busy, no cold — reject",
			request: makeSheddableRequest("100", "30"),
			endpoints: []schedulingtypes.Endpoint{
				makeLatencyAdmissionEndpoint("pod1", 0.5, 5),
				makeLatencyAdmissionEndpoint("pod2", 0.4, 3),
			},
			setupFn: func(endpoints []schedulingtypes.Endpoint) {
				endpoints[0].Put(attrlatency.LatencyPredictionInfoKey,
					attrlatency.NewLatencyPredictionInfo(false, false, -50, -10, 150, 40, 0))
				endpoints[1].Put(attrlatency.LatencyPredictionInfoKey,
					attrlatency.NewLatencyPredictionInfo(false, false, -30, -5, 130, 35, 0))
			},
			wantErr: true,
		},
		{
			name:    "sheddable, all invalid, but one pod idle — admit",
			request: makeSheddableRequest("100", "30"),
			endpoints: []schedulingtypes.Endpoint{
				makeLatencyAdmissionEndpoint("pod1", 0.5, 5),
				makeLatencyAdmissionEndpoint("pod2", 0.4, 0), // idle
			},
			setupFn: func(endpoints []schedulingtypes.Endpoint) {
				endpoints[0].Put(attrlatency.LatencyPredictionInfoKey,
					attrlatency.NewLatencyPredictionInfo(false, false, -50, -10, 150, 40, 0))
				endpoints[1].Put(attrlatency.LatencyPredictionInfoKey,
					attrlatency.NewLatencyPredictionInfo(false, false, -30, -5, 130, 35, 0))
			},
			wantErr: false,
		},
		{
			name:    "sheddable, all invalid, but cold pod exists — admit",
			request: makeSheddableRequest("100", "30"),
			endpoints: []schedulingtypes.Endpoint{
				makeLatencyAdmissionEndpoint("pod1", 0.5, 5),
				makeLatencyAdmissionEndpoint("pod2", 0.01, 3), // cold
			},
			setupFn: func(endpoints []schedulingtypes.Endpoint) {
				endpoints[0].Put(attrlatency.LatencyPredictionInfoKey,
					attrlatency.NewLatencyPredictionInfo(false, false, -50, -10, 150, 40, 0))
				endpoints[1].Put(attrlatency.LatencyPredictionInfoKey,
					attrlatency.NewLatencyPredictionInfo(false, false, -30, -5, 130, 35, 0))
			},
			wantErr: false,
		},
		{
			name:    "sheddable, one valid endpoint — admit",
			request: makeSheddableRequest("100", "30"),
			endpoints: []schedulingtypes.Endpoint{
				makeLatencyAdmissionEndpoint("pod1", 0.5, 5),
				makeLatencyAdmissionEndpoint("pod2", 0.4, 3),
			},
			setupFn: func(endpoints []schedulingtypes.Endpoint) {
				endpoints[0].Put(attrlatency.LatencyPredictionInfoKey,
					attrlatency.NewLatencyPredictionInfo(false, false, -50, -10, 150, 40, 0))
				endpoints[1].Put(attrlatency.LatencyPredictionInfoKey,
					attrlatency.NewLatencyPredictionInfo(true, true, 20, 5, 80, 25, 0)) // valid
			},
			wantErr: false,
		},
		{
			name:    "sheddable, no prediction data on endpoints — admit (fail-open)",
			request: makeSheddableRequest("100", "30"),
			endpoints: []schedulingtypes.Endpoint{
				makeLatencyAdmissionEndpoint("pod1", 0.5, 5),
			},
			// no setupFn — no latency attributes set
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setupFn != nil {
				tt.setupFn(tt.endpoints)
			}
			err := plugin.AdmitRequest(context.Background(), tt.request, tt.endpoints)
			if (err != nil) != tt.wantErr {
				t.Errorf("AdmitRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHardSLOAdmission_AdmitRequest(t *testing.T) {
	plugin := NewHardSLOAdmission(HardSLOAdmissionDefaultConfig)

	tests := []struct {
		name        string
		config      HardSLOAdmissionConfig
		request     *schedulingtypes.InferenceRequest
		endpoints   []schedulingtypes.Endpoint
		setupFn     func(endpoints []schedulingtypes.Endpoint)
		wantErrCode string
	}{
		{
			name:    "no SLO headers admits",
			config:  HardSLOAdmissionDefaultConfig,
			request: makeNonSheddableRequest("", ""),
			endpoints: []schedulingtypes.Endpoint{
				makeLatencyAdmissionEndpoint("pod1", 0.5, 3),
			},
		},
		{
			name:    "all endpoints violate TTFT rejects with resource exhausted",
			config:  HardSLOAdmissionDefaultConfig,
			request: makeNonSheddableRequest("100", ""),
			endpoints: []schedulingtypes.Endpoint{
				makeLatencyAdmissionEndpoint("pod1", 0.5, 3),
				makeLatencyAdmissionEndpoint("pod2", 0.6, 4),
			},
			setupFn: func(endpoints []schedulingtypes.Endpoint) {
				endpoints[0].Put(attrlatency.LatencyPredictionInfoKey,
					attrlatency.NewLatencyPredictionInfo(false, true, -20, 10, 120, 20, 0))
				endpoints[1].Put(attrlatency.LatencyPredictionInfoKey,
					attrlatency.NewLatencyPredictionInfo(false, true, -30, 10, 130, 20, 0))
			},
			wantErrCode: errcommon.ResourceExhausted,
		},
		{
			name:    "one endpoint satisfies TTFT admits",
			config:  HardSLOAdmissionDefaultConfig,
			request: makeNonSheddableRequest("100", ""),
			endpoints: []schedulingtypes.Endpoint{
				makeLatencyAdmissionEndpoint("pod1", 0.5, 3),
				makeLatencyAdmissionEndpoint("pod2", 0.6, 4),
			},
			setupFn: func(endpoints []schedulingtypes.Endpoint) {
				endpoints[0].Put(attrlatency.LatencyPredictionInfoKey,
					attrlatency.NewLatencyPredictionInfo(false, true, -20, 10, 120, 20, 0))
				endpoints[1].Put(attrlatency.LatencyPredictionInfoKey,
					attrlatency.NewLatencyPredictionInfo(true, true, 20, 10, 80, 20, 0))
			},
		},
		{
			name:    "TPOT check rejects when TPOT SLO is present",
			config:  HardSLOAdmissionDefaultConfig,
			request: makeNonSheddableRequest("", "30"),
			endpoints: []schedulingtypes.Endpoint{
				makeLatencyAdmissionEndpoint("pod1", 0.5, 3),
			},
			setupFn: func(endpoints []schedulingtypes.Endpoint) {
				endpoints[0].Put(attrlatency.LatencyPredictionInfoKey,
					attrlatency.NewLatencyPredictionInfo(true, false, 20, -10, 80, 40, 0))
			},
			wantErrCode: errcommon.ResourceExhausted,
		},
		{
			name: "onlySheddable admits non-sheddable requests",
			config: HardSLOAdmissionConfig{
				RejectOnTTFTViolation:       true,
				RejectOnTPOTViolation:       true,
				OnlySheddable:               true,
				FailOpenOnMissingPrediction: true,
			},
			request: makeNonSheddableRequest("100", ""),
			endpoints: []schedulingtypes.Endpoint{
				makeLatencyAdmissionEndpoint("pod1", 0.5, 3),
			},
			setupFn: func(endpoints []schedulingtypes.Endpoint) {
				endpoints[0].Put(attrlatency.LatencyPredictionInfoKey,
					attrlatency.NewLatencyPredictionInfo(false, true, -20, 10, 120, 20, 0))
			},
		},
		{
			name:    "missing predictions fail open by default",
			config:  HardSLOAdmissionDefaultConfig,
			request: makeNonSheddableRequest("100", ""),
			endpoints: []schedulingtypes.Endpoint{
				makeLatencyAdmissionEndpoint("pod1", 0.5, 3),
			},
		},
		{
			name: "missing predictions can fail closed",
			config: HardSLOAdmissionConfig{
				RejectOnTTFTViolation:       true,
				RejectOnTPOTViolation:       true,
				FailOpenOnMissingPrediction: false,
			},
			request: makeNonSheddableRequest("100", ""),
			endpoints: []schedulingtypes.Endpoint{
				makeLatencyAdmissionEndpoint("pod1", 0.5, 3),
			},
			wantErrCode: errcommon.ResourceExhausted,
		},
		{
			name: "idle fallback admits",
			config: HardSLOAdmissionConfig{
				RejectOnTTFTViolation:       true,
				RejectOnTPOTViolation:       true,
				FailOpenOnMissingPrediction: true,
				AllowIdleFallback:           true,
			},
			request: makeNonSheddableRequest("100", ""),
			endpoints: []schedulingtypes.Endpoint{
				makeLatencyAdmissionEndpoint("pod1", 0.5, 0),
			},
			setupFn: func(endpoints []schedulingtypes.Endpoint) {
				endpoints[0].Put(attrlatency.LatencyPredictionInfoKey,
					attrlatency.NewLatencyPredictionInfo(false, true, -20, 10, 120, 20, 0))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin.config = tt.config
			if tt.setupFn != nil {
				tt.setupFn(tt.endpoints)
			}

			err := plugin.AdmitRequest(context.Background(), tt.request, tt.endpoints)
			if tt.wantErrCode == "" {
				if err != nil {
					t.Fatalf("AdmitRequest() error = %v, want nil", err)
				}
				return
			}

			var e errcommon.Error
			if !errors.As(err, &e) {
				t.Fatalf("AdmitRequest() error = %T %v, want errcommon.Error", err, err)
			}
			if e.Code != tt.wantErrCode {
				t.Fatalf("AdmitRequest() code = %s, want %s", e.Code, tt.wantErrCode)
			}
		})
	}
}
