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

package ttftsloviolation

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
	schedulingtypes "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
	attrlatency "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/datalayer/attribute/latency"
)

func makeEndpointWithPrediction(name string, info *attrlatency.LatencyPredictionInfo) fwkdl.Endpoint {
	meta := &fwkdl.EndpointMetadata{
		NamespacedName: types.NamespacedName{Name: name, Namespace: "ns1"},
	}
	ep := fwkdl.NewEndpoint(meta, fwkdl.NewMetrics())
	if info != nil {
		ep.GetAttributes().Put(attrlatency.LatencyPredictionInfoKey, info)
	}
	return ep
}

func makeSchedulingEndpointWithPrediction(name string, info *attrlatency.LatencyPredictionInfo) schedulingtypes.Endpoint {
	meta := &fwkdl.EndpointMetadata{
		NamespacedName: types.NamespacedName{Name: name, Namespace: "ns1"},
	}
	attrs := fwkdl.NewAttributes()
	if info != nil {
		attrs.Put(attrlatency.LatencyPredictionInfoKey, info)
	}
	return schedulingtypes.NewEndpoint(meta, fwkdl.NewMetrics(), attrs)
}

// pred constructs a LatencyPredictionInfo. All durations are in milliseconds.
func pred(ttft, ttftHeadroom, tpot, tpotHeadroom float64, ttftValid, tpotValid bool) *attrlatency.LatencyPredictionInfo {
	return attrlatency.NewLatencyPredictionInfo(ttftValid, tpotValid, ttftHeadroom, tpotHeadroom, ttft, tpot, 0)
}

// TestTTFTSLOViolationDetectorFactory evaluates instantiation properties and
// config parsing constraints. It guards against improper configuration block
// parameters failing initialization correctly.
func TestTTFTSLOViolationDetectorFactory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configJSON []byte
		wantError  bool
	}{
		{
			name:       "valid empty configuration",
			configJSON: []byte(`{}`),
			wantError:  false,
		},
		{
			name:       "valid suppressOnTPOTViolation true",
			configJSON: []byte(`{"suppressOnTPOTViolation": true}`),
			wantError:  false,
		},
		{
			name:       "nil parameters applies defaults",
			configJSON: nil,
			wantError:  false,
		},
		{
			name:       "invalid schema",
			configJSON: []byte(`{"suppressOnTPOTViolation": "not-a-bool"}`),
			wantError:  true,
		},
		{
			name:       "malformed json",
			configJSON: []byte(`{"suppressOnTPOTViolation":`),
			wantError:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			plugin, err := TTFTSLOViolationDetectorFactory("test-ttft-slo-violation-detector",
				tc.configJSON, fwkplugin.NewEppHandle(t.Context(), func() []types.NamespacedName { return nil }))
			if tc.wantError {
				require.Error(t, err, "Expected initialization to fail on invalid configuration")
				require.Nil(t, plugin, "Plugin must be nil when initialization fails")
			} else {
				require.NoError(t, err, "Expected initialization to succeed with valid configuration")
				require.NotNil(t, plugin, "Plugin must not be nil on success")
			}
		})
	}
}

// TestDetector_TypedName provides structural assurance that initialization assigns proper types.
func TestDetector_TypedName(t *testing.T) {
	t.Parallel()
	plugin, err := TTFTSLOViolationDetectorFactory("test-plugin", []byte(`{}`),
		fwkplugin.NewEppHandle(t.Context(), func() []types.NamespacedName { return nil }))
	require.NoError(t, err, "Plugin initialization should succeed")
	require.Equal(t, "test-plugin", plugin.TypedName().Name,
		"TypedName must match the name provided during initialization")
	require.Equal(t, "ttft-slo-violation-detector", plugin.TypedName().Type,
		"TypedName.Type must be exactly 'ttft-slo-violation-detector'")
}

// TestDetector_Consumes confirms the LatencyPredictionInfo dependency declaration
// that places the detector into the SchedulingLayer.
func TestDetector_Consumes(t *testing.T) {
	t.Parallel()
	detector := newDetector("test-detector", config{}, logr.Discard())
	consumes := detector.Consumes()
	_, ok := consumes[attrlatency.LatencyPredictionInfoKey]
	require.True(t, ok, "Detector must declare LatencyPredictionInfoKey as a consumed attribute")
}

func TestDetector_Saturation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                    string
		suppressOnTPOTViolation bool
		endpoints               []fwkdl.Endpoint
		wantSaturation          float64
	}{
		{
			name:           "Empty pool returns zero",
			endpoints:      []fwkdl.Endpoint{},
			wantSaturation: 0.0,
		},
		{
			name: "All endpoints missing predictions",
			endpoints: []fwkdl.Endpoint{
				makeEndpointWithPrediction("pod1", nil),
				makeEndpointWithPrediction("pod2", nil),
				makeEndpointWithPrediction("pod3", nil),
			},
			wantSaturation: 0.0,
		},
		{
			name: "All TTFT valid",
			endpoints: []fwkdl.Endpoint{
				// SLO=500, predicted=100 and 200 → both healthy.
				makeEndpointWithPrediction("pod1", pred(100, 400, 0, 0, true, true)),
				makeEndpointWithPrediction("pod2", pred(200, 300, 0, 0, true, true)),
			},
			wantSaturation: 0.0,
		},
		{
			name: "All TTFT violating",
			endpoints: []fwkdl.Endpoint{
				makeEndpointWithPrediction("pod1", pred(600, -100, 0, 0, false, true)),
				makeEndpointWithPrediction("pod2", pred(700, -200, 0, 0, false, true)),
			},
			wantSaturation: 1.0,
		},
		{
			name: "Half TTFT violating",
			endpoints: []fwkdl.Endpoint{
				makeEndpointWithPrediction("pod1", pred(100, 400, 0, 0, true, true)),
				makeEndpointWithPrediction("pod2", pred(600, -100, 0, 0, false, true)),
			},
			wantSaturation: 0.5,
		},
		{
			name: "Endpoint with no attribute excluded",
			endpoints: []fwkdl.Endpoint{
				makeEndpointWithPrediction("pod1", pred(600, -100, 0, 0, false, true)),
				makeEndpointWithPrediction("pod2", nil),
			},
			wantSaturation: 1.0,
		},
		{
			name: "Zero TTFT excluded as missing",
			endpoints: []fwkdl.Endpoint{
				// Predictor returned no value at all.
				makeEndpointWithPrediction("pod1", pred(0, 0, 0, 0, false, false)),
			},
			wantSaturation: 0.0,
		},
		{
			name: "Cold-start fingerprint excluded",
			endpoints: []fwkdl.Endpoint{
				// SLO header not propagated: predicted + headroom == 0 (recovered SLO=0).
				makeEndpointWithPrediction("pod1", pred(46657, -46657, 0, 0, false, true)),
			},
			wantSaturation: 0.0,
		},
		{
			name: "Missing mixed with one violator",
			endpoints: []fwkdl.Endpoint{
				makeEndpointWithPrediction("pod1", pred(100, 400, 0, 0, true, true)),
				makeEndpointWithPrediction("pod2", nil),
				makeEndpointWithPrediction("pod3", pred(600, -100, 0, 0, false, true)),
			},
			// 1 violator out of 2 usable (pod2 excluded).
			wantSaturation: 0.5,
		},
		{
			name:                    "Suppress off: TPOT violation does not gate TTFT",
			suppressOnTPOTViolation: false,
			endpoints: []fwkdl.Endpoint{
				// Both axes violating; with suppress off this still counts.
				makeEndpointWithPrediction("pod1", pred(600, -100, 30, -10, false, false)),
			},
			wantSaturation: 1.0,
		},
		{
			name:                    "Suppress on: both TTFT and TPOT violating excluded as violator",
			suppressOnTPOTViolation: true,
			endpoints: []fwkdl.Endpoint{
				makeEndpointWithPrediction("pod1", pred(600, -100, 30, -10, false, false)),
			},
			wantSaturation: 0.0,
		},
		{
			name:                    "Suppress on: TTFT-only violator counted",
			suppressOnTPOTViolation: true,
			endpoints: []fwkdl.Endpoint{
				makeEndpointWithPrediction("pod1", pred(600, -100, 30, 20, false, true)),
			},
			wantSaturation: 1.0,
		},
		{
			name:                    "Suppress on: TTFT healthy not a violator",
			suppressOnTPOTViolation: true,
			endpoints: []fwkdl.Endpoint{
				makeEndpointWithPrediction("pod1", pred(100, 400, 30, -10, true, false)),
			},
			wantSaturation: 0.0,
		},
		{
			name:                    "Suppress on: mixed pool",
			suppressOnTPOTViolation: true,
			endpoints: []fwkdl.Endpoint{
				// healthy.
				makeEndpointWithPrediction("pod1", pred(100, 400, 30, 20, true, true)),
				// TTFT-only violator, counted.
				makeEndpointWithPrediction("pod2", pred(600, -100, 30, 20, false, true)),
				// Both axes violating, suppressed.
				makeEndpointWithPrediction("pod3", pred(600, -100, 30, -10, false, false)),
			},
			wantSaturation: 1.0 / 3.0,
		},
		{
			name:                    "Suppress on: missing TPOT excludes endpoint",
			suppressOnTPOTViolation: true,
			endpoints: []fwkdl.Endpoint{
				// TPOT==0 → unusable when suppress is on.
				makeEndpointWithPrediction("pod1", pred(600, -100, 0, 0, false, false)),
				// Fully usable; TTFT violating; TPOT OK.
				makeEndpointWithPrediction("pod2", pred(600, -100, 30, 20, false, true)),
			},
			wantSaturation: 1.0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			detector := newDetector("test-detector",
				config{suppressOnTPOTViolation: tc.suppressOnTPOTViolation},
				logr.Discard())

			got := detector.Saturation(context.Background(), tc.endpoints)
			require.InDelta(t, tc.wantSaturation, got, 1e-9, "Saturation mismatch")
		})
	}
}

// TestDetector_Filter confirms that Filter is a no-op pass-through.
func TestDetector_Filter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		endpoints []schedulingtypes.Endpoint
		wantLen   int
	}{
		{
			name:      "nil input",
			endpoints: nil,
			wantLen:   0,
		},
		{
			name:      "empty input",
			endpoints: []schedulingtypes.Endpoint{},
			wantLen:   0,
		},
		{
			name: "mixed predictions passes all through",
			endpoints: []schedulingtypes.Endpoint{
				makeSchedulingEndpointWithPrediction("pod1", pred(100, 400, 0, 0, true, true)),
				makeSchedulingEndpointWithPrediction("pod2", pred(700, -200, 0, 0, false, true)),
				makeSchedulingEndpointWithPrediction("pod3", nil),
			},
			wantLen: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			detector := newDetector("test-detector", config{}, logr.Discard())
			got := detector.Filter(context.Background(), nil, nil, tc.endpoints)
			require.Len(t, got, tc.wantLen)
		})
	}
}
