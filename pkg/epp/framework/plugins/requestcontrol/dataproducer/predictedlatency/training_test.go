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

package predictedlatency

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/types"
	latencypredictor "sigs.k8s.io/gateway-api-inference-extension/sidecars/latencypredictorasync"

	fwkdl "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	fwkrh "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/requesthandling"
	schedulingtypes "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
)

func TestBulkPredictWithMetrics(t *testing.T) {
	mockPredictor := &mockPredictor{
		predictions: map[string]*latencypredictor.PredictionResponse{
			"0.5": {TTFT: 0.5, TPOT: 0.03},
			"0.6": {TTFT: 0.6, TPOT: 0.04},
		},
	}

	metricsStates := []*fwkdl.Metrics{
		{KVCacheUsagePercent: 0.5},
		{KVCacheUsagePercent: 0.6},
	}
	pods := []*fwkdl.EndpointMetadata{
		{
			NamespacedName: types.NamespacedName{Namespace: "default", Name: "pod1"},
		},
		{
			NamespacedName: types.NamespacedName{Namespace: "default", Name: "pod2"},
		},
	}
	prompts := []string{"prompt1", "prompt2"}
	generatedTokenCounts := []int{1, 1}
	prefixCacheScores := []float64{0.0, 0.0}

	results, err := bulkPredictWithMetrics(context.Background(), nil, mockPredictor, metricsStates, "", pods, prompts, generatedTokenCounts, prefixCacheScores, nil)

	assert.NoError(t, err)
	assert.Len(t, results, 2)
	assert.Equal(t, 0.5, results[0].TTFT)
	assert.Equal(t, 0.03, results[0].TPOT)
	assert.Equal(t, 0.6, results[1].TTFT)
	assert.Equal(t, 0.04, results[1].TPOT)
}

func TestBulkPredictWithMetrics_Error(t *testing.T) {
	mockPredictor := &mockPredictor{
		err: errors.New("prediction failed"),
	}

	metricsStates := []*fwkdl.Metrics{
		{KVCacheUsagePercent: 0.5},
	}
	pods := []*fwkdl.EndpointMetadata{
		{
			NamespacedName: types.NamespacedName{Namespace: "default", Name: "pod1"},
		},
	}
	prompts := []string{"prompt1"}
	generatedTokenCounts := []int{1}
	prefixCacheScores := []float64{0.0}

	results, err := bulkPredictWithMetrics(context.Background(), nil, mockPredictor, metricsStates, "", pods, prompts, generatedTokenCounts, prefixCacheScores, nil)

	assert.Error(t, err)
	assert.Nil(t, results)
}

func TestBulkPredictWithMetrics_InputMismatch(t *testing.T) {
	mockPredictor := &mockPredictor{}
	metricsStates := []*fwkdl.Metrics{{}}
	pods := []*fwkdl.EndpointMetadata{
		{
			NamespacedName: types.NamespacedName{Namespace: "default", Name: "pod1"},
		},
	}
	prompts := []string{"prompt1", "prompt2"} // Mismatch length
	generatedTokenCounts := []int{1}
	prefixCacheScores := []float64{0.0}

	results, err := bulkPredictWithMetrics(context.Background(), nil, mockPredictor, metricsStates, "", pods, prompts, generatedTokenCounts, prefixCacheScores, nil)

	assert.Error(t, err)
	assert.Nil(t, results)
	assert.True(t, strings.Contains(err.Error(), "input slice lengths must match"))
}

func TestBulkPredictWithMetrics_WithPredictedLatencyCtx(t *testing.T) {
	mockPredictor := &mockPredictor{
		predictions: map[string]*latencypredictor.PredictionResponse{
			"0.5": {TTFT: 0.5, TPOT: 0.03},
		},
	}

	metricsStates := []*fwkdl.Metrics{
		{KVCacheUsagePercent: 0.5},
	}
	pods := []*fwkdl.EndpointMetadata{
		{
			NamespacedName: types.NamespacedName{Namespace: "default", Name: "pod1"},
		},
	}
	prompts := []string{"prompt1"}
	generatedTokenCounts := []int{1}
	prefixCacheScores := []float64{0.0}

	plCtx := &predictedLatencyCtx{
		schedulingRequest: schedulingtypes.InferenceRequest{
			TargetModel: "test-model",
		},
		incomingModelName: "incoming-model",
	}

	results, err := bulkPredictWithMetrics(context.Background(), plCtx, mockPredictor, metricsStates, "", pods, prompts, generatedTokenCounts, prefixCacheScores, nil)

	assert.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, 0.5, results[0].TTFT)
	assert.Equal(t, 0.03, results[0].TPOT)
}

func TestBulkPredictWithMetrics_ChatCompletionsPrompt(t *testing.T) {
	mp := &mockPredictor{
		predictions: map[string]*latencypredictor.PredictionResponse{
			"0.5": {TTFT: 0.5, TPOT: 0.03},
		},
	}

	metricsStates := []*fwkdl.Metrics{{KVCacheUsagePercent: 0.5}}
	pods := []*fwkdl.EndpointMetadata{
		{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pod1"}},
	}

	chatBody := &fwkrh.InferenceRequestBody{
		ChatCompletions: &fwkrh.ChatCompletionsRequest{
			Messages: []fwkrh.Message{
				{Role: "user", Content: fwkrh.Content{Raw: "Hello world"}},
			},
		},
	}
	prompts := []string{chatBody.PromptText()}
	generatedTokenCounts := []int{1}
	prefixCacheScores := []float64{0.0}

	results, err := bulkPredictWithMetrics(context.Background(), nil, mp, metricsStates, "", pods, prompts, generatedTokenCounts, prefixCacheScores, []int64{0})

	assert.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, 0.5, results[0].TTFT)
}

func TestBulkPredictWithMetrics_NilMetricsState(t *testing.T) {
	mockPredictor := &mockPredictor{}
	metricsStates := []*fwkdl.Metrics{nil} // Nil metrics state
	pods := []*fwkdl.EndpointMetadata{
		{
			NamespacedName: types.NamespacedName{Namespace: "default", Name: "pod1"},
		},
	}
	prompts := []string{"prompt1"}
	generatedTokenCounts := []int{1}
	prefixCacheScores := []float64{0.0}

	results, err := bulkPredictWithMetrics(context.Background(), nil, mockPredictor, metricsStates, "", pods, prompts, generatedTokenCounts, prefixCacheScores, nil)

	assert.Error(t, err)
	assert.Nil(t, results)
	assert.True(t, strings.Contains(err.Error(), "metrics state at index 0 cannot be nil"))
}

// TestNormalizePodType verifies pod_type values are mapped to the trainer's
// accepted vocabulary ("", "prefill", "decode"); other values would NaN.
func TestNormalizePodType(t *testing.T) {
	tests := []struct {
		name string
		role string
		want string
	}{
		{name: "empty role passes through", role: "", want: ""},
		{name: "prefill role preserved", role: "prefill", want: "prefill"},
		{name: "decode role preserved", role: "decode", want: "decode"},
		{name: "flexible-decode mapped to prefill", role: "flexible-decode", want: "decode"},
		{name: "unknown role mapped to monolithic", role: "encode-prefill-decode", want: ""},
		{name: "arbitrary string mapped to monolithic", role: "some-future-role", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, normalizePodType(tc.role))
		})
	}
}

// TestBuildPredictionRequest_PodType exercises the label-extraction guard and normalisation end-to-end.
func TestBuildPredictionRequest_PodType(t *testing.T) {
	tests := []struct {
		name              string
		endpointRoleLabel string
		meta              *fwkdl.EndpointMetadata
		want              string
	}{
		{name: "no role label configured returns empty", endpointRoleLabel: "", meta: &fwkdl.EndpointMetadata{Labels: map[string]string{"llm-d.ai/role": "prefill"}}, want: ""},
		{name: "nil metadata returns empty", endpointRoleLabel: "llm-d.ai/role", meta: nil, want: ""},
		{name: "nil labels returns empty", endpointRoleLabel: "llm-d.ai/role", meta: &fwkdl.EndpointMetadata{}, want: ""},
		{name: "missing label returns empty", endpointRoleLabel: "llm-d.ai/role", meta: &fwkdl.EndpointMetadata{Labels: map[string]string{"other": "decode"}}, want: ""},
		{name: "prefill label preserved", endpointRoleLabel: "llm-d.ai/role", meta: &fwkdl.EndpointMetadata{Labels: map[string]string{"llm-d.ai/role": "prefill"}}, want: "prefill"},
		{name: "flexible-decode label normalised to prefill", endpointRoleLabel: "llm-d.ai/role", meta: &fwkdl.EndpointMetadata{Labels: map[string]string{"llm-d.ai/role": "flexible-decode"}}, want: "decode"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := buildPredictionRequest(tc.endpointRoleLabel, tc.meta, fwkdl.NewMetrics(), "hello world", 0, 0.0)
			assert.Equal(t, tc.want, got.PodType)
		})
	}
}

func TestBuildTrainingEntry_PodType(t *testing.T) {
	meta := &fwkdl.EndpointMetadata{
		NamespacedName: types.NamespacedName{Namespace: "ns", Name: "flex-0"},
		Labels:         map[string]string{"llm-d.ai/role": "flexible-decode"},
	}
	got := buildTrainingEntry("llm-d.ai/role", meta, fwkdl.NewMetrics(), "hello world", 100, 10, time.Time{}, 0, 0.0)
	assert.Equal(t, "decode", got.PodType,
		"flexible-decode must be normalised to 'prefill' before reaching the predictor")
}
