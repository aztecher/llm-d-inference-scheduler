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

package disagg

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
)

// makeFlexEndpoint returns a scheduling.Endpoint with the given metrics.
func makeFlexEndpoint(t *testing.T, kvUsage float64, waitingQueue int) scheduling.Endpoint {
	t.Helper()
	meta := &fwkdl.EndpointMetadata{
		NamespacedName: k8stypes.NamespacedName{Namespace: "default", Name: "flex-0"},
		Address:        "10.0.0.1",
		Port:           "8000",
	}
	m := fwkdl.NewMetrics()
	m.KVCacheUsagePercent = kvUsage
	m.WaitingQueueSize = waitingQueue
	return scheduling.NewEndpoint(meta, m, fwkdl.NewAttributes())
}

func TestBuildSelfDecodePolicy_DefaultsToUtilizationBased(t *testing.T) {
	policy, err := buildSelfDecodePolicy("", nil)
	assert.NoError(t, err)
	_, ok := policy.(*UtilizationBasedSelfDecodePolicy)
	assert.True(t, ok, "empty policy name should resolve to utilization-based")
}

func TestBuildSelfDecodePolicy_PolicyParameters(t *testing.T) {
	params := json.RawMessage(`{"kvCacheThreshold": 0.5, "queueDepthThreshold": 20}`)
	policy, err := buildSelfDecodePolicy(UtilizationBasedSelfDecodePolicyName, params)
	assert.NoError(t, err)
	u, ok := policy.(*UtilizationBasedSelfDecodePolicy)
	assert.True(t, ok)
	assert.InDelta(t, 0.5, u.kvCacheThreshold, 1e-9)
	assert.InDelta(t, 20.0, u.queueDepthThreshold, 1e-9)
}

func TestBuildSelfDecodePolicy_InvalidParametersJSON(t *testing.T) {
	_, err := buildSelfDecodePolicy(UtilizationBasedSelfDecodePolicyName, json.RawMessage(`{not json}`))
	assert.Error(t, err, "should reject invalid json")
}

func TestUtilizationBasedSelfDecodePolicy_AppliesDefaults(t *testing.T) {
	p, err := NewUtilizationBasedSelfDecodePolicy(UtilizationBasedSelfDecodePolicyConfig{})
	assert.NoError(t, err)
	assert.InDelta(t, defaultUtilizationBasedKVCacheThreshold, p.kvCacheThreshold, 1e-9)
	assert.InDelta(t, defaultUtilizationBasedQueueDepthThreshold, p.queueDepthThreshold, 1e-9)
}

func TestUtilizationBasedSelfDecodePolicy_OutOfRangeKVThreshold(t *testing.T) {
	_, err := NewUtilizationBasedSelfDecodePolicy(UtilizationBasedSelfDecodePolicyConfig{
		KVCacheThreshold:    1.5,
		QueueDepthThreshold: 20,
	})
	assert.Error(t, err, "should reject kvCacheThreshold out-of-range")
}

func TestUtilizationBasedSelfDecodePolicy_NegativeKVThreshold(t *testing.T) {
	_, err := NewUtilizationBasedSelfDecodePolicy(UtilizationBasedSelfDecodePolicyConfig{
		KVCacheThreshold:    -0.1,
		QueueDepthThreshold: 20,
	})
	assert.Error(t, err, "should reject nagetive KvCacheThreshold")
}

func TestUtilizationBasedSelfDecodePolicy_NegativeQueueDepthThreshold(t *testing.T) {
	_, err := NewUtilizationBasedSelfDecodePolicy(UtilizationBasedSelfDecodePolicyConfig{
		KVCacheThreshold:    0.5,
		QueueDepthThreshold: -1,
	})
	assert.Error(t, err, "should reject negative queueDepthThreshold")
}

func TestUtilizationBasedSelfDecodePolicy_Allow(t *testing.T) {
	p, err := NewUtilizationBasedSelfDecodePolicy(UtilizationBasedSelfDecodePolicyConfig{
		KVCacheThreshold:    0.8,
		QueueDepthThreshold: 40,
	})
	assert.NoError(t, err)

	tests := []struct {
		name     string
		kvUsage  float64
		queue    int
		expected bool
	}{
		{"both under threshold", 0.1, 10, true},
		{"kv at threshold", 0.8, 10, false},    // boundary: strict <
		{"queue at threshold", 0.1, 40, false}, // boundary: strict <
		{"both over", 0.9, 50, false},
		{"kv over, queue ok", 0.9, 10, false},
		{"kv ok, queue over", 0.1, 50, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ep := makeFlexEndpoint(t, tc.kvUsage, tc.queue)
			got := p.Allow(context.Background(), &scheduling.InferenceRequest{}, ep)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestUtilizationBasedSelfDecodePolicy_AllowNilEndpoint(t *testing.T) {
	p, _ := NewUtilizationBasedSelfDecodePolicy(UtilizationBasedSelfDecodePolicyConfig{})
	assert.False(t, p.Allow(context.Background(), &scheduling.InferenceRequest{}, nil))
}
