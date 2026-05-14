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
	"errors"
	"fmt"

	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
)

// SelfDecodePolicy decides whether a flex endpoint may handle self-decode.
type SelfDecodePolicy interface {
	// Allow returns true to permit self_decode, false for prefill_and_forward.
	Allow(ctx context.Context, request *scheduling.InferenceRequest, flexEndpoint scheduling.Endpoint) bool
}

// selfDecodePolicyProvider exposes a configured SelfDecodePolicy.
type selfDecodePolicyProvider interface {
	SelfDecodePolicy() SelfDecodePolicy
}

// selfDecodePolicyFactory builds a SelfDecodePolicy from raw JSON parameters.
type selfDecodePolicyFactory func(rawParameters json.RawMessage) (SelfDecodePolicy, error)

const (
	DefaultSelfDecodePolicyName = UtilizationBasedSelfDecodePolicyName
)

// selfDecodePolicyRegistry maps a policy name to its factory.
var selfDecodePolicyRegistry = map[string]selfDecodePolicyFactory{
	UtilizationBasedSelfDecodePolicyName: newUtilizationBasedSelfDecodePolicy,
}

// buildSelfDecodePolicy resolves a policy by name with the supplied parameters.
func buildSelfDecodePolicy(name string, params json.RawMessage) (SelfDecodePolicy, error) {
	if name == "" {
		name = DefaultSelfDecodePolicyName
	}
	factory, ok := selfDecodePolicyRegistry[name]
	if !ok {
		known := make([]string, 0, len(selfDecodePolicyRegistry))
		for k := range selfDecodePolicyRegistry {
			known = append(known, k)
		}
		return nil, fmt.Errorf("unknown selfDecode.policy %q (known: %v)", name, known)
	}
	return factory(params)
}

const (
	UtilizationBasedSelfDecodePolicyName       = "utilization-based"
	defaultUtilizationBasedKVCacheThreshold    = 0.8
	defaultUtilizationBasedQueueDepthThreshold = 40.0
)

// UtilizationBasedSelfDecodePolicyConfig holds the parameters for UtilizationBasedSelfDecodePolicy.
type UtilizationBasedSelfDecodePolicyConfig struct {
	// KVCacheThreshold is the maximum KV cache usage (0..1] permitting self-decode.
	KVCacheThreshold float64 `json:"kvCacheThreshold,omitempty"`
	// QueueDepthThreshold is the maximum waiting queue depth permitting self-decode.
	QueueDepthThreshold float64 `json:"queueDepthThreshold,omitempty"`
}

var _ SelfDecodePolicy = &UtilizationBasedSelfDecodePolicy{}

// UtilizationBasedSelfDecodePolicy permits self-decode when KV cache usage and waiting queue depth are below thresholds.
type UtilizationBasedSelfDecodePolicy struct {
	kvCacheThreshold    float64
	queueDepthThreshold float64
}

// NewUtilizationBasedSelfDecodePolicy initializes a UtilizationBasedSelfDecodePolicy and returns its pointer.
func NewUtilizationBasedSelfDecodePolicy(cfg UtilizationBasedSelfDecodePolicyConfig) (*UtilizationBasedSelfDecodePolicy, error) {
	if cfg.KVCacheThreshold == 0 {
		cfg.KVCacheThreshold = defaultUtilizationBasedKVCacheThreshold
	}
	if cfg.QueueDepthThreshold == 0 {
		cfg.QueueDepthThreshold = defaultUtilizationBasedQueueDepthThreshold
	}
	if cfg.KVCacheThreshold <= 0 || cfg.KVCacheThreshold > 1.0 {
		return nil, fmt.Errorf("%s: kvCacheThreshold must be in (0, 1.0], got %f", UtilizationBasedSelfDecodePolicyName, cfg.KVCacheThreshold)
	}
	if cfg.QueueDepthThreshold <= 0 {
		return nil, errors.New(UtilizationBasedSelfDecodePolicyName + ": queueDepthThreshold must be positive")
	}
	return &UtilizationBasedSelfDecodePolicy{
		kvCacheThreshold:    cfg.KVCacheThreshold,
		queueDepthThreshold: cfg.QueueDepthThreshold,
	}, nil
}

// newUtilizationBasedSelfDecodePolicy is the registry-side factory.
func newUtilizationBasedSelfDecodePolicy(rawParameters json.RawMessage) (SelfDecodePolicy, error) {
	cfg := UtilizationBasedSelfDecodePolicyConfig{}
	if len(rawParameters) > 0 {
		if err := json.Unmarshal(rawParameters, &cfg); err != nil {
			return nil, fmt.Errorf("invalid %s parameters: %w", UtilizationBasedSelfDecodePolicyName, err)
		}
	}
	return NewUtilizationBasedSelfDecodePolicy(cfg)
}

// Allow implements SelfDecodePolicy.
func (p *UtilizationBasedSelfDecodePolicy) Allow(_ context.Context, _ *scheduling.InferenceRequest, flexEndpoint scheduling.Endpoint) bool {
	if flexEndpoint == nil {
		return false
	}
	m := flexEndpoint.GetMetrics()
	if m == nil {
		return false
	}
	return m.KVCacheUsagePercent < p.kvCacheThreshold &&
		float64(m.WaitingQueueSize) < p.queueDepthThreshold
}
