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

// Package poolview provides a request-scoped, per-pool view of scheduling endpoints.
package poolview

import (
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
)

// PoolViewKey is the PluginState key under which PoolView is stored.
const PoolViewKey plugin.StateKey = "llm-d.poolview"

const (
	// DefaultPrefillPool is the conventional pool name for prefill endpoints.
	DefaultPrefillPool = "prefill"
	// DefaultDecodePool is the conventional pool name for decode endpoints.
	DefaultDecodePool = "decode"
	// DefaultFlexPool is the conventional pool name for FlexibleDecode endpoints.
	DefaultFlexPool = "flex"
)

// PoolView holds per-pool snapshots of scheduling endpoints for one request.
type PoolView struct {
	pools map[string][]scheduling.Endpoint
}

// NewPoolView constructs a PoolView with the supplied per-pool endpoint slices.
func NewPoolView(pools map[string][]scheduling.Endpoint) *PoolView {
	return &PoolView{pools: pools}
}

// Pool returns the endpoints belonging to the named pool.
func (v *PoolView) Pool(name string) []scheduling.Endpoint {
	if v == nil {
		return nil
	}
	return v.pools[name]
}

// Pools returns the names of all configured pools.
func (v *PoolView) Pools() []string {
	if v == nil {
		return nil
	}
	out := make([]string, 0, len(v.pools))
	for name := range v.pools {
		out = append(out, name)
	}
	return out
}

// Clone implements plugin.StateData.
func (v *PoolView) Clone() plugin.StateData {
	if v == nil {
		return nil
	}
	cloned := make(map[string][]scheduling.Endpoint, len(v.pools))
	for k, eps := range v.pools {
		cp := make([]scheduling.Endpoint, len(eps))
		copy(cp, eps)
		cloned[k] = cp
	}
	return &PoolView{pools: cloned}
}
