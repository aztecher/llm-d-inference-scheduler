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

package poolview

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-inference-scheduler/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/requestcontrol"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
)

// PoolViewProducerPluginType is the type-name of the PoolViewProducer plugin.
const PoolViewProducerPluginType = "pool-view-producer"

// DefaultPoolViewProducerName is the conventional plugin instance name.
const DefaultPoolViewProducerName = "pool-view-producer"

// poolConfig is the JSON shape for one named pool.
type poolConfig struct {
	// Roles is the list of `llm-d.ai/role` values that belong to this pool.
	Roles []string `json:"roles"`
}

// Config holds the configuration for the PoolViewProducer plugin.
type Config struct {
	// Pools maps a pool name to its membership rules.
	Pools map[string]poolConfig `json:"pools"`
}

func (c *Config) validate() error {
	if len(c.Pools) == 0 {
		return errors.New("at least one pool must be declared in `pools`")
	}
	for name, pc := range c.Pools {
		if name == "" {
			return errors.New("pool name must not be empty")
		}
		if len(pc.Roles) == 0 {
			return fmt.Errorf("pool %q must declare at least one role", name)
		}
	}
	return nil
}

// PoolViewProducer partitions the candidate endpoint snapshot into named pools
// per request and publishes the result to PluginState under PoolViewKey.
type PoolViewProducer struct {
	typedName   plugin.TypedName
	selectors   map[string]PoolSelector
	pluginState *plugin.PluginState
}

var (
	_ requestcontrol.DataProducer = &PoolViewProducer{}
	_ plugin.Plugin               = &PoolViewProducer{}
)

// NewPoolViewProducer constructs a PoolViewProducer with the given selectors.
func NewPoolViewProducer(ctx context.Context, name string, selectors map[string]PoolSelector) *PoolViewProducer {
	return &PoolViewProducer{
		typedName:   plugin.TypedName{Type: PoolViewProducerPluginType, Name: name},
		selectors:   selectors,
		pluginState: plugin.NewPluginState(ctx),
	}
}

// PoolViewProducerFactory defines the factory function for creating
// a new instance of the PoolViewProducer.
func PoolViewProducerFactory(name string, rawParameters json.RawMessage, handle plugin.Handle) (plugin.Plugin, error) {
	var cfg Config
	if len(rawParameters) > 0 {
		if err := json.Unmarshal(rawParameters, &cfg); err != nil {
			return nil, fmt.Errorf("failed to parse %s plugin config: %w", PoolViewProducerPluginType, err)
		}
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid %s plugin config: %w", PoolViewProducerPluginType, err)
	}

	selectors := make(map[string]PoolSelector, len(cfg.Pools))
	for poolName, pc := range cfg.Pools {
		selectors[poolName] = NewRoleLabelSelector(pc.Roles)
	}

	logger := log.FromContext(handle.Context())
	logger.V(logutil.DEFAULT).Info("Created PoolViewProducer",
		"name", name,
		"pools", poolNames(cfg.Pools))

	return NewPoolViewProducer(handle.Context(), name, selectors), nil
}

func (p *PoolViewProducer) TypedName() plugin.TypedName {
	return p.typedName
}

func (p *PoolViewProducer) WithName(name string) *PoolViewProducer {
	p.typedName.Name = name
	return p
}

// Produces declares that this plugin emits PoolView state under PoolViewKey.
func (p *PoolViewProducer) Produces() map[string]any {
	return map[string]any{string(PoolViewKey): &PoolView{}}
}

func (p *PoolViewProducer) Consumes() map[string]any {
	return nil
}

// PrepareRequestData partitions the endpoint snapshot by pool and writes the
// resulting PoolView to PluginState keyed by request.RequestId.
func (p *PoolViewProducer) PrepareRequestData(ctx context.Context, request *scheduling.InferenceRequest, endpoints []scheduling.Endpoint) error {
	if request == nil {
		return errors.New("PoolViewProducer: nil request")
	}

	pools := make(map[string][]scheduling.Endpoint, len(p.selectors))
	for poolName, sel := range p.selectors {
		bucket := make([]scheduling.Endpoint, 0, len(endpoints))
		for _, ep := range endpoints {
			if sel.Selects(ep) {
				bucket = append(bucket, ep)
			}
		}
		pools[poolName] = bucket
	}
	view := NewPoolView(pools)
	p.pluginState.Write(request.RequestId, PoolViewKey, view)

	logger := log.FromContext(ctx).V(logutil.TRACE).WithName(PoolViewProducerPluginType)
	if logger.Enabled() {
		summary := make(map[string]int, len(pools))
		for k, v := range pools {
			summary[k] = len(v)
		}
		logger.Info("PoolView prepared",
			"request_id", request.RequestId,
			"pool_sizes", summary)
	}
	return nil
}

// PluginState returns the shared PluginState instance.
func (p *PoolViewProducer) PluginState() *plugin.PluginState {
	return p.pluginState
}

// poolNames is a helper for logging.
func poolNames(pools map[string]poolConfig) []string {
	out := make([]string, 0, len(pools))
	for name := range pools {
		out = append(out, name)
	}
	return out
}
