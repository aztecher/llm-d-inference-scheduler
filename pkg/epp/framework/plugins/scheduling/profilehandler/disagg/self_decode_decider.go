package disagg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
)

const (
	// SelfDecodeCapacityDeciderPluginType is the type-name of the SelfDecodeCapacityDecider plugin.
	SelfDecodeCapacityDeciderPluginType = "self-decode-capacity-decider"
)

// SelfDecodeCapacityDeciderConfig holds the configuration for the SelfDecodeCapacityDecider plugin.
type SelfDecodeCapacityDeciderConfig struct {
	// KVCacheThreshold is the maximum KV cache usage (fraction, e.g. 0.8 = 80%) below which
	// the FlexibleDecoder endpoint is considered capable of self-decode. Default: 0.8.
	KVCacheThreshold float64 `json:"kvCacheThreshold,omitempty"`
	// QueueDepthThreshold is the maximum waiting queue depth below which the FlexibleDecoder
	// endpoint is considered capable of self-decode. Default: 40.
	QueueDepthThreshold float64 `json:"queueDepthThreshold,omitempty"`
}

const (
	defaultKVCacheThreshold    = 0.8
	defaultQueueDepthThreshold = 40.0
)

func (c *SelfDecodeCapacityDeciderConfig) applyDefaults() {
	if c.KVCacheThreshold == 0 {
		c.KVCacheThreshold = defaultKVCacheThreshold
	}
	if c.QueueDepthThreshold == 0 {
		c.QueueDepthThreshold = defaultQueueDepthThreshold
	}
}

func (c SelfDecodeCapacityDeciderConfig) validate() error {
	if c.KVCacheThreshold <= 0 || c.KVCacheThreshold > 1.0 {
		return errors.New("kvCacheThreshold must be in range (0, 1.0]")
	}
	if c.QueueDepthThreshold <= 0 {
		return errors.New("queueDepthThreshold must be positive")
	}
	return nil
}

// compile-time type assertion: SelfDecodeCapacityDecider implements FlexDeciderPlugin.
var _ FlexDeciderPlugin = &SelfDecodeCapacityDecider{}

// SelfDecodeCapacityDecider decides whether a FlexibleDecoder endpoint has enough remaining
// capacity to perform both Prefill and Decode for the request (self-decode mode).
//
// Returns true when the endpoint's KV cache usage and waiting queue depth are both
// below the configured thresholds. When false, the FlexibleDecoder will only handle
// Prefill and forward the KV to a separate Decode pod (prefill-and-forward mode).
type SelfDecodeCapacityDecider struct {
	typedName plugin.TypedName
	config    SelfDecodeCapacityDeciderConfig
}

// SelfDecodeCapacityDeciderPluginFactory is the factory function for SelfDecodeCapacityDecider.
func SelfDecodeCapacityDeciderPluginFactory(name string, rawParameters json.RawMessage, _ plugin.Handle) (plugin.Plugin, error) {
	config := SelfDecodeCapacityDeciderConfig{}
	if rawParameters != nil {
		if err := json.Unmarshal(rawParameters, &config); err != nil {
			return nil, fmt.Errorf("failed to parse %s plugin config: %w", SelfDecodeCapacityDeciderPluginType, err)
		}
	}
	config.applyDefaults()
	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("invalid %s plugin config: %w", SelfDecodeCapacityDeciderPluginType, err)
	}
	return NewSelfDecodeCapacityDecider(config).WithName(name), nil
}

// NewSelfDecodeCapacityDecider creates a new SelfDecodeCapacityDecider.
func NewSelfDecodeCapacityDecider(config SelfDecodeCapacityDeciderConfig) *SelfDecodeCapacityDecider {
	config.applyDefaults()
	return &SelfDecodeCapacityDecider{
		typedName: plugin.TypedName{Type: SelfDecodeCapacityDeciderPluginType},
		config:    config,
	}
}

// TypedName returns the typed name of the plugin.
func (d *SelfDecodeCapacityDecider) TypedName() plugin.TypedName {
	return d.typedName
}

// WithName sets the name of the plugin.
func (d *SelfDecodeCapacityDecider) WithName(name string) *SelfDecodeCapacityDecider {
	d.typedName.Name = name
	return d
}

// Disaggregate implements FlexDeciderPlugin.
// Returns true when the FlexibleDecoder endpoint has sufficient spare capacity for self-decode.
// Returns false when the endpoint is too loaded, triggering prefill-and-forward mode instead.
func (d *SelfDecodeCapacityDecider) Disaggregate(_ context.Context, _ *scheduling.InferenceRequest, flexEndpoint scheduling.Endpoint) bool {
	if flexEndpoint == nil {
		return false
	}
	m := flexEndpoint.GetMetrics()
	if m == nil {
		return false
	}
	return m.KVCacheUsagePercent < d.config.KVCacheThreshold &&
		float64(m.WaitingQueueSize) < d.config.QueueDepthThreshold
}
