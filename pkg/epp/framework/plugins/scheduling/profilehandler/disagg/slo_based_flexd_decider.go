package disagg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"

	"sigs.k8s.io/controller-runtime/pkg/log"

	fwkdl "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	fwkflowcontrol "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/flowcontrol"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/requestcontrol/dataproducer/poolview"
	schedmetrics "github.com/llm-d/llm-d-inference-scheduler/pkg/metrics"
)

const (
	SLOBasedFlexDeciderPluginType = "slo-based-flexd-decider"
	defaultSaturationThreshold    = 1.0
	// envForceSaturation overrides the computed saturation value with a parseable float.
	// PoC-only debug feature; do not use in production.
	envForceSaturation = "LLM_D_FORCE_SATURATION"
)

// SLOBasedFlexDeciderConfig holds the configuration for the SLOBasedFlexDecider plugin.
type SLOBasedFlexDeciderConfig struct {
	// SaturationDetectorPluginName is the name of the SaturationDetector plugin to use.
	SaturationDetectorPluginName string `json:"saturationDetectorPluginName"`
	// SaturationThreshold is the pool saturation level at which FlexibleDecode activates. Default: 1.0.
	SaturationThreshold float64 `json:"saturationThreshold,omitempty"`
	// Pool is the name of the pool whose saturation is evaluated. Default: "prefill".
	Pool string `json:"pool,omitempty"`
	// PoolViewProducerName is the name of the PoolViewProducer plugin instance.
	PoolViewProducerName string `json:"poolViewProducerName,omitempty"`
	// SelfDecode configures the SelfDecodePolicy. When omitted, the default policy is applied.
	SelfDecode *SelfDecodeConfig `json:"selfDecode,omitempty"`
}

// SelfDecodeConfig selects a SelfDecodePolicy implementation and supplies its parameters.
type SelfDecodeConfig struct {
	// Policy is the registry name of the SelfDecodePolicy implementation.
	Policy string `json:"policy,omitempty"`
	// Parameters is the raw JSON parameters block for the chosen policy.
	Parameters json.RawMessage `json:"parameters,omitempty"`
}

func (c SLOBasedFlexDeciderConfig) validate() error {
	if c.SaturationDetectorPluginName == "" {
		return errors.New("saturationDetectorPluginName must be specified")
	}
	if c.SaturationThreshold < 0 {
		return errors.New("saturationThreshold cannot be negative")
	}
	return nil
}

var _ deciderPlugin = &SLOBasedFlexDecider{}

// SLOBasedFlexDecider activates FlexibleDecode as a temporary Prefiller when the configured pool's saturation crosses the threshold.
type SLOBasedFlexDecider struct {
	typedName          plugin.TypedName
	config             SLOBasedFlexDeciderConfig
	saturationDetector fwkflowcontrol.SaturationDetector
	poolViewState      *plugin.PluginState
	poolName           string
	detectorName       string
	forceSaturation    *float64
	selfDecodePolicy   SelfDecodePolicy
}

// SelfDecodePolicy returns the configured SelfDecodePolicy.
func (d *SLOBasedFlexDecider) SelfDecodePolicy() SelfDecodePolicy {
	return d.selfDecodePolicy
}

// SLOBasedFlexDeciderPluginFactory defines the factory function for creating
// a new instance of the SLOBasedFlexDecider.
func SLOBasedFlexDeciderPluginFactory(name string, rawParameters json.RawMessage, handle plugin.Handle) (plugin.Plugin, error) {
	config := SLOBasedFlexDeciderConfig{
		SaturationThreshold:  defaultSaturationThreshold,
		Pool:                 poolview.DefaultPrefillPool,
		PoolViewProducerName: poolview.DefaultPoolViewProducerName,
	}
	if rawParameters != nil {
		if err := json.Unmarshal(rawParameters, &config); err != nil {
			return nil, fmt.Errorf("failed to parse %s plugin config: %w", SLOBasedFlexDeciderPluginType, err)
		}
	}
	if config.Pool == "" {
		config.Pool = poolview.DefaultPrefillPool
	}
	if config.PoolViewProducerName == "" {
		config.PoolViewProducerName = poolview.DefaultPoolViewProducerName
	}
	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("invalid %s plugin config: %w", SLOBasedFlexDeciderPluginType, err)
	}

	satDetectorPlugin := handle.Plugin(config.SaturationDetectorPluginName)
	if satDetectorPlugin == nil {
		return nil, fmt.Errorf("%s: saturation detector plugin %q not found",
			SLOBasedFlexDeciderPluginType, config.SaturationDetectorPluginName)
	}
	satDetector, ok := satDetectorPlugin.(fwkflowcontrol.SaturationDetector)
	if !ok {
		return nil, fmt.Errorf("%s: plugin %q does not implement SaturationDetector",
			SLOBasedFlexDeciderPluginType, config.SaturationDetectorPluginName)
	}

	poolProducerPlugin := handle.Plugin(config.PoolViewProducerName)
	if poolProducerPlugin == nil {
		return nil, fmt.Errorf("%s: pool-view-producer plugin %q not found",
			SLOBasedFlexDeciderPluginType, config.PoolViewProducerName)
	}
	poolProducer, ok := poolProducerPlugin.(*poolview.PoolViewProducer)
	if !ok {
		return nil, fmt.Errorf("%s: plugin %q is not a PoolViewProducer",
			SLOBasedFlexDeciderPluginType, config.PoolViewProducerName)
	}

	var (
		selfDecodePolicyName   string
		selfDecodePolicyParams json.RawMessage
	)
	if config.SelfDecode != nil {
		selfDecodePolicyName = config.SelfDecode.Policy
		selfDecodePolicyParams = config.SelfDecode.Parameters
	}
	selfDecodePolicy, err := buildSelfDecodePolicy(selfDecodePolicyName, selfDecodePolicyParams)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", SLOBasedFlexDeciderPluginType, err)
	}

	decider := &SLOBasedFlexDecider{
		typedName:          plugin.TypedName{Type: SLOBasedFlexDeciderPluginType, Name: name},
		config:             config,
		saturationDetector: satDetector,
		poolViewState:      poolProducer.PluginState(),
		poolName:           config.Pool,
		detectorName:       config.SaturationDetectorPluginName,
		selfDecodePolicy:   selfDecodePolicy,
	}

	schedmetrics.RecordPoolSaturationThreshold(decider.poolName, decider.detectorName, decider.config.SaturationThreshold)

	logger := log.FromContext(handle.Context()).WithName(SLOBasedFlexDeciderPluginType)
	logger.Info("SLOBasedFlexDecider created",
		"name", name,
		"saturationDetector", config.SaturationDetectorPluginName,
		"saturationThreshold", config.SaturationThreshold,
		"pool", config.Pool,
		"poolViewProducer", config.PoolViewProducerName)

	if v, ok := os.LookupEnv(envForceSaturation); ok {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			decider.forceSaturation = &f
			logger.Info("LLM_D_FORCE_SATURATION is set; SaturationDetector will be bypassed",
				"forced_saturation", f,
				"threshold", decider.config.SaturationThreshold,
				"will_activate_flexd", f >= decider.config.SaturationThreshold)
		} else {
			logger.Error(err, "LLM_D_FORCE_SATURATION is set but not parseable as float; ignoring",
				"value", v)
		}
	}

	return decider, nil
}

// NewSLOBasedFlexDecider initializes a SLOBasedFlexDecider and returns its pointer.
func NewSLOBasedFlexDecider(
	config SLOBasedFlexDeciderConfig,
	satDetector fwkflowcontrol.SaturationDetector,
	poolViewState *plugin.PluginState,
) (*SLOBasedFlexDecider, error) {
	if satDetector == nil {
		return nil, errors.New("saturation detector must not be nil")
	}
	if poolViewState == nil {
		return nil, errors.New("pool view state must not be nil")
	}
	threshold := config.SaturationThreshold
	if threshold == 0 {
		threshold = defaultSaturationThreshold
	}
	pool := config.Pool
	if pool == "" {
		pool = poolview.DefaultPrefillPool
	}
	// Resolve self-decode policy (defaults applied when config.SelfDecode nil).
	var (
		selfDecodePolicyName   string
		selfDecodePolicyParams json.RawMessage
	)
	if config.SelfDecode != nil {
		selfDecodePolicyName = config.SelfDecode.Policy
		selfDecodePolicyParams = config.SelfDecode.Parameters
	}
	selfDecodePolicy, err := buildSelfDecodePolicy(selfDecodePolicyName, selfDecodePolicyParams)
	if err != nil {
		return nil, err
	}

	return &SLOBasedFlexDecider{
		typedName: plugin.TypedName{Type: SLOBasedFlexDeciderPluginType},
		config: SLOBasedFlexDeciderConfig{
			SaturationDetectorPluginName: config.SaturationDetectorPluginName,
			SaturationThreshold:          threshold,
			Pool:                         pool,
			PoolViewProducerName:         config.PoolViewProducerName,
		},
		saturationDetector: satDetector,
		poolViewState:      poolViewState,
		poolName:           pool,
		detectorName:       config.SaturationDetectorPluginName,
		selfDecodePolicy:   selfDecodePolicy,
	}, nil
}

func (d *SLOBasedFlexDecider) TypedName() plugin.TypedName {
	return d.typedName
}

func (d *SLOBasedFlexDecider) WithName(name string) *SLOBasedFlexDecider {
	d.typedName.Name = name
	return d
}

// disaggregate implements deciderPlugin.
// Returns true when the configured pool's saturation crosses the threshold.
// The endpoint argument (selected Decode endpoint) is not used.
func (d *SLOBasedFlexDecider) disaggregate(ctx context.Context, request *scheduling.InferenceRequest, _ scheduling.Endpoint) bool {
	logger := log.FromContext(ctx).WithName(SLOBasedFlexDeciderPluginType)

	requestID := ""
	if request != nil {
		requestID = request.RequestId
	}

	view, err := plugin.ReadPluginStateKey[*poolview.PoolView](d.poolViewState, requestID, poolview.PoolViewKey)
	if err != nil {
		schedmetrics.RecordFlexibleDecodeActivationEvaluation(schedmetrics.FlexibleDecodeActivationUnwired)
		logger.V(1).Info("disaggregate skipped: PoolView not found in PluginState",
			"request_id", requestID,
			"pool", d.poolName,
			"err", err.Error(),
			"decision", false,
			"reason", "unwired")
		return false
	}

	endpoints := view.Pool(d.poolName)
	if len(endpoints) == 0 {
		schedmetrics.RecordFlexibleDecodeActivationEvaluation(schedmetrics.FlexibleDecodeActivationNoEndpoints)
		logger.V(1).Info("disaggregate skipped: pool is empty",
			"request_id", requestID,
			"pool", d.poolName,
			"decision", false,
			"reason", "no_endpoints")
		return false
	}

	if d.forceSaturation != nil {
		forced := *d.forceSaturation
		schedmetrics.RecordPoolSaturation(d.poolName, d.detectorName, forced)
		decision := forced >= d.config.SaturationThreshold
		if decision {
			schedmetrics.RecordFlexibleDecodeActivationEvaluation(schedmetrics.FlexibleDecodeActivationAboveThreshold)
		} else {
			schedmetrics.RecordFlexibleDecodeActivationEvaluation(schedmetrics.FlexibleDecodeActivationBelowThreshold)
		}
		logger.Info("disaggregate using LLM_D_FORCE_SATURATION override",
			"request_id", requestID,
			"forced_saturation", forced,
			"threshold", d.config.SaturationThreshold,
			"decision", decision,
			"pool", d.poolName,
			"pool_size", len(endpoints))
		return decision
	}

	saturation := d.saturationDetector.Saturation(ctx, schedulingToDataLayer(endpoints))
	schedmetrics.RecordPoolSaturation(d.poolName, d.detectorName, saturation)
	decision := saturation >= d.config.SaturationThreshold

	if decision {
		schedmetrics.RecordFlexibleDecodeActivationEvaluation(schedmetrics.FlexibleDecodeActivationAboveThreshold)
	} else {
		schedmetrics.RecordFlexibleDecodeActivationEvaluation(schedmetrics.FlexibleDecodeActivationBelowThreshold)
	}

	logger.V(2).Info("disaggregate evaluated",
		"request_id", requestID,
		"saturation", saturation,
		"threshold", d.config.SaturationThreshold,
		"decision", decision,
		"pool", d.poolName,
		"pool_size", len(endpoints),
		"detector", d.detectorName)

	return decision
}

// schedEndpointAdapter adapts a scheduling.Endpoint to the datalayer.Endpoint
// interface required by SaturationDetector.Saturation. Update* methods are
// no-ops because the per-request snapshot is immutable.
type schedEndpointAdapter struct {
	scheduling.Endpoint
}

// GetAttributes returns the embedded scheduling.Endpoint.
func (a *schedEndpointAdapter) GetAttributes() fwkdl.AttributeMap {
	return a.Endpoint
}

// UpdateMetadata is a no-op.
func (a *schedEndpointAdapter) UpdateMetadata(*fwkdl.EndpointMetadata) {}

// UpdateMetrics is a no-op.
func (a *schedEndpointAdapter) UpdateMetrics(*fwkdl.Metrics) {}

// schedulingToDataLayer wraps scheduling endpoints with schedEndpointAdapter.
func schedulingToDataLayer(eps []scheduling.Endpoint) []fwkdl.Endpoint {
	out := make([]fwkdl.Endpoint, len(eps))
	for i, e := range eps {
		out[i] = &schedEndpointAdapter{Endpoint: e}
	}
	return out
}
