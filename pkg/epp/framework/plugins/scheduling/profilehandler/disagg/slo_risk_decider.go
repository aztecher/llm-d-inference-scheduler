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
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/scheduling/filter/bylabel"
	schedmetrics "github.com/llm-d/llm-d-inference-scheduler/pkg/metrics"
)

const (
	// SLORiskDeciderPluginType is the type-name of the SLORiskDecider plugin.
	SLORiskDeciderPluginType = "slo-risk-decider"

	defaultSaturationThreshold = 1.0

	// poolLabelPrefill is the gauge label for the Prefill pool. Currently the
	// SLORiskDecider only ever evaluates the Prefill pool, so this is constant.
	poolLabelPrefill = "prefill"

	// envForceSaturation lets operators override the computed saturation value
	// during PoC verification. When set to a parseable float, the SLORiskDecider
	// skips the SaturationDetector entirely and uses this value. Useful for
	// validating the FlexibleDecoder activation pipeline without having to
	// physically saturate the Prefill pool.
	//
	// WARNING: This bypasses normal logic; a WARN log is emitted on every
	// invocation while it is active.
	envForceSaturation = "LLM_D_FORCE_SATURATION"
)

// PrefillEndpointProvider is a function that returns the current list of Prefill pool endpoints.
// It is injected into the SLORiskDecider at construction time.
// When nil, the SLORiskDecider conservatively returns false (no disaggregation).
type PrefillEndpointProvider func() []fwkdl.Endpoint

// PrefillEndpointStore is a minimal interface for retrieving pod endpoints.
// datastore.Datastore satisfies this interface.
type PrefillEndpointStore interface {
	PodList(predicate func(fwkdl.Endpoint) bool) []fwkdl.Endpoint
}

// isPrefillEndpoint returns true for endpoints whose role allows acting as a Prefill worker.
func isPrefillEndpoint(ep fwkdl.Endpoint) bool {
	md := ep.GetMetadata()
	if md == nil {
		return false
	}
	role := md.Labels[bylabel.RoleLabel]
	return role == bylabel.RolePrefill ||
		role == bylabel.RoleEncodePrefill ||
		role == bylabel.RolePrefillDecode ||
		role == bylabel.RoleEncodePrefillDecode
}

// WirePrefillEndpoints sets a datastore-backed PrefillEndpointProvider on every SLORiskDecider
// found in plugins. The provider returns only Prefill-role endpoints from the store.
// Call this after loader.InstantiateAndConfigure returns.
func WirePrefillEndpoints(plugins []plugin.Plugin, store PrefillEndpointStore) {
	for _, p := range plugins {
		if d, ok := p.(*SLORiskDecider); ok {
			d.WithPrefillEndpointProvider(func() []fwkdl.Endpoint {
				return store.PodList(isPrefillEndpoint)
			})
		}
	}
}

// SLORiskDeciderConfig holds the configuration for the SLORiskDecider plugin.
type SLORiskDeciderConfig struct {
	// SaturationDetectorPluginName is the name of the SaturationDetector plugin to use.
	SaturationDetectorPluginName string `json:"saturationDetectorPluginName"`
	// SaturationThreshold is the Prefill pool saturation level at which FlexibleDecoder
	// is activated as a temporary Prefiller. Defaults to 1.0 (fully saturated).
	SaturationThreshold float64 `json:"saturationThreshold,omitempty"`
}

func (c SLORiskDeciderConfig) validate() error {
	if c.SaturationDetectorPluginName == "" {
		return errors.New("saturationDetectorPluginName must be specified")
	}
	if c.SaturationThreshold < 0 {
		return errors.New("saturationThreshold cannot be negative")
	}
	return nil
}

// compile-time type assertions
var _ deciderPlugin = &SLORiskDecider{}
var _ FlexDeciderPlugin = &SLORiskDecider{}

// SLORiskDecider is a decider plugin that activates FlexibleDecoder as a temporary Prefiller
// when the Prefill pool saturation exceeds the configured threshold.
//
// It computes aggregate saturation across all Prefill endpoints using the configured
// SaturationDetector (typically UtilizationDetector). The disaggregate() method returns
// true when saturation >= SaturationThreshold, indicating that FlexibleDecoder should
// act as a Prefiller to protect TTFT SLO.
//
// NOTE (PoC): The PrefillEndpointProvider must be set via WithPrefillEndpointProvider()
// after construction to enable live saturation checks. When not set, disaggregate()
// conservatively returns false.
type SLORiskDecider struct {
	typedName          plugin.TypedName
	config             SLORiskDeciderConfig
	saturationDetector fwkflowcontrol.SaturationDetector
	prefillEndpoints   PrefillEndpointProvider

	// forceSaturation, when non-nil, overrides the computed saturation value.
	// Set via the LLM_D_FORCE_SATURATION env var at construction time.
	// PoC-only debug feature; do not use in production.
	forceSaturation *float64

	// detectorName is recorded as a label on the saturation gauges so dashboards
	// can distinguish e.g. utilization-detector vs concurrency-detector when both
	// configurations are exercised.
	detectorName string
}

// SLORiskDeciderPluginFactory defines the factory function for the SLORiskDecider.
// The PrefillEndpointProvider must be wired separately via WithPrefillEndpointProvider().
func SLORiskDeciderPluginFactory(name string, rawParameters json.RawMessage, handle plugin.Handle) (plugin.Plugin, error) {
	config := SLORiskDeciderConfig{
		SaturationThreshold: defaultSaturationThreshold,
	}
	if rawParameters != nil {
		if err := json.Unmarshal(rawParameters, &config); err != nil {
			return nil, fmt.Errorf("failed to parse %s plugin config: %w", SLORiskDeciderPluginType, err)
		}
	}
	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("invalid %s plugin config: %w", SLORiskDeciderPluginType, err)
	}

	satDetectorPlugin := handle.Plugin(config.SaturationDetectorPluginName)
	if satDetectorPlugin == nil {
		return nil, fmt.Errorf("%s: saturation detector plugin %q not found",
			SLORiskDeciderPluginType, config.SaturationDetectorPluginName)
	}
	satDetector, ok := satDetectorPlugin.(fwkflowcontrol.SaturationDetector)
	if !ok {
		return nil, fmt.Errorf("%s: plugin %q does not implement SaturationDetector",
			SLORiskDeciderPluginType, config.SaturationDetectorPluginName)
	}

	logger := log.FromContext(handle.Context())
	logger.Info("SLORiskDecider created; PrefillEndpointProvider not yet wired — call WithPrefillEndpointProvider() before serving requests",
		"saturationDetector", config.SaturationDetectorPluginName,
		"saturationThreshold", config.SaturationThreshold)

	decider, err := NewSLORiskDecider(config, satDetector, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create %s plugin: %w", SLORiskDeciderPluginType, err)
	}
	decider.detectorName = config.SaturationDetectorPluginName

	// Expose the configured threshold as a gauge so dashboards can render the
	// threshold line alongside the live saturation value.
	schedmetrics.RecordPoolSaturationThreshold(poolLabelPrefill, decider.detectorName, decider.config.SaturationThreshold)

	// Honor the debug override env var if set.
	if v, ok := os.LookupEnv(envForceSaturation); ok {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			decider.forceSaturation = &f
			logger.Info("LLM_D_FORCE_SATURATION is set — the SLORiskDecider will ignore the SaturationDetector and always return this value. PoC-only debug feature.",
				"forced_saturation", f,
				"threshold", decider.config.SaturationThreshold,
				"will_activate_flexd", f >= decider.config.SaturationThreshold)
		} else {
			logger.Error(err, "LLM_D_FORCE_SATURATION is set but not parseable as float; ignoring",
				"value", v)
		}
	}

	return decider.WithName(name), nil
}

// NewSLORiskDecider creates a new SLORiskDecider.
// provider may be nil; when nil, disaggregate() always returns false until
// WithPrefillEndpointProvider() is called.
func NewSLORiskDecider(config SLORiskDeciderConfig, satDetector fwkflowcontrol.SaturationDetector, provider PrefillEndpointProvider) (*SLORiskDecider, error) {
	if satDetector == nil {
		return nil, errors.New("saturation detector must not be nil")
	}
	threshold := config.SaturationThreshold
	if threshold == 0 {
		threshold = defaultSaturationThreshold
	}
	return &SLORiskDecider{
		typedName:          plugin.TypedName{Type: SLORiskDeciderPluginType},
		config:             SLORiskDeciderConfig{SaturationThreshold: threshold},
		saturationDetector: satDetector,
		prefillEndpoints:   provider,
	}, nil
}

// TypedName returns the typed name of the plugin.
func (d *SLORiskDecider) TypedName() plugin.TypedName {
	return d.typedName
}

// WithName sets the name of the plugin.
func (d *SLORiskDecider) WithName(name string) *SLORiskDecider {
	d.typedName.Name = name
	return d
}

// WithPrefillEndpointProvider sets the function used to retrieve live Prefill pool endpoints.
// Must be called before requests are served when using this decider in production.
func (d *SLORiskDecider) WithPrefillEndpointProvider(p PrefillEndpointProvider) *SLORiskDecider {
	d.prefillEndpoints = p
	return d
}

// Disaggregate is the exported method implementing FlexDeciderPlugin.
// It delegates to disaggregate().
func (d *SLORiskDecider) Disaggregate(ctx context.Context, request *scheduling.InferenceRequest, endpoint scheduling.Endpoint) bool {
	return d.disaggregate(ctx, request, endpoint)
}

// disaggregate checks Prefill pool saturation and returns true when FlexibleDecoder
// should temporarily act as a Prefiller to protect TTFT SLO.
// The endpoint argument (selected Decode endpoint) is not used for the saturation check.
func (d *SLORiskDecider) disaggregate(ctx context.Context, request *scheduling.InferenceRequest, _ scheduling.Endpoint) bool {
	logger := log.FromContext(ctx).WithName(SLORiskDeciderPluginType)

	requestID := ""
	if request != nil {
		requestID = request.RequestId
	}

	if d.prefillEndpoints == nil {
		// No provider wired yet: conservative behaviour — don't activate FlexibleDecoder.
		schedmetrics.RecordSLORiskEvaluation(schedmetrics.SLORiskDecisionUnwired)
		logger.V(1).Info("disaggregate skipped: PrefillEndpointProvider not wired",
			"request_id", requestID,
			"decision", false,
			"reason", "unwired")
		return false
	}
	endpoints := d.prefillEndpoints()
	if len(endpoints) == 0 {
		schedmetrics.RecordSLORiskEvaluation(schedmetrics.SLORiskDecisionNoEndpoints)
		logger.V(1).Info("disaggregate skipped: no Prefill endpoints available",
			"request_id", requestID,
			"decision", false,
			"reason", "no_endpoints")
		return false
	}

	// PoC debug override: if LLM_D_FORCE_SATURATION is set, bypass the detector.
	if d.forceSaturation != nil {
		forced := *d.forceSaturation
		schedmetrics.RecordPoolSaturation(poolLabelPrefill, d.detectorName, forced)
		decision := forced >= d.config.SaturationThreshold
		if decision {
			schedmetrics.RecordSLORiskEvaluation(schedmetrics.SLORiskDecisionAboveThreshold)
		} else {
			schedmetrics.RecordSLORiskEvaluation(schedmetrics.SLORiskDecisionBelowThreshold)
		}
		// WARN-level on every call so the override remains highly visible in logs.
		logger.Info("disaggregate using LLM_D_FORCE_SATURATION override (PoC debug)",
			"request_id", requestID,
			"forced_saturation", forced,
			"threshold", d.config.SaturationThreshold,
			"decision", decision,
			"pool_size", len(endpoints))
		return decision
	}

	saturation := d.saturationDetector.Saturation(ctx, endpoints)
	schedmetrics.RecordPoolSaturation(poolLabelPrefill, d.detectorName, saturation)
	decision := saturation >= d.config.SaturationThreshold

	if decision {
		schedmetrics.RecordSLORiskEvaluation(schedmetrics.SLORiskDecisionAboveThreshold)
	} else {
		schedmetrics.RecordSLORiskEvaluation(schedmetrics.SLORiskDecisionBelowThreshold)
	}

	// V(2) keeps this out of normal operator logs but makes it readily available
	// during PoC validation: `kubectl logs ... -v 2 | grep slo-risk-decider`.
	logger.V(2).Info("disaggregate evaluated",
		"request_id", requestID,
		"saturation", saturation,
		"threshold", d.config.SaturationThreshold,
		"decision", decision,
		"pool_size", len(endpoints),
		"detector", d.detectorName)

	return decision
}
