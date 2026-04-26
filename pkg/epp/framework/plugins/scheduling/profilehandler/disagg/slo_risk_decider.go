package disagg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/log"

	fwkdl "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	fwkflowcontrol "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/flowcontrol"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/scheduling/filter/bylabel"
)

const (
	// SLORiskDeciderPluginType is the type-name of the SLORiskDecider plugin.
	SLORiskDeciderPluginType = "slo-risk-decider"

	defaultSaturationThreshold = 1.0
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
func (d *SLORiskDecider) disaggregate(ctx context.Context, _ *scheduling.InferenceRequest, _ scheduling.Endpoint) bool {
	if d.prefillEndpoints == nil {
		// No provider wired yet: conservative behaviour — don't activate FlexibleDecoder.
		return false
	}
	endpoints := d.prefillEndpoints()
	if len(endpoints) == 0 {
		return false
	}
	saturation := d.saturationDetector.Saturation(ctx, endpoints)
	return saturation >= d.config.SaturationThreshold
}
