/*
Copyright 2026 The Kubernetes Authors.

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

// Package composite implements a SaturationDetector that combines other
// SaturationDetector plugins. It is intended for offline tuning through
// EndpointPickerConfig: operators can add, remove, re-weight, or re-threshold
// signals without changing the decider that consumes the aggregate signal.
package composite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/go-logr/logr"
	framework "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-inference-scheduler/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/flowcontrol"
	fwkplugin "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
)

const (
	// CompositeDetectorType is the type-name of the composite SaturationDetector plugin.
	CompositeDetectorType = "composite-saturation-detector"

	strategyMax             = "max"
	strategyMin             = "min"
	strategyAverage         = "average"
	strategyWeightedAverage = "weightedAverage"
)

type detectorRefConfig struct {
	// PluginName is the configured plugin instance name of a SaturationDetector.
	PluginName string `json:"pluginName"`
	// Weight is used by weightedAverage. Defaults to 1.
	Weight float64 `json:"weight,omitempty"`
	// Threshold normalizes a detector's raw saturation into an activation ratio.
	// A raw value equal to Threshold becomes 1.0. Defaults to 1.
	Threshold float64 `json:"threshold,omitempty"`
	// Enabled lets manifests keep candidate signals around while excluding them
	// from the aggregate without deleting the block. Defaults to true.
	Enabled *bool `json:"enabled,omitempty"`
}

type apiConfig struct {
	// Strategy selects how normalized detector values are aggregated.
	// Supported: max, min, average, weightedAverage. Defaults to max.
	Strategy string `json:"strategy,omitempty"`
	// Detectors lists the child SaturationDetector plugin instances to combine.
	Detectors []detectorRefConfig `json:"detectors"`
	// EmptyValue is returned when all detector refs are disabled. Defaults to 0.
	EmptyValue *float64 `json:"emptyValue,omitempty"`
}

type detectorRef struct {
	name      string
	detector  flowcontrol.SaturationDetector
	weight    float64
	threshold float64
}

// Detector combines multiple child SaturationDetectors into one saturation signal.
type Detector struct {
	typedName  fwkplugin.TypedName
	strategy   string
	detectors  []detectorRef
	emptyValue float64
}

var (
	_ flowcontrol.SaturationDetector = &Detector{}
	_ fwkplugin.ConsumerPlugin       = &Detector{}
	_ framework.Filter               = &Detector{}
)

// Factory creates a CompositeDetector from EndpointPickerConfig parameters.
func Factory(name string, params json.RawMessage, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
	cfg := apiConfig{Strategy: strategyMax}
	if len(params) > 0 {
		if err := json.Unmarshal(params, &cfg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal %s config: %w", CompositeDetectorType, err)
		}
	}
	detector, err := newDetector(name, cfg, handle, log.FromContext(handle.Context()))
	if err != nil {
		return nil, err
	}
	return detector, nil
}

func newDetector(name string, cfg apiConfig, handle fwkplugin.Handle, logger logr.Logger) (*Detector, error) {
	if cfg.Strategy == "" {
		cfg.Strategy = strategyMax
	}
	switch cfg.Strategy {
	case strategyMax, strategyMin, strategyAverage, strategyWeightedAverage:
	default:
		return nil, fmt.Errorf("%s: unsupported strategy %q", CompositeDetectorType, cfg.Strategy)
	}

	refs := make([]detectorRef, 0, len(cfg.Detectors))
	for i, refCfg := range cfg.Detectors {
		if refCfg.Enabled != nil && !*refCfg.Enabled {
			continue
		}
		if refCfg.PluginName == "" {
			return nil, fmt.Errorf("%s: detectors[%d].pluginName must be specified", CompositeDetectorType, i)
		}
		weight := refCfg.Weight
		if weight == 0 {
			weight = 1
		}
		if weight < 0 {
			return nil, fmt.Errorf("%s: detectors[%d].weight must be non-negative", CompositeDetectorType, i)
		}
		threshold := refCfg.Threshold
		if threshold == 0 {
			threshold = 1
		}
		if threshold < 0 {
			return nil, fmt.Errorf("%s: detectors[%d].threshold must be positive", CompositeDetectorType, i)
		}

		plugin := handle.Plugin(refCfg.PluginName)
		if plugin == nil {
			return nil, fmt.Errorf("%s: detector plugin %q not found", CompositeDetectorType, refCfg.PluginName)
		}
		child, ok := plugin.(flowcontrol.SaturationDetector)
		if !ok {
			return nil, fmt.Errorf("%s: plugin %q does not implement SaturationDetector", CompositeDetectorType, refCfg.PluginName)
		}
		refs = append(refs, detectorRef{
			name:      refCfg.PluginName,
			detector:  child,
			weight:    weight,
			threshold: threshold,
		})
	}

	if len(refs) == 0 && cfg.EmptyValue == nil {
		return nil, errors.New(CompositeDetectorType + ": at least one enabled detector must be configured")
	}

	emptyValue := 0.0
	if cfg.EmptyValue != nil {
		emptyValue = *cfg.EmptyValue
	}

	typedName := fwkplugin.TypedName{Type: CompositeDetectorType, Name: name}
	logger.WithName(typedName.String()).V(logutil.DEFAULT).Info("Creating CompositeDetector",
		"strategy", cfg.Strategy,
		"detectors", detectorNames(refs),
		"emptyValue", emptyValue)

	return &Detector{
		typedName:  typedName,
		strategy:   cfg.Strategy,
		detectors:  refs,
		emptyValue: emptyValue,
	}, nil
}

// TypedName returns the plugin type and instance name.
func (d *Detector) TypedName() fwkplugin.TypedName {
	return d.typedName
}

// Consumes returns the union of all child detector data dependencies.
func (d *Detector) Consumes() map[string]any {
	consumes := map[string]any{}
	for _, ref := range d.detectors {
		consumer, ok := ref.detector.(fwkplugin.ConsumerPlugin)
		if !ok {
			continue
		}
		for key, value := range consumer.Consumes() {
			consumes[key] = value
		}
	}
	if len(consumes) == 0 {
		return nil
	}
	return consumes
}

// Filter is a no-op pass-through. The detector implements framework.Filter so
// it is classified into the SchedulingLayer by DAG ordering, allowing it to
// consume LatencyPredictionInfo produced at the RequestControlLayer.
func (d *Detector) Filter(_ context.Context, _ *framework.CycleState, _ *framework.InferenceRequest, endpoints []framework.Endpoint) []framework.Endpoint {
	return endpoints
}

// Saturation returns the configured aggregate of normalized child detector signals.
func (d *Detector) Saturation(ctx context.Context, endpoints []datalayer.Endpoint) float64 {
	if len(d.detectors) == 0 {
		return d.emptyValue
	}

	switch d.strategy {
	case strategyMax:
		value := math.Inf(-1)
		for _, ref := range d.detectors {
			value = max(value, d.normalized(ctx, ref, endpoints))
		}
		return value
	case strategyMin:
		value := math.Inf(1)
		for _, ref := range d.detectors {
			value = min(value, d.normalized(ctx, ref, endpoints))
		}
		return value
	case strategyAverage:
		var total float64
		for _, ref := range d.detectors {
			total += d.normalized(ctx, ref, endpoints)
		}
		return total / float64(len(d.detectors))
	case strategyWeightedAverage:
		var total, weights float64
		for _, ref := range d.detectors {
			total += d.normalized(ctx, ref, endpoints) * ref.weight
			weights += ref.weight
		}
		if weights == 0 {
			return d.emptyValue
		}
		return total / weights
	default:
		return d.emptyValue
	}
}

func (d *Detector) normalized(ctx context.Context, ref detectorRef, endpoints []datalayer.Endpoint) float64 {
	return ref.detector.Saturation(ctx, endpoints) / ref.threshold
}

func detectorNames(refs []detectorRef) []string {
	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		names = append(names, ref.name)
	}
	return names
}
