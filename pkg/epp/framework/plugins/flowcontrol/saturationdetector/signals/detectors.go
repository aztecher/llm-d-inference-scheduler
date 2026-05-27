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

// Package signals contains small SaturationDetector plugins for common PD
// disaggregation scheduling signals. They are intentionally single-purpose so
// manifests can combine them with composite-saturation-detector.
package signals

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-inference-scheduler/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/flowcontrol"
	fwkplugin "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
	framework "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
	attrlatency "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/datalayer/attribute/latency"
	metricextractor "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/datalayer/extractor/metrics"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/flowcontrol/saturationdetector/utilization"
)

const (
	// PredictedTTFTPercentileDetectorType measures a percentile of predicted TTFT against an SLO/threshold.
	PredictedTTFTPercentileDetectorType = "predicted-ttft-percentile-detector"
	// QueueDepthDetectorType measures waiting queue depth pressure.
	QueueDepthDetectorType = "queue-depth-detector"
	// KVCachePressureDetectorType measures KV cache pressure.
	KVCachePressureDetectorType = "kv-cache-pressure-detector"

	aggregationAverage    = "average"
	aggregationMax        = "max"
	aggregationPercentile = "percentile"
)

type percentileConfig struct {
	Percentile   *float64 `json:"percentile,omitempty"`
	ThresholdMs  *float64 `json:"thresholdMs,omitempty"`
	NoDataValue  *float64 `json:"noDataValue,omitempty"`
	UseViolation *bool    `json:"useViolation,omitempty"`
}

type queueDepthConfig struct {
	Threshold   *float64 `json:"threshold,omitempty"`
	Aggregation string   `json:"aggregation,omitempty"`
	Percentile  *float64 `json:"percentile,omitempty"`
	NoDataValue *float64 `json:"noDataValue,omitempty"`
}

type kvCacheConfig struct {
	Threshold                 *float64         `json:"threshold,omitempty"`
	Aggregation               string           `json:"aggregation,omitempty"`
	Percentile                *float64         `json:"percentile,omitempty"`
	NoDataValue               *float64         `json:"noDataValue,omitempty"`
	StaleValue                *float64         `json:"staleValue,omitempty"`
	MetricsStalenessThreshold *metav1.Duration `json:"metricsStalenessThreshold,omitempty"`
}

// PredictedTTFTPercentileDetector returns percentile(predicted TTFT) / thresholdMs.
type PredictedTTFTPercentileDetector struct {
	typedName    fwkplugin.TypedName
	percentile   float64
	thresholdMs  float64
	noDataValue  float64
	useViolation bool
}

// QueueDepthDetector returns aggregate(waiting queue size) / threshold.
type QueueDepthDetector struct {
	typedName   fwkplugin.TypedName
	threshold   float64
	aggregation string
	percentile  float64
	noDataValue float64
}

// KVCachePressureDetector returns aggregate(KV cache usage percent) / threshold.
type KVCachePressureDetector struct {
	typedName                 fwkplugin.TypedName
	threshold                 float64
	aggregation               string
	percentile                float64
	noDataValue               float64
	staleValue                float64
	metricsStalenessThreshold time.Duration
}

var (
	_ flowcontrol.SaturationDetector = &PredictedTTFTPercentileDetector{}
	_ fwkplugin.ConsumerPlugin       = &PredictedTTFTPercentileDetector{}
	_ framework.Filter               = &PredictedTTFTPercentileDetector{}
	_ flowcontrol.SaturationDetector = &QueueDepthDetector{}
	_ fwkplugin.ConsumerPlugin       = &QueueDepthDetector{}
	_ framework.Filter               = &QueueDepthDetector{}
	_ flowcontrol.SaturationDetector = &KVCachePressureDetector{}
	_ fwkplugin.ConsumerPlugin       = &KVCachePressureDetector{}
	_ framework.Filter               = &KVCachePressureDetector{}
)

// PredictedTTFTPercentileDetectorFactory creates a predicted TTFT percentile detector.
func PredictedTTFTPercentileDetectorFactory(name string, rawParameters json.RawMessage, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
	cfg := percentileConfig{}
	if len(rawParameters) > 0 {
		if err := json.Unmarshal(rawParameters, &cfg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal %s config: %w", PredictedTTFTPercentileDetectorType, err)
		}
	}
	detector, err := newPredictedTTFTPercentileDetector(name, cfg, log.FromContext(handle.Context()))
	if err != nil {
		return nil, err
	}
	return detector, nil
}

func newPredictedTTFTPercentileDetector(name string, cfg percentileConfig, logger logr.Logger) (*PredictedTTFTPercentileDetector, error) {
	percentile := valueOr(cfg.Percentile, 0.99)
	if percentile <= 0 || percentile > 1 {
		return nil, fmt.Errorf("%s: percentile must be in (0, 1], got %f", PredictedTTFTPercentileDetectorType, percentile)
	}
	thresholdMs := valueOr(cfg.ThresholdMs, 1)
	if thresholdMs <= 0 {
		return nil, fmt.Errorf("%s: thresholdMs must be positive", PredictedTTFTPercentileDetectorType)
	}
	detector := &PredictedTTFTPercentileDetector{
		typedName:    fwkplugin.TypedName{Type: PredictedTTFTPercentileDetectorType, Name: name},
		percentile:   percentile,
		thresholdMs:  thresholdMs,
		noDataValue:  valueOr(cfg.NoDataValue, 0),
		useViolation: boolValueOr(cfg.UseViolation, false),
	}
	logger.WithName(detector.typedName.String()).V(logutil.DEFAULT).Info("Creating PredictedTTFTPercentileDetector",
		"percentile", percentile,
		"thresholdMs", thresholdMs,
		"noDataValue", detector.noDataValue,
		"useViolation", detector.useViolation)
	return detector, nil
}

func (d *PredictedTTFTPercentileDetector) TypedName() fwkplugin.TypedName {
	return d.typedName
}

func (d *PredictedTTFTPercentileDetector) Consumes() map[string]any {
	return map[string]any{attrlatency.LatencyPredictionInfoKey: attrlatency.LatencyPredictionInfo{}}
}

// Filter is a no-op pass-through. The detector implements framework.Filter so
// it is classified into the SchedulingLayer by DAG ordering, allowing it to
// consume LatencyPredictionInfo produced at the RequestControlLayer.
func (d *PredictedTTFTPercentileDetector) Filter(_ context.Context, _ *framework.CycleState, _ *framework.InferenceRequest, endpoints []framework.Endpoint) []framework.Endpoint {
	return endpoints
}

func (d *PredictedTTFTPercentileDetector) Saturation(_ context.Context, endpoints []datalayer.Endpoint) float64 {
	values := make([]float64, 0, len(endpoints))
	for _, endpoint := range endpoints {
		info, ok := latencyInfo(endpoint)
		if !ok || info.TTFT() <= 0 {
			continue
		}
		if d.useViolation && info.TTFTValid() {
			continue
		}
		values = append(values, info.TTFT())
	}
	if len(values) == 0 {
		return d.noDataValue
	}
	return percentile(values, d.percentile) / d.thresholdMs
}

// QueueDepthDetectorFactory creates a queue depth detector.
func QueueDepthDetectorFactory(name string, rawParameters json.RawMessage, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
	cfg := queueDepthConfig{}
	if len(rawParameters) > 0 {
		if err := json.Unmarshal(rawParameters, &cfg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal %s config: %w", QueueDepthDetectorType, err)
		}
	}
	detector, err := newQueueDepthDetector(name, cfg, log.FromContext(handle.Context()))
	if err != nil {
		return nil, err
	}
	return detector, nil
}

func newQueueDepthDetector(name string, cfg queueDepthConfig, logger logr.Logger) (*QueueDepthDetector, error) {
	aggregation := cfg.Aggregation
	if aggregation == "" {
		aggregation = aggregationAverage
	}
	if err := validateAggregation(QueueDepthDetectorType, aggregation); err != nil {
		return nil, err
	}
	threshold := valueOr(cfg.Threshold, float64(utilization.DefaultQueueDepthThreshold))
	if threshold <= 0 {
		return nil, fmt.Errorf("%s: threshold must be positive", QueueDepthDetectorType)
	}
	p := valueOr(cfg.Percentile, 0.99)
	if p <= 0 || p > 1 {
		return nil, fmt.Errorf("%s: percentile must be in (0, 1], got %f", QueueDepthDetectorType, p)
	}
	detector := &QueueDepthDetector{
		typedName:   fwkplugin.TypedName{Type: QueueDepthDetectorType, Name: name},
		threshold:   threshold,
		aggregation: aggregation,
		percentile:  p,
		noDataValue: valueOr(cfg.NoDataValue, 0),
	}
	logger.WithName(detector.typedName.String()).V(logutil.DEFAULT).Info("Creating QueueDepthDetector",
		"threshold", threshold,
		"aggregation", aggregation,
		"percentile", p)
	return detector, nil
}

func (d *QueueDepthDetector) TypedName() fwkplugin.TypedName {
	return d.typedName
}

func (d *QueueDepthDetector) Consumes() map[string]any {
	return map[string]any{metricextractor.WaitingQueueSizeKey: int(0)}
}

// Filter is a no-op pass-through. The detector implements framework.Filter so
// it is classified into the SchedulingLayer by DAG ordering.
func (d *QueueDepthDetector) Filter(_ context.Context, _ *framework.CycleState, _ *framework.InferenceRequest, endpoints []framework.Endpoint) []framework.Endpoint {
	return endpoints
}

func (d *QueueDepthDetector) Saturation(_ context.Context, endpoints []datalayer.Endpoint) float64 {
	values := make([]float64, 0, len(endpoints))
	for _, endpoint := range endpoints {
		metrics := endpoint.GetMetrics()
		if metrics == nil {
			continue
		}
		values = append(values, float64(metrics.WaitingQueueSize))
	}
	if len(values) == 0 {
		return d.noDataValue
	}
	return aggregate(values, d.aggregation, d.percentile) / d.threshold
}

// KVCachePressureDetectorFactory creates a KV cache pressure detector.
func KVCachePressureDetectorFactory(name string, rawParameters json.RawMessage, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
	cfg := kvCacheConfig{}
	if len(rawParameters) > 0 {
		if err := json.Unmarshal(rawParameters, &cfg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal %s config: %w", KVCachePressureDetectorType, err)
		}
	}
	detector, err := newKVCachePressureDetector(name, cfg, log.FromContext(handle.Context()))
	if err != nil {
		return nil, err
	}
	return detector, nil
}

func newKVCachePressureDetector(name string, cfg kvCacheConfig, logger logr.Logger) (*KVCachePressureDetector, error) {
	aggregation := cfg.Aggregation
	if aggregation == "" {
		aggregation = aggregationAverage
	}
	if err := validateAggregation(KVCachePressureDetectorType, aggregation); err != nil {
		return nil, err
	}
	threshold := valueOr(cfg.Threshold, utilization.DefaultKVCacheUtilThreshold)
	if threshold <= 0 {
		return nil, fmt.Errorf("%s: threshold must be positive", KVCachePressureDetectorType)
	}
	p := valueOr(cfg.Percentile, 0.99)
	if p <= 0 || p > 1 {
		return nil, fmt.Errorf("%s: percentile must be in (0, 1], got %f", KVCachePressureDetectorType, p)
	}
	staleness := utilization.DefaultMetricsStalenessThreshold
	if cfg.MetricsStalenessThreshold != nil {
		staleness = cfg.MetricsStalenessThreshold.Duration
	}
	detector := &KVCachePressureDetector{
		typedName:                 fwkplugin.TypedName{Type: KVCachePressureDetectorType, Name: name},
		threshold:                 threshold,
		aggregation:               aggregation,
		percentile:                p,
		noDataValue:               valueOr(cfg.NoDataValue, 0),
		staleValue:                valueOr(cfg.StaleValue, 1),
		metricsStalenessThreshold: staleness,
	}
	logger.WithName(detector.typedName.String()).V(logutil.DEFAULT).Info("Creating KVCachePressureDetector",
		"threshold", threshold,
		"aggregation", aggregation,
		"percentile", p,
		"metricsStalenessThreshold", staleness.String())
	return detector, nil
}

func (d *KVCachePressureDetector) TypedName() fwkplugin.TypedName {
	return d.typedName
}

func (d *KVCachePressureDetector) Consumes() map[string]any {
	return map[string]any{metricextractor.KVCacheUsagePercentKey: float64(0)}
}

// Filter is a no-op pass-through. The detector implements framework.Filter so
// it is classified into the SchedulingLayer by DAG ordering.
func (d *KVCachePressureDetector) Filter(_ context.Context, _ *framework.CycleState, _ *framework.InferenceRequest, endpoints []framework.Endpoint) []framework.Endpoint {
	return endpoints
}

func (d *KVCachePressureDetector) Saturation(_ context.Context, endpoints []datalayer.Endpoint) float64 {
	values := make([]float64, 0, len(endpoints))
	now := time.Now()
	for _, endpoint := range endpoints {
		metrics := endpoint.GetMetrics()
		if metrics == nil {
			continue
		}
		if !metrics.UpdateTime.IsZero() && now.Sub(metrics.UpdateTime) > d.metricsStalenessThreshold {
			values = append(values, d.staleValue*d.threshold)
			continue
		}
		values = append(values, metrics.KVCacheUsagePercent)
	}
	if len(values) == 0 {
		return d.noDataValue
	}
	return aggregate(values, d.aggregation, d.percentile) / d.threshold
}

func latencyInfo(endpoint datalayer.Endpoint) (*attrlatency.LatencyPredictionInfo, bool) {
	if endpoint == nil || endpoint.GetAttributes() == nil {
		return nil, false
	}
	raw, ok := endpoint.GetAttributes().Get(attrlatency.LatencyPredictionInfoKey)
	if !ok {
		return nil, false
	}
	info, ok := raw.(*attrlatency.LatencyPredictionInfo)
	return info, ok
}

func aggregate(values []float64, method string, p float64) float64 {
	switch method {
	case aggregationMax:
		return maxValue(values)
	case aggregationPercentile:
		return percentile(values, p)
	default:
		return average(values)
	}
}

func average(values []float64) float64 {
	var total float64
	for _, value := range values {
		total += value
	}
	return total / float64(len(values))
}

func maxValue(values []float64) float64 {
	maximum := math.Inf(-1)
	for _, value := range values {
		maximum = max(maximum, value)
	}
	return maximum
}

func percentile(values []float64, p float64) float64 {
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	index := int(math.Ceil(p*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func validateAggregation(pluginType, aggregation string) error {
	switch aggregation {
	case aggregationAverage, aggregationMax, aggregationPercentile:
		return nil
	default:
		return fmt.Errorf("%s: unsupported aggregation %q", pluginType, aggregation)
	}
}

func valueOr(value *float64, fallback float64) float64 {
	if value == nil {
		return fallback
	}
	return *value
}

func boolValueOr(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}
