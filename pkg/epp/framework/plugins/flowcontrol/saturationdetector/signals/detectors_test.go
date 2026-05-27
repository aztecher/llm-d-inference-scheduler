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

package signals

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
	attrlatency "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/datalayer/attribute/latency"
)

func testHandle() plugin.Handle {
	return plugin.NewEppHandle(context.Background(), nil)
}

func testEndpoint(name string, metrics *datalayer.Metrics) datalayer.Endpoint {
	return datalayer.NewEndpoint(&datalayer.EndpointMetadata{
		NamespacedName: types.NamespacedName{Namespace: "default", Name: name},
	}, metrics)
}

func testEndpointWithTTFT(name string, ttft float64, valid bool) datalayer.Endpoint {
	endpoint := testEndpoint(name, datalayer.NewMetrics())
	endpoint.GetAttributes().Put(attrlatency.LatencyPredictionInfoKey,
		attrlatency.NewLatencyPredictionInfo(valid, true, 500-ttft, 0, ttft, 0, 0))
	return endpoint
}

func TestPredictedTTFTPercentileDetector(t *testing.T) {
	plugin, err := PredictedTTFTPercentileDetectorFactory("p99-ttft", []byte(`{
		"percentile": 0.99,
		"thresholdMs": 500
	}`), testHandle())
	require.NoError(t, err)

	got := plugin.(*PredictedTTFTPercentileDetector).Saturation(context.Background(), []datalayer.Endpoint{
		testEndpointWithTTFT("a", 100, true),
		testEndpointWithTTFT("b", 300, true),
		testEndpointWithTTFT("c", 700, false),
	})
	require.InDelta(t, 1.4, got, 0.0001)
	require.Contains(t, plugin.(*PredictedTTFTPercentileDetector).Consumes(), attrlatency.LatencyPredictionInfoKey)
}

func TestPredictedTTFTPercentileDetector_UseViolation(t *testing.T) {
	plugin, err := PredictedTTFTPercentileDetectorFactory("violating-ttft", []byte(`{
		"percentile": 0.5,
		"thresholdMs": 500,
		"useViolation": true
	}`), testHandle())
	require.NoError(t, err)

	got := plugin.(*PredictedTTFTPercentileDetector).Saturation(context.Background(), []datalayer.Endpoint{
		testEndpointWithTTFT("a", 100, true),
		testEndpointWithTTFT("b", 600, false),
		testEndpointWithTTFT("c", 900, false),
	})
	require.InDelta(t, 1.2, got, 0.0001)
}

func TestQueueDepthDetector(t *testing.T) {
	plugin, err := QueueDepthDetectorFactory("queue-p99", []byte(`{
		"threshold": 10,
		"aggregation": "percentile",
		"percentile": 0.99
	}`), testHandle())
	require.NoError(t, err)

	got := plugin.(*QueueDepthDetector).Saturation(context.Background(), []datalayer.Endpoint{
		testEndpoint("a", &datalayer.Metrics{WaitingQueueSize: 1}),
		testEndpoint("b", &datalayer.Metrics{WaitingQueueSize: 4}),
		testEndpoint("c", &datalayer.Metrics{WaitingQueueSize: 12}),
	})
	require.InDelta(t, 1.2, got, 0.0001)
}

func TestKVCachePressureDetector(t *testing.T) {
	plugin, err := KVCachePressureDetectorFactory("kv-max", []byte(`{
		"threshold": 0.8,
		"aggregation": "max"
	}`), testHandle())
	require.NoError(t, err)

	now := time.Now()
	got := plugin.(*KVCachePressureDetector).Saturation(context.Background(), []datalayer.Endpoint{
		testEndpoint("a", &datalayer.Metrics{KVCacheUsagePercent: 0.4, UpdateTime: now}),
		testEndpoint("b", &datalayer.Metrics{KVCacheUsagePercent: 0.9, UpdateTime: now}),
	})
	require.InDelta(t, 1.125, got, 0.0001)
}

func TestKVCachePressureDetector_StaleMetrics(t *testing.T) {
	plugin, err := KVCachePressureDetectorFactory("kv-stale", []byte(`{
		"threshold": 0.8,
		"aggregation": "max",
		"metricsStalenessThreshold": "1ms",
		"staleValue": 1.5
	}`), testHandle())
	require.NoError(t, err)

	got := plugin.(*KVCachePressureDetector).Saturation(context.Background(), []datalayer.Endpoint{
		testEndpoint("a", &datalayer.Metrics{KVCacheUsagePercent: 0.1, UpdateTime: time.Now().Add(-time.Hour)}),
	})
	require.InDelta(t, 1.5, got, 0.0001)
}

func TestFactoryValidation(t *testing.T) {
	_, err := PredictedTTFTPercentileDetectorFactory("bad", []byte(`{"percentile": 2}`), testHandle())
	require.Error(t, err)

	_, err = QueueDepthDetectorFactory("bad", []byte(`{"aggregation": "sum"}`), testHandle())
	require.Error(t, err)

	_, err = KVCachePressureDetectorFactory("bad", []byte(`{"threshold": -1}`), testHandle())
	require.Error(t, err)
}
