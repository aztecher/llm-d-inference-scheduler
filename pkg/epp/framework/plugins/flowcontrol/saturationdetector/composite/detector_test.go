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

package composite

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
)

type stubDetector struct {
	typedName plugin.TypedName
	value     float64
	consumes  map[string]any
}

func (d *stubDetector) TypedName() plugin.TypedName {
	return d.typedName
}

func (d *stubDetector) Saturation(context.Context, []datalayer.Endpoint) float64 {
	return d.value
}

func (d *stubDetector) Consumes() map[string]any {
	return d.consumes
}

type nonDetector struct {
	typedName plugin.TypedName
}

func (p *nonDetector) TypedName() plugin.TypedName {
	return p.typedName
}

func newHandleWithDetectors() plugin.Handle {
	handle := plugin.NewEppHandle(context.Background(), nil)
	handle.AddPlugin("ttft", &stubDetector{
		typedName: plugin.TypedName{Type: "stub-detector", Name: "ttft"},
		value:     0.6,
		consumes:  map[string]any{"latency": "latency"},
	})
	handle.AddPlugin("queue", &stubDetector{
		typedName: plugin.TypedName{Type: "stub-detector", Name: "queue"},
		value:     0.8,
		consumes:  map[string]any{"queue": "queue"},
	})
	return handle
}

func TestSaturation_MaxNormalizesByThreshold(t *testing.T) {
	handle := newHandleWithDetectors()
	plugin, err := Factory("composite", []byte(`{
		"strategy": "max",
		"detectors": [
			{"pluginName": "ttft", "threshold": 0.3},
			{"pluginName": "queue", "threshold": 1.0}
		]
	}`), handle)
	require.NoError(t, err)

	detector := plugin.(*Detector)
	require.InDelta(t, 2.0, detector.Saturation(context.Background(), nil), 0.0001)
}

func TestSaturation_WeightedAverage(t *testing.T) {
	handle := newHandleWithDetectors()
	plugin, err := Factory("composite", []byte(`{
		"strategy": "weightedAverage",
		"detectors": [
			{"pluginName": "ttft", "weight": 3.0},
			{"pluginName": "queue", "weight": 1.0}
		]
	}`), handle)
	require.NoError(t, err)

	detector := plugin.(*Detector)
	require.InDelta(t, 0.65, detector.Saturation(context.Background(), nil), 0.0001)
}

func TestSaturation_DisabledDetectorIsIgnored(t *testing.T) {
	handle := newHandleWithDetectors()
	plugin, err := Factory("composite", []byte(`{
		"strategy": "average",
		"detectors": [
			{"pluginName": "ttft"},
			{"pluginName": "queue", "enabled": false}
		]
	}`), handle)
	require.NoError(t, err)

	detector := plugin.(*Detector)
	require.InDelta(t, 0.6, detector.Saturation(context.Background(), nil), 0.0001)
}

func TestConsumes_UnionsChildConsumes(t *testing.T) {
	handle := newHandleWithDetectors()
	plugin, err := Factory("composite", []byte(`{
		"detectors": [
			{"pluginName": "ttft"},
			{"pluginName": "queue"}
		]
	}`), handle)
	require.NoError(t, err)

	consumes := plugin.(*Detector).Consumes()
	require.Contains(t, consumes, "latency")
	require.Contains(t, consumes, "queue")
}

func TestFactoryValidation(t *testing.T) {
	t.Run("missing plugin", func(t *testing.T) {
		_, err := Factory("composite", []byte(`{"detectors": [{"pluginName": "missing"}]}`), newHandleWithDetectors())
		require.Error(t, err)
	})

	t.Run("plugin is not detector", func(t *testing.T) {
		handle := newHandleWithDetectors()
		handle.AddPlugin("not-detector", &nonDetector{typedName: plugin.TypedName{Type: "x", Name: "not-detector"}})
		_, err := Factory("composite", []byte(`{"detectors": [{"pluginName": "not-detector"}]}`), handle)
		require.Error(t, err)
	})

	t.Run("unsupported strategy", func(t *testing.T) {
		_, err := Factory("composite", []byte(`{"strategy": "median", "detectors": [{"pluginName": "ttft"}]}`), newHandleWithDetectors())
		require.Error(t, err)
	})
}
