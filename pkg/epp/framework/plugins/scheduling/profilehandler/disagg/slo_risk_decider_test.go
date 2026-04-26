package disagg

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
)

// mockSaturationDetector implements flowcontrol.SaturationDetector for tests.
type mockSaturationDetector struct {
	saturation float64
}

func (m *mockSaturationDetector) TypedName() plugin.TypedName {
	return plugin.TypedName{Type: "mock-saturation-detector"}
}

func (m *mockSaturationDetector) Saturation(_ context.Context, _ []fwkdl.Endpoint) float64 {
	return m.saturation
}

func makePrefillEndpoint(name, ip string, kv float64, queue int) fwkdl.Endpoint {
	metrics := fwkdl.NewMetrics()
	metrics.KVCacheUsagePercent = kv
	metrics.WaitingQueueSize = queue
	metrics.UpdateTime = time.Now()
	ep := fwkdl.NewEndpoint(
		&fwkdl.EndpointMetadata{
			NamespacedName: k8stypes.NamespacedName{Namespace: "default", Name: name},
			Address:        ip,
			Port:           "8000",
		},
		metrics,
	)
	return ep
}

func TestSLORiskDecider_NoProvider(t *testing.T) {
	detector := &mockSaturationDetector{saturation: 2.0}
	decider, err := NewSLORiskDecider(SLORiskDeciderConfig{SaturationThreshold: 1.0}, detector, nil)
	assert.NoError(t, err)

	// Without a provider, should conservatively return false.
	got := decider.disaggregate(context.Background(), &scheduling.InferenceRequest{}, nil)
	assert.False(t, got, "should return false when no PrefillEndpointProvider is set")
}

func TestSLORiskDecider_EmptyEndpoints(t *testing.T) {
	detector := &mockSaturationDetector{saturation: 2.0}
	provider := func() []fwkdl.Endpoint { return nil }
	decider, err := NewSLORiskDecider(SLORiskDeciderConfig{SaturationThreshold: 1.0}, detector, provider)
	assert.NoError(t, err)

	got := decider.disaggregate(context.Background(), &scheduling.InferenceRequest{}, nil)
	assert.False(t, got, "should return false when prefill endpoint list is empty")
}

func TestSLORiskDecider_Disaggregate(t *testing.T) {
	prefillEndpoint := makePrefillEndpoint("prefill-1", "10.0.0.1", 0.9, 50)
	provider := func() []fwkdl.Endpoint { return []fwkdl.Endpoint{prefillEndpoint} }

	tests := []struct {
		name       string
		saturation float64
		threshold  float64
		want       bool
	}{
		{
			name:       "saturation at threshold → activate FlexibleDecoder",
			saturation: 1.0,
			threshold:  1.0,
			want:       true,
		},
		{
			name:       "saturation above threshold → activate FlexibleDecoder",
			saturation: 1.5,
			threshold:  1.0,
			want:       true,
		},
		{
			name:       "saturation below threshold → do not activate",
			saturation: 0.8,
			threshold:  1.0,
			want:       false,
		},
		{
			name:       "custom threshold: saturation above → activate",
			saturation: 0.85,
			threshold:  0.8,
			want:       true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			detector := &mockSaturationDetector{saturation: tc.saturation}
			decider, err := NewSLORiskDecider(SLORiskDeciderConfig{SaturationThreshold: tc.threshold}, detector, provider)
			assert.NoError(t, err)

			got := decider.disaggregate(context.Background(), &scheduling.InferenceRequest{}, nil)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestSLORiskDecider_WithPrefillEndpointProvider(t *testing.T) {
	prefillEndpoint := makePrefillEndpoint("prefill-1", "10.0.0.1", 0.9, 50)
	detector := &mockSaturationDetector{saturation: 1.5}

	decider, err := NewSLORiskDecider(SLORiskDeciderConfig{SaturationThreshold: 1.0}, detector, nil)
	assert.NoError(t, err)

	// Initially no provider: returns false.
	assert.False(t, decider.disaggregate(context.Background(), nil, nil))

	// After setting provider: returns based on saturation.
	decider.WithPrefillEndpointProvider(func() []fwkdl.Endpoint { return []fwkdl.Endpoint{prefillEndpoint} })
	assert.True(t, decider.disaggregate(context.Background(), nil, nil))
}

func TestSLORiskDecider_ExportedDisaggregateMethod(t *testing.T) {
	prefillEndpoint := makePrefillEndpoint("prefill-1", "10.0.0.1", 0.9, 50)
	provider := func() []fwkdl.Endpoint { return []fwkdl.Endpoint{prefillEndpoint} }
	detector := &mockSaturationDetector{saturation: 1.2}

	decider, err := NewSLORiskDecider(SLORiskDeciderConfig{SaturationThreshold: 1.0}, detector, provider)
	assert.NoError(t, err)

	// Exported Disaggregate() should behave identically to disaggregate().
	got := decider.Disaggregate(context.Background(), &scheduling.InferenceRequest{}, nil)
	assert.True(t, got)
}

func TestNewSLORiskDecider_NilDetector(t *testing.T) {
	_, err := NewSLORiskDecider(SLORiskDeciderConfig{SaturationThreshold: 1.0}, nil, nil)
	assert.Error(t, err, "should reject nil saturation detector")
}
