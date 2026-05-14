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
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/requestcontrol/dataproducer/poolview"
)

// mockSaturationDetector implements flowcontrol.SaturationDetector for tests.
type mockSaturationDetector struct {
	saturation float64
	gotCount   int // recorded for assertions
}

func (m *mockSaturationDetector) TypedName() plugin.TypedName {
	return plugin.TypedName{Type: "mock-saturation-detector"}
}

func (m *mockSaturationDetector) Saturation(_ context.Context, eps []fwkdl.Endpoint) float64 {
	m.gotCount = len(eps)
	return m.saturation
}

// makeSchedulingEndpoint constructs a per-request snapshot endpoint
// (the type PoolViewProducer publishes to PluginState).
func makeSchedulingEndpoint(name string) scheduling.Endpoint {
	metrics := fwkdl.NewMetrics()
	metrics.UpdateTime = time.Now()
	return scheduling.NewEndpoint(
		&fwkdl.EndpointMetadata{
			NamespacedName: k8stypes.NamespacedName{Namespace: "default", Name: name},
			Address:        "10.0.0.1",
			Port:           "8000",
		},
		metrics,
		fwkdl.NewAttributes(),
	)
}

// setupPluginStateWithPool populates a PluginState with a single-pool PoolView.
func setupPluginStateWithPool(requestID, poolName string, endpoints []scheduling.Endpoint) *plugin.PluginState {
	state := plugin.NewPluginState(context.Background())
	view := poolview.NewPoolView(map[string][]scheduling.Endpoint{
		poolName: endpoints,
	})
	state.Write(requestID, poolview.PoolViewKey, view)
	return state
}

// makeDecider is a helper that wires SLOBasedFlexDecider for a given request id.
func makeDecider(t *testing.T, threshold float64, sat float64, endpoints []scheduling.Endpoint, requestID string) (*SLOBasedFlexDecider, *mockSaturationDetector) {
	t.Helper()
	detector := &mockSaturationDetector{saturation: sat}
	state := setupPluginStateWithPool(requestID, poolview.DefaultPrefillPool, endpoints)
	decider, err := NewSLOBasedFlexDecider(
		SLOBasedFlexDeciderConfig{
			SaturationDetectorPluginName: "mock",
			SaturationThreshold:          threshold,
			Pool:                         poolview.DefaultPrefillPool,
		},
		detector,
		state,
	)
	assert.NoError(t, err)
	return decider, detector
}

func TestNewSLOBasedFlexDecider_NilDetector(t *testing.T) {
	state := plugin.NewPluginState(context.Background())
	_, err := NewSLOBasedFlexDecider(SLOBasedFlexDeciderConfig{SaturationThreshold: 1.0}, nil, state)
	assert.Error(t, err, "should reject nil saturation detector")
}

func TestNewSLOBasedFlexDecider_NilPluginState(t *testing.T) {
	detector := &mockSaturationDetector{saturation: 0.5}
	_, err := NewSLOBasedFlexDecider(SLOBasedFlexDeciderConfig{SaturationThreshold: 1.0}, detector, nil)
	assert.Error(t, err, "should reject nil plugin state")
}

func TestNewSLOBasedFlexDecider_DefaultsApplied(t *testing.T) {
	detector := &mockSaturationDetector{saturation: 0.5}
	state := plugin.NewPluginState(context.Background())
	d, err := NewSLOBasedFlexDecider(SLOBasedFlexDeciderConfig{}, detector, state)
	assert.NoError(t, err)
	assert.Equal(t, defaultSaturationThreshold, d.config.SaturationThreshold,
		"zero threshold should be replaced by default")
	assert.Equal(t, poolview.DefaultPrefillPool, d.poolName,
		"empty pool should default to prefill")
}

func TestSLOBasedFlexDecider_NoPoolView(t *testing.T) {
	detector := &mockSaturationDetector{saturation: 2.0}
	state := plugin.NewPluginState(context.Background()) // empty: no PoolView for this request
	decider, err := NewSLOBasedFlexDecider(
		SLOBasedFlexDeciderConfig{
			SaturationDetectorPluginName: "mock",
			SaturationThreshold:          1.0,
			Pool:                         poolview.DefaultPrefillPool,
		},
		detector,
		state,
	)
	assert.NoError(t, err)

	req := &scheduling.InferenceRequest{RequestId: "no-view"}
	got := decider.disaggregate(context.Background(), req, nil)
	assert.False(t, got, "should return false when PoolView is missing from PluginState")
	assert.Equal(t, 0, detector.gotCount, "saturation detector should not be invoked")
}

func TestSLOBasedFlexDecider_EmptyPool(t *testing.T) {
	detector := &mockSaturationDetector{saturation: 2.0}
	requestID := "empty-pool"
	// PoolView exists but the requested pool is empty.
	state := setupPluginStateWithPool(requestID, poolview.DefaultPrefillPool, nil)
	decider, err := NewSLOBasedFlexDecider(
		SLOBasedFlexDeciderConfig{
			SaturationDetectorPluginName: "mock",
			SaturationThreshold:          1.0,
			Pool:                         poolview.DefaultPrefillPool,
		},
		detector,
		state,
	)
	assert.NoError(t, err)

	req := &scheduling.InferenceRequest{RequestId: requestID}
	got := decider.disaggregate(context.Background(), req, nil)
	assert.False(t, got, "should return false when the pool has no endpoints")
	assert.Equal(t, 0, detector.gotCount, "saturation detector should not be invoked")
}

func TestSLOBasedFlexDecider_DifferentPoolName(t *testing.T) {
	detector := &mockSaturationDetector{saturation: 2.0}
	requestID := "wrong-pool"
	endpoints := []scheduling.Endpoint{makeSchedulingEndpoint("p-0")}
	// PoolView holds endpoints under "decode" but decider asks for "prefill".
	state := setupPluginStateWithPool(requestID, "decode", endpoints)
	decider, err := NewSLOBasedFlexDecider(
		SLOBasedFlexDeciderConfig{
			SaturationDetectorPluginName: "mock",
			SaturationThreshold:          1.0,
			Pool:                         poolview.DefaultPrefillPool,
		},
		detector,
		state,
	)
	assert.NoError(t, err)

	got := decider.disaggregate(context.Background(),
		&scheduling.InferenceRequest{RequestId: requestID}, nil)
	assert.False(t, got, "should return false when the configured pool is not in the PoolView")
}

func TestSLOBasedFlexDecider_disaggregate(t *testing.T) {
	endpoints := []scheduling.Endpoint{makeSchedulingEndpoint("p-0")}
	requestID := "threshold-test"

	tests := []struct {
		name       string
		saturation float64
		threshold  float64
		want       bool
	}{
		{
			name:       "saturation at threshold - activate FlexibleDecode",
			saturation: 1.0,
			threshold:  1.0,
			want:       true,
		},
		{
			name:       "saturation above threshold - activate FlexibleDecode",
			saturation: 1.5,
			threshold:  1.0,
			want:       true,
		},
		{
			name:       "saturation below threshold - do not activate",
			saturation: 0.8,
			threshold:  1.0,
			want:       false,
		},
		{
			name:       "custom threshold: saturation above - activate",
			saturation: 0.85,
			threshold:  0.8,
			want:       true,
		},
		{
			name:       "small saturation, small threshold (non-zero) - activate (>= comparison)",
			saturation: 0.1,
			threshold:  0.1,
			want:       true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			decider, detector := makeDecider(t, tc.threshold, tc.saturation, endpoints, requestID)
			got := decider.disaggregate(context.Background(),
				&scheduling.InferenceRequest{RequestId: requestID}, nil)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, len(endpoints), detector.gotCount,
				"saturation detector should receive the pool endpoints")
		})
	}
}
