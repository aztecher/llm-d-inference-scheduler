package disagg

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
)

func makeEndpointWithMetrics(name, ip, port string, kvCache float64, queue int) scheduling.Endpoint {
	m := fwkdl.NewMetrics()
	m.KVCacheUsagePercent = kvCache
	m.WaitingQueueSize = queue
	m.UpdateTime = time.Now()
	return scheduling.NewEndpoint(
		&fwkdl.EndpointMetadata{
			NamespacedName: k8stypes.NamespacedName{Namespace: "default", Name: name},
			Address:        ip,
			Port:           port,
		},
		m,
		fwkdl.NewAttributes(),
	)
}

func TestSelfDecodeCapacityDecider_NilEndpoint(t *testing.T) {
	d := NewSelfDecodeCapacityDecider(SelfDecodeCapacityDeciderConfig{
		KVCacheThreshold:    0.8,
		QueueDepthThreshold: 40,
	})
	assert.False(t, d.Disaggregate(context.Background(), nil, nil))
}

func TestSelfDecodeCapacityDecider_Disaggregate(t *testing.T) {
	config := SelfDecodeCapacityDeciderConfig{
		KVCacheThreshold:    0.8,
		QueueDepthThreshold: 40.0,
	}
	d := NewSelfDecodeCapacityDecider(config)

	tests := []struct {
		name    string
		kvCache float64
		queue   int
		want    bool
	}{
		{
			name:    "both below threshold → self-decode OK",
			kvCache: 0.5,
			queue:   10,
			want:    true,
		},
		{
			name:    "KV cache at threshold → self-decode NOT OK",
			kvCache: 0.8,
			queue:   10,
			want:    false,
		},
		{
			name:    "KV cache above threshold → self-decode NOT OK",
			kvCache: 0.9,
			queue:   10,
			want:    false,
		},
		{
			name:    "queue at threshold → self-decode NOT OK",
			kvCache: 0.5,
			queue:   40,
			want:    false,
		},
		{
			name:    "queue above threshold → self-decode NOT OK",
			kvCache: 0.5,
			queue:   50,
			want:    false,
		},
		{
			name:    "both at/above threshold → self-decode NOT OK",
			kvCache: 0.9,
			queue:   50,
			want:    false,
		},
		{
			name:    "zero KV cache, zero queue → self-decode OK",
			kvCache: 0.0,
			queue:   0,
			want:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ep := makeEndpointWithMetrics("pod", "10.0.0.1", testPodPort, tc.kvCache, tc.queue)
			got := d.Disaggregate(context.Background(), &scheduling.InferenceRequest{}, ep)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestSelfDecodeCapacityDecider_DefaultConfig(t *testing.T) {
	d := NewSelfDecodeCapacityDecider(SelfDecodeCapacityDeciderConfig{})
	assert.Equal(t, defaultKVCacheThreshold, d.config.KVCacheThreshold)
	assert.Equal(t, defaultQueueDepthThreshold, d.config.QueueDepthThreshold)
}

func TestSelfDecodeCapacityDecider_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  SelfDecodeCapacityDeciderConfig
		wantErr bool
	}{
		{
			name:    "zero KV threshold (will be defaulted)",
			config:  SelfDecodeCapacityDeciderConfig{KVCacheThreshold: 0, QueueDepthThreshold: 40},
			wantErr: false,
		},
		{
			name:    "KV threshold above 1.0",
			config:  SelfDecodeCapacityDeciderConfig{KVCacheThreshold: 1.1, QueueDepthThreshold: 40},
			wantErr: true,
		},
		{
			name:    "negative KV threshold",
			config:  SelfDecodeCapacityDeciderConfig{KVCacheThreshold: -0.1, QueueDepthThreshold: 40},
			wantErr: true,
		},
		{
			name:    "negative queue threshold",
			config:  SelfDecodeCapacityDeciderConfig{KVCacheThreshold: 0.8, QueueDepthThreshold: -1},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.config.applyDefaults()
			err := tc.config.validate()
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
