/*
Copyright 2025 The Kubernetes Authors.

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

package poolview

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/types"

	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
)

// stubHandle is a minimal plugin.Handle for tests that only consult Context().
type stubHandle struct{ ctx context.Context }

func (h stubHandle) Context() context.Context        { return h.ctx }
func (h stubHandle) Plugin(string) plugin.Plugin     { return nil }
func (h stubHandle) AddPlugin(string, plugin.Plugin) {}
func (h stubHandle) GetAllPlugins() []plugin.Plugin  { return nil }
func (h stubHandle) GetAllPluginsWithNames() map[string]plugin.Plugin {
	return map[string]plugin.Plugin{}
}
func (h stubHandle) PodList() []types.NamespacedName { return nil }

func TestPoolViewProducerFactory(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{
			name:    "empty config: no pools",
			raw:     `{}`,
			wantErr: true,
		},
		{
			name:    "valid config",
			raw:     `{"pools": {"prefill": {"roles": ["prefill"]}}}`,
			wantErr: false,
		},
		{
			name:    "pool with empty roles",
			raw:     `{"pools": {"prefill": {"roles": []}}}`,
			wantErr: true,
		},
		{
			name:    "multiple pools",
			raw:     `{"pools": {"prefill": {"roles": ["prefill"]}, "flex": {"roles": ["flexible-decode"]}}}`,
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			handle := stubHandle{ctx: context.Background()}
			_, err := PoolViewProducerFactory("test", json.RawMessage(tc.raw), handle)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestPoolViewProducer_TypedName(t *testing.T) {
	p := NewPoolViewProducer(context.Background(), "my-name", map[string]PoolSelector{
		"prefill": NewRoleLabelSelector([]string{"prefill"}),
	})
	assert.Equal(t, PoolViewProducerPluginType, p.TypedName().Type)
	assert.Equal(t, "my-name", p.TypedName().Name)
}

func TestProducesConsumes(t *testing.T) {
	p := NewPoolViewProducer(context.Background(), "x", map[string]PoolSelector{
		"prefill": NewRoleLabelSelector([]string{"prefill"}),
	})
	produces := p.Produces()
	_, ok := produces[string(PoolViewKey)]
	assert.True(t, ok, "Produces() must declare PoolViewKey")
	assert.Nil(t, p.Consumes(),
		"Consumes() returns nil because PoolView only filters; AttributeMap data is read by downstream consumers, not by this producer")
}

func TestPrepareRequestData_PartitionsByRole(t *testing.T) {
	p := NewPoolViewProducer(context.Background(), "test", map[string]PoolSelector{
		"prefill": NewRoleLabelSelector([]string{"prefill", "prefill-decode"}),
		"flex":    NewRoleLabelSelector([]string{"flexible-decode"}),
	})

	endpoints := []scheduling.Endpoint{
		makeEndpointWithRole("p-0", "prefill"),
		makeEndpointWithRole("p-1", "prefill-decode"),
		makeEndpointWithRole("d-0", "decode"),
		makeEndpointWithRole("f-0", "flexible-decode"),
		makeEndpointWithRole("u-0", ""), // unlabeled
	}

	req := &scheduling.InferenceRequest{RequestId: "test-req"}
	err := p.PrepareRequestData(context.Background(), req, endpoints)
	assert.NoError(t, err)

	view, err := plugin.ReadPluginStateKey[*PoolView](p.PluginState(), "test-req", PoolViewKey)
	assert.NoError(t, err)
	assert.Len(t, view.Pool("prefill"), 2, "two endpoints have prefill-class role")
	assert.Len(t, view.Pool("flex"), 1, "one endpoint has flexible-decode role")
	assert.Nil(t, view.Pool("nonexistent"))
}

func TestPrepareRequestData_EmptyEndpoints(t *testing.T) {
	p := NewPoolViewProducer(context.Background(), "test", map[string]PoolSelector{
		"prefill": NewRoleLabelSelector([]string{"prefill"}),
	})

	req := &scheduling.InferenceRequest{RequestId: "empty-req"}
	err := p.PrepareRequestData(context.Background(), req, nil)
	assert.NoError(t, err)

	view, err := plugin.ReadPluginStateKey[*PoolView](p.PluginState(), "empty-req", PoolViewKey)
	assert.NoError(t, err)
	assert.Empty(t, view.Pool("prefill"), "empty input → empty pool")
}

func TestPrepareRequestData_NilRequest_Errors(t *testing.T) {
	p := NewPoolViewProducer(context.Background(), "test", map[string]PoolSelector{
		"prefill": NewRoleLabelSelector([]string{"prefill"}),
	})

	err := p.PrepareRequestData(context.Background(), nil, nil)
	assert.Error(t, err, "nil request must be rejected to avoid PluginState corruption")
}

// Ensure independent requests get independent PoolViews.
func TestPrepareRequestData_PerRequestIsolation(t *testing.T) {
	p := NewPoolViewProducer(context.Background(), "test", map[string]PoolSelector{
		"prefill": NewRoleLabelSelector([]string{"prefill"}),
	})

	req1 := &scheduling.InferenceRequest{RequestId: "req-1"}
	req2 := &scheduling.InferenceRequest{RequestId: "req-2"}

	endpointsA := []scheduling.Endpoint{makeEndpointWithRole("a-0", "prefill")}
	endpointsB := []scheduling.Endpoint{
		makeEndpointWithRole("b-0", "prefill"),
		makeEndpointWithRole("b-1", "prefill"),
	}

	assert.NoError(t, p.PrepareRequestData(context.Background(), req1, endpointsA))
	assert.NoError(t, p.PrepareRequestData(context.Background(), req2, endpointsB))

	v1, err := plugin.ReadPluginStateKey[*PoolView](p.PluginState(), "req-1", PoolViewKey)
	assert.NoError(t, err)
	assert.Len(t, v1.Pool("prefill"), 1)

	v2, err := plugin.ReadPluginStateKey[*PoolView](p.PluginState(), "req-2", PoolViewKey)
	assert.NoError(t, err)
	assert.Len(t, v2.Pool("prefill"), 2)
}

func TestPluginState_Sharing(t *testing.T) {
	p := NewPoolViewProducer(context.Background(), "test", map[string]PoolSelector{
		"prefill": NewRoleLabelSelector([]string{"prefill"}),
	})
	// Same instance returned every time so consumers can grab a stable reference.
	assert.Same(t, p.PluginState(), p.PluginState())
}
