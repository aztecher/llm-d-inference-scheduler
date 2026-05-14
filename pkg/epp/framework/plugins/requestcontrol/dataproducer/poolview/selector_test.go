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
	"testing"

	"github.com/stretchr/testify/assert"
	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/scheduling/filter/bylabel"
)

func makeEndpointWithRole(name, role string) scheduling.Endpoint {
	labels := map[string]string{}
	if role != "" {
		labels[bylabel.RoleLabel] = role
	}
	return scheduling.NewEndpoint(
		&fwkdl.EndpointMetadata{
			NamespacedName: k8stypes.NamespacedName{Namespace: "default", Name: name},
			Address:        "10.0.0.1",
			Port:           "8000",
			Labels:         labels,
		},
		fwkdl.NewMetrics(),
		fwkdl.NewAttributes(),
	)
}

func TestSelects(t *testing.T) {
	s := NewRoleLabelSelector([]string{"prefill", "prefill-decode"})

	tests := []struct {
		name string
		role string
		want bool
	}{
		{"matches prefill", "prefill", true},
		{"matches prefill-decode", "prefill-decode", true},
		{"rejects decode", "decode", false},
		{"rejects flexible-decode", "flexible-decode", false},
		{"rejects empty role", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ep := makeEndpointWithRole("ep-1", tc.role)
			assert.Equal(t, tc.want, s.Selects(ep))
		})
	}
}

func TestNilEndpoint(t *testing.T) {
	s := NewRoleLabelSelector([]string{"prefill"})
	assert.False(t, s.Selects(nil), "nil endpoint must not be selected")
}

func TestNilMetadata(t *testing.T) {
	s := NewRoleLabelSelector([]string{"prefill"})
	stub := stubEndpoint{}
	assert.False(t, s.Selects(stub))
}

func TestEmptyAllowList(t *testing.T) {
	s := NewRoleLabelSelector(nil)
	ep := makeEndpointWithRole("ep-1", "prefill")
	assert.False(t, s.Selects(ep), "no allowed roles → never selects")
}

// stubEndpoint implements just enough of scheduling.Endpoint to test the nil
// branch in RoleLabelSelector. It is not used for any real scheduling logic.
type stubEndpoint struct{}

func (stubEndpoint) GetMetadata() *fwkdl.EndpointMetadata { return nil }
func (stubEndpoint) GetMetrics() *fwkdl.Metrics           { return nil }
func (stubEndpoint) String() string                       { return "stub" }
func (stubEndpoint) Get(string) (fwkdl.Cloneable, bool)   { return nil, false }
func (stubEndpoint) Put(string, fwkdl.Cloneable)          {}
func (stubEndpoint) Keys() []string                       { return nil }
func (stubEndpoint) Clone() fwkdl.AttributeMap            { return nil }
