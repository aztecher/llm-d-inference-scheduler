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
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
)

func makeEndpoint(name string) scheduling.Endpoint {
	return scheduling.NewEndpoint(
		&fwkdl.EndpointMetadata{
			NamespacedName: k8stypes.NamespacedName{Namespace: "default", Name: name},
			Address:        "10.0.0.1",
			Port:           "8000",
		},
		fwkdl.NewMetrics(),
		fwkdl.NewAttributes(),
	)
}

func TestNewAndPool(t *testing.T) {
	prefill := []scheduling.Endpoint{makeEndpoint("p-0"), makeEndpoint("p-1")}
	flex := []scheduling.Endpoint{makeEndpoint("f-0")}

	v := NewPoolView(map[string][]scheduling.Endpoint{
		"prefill": prefill,
		"flex":    flex,
	})

	assert.Len(t, v.Pool("prefill"), 2)
	assert.Len(t, v.Pool("flex"), 1)
	assert.Nil(t, v.Pool("decode"), "unknown pool returns nil")
}

func TestPools_ReturnsAllNames(t *testing.T) {
	v := NewPoolView(map[string][]scheduling.Endpoint{
		"prefill": {makeEndpoint("p-0")},
		"flex":    {makeEndpoint("f-0")},
		"decode":  nil,
	})
	got := v.Pools()
	sort.Strings(got)
	assert.Equal(t, []string{"decode", "flex", "prefill"}, got)
}

func TestSateRead_NilReceiver(t *testing.T) {
	var v *PoolView
	assert.Nil(t, v.Pool("anything"))
	assert.Nil(t, v.Pools())
}

func TestClone_DeepCopiesSlicesShareEndpoints(t *testing.T) {
	original := []scheduling.Endpoint{makeEndpoint("p-0"), makeEndpoint("p-1")}
	v := NewPoolView(map[string][]scheduling.Endpoint{"prefill": original})

	cloned, ok := v.Clone().(*PoolView)
	assert.True(t, ok)

	// Mutating the cloned slice (e.g., reassigning slot) should not affect the
	// original. The endpoint pointers themselves are shared, which is fine
	// because per-request snapshots are immutable.
	clonedPool := cloned.Pool("prefill")
	clonedPool[0] = makeEndpoint("replacement")
	assert.NotEqual(t,
		v.Pool("prefill")[0].GetMetadata().NamespacedName.Name,
		cloned.Pool("prefill")[0].GetMetadata().NamespacedName.Name,
		"clone's slice mutation must not bleed into original",
	)
}

func TestPoolClone_NilReceiver(t *testing.T) {
	var v *PoolView
	assert.Nil(t, v.Clone())
}
