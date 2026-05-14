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
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/scheduling/filter/bylabel"
)

// PoolSelector decides whether a scheduling endpoint belongs to a named pool.
type PoolSelector interface {
	Selects(scheduling.Endpoint) bool
}

// RoleLabelSelector matches endpoints by their `llm-d.ai/role` label against an allow-list.
type RoleLabelSelector struct {
	allowedRoles map[string]struct{}
}

// NewRoleLabelSelector returns a selector that matches endpoints whose role label is in roles.
func NewRoleLabelSelector(roles []string) *RoleLabelSelector {
	allowed := make(map[string]struct{}, len(roles))
	for _, r := range roles {
		allowed[r] = struct{}{}
	}
	return &RoleLabelSelector{allowedRoles: allowed}
}

// Selects implements PoolSelector.
func (s *RoleLabelSelector) Selects(ep scheduling.Endpoint) bool {
	if ep == nil {
		return false
	}
	md := ep.GetMetadata()
	if md == nil {
		return false
	}
	role := md.Labels[bylabel.RoleLabel]
	_, ok := s.allowedRoles[role]
	return ok
}
