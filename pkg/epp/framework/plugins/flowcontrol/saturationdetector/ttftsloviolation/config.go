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

package ttftsloviolation

import (
	"k8s.io/utils/ptr"
)

const (
	defaultSuppressOnTPOTViolation = false
)

// apiConfig is the external JSON shape for the plugin parameters block.
type apiConfig struct {
	// SuppressOnTPOTViolation gates TTFT-violation reporting on TPOT health.
	// When true, an endpoint that violates BOTH TTFT and TPOT SLOs is NOT
	// counted as a violator
	SuppressOnTPOTViolation *bool `json:"suppressOnTPOTViolation,omitempty"`
}

// config is the validated configuration used by the detector.
type config struct {
	suppressOnTPOTViolation bool
}

// buildConfig applies defaults to the supplied apiConfig.
func buildConfig(apiCfg *apiConfig) *config {
	var safe apiConfig
	if apiCfg != nil {
		safe = *apiCfg
	}
	applyDefaults(&safe)
	return &config{
		suppressOnTPOTViolation: *safe.SuppressOnTPOTViolation,
	}
}

// applyDefaults populates unset fields with their default values.
func applyDefaults(cfg *apiConfig) {
	if cfg.SuppressOnTPOTViolation == nil {
		cfg.SuppressOnTPOTViolation = ptr.To(defaultSuppressOnTPOTViolation)
	}
}
