package disagg

import (
	"context"

	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
)

// FlexDeciderPlugin is an exported decider interface used by the FlexibleDecoderProfileHandler.
// Implementations decide whether the FlexibleDecoder should be activated for a given request.
type FlexDeciderPlugin interface {
	plugin.Plugin
	// Disaggregate returns true when the FlexibleDecoder should be activated (e.g., used as Prefiller).
	Disaggregate(ctx context.Context, request *scheduling.InferenceRequest, endpoint scheduling.Endpoint) bool
}
