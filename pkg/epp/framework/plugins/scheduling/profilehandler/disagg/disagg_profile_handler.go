// Package disagg provides profile handler plugins for the epp.
package disagg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/interface/scheduling"
	dl_prefix "github.com/llm-d/llm-d-inference-scheduler/pkg/epp/framework/plugins/datalayer/attribute/prefix"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/metrics"
	"github.com/llm-d/llm-d-inference-scheduler/pkg/telemetry"
)

// ── Constants ───────────────────────────────────────────────────────────────

const (
	// DisaggProfileHandlerType is the canonical type for the unified disaggregation profile handler.
	DisaggProfileHandlerType = "disagg-profile-handler"

	defaultDecodeProfile      = "decode"
	defaultPrefillProfile     = "prefill"
	defaultEncodeProfile      = "encode"
	defaultFlexDecoderProfile = "flex-decoder"
)

// flexDecAction represents the routing action decided for a FlexibleDecoder request.
type flexDecAction string

const (
	flexDecActionSelfDecode        flexDecAction = "self_decode"
	flexDecActionPrefillAndForward flexDecAction = "prefill_and_forward"
)

// flexDecActionStateKey is the CycleState key for the FlexibleDecoder routing action.
const flexDecActionStateKey = plugin.StateKey("flexible-decoder-action")

// flexDecActionState stores the decided FlexibleDecoder action in CycleState.
type flexDecActionState struct {
	action flexDecAction
}

// Clone implements plugin.StateData.
func (s flexDecActionState) Clone() plugin.StateData {
	return flexDecActionState{action: s.action}
}

// ── Factory & constructor ────────────────────────────────────────────────────

type disaggProfilesParameters struct {
	Decode      string `json:"decode,omitempty"`
	Prefill     string `json:"prefill,omitempty"`
	Encode      string `json:"encode,omitempty"`
	FlexDecoder string `json:"flexDecoder,omitempty"`
}

type disaggDecidersParameters struct {
	Prefill    string `json:"prefill,omitempty"`
	Encode     string `json:"encode,omitempty"`
	SLORisk    string `json:"sloRisk,omitempty"`
	SelfDecode string `json:"selfDecode,omitempty"`
}

// disaggProfileHandlerParameters is the current parameter format using nested maps.
type disaggProfileHandlerParameters struct {
	Profiles disaggProfilesParameters `json:"profiles"`
	Deciders disaggDecidersParameters `json:"deciders"`
}

// legacyDisaggProfileHandlerParameters is the deprecated flat parameter format.
// Unknown fields (e.g. pd-profile-handler's prefixPluginType, primaryPort) are
// silently ignored by json.Unmarshal, so they need not be declared here.
type legacyDisaggProfileHandlerParameters struct {
	DecodeProfile            string `json:"decodeProfile"`
	PrefillProfile           string `json:"prefillProfile"`
	EncodeProfile            string `json:"encodeProfile"`
	PrefillDeciderPluginName string `json:"prefillDeciderPluginName"`
	EncodeDeciderPluginName  string `json:"encodeDeciderPluginName"`
	// DeciderPluginName is a legacy alias from pd-profile-handler, maps to deciders.prefill.
	DeciderPluginName string `json:"deciderPluginName"`
}

// toDisaggParams copies legacy flat fields into the nested format, logging a
// deprecation warning for each field in use.
func (l *legacyDisaggProfileHandlerParameters) toDisaggParams(logger logr.Logger) disaggProfileHandlerParameters {
	p := disaggProfileHandlerParameters{}
	if l.DecodeProfile != "" {
		logger.Info("Deprecated parameter 'decodeProfile', use 'profiles.decode' instead")
		p.Profiles.Decode = l.DecodeProfile
	}
	if l.PrefillProfile != "" {
		logger.Info("Deprecated parameter 'prefillProfile', use 'profiles.prefill' instead")
		p.Profiles.Prefill = l.PrefillProfile
	}
	if l.EncodeProfile != "" {
		logger.Info("Deprecated parameter 'encodeProfile', use 'profiles.encode' instead")
		p.Profiles.Encode = l.EncodeProfile
	}
	if l.PrefillDeciderPluginName != "" {
		logger.Info("Deprecated parameter 'prefillDeciderPluginName', use 'deciders.prefill' instead")
		p.Deciders.Prefill = l.PrefillDeciderPluginName
	}
	// DeciderPluginName is a lower-priority alias for prefill decider (from pd-profile-handler).
	if l.DeciderPluginName != "" && p.Deciders.Prefill == "" {
		logger.Info("Deprecated parameter 'deciderPluginName', use 'deciders.prefill' instead")
		p.Deciders.Prefill = l.DeciderPluginName
	}
	if l.EncodeDeciderPluginName != "" {
		logger.Info("Deprecated parameter 'encodeDeciderPluginName', use 'deciders.encode' instead")
		p.Deciders.Encode = l.EncodeDeciderPluginName
	}
	return p
}

// HandlerFactory is the unified factory for all disaggregation profile handlers.
//
//	if parameters.deciders.prefill is set   - P disaggregation will be supported
//	if parameters.deciders.encode is set    - E disaggregation will be supported
//	if parameters.deciders.sloRisk is set   - FlexibleDecoder fallback will be supported
func HandlerFactory(name string, rawParameters json.RawMessage, handle plugin.Handle) (plugin.Plugin, error) {
	logger := log.FromContext(handle.Context())

	parameters := disaggProfileHandlerParameters{}
	if rawParameters != nil {
		legacy := legacyDisaggProfileHandlerParameters{}

		if err := json.Unmarshal(rawParameters, &parameters); err != nil {
			return nil, fmt.Errorf("failed to parse parameters of the disagg-profile-handler - %w", err)
		}
		if err := json.Unmarshal(rawParameters, &legacy); err != nil {
			return nil, fmt.Errorf("failed to parse parameters of the disagg-profile-handler - %w", err)
		}

		if parameters.Profiles != (disaggProfilesParameters{}) ||
			parameters.Deciders != (disaggDecidersParameters{}) {
			// Make sure the legacy parameters were not used
			if legacy != (legacyDisaggProfileHandlerParameters{}) {
				return nil, errors.New("cannot mix deprecated flat parameters (decodeProfile, prefillProfile, encodeProfile, " +
					"deciderPluginName, prefillDeciderPluginName, encodeDeciderPluginName) " +
					"with nested parameters (profiles, deciders): use one format or the other")
			}
		} else {
			logger.Info("Deprecated: using flat parameter format, migrate to nested profiles/deciders format")
			parameters = legacy.toDisaggParams(logger)
		}
	}

	// Apply profile name defaults for any fields still unset.
	if parameters.Profiles.Decode == "" {
		parameters.Profiles.Decode = defaultDecodeProfile
	}
	if parameters.Profiles.Prefill == "" {
		parameters.Profiles.Prefill = defaultPrefillProfile
	}
	if parameters.Profiles.Encode == "" {
		parameters.Profiles.Encode = defaultEncodeProfile
	}
	if parameters.Profiles.FlexDecoder == "" {
		parameters.Profiles.FlexDecoder = defaultFlexDecoderProfile
	}

	// Resolve PD decider (optional).
	var pdDecider deciderPlugin
	if parameters.Deciders.Prefill != "" {
		p := handle.Plugin(parameters.Deciders.Prefill)
		if p == nil {
			return nil, fmt.Errorf("deciders.prefill plugin not found: %s", parameters.Deciders.Prefill)
		}
		var ok bool
		pdDecider, ok = p.(deciderPlugin)
		if !ok {
			return nil, fmt.Errorf("plugin %s does not implement prefillDeciderPlugin", parameters.Deciders.Prefill)
		}
	} else {
		logger.Info("No deciders.prefill configured, P/D disaggregation disabled")
	}
	// Resolve encode decider (optional).
	var encodeDecider deciderPlugin
	if parameters.Deciders.Encode != "" {
		ep := handle.Plugin(parameters.Deciders.Encode)
		if ep == nil {
			return nil, fmt.Errorf("deciders.encode plugin not found: %s", parameters.Deciders.Encode)
		}
		var ok bool
		encodeDecider, ok = ep.(deciderPlugin)
		if !ok {
			return nil, fmt.Errorf("plugin %s does not implement encodeDeciderPlugin", parameters.Deciders.Encode)
		}
	} else {
		logger.Info("No deciders.encode configured, E disaggregation disabled")
	}
	// Resolve SLO risk decider (optional; enables FlexibleDecoder fallback).
	var sloRiskDecider FlexDeciderPlugin
	if parameters.Deciders.SLORisk != "" {
		sp := handle.Plugin(parameters.Deciders.SLORisk)
		if sp == nil {
			return nil, fmt.Errorf("deciders.sloRisk plugin not found: %s", parameters.Deciders.SLORisk)
		}
		var ok bool
		sloRiskDecider, ok = sp.(FlexDeciderPlugin)
		if !ok {
			return nil, fmt.Errorf("plugin %s does not implement FlexDeciderPlugin", parameters.Deciders.SLORisk)
		}
	} else {
		logger.Info("No deciders.sloRisk configured, FlexibleDecoder fallback disabled")
	}
	// Resolve self-decode decider (optional; when absent, FlexibleDecoder always uses prefill-and-forward).
	var selfDecodeDecider FlexDeciderPlugin
	if parameters.Deciders.SelfDecode != "" {
		sdp := handle.Plugin(parameters.Deciders.SelfDecode)
		if sdp == nil {
			return nil, fmt.Errorf("deciders.selfDecode plugin not found: %s", parameters.Deciders.SelfDecode)
		}
		var ok bool
		selfDecodeDecider, ok = sdp.(FlexDeciderPlugin)
		if !ok {
			return nil, fmt.Errorf("plugin %s does not implement FlexDeciderPlugin", parameters.Deciders.SelfDecode)
		}
	} else if sloRiskDecider != nil {
		logger.Info("No deciders.selfDecode configured; FlexibleDecoder will always use prefill-and-forward mode")
	}
	// Create handler.
	handler := NewDisaggProfileHandlerWithFlex(
		parameters.Profiles.Decode, parameters.Profiles.Prefill,
		parameters.Profiles.Encode, parameters.Profiles.FlexDecoder,
		pdDecider, encodeDecider, sloRiskDecider, selfDecodeDecider,
	)
	return handler.WithName(name), nil
}

// NewDisaggProfileHandler creates a Handler directly.
// Active stages are determined by non-empty deciders and profile names.
func NewDisaggProfileHandler(decodeProfile, prefillProfile, encodeProfile string, pdDecider, encodeDecider deciderPlugin) *Handler {
	return newDisaggProfileHandler(
		DisaggProfileHandlerType,
		decodeProfile, prefillProfile, encodeProfile, defaultFlexDecoderProfile,
		pdDecider, encodeDecider, nil, nil,
	)
}

// NewDisaggProfileHandlerWithFlex creates a Handler with FlexibleDecoder support.
func NewDisaggProfileHandlerWithFlex(
	decodeProfile, prefillProfile, encodeProfile, flexDecoderProfile string,
	pdDecider, encodeDecider deciderPlugin,
	sloRiskDecider, selfDecodeDecider FlexDeciderPlugin,
) *Handler {
	return newDisaggProfileHandler(
		DisaggProfileHandlerType,
		decodeProfile, prefillProfile, encodeProfile, flexDecoderProfile,
		pdDecider, encodeDecider, sloRiskDecider, selfDecodeDecider,
	)
}

// ── Shared implementation ───────────────────────────────────────────────────

// compile-time assertion
var _ scheduling.ProfileHandler = &Handler{}

// Handler is the unified disaggregation profile handler.
// It drives one or more of the following stages, each optional except decode:
//
//   - Encode       (E):   schedules encoder pods for multimodal content
//   - Prefill      (P):   schedules a prefill pod for KV-cache disaggregation
//   - FlexDecoder  (FD):  schedules a flex-decoder pod as a temporary prefiller when the
//     Prefill pool is saturated (TTFT SLO protection)
//   - Decode       (D):   schedules the decode pod (always runs first)
//
// All handler types (D, P/D, E/PD, E/P/D, FD/D, P/FD/D, …) share this single implementation;
// active stages are selected by the configured deciders and profile names.
type Handler struct {
	typedName          plugin.TypedName
	decodeProfile      string
	prefillProfile     string
	encodeProfile      string
	flexDecoderProfile string
	pdDecider          deciderPlugin
	encodeDecider      deciderPlugin
	sloRiskDecider     FlexDeciderPlugin
	selfDecodeDecider  FlexDeciderPlugin
}

// TypedName returns the typed name of the plugin.
func (h *Handler) TypedName() plugin.TypedName { return h.typedName }

// WithName sets the instance name of the plugin.
func (h *Handler) WithName(name string) *Handler {
	h.typedName.Name = name
	return h
}

// Consumes defines data types consumed by this plugin (through the PD decider).
func (*Handler) Consumes() map[string]any {
	return map[string]any{dl_prefix.PrefixCacheMatchInfoKey: dl_prefix.PrefixCacheMatchInfo{}}
}

func newDisaggProfileHandler(
	handlerType, decodeProfile, prefillProfile, encodeProfile, flexDecoderProfile string,
	pdDecider, encodeDecider deciderPlugin,
	sloRiskDecider, selfDecodeDecider FlexDeciderPlugin,
) *Handler {
	return &Handler{
		typedName:          plugin.TypedName{Type: handlerType},
		decodeProfile:      decodeProfile,
		prefillProfile:     prefillProfile,
		encodeProfile:      encodeProfile,
		flexDecoderProfile: flexDecoderProfile,
		pdDecider:          pdDecider,
		encodeDecider:      encodeDecider,
		sloRiskDecider:     sloRiskDecider,
		selfDecodeDecider:  selfDecodeDecider,
	}
}

// Pick implements scheduling.ProfileHandler.
// Stages run in order: decode → encode (optional) → prefill/flex-decoder (optional).
//
// FlexibleDecoder integration: when sloRiskDecider is configured and detects Prefill pool
// saturation, the flex-decoder profile is scheduled instead of (or in place of) the
// regular prefill profile. After the flex-decoder runs, the action (self_decode or
// prefill_and_forward) is recorded in CycleState for ProcessResults to consume.
//
// Returns the next profile to execute, or an empty map when all stages are done.
func (h *Handler) Pick(ctx context.Context, cycleState *scheduling.CycleState, request *scheduling.InferenceRequest, profiles map[string]scheduling.SchedulerProfile,
	profileResults map[string]*scheduling.ProfileRunResult,
) map[string]scheduling.SchedulerProfile {
	tracer := telemetry.Tracer()
	ctx, span := tracer.Start(ctx, "llm_d.epp.disagg.profile_handler.pick",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	if request == nil {
		span.SetAttributes(attribute.String("llm_d.profile_handler.decision", "complete_nil_request"))
		return map[string]scheduling.SchedulerProfile{}
	}

	if request.TargetModel != "" {
		span.SetAttributes(attribute.String("gen_ai.request.model", request.TargetModel))
	}
	span.SetAttributes(attribute.String("gen_ai.request.id", request.RequestId))

	// ── Stage 1: Decode ────────────────────────────────────────────────────
	if _, executed := profileResults[h.decodeProfile]; !executed {
		decodeProfile, ok := profiles[h.decodeProfile]
		if !ok {
			span.SetAttributes(attribute.String("llm_d.profile_handler.decision", "error_missing_decode_profile"))
			return map[string]scheduling.SchedulerProfile{}
		}
		span.SetAttributes(attribute.String("llm_d.profile_handler.decision", "run_decode"))
		return map[string]scheduling.SchedulerProfile{h.decodeProfile: decodeProfile}
	}

	decodeRes := profileResults[h.decodeProfile]
	if decodeRes == nil || len(decodeRes.TargetEndpoints) == 0 {
		span.SetAttributes(
			attribute.String("llm_d.profile_handler.decision", "complete"),
			attribute.Bool("llm_d.profile_handler.decode_failed", true),
		)
		return map[string]scheduling.SchedulerProfile{}
	}

	// ── Stage 2: Encode (optional) ─────────────────────────────────────────
	if _, hasEncodeProfile := profiles[h.encodeProfile]; hasEncodeProfile {
		if _, executed := profileResults[h.encodeProfile]; !executed {
			if h.encodeDecider != nil && h.encodeDecider.disaggregate(ctx, request, decodeRes.TargetEndpoints[0]) {
				span.SetAttributes(attribute.String("llm_d.profile_handler.decision", "run_encode"))
				return map[string]scheduling.SchedulerProfile{h.encodeProfile: profiles[h.encodeProfile]}
			}
			// Decider rejected encode - mark as evaluated so we don't re-run the decider.
			profileResults[h.encodeProfile] = nil
			span.SetAttributes(attribute.String("llm_d.profile_handler.decision", "skip_encode"))
		}
	}

	// ── Stage 3a: FlexDecoder result handling ──────────────────────────────
	// If the flex-decoder profile has run and produced endpoints, determine the routing
	// action (self_decode vs prefill_and_forward) and store it in CycleState.
	if h.sloRiskDecider != nil {
		if flexRes, flexExecuted := profileResults[h.flexDecoderProfile]; flexExecuted && flexRes != nil {
			if len(flexRes.TargetEndpoints) > 0 {
				flexEndpoint := flexRes.TargetEndpoints[0]
				if h.selfDecodeDecider != nil && h.selfDecodeDecider.Disaggregate(ctx, request, flexEndpoint) {
					cycleState.Write(flexDecActionStateKey, flexDecActionState{action: flexDecActionSelfDecode})
					span.SetAttributes(
						attribute.String("llm_d.profile_handler.decision", "complete_self_decode"),
						attribute.String("llm_d.flexdec.flex_endpoint", flexEndpoint.GetMetadata().Address),
					)
				} else {
					cycleState.Write(flexDecActionStateKey, flexDecActionState{action: flexDecActionPrefillAndForward})
					span.SetAttributes(
						attribute.String("llm_d.profile_handler.decision", "complete_prefill_and_forward"),
						attribute.String("llm_d.flexdec.flex_endpoint", flexEndpoint.GetMetadata().Address),
					)
				}
			} else {
				// Flex-decoder returned no endpoints: fall back to normal decode.
				span.SetAttributes(attribute.String("llm_d.profile_handler.decision", "complete_no_flex_endpoints"))
			}
			encodeUsed := profileResults[h.encodeProfile] != nil
			metrics.RecordDisaggDecision(request.TargetModel, metrics.DisaggDecisionType(encodeUsed, true))
			return map[string]scheduling.SchedulerProfile{}
		}
	}

	// ── Stage 3b: Prefill / FlexDecoder scheduling ─────────────────────────
	// Neither prefill nor flex-decoder has been decided yet: evaluate which path to take.
	_, prefillDecided := profileResults[h.prefillProfile]
	_, flexDecided := profileResults[h.flexDecoderProfile]

	if !prefillDecided && !flexDecided {
		// Evaluate SLO risk first (takes priority over normal P/D).
		sloRisk := h.sloRiskDecider != nil && h.sloRiskDecider.Disaggregate(ctx, request, decodeRes.TargetEndpoints[0])
		needsPrefill := h.pdDecider != nil && h.pdDecider.disaggregate(ctx, request, decodeRes.TargetEndpoints[0])

		switch {
		case sloRisk:
			// Prefill pool saturated: activate FlexibleDecoder as temporary prefiller.
			// Mark regular prefill as skipped so it won't be re-evaluated.
			profileResults[h.prefillProfile] = nil
			if flexProfile, ok := profiles[h.flexDecoderProfile]; ok {
				span.SetAttributes(attribute.String("llm_d.profile_handler.decision", "run_flex_decoder"))
				return map[string]scheduling.SchedulerProfile{h.flexDecoderProfile: flexProfile}
			}
			// Flex-decoder profile not present in schedulingProfiles: skip it.
			profileResults[h.flexDecoderProfile] = nil
			span.SetAttributes(attribute.String("llm_d.profile_handler.decision", "skip_flex_decoder_no_profile"))
		case needsPrefill:
			// Normal P/D: schedule regular prefill. Skip flex-decoder.
			profileResults[h.flexDecoderProfile] = nil
			if prefillProfile, ok := profiles[h.prefillProfile]; ok {
				span.SetAttributes(attribute.String("llm_d.profile_handler.decision", "run_prefill"))
				return map[string]scheduling.SchedulerProfile{h.prefillProfile: prefillProfile}
			}
			// Prefill profile not present in schedulingProfiles: skip it.
			profileResults[h.prefillProfile] = nil
			span.SetAttributes(attribute.String("llm_d.profile_handler.decision", "skip_prefill_no_profile"))
		default:
			// No disaggregation needed: skip both.
			profileResults[h.prefillProfile] = nil
			profileResults[h.flexDecoderProfile] = nil
			span.SetAttributes(attribute.String("llm_d.profile_handler.decision", "skip_prefill"))
		}
	}

	// ── All stages done: record routing decision ───────────────────────────
	encodeUsed := profileResults[h.encodeProfile] != nil
	prefillUsed := profileResults[h.prefillProfile] != nil

	decision := metrics.DisaggDecisionType(encodeUsed, prefillUsed)
	metrics.RecordDisaggDecision(request.TargetModel, decision)
	span.SetAttributes(attribute.String("llm_d.profile_handler.decision", "complete_"+decision))

	return map[string]scheduling.SchedulerProfile{}
}

// ProcessResults implements scheduling.ProfileHandler.
// Builds the final SchedulingResult from whichever stages ran successfully.
//
// When a FlexDecider action was recorded in CycleState by Pick():
//   - self_decode:         primary = flex-decoder pod; request goes to flex-decoder for Prefill+Decode.
//   - prefill_and_forward: primary = decode pod; flex-decoder is in ProfileResults for header injection.
//
// In all other cases (normal P/D or decode-only), existing behaviour is unchanged.
func (h *Handler) ProcessResults(
	_ context.Context,
	cycleState *scheduling.CycleState,
	request *scheduling.InferenceRequest,
	profileResults map[string]*scheduling.ProfileRunResult,
) (*scheduling.SchedulingResult, error) {
	if request == nil {
		return nil, errors.New("request is nil")
	}

	decodeRunResults := profileResults[h.decodeProfile]

	// Check whether a FlexibleDecoder action was recorded during Pick().
	if cycleState != nil {
		if actionState, err := scheduling.ReadCycleStateKey[flexDecActionState](cycleState, flexDecActionStateKey); err == nil {
			flexRes := profileResults[h.flexDecoderProfile]

			switch actionState.action {
			case flexDecActionSelfDecode:
				// FlexibleDecoder handles both Prefill and Decode; it is the primary endpoint.
				// Fall back to regular decode if no flex endpoint is available.
				if flexRes != nil && len(flexRes.TargetEndpoints) > 0 {
					return &scheduling.SchedulingResult{
						PrimaryProfileName: h.flexDecoderProfile,
						ProfileResults:     map[string]*scheduling.ProfileRunResult{h.flexDecoderProfile: flexRes},
					}, nil
				}
				// No flex endpoint: fall back to regular decode (handled below).

			case flexDecActionPrefillAndForward:
				// Regular decode pod is primary; FlexibleDecoder acts as Prefiller.
				// FlexibleDecoderHeadersHandler reads flexDecoderProfile from ProfileResults
				// to set the x-prefill-host header.
				//
				// TODO: If the decode SchedulingProfile's filter includes flexible-decoder pods
				// (decode-filter does so by default via NewDecodeRole), the decode primary and
				// the flex-decoder prefiller may resolve to the same pod. The engine then receives
				// a self-referential x-prefill-host header — semantically equivalent to self_decode
				// but with an unnecessary KV-transfer signal. Whether the engine handles this
				// gracefully is engine-specific (vLLM). Resolution options:
				//   (a) Exclude flexible-decoder pods from the decode SchedulingProfile via YAML
				//       (use a custom by-label filter without the "flexible-decoder" value).
				//   (b) Detect the self-referential case here and promote the result to self_decode
				//       to avoid setting the x-prefill-host header entirely.
				if decodeRunResults == nil || len(decodeRunResults.TargetEndpoints) == 0 {
					return nil, errors.New("failed to find available decode workers")
				}
				results := map[string]*scheduling.ProfileRunResult{h.decodeProfile: decodeRunResults}
				if flexRes != nil && len(flexRes.TargetEndpoints) > 0 {
					results[h.flexDecoderProfile] = flexRes
				}
				return &scheduling.SchedulingResult{
					PrimaryProfileName: h.decodeProfile,
					ProfileResults:     results,
				}, nil
			}
		}
	}

	// Normal path (no FlexDecoder action, or self_decode fallback to regular decode).
	if decodeRunResults == nil || len(decodeRunResults.TargetEndpoints) == 0 {
		return nil, errors.New("failed to find available decode workers")
	}

	updatedResults := map[string]*scheduling.ProfileRunResult{}
	updatedResults[h.decodeProfile] = decodeRunResults

	if prefillRes, ok := profileResults[h.prefillProfile]; ok && prefillRes != nil {
		updatedResults[h.prefillProfile] = prefillRes
	}

	if encodeRes, ok := profileResults[h.encodeProfile]; ok && encodeRes != nil {
		updatedResults[h.encodeProfile] = encodeRes
	}

	return &scheduling.SchedulingResult{
		PrimaryProfileName: h.decodeProfile,
		ProfileResults:     updatedResults,
	}, nil
}
