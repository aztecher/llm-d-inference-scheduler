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

	defaultDecodeProfile         = "decode"
	defaultPrefillProfile        = "prefill"
	defaultEncodeProfile         = "encode"
	defaultFlexibleDecodeProfile = "flexible-decode"
)

// flexDecAction represents the routing action decided for a FlexibleDecode request.
type flexDecAction string

const (
	flexDecActionSelfDecode        flexDecAction = "self_decode"
	flexDecActionPrefillAndForward flexDecAction = "prefill_and_forward"
)

// flexDecActionStateKey is the CycleState key for the FlexibleDecode routing action.
const flexDecActionStateKey = plugin.StateKey("flexible-decode-action")

// flexDecActionState stores the decided FlexibleDecode action in CycleState.
type flexDecActionState struct {
	action flexDecAction
}

// Clone implements plugin.StateData.
func (s flexDecActionState) Clone() plugin.StateData {
	return flexDecActionState{action: s.action}
}

// ── Factory & constructor ────────────────────────────────────────────────────

type disaggProfilesParameters struct {
	Decode         string `json:"decode,omitempty"`
	Prefill        string `json:"prefill,omitempty"`
	Encode         string `json:"encode,omitempty"`
	FlexibleDecode string `json:"flexibleDecode,omitempty"`
}

type disaggDecidersParameters struct {
	Prefill        string `json:"prefill,omitempty"`
	Encode         string `json:"encode,omitempty"`
	FlexibleDecode string `json:"flexibleDecode,omitempty"`
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
//	if parameters.deciders.flexibleDecode is set   - FlexibleDecode function will be supported
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
	if parameters.Profiles.FlexibleDecode == "" {
		parameters.Profiles.FlexibleDecode = defaultFlexibleDecodeProfile
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

	// Resolve flexible decoder decider (optional).
	var flexibleDecodeDecider deciderPlugin
	if parameters.Deciders.FlexibleDecode != "" {
		sp := handle.Plugin(parameters.Deciders.FlexibleDecode)
		if sp == nil {
			return nil, fmt.Errorf("deciders.flexibleDecode plugin not found: %s", parameters.Deciders.FlexibleDecode)
		}
		var ok bool
		flexibleDecodeDecider, ok = sp.(deciderPlugin)
		if !ok {
			return nil, fmt.Errorf("plugin %s does not implement deciderPlugin", parameters.Deciders.FlexibleDecode)
		}
	} else {
		logger.Info("No deciders.flexibleDecode configured, FlexibleDecode fallback disabled")
	}
	// Create handler.
	handler := NewDisaggProfileHandlerWithFlex(
		parameters.Profiles.Decode, parameters.Profiles.Prefill,
		parameters.Profiles.Encode, parameters.Profiles.FlexibleDecode,
		pdDecider, encodeDecider, flexibleDecodeDecider,
	)
	return handler.WithName(name), nil
}

// NewDisaggProfileHandler creates a Handler directly.
// Active stages are determined by non-empty deciders.
func NewDisaggProfileHandler(decodeProfile, prefillProfile, encodeProfile string, pdDecider, encodeDecider deciderPlugin) *Handler {
	return newDisaggProfileHandler(
		DisaggProfileHandlerType,
		decodeProfile, prefillProfile, encodeProfile, defaultFlexibleDecodeProfile,
		pdDecider, encodeDecider, nil,
	)
}

// NewDisaggProfileHandlerWithFlex creates a Handler with FlexibleDecode support.
// The self_decode vs prefill_and_forward mode decision is delegated to the SelfDecodePolicy.
func NewDisaggProfileHandlerWithFlex(
	decodeProfile, prefillProfile, encodeProfile, flexibleDecodeProfile string,
	pdDecider, encodeDecider deciderPlugin,
	flexibleDecodeDecider deciderPlugin,
) *Handler {
	return newDisaggProfileHandler(
		DisaggProfileHandlerType,
		decodeProfile, prefillProfile, encodeProfile, flexibleDecodeProfile,
		pdDecider, encodeDecider, flexibleDecodeDecider,
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
//   - Decode       (D):   schedules the decode pod (always runs first)
//   - FlexibleDecode  (FD):  schedules a flexible-decode pod as a temporary prefiller when the
//     Prefill pool is saturated (TTFT SLO protection)
//
// All handler types (D, P/D, E/PD, E/P/D, FD/D, P/FD/D, …) share this single implementation;
// active stages are selected by the configured deciders and profile names.
type Handler struct {
	typedName             plugin.TypedName
	decodeProfile         string
	prefillProfile        string
	encodeProfile         string
	flexibleDecodeProfile string
	pdDecider             deciderPlugin
	encodeDecider         deciderPlugin
	flexibleDecodeDecider deciderPlugin
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
	handlerType, decodeProfile, prefillProfile, encodeProfile, flexibleDecodeProfile string,
	pdDecider, encodeDecider deciderPlugin,
	flexibleDecodeDecider deciderPlugin,
) *Handler {
	return &Handler{
		typedName:             plugin.TypedName{Type: handlerType},
		decodeProfile:         decodeProfile,
		prefillProfile:        prefillProfile,
		encodeProfile:         encodeProfile,
		flexibleDecodeProfile: flexibleDecodeProfile,
		pdDecider:             pdDecider,
		encodeDecider:         encodeDecider,
		flexibleDecodeDecider: flexibleDecodeDecider,
	}
}

// Pick implements scheduling.ProfileHandler.
// Stages run in order: decode → encode (optional) → prefill/flexible-decode (optional).
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

	// ── Stage 3a: FlexibleDecode result handling ──────────────────────────────
	// When flexibleDecodeDecider is configured and detects Prefill pool saturation,d
	// the flexible-decode profile is scheduled instead of the regular prefill profile.
	// After the flexible-decode runs, the action (self_decode or prefill_and_forward)
	// is recorded in CycleState for ProcessResults to consume.
	if h.flexibleDecodeDecider != nil {
		if flexRes, flexExecuted := profileResults[h.flexibleDecodeProfile]; flexExecuted && flexRes != nil {
			if len(flexRes.TargetEndpoints) > 0 {
				flexEndpoint := flexRes.TargetEndpoints[0]
				// The mode decision (self_decode vs prefill_and_forward) is
				// decided by the flex decider's SelfDecodePolicy.
				// When the decider does not expose one, we conservatively fallback to self_decode
				allowSelfDecode := true
				if provider, ok := h.flexibleDecodeDecider.(selfDecodePolicyProvider); ok {
					if policy := provider.SelfDecodePolicy(); policy != nil {
						allowSelfDecode = policy.Allow(ctx, request, flexEndpoint)
					}
				}
				if allowSelfDecode {
					cycleState.Write(flexDecActionStateKey, flexDecActionState{action: flexDecActionSelfDecode})
					span.SetAttributes(
						attribute.String("llm_d.profile_handler.decision", "complete_self_decode"),
						attribute.String("llm_d.flexdec.flex_endpoint", flexEndpoint.GetMetadata().Address),
					)
					metrics.RecordFlexibleDecodeActivation(request.TargetModel, metrics.FlexibleDecodeActionSelfDecode)
				} else {
					cycleState.Write(flexDecActionStateKey, flexDecActionState{action: flexDecActionPrefillAndForward})
					span.SetAttributes(
						attribute.String("llm_d.profile_handler.decision", "complete_prefill_and_forward"),
						attribute.String("llm_d.flexdec.flex_endpoint", flexEndpoint.GetMetadata().Address),
					)
					metrics.RecordFlexibleDecodeActivation(request.TargetModel, metrics.FlexibleDecodeActionPrefillAndForward)
				}
			} else {
				// Flex-decode returned no endpoints: fall back to normal decode.
				span.SetAttributes(attribute.String("llm_d.profile_handler.decision", "complete_no_flex_endpoints"))
				metrics.RecordFlexibleDecodeActivation(request.TargetModel, metrics.FlexibleDecodeActionNoEndpoints)
			}
			encodeUsed := profileResults[h.encodeProfile] != nil
			metrics.RecordDisaggDecision(request.TargetModel, metrics.DisaggDecisionType(encodeUsed, true))
			return map[string]scheduling.SchedulerProfile{}
		}
	}

	// ── Stage 3b: Prefill / FlexibleDecode scheduling ─────────────────────────
	// Neither prefill nor flexible-decode has been decided yet: evaluate which path to take.
	_, prefillDecided := profileResults[h.prefillProfile]
	_, flexDecided := profileResults[h.flexibleDecodeProfile]

	if !prefillDecided && !flexDecided {
		activateFlex := h.flexibleDecodeDecider != nil && h.flexibleDecodeDecider.disaggregate(ctx, request, decodeRes.TargetEndpoints[0])
		needsPrefill := h.pdDecider != nil && h.pdDecider.disaggregate(ctx, request, decodeRes.TargetEndpoints[0])

		switch {
		case activateFlex:
			// Prefill pool saturated: activate FlexibleDecode as temporary prefiller.
			// Mark regular prefill as skipped so it won't be re-evaluated.
			profileResults[h.prefillProfile] = nil
			if flexProfile, ok := profiles[h.flexibleDecodeProfile]; ok {
				span.SetAttributes(attribute.String("llm_d.profile_handler.decision", "run_flexible_decode"))
				return map[string]scheduling.SchedulerProfile{h.flexibleDecodeProfile: flexProfile}
			}
			// Flex-decode profile not present in schedulingProfiles: skip it
			profileResults[h.flexibleDecodeProfile] = nil
			span.SetAttributes(attribute.String("llm_d.profile_handler.decision", "skip_flexible_decode_no_profile"))
		case needsPrefill:
			// Normal P/D: schedule regular prefill.
			profileResults[h.flexibleDecodeProfile] = nil
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
			profileResults[h.flexibleDecodeProfile] = nil
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

	// Check whether a FlexibleDecode action was recorded during Pick.
	if cycleState != nil {
		if actionState, err := scheduling.ReadCycleStateKey[flexDecActionState](cycleState, flexDecActionStateKey); err == nil {
			flexRes := profileResults[h.flexibleDecodeProfile]

			switch actionState.action {
			case flexDecActionSelfDecode:
				// FlexibleDecode handles both Prefill and Decode; it is the primary endpoint.
				// Fall back to regular decode if no flex endpoint is available.
				if flexRes != nil && len(flexRes.TargetEndpoints) > 0 {
					return &scheduling.SchedulingResult{
						PrimaryProfileName: h.flexibleDecodeProfile,
						ProfileResults:     map[string]*scheduling.ProfileRunResult{h.flexibleDecodeProfile: flexRes},
					}, nil
				}
				// No flex endpoint: fall back to regular decode (handled below).

			case flexDecActionPrefillAndForward:
				// TODO: If the decode SchedulingProfile's filter includes flexible-decode pods,
				// the decode primary and the flexible-decode prefiller may resolve to the same pod.
				// The engine then receives a self-referential x-prefill-host header — semantically
				// equivalent to self_decode but with an unnecessary KV-transfer signal.
				//
				// Planned resolution: detect the self-referential case here (compare decode primary
				// vs flex prefiller addresses) and promote the result to self_decode so the
				// x-prefill-host header is not set. This keeps flexible-decode pods available as
				// regular decode candidates during non-saturated periods, and only auto-demotes
				// when the two stages happen to pick the same pod.
				if decodeRunResults == nil || len(decodeRunResults.TargetEndpoints) == 0 {
					return nil, errors.New("failed to find available decode workers")
				}
				results := map[string]*scheduling.ProfileRunResult{h.decodeProfile: decodeRunResults}
				if flexRes != nil && len(flexRes.TargetEndpoints) > 0 {
					results[h.flexibleDecodeProfile] = flexRes
				}
				return &scheduling.SchedulingResult{
					PrimaryProfileName: h.decodeProfile,
					ProfileResults:     results,
				}, nil
			}
		}
	}

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
