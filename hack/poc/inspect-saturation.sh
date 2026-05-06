#!/usr/bin/env bash
# inspect-saturation.sh
#
# Polls the EPP /metrics endpoint and prints the live Saturation gauge,
# threshold, FlexD activation count and Sidecar fallback count once per second.
# Designed for quick "is the pipeline producing the values we expect?" checks
# during PoC validation.
#
# Env:
#   EPP_NAMESPACE   namespace EPP runs in     (default: default)
#   EPP_LABEL       label selector for EPP pod (default: app=epp)
#   POLL_INTERVAL   seconds between samples   (default: 1)
#   POOL            saturation gauge label    (default: prefill)
#
# Stops when interrupted (Ctrl-C).

set -euo pipefail

EPP_NAMESPACE="${EPP_NAMESPACE:-default}"
EPP_LABEL="${EPP_LABEL:-app=epp}"
POLL_INTERVAL="${POLL_INTERVAL:-1}"
POOL="${POOL:-prefill}"

pod="$(kubectl get pod -n "$EPP_NAMESPACE" -l "$EPP_LABEL" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
if [[ -z "$pod" ]]; then
  echo "no EPP pod found in namespace=$EPP_NAMESPACE selector=$EPP_LABEL" >&2
  exit 1
fi

echo "watching EPP pod: $pod   pool=$POOL   interval=${POLL_INTERVAL}s"
echo "(Ctrl-C to stop)"

# port-forward to the EPP metrics endpoint
local_port=19090
kubectl -n "$EPP_NAMESPACE" port-forward "$pod" "${local_port}:9090" >/dev/null 2>&1 &
pf_pid=$!
trap 'kill ${pf_pid} 2>/dev/null || true' EXIT

# wait for port-forward to come up
for _ in 1 2 3 4 5 6 7 8 9 10; do
  if curl -s --max-time 1 "http://127.0.0.1:${local_port}/metrics" >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done

extract() {
  # extract "metric{...}" value from a metrics page
  local metric_pattern="$1"
  awk -v pat="$metric_pattern" '$0 ~ pat && $0 !~ /^#/ { print $NF; exit }'
}

while true; do
  body="$(curl -s --max-time 2 "http://127.0.0.1:${local_port}/metrics" || true)"

  saturation="$(printf '%s' "$body" \
      | extract "^llm_d_inference_scheduler_pool_saturation\\{[^}]*pool=\"${POOL}\"")"
  threshold="$(printf '%s' "$body" \
      | extract "^llm_d_inference_scheduler_pool_saturation_threshold\\{[^}]*pool=\"${POOL}\"")"
  flex_activations="$(printf '%s' "$body" \
      | awk '/^llm_d_inference_scheduler_flex_decoder_activation_total\{/ {sum+=$NF} END {print sum+0}')"
  fallbacks="$(printf '%s' "$body" \
      | awk '/^llm_d_sidecar_prefill_fallback_total\{/ {sum+=$NF} END {print sum+0}')"

  : "${saturation:=NA}"
  : "${threshold:=NA}"
  above="false"
  if [[ "$saturation" != "NA" && "$threshold" != "NA" ]]; then
    above="$(awk -v s="$saturation" -v t="$threshold" 'BEGIN { print (s>=t)?"true":"false" }')"
  fi

  printf 'ts=%s saturation=%s threshold=%s above=%s  flex_activations=%s  fallbacks=%s\n' \
      "$(date +%H:%M:%S)" "$saturation" "$threshold" "$above" "$flex_activations" "$fallbacks"

  sleep "$POLL_INTERVAL"
done
