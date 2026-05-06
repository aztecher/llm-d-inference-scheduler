#!/usr/bin/env bash
# generate-load.sh
#
# Generates burst traffic against the gateway to drive the Prefill pool toward
# saturation. Uses vegeta when available; otherwise falls back to a parallel
# curl loop. The intent is to exercise the SaturationDetector → SLORiskDecider
# → FlexibleDecoder pipeline.
#
# Env:
#   GATEWAY     base URL                       (default: http://localhost:8080)
#   MODEL       model name                     (default: Qwen/Qwen2-0.5B)
#   RATE        requests per second (vegeta)   (default: 20)
#   DURATION    test duration (vegeta)         (default: 30s)
#   CONCURRENCY parallel curls (fallback)      (default: 10)
#   COUNT       total requests (fallback)      (default: 200)
#   PROMPT      prompt text                    (default: long synthetic prompt)
#   MAX_TOKENS  generation length              (default: 64)

set -euo pipefail

GATEWAY="${GATEWAY:-http://localhost:8080}"
MODEL="${MODEL:-Qwen/Qwen2-0.5B}"
RATE="${RATE:-20}"
DURATION="${DURATION:-30s}"
CONCURRENCY="${CONCURRENCY:-10}"
COUNT="${COUNT:-200}"
MAX_TOKENS="${MAX_TOKENS:-64}"

# Long-ish synthetic prompt to ensure prefill compute time is non-trivial.
default_prompt="$(python3 -c "import sys; sys.stdout.write(' '.join(['the quick brown fox jumps over the lazy dog']*50))" 2>/dev/null || \
  printf 'the quick brown fox jumps over the lazy dog %.0s' {1..50})"
PROMPT="${PROMPT:-$default_prompt}"

payload="$(mktemp)"
trap 'rm -f "$payload"' EXIT

# JSON-encode the prompt safely.
python3 - "$MODEL" "$PROMPT" "$MAX_TOKENS" >"$payload" <<'PY'
import json, sys
model, prompt, max_tokens = sys.argv[1], sys.argv[2], int(sys.argv[3])
print(json.dumps({"model": model, "prompt": prompt, "max_tokens": max_tokens, "temperature": 0}))
PY

echo "──────────────────────────────────────────────────────────"
echo "Gateway        : $GATEWAY"
echo "Model          : $MODEL"
echo "Prompt length  : $(wc -c <<<"$PROMPT") chars"
echo "Max tokens     : $MAX_TOKENS"
echo "──────────────────────────────────────────────────────────"

if command -v vegeta >/dev/null 2>&1; then
  echo "vegeta detected: rate=$RATE duration=$DURATION"
  body="$(cat "$payload")"
  # vegeta's @file expects HTTP target syntax
  printf 'POST %s/v1/completions\nContent-Type: application/json\n@%s\n' \
      "$GATEWAY" "$payload" \
    | vegeta attack -rate="$RATE" -duration="$DURATION" -timeout=120s \
    | vegeta report -type=text
  exit 0
fi

echo "vegeta not found; using parallel curl fallback"
echo "concurrency=$CONCURRENCY count=$COUNT"

submit_one() {
  local i="$1"
  curl -sS -o /dev/null -w "%{http_code} %{time_total}s\n" \
       -X POST "${GATEWAY}/v1/completions" \
       -H 'Content-Type: application/json' \
       -H "x-request-id: load-${i}" \
       -d "@$payload" || true
}
export -f submit_one
export GATEWAY payload

seq "$COUNT" | xargs -n 1 -P "$CONCURRENCY" -I{} bash -c 'submit_one "$@"' _ {} \
  | sort | uniq -c | sort -rn | head -10

echo "──────────────────────────────────────────────────────────"
echo "Done. Check Prometheus for:"
echo "  - llm_d_inference_scheduler_pool_saturation"
echo "  - llm_d_inference_scheduler_flex_decoder_activation_total"
echo "  - llm_d_sidecar_prefill_fallback_total (should be 0)"
