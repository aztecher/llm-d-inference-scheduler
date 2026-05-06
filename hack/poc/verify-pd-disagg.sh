#!/usr/bin/env bash
# verify-pd-disagg.sh
#
# Sends one request through the gateway with a deterministic x-request-id,
# then greps logs from EPP / Sidecar / Prefiller / Decoder vLLM to confirm
# the request followed the expected PD-disaggregation flow.
#
# Env:
#   GATEWAY     base URL                   (default: http://localhost:8080)
#   MODEL       model name                 (default: Qwen/Qwen2-0.5B)
#   PROMPT      request prompt             (default: "Hello, world.")
#   MAX_TOKENS  generation length          (default: 32)
#   PREFILL_LBL label to find prefiller    (default: llm-d.ai/role=prefill)
#   DECODE_LBL  label to find decoder      (default: llm-d.ai/role=decode)
#   FLEXD_LBL   label to find flexdecoder  (default: llm-d.ai/role=flexible-decoder)
#   EPP_LBL     label to find EPP          (default: app=epp)

set -euo pipefail

GATEWAY="${GATEWAY:-http://localhost:8080}"
MODEL="${MODEL:-Qwen/Qwen2-0.5B}"
PROMPT="${PROMPT:-Hello, world.}"
MAX_TOKENS="${MAX_TOKENS:-32}"
PREFILL_LBL="${PREFILL_LBL:-llm-d.ai/role=prefill}"
DECODE_LBL="${DECODE_LBL:-llm-d.ai/role=decode}"
FLEXD_LBL="${FLEXD_LBL:-llm-d.ai/role=flexible-decoder}"
EPP_LBL="${EPP_LBL:-app=epp}"

reqid="poc-verify-$(date +%s)-$$"

echo "──────────────────────────────────────────────────────────"
echo "Request ID: ${reqid}"
echo "Sending  : ${GATEWAY}/v1/completions"
echo "──────────────────────────────────────────────────────────"

# Send the request, capture all response headers (plus body briefly).
hdr_file="$(mktemp)"
trap 'rm -f "$hdr_file"' EXIT
body_file="$(mktemp)"
trap 'rm -f "$body_file"' EXIT

http_status="$(curl -s -o "$body_file" -D "$hdr_file" -w '%{http_code}' \
  -X POST "${GATEWAY}/v1/completions" \
  -H 'Content-Type: application/json' \
  -H "x-request-id: ${reqid}" \
  -d "{\"model\":\"${MODEL}\",\"prompt\":\"${PROMPT}\",\"max_tokens\":${MAX_TOKENS},\"temperature\":0}")"

echo "HTTP status: ${http_status}"
echo "Response headers (selected):"
grep -i '^x-' "$hdr_file" || echo "  (no x-* headers)"
echo
echo "Response body (truncated to 200 chars):"
head -c 200 "$body_file"; echo
echo "──────────────────────────────────────────────────────────"

# Allow a short grace period for log shippers.
sleep 1

dump_logs() {
  local label="$1"
  local heading="$2"
  echo
  echo ">>> ${heading} (label=${label})"
  kubectl logs -l "${label}" --all-containers --tail=400 --prefix --ignore-errors=true 2>/dev/null \
      | grep -i "${reqid}" || echo "  (no log lines mentioning ${reqid})"
}

dump_logs "${EPP_LBL}"     "EPP logs"
dump_logs "${PREFILL_LBL}" "Prefiller logs"
dump_logs "${DECODE_LBL}"  "Decoder logs"
dump_logs "${FLEXD_LBL}"   "FlexibleDecoder logs"

echo
echo "──────────────────────────────────────────────────────────"
echo "If you saw:"
echo "  - EPP: 'run_prefill' or 'run_flex_decoder'"
echo "  - Sidecar: 'sending prefill request'"
echo "  - Prefiller vLLM: 'do_remote_decode'"
echo "  - Decoder vLLM: 'do_remote_prefill' or KV pull"
echo "→ PD disaggregation is functioning end-to-end."
echo "If you saw 'WARN: fallback to decode' instead, the prefill stage failed"
echo "silently; check the prefiller pod for errors."
