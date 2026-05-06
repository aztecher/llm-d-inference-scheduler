# PoC Validation Scripts

Helper scripts for verifying the FlexibleDecoder PoC end-to-end.
All scripts are idempotent and safe to run repeatedly.

## Prerequisites

- `kubectl` configured against the target cluster
- `curl` and `jq`
- Optional: `vegeta` for load generation (`go install github.com/tsenart/vegeta/v12@latest`)

## Scripts

### `inspect-saturation.sh`

Polls the EPP `/metrics` endpoint for saturation gauges in real time.

```bash
EPP_NAMESPACE=llm-d EPP_LABEL=app=epp ./inspect-saturation.sh
```

Output format (one line per second):

```
ts=14:32:01 saturation=0.250 threshold=0.500 above=false  flex_activations=0  fallbacks=0
```

### `verify-pd-disagg.sh`

Sends a single request through the gateway, then greps Prefiller / Decoder /
Sidecar logs for the request id to confirm:

1. EPP routed to a Prefiller (`run_prefill` decision in EPP logs)
2. Sidecar issued a prefill HTTP call
3. Prefiller vLLM received `do_remote_decode=true`
4. Decoder vLLM received `do_remote_prefill=true`

```bash
GATEWAY=http://localhost:8080 MODEL=Qwen/Qwen2-0.5B ./verify-pd-disagg.sh
```

### `generate-load.sh`

Drives burst traffic through the gateway to push the Prefill pool toward
saturation. Uses `vegeta` if available, falls back to a parallel `curl` loop.

```bash
GATEWAY=http://localhost:8080 MODEL=Qwen/Qwen2-0.5B \
  RATE=20 DURATION=60s ./generate-load.sh
```

### Force-saturation debug mode

Without code changes, validate the FlexibleDecoder activation pipeline by
forcing the SLORiskDecider to always report saturated:

```bash
kubectl set env deploy/<epp-deploy> LLM_D_FORCE_SATURATION=0.95 -n <ns>
# now any request will trigger FlexD activation (assuming pods are wired)
# verify
kubectl logs -l app=<epp-app> -n <ns> | grep -i "FORCE_SATURATION\|flex_decoder_activation"
# revert
kubectl set env deploy/<epp-deploy> LLM_D_FORCE_SATURATION- -n <ns>
```
