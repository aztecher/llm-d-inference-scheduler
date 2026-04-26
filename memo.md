# HOW TO DEVELOP

## Change Codes

Change codes for your contribution

## Build Image

```bash
EPP_TAG=poc LDFLAGS="" make image-build-epp
```

Created `ghcr.io/llm-d/llm-d-inference-scheduler:poc`

## Create kind environment

Modified Makefile, so reset before upstreaming

```bash
diff --git a/Makefile b/Makefile
index 6ae2531..17a95f2 100644
--- a/Makefile
+++ b/Makefile
@@ -19,7 +19,7 @@ BUILDER_IMAGE_NAME ?= llm-d-builder
 IMAGE_REGISTRY ?= ghcr.io/llm-d

 IMAGE_TAG_BASE ?= $(IMAGE_REGISTRY)/$(PROJECT_NAME)
-EPP_TAG ?= dev
+EPP_TAG ?= poc # FIXME
 export EPP_IMAGE ?= $(IMAGE_TAG_BASE):$(EPP_TAG)

 SIDECAR_TAG ?= dev
```

```bash
PD_ENABLED=true EPP_TAG=poc LDFLAGS="" make env-dev-kind
```

## Make Request

Use port-forward

```bash
kubectl port-forward service/inference-gateway-istio 8080:80
```

confirm model

```bash
curl -s http://localhost:8080/v1/models | jq
```

req

```bash
curl -s -w '\n' http://localhost:8080/v1/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"TinyLlama/TinyLlama-1.1B-Chat-v1.0","prompt":"hi","max_tokens":10,"temperature":0}' | jq
```


## Clenaup kind environment

```bash
make clean-env-dev-kind
```

---

## Test

### Unit test

```bash
make test-unit
make test-unit-epp
make test-unit-sidecar
```

### Integration test

```bash
make test-integration
```

### End-to-End test

```bash
make test-e2e

# keep cluster
E2E_KEEP_CLUSTER_ON_FAILURE=true make test-e2e
```

---

## Config
