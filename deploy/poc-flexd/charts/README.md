# POC - Deploy Flexible Decoder 

## Prerequisites

- Kubernetes
  - [Gateway API Inference Extension](https://github.com/kubernetes-sigs/gateway-api-inference-extension)
  - Prometheus ([kube-prometheus-stack](https://github.com/prometheus-community/helm-charts/tree/main/charts/kube-prometheus-stack) is recommended）
- GPU Worker （Require: 4x GPU at least）
- Options
  - Grafana ([kube-prometheus-stack](https://github.com/prometheus-community/helm-charts/tree/main/charts/kube-prometheus-stack) is recommended）
  - [ExternalDNS](https://github.com/kubernetes-sigs/external-dns)


## Deploy

First, you need to create secrets that contains the tokens of huggingface.

```bash
# Set target namespace
export NAMESPACE=<your namespace>

# Create secrets of HF_TOKEN
kubectl create secret generic llm-d-hf-token \
  --from-literal=HF_TOKEN="<huggingface token>" \
  -n $NAMESPACE
```

Second, copy `sample.values.yaml` to `values.yaml` and edit to fit for your k8s environment.

Finally, deploy by `helm` command.

```bash
# POC (prefill = 1, decode = 1, flexible decode = 2)
helm upgrade -i --create-namespace \
  --namespace $NAMESPACE \
  llm-d-router-poc \
  . \
  -f values.yaml \
  --set preset="poc"

# Default (prefill = 1, decode = 3)
helm upgrade -i --create-namespace \
  --namespace $NAMESPACE \
  llm-d-router-poc \
  . \
  -f values.yaml \
  --set preset="default"
```
