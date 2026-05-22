# POC - Benchmarking Flexible Decoder

## Prerequisites

Please refer to the [`../charts/README.md`](../charts/README.md) and deploy Flexible Decoder to your k8s environment.

## Run Benchmark

### inference-perf

You can use [`inference-perf`](https://github.com/kubernetes-sigs/inference-perf) for Benchmarking.

A sample configuration file（[config.yml](./config.yml)）is available, but you need to update the and parameters（`<LLM API URL>` and `<PROMETHEUS URL>`）

The sample results stored in `results` directory.

| Directory       | Configuration                                         |
|-----------------|-------------------------------------------------------|
| results/default | preset='default'（prefill=1, decode=3）                |
| results/poc     | preset='poc'（prefill=1, decode=1, flexible decode=2）|


Here are the TTFT latency ms (%tile) results from each `summary_lifecycle_metrics.json`:

|         | P50 (ms) | P90 (ms) | P95 (ms) | P99 (ms) |
|---------|----------|----------|----------|----------|
| Default | 223      | 372      | 1255     | 5572     |
| POC     | 247      | 419      | 480      | 2269     |

