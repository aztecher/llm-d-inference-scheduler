# POC - Benchmarking by inference-perf

## Prerequisites

Please refer to the [`../../charts/README.md`](../../charts/README.md) and deploy Flexible Decoder to your k8s environment.

## Run Benchmark

### inference-perf

You can use [`inference-perf`](https://github.com/kubernetes-sigs/inference-perf) for Benchmarking.

A sample configuration file（[config.yml](./config.yml)）is available, but you need to update the and parameters（`<LLM API URL>` and `<PROMETHEUS URL>`）

The sample results stored in `results` directory.

| Directory       | Configuration                                         |
|-----------------|-------------------------------------------------------|
| results/default | preset='default'（prefill=1, decode=3）                |
| results/poc     | preset='poc'（prefill=1, decode=1, flexible decode=2）|


### Results

> [!Note]
> In this result, we doesn't changes the configuration of llm-d-router from [`../../charts/sample.values.yaml`](../../charts/sample.values.yaml)
> The config of llm-d-router used to run this benchmark was taken directly from `sample.values.yaml` ( no changes ware made).

Here are the TTFT latency seconds (%tile) results from each `summary_lifecycle_metrics.json`:

|         | P50 (s) | P90 (s) | P95 (s) | P99 (s) | P99.9 (s) |
|---------|----------|----------|----------|----------|------------|
| Default | 4.279     | 4.889     | 6.101     | 19.018    | 26.317      |
| POC     | 4.357     | 4.788     | 5.297    | 6.487    | 9.608      |

