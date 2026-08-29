# jetstream-collector

`jetstream-collector` is a set of OpenTelemetry Collector components for moving OTLP data through NATS JetStream.

The repository is organized into these areas:

- Receivers that read OTLP export requests from JetStream and feed them into a collector pipeline.
- Exporters that publish OTLP export requests to JetStream or persist data to object storage.
- Processors that reshape or enrich telemetry before it reaches an exporter.
- Confmap providers that load Collector configuration from external backends such as OpenBao.

## Components

### Receivers

| Component | Purpose | Docs |
| --- | --- | --- |
| `jetstream` | Consumes OTLP logs, metrics, traces, or profiles from a JetStream pull consumer and forwards them into the collector pipeline. | [jetstreamreceiver](receiver/jetstreamreceiver/README.md) |

### Exporters

| Component | Purpose | Docs |
| --- | --- | --- |
| `jetstream` | Publishes OTLP logs, metrics, traces, or profiles as JetStream messages on a target subject. | [jetstreamexporter](exporter/jetstreamexporter/README.md) |
| `s3` | Stores OTLP logs in S3-compatible object storage. | [s3exporter](exporter/s3exporter/README.md) |

### Processors

| Component | Purpose | Docs |
| --- | --- | --- |
| `clientmetadata` | Extracts configured resource and telemetry values and stores them in the collector client metadata context. | [clientmetadataprocessor](processor/clientmetadataprocessor/README.md) |
| `kubemapping` | Maps collector client metadata through Kubernetes resources and writes extracted values back to the client metadata context. | [kubemappingprocessor](processor/kubemappingprocessor/README.md) |
| `partitionbyattrs` | Splits incoming batches into multiple downstream batches based on configured resource and telemetry attributes. | [partitionbyattrsprocessor](processor/partitionbyattrsprocessor/README.md) |

### Confmap Providers

| Provider | Purpose | Docs |
| --- | --- | --- |
| `openbao` | Loads Collector configuration or individual fields from OpenBao KV v2 secrets. | [openbaoprovider](confmap/provider/openbaoprovider/README.md) |

## Examples

### OTLP Ingress

```yaml
---
apiVersion: opentelemetry.io/v1beta1
kind: OpenTelemetryCollector
metadata:
  name: otlp-in
  namespace: collectors
spec:
  mode: deployment
  replicas: 1

  image: ghcr.io/grepplabs/jetstream-collector:0.1

  ports:
    - name: otlp-grpc
      port: 4317
      protocol: TCP
    - name: otlp-http
      port: 4318
      protocol: TCP

  config:
    receivers:
      otlp/input:
        protocols:
          grpc:
            endpoint: 0.0.0.0:4317
          http:
            endpoint: 0.0.0.0:4318

    exporters:
      jetstream/logs-input:
        url: nats://nats.nats.svc.cluster.local:4222
        subject: logs.input
        include_subject: true
        compression: gzip
        content_type: proto
        msg_id: true

      jetstream/metrics-input:
        url: nats://nats.nats.svc.cluster.local:4222
        subject: metrics.input
        include_subject: true
        compression: gzip
        content_type: proto
        msg_id: true

    service:
      pipelines:
        logs/input:
          receivers:
            - otlp/input
          exporters:
            - jetstream/logs-input

        metrics/input:
          receivers:
            - otlp/input
          exporters:
            - jetstream/metrics-input

      telemetry:
        logs:
          level: debug
        metrics:
          level: detailed
          readers:
            - pull:
                exporter:
                  prometheus:
                    host: 0.0.0.0
                    port: 8888
```

### Local Examples

The `scripts/local/collectors` directory contains runnable example collector manifests that use the published image `ghcr.io/grepplabs/jetstream-collector:0.1`.

Recommended examples to start with:

| Manifest                                                           | Purpose                                                                                                |
|--------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| [otlp-in](scripts/local/collectors/collector-hub.yaml)        | OTLP ingress example that accepts OTLP and publishes logs and metrics to JetStream.                       |
| [tenant](scripts/local/collectors/collector-tenant.yaml)           | Tenant-aware routing example that uses OTLP metadata to publish to tenant-specific JetStream subjects. |
| [splitter](scripts/local/collectors/collector-splitter.yaml)   | Log splitting example that partitions records by resource attributes before publishing them.           |
| [s3](scripts/local/collectors/collector-backflush.yaml)       | S3-backed example that writes logs to S3-compatible object storage.                                          |
| [link-router](scripts/local/collectors/collector-link-router.yaml) | Kubernetes mapping example that routes logs based on tenant metadata derived from a CRD.                      |
