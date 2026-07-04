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

