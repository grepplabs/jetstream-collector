# S3 Exporter

The `s3` exporter stores OTLP logs in S3-compatible object storage.

It marshals each exported log batch, optionally compresses the payload, and uploads it as a single object.

## Configuration

```yaml
exporters:
  s3:
    bucket: jetstream-logs
    endpoint: https://s3.eu-central-1.amazonaws.com
    region: eu-central-1
    secure: true
    force_path_style: false
    credentials:
      access_key_id: AKIA...
      secret_access_key: secret
    filename_template: logs/$${resource:service.name}/$${timefmt:%Y/%m/%d/}
    filename_append_uuid: true
    filename_extension: log
    marshaler: proto
    compression: gzip
    metrics_buckets:
      upload_duration: [0.001, 0.01, 0.1, 1, 5]
      payload_size: [128, 1024, 8192, 65536]
    timeout: 10s
    retry_on_failure:
      enabled: true
    sending_queue:
      enabled: true
      queue_size: 1000
```

## Top-Level Fields

- `bucket`: Target S3 bucket. Required.
- `endpoint`: S3 endpoint. If empty, the exporter uses the AWS-style endpoint for the configured region.
- `region`: AWS region used when resolving the default endpoint and client configuration. Default: `eu-central-1`.
- `secure`: Use HTTPS when connecting to the endpoint. Default: `true`.
- `force_path_style`: Use path-style bucket lookup instead of virtual-hosted style.
- `credentials`: Authentication settings.
- `filename_template`: Optional object key prefix template.
- `filename_append_uuid`: Appends a UUID to the generated object name. Default: `true`.
- `filename_extension`: File extension used before any compression suffix. Default: `log`.
- `marshaler`: OTLP encoding to upload. Supported values: `proto` and `json`. Default: `proto`.
- `compression`: Payload compression. Supported values: `none` and `gzip`. Default: `none`.
- `timeout`: Export timeout used by the collector exporter helper.
- `retry_on_failure`: Standard collector retry configuration.
- `sending_queue`: Standard collector exporter queue configuration.

## Credentials

Provide credentials in one of two ways:

- Static credentials with `credentials.access_key_id` and `credentials.secret_access_key`.
- A credentials provider extension with `credentials.provider`.

If static credentials are used, `access_key_id` and `secret_access_key` must be set together.

### Credentials Example

```yaml
credentials:
  access_key_id: AKIA...
  secret_access_key: secret
  session_token: optional-token
```

## Filename Templates

When `filename_template` is set, the exporter resolves a key prefix from the current log batch.

Supported template sources:

- `header`
- `resource`
- `record`
- `telemetry`
- `timefmt`

Example:

```yaml
filename_template: logs/$${resource:service.name}/$${timefmt:%Y/%m/%d/}
```

Rules:

- `header:<key>` reads from collector client metadata.
- `resource:<key>` reads from resource attributes.
- `record:<key>` reads from log record attributes.
- `telemetry:<key>` reads from telemetry attributes.
- `timefmt:<layout>` formats the batch timestamp using the supplied layout.

If `filename_append_uuid` is also enabled, the exporter appends a UUID after the resolved prefix.

## Object Naming

The final object name is built from:

1. The resolved filename template prefix, if present.
2. A UUID, when `filename_append_uuid` is `true`.
3. The file extension, if configured.
4. A `.gz` suffix when `compression: gzip` is enabled.

Example output:

- `logs/service-a/2026/07/31/8f2c...c1.log`
- `logs/service-a/2026/07/31/8f2c...c1.log.gz`

## Payload Format

- `marshaler: proto` uploads OTLP protobuf payloads.
- `marshaler: json` uploads OTLP JSON payloads.
- The exporter currently handles logs only.

## Compression

- `compression: none` uploads the marshaled payload as-is.
- `compression: gzip` compresses the payload and adds the `.gz` suffix.

## Metrics

The exporter reports counters and histograms for:

- Startup success and failure
- Upload attempts, successes, and failures
- Upload duration
- Uploaded payload size

## Notes

- The exporter uses MinIO-compatible S3 client APIs.
- When `endpoint` is omitted, the exporter resolves an AWS-style endpoint from `region`.
- If `filename_template` is empty, `filename_append_uuid` must be enabled so the object name is not empty.
