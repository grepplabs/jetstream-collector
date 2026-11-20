# S3 Exporter

The `s3` exporter stores OTLP logs, metrics, and traces in S3-compatible object storage.

It marshals each exported signal batch, optionally compresses the payload, and uploads it as a single object.

## Configuration

```yaml
extensions:
  jetstreams3event/events:
    url: nats://127.0.0.1:4222
    subject: s3.object.created
    msg_id: true
    bootstrap:
      stream:
        name: S3_EVENTS
        subjects: [s3.object.created]

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
    notification:
      target: jetstreams3event/events
    metrics_buckets:
      upload_duration: [0.001, 0.01, 0.1, 1, 5]
      payload_size: [128, 1024, 8192, 65536]
    timeout: 10s
    retry_on_failure:
      enabled: true
    sending_queue:
      enabled: true
      queue_size: 1000

service:
  extensions: [jetstreams3event/events]
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
- `notification.target`: Optional Collector extension ID implementing `pkg/s3event.Notifier`. The exporter sends a CloudEvent after each successful S3 upload.

## Upload Notifications

The notification is a CloudEvents 1.0 event with type `com.grepplabs.s3.object.created.v1`. Its data contains a reference to the stored object, including bucket, key, signal, content type, compression, byte size, and an optional SHA-256 digest of the stored bytes. The telemetry payload is not included. The event ID is stable for the same bucket and key so consumers can deduplicate retries.

The configured target is resolved from Collector extensions at startup. The `jetstreams3event` extension publishes structured CloudEvents to JetStream and waits for a publish acknowledgement. Configure its URL, subject, TLS, authentication, and optional `bootstrap.stream`. Notification errors fail the export after the S3 PUT; Collector retries can therefore repeat the upload. With `msg_id: true`, JetStream uses the stable CloudEvent ID for duplicate suppression within the stream's duplicate window. When no notification target is configured, uploads work as before.

The extension also accepts `timeout` (default `5s`), `headers`, `connection.domain`, `tls`, and `auth`. The structured CloudEvent uses `application/cloudevents+json` as the NATS message content type. Add the configured publish subject to the bootstrapped stream's `subjects`.

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

When `filename_template` is set, the exporter resolves a key prefix from the current signal batch.

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
- `record:<key>` and `telemetry:<key>` are aliases. Both read from log record attributes, metric data point attributes, or span attributes, depending on the signal.
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
- The exporter supports logs, metrics, and traces.

## Compression

- `compression: none` uploads the marshaled payload as-is.
- `compression: gzip` compresses the payload and adds the `.gz` suffix.

## Metrics

The exporter reports counters and histograms for:

- Startup success and failure
- Upload attempts, successes, and failures
- Upload duration
- Uploaded payload size

Upload metrics include a `signal` attribute (`logs`, `metrics`, or `traces`). Startup metrics are signal independent.

## Notes

- The exporter uses MinIO-compatible S3 client APIs.
- When `endpoint` is omitted, the exporter resolves an AWS-style endpoint from `region`.
- If `filename_template` is empty, `filename_append_uuid` must be enabled so the object name is not empty.
