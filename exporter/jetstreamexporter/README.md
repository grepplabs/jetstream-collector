# JetStream Exporter

The `jetstream` exporter publishes OTLP logs, metrics, traces, and profiles as OTLP export requests to a NATS JetStream subject.

## Configuration

```yaml
exporters:
  jetstream:
    url: nats://127.0.0.1:4222
    subject: otel.logs
    subject_pattern: otel.$${header:X-Tenant}.$${resource:service.name}.$${telemetry:tenant}
    compression: none
    metrics_buckets:
      publish_duration: [0.001, 0.01, 0.1, 1, 5]
      payload_size: [128, 1024, 8192, 65536]
    content_type: proto
    msg_id: false
    timeout: 5s
    retry_on_failure:
      enabled: true
    sending_queue:
      enabled: true
      queue_size: 1000
    bootstrap:
      stream:
        name: otel_logs
        subjects: [otel.logs]
    headers:
      X-Tenant: acme
    metadata_headers:
      - X-Tenant
    tls:
      enabled: false
    auth: {}
```

### Top-level fields

- `url`: NATS server URL. Default: `nats://127.0.0.1:4222`.
- `subject`: JetStream subject used when `subject_pattern` is not set. Required unless `subject_pattern` is configured.
- `subject_pattern`: Optional subject template using explicit source prefixes like `header`, `resource`, and `telemetry`, for example `otel.$${header:X-Tenant}.$${resource:service.name}.$${telemetry:tenant}`. When set, it overrides `subject`.
- `include_subject`: When enabled, publish-side custom metrics add a `subject` attribute. Default: `false`.
- `compression`: Payload compression to apply before publish. Supported values: `none`, `identity`, `gzip`. Default: `none`.
- `content_type`: OTLP encoding used for the payload body. Supported values: `proto`, `protobuf`, `application/x-protobuf`, `application/protobuf`, `application/octet-stream`, `application/grpc+proto`, `json`, `application/json`, `application/x-json`, `text/json`, and `+json` / `+proto` suffixes. Default: `proto`.
- `msg_id`: When enabled, sets `Nats-Msg-Id` to `v1:` plus a subject-scoped hex SHA-256 of the marshaled OTLP payload bytes. Default: `false`.
- `timeout`: Export timeout used by the collector exporter helper. Default: `5s`.
- `retry_on_failure`: Standard collector retry configuration. Default: retries disabled.
- `sending_queue`: Standard collector exporter queue configuration. Omit it to keep queueing disabled, or set it to enable the helper queue.
- `bootstrap`: Optional JetStream bootstrap container. Set `bootstrap.stream` to provision the stream before the exporter starts.
- `headers`: Optional static headers added to every message before protocol headers are set.
- `metadata_headers`: A list of metadata header names to copy from `client.FromContext(ctx).Metadata` into NATS headers. Each matching metadata value is appended to the message header.
- `tls`: Optional TLS client config.
- `auth`: Optional NATS auth config.

### `subject_pattern`

`subject_pattern` supports source-prefixed placeholders in the form `${source:key}`. Use `header` for collector metadata, `resource` for resource attributes, and `telemetry` for the lowest exported item attributes.

Example:

```yaml
subject_pattern: otel.$${header:X-Tenant}.$${resource:service.name}.$${telemetry:tenant}
```

Rules:

- `header:<key>` reads from `client.FromContext(ctx).Metadata`.
- `resource:<key>` reads from resource attributes.
- `telemetry:<key>` reads from the lowest exported item attributes.
- Logs support `header`, `resource`, and `telemetry`.
- Metrics, traces, and profiles currently support `header` only.
- If a `resource` or `telemetry` placeholder resolves to different values inside one exported log batch, the export fails instead of guessing.


### `retry_on_failure`

This section uses the standard collector retry configuration.

- `enabled`: Enables or disables retries.
- `initial_interval`: Delay before the first retry.
- `multiplier`: Backoff multiplier.
- `max_interval`: Maximum delay between retries.
- `max_elapsed_time`: Maximum total retry time.

### `sending_queue`

This section uses the standard collector exporter queue configuration.

- `enabled`: Enables or disables queueing.
- `queue_size`: Maximum number of queued requests.
- `num_consumers`: Number of queue workers.
- `batch`: Optional queue batching settings when supported by the collector helper.

### `headers`

`headers` is a map of static NATS headers added to every published message.

- Custom headers are applied first.
- The exporter then overwrites `Content-Type` and `Content-Encoding` so protocol metadata stays correct.
- Example:

```yaml
headers:
  X-Tenant: acme
  X-Source: collector-a
```

### `metadata_headers`

`metadata_headers` is a list of metadata keys copied from the collector client context into NATS headers.

- The exporter looks up each configured name in `client.FromContext(ctx).Metadata`.
- If a key has multiple values, each value is appended to the NATS header.
- Static `headers` are still applied first, and protocol headers continue to be set last.
- Example:

```yaml
metadata_headers:
  - X-Tenant
  - X-Trace-ID
```

### `bootstrap`

When present, `bootstrap` provisions the JetStream stream before the exporter starts.

- `bootstrap.stream.name`: Stream name. Required when `bootstrap.stream` is set.
- `bootstrap.stream.subjects`: Subjects assigned to the stream. Required when `bootstrap.stream` is set.
- Optional JetStream stream settings: `description`, `retention`, `max_consumers`, `max_msgs`, `max_bytes`, `discard`, `discard_new_per_subject`, `max_age`, `max_msgs_per_subject`, `max_msg_size`, `storage`, `num_replicas`, `no_ack`, `duplicate_window`, `sealed`, `deny_delete`, `deny_purge`, `allow_rollup_hdrs`, `compression`, `first_seq`, `allow_direct`, `mirror_direct`, `allow_msg_ttl`, `subject_delete_marker_ttl`, and `metadata`.

### `tls`

- `tls.enabled`: Enables TLS.
- `tls.insecure_skip_verify`: Skips server certificate verification.
- `tls.server_name`: Overrides the TLS server name.
- `tls.ca_file`: Path to a CA bundle file.
- `tls.cert_file`: Path to a client certificate file.
- `tls.key_file`: Path to a client private key file.

### `auth`

- `auth.username`: NATS username.
- `auth.password`: NATS password.
- `auth.token`: NATS token.
- `auth.credentials_file`: Path to a NATS credentials file.

## Payload format

The exporter encodes OTLP export requests as JetStream messages.

- By default, one export call produces one message on `subject`.
- This applies to logs, metrics, traces, and profiles.
- Default content type: protobuf.
- When `content_type: json`, the exporter marshals OTLP JSON.
- The message always gets `Content-Type` set to the matching MIME type.

## Compression

- `compression: none` sends the payload as-is.
- `compression: gzip` compresses the payload and sets `Content-Encoding: gzip`.
- If you set a conflicting `Content-Encoding` in `headers`, the exporter overwrites it.
