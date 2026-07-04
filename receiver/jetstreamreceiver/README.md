# JetStream Receiver

The `jetstream` receiver consumes OTLP logs, metrics, traces, or profiles export requests from NATS JetStream using a pull consumer and forwards them into the OpenTelemetry Collector pipeline.

## Configuration
```yaml
receivers:
  jetstream:
    url: nats://127.0.0.1:4222
    stream: otel_logs
    subject: otel.logs
    consumer_name: jetstream-dev
    processing_mode: single
    workers: 0
    batch_max_messages: 16
    batch_max_wait: 500ms
    metrics_buckets:
      consume_duration: [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5]
      payload_size: [128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536]
    batch_group_by_subject: false
    compression: none
    bootstrap:
      stream:
        name: otel_logs
        subjects: [otel.logs]
      consumer:
        name: jetstream-dev
        filter_subject: otel.logs
        ack_wait: 60s
        deliver_policy: all
        max_deliver: 0
        max_ack_pending: 1024
    tls:
      enabled: false
    auth: {}
```

### Top-level fields

- `url`: NATS server URL. Default: `nats://127.0.0.1:4222`.
- `stream`: JetStream stream name to consume from. Required.
- `subject`: Filter subject used when creating the consumer. Required.
- `consumer_name`: Name of the existing pull consumer to attach to. Use the same value on every replica when sharing one consumer.
- `processing_mode`: Receiver execution mode. Supported values: `single`, `batch`. Default: `single`.
- `workers`: Number of worker goroutines. Default: `0`. In `single` mode it enables the internal queue and parallel message handling; in `batch` mode it controls how many batch fetch loops run in parallel.
- `batch_max_messages`: Maximum number of messages fetched per batch. Default: `16`.
- `batch_max_wait`: Maximum wait time for a batch fetch before processing a partial batch. Default: `500ms`.
- `metrics_buckets`: Histogram bucket configuration. Fields are optional and default independently when omitted.
- `metrics_buckets.consume_duration`: Bucket boundaries for `jetstream_receiver_consume_duration` and `jetstream_receiver_batch_duration`. If omitted, the current defaults are used.
- `metrics_buckets.payload_size`: Bucket boundaries for `jetstream_receiver_payload_size` and `jetstream_receiver_batch_size`. If omitted, the current defaults are used.
- `batch_group_by_subject`: When enabled in batch mode, decoded messages are grouped by subject before downstream processing. Default: `false`.
- `include_subject`: When enabled, consume-side custom metrics add a `subject` attribute. Default: `false`.
- `compression`: Default payload compression when `Content-Encoding` is missing. Supported values: `none`, `identity`, `gzip`. Default: `none`.
- `bootstrap`: Optional JetStream bootstrap container. Set `bootstrap.stream` and `bootstrap.consumer` as needed.
- `tls`: Optional TLS client config.
- `auth`: Optional NATS auth config.

### `bootstrap`

When present, `bootstrap` provisions the JetStream stream and/or consumer before the receiver starts.

- `bootstrap.stream.name`: Stream name. Required when `bootstrap.stream` is set.
- `bootstrap.stream.subjects`: Subjects assigned to the stream. Required when `bootstrap.stream` is set.
- Optional JetStream stream settings: `description`, `retention`, `max_consumers`, `max_msgs`, `max_bytes`, `discard`, `discard_new_per_subject`, `max_age`, `max_msgs_per_subject`, `max_msg_size`, `storage`, `num_replicas`, `no_ack`, `duplicate_window`, `sealed`, `deny_delete`, `deny_purge`, `allow_rollup_hdrs`, `compression`, `first_seq`, `allow_direct`, `mirror_direct`, `allow_msg_ttl`, `subject_delete_marker_ttl`, and `metadata`.
- `bootstrap.consumer.name`: Consumer name. Required when `bootstrap.consumer` is set.
- `bootstrap.consumer.filter_subject`: Filter subject used by the consumer. Required when `bootstrap.consumer` is set.
- `bootstrap.consumer.description`: Optional consumer description.
- `bootstrap.consumer.ack_wait`: JetStream acknowledgment timeout.
- `bootstrap.consumer.deliver_policy`: Initial delivery policy. Supported values include `all`, `new`, `last`, `last_per_subject`, `by_start_sequence`, and `by_start_time`. Default: `all`.
- `bootstrap.consumer.max_deliver`: Maximum delivery attempts before JetStream stops redelivering. Default: `0`.
- `bootstrap.consumer.max_ack_pending`: Maximum number of unacked messages allowed.

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

## Payload Format

Each JetStream message is expected to contain one OTLP `ExportRequest`.

- Default format: protobuf.
- Set `Content-Type: application/json` to decode JSON.
- Set `Content-Type: application/x-protobuf` to force protobuf.

## Client metadata

The receiver adds the consumed NATS subject to the client metadata passed to the next component:

- `JetStream-Subject`: the complete subject.
- `JetStream-Subject-Last-Token`: the token after the last dot, or the complete subject when it contains no dots.

In batch mode, a downstream call contains ordered, de-duplicated metadata values for all subjects in that batch. Set `batch_group_by_subject: true` when downstream components require exactly one subject value per call.

## Compression

- `Content-Encoding` on the message overrides the receiver default.
- Set `compression: gzip` if producers do not send `Content-Encoding: gzip`.
- `compression: none` is the default.

## Concurrency

- `processing_mode: single` keeps the current per-message callback path.
- `processing_mode: batch` fetches messages in batches using `batch_max_messages` and `batch_max_wait`.
- `batch_group_by_subject: true` groups each batch by subject before calling downstream consumers.
- `workers: 0` keeps the single-mode inline behavior with no internal queue.
- `workers: 1` or greater enables single-mode parallel message handling with an internal queue sized from `workers`.
- In `batch` mode, `workers` controls how many batch fetch loops run in parallel.
- `consumer_name` must match the consumer that already exists or is created through `bootstrap.consumer`.
