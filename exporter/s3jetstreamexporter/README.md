# s3jetstreamexporter

`s3jetstream` is a signal-specific exporter. It stores one OTLP protobuf request per S3 object, then publishes a versioned JSON object reference to JetStream. Export succeeds only after the S3 PUT and JetStream PubAck both succeed. A failed publish can leave an orphan object; configure an S3 lifecycle rule long enough to cover expected outages and retries.

```yaml
exporters:
  s3jetstream/traces:
    signal: traces
    tenant:
      metadata_key: x-tenant-id # Use only metadata set by a trusted/authenticated ingress.
    s3:
      endpoint: ${env:S3_ENDPOINT}
      region: ${env:S3_REGION}
      bucket: otlp-data
      prefix: telemetry
      credentials:
        access_key_id: ${env:S3_ACCESS_KEY}
        secret_access_key: ${env:S3_SECRET_KEY}
    jetstream:
      url: ${env:NATS_URL}
      subject: otlp.traces # Fallback if metadata is absent.
      subject_from_metadata_key: x-jetstream-subject # Optional full subject from trusted client metadata.
    object:
      compression: gzip
```

Use exactly one of `tenant.id` and `tenant.metadata_key`. Metadata mode requires exactly one nonempty value and path-safe tenant text. Keep tenant identity attached through the Collector pipeline; batching data from multiple tenants under one context is unsupported. Do not put this exporter behind a sending queue if the caller relies on synchronous PUT plus PubAck durability.

`jetstream.subject_from_metadata_key` optionally selects the full publish subject from trusted client metadata. If the key is absent, the configured `jetstream.subject` is used. Multiple, empty, or invalid values fail the export before S3 upload.

The exporter config requires `signal` and rejects a factory pipeline with a different signal. This makes accidental reuse of one subject across signals detectable. Use a separate exporter instance and subject for each signal. The key format is `{prefix}/telemetry/YYYY/MM/DD/{tenant}/{uuid-v7}.otlp`, with `.gz` appended when gzip compression is enabled; prefix is configurable. Reference `size` and `sha256` describe the stored bytes, including gzip when enabled.

The exporter acknowledges only JetStream acceptance, not final delivery to a downstream backend. JetStream retries and upstream retries can create duplicate objects or duplicate downstream work; the transport is at least once, not exactly once.
