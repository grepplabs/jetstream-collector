# s3jetstreamreceiver

The receiver uses a signal-specific durable pull consumer. Configure a separate durable consumer with a non-overlapping filter subject for each signal, and share that durable consumer across replicas of the same signal. If bootstrap is enabled, provision a stream and consumer before traffic; use `workqueue` retention for work streams and review limits so pending references are not silently evicted.

```yaml
receivers:
  s3jetstream/traces:
    signal: traces
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
      stream: OTLP
      consumer: otlp-traces
      subject: otlp.traces
    processing:
      workers: 16
      max_object_size: 33554432
      ack_progress: false
    cleanup:
      subject: otlp.delete
```

At startup the receiver checks that the durable consumer has explicit ACKs, is pull-based, and filters exactly the configured subject. It validates the reference signal and S3 bucket/key namespace, bounds stored and decompressed object sizes, checks the stored-byte checksum when present, decodes OTLP protobuf, and invokes the matching downstream consumer. It publishes the exact original reference bytes to the cleanup subject and waits for PubAck before double-acking the processing message. Long-running work sends ACK progress updates.

An S3 404 is NAKed and retried. The cleanup worker is intentionally not included yet: deleting before the original ACK is confirmed creates a redelivery race, and no durable completion/tombstone protocol has been selected. Do not configure destructive cleanup until that protocol is designed, implemented, and tested. `Consume*` success means only that the downstream consumer accepted the data, possibly into its own queue; it does not mean final backend delivery. Duplicate downstream deliveries remain possible.
