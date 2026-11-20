# S3 Event Resolver Extension

The `s3eventresolver` extension resolves structured S3 object-created CloudEvents into inline OTLP payloads for receivers that implement the `payloadresolver` contract.

```yaml
extensions:
  s3eventresolver:
    endpoint: http://seaweedfs:8333
    region: eu-central-1
    bucket: otlp
    cloud_events_media_type: application/cloudevents+json
    secure: false
    force_path_style: true
    max_object_size: 33554432
    credentials:
      access_key_id: access
      secret_access_key: secret

receivers:
  jetstream:
    # Other JetStream settings omitted.

service:
  extensions: [s3eventresolver]
```

The resolver registers the configured `cloud_events_media_type`, which defaults to `application/cloudevents+json`. Set it to the same value as the publisher extension. It validates the event reference, downloads the object, verifies its size and optional SHA-256, decompresses gzip objects, and returns the OTLP media type declared in the event. `max_object_size` limits both stored and decompressed bytes and defaults to 32 MiB. Credentials can be supplied directly or through `credentials.provider`.
