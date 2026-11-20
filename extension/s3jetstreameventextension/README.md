# S3 JetStream Event Extension

The `jetstreams3event` extension implements the `s3event.Notifier` interface and publishes structured CloudEvents
received from `s3exporter` to JetStream. It waits for a JetStream publish acknowledgement before returning success.

```yaml
extensions:
  jetstreams3event/events:
    url: nats://127.0.0.1:4222
    subject: s3.object.created
    cloud_events_media_type: application/cloudevents+json
    timeout: 5s
    msg_id: true
    headers: { }
    connection:
      domain: ""
    bootstrap:
      stream:
        name: S3_EVENTS
        subjects: [ s3.object.created ]
        storage: file
    tls:
      enabled: false
    auth: { }

service:
  extensions: [ jetstreams3event/events ]
```

`cloud_events_media_type` controls the message `Content-Type` header and defaults to `application/cloudevents+json`.

