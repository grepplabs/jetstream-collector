# Client Metadata Processor

The `clientmetadata` processor extracts configured values from OTLP logs and stores them in the collector client metadata context for downstream consumers.

## Configuration

```yaml
processors:
  clientmetadata:
    client_metadata:
      - resource:service.name
      - telemetry:acme.resource.id
```

### `client_metadata`

Each entry uses the form `${source:key}` without the braces:

- `resource:<key>` reads a resource attribute from the exported log batch.
- `telemetry:<key>` reads a log record attribute from the exported log batch.

The processor copies the resolved values into `client.FromContext(ctx).Metadata` under the same key name.

Rules:

- Empty entries are rejected.
- Unsupported sources are rejected.
- Duplicate output keys are rejected.
- Missing or conflicting values inside one export batch fail the export.

## Behavior

- Existing client metadata is preserved.
- Configured keys overwrite any existing metadata with the same name.
- The processor currently supports logs only.
