# Client Metadata Selector Processor

The `clientmetadataselector` processor selects one configured value for each incoming request and stores it in client metadata for downstream components. It supports logs, metrics, traces, and profiles.

```yaml
processors:
  clientmetadataselector:
    metadata_key: shard
    values: ["0", "1", "2", "3"]
    strategy: round_robin

exporters:
  jetstream:
    subject_pattern: otel.logs.$${header:shard}
```

`metadata_key` is required. `values` must contain at least one non-empty string. Numeric values should be quoted in YAML. `strategy` supports `round_robin` and `random`; it defaults to `round_robin`.

Round-robin advances once per request and is shared safely by concurrent calls to this processor instance. 
Random selection chooses uniformly from the configured values. 
Both strategies are local to each collector process. Existing client metadata is preserved, and the selected value replaces any existing value for `metadata_key`.
