# CEL Partition Processor

The `celpartition` processor evaluates CEL expressions for each log record, metric data point, or span and groups telemetry by the resulting ordered list of values.

```yaml
processors:
  celpartition:
    partition_by:
      logs:
        - "record.attributes['example.tenant.id']"
        - "record.attributes['example.log.type']"
      metrics:
        - "datapoint.attributes['example.tenant.id']"
        - "datapoint.attributes['example.visibility']"
      traces:
        - "span.attributes['example.tenant.id']"
        - "span.attributes['example.visibility']"
    on_error: reject
```

Each expression must return a string. Expressions are compiled when the processor is created and evaluated once per item. Their results are quoted and joined in order to form the partition key. Metrics are partitioned per data point and retain their metric type and aggregation metadata.

`on_error` controls CEL evaluation failures. `drop` (default) skips the item; `reject` returns a permanent error. Missing map keys are evaluation errors. If a signal has no configured expressions, its items share one partition.

The expression activation exposes `record`, `datapoint`, or `span`, each with an `attributes` map of item attributes and a `resource.attributes` map of resource attributes. For example, a metric key may use `datapoint.resource.attributes['service.name']`.
