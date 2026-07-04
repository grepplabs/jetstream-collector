# Partition By Attributes Processor

The `partitionbyattrs` processor splits incoming OTLP logs into multiple downstream `ConsumeLogs` calls based on configured resource and telemetry attributes.

It guarantees that each downstream batch contains only one unique combination of the configured partition values. This is useful for exporters that require homogeneous batches, for example when building routing keys or JetStream subjects.

## Configuration

```yaml
processors:
  partitionbyattrs:
    partition_by:
      resource:
        - service.name
      telemetry:
        - my.resource.id
    missing_attribute_action: error
```

### `partition_by`

- `resource`: Resource attribute keys used to build the partition key.
- `telemetry`: Log record attribute keys used to build the partition key.

All configured keys are normalized by trimming whitespace. Empty keys and duplicates are rejected during configuration validation.

### `missing_attribute_action`

Supported values:

- `error` (default): reject the whole batch if any configured attribute is missing.
- `drop`: skip records that are missing any configured partition attribute.

## Behavior

- Resource and telemetry attributes are read for every log record.
- Each record is assigned to exactly one partition key.
- Records with the same partition key are forwarded together.
- When `partition_by` is empty, each log record becomes its own downstream batch.

## Example

Input records:

- `A` with `my.resource.id=A`
- `A` with `my.resource.id=A`
- `B` with `my.resource.id=B`

With:

```yaml
partition_by:
  telemetry:
    - my.resource.id
```

the processor forwards two batches:

- batch 1: `A`, `A`
- batch 2: `B`
