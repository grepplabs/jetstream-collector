# Kubernetes Mapping Processor

The `kubemapping` processor finds Kubernetes objects using OpenTelemetry client metadata and writes a selected object field or label back to the metadata.

## Configuration

```yaml
processors:
  kubemapping:
    kubeconfig: /path/to/kubeconfig # optional
    context: production            # optional
    cache:
      ttl: 1m                      # default; 0s disables the cache
      capacity: 10000              # default
      cache_misses: true           # default; cache unsuccessful lookups
    error_mode: propagate          # propagate (default) or ignore
    logger:
      development: false           # true uses console/debug-friendly defaults
      encoder: json                # json (default) or console
      level: info                  # debug, info, warn, error
    mappings:
      - resource:
          group: api.hub.acme.cloud # empty for the core API group
          version: v1alpha1         # required
          kind: Link                # required
          namespace: collectors     # optional; empty searches all namespaces
        selector:
          labels:
            - 'acme.cloud/resource-id=$${header:JetStream-Subject-Last-Token}'
          fields:
            - 'metadata.name=$${header:Resource-Name}'
        value:
          field: spec.tenantId
        target: X-Tenant-ID

      - resource:
          version: v1
          kind: Namespace
        selector:
          labels:
            - 'kubernetes.io/metadata.name=$${header:Namespace}'
        value:
          label: acme.cloud/tenant-id
        target: X-Tenant-ID
```

### Options

- `kubeconfig` and `context` select a kubeconfig file and context. When omitted, normal in-cluster or default kubeconfig loading is used.
- `cache.ttl` controls how long mapped values are cached.
- `cache.capacity` is required to be greater than zero when caching is enabled.
- `cache.cache_misses` controls whether unsuccessful lookups are also cached.
- `error_mode` controls mapping failures: `propagate` returns the error; `ignore` logs it and continues without changing the target.
- `logger.development` switches between production defaults (`json`/`info`) and dev-friendly defaults (`console`/`debug`/warn stacktraces).
- `logger.encoder` accepts `json` or `console`.
- `logger.level` accepts `debug`, `info`, `warn`, or `error`.
- `logger.stacktrace_level` accepts `debug`, `info`, `warn`, or `error`.
- `mappings` run in order, so a mapping may use metadata written by an earlier mapping.
- `resource.version`, `resource.kind`, and `target` are required. `resource.group` and `resource.namespace` are optional.
- `selector.labels` and `selector.fields` use Kubernetes selector syntax and are combined with AND; field selectors support equality only. At least one selector is required, and it must resolve to exactly one object.
- `value` must contain exactly one of `field` (a dot-separated object field path) or `label` (an object label name).
- Header substitutions use `$${header:<name>}`. The extra `$` escapes Collector environment-variable expansion.

## Pipeline example

```yaml
receivers:
  jetstream/resource-logs:
    url: nats://nats:4222
    stream: LOGS_RESOURCE
    subject: logs.resource.*
    consumer_name: resource-logs

processors:
  kubemapping/resource-to-tenant:
    mappings:
      - resource:
          group: api.hub.acme.cloud
          version: v1alpha1
          kind: Link
          namespace: collectors
        selector:
          labels:
            - 'acme.cloud/resource-id=$${header:JetStream-Subject-Last-Token}'
        value:
          field: spec.tenantId
        target: X-Tenant-ID

exporters:
  jetstream/tenant-logs:
    url: nats://nats:4222
    subject_pattern: logs.tenant.$${header:X-Tenant-ID}

service:
  pipelines:
    logs:
      receivers: [jetstream/resource-logs]
      processors: [kubemapping/resource-to-tenant]
      exporters: [jetstream/tenant-logs]
```

## RBAC

The Collector service account needs `list` and `watch` permissions for every mapped resource.

```yaml
rules:
  - apiGroups: [api.hub.acme.cloud]
    resources: [telemetrylinks]
    verbs: [list, watch]
```
