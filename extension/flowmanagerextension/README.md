# Dynamic OpenTelemetry Collector Pipelines

## Current implementation status

Implemented:

- Typed `TelemetryFlow` API types with controller-gen generated deepcopy code and CRD YAML.
- A controller-runtime cache filtered by each extension instance's namespace and label selector.
- Dynamic logs, metrics, and traces graphs wired from the receiver, processor, exporter, and extension factories bundled in the custom OCB distribution.
- Multiple named `flowmanager` instances, each with an independent cache and graph.
- Taskfile automation for formatting, generation, unit tests, envtest integration tests, and the OCB build.

The envtest integration test covers selector filtering and flow create/update/delete, but not restart recovery. This is not yet production-complete: connectors, profiles, API status updates, and a generated/deployed RBAC and installation bundle remain to be completed. Reconciliation currently stops the active graph before starting its replacement and attempts to reconstruct the old graph if startup fails, so replacement can cause a brief ingress interruption.

## Goal

Use Kubernetes `TelemetryFlow` resources to wire existing OpenTelemetry Collector receivers, processors, and exporters into logical pipelines. Adding, changing, or deleting a selected flow should not restart the Collector or reload its bootstrap configuration.

This project is about **wiring existing components**. It does not implement replacements for processors such as `attributes`, `filter`, or `batch`, or for receivers and exporters already provided by the Collector distribution. A flow can only use component types compiled into the distribution and exposed through the runtime's allowlisted factory registry.

Each `TelemetryFlow` can define any subset of logs, metrics, and traces pipelines. The graph builder uses the corresponding signal-specific Collector factory methods and the same declarative pipeline shape. Connectors and profiles are not currently supported by the dynamic graph.

## Architecture

```text
Kubernetes API
  TelemetryFlow resources
          |
          | namespace + Kubernetes label selector
          v
One or more flowmanager extension instances
  typed cache -> validate -> build -> activate
          |
          | allowlisted existing Collector factories
          v
Logical Collector component graphs
  receiver -> ordered processors -> exporter(s)
```

Each `flowmanager` extension instance uses a controller-runtime manager with a typed `TelemetryFlow` cache, scoped by its configured namespace and standard Kubernetes label selector. It creates the requested component instances using the distribution's existing factories, connects their normal Collector consumer interfaces, and owns their lifecycle. Multiple named instances are supported; each has its own cache, watch, and graph. Use disjoint selectors to avoid processing one flow more than once.

These logical pipelines are hosted by the custom manager; they are not entries in native `service.pipelines`. The Collector's bootstrap configuration stays stable. If the requirement later becomes adding native `service.pipelines` entries, that needs a separate Collector configuration reload or rollout design.

### Component factory access

`component.Host` exposes enabled extensions, not the full receiver, processor, and exporter factory registries. The manager therefore needs explicit access to factories. The custom distribution must provide an allowlisted registry of the same factory implementations used by ordinary Collector configuration. Component behavior remains the upstream or repository implementation; the custom code handles construction, wiring, validation, and lifecycle.

The manager currently maintains an explicit allowlist matching the receiver, processor, exporter, and extension factories bundled in this OCB distribution, except for the manager extension itself. Connectors are registered in the distribution but are not yet represented or wired by the dynamic graph. Any distribution with a different component set needs its own aligned registry. The static and dynamic factory lists are still maintained separately and can drift.

### Signal fan-out

The extension implements capability-aware fan-out for logs, metrics, traces, and profiles. It clones data for mutating branches as needed, marks shared read-only data read-only, and aggregates errors from all branches. Dynamic pipelines use the logs, metrics, and traces implementations. Profiles fan-out is available, but the Collector v0.165 receiver, processor, and exporter factory APIs do not expose profile creation methods.

The behavior follows the Collector's internal implementation: [logs](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.165.0/internal/fanoutconsumer/logs.go), [metrics](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.165.0/internal/fanoutconsumer/metrics.go), [traces](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.165.0/internal/fanoutconsumer/traces.go), and [profiles](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.165.0/internal/fanoutconsumer/profiles.go). Those packages are under Go's `internal` boundary, so this module carries its own implementation rather than importing them.

## Bootstrap configuration

The Collector requires at least one native pipeline in its configuration. The flowmanager also builds logical pipelines from `TelemetryFlow` resources, but those do not replace the Collector's required static pipeline. A no-op logs pipeline satisfies this bootstrap requirement without consuming or exporting telemetry:

```yaml
receivers:
  nop:

exporters:
  nop:

extensions:
  flowmanager:
    namespace: observability
    selector:
      matchLabels:
        telemetry.grepplabs.io/collector: production

service:
  extensions: [flowmanager]
  pipelines:
    logs:
      receivers: [nop]
      exporters: [nop]
```

`namespace` and `selector` select `TelemetryFlow` CRDs, not telemetry records. Use Kubernetes label-selector semantics (`matchLabels` and `matchExpressions`). Require a non-empty selector to avoid accidentally loading every flow in the namespace.

For example, separate extension instances can select different environments:

```yaml
extensions:
  flowmanager/production:
    namespace: observability
    selector:
      matchLabels:
        collector: production
  flowmanager/staging:
    namespace: observability
    selector:
      matchLabels:
        collector: staging

service:
  extensions: [flowmanager/production, flowmanager/staging]
  pipelines:
    logs:
      receivers: [nop]
      exporters: [nop]
```

Each instance runs an independent watch and flow graph. Selectors should not overlap unless duplicate ingestion or competing receivers are intended. Also ensure component resources such as listening ports do not conflict between the graphs.

## `TelemetryFlow` API

The CRD mirrors the familiar Collector configuration shape: component IDs map to opaque component configuration, and each pipeline lists receiver, processor, and exporter IDs. The current API requires `receivers`, `exporters`, and `service`; `processors` and `extensions` are optional. Component-specific configuration is opaque and validated by the selected component factory at runtime.

```yaml
apiVersion: telemetry.grepplabs.io/v1alpha1
kind: TelemetryFlow
metadata:
  name: orders
  namespace: observability
  labels:
    telemetry.grepplabs.io/collector: production
spec:
  config:
    receivers:
      otlp/orders:
        protocols:
          grpc:
            endpoint: 0.0.0.0:4317

    processors:
      attributes/orders:
        actions:
          - key: environment
            value: production
            action: upsert
      batch/orders: {}

    exporters:
      otlp/backend:
        endpoint: backend:4317
        tls:
          insecure: true

    service:
      pipelines:
        logs/orders:
          receivers: [otlp/orders]
          processors: [attributes/orders, batch/orders]
          exporters: [otlp/backend]
```

The example uses already-implemented `otlp`, `attributes`, `batch`, and `otlp` component types. The dynamic runtime does not implement their behavior. It validates IDs and signal compatibility, decodes each opaque config with the selected factory's default config, creates the component, and wires it into the declared graph.

Signal selection is per pipeline ID. A flow may define only `metrics/system` pipelines, only `logs/application` pipelines, or any combination of logs, metrics, and traces. It does not need a pipeline for every signal.

The API uses local `AnyConfig`, `Pipeline`, and `CollectorConfig` types shaped after the OpenTelemetry Operator's configuration types. Keeping local types avoids coupling this CRD module to the Operator API and its versioning.

## Reconciliation and lifecycle

For each typed cache update, the manager builds a complete desired snapshot:

1. Read all selected flows and deterministically merge their desired component and pipeline definitions.
2. Reject duplicate IDs, unsupported component types, invalid component configuration, dangling references, unsupported signals, and invalid graph structure.
3. Construct exporters, processors from downstream to upstream, and receivers, so the graph is wired before startup.
4. Stop the active graph, then start the candidate and publish it as active.
5. If startup fails, attempt to rebuild and restart the previous graph. The current implementation does not provide an uninterrupted handoff or bounded drain for in-flight work.

If validation or construction fails, the active graph remains running. If startup fails after the active graph has stopped, restoration is attempted but can also fail. Unchanged component configurations do not cause restarts. Kubernetes API outages leave the active graph running while controller-runtime retries watch activity.

Component IDs need a deterministic namespace across all selected flows. The initial design should reject collisions rather than silently merge or override definitions. A future API could qualify IDs by flow if sharing a component definition is required.

## Partial component replacement

Partial restarts are **not implemented yet**. Any change that alters the merged flow configuration currently causes the manager to rebuild the entire graph owned by that `flowmanager` instance. An unchanged snapshot is a no-op. If candidate startup fails after the old graph has stopped, the manager attempts to recreate the prior graph.

The main constraint is that Collector processors are constructed with a downstream consumer. Replacing a downstream component can invalidate the consumers wired into upstream processors and receivers. To replace only the affected portion, implement:

1. A normalized runtime graph keyed by component ID and pipeline edge, with effective config hashes and dependency tracking.
2. Stable consumer handoff points on every edge between receivers, processors, and exporters. Each component sends to a proxy whose target can be atomically changed while the other graph nodes remain active.
3. An affected-component diff. Changing an exporter can stage just that exporter and switch its incoming proxy. Changing a processor can stage just that processor using the same downstream proxy, then switch its incoming proxy. A receiver config change replaces that receiver while leaving downstream components active.
4. In-flight accounting on each handoff so calls using the old target can drain before shutdown. Collector components do not share one generic drain API, so the manager needs bounded handoff/drain semantics and must define timeout behavior.
5. Staged activation and rollback. Start replacements before switching where possible, publish the new target, then shut down old components in reverse dependency order. A receiver that binds the same port may still need a brief stop/start unless it supports socket reuse.
6. Tests proving an exporter credential update replaces the sink without restarting the receiver, and that failed staging leaves the old route active.

An extension update must also replace components that use that extension through `component.Host`. Secret rotation needs a separate mechanism: the manager currently watches `TelemetryFlow`, not Kubernetes Secrets. Add explicit Secret references, Secret watch/RBAC, and dependency tracking if Secret changes should trigger this partial replacement path.

## Kubernetes selection and permissions

- Each extension instance's `namespace` plus label selector selects flow CRDs.
- Each instance uses one controller-runtime cache and needs read-only `list`/`watch` access to `telemetryflows` in its configured namespace.
- Do not confuse CRD selection with routing telemetry inside a flow. Telemetry routing is represented by the pipeline graph and any configured existing processors/connectors.
- The CRD defines a status subresource, but the manager does not currently write status. Reconciliation failures are logged; dedicated flow manager metrics are not yet implemented.

## Delivery and replica behavior

Replicas are not inherently unsafe. Each pod starts an independent flowmanager instance, cache, and graph. Stateless ingress such as OTLP can normally scale behind a Kubernetes Service. Stateful or polling sources need source-specific coordination: this repository's JetStream receiver supports using the same `consumer_name` on every replica to share one pull consumer, while separate consumer names can independently receive the same stream data and duplicate processing. The flow manager does not elect a leader or shard flows across pods; each replica reconciles every flow selected by its selector.

Validate scaling per receiver and destination, including delivery and acknowledgment behavior, bootstrap races, and idempotency. Replica count depends on source semantics and deployment topology; there is no universal single-replica requirement.

Define these behaviors for every pipeline:

- receiver acknowledgment relative to downstream delivery;
- exporter retries and partial fan-out failure behavior;
- bounded queues and backpressure;
- shutdown and drain timeouts;
- behavior when a destination is unavailable;
- whether parallel exporters can deliver duplicates after a retry.

## Implementation boundaries

### In scope initially

- One `TelemetryFlow` CRD with Collector-shaped component config and pipeline references.
- Kubernetes label selection, typed cache watch, validation, reconciliation, and last-known-good behavior for validation and construction errors.
- Reuse of existing registered logs receivers, processors, and exporters.
- Dynamic logical graph creation and replacement without Collector reload.
- Taskfile automation for formatting, CRD generation, module tests, envtest integration, and distribution builds.

### Out of scope initially

- Implementing new versions of existing Collector processors, receivers, or exporters.
- Loading Go component implementations that were not compiled into the distribution.
- Arbitrary native `service.pipelines` mutation or configuration reload.
- Exactly-once delivery across multiple independent exporters.
- Multi-replica source sharding unless designed explicitly.

## Remaining implementation work

1. Extend envtest coverage to invalid flows, restoration failures, and manager restart behavior.
2. Improve replacement so ingress pauses only for the handoff, downstream components drain with bounded timeouts, and failed starts reliably restore the previous graph.
3. Add reconciliation status and metrics for observed generation, errors, component health, retries, and dropped telemetry.
4. Add namespace-scoped RBAC and an installation bundle, with Taskfile tasks to generate/check/install the CRD and permissions.
5. Add connector wiring, then metrics and traces using signal-specific component interfaces and validation.
6. Consolidate factory definitions so the dynamic allowlist and static Collector registration cannot drift.

## Open design decisions

1. What are the precise semantics for multiple pipelines referencing the same processor or exporter ID?
