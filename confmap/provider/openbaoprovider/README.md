# OpenBao configuration provider

The `openbao` provider loads a complete OpenTelemetry Collector configuration
from an OpenBao KV v2 secret.

The secret reference has this form:

```text
openbao:<mount>/<path>
```

The referenced secret must contain a string field named `config`. For example:

```bash
bao kv put -mount=secret otel/collector config=@collector.yaml
```

Configure token authentication and start a Collector distribution containing this provider:

```bash
export BAO_ADDR=http://127.0.0.1:8200
export BAO_AUTH_METHOD=token
export BAO_TOKEN=root

jetstream-collector --config=openbao:secret/otel/collector
```

`BAO_AUTH_METHOD` is optional in this version and defaults to token authentication.

## Bootstrap configuration file

Set `BAO_CONFIG` to load connection and authentication settings from a separate YAML file:

```yaml
address: https://openbao.internal:8200
namespace: team-a
auth:
  method: token
  token: literal-token
tls:
  ca_cert: /etc/openbao/ca.pem
  client_cert: /etc/openbao/client.pem
  client_key: /etc/openbao/client-key.pem
```

String values may reference environment variables:

```yaml
address: ${env:BAO_ADDR}
auth:
  method: token
  token: ${env:BAO_TOKEN}
```

```bash
export BAO_CONFIG=/etc/otelcol/openbao.yaml
export BAO_ADDR=https://openbao.internal:8200
export BAO_TOKEN=secret
```

When `BAO_CONFIG` is set, the file is authoritative. Environment variables are
used only where the file explicitly contains `${env:...}` references. Without
`BAO_CONFIG`, the provider builds the same configuration from the standard
`BAO_*` variables. The bootstrap file is separate from the Collector
configuration because it is needed before the OpenBao source can be read.

## Environment

| Variable | Required | Description |
| --- | --- | --- |
| `BAO_CONFIG` | no | OpenBao provider bootstrap YAML file |
| `BAO_ADDR` | unless configured in the file | OpenBao server URL |
| `BAO_TOKEN` | unless configured in the file | Token used to read the secret |
| `BAO_AUTH_METHOD` | no | Must be `token` when set |
| `BAO_NAMESPACE` | no | OpenBao namespace |
| `BAO_CACERT` | no | PEM CA certificate file |
| `BAO_CLIENT_CERT` | no | PEM client certificate file |
| `BAO_CLIENT_KEY` | no | PEM client private-key file |
| `BAO_WATCH_ENABLED` | no | Enable KV v2 version polling; defaults to `false` |
| `BAO_WATCH_INTERVAL` | no | Poll interval when watching is enabled; defaults to `30s` |


### Examples:

`openbao-provider.yaml` runnable bootstrap using the repository's Collector distribution.

```yaml
address: ${env:BAO_ADDR:-http://127.0.0.1:8200}
auth:
  method: token
  token: ${env:BAO_TOKEN:-root}
```

`config-openbao-fragments.yaml` individual field references in a local Collector configuration

```yaml
receivers:
  nop:

exporters:
  otlphttp/openbao:
    endpoint: ${openbao:secret/otel/backend#endpoint}
    headers:
      Authorization: ${openbao:secret/otel/backend#authorization}
      X-Tenant: ${openbao:secret/otel/backend#tenant}

service:
  pipelines:
    traces:
      receivers: [nop]
      exporters: [otlphttp/openbao]
```


## Individual fields

Append a field selector to retrieve one string value from a secret:

```text
openbao:<mount>/<path>#<field>
```

This can be used for inline Collector substitutions:

```yaml
exporters:
  otlp:
    headers:
      Authorization: ${openbao:secret/otel/credentials#token}
```

Selectors address top-level secret fields. The selected field must exist and
must be a string. Nested selectors and automatic type conversion are not
supported.

## Change watching

Change watching is optional and disabled by default. Enable it with:

```bash
export BAO_WATCH_ENABLED=true
export BAO_WATCH_INTERVAL=30s
```

The same settings can be supplied in `BAO_CONFIG`:

```yaml
watch:
  enabled: true
  interval: 30s
```

When watching is enabled and the Collector supplies a watcher callback, the provider
records the version returned by the initial `Retrieve` and polls KV v2 metadata at the
configured interval.
