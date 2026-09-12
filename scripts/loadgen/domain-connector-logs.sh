#!/usr/bin/env bash
set -euo pipefail

ENDPOINT=${ENDPOINT:-localhost:4318}
ACME_LOG_TYPE=${ACME_LOG_TYPE:-SERVICE}
ACME_VISIBILITY=${ACME_VISIBILITY:-PUBLIC}

# Use with cmd/jetstream-collector/config-test-domain-connector.yaml.
# It sends two logs with acme.resource.id values that match the routing connector:
#   00000000-0000-0000-0000-000000000001 -> edge.edge_01.tenant.tenant-001.logs
#   00000000-0000-0000-0000-000000000002 -> edge.edge_02.tenant.tenant-00N.logs
#
# Inspect:
#   nats --server nats://127.0.0.1:4224 --js-domain CORE stream info LOGS_ROUTED
#   nats --server nats://127.0.0.1:4224 --js-domain CORE stream report
#   nats --server nats://127.0.0.1:4225 --js-domain EDGE_01 consumer report LOGS_TENANT
#   nats --server nats://127.0.0.1:4226 --js-domain EDGE_02 consumer report LOGS_TENANT

send_log() {
  local resource_id="$1"
  local service_name="$2"
  local path="$3"

  telemetrygen logs \
    --otlp-http \
    --otlp-endpoint "${ENDPOINT}" \
    --logs 1 \
    --workers 1 \
    --batch-size 1 \
    --otlp-insecure \
    --otlp-attributes 'service.name="orders"' \
    --otlp-attributes 'service.namespace="payments"' \
    --otlp-attributes 'deployment.environment="local"' \
    --telemetry-attributes "service.instance.id=\"$(uuidgen)\"" \
    --telemetry-attributes "service.name=\"${service_name}\"" \
    --telemetry-attributes 'acme.resource.type="PROJECT"' \
    --telemetry-attributes 'cloud.region="eu01"' \
    --telemetry-attributes "acme.log.type=\"${ACME_LOG_TYPE}\"" \
    --telemetry-attributes "acme.resource.id=\"${resource_id}\"" \
    --telemetry-attributes "acme.visibility=\"${ACME_VISIBILITY}\"" \
    --telemetry-attributes "acme.log.id=\"$(uuidgen)\"" \
    --telemetry-attributes 'http.request.method="GET"' \
    --telemetry-attributes "url.path=\"${path}\"" \
    --telemetry-attributes 'client.address="192.168.1.42"' \
    --telemetry-attributes 'server.address="api.acme.cloud"' \
    --telemetry-attributes 'user_agent.original="domain-connector-test/v1"'
}

send_log "00000000-0000-0000-0000-000000000001" "jetstream-domain-connector-edge-01" "/api/v1/domain/edge-01"
send_log "00000000-0000-0000-0000-000000000002" "jetstream-domain-connector-edge-02" "/api/v1/domain/edge-02"
