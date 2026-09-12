#!/usr/bin/env bash
set -euo pipefail

ENDPOINT=${ENDPOINT:-localhost:4318}
ACME_RESOURCE_ID=${ACME_RESOURCE_ID:-00000000-0000-0000-0000-000000000001}
ACME_LOG_TYPE=${ACME_LOG_TYPE:-SERVICE}
ACME_VISIBILITY=${ACME_VISIBILITY:-PUBLIC}

# Use with cmd/jetstream-collector/config-test-domain.yaml.
# This sends one OTLP log to the collector, which publishes it to:
#   CORE/LOGS_ROUTED edge.edge_01.tenant.tenant-123.logs
#
# Inspect:
#   nats --server nats://127.0.0.1:4224 --js-domain CORE stream info LOGS_ROUTED
#   nats --server nats://127.0.0.1:4225 --js-domain EDGE_01 stream info LOGS_TENANT
#   nats --server nats://127.0.0.1:4225 --js-domain EDGE_01 consumer report LOGS_TENANT

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
  --telemetry-attributes 'service.name="jetstream-domain-logs"' \
  --telemetry-attributes 'acme.resource.type="PROJECT"' \
  --telemetry-attributes 'cloud.region="eu01"' \
  --telemetry-attributes "acme.log.type=\"${ACME_LOG_TYPE}\"" \
  --telemetry-attributes "acme.resource.id=\"${ACME_RESOURCE_ID}\"" \
  --telemetry-attributes "acme.visibility=\"${ACME_VISIBILITY}\"" \
  --telemetry-attributes "acme.log.id=\"$(uuidgen)\"" \
  --telemetry-attributes 'http.request.method="GET"' \
  --telemetry-attributes 'url.path="/api/v1/domain"' \
  --telemetry-attributes 'client.address="192.168.1.42"' \
  --telemetry-attributes 'server.address="api.acme.cloud"' \
  --telemetry-attributes 'user_agent.original="domain-test/v1"'
