#!/usr/bin/env bash

ENDPOINT=localhost:30418

ACK_RESOURCE_ID=04b557a7-c849-494f-bfe9-4c13b9e7aea0
ACME_LOG_TYPE=SERVICE
ACME_VISIBILITY=PUBLIC

telemetrygen logs \
  --otlp-http \
  --otlp-endpoint ${ENDPOINT} \
  --duration 60s \
  --rate 1000 \
  --batch-size 10 \
  --otlp-insecure \
  --otlp-attributes 'service.name="orders"' \
  --otlp-attributes 'service.namespace="payments"' \
  --otlp-attributes 'deployment.environment="qa"' \
  --telemetry-attributes "service.instance.id=\"$(uuidgen)\"" \
  --telemetry-attributes 'service.name="jetstream-service-logs-hub"' \
  --telemetry-attributes 'acme.resource.type="PROJECT"' \
  --telemetry-attributes 'cloud.region="eu01"' \
  --telemetry-attributes "acme.log.type=\"${ACME_LOG_TYPE}\"" \
  --telemetry-attributes "acme.resource.id=\"${ACME_RESOURCE_ID}\"" \
  --telemetry-attributes "acme.visibility=\"${VISIBILITY}\"" \
  --telemetry-attributes "acme.log.id=\"$(uuidgen)\"" \
  --telemetry-attributes 'http.request.method="GET"' \
  --telemetry-attributes 'url.path="/api/v1/resource"' \
  --telemetry-attributes 'client.address="192.168.1.42"' \
  --telemetry-attributes 'server.address="api.acme.cloud"' \
  --telemetry-attributes 'user_agent.original="michal-test/v1.2.3 (internal client)"'
