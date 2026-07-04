#!/usr/bin/env bash

ENDPOINT=localhost:30418
#ENDPOINT=localhost:4318

telemetrygen metrics \
  --otlp-http \
  --otlp-endpoint ${ENDPOINT} \
  --metrics 1 \
  --workers 1 \
  --batch-size 1 \
  --otlp-insecure \
  --otlp-attributes 'service.name="orders"' \
  --otlp-attributes 'service.namespace="payments"' \
  --otlp-attributes 'deployment.environment="qa"' \
  --telemetry-attributes 'tenant="tenant-a"' \
  --telemetry-attributes 'region="eu01"' \
  --telemetry-attributes 'cluster="cluster-1"' \
  --telemetry-attributes 'instance="instance-01"' \
  --telemetry-attributes 'acme.resource.id="1344797a-fbcc-4a3c-b729-71e01c3af67c"'
