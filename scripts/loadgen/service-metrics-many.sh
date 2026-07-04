#!/usr/bin/env bash

ENDPOINT=localhost:30418
#ENDPOINT=localhost:4318

telemetrygen metrics \
  --otlp-http \
  --otlp-endpoint ${ENDPOINT} \
  --duration 60s \
  --workers 16 \
  --rate 1000 \
  --batch-size 10 \
  --otlp-insecure \
  --otlp-attributes 'service.name="orders"' \
  --otlp-attributes 'service.namespace="payments"' \
  --otlp-attributes 'deployment.environment="qa"' \
  --telemetry-attributes 'tenant="tenant-a"' \
  --telemetry-attributes 'region="eu01"' \
  --telemetry-attributes 'cluster="cluster-1"' \
  --telemetry-attributes 'instance="instance-01"'

