#!/usr/bin/bash

ACME_RESOURCE_ID="c34ece9f-4e2c-41d5-8088-856efd665e23"

curl -s -G 'http://localhost:7280/api/v1/otel-logs-v0_9/search' \
  --data-urlencode "query=attributes.acme.resource.id:\"$ACME_RESOURCE_ID\"" \
  --data-urlencode 'max_hits=5' 2>&1
