#!/usr/bin/env bash

ENDPOINT=localhost:30518
#ENDPOINT=localhost:4318

# link
#    - name: telemetry-link-project-04b557a7-c849-494f-bfe9-4c13b9e7aea0
#      resourceId: 04b557a7-c849-494f-bfe9-4c13b9e7aea0
#      tenantId: 74af337b-886c-46aa-bfe2-f818b6d14420
# router
#  - tenantId: 74af337b-886c-46aa-bfe2-f818b6d14420
#
#    otlp-destinations:
#      - destinationId: 512616ab-f1de-4bcd-87e4-5dfbcdb0e3fe
#        endpoint: http://loki-gateway.loki.svc.cluster.local
#        authorization: Basic bG9raTpsb2tpMTIz
#
#   s3-destinations:
#      - destinationId: 92c60094-94cf-4954-9363-f896a9f68ba9
#        bucket: bucket-92c60094-94cf-4954-9363-f896a9f68ba9
#        endpoint: http://seaweed-s3.seaweedfs-cluster.svc.cluster.local:8333

## AWS_ENV=AWS_EC2_METADATA_DISABLED=true AWS_PAGER="" AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=admin123 aws --endpoint-url http://s3.127.0.0.1.nip.io:8333 s3 ls s3://bucket-92c60094-94cf-4954-9363-f896a9f68ba9 --recursive | sort -k1,1 -k2,2

# nats --server localhost:30422 consumer report LOGS_TENANT
# logcli --addr=http://loki.127.0.0.1.nip.io:3100  --username=loki --password=loki123 query --limit=1000 '{service_name=~".+"}'
# logcli --addr=http://loki.127.0.0.1.nip.io:3100  --username=loki --password=loki123 labels
# logcli --addr=http://loki.127.0.0.1.nip.io:3100  --username=loki --password=loki123 query '{service_name="orders"} | service_name_extracted="jetstream-service-logs-tenant"' --since=1h


ACME_RESOURCE_ID=04b557a7-c849-494f-bfe9-4c13b9e7aea0
ACME_LOG_TYPE=SERVICE
ACME_VISIBILITY=PUBLIC
TENANT_ID=74af337b-886c-46aa-bfe2-f818b6d14420

telemetrygen logs \
  --otlp-http \
  --otlp-endpoint ${ENDPOINT} \
  --otlp-header "X-Tenant-ID=\"${TENANT_ID}\"" \
  --logs 1 \
  --workers 1 \
  --batch-size 1 \
  --otlp-insecure \
  --otlp-attributes 'service.name="orders"' \
  --otlp-attributes 'service.namespace="payments"' \
  --otlp-attributes 'deployment.environment="qa"' \
  --telemetry-attributes "service.instance.id=\"$(uuidgen)\"" \
  --telemetry-attributes 'service.name="jetstream-service-logs-tenant"' \
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
