#!/usr/bin/env bash

ENDPOINT=localhost:30418
#ENDPOINT=localhost:4318

# link
#    - name: link-project-c34ece9f-4e2c-41d5-8088-856efd665e23
#      resourceId: c34ece9f-4e2c-41d5-8088-856efd665e23
#      tenantId: 19469859-93dc-4e05-bf60-9cf43e0cd57c

# router
#  - tenantId: 19469859-93dc-4e05-bf60-9cf43e0cd57c
#
#    otlp-destinations:
#      - destinationId: a5c661dc-807e-47e1-bba7-f680026559f0
#        endpoint: http://loki-gateway.loki.svc.cluster.local
#        authorization: Basic bG9raTpsb2tpMTIz
#
#    s3-destinations:
#      - destinationId: f8c0fa53-367a-4696-8c3f-0fde71d77385
#        bucket: bucket-f8c0fa53-367a-4696-8c3f-0fde71d77385
#        endpoint: http://seaweed-s3.seaweedfs-cluster.svc.cluster.local:8333

## AWS_ENV=AWS_EC2_METADATA_DISABLED=true AWS_PAGER="" AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=admin123 aws --endpoint-url http://s3.127.0.0.1.nip.io:8333 s3 ls s3://bucket-f8c0fa53-367a-4696-8c3f-0fde71d77385 --recursive | sort -k1,1 -k2,2
## AWS_ENV=AWS_EC2_METADATA_DISABLED=true AWS_PAGER="" AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=admin123 aws --endpoint-url http://s3.127.0.0.1.nip.io:8333 s3 ls s3://backflush --recursive | sort -k1,1 -k2,2
## AWS_ENV=AWS_EC2_METADATA_DISABLED=true AWS_PAGER="" AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=admin123 aws --endpoint-url http://s3.127.0.0.1.nip.io:8333 s3 ls s3://loki --recursive | sort -k1,1 -k2,2
## stern -n collectors 'router-19469859-93dc-4e05-bf60-9cf43e0cd57c-collector-*'
## stern -n collectors 'link-router-collector-*'
## kubectl port-forward -n collectors svc/router-19469859-93dc-4e05-bf60-9cf43e0cd57c-collecto-monitoring 8888:8888
##   curl localhost:8888/metrics



ACME_RESOURCE_ID=c34ece9f-4e2c-41d5-8088-856efd665e23
ACME_LOG_TYPE=AUDIT
ACME_VISIBILITY=PUBLIC


telemetrygen logs \
  --otlp-http \
  --otlp-endpoint ${ENDPOINT} \
  --logs 1 \
  --workers 1 \
  --batch-size 1 \
  --otlp-insecure \
  --otlp-attributes 'service.name="orders"' \
  --otlp-attributes 'service.namespace="payments"' \
  --otlp-attributes 'deployment.environment="qa"' \
  --telemetry-attributes "service.instance.id=\"$(uuidgen)\"" \
  --telemetry-attributes 'service.name="jetstream-audit-logs-hub"' \
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
