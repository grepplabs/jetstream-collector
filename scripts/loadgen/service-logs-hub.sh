#!/usr/bin/env bash

ENDPOINT=localhost:30418
#ENDPOINT=localhost:4318

# link
#    - name: link-project-04b557a7-c849-494f-bfe9-4c13b9e7aea0
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


## AWS_ENV=AWS_EC2_METADATA_DISABLED=true AWS_PAGER="" AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=admin123 aws --endpoint-url http://s3.127.0.0.1.nip.io:8333 s3 ls s3://backflush --recursive | sort -k1,1 -k2,2
## AWS_ENV=AWS_EC2_METADATA_DISABLED=true AWS_PAGER="" AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=admin123 aws --endpoint-url http://s3.127.0.0.1.nip.io:8333 s3 ls s3://loki --recursive | sort -k1,1 -k2,2

## AWS_ENV=AWS_EC2_METADATA_DISABLED=true AWS_PAGER="" AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=admin123 aws --endpoint-url http://s3.127.0.0.1.nip.io:8333 s3 ls s3://bucket-92c60094-94cf-4954-9363-f896a9f68ba9 --recursive | sort -k1,1 -k2,2

## kubectl get secret -n collectors  bucket-92c60094-94cf-4954-9363-f896a9f68ba9-s3-credentials -o jsonpath='{.data.accessKey}' | base64 -d
## kubectl get secret -n collectors  bucket-92c60094-94cf-4954-9363-f896a9f68ba9-s3-credentials -o jsonpath='{.data.secretKey}' | base64 -d
## AWS_ENV=AWS_EC2_METADATA_DISABLED=true AWS_PAGER="" AWS_ACCESS_KEY_ID=GNLTUF114C92AM7OC1NXD AWS_SECRET_ACCESS_KEY=QitLSEnntxWz8t0dZuOFFFbBvdzHOURe8Sk5BuMffP aws --endpoint-url http://s3.127.0.0.1.nip.io:8333 s3 ls s3://bucket-92c60094-94cf-4954-9363-f896a9f68ba9 --recursive | sort -k1,1 -k2,2

## stern -n collectors 'router-74af337b-886c-46aa-bfe2-f818b6d14420-collector-*'
## stern -n collectors 'link-router-collector-*'
# kubectl port-forward -n collectors svc/router-74af337b-886c-46aa-bfe2-f818b6d14420-collecto-monitoring 8888:8888
##   curl localhost:8888/metrics

# kubectl port-forward -n loki svc/loki-gateway 3101:80
# curl -X POST http://localhost:3100/flush

# nats --server localhost:30422 consumer report LOGS_TENANT
# logcli --addr=http://loki.127.0.0.1.nip.io:3100  --username=loki --password=loki123 query --limit=1000 '{service_name=~".+"}'
# logcli --addr=http://loki.127.0.0.1.nip.io:3100  --username=loki --password=loki123 labels
# logcli --addr=http://loki.127.0.0.1.nip.io:3100  --username=loki --password=loki123 query '{service_name="orders"} | service_name_extracted="jetstream-service-logs-hub"' --since=1h

ACME_RESOURCE_ID=04b557a7-c849-494f-bfe9-4c13b9e7aea0
ACME_LOG_TYPE=SERVICE
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
