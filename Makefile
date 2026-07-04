SHELL := /usr/bin/env bash
.SHELLFLAGS += -o pipefail -O extglob
.DEFAULT_GOAL := help

ROOT_DIR       = $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

LOCAL_IMAGE := local/jetstream-collector:local
LOCAL_SCRIPTS_DIR = $(ROOT_DIR)/scripts/local
LOCAL_CLUSTER_ROOT_DIR = $(LOCAL_SCRIPTS_DIR)/local-cluster
LOCAL_CLUSTER_NAME = jetstream-collector
LOCAL_KIND_CONFIG = $(ROOT_DIR)/kind-config-$(LOCAL_CLUSTER_NAME).yaml
LOCAL_KUBECONFIG = $(ROOT_DIR)/kubeconfig-$(LOCAL_CLUSTER_NAME)

include ./Makefile.Common

ALL_MODULES := ./confmap/provider/openbaoprovider ./exporter/jetstreamexporter ./exporter/s3exporter ./processor/clientmetadataprocessor ./processor/partitionbyattrsprocessor ./receiver/jetstreamreceiver
GROUP ?= all
FOR_GROUP_TARGET=for-$(GROUP)-target

GOMODULES := $(ALL_MODULES)

##@ General

.PHONY: help
help: ## display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)


##@ Build

build-distributions: ## Build collector distributions as Docker images
	docker build -f distributions/jetstream-collector/Dockerfile . -t $(LOCAL_IMAGE)

.PHONY: $(GOMODULES)
$(GOMODULES):
	@:
	$(MAKE) -C $@ $(TARGET)

.PHONY: for-all-target
for-all-target: $(GOMODULES)

.PHONY: gotest
gotest:
	@$(MAKE) $(FOR_GROUP_TARGET) TARGET="test"

.PHONY: gotidy
gotidy:
	@$(MAKE) $(FOR_GROUP_TARGET) TARGET="tidy"

.PHONY: test
test: gotest

##@ Run NATS compose

.PHONY: start-nats-compose
start-nats-compose: ## start NATS server
	docker compose -f scripts/nats-compose/docker-compose.yaml up -d --force-recreate --wait

.PHONY: stop-nats-compose
stop-nats-compose: ## stop NATS server
	docker compose -f scripts/nats-compose/docker-compose.yaml down --volumes

##@ Run OpenBao compose

.PHONY: start-openbao-compose
start-openbao-compose: ## start OpenBao server
	docker compose -f scripts/openbao-compose/docker-compose.yaml up -d --force-recreate --wait

.PHONY: stop-openbao-compose
stop-openbao-compose: ## stop OpenBao server
	docker compose -f scripts/openbao-compose/docker-compose.yaml down --volumes

##@ Local cluster

.PHONY: local-cluster-create
local-cluster-create:  ## create local kind cluster
	USER_HOME="$(HOME)" yq 'with(.nodes[].extraMounts; . += [{"containerPath": "/var/lib/kubelet/config.json", "hostPath": strenv(USER_HOME) + "/.docker/config.json"}])' \
		< "$(LOCAL_CLUSTER_ROOT_DIR)/kind-config.yaml" > "$(LOCAL_KIND_CONFIG)"
	kind create cluster --name "${LOCAL_CLUSTER_NAME}" --config "${LOCAL_KIND_CONFIG}" --kubeconfig "${LOCAL_KUBECONFIG}" \
	  || (echo "Cluster may already exist, waiting for it to become ready..."; \
		  KUBECONFIG="${LOCAL_KUBECONFIG}" kubectl wait --for=condition=Ready nodes --all --timeout=120s)

.PHONY: local-cluster-delete
local-cluster-delete:  ## delete local kind cluster
	rm -f $(LOCAL_KIND_CONFIG)
	rm -f $(LOCAL_KUBECONFIG)
	kind delete cluster --name $(LOCAL_CLUSTER_NAME)


.PHONY: local-apply
local-apply: export KUBECONFIG=$(LOCAL_KUBECONFIG)
local-apply:
	kind load docker-image --name ${LOCAL_CLUSTER_NAME} $(LOCAL_IMAGE)
	@echo "install crds"
	kubectl kustomize $(LOCAL_SCRIPTS_DIR)/crds --enable-helm | kubectl apply --server-side=true -f -

	@echo "install cert-manager"
	kubectl kustomize $(LOCAL_SCRIPTS_DIR)/cert-manager --enable-helm | kubectl apply --server-side=true --force-conflicts -f -
	kubectl wait --for=condition=available deployment --all -A --timeout=300s

	@echo "install nats"
	kubectl kustomize $(LOCAL_SCRIPTS_DIR)/nats --enable-helm | kubectl apply --server-side=true --force-conflicts -f -
	kubectl wait --for=condition=available deployment --all -A --timeout=300s

	@echo "install nack"
	kubectl kustomize $(LOCAL_SCRIPTS_DIR)/nack --enable-helm | kubectl apply --server-side=true --force-conflicts -f -
	kubectl wait --for=condition=available deployment --all -A --timeout=300s

	@echo "install seaweedfs/s3"
	kubectl kustomize $(LOCAL_SCRIPTS_DIR)/seaweedfs-operator/ --enable-helm | kubectl apply --server-side=true -f -
	kubectl wait --for=condition=available deployment --all -n seaweedfs-operator --timeout=300s
	$(MAKE) wait-seaweedfs-webhook-ca
	kubectl kustomize $(LOCAL_SCRIPTS_DIR)/seaweedfs-cluster/ --enable-helm | kubectl apply --server-side=true -f -
	$(MAKE) wait-seaweedfs-cluster

	@echo "install opentelemetry-operator"
	kubectl kustomize $(LOCAL_SCRIPTS_DIR)/opentelemetry-operator --enable-helm | kubectl apply --server-side=true --force-conflicts -f -
	kubectl wait --for=condition=available deployment --all -A --timeout=300s

	@echo "all cluster resources"
	kubectl kustomize $(LOCAL_CLUSTER_ROOT_DIR) --enable-helm | kubectl apply --server-side=true --force-conflicts -f -
	$(MAKE) create-secrets
	kubectl wait --for=condition=available deployment --all -A --timeout=300s
	$(MAKE) create-link-collectors
	$(MAKE) create-router-collectors

.PHONY: create-link-collectors
create-link-collectors: export KUBECONFIG=$(LOCAL_KUBECONFIG)
create-link-collectors:
	helm upgrade --install collectors-link --namespace collectors $(LOCAL_CLUSTER_ROOT_DIR)/../collectors-link


.PHONY: create-router-collectors
create-router-collectors: export KUBECONFIG=$(LOCAL_KUBECONFIG)
create-router-collectors:
	helm upgrade --install collectors-router --namespace collectors $(LOCAL_CLUSTER_ROOT_DIR)/../collectors-router

.PHONY: create-collectors
create-collectors: export KUBECONFIG=$(LOCAL_KUBECONFIG)
create-collectors:
	kubectl kustomize $(LOCAL_SCRIPTS_DIR)/collectors/ --enable-helm | kubectl apply --server-side=true -f -
	kubectl delete pods -n collectors --all
	kubectl wait --for=condition=available deployment --all -A --timeout=300s

.PHONY: local-deploy
local-deploy: export KUBECONFIG=$(LOCAL_KUBECONFIG)
local-deploy: build-distributions local-apply ## deploy to local kind cluster
	kubectl delete pods -n collectors --all
	kubectl wait --for=condition=available deployment --all -A --timeout=300s


ENV_FILE := .env
create-external-secrets: export KUBECONFIG=$(LOCAL_KUBECONFIG)
create-external-secrets:
	kubectl -n collectors create secret generic victoriametrics \
		--from-env-file=.env \
		--dry-run=client -o yaml | kubectl apply -f -

create-secrets: export KUBECONFIG=$(LOCAL_KUBECONFIG)
create-secrets:
	kubectl -n collectors create secret generic victoriametrics \
		--from-literal=VICTORIAMETRICS_ENDPOINT=http://victoria-metrics-single-server.victoria-metrics-single.svc:8428/opentelemetry/v1/metrics \
		--from-literal=VICTORIAMETRICS_USERNAME=dummy \
		--from-literal=VICTORIAMETRICS_PASSWORD=dummy \
		--dry-run=client -o yaml | kubectl apply -f -

.PHONY: local-init
local-init: local-cluster-create local-deploy ## init local cluster

##@ seaweedfs

.PHONY: wait-seaweedfs-webhook-ca
wait-seaweedfs-webhook-ca: export KUBECONFIG=$(LOCAL_KUBECONFIG)
wait-seaweedfs-webhook-ca:
	@echo "Waiting for webhook CA bundle..."
	@until [ "$$(kubectl get mutatingwebhookconfiguration \
		seaweedfs-operator-mutating-webhook-configuration \
		-o jsonpath='{.webhooks[0].clientConfig.caBundle}' | wc -c)" -gt 1 ]; do \
		echo "CA bundle not ready yet..."; \
		sleep 2; \
	done
	@echo "Webhook CA bundle ready"


wait-for-seaweedfs-cluster-resource = \
	echo "Waiting for $(1)/$(2)..."; \
	timeout=300; \
	start=$$(date +%s); \
	until kubectl get $(1) $(2) -n seaweedfs-cluster >/dev/null 2>&1; do \
		now=$$(date +%s); \
		if [ $$((now-start)) -ge $$timeout ]; then \
			echo "Timeout waiting for $(1)/$(2)"; \
			exit 1; \
		fi; \
		sleep 2; \
	done

wait-seaweedfs-cluster: export KUBECONFIG=$(LOCAL_KUBECONFIG)
wait-seaweedfs-cluster:
	@$(call wait-for-seaweedfs-cluster-resource,sts,seaweed-master)
	@$(call wait-for-seaweedfs-cluster-resource,sts,seaweed-volume)
	@$(call wait-for-seaweedfs-cluster-resource,sts,seaweed-filer)

	kubectl rollout status sts/seaweed-master -n seaweedfs-cluster --timeout=300s
	kubectl rollout status sts/seaweed-volume -n seaweedfs-cluster --timeout=300s
	kubectl rollout status sts/seaweed-filer -n seaweedfs-cluster --timeout=300s


SEAWEEDFS_URL=http://s3.127.0.0.1.nip.io:8333

seaweedfs-list-s3: AWS_ENV=AWS_EC2_METADATA_DISABLED=true AWS_PAGER="" AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=admin123
seaweedfs-list-s3:
	$(AWS_ENV) aws --endpoint-url $(SEAWEEDFS_URL) s3 ls


seaweedfs-list-backflush: AWS_ENV=AWS_EC2_METADATA_DISABLED=true AWS_PAGER="" AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=admin123
seaweedfs-list-backflush:
	$(AWS_ENV) aws --endpoint-url $(SEAWEEDFS_URL) s3 ls s3://backflush --recursive | sort -k1,1 -k2,2

seaweedfs-list-loki: AWS_ENV=AWS_EC2_METADATA_DISABLED=true AWS_PAGER="" AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=admin123
seaweedfs-list-loki:
	$(AWS_ENV) aws --endpoint-url $(SEAWEEDFS_URL) s3 ls s3://loki --recursive | sort -k1,1 -k2,2

seaweedfs-clean-backflush: AWS_ENV=AWS_EC2_METADATA_DISABLED=true AWS_PAGER="" AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=admin123
seaweedfs-clean-backflush:
	$(AWS_ENV) aws --endpoint-url $(SEAWEEDFS_URL) s3 rm s3://backflush --recursive

seaweedfs-clean-loki: AWS_ENV=AWS_EC2_METADATA_DISABLED=true AWS_PAGER="" AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=admin123
seaweedfs-clean-loki:
	$(AWS_ENV) aws --endpoint-url $(SEAWEEDFS_URL) s3 rm s3://loki --recursive


seaweedfs-clean-buckets: seaweedfs-clean-backflush seaweedfs-clean-loki


##@ loki

loki-query:
	logcli --addr=http://loki.127.0.0.1.nip.io:3100 query --limit=1000 '{service_name=~".+"}' --username=loki --password=loki123
