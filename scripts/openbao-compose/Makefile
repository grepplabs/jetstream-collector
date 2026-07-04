SHELL := /bin/bash
.SHELLFLAGS += -o pipefail -O extglob
.DEFAULT_GOAL := help

ROOT_DIR  := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

export BAO_ADDR=http://0.0.0.0:8200
export BAO_TOKEN=root

.PHONY: help
help: ## Display this help
	@printf "\nUsage:\n  make \033[36m<target>\033[0m\n"
	@awk 'BEGIN {FS = ":.*##";} \
		/^[a-zA-Z_0-9-]+:.*?##/ { \
			printf "  \033[36m%-35s\033[0m %s\n", $$1, $$2 } \
			/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) \
		} ' \
	$(MAKEFILE_LIST)

.PHONY: bao-status
bao-status: ## Check the status of the OpenBao server
	bao status
	bao token lookup
	bao auth list


.PHONY: bao-config-collector
bao-config-collector: ## Configure OpenBao with the collector configuration
	bao kv put -mount=secret otel/collector config=@config.yaml
	bao kv put -mount=secret otel/service name="jetstream-collector-bao" version="0.0.0-bao"

.PHONY: bao-get-config-collector
boa-get-config-collector:  ## Get the collector configuration from OpenBao
	bao kv get -mount=secret otel/collector
	bao kv get -mount=secret otel/service

run-collector-env: bao-config-collector ## Run the collector using OpenBao connection settings from environment variables
	cd $(ROOT_DIR)/../../cmd/jetstream-collector && go run . --config=openbao:secret/otel/collector

run-collector-config: bao-config-collector ## Run the collector using OpenBao connection settings from a provider config file
	unset BAO_ADDR && unset BAO_TOKEN && export BAO_CONFIG=$(ROOT_DIR)/openbao-provider.yaml && cd $(ROOT_DIR)/../../cmd/jetstream-collector && go run . --config=openbao:secret/otel/collector