OCB ?= ocb

ROOT_DIR = $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

BUILD_DIR = $(ROOT_DIR)/dist
CONFIG = ./builder.yaml
DOCKER_IMAGE ?= otel/opentelemetry-collector-builder:latest
PROJECT_ROOT ?= $(abspath ../..)

BINARY_NAME = jetstream-collector
INSTALL_PATH ?= /usr/local/bin/$(BINARY_NAME)

.PHONY: build clean build-docker

build:
	$(OCB) --config=$(CONFIG)

build-docker:
	rm -rf $(BUILD_DIR)
	docker run --rm \
		--user "$(shell id -u):$(shell id -g)" \
		-e HOME=/tmp \
		-e GOCACHE=/tmp/go-build \
		-e GOMODCACHE=/tmp/go-mod-cache \
		-v "$(PROJECT_ROOT):/build" \
		-w /build/cmd/jetstream-collector-ocb \
		$(DOCKER_IMAGE) \
		--config=/build/cmd/jetstream-collector-ocb/builder.yaml

install: build
	sudo install -m 755 $(BUILD_DIR)/$(BINARY_NAME) $(INSTALL_PATH)
	@echo "Installed to $(INSTALL_PATH)"

clean:
	rm -rf $(BUILD_DIR)/*
	rm -rf go.work.sum

run:
	cd $(BUILD_DIR) && go run . --config ../config.yaml
