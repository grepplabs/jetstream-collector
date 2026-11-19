include ../../Makefile.Common

.PHONY: seaweedfs-integration-test
seaweedfs-integration-test: ## Run the SeaweedFS integration tests for logs, metrics, and traces
	S3_INTEGRATION_ENDPOINT="$${S3_INTEGRATION_ENDPOINT:-http://s3.seaweed.127.0.0.1.nip.io:8333}" \
	go test -tags=integration -count=1 -run '^TestSeaweedFSIntegration$$' -v ./...
