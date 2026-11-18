include ../../Makefile.Common

.PHONY: seaweedfs-integration-test
seaweedfs-integration-test: ## Run the SeaweedFS and JetStream integration test
	S3_INTEGRATION_ENDPOINT="$${S3_INTEGRATION_ENDPOINT:-http://s3.seaweed.127.0.0.1.nip.io:8333}" \
	NATS_URL="$${NATS_URL:-nats://localhost:4222}" \
	go test -tags=integration -count=1 -run '^TestSeaweedFSJetStreamIntegration$$' -v ./...
