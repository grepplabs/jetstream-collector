include ../../Makefile.Common

.DEFAULT_GOAL := test

.PHONY: bench
bench:
	$(GOTEST) -run '^$$' -bench '^BenchmarkMappingCache$$' -benchmem ./...

.PHONY: generate
generate:
	$(MDATAGEN) metadata.yaml
	goimports -w ./
