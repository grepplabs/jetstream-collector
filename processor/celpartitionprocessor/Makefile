include ../../Makefile.Common

.DEFAULT_GOAL := test

.PHONY: bench
bench:
	$(GOTEST) -run '^$$' -bench '^BenchmarkPartition' -benchmem ./...
