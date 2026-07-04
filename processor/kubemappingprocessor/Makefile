include ../../Makefile.Common

.DEFAULT_GOAL := test

.PHONY: generate
generate:
	$(MDATAGEN) metadata.yaml
	goimports -w ./
