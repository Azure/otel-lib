CARGO := cargo
SHELL := /usr/bin/env bash


CARGO_PROFILE := $(if $(value RELEASE),--release,)
STAGING_DESTINATION := /tmp/main
TARGET_SUBDIRECTORY := $(if $(value RELEASE),release,debug)

REPOSITORY_ROOT := $(shell git rev-parse --show-toplevel)


.PHONY: check coverage default test

default:

check:
	exec $(REPOSITORY_ROOT)/ci/check.sh

test:
	$(eval export FEATURES INSTRUMENTED MANIFEST)
	exec $(REPOSITORY_ROOT)/ci/test.sh "$${MANIFEST}"


# NOTE: `export ${VAR} := ..` is an option, but `$(eval export ..)` is
# used to make target arguments clearer.
coverage: INSTRUMENTED := 1
coverage: $(if $(value SKIP_TEST),,test)
	$(eval export BAD GOOD MANIFEST SUMMARY_ONLY TARGET_KEY)
	exec $(REPOSITORY_ROOT)/ci/coverage.sh "$${MANIFEST}"

