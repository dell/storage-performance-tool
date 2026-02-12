# Top-level orchestrator for the Storage Performance Tool (SPT)

SHELL := /bin/bash
.ONESHELL:
MAKEFLAGS += --warn-undefined-variables
.DEFAULT_GOAL := help

CLI_DIR := cli
ENGINE_DIR := engine

.PHONY: help setup build test lint build-cli test-cli lint-cli build-engine test-engine lint-engine clean distclean test-coverage test-coverage-cli test-coverage-engine

help:
	@echo "Storage Performance Tool (SPT) — available targets"
	@echo "  make setup         Validate that core prerequisites are installed"
	@echo "  make build         Build CLI binary and engine bundle"
	@echo "  make test          Run CLI and engine test suites"
	@echo "  make lint          Run linters/format checks for CLI and engine"
	@echo "  make test-coverage Run CLI and engine coverage workflows"
	@echo "  make clean         Remove build artifacts in both projects"
	@echo "  make distclean     Deep clean all artifacts (CLI + engine + native libs)"
	@echo "  make build-cli     Build only the CLI binary"
	@echo "  make test-cli      Run only the CLI test suite"
	@echo "  make lint-cli      Run only the CLI lint workflow"
	@echo "  make test-coverage-cli     Run only the CLI coverage workflow"
	@echo "  make build-engine  Build only the engine bundle"
	@echo "  make test-engine   Run only the engine test suite"
	@echo "  make lint-engine   Run only the engine formatting/lint checks"
	@echo "  make test-coverage-engine  Run only the engine coverage workflow"

setup:
	@missing=0 ; \
	for tool in go docker java; do \
	  if ! command -v "$$tool" >/dev/null 2>&1; then \
	    echo "[missing] $$tool" ; \
	    missing=1 ; \
	  fi ; \
	done ; \
	if ! [ -x "$(ENGINE_DIR)/gradlew" ]; then \
	  echo "[missing] $(ENGINE_DIR)/gradlew (Gradle wrapper not executable)" ; \
	  missing=1 ; \
	fi ; \
	if [ $$missing -eq 0 ]; then \
	  echo "All prerequisites detected." ; \
	else \
	  echo "One or more prerequisites are missing. Please install the tools above." ; \
	  exit 1 ; \
	fi

build: build-cli build-engine

build-cli:
	$(MAKE) -C $(CLI_DIR) build

build-engine:
	$(MAKE) -C $(ENGINE_DIR) build

test: test-cli test-engine

test-cli:
	$(MAKE) -C $(CLI_DIR) test

test-engine:
	$(MAKE) -C $(ENGINE_DIR) test

lint: lint-cli lint-engine

lint-cli:
	$(MAKE) -C $(CLI_DIR) lint

lint-engine:
	$(MAKE) -C $(ENGINE_DIR) check

test-coverage: test-coverage-cli test-coverage-engine

test-coverage-cli:
	$(MAKE) -C $(CLI_DIR) test-coverage

test-coverage-engine:
	$(MAKE) -C $(ENGINE_DIR) test-coverage

clean:
	$(MAKE) -C $(CLI_DIR) clean
	$(MAKE) -C $(ENGINE_DIR) clean

distclean:
	$(MAKE) -C $(CLI_DIR) distclean
	$(MAKE) -C $(ENGINE_DIR) distclean
