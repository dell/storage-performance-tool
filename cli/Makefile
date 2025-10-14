# Makefile for spt project

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build -mod=mod
GOCLEAN=$(GOCMD) clean -mod=mod
GOTEST=$(GOCMD) test -mod=mod
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=gofmt
GOVET=$(GOCMD) vet -mod=mod

# Binary name
BINARY_NAME=spt
BINARY_PATH=./$(BINARY_NAME)

# Build variables
BUILD_DIR=build
LDFLAGS=-ldflags "-s -w"

# Default target - show help
help:
	@echo "Available targets:"
	@echo "  help         - Show this help message"
	@echo "  all          - Run tests and build binary"
	@echo "  build        - Build the spt binary"
	@echo "  install      - Install the binary to GOPATH/bin"
	@echo "  test         - Run all tests with summary"
	@echo "  test-verbose - Run all tests with detailed output"
	@echo "  test-quick   - Run tests without verbose output"
	@echo "  test-summary - Parse existing test results and show summary"
	@echo "  testclean    - Clean test cache (forces tests to run fresh)"
	@echo "  test-coverage- Run tests with coverage report (generates coverage.html)"
	@echo "  test-deadcode - Run dead code analyzer (writes report.json)"
	@echo "  fmt          - Format Go code using gofmt"
	@echo "  vet          - Run go vet static analysis"
	@echo "  lint         - Run golangci-lint (requires installation)"
	@echo "  clean        - Remove build artifacts (binary, coverage files)"
	@echo "  distclean    - Deep clean all artifacts and reset to pre-built state"
	@echo "  deps         - Download Go module dependencies"
	@echo "  tidy         - Tidy Go module dependencies"
	@echo "  run          - Build and run the application"
	@echo "  build-all    - Build binaries for Linux, macOS, and Windows"
	@echo "  build-linux  - Build Linux AMD64 binary"
	@echo "  build-darwin - Build macOS AMD64 and ARM64 binaries"
	@echo "  build-windows- Build Windows AMD64 binary"
	@echo "  dev          - Development workflow: format, vet, test, build"
	@echo "  ci           - CI/CD workflow: deps, format, vet, test-coverage, build"
	@echo "  ci-local     - Run local CI checks (tidy, fmt-check, vet, build, optional lint, compile tests)"
	@echo "  install-hooks- Install pre-commit hook (set core.hooksPath)"

# Build and test target
all: test build

# Build the project
build:
	@echo "Building $(BINARY_NAME)..."
	$(GOBUILD) $(LDFLAGS) -o $(BINARY_PATH) -v

# Install the binary
install:
	@echo "Installing $(BINARY_NAME)..."
	$(GOCMD) install $(LDFLAGS)

# Run tests with summary
test:
	@bash tools/test_summary.sh

# Run tests with verbose output (original behavior)
test-verbose:
	@echo "Running tests (verbose)..."
	$(GOTEST) -v ./...

# Run tests quickly without verbose output
test-quick:
	@echo "Running quick tests..."
	$(GOTEST) ./...

# Parse existing test results
test-summary:
	@bash tools/test_summary.sh --parse-only

# Clean test cache
testclean:
	@echo "Cleaning test cache..."
	$(GOCLEAN) -testcache
	@echo "Test cache cleaned"

# Run tests with coverage
test-coverage:
	@bash tools/coverage_summary.sh

# Format code
fmt:
	@echo "Formatting code..."
	$(GOFMT) -w .
	@echo "Code formatting complete"

# Run go vet
vet:
	@echo "Running go vet..."
	$(GOVET) ./...

# Run linter (two-pass policy: prod full, tests unused-only)
lint:
	@echo "Running linter (two-pass policy)..."
	@if ! command -v golangci-lint >/dev/null 2>&1; then \
		echo "golangci-lint not installed. Install with:"; \
		echo "  curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $$(go env GOPATH)/bin"; \
		exit 1; \
	fi
	@MIN_VER=2.2.0; \
	ACTUAL_VER=$$(golangci-lint version 2>&1 | sed -n 's/.*version \([0-9]\+\.[0-9]\+\.[0-9]\+\).*/\1/p' | head -n1); \
	if [ -z "$$ACTUAL_VER" ]; then echo "Unable to parse golangci-lint version"; exit 1; fi; \
	LEAST=$$(printf '%s\n' "$$MIN_VER" "$$ACTUAL_VER" | sort -V | head -n1); \
	if [ "$$LEAST" != "$$MIN_VER" ]; then echo "golangci-lint $$ACTUAL_VER is too old; require $$MIN_VER+"; exit 1; fi
	@echo "\n▶ Lint: production (full set incl. unused)"; \
	if [ "${LINT_STRICT:-0}" = "1" ]; then \
		EXTRA_OPTS=""; \
		echo "(strict mode) running full production linters"; \
	else \
		EXTRA_OPTS="-D cyclop -D nestif -D dupl"; \
		echo "(soft mode) disabling: cyclop, nestif, dupl"; \
	fi; \
	if golangci-lint run -c .golangci.yml --tests=false --timeout=5m $$EXTRA_OPTS; then \
		echo "✓ Production lint clean"; \
	else \
		echo "✗ Production lint failed"; exit 1; \
	fi
	@echo "\n▶ Lint: tests (unused only)"; \
	TEST_CFG=tools/golangci-tests.yml; \
	if golangci-lint run -c "$$TEST_CFG" --tests --timeout=5m -D errcheck -D staticcheck -D govet -D ineffassign; then \
		echo "✓ Test lint clean (unused only)"; \
	else \
		echo "✗ Test lint (unused) failed"; cat "$$TEST_CFG"; exit 1; \
	fi; \
	:

# Clean build artifacts
clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
	rm -f coverage.out coverage.html

# Deep clean - remove all artifacts and reset to pre-built state
distclean: clean
	@echo "Deep cleaning all artifacts..."
	rm -rf $(BUILD_DIR)
	rm -f *.out *.html *.log
	rm -f cmd_coverage.out cmd_coverage.html
	rm -f spt-scenario-*.js
	$(GOCLEAN) -cache -testcache -modcache
	@echo "Repository reset to pre-built state"

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	$(GOMOD) download

# Tidy dependencies
tidy:
	@echo "Tidying dependencies..."
	$(GOMOD) tidy

# Run the application
run:
	@echo "Running $(BINARY_NAME)..."
	$(GOBUILD) -o $(BINARY_PATH) -v
	$(BINARY_PATH)

# Build for multiple platforms
build-all: build-linux build-darwin build-windows

build-linux:
	@echo "Building for Linux..."
	GOOS=linux GOARCH=amd64 $(GOCMD) build -mod=mod $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-linux-amd64

build-darwin:
	@echo "Building for macOS..."
	GOOS=darwin GOARCH=amd64 $(GOCMD) build -mod=mod $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-amd64
	GOOS=darwin GOARCH=arm64 $(GOCMD) build -mod=mod $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-arm64

build-windows:
	@echo "Building for Windows..."
	GOOS=windows GOARCH=amd64 $(GOCMD) build -mod=mod $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-windows-amd64.exe

# Development workflow
dev: fmt vet test build

# CI/CD workflow
ci: deps fmt vet test-coverage build

# Local CI-like checks: tidy consistency, fmt-check, vet, build, optional lint, compile tests
ci-local:
	@bash tools/local_ci.sh

# Run dead code analyzer and write report
test-deadcode:
	@echo "Running dead code analyzer..."
	@cd tools/codehealth/deadfinder && go build ./...
	@tools/codehealth/deadfinder/deadfinder -root . -cover coverage.out > tools/codehealth/deadfinder/report.json
	@echo "Report generated: tools/codehealth/deadfinder/report.json"

.PHONY: help all build install test test-verbose test-quick test-summary testclean test-coverage fmt vet lint clean distclean deps tidy run build-all build-linux build-darwin build-windows dev ci ci-local test-deadcode

# Install repo git hooks (pre-commit)
install-hooks:
	@if [ ! -f .githooks/pre-commit ]; then \
		echo ".githooks/pre-commit not found"; \
		exit 1; \
	fi
	@chmod +x .githooks/pre-commit
	@git config --local core.hooksPath .githooks
	@echo "Git hooks path set to .githooks"
	@echo "Pre-commit hook installed. It will run gofmt -s and golangci-lint on staged files."
