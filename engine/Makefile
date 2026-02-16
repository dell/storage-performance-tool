# Spt v5 Bundle Makefile
# 
# IMPORTANT: This Makefile is a convenience wrapper around Gradle commands.
# The Gradle Wrapper (./gradlew) is the official build tool for this project.
# 
# This Makefile exists to provide:
#   - Familiar commands for developers coming from Make-based projects
#   - Shortcuts for common development tasks
#   - Consistent command interface alongside gradlew
#
# For IDE integration, CI/CD, and official builds, please use:
#   ./gradlew (Unix/Linux/macOS) or gradlew.bat (Windows)
#
# All make targets ultimately execute gradlew commands.

# Default target
.DEFAULT_GOAL := help

# Variables
GRADLE := ./gradlew
DIST_DIR := bundle/build/dist
SPT_JAR := $(DIST_DIR)/spt.jar
VERSION_FILE := ../VERSION
SPT_VERSION := $(shell if [ -f $(VERSION_FILE) ]; then tr -d '\n' < $(VERSION_FILE); else echo "0.0.0-dev"; fi)
SPT_RELEASE_VERSION := $(firstword $(subst -, ,$(SPT_VERSION)))
ZIP_FILE := bundle/build/distributions/spt-bundle-$(SPT_VERSION).zip
# Build options - tests disabled by default for faster builds
# The gradle.properties file enables parallel execution, build caching, etc.
BUILD_OPTS := --no-daemon

# Phony targets (not files)
.PHONY: all build clean distclean test test-coverage bundle help check lint spotless docker docker-run docker-push
.PHONY: test-integration test-performance test-all test-coverage-all errorprone

# Help target - displays available targets
help:
	@echo "Spt v5 Bundle Build Targets:"
	@echo "  make build      - Build the complete distribution (default)"
	@echo "  make clean      - Clean all build artifacts"
	@echo "  make distclean  - Deep clean including Gradle cache and wrapper"
	@echo "  make test       - Run all tests"
	@echo "  make test-integration - Run only integration tests"
	@echo "  make test-performance - Run only performance tests"
	@echo "  make test-all   - Run unit + integration + performance tests"
	@echo "  make test-coverage - Run unit tests with coverage report"
	@echo "  make test-coverage-all - Run all tests with combined coverage report"
	@echo "  make check      - Run spotless checks"
	@echo "  make lint       - Apply spotless formatting"
	@echo "  make spotless   - Apply spotless formatting (alias for lint)"
	@echo "  make bundle     - Build only the bundle module"
	@echo "  make dist       - Create ZIP distribution"
	@echo "  make run        - Run Spt from distribution"
	@echo "  make vscode     - Create VS Code launch configuration"
	@echo "  make quick      - Quick build without tests or checks"
	@echo "  make cache-report - Build with configuration cache debugging"
	@echo "  make docker     - Build Docker image"
	@echo "  make docker-run - Run Spt in Docker container"
	@echo "  make docker-push- Push Docker image to Dell registry"
	@echo "  make docker-build-push - Build and push with authentication"
	@echo "  make docker-setup - Instructions for Harbor robot account setup"
	@echo "  make docker-login - Manual login to Harbor registry"
	@echo "  make docker-scenario SCENARIO=file.js - Run with custom scenario"
	@echo "  make docker-scenarios - List built-in scenarios"
	@echo "  make docker-compose-run - Run Spt with Docker Compose"
	@echo "  make all        - Clean and build everything"
	@echo "  make errorprone - Run Error Prone analysis (writes errorprone_scan.txt)"
	@echo "  make help       - Show this help message"

# Build the complete distribution
build: $(SPT_JAR)

# Build rule for the distribution
$(SPT_JAR):
	@echo "Building Spt v5 distribution..."
	$(GRADLE) build shadowJar $(BUILD_OPTS)
	@echo "Distribution built successfully in: $(DIST_DIR)"
	@echo "Main JAR: $(SPT_JAR)"
	@echo "Extensions: $(DIST_DIR)/ext/"
	@ls -la $(DIST_DIR) 2>/dev/null || true

# Build only the bundle module
bundle:
	@echo "Building bundle module..."
	$(GRADLE) :bundle:build $(BUILD_OPTS)

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	$(GRADLE) clean
	@echo "Clean complete."

# Deep clean - removes all generated files and caches
distclean: clean
	@echo "Performing deep clean..."
	rm -rf .gradle
	rm -rf build
	rm -rf */build
	rm -rf */*/build
	rm -rf */*/*/build
	rm -rf */*/*/*/build
	rm -rf ~/.gradle/caches/modules-2/files-2.1/io.github.dell.spt
	find . -name "*.class" -delete
	find . -name "*.jar" -not -path "./gradle/wrapper/*" -delete
	# RDMA native library (CMake build artifacts)
	rm -rf extensions/storage-drivers/implementations/s3-rdma/src/main/native/build
	@echo "Deep clean complete."

# Run all tests
test:
	@echo "Running all tests..."
	$(GRADLE) test

# Run only integration tests
test-integration:
	@echo "Running integration tests..."
	$(GRADLE) testIntegration

# Run only performance tests
test-performance:
	@echo "Running performance tests..."
	$(GRADLE) testPerformance

# Run all test categories
test-all:
	@echo "Running unit + integration + performance tests..."
	$(GRADLE) testAll

# Run tests with coverage reporting
test-coverage:
	@echo "Running tests with coverage analysis..."
	@echo "This may take a minute..."
	@# Always run fresh tests and generate new coverage report (with shadow JARs for integration tests)
	@$(GRADLE) clean --no-daemon > /tmp/gradle-coverage.log 2>&1
	@$(GRADLE) shadowJar test jacocoRootReport --continue --no-daemon >> /tmp/gradle-coverage.log 2>&1
	@# Show build status
	@grep -E "(BUILD|FAILED)" /tmp/gradle-coverage.log | tail -1 || echo "BUILD COMPLETED"
	@echo ""
	@# Generate and show coverage statistics
	@./tools/get-coverage-stats.sh
	@# If coverage is still N/A, run again without clean (sometimes needed for aggregate report)
	@if ./tools/get-coverage-stats.sh | grep -q "Overall Coverage: N/A"; then \
		echo ""; \
		echo "Finalizing coverage report..."; \
		$(GRADLE) jacocoRootReport --no-daemon > /tmp/gradle-coverage.log 2>&1; \
		./tools/get-coverage-stats.sh; \
	fi
	@echo ""
	@echo "Detailed coverage report: build/reports/jacoco/aggregate/html/index.html"
	@# Clean up
	@rm -f /tmp/gradle-coverage.log

# Run combined coverage (unit + integration + performance)
test-coverage-all:
	@echo "Running all tests with combined coverage analysis..."
	@echo "This may take a few minutes..."
	@$(GRADLE) clean --no-daemon > /tmp/gradle-coverage-all.log 2>&1
	@$(GRADLE) shadowJar testAll jacocoRootReportAll --continue --no-daemon >> /tmp/gradle-coverage-all.log 2>&1
	@grep -E "(BUILD|FAILED)" /tmp/gradle-coverage-all.log | tail -1 || echo "BUILD COMPLETED"
	@echo ""
	@# Generate and show coverage statistics
	@./tools/get-coverage-stats.sh
	@# If coverage is still N/A, run again without clean (sometimes needed for aggregate report)
	@if ./tools/get-coverage-stats.sh | grep -q "Overall Coverage: N/A"; then \
		echo ""; \
		echo "Finalizing coverage report..."; \
		$(GRADLE) jacocoRootReport --no-daemon > /tmp/gradle-coverage.log 2>&1; \
		./tools/get-coverage-stats.sh; \
	fi
	@echo ""
	@echo "Detailed coverage report: build/reports/jacoco/aggregate/html/index.html"
	@# Clean up
	@rm -f /tmp/gradle-coverage.log

# Run spotless check
check:
	@echo "Running spotless check..."
	$(GRADLE) spotlessCheck

# Apply spotless formatting
lint:
	@echo "Applying spotless formatting..."
	$(GRADLE) spotlessApply

# Run Error Prone analysis (compile-only)
errorprone:
	@echo "Running Error Prone analysis (compileJava + compileTestJava)..."
	@bash -c 'set -o pipefail; $(GRADLE) -I tools/errorprone-init.gradle :core:spt-base:compileJava :core:spt-base:compileTestJava --rerun-tasks -x test 2>&1 | tee errorprone_scan.txt'

# Alias for spotless formatting (alternative name)
spotless:
	@echo "Applying spotless formatting..."
	$(GRADLE) spotlessApply

# Build everything from scratch
all: clean build

# Specific component builds
core:
	@echo "Building core components..."
	$(GRADLE) :core:spt-base:build $(BUILD_OPTS)

extensions:
	@echo "Building all extensions..."
	$(GRADLE) :extensions:load-steps:pipeline:build $(BUILD_OPTS)
	$(GRADLE) :extensions:load-steps:weighted:build $(BUILD_OPTS)
	$(GRADLE) :extensions:storage-drivers:primitives:coop:build $(BUILD_OPTS)
	$(GRADLE) :extensions:storage-drivers:primitives:netty:build $(BUILD_OPTS)
	$(GRADLE) :extensions:storage-drivers:primitives:nio:build $(BUILD_OPTS)
	$(GRADLE) :extensions:storage-drivers:protocols:http:build $(BUILD_OPTS)
	$(GRADLE) :extensions:storage-drivers:protocols:fs:build $(BUILD_OPTS)
	$(GRADLE) :extensions:storage-drivers:implementations:atmos:build $(BUILD_OPTS)
	$(GRADLE) :extensions:storage-drivers:implementations:s3:build $(BUILD_OPTS)
	$(GRADLE) :extensions:storage-drivers:implementations:swift:build $(BUILD_OPTS)

# Install the bundle to local Maven repository
install:
	@echo "Installing bundle to local Maven repository..."
	$(GRADLE) :bundle:publishToMavenLocal

# Create ZIP distribution
dist: $(ZIP_FILE)

$(ZIP_FILE):
	@echo "Creating ZIP distribution..."
	$(GRADLE) :bundle:distZip
	@echo "Distribution ZIP created: $(ZIP_FILE)"
	@ls -lh $(ZIP_FILE) 2>/dev/null || true

# Create VS Code launch configuration
vscode:
	@echo "Creating VS Code launch configuration..."
	$(GRADLE) :bundle:createVSCodeLaunch

# Show project information
info:
	@echo "Spt v5 Bundle Information:"
	@echo "  Version: $(SPT_VERSION)"
	@echo "  Distribution: $(DIST_DIR)"
	@echo "  Main JAR: spt.jar"
	@echo "  Extensions: ext/ directory"
	@echo "  Components: 11 (1 core + 10 extensions)"
	@$(GRADLE) --version

# Run the bundle (for testing)
run: $(SPT_JAR)
	@echo "Running Spt from distribution..."
	cd $(DIST_DIR) && java -jar spt.jar --help

# Gradle wrapper update
update-wrapper:
	@echo "Updating Gradle wrapper..."
	$(GRADLE) wrapper --gradle-version=8.10

# Quick build without tests
quick: 
	@echo "Quick build (no tests)..."
	$(GRADLE) :bundle:build -x test -x spotlessCheck --parallel

# Build with configuration cache report (for debugging)
cache-report:
	@echo "Building with configuration cache report..."
	$(GRADLE) build $(BUILD_OPTS) --configuration-cache-problems=warn

# Docker targets ---------------------------------------------------------------

# Docker registry configuration
DOCKER_REGISTRY := ghcr.io
DOCKER_NAMESPACE := spt
DOCKER_IMAGE := $(DOCKER_REGISTRY)/$(DOCKER_NAMESPACE)/spt
DOCKER_TAG := $(SPT_VERSION)

# Build Docker image
docker: $(SPT_JAR)
	@echo "Building Spt Docker image..."
	$(GRADLE) :bundle:dockerBuildImage
	@echo "Docker image built successfully: $(DOCKER_IMAGE):$(DOCKER_TAG)"
	@echo "Run with: docker run --rm $(DOCKER_IMAGE):latest --help"

# Run Spt in Docker container (interactive)
docker-run: docker
	@echo "Running Spt in Docker container..."
	docker run --rm -it $(DOCKER_IMAGE):latest --help

# Test Docker image with a simple S3 benchmark
docker-test: docker
	@echo "Testing Spt Docker image with S3 benchmark..."
	@echo "Note: Set SPT_S3_ENDPOINT, SPT_S3_ACCESS_KEY, SPT_S3_SECRET_KEY environment variables"
	docker run --rm $(DOCKER_IMAGE):latest \
		--storage-driver-type=s3 \
		--storage-net-node-addrs=$${SPT_S3_ENDPOINT:-your-s3-endpoint:9000} \
		--storage-auth-uid=$${SPT_S3_ACCESS_KEY:-your-access-key} \
		--storage-auth-secret=$${SPT_S3_SECRET_KEY:-your-secret-key} \
		--item-data-size=1MB \
		--load-op-limit-count=10 \
		--load-op-limit-time=10s

# Login to Dell Harbor registry (interactive)
docker-login:
	@echo "Logging into Dell Harbor registry..."
	@echo "Note: Use robot account credentials or personal CLI secret"
	@echo "Robot account format: robot\$$spt+name"
	@docker login $(DOCKER_REGISTRY)

# Push Docker image to Dell registry (automated with credentials)
docker-push: docker
	@echo "Pushing Spt Docker image to Dell registry..."
	@if [ -f .env ] || [ -n "$$HARBOR_ROBOT_USER" ]; then \
		./tools/docker-push-with-auth.sh --push-only; \
	else \
		echo "No credentials found. Using Gradle (requires manual login)..."; \
		echo "Run 'make docker-login' first or set up .env file"; \
		$(GRADLE) :bundle:dockerPushImage; \
	fi

# Build and push with automated authentication
docker-build-push:
	@echo "Building and pushing Docker image with authentication..."
	@./tools/docker-push-with-auth.sh

# Setup Harbor robot account
docker-setup:
	@echo "Harbor Robot Account Setup"
	@echo "=========================="
	@echo ""
	@echo "1. Login to Harbor: https://$(DOCKER_REGISTRY)"
	@echo "2. Navigate to project: spt"
	@echo "3. Go to 'Robot Accounts' tab"
	@echo "4. Click 'New Robot Account'"
	@echo "5. Name: spt-ci (or your preferred name)"
	@echo "6. Expiration: Set as needed (or never)"
	@echo "7. Permissions: Select 'Push' and 'Pull' for spt"
	@echo "8. Click 'Add' and copy the token (shown only once!)"
	@echo ""
	@echo "9. Create .env file from template:"
	@echo "   cp .env.example .env"
	@echo ""
	@echo "10. Edit .env and add:"
	@echo "    HARBOR_ROBOT_USER=robot\$$spt+spt-ci"
	@echo "    HARBOR_ROBOT_TOKEN=<your-token-here>"
	@echo ""
	@echo "Then use: make docker-build-push"

# Build all Docker images (including testing image)
docker-all:
	@echo "Building all Spt Docker images..."
	$(GRADLE) :bundle:dockerBuildAll
	@echo "All Docker images built successfully"

# Run Docker image with a scenario file
docker-scenario: docker
	@if [ -z "$(SCENARIO)" ]; then \
		echo "Error: SCENARIO variable not set. Usage: make docker-scenario SCENARIO=path/to/scenario.js"; \
		exit 1; \
	fi
	@echo "Running Spt with scenario: $(SCENARIO)"
	docker run --rm \
		-v $(shell pwd):/workspace:ro \
		-v $(shell pwd)/logs:/home/spt/log \
		$(DOCKER_IMAGE):latest \
		--run-scenario=/workspace/$(SCENARIO)

# Run Docker image with built-in weighted scenario
docker-weighted: docker
	@echo "Running Spt with weighted load scenario..."
	@mkdir -p logs
	docker run --rm \
		-v $(shell pwd)/logs:/home/spt/log \
		$(DOCKER_IMAGE):latest \
		--run-scenario=/opt/spt/scenarios/js/types/weighted.js \
		--storage-driver-type=s3 \
		--storage-net-node-addrs=$${SPT_S3_ENDPOINT:-your-s3-endpoint:9000} \
		--storage-auth-uid=$${SPT_S3_ACCESS_KEY:-your-access-key} \
		--storage-auth-secret=$${SPT_S3_SECRET_KEY:-your-secret-key}

# List available built-in scenarios
docker-scenarios: docker
	@echo "Available built-in scenarios:"
	@docker run --rm $(DOCKER_IMAGE):latest \
		sh -c 'find $$SPT_SCENARIOS -name "*.js" -o -name "*.groovy" -o -name "*.py" | sort'

# Run Spt with Docker Compose
docker-compose-run:
	@echo "Running Spt with Docker Compose..."
	@echo "Note: Set SPT_S3_ENDPOINT, SPT_S3_ACCESS_KEY, SPT_S3_SECRET_KEY, SPT_S3_BUCKET environment variables"
	cd bundle && docker-compose up spt-basic

# Run custom scenario with Docker Compose
docker-compose-custom:
	@echo "Running custom scenario with Docker Compose..."
	cd bundle && docker-compose up spt-custom

# Run weighted scenario with Docker Compose
docker-compose-weighted:
	@echo "Running weighted scenario with Docker Compose..."
	cd bundle && docker-compose up spt-weighted
