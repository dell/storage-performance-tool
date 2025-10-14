# Spt - Storage Performance Testing Tool


## Overview

Spt is a powerful distributed storage performance testing tool designed to validate and measure the performance of storage systems at scale. Originally developed to test object storage systems, Spt has evolved into a comprehensive performance testing framework supporting multiple storage protocols and complex testing scenarios.

This repository contains the Spt monorepo, consolidating all core components and extensions into a single, cohesive codebase.

## Key Features

- **High Performance**: Leverages Java fibers to sustain millions of concurrent operations
- **Distributed Testing**: P2P architecture enables horizontal scaling across multiple nodes
- **Extensible Architecture**: Plugin-based design supports custom storage drivers and load patterns
- **Comprehensive Metrics**: Detailed performance metrics including latency distributions, throughput, and concurrency
- **Multiple Storage Protocols**: Supports S3, Swift, Atmos, filesystem, and custom protocols
- **Flexible Load Patterns**: Create, read, update, delete operations with partial updates and composite operations
- **Scenario Support**: Complex test scenarios using JavaScript, Python, or Groovy

## Prerequisites

- Java 21 or higher
- Docker (optional, for containerized deployment)
- Git

## Quick Start

### Building from Source

#### Standard Build Method (Recommended)

The project uses Gradle with the Gradle Wrapper for building. This ensures consistent builds across all platforms:

```bash
# Clone the repository
git clone git@eos2git.cec.lab.dell.com:VSSW/spt.git
cd spt

# Build all components using Gradle Wrapper
./gradlew clean build -x test    # Unix/Linux/macOS
# OR
gradlew.bat clean build -x test   # Windows

# The distribution will be available at:
# bundle/build/dist/
#   ├── spt.jar       # Main application JAR
#   ├── ext/              # Extension JARs directory
#   ├── run.sh            # Unix/Linux/macOS launcher
#   └── run.bat           # Windows launcher

# For distribution, a ZIP file is also created:
# bundle/build/distributions/spt-bundle-5.0.0-SNAPSHOT.zip
```

#### Alternative: Using Make (Optional)

For convenience, a Makefile is provided that wraps common Gradle commands:

```bash
# Build the bundle
make build

# Other useful commands
make clean      # Clean build artifacts  
make test       # Run tests
make dist       # Create ZIP distribution
make help       # Show all available targets
```

Note: The Makefile is a convenience wrapper and requires a Unix-like environment. For official builds, CI/CD, and cross-platform compatibility, use the Gradle Wrapper (`./gradlew`).

### Running Your First Test

```bash
# Navigate to the distribution directory
cd bundle/build/dist

# Check version and available extensions
./run.sh --version    # Unix/Linux/macOS
# OR
run.bat --version     # Windows

# Basic S3 write test
./run.sh \
  --storage-driver-type=s3 \
  --storage-net-node-addrs=your-s3-endpoint.com \
  --storage-auth-uid=your-access-key \
  --storage-auth-secret=your-secret-key \
  --item-data-size=1MB \
  --load-op-limit-count=1000
```

Note: The run scripts automatically handle setting up the extensions in the correct location (`~/.spt/5.0.0/ext/`).

### Common Command-Line Options

```bash
# Display version and available extensions
./run.sh --version
./run.sh -v

# Show help and all available options
./run.sh --help

# Run with custom configuration file
./run.sh --run-scenario=mytest.js
```

### Distribution Structure

After building, the distribution contains:

- **spt.jar** - The main application JAR containing core Spt functionality
- **ext/** - Directory containing all extension JARs (storage drivers, load patterns)
- **run.sh/run.bat** - Platform-specific launcher scripts that properly configure the classpath

The launcher scripts handle:
- Setting up the correct extension directory structure
- Linking/copying extensions to `~/.spt/5.0.0/ext/`
- Launching Spt with the proper classpath configuration

### IDE Integration

For development in VS Code:

```bash
# Create VS Code launch configurations
./gradlew :bundle:createVSCodeLaunch
# OR
make vscode
```

This creates debug configurations for:
- Running Spt with `--help`
- Running S3 performance tests
- Checking version with `--version`

### Using Docker

```bash
# Build the Docker image locally
make docker

# Run a simple test
docker run --rm dellspt/spt:latest \
  --storage-driver-type=s3 \
  --storage-net-node-addrs=your-s3-endpoint.com \
  --storage-auth-uid=your-access-key \
  --storage-auth-secret=your-secret-key

# Run with a built-in scenario
docker run --rm dellspt/spt:latest \
  --run-scenario=/opt/spt/scenarios/js/types/weighted.js \
  --storage-driver-type=s3 \
  --storage-net-node-addrs=localhost:9000

# Run with your custom scenario
docker run --rm \
  -v $(pwd):/workspace \
  -v $(pwd)/logs:/home/spt/log \
  dellspt/spt:latest \
  --run-scenario=/workspace/my-scenario.js

# List available built-in scenarios
make docker-scenarios

# Run with a scenario using make
make docker-scenario SCENARIO=example-scenario.js
```

#### Docker Volumes

The Spt Docker image supports several volume mount points:

- `/home/spt/log` - Log files output directory
- `/workspace` - Custom scenarios and data files
- `/data` - Input/output data files

Example with all volumes:
```bash
docker run --rm \
  -v $(pwd)/scenarios:/workspace \
  -v $(pwd)/logs:/home/spt/log \
  -v $(pwd)/data:/data \
  dellspt/spt:latest \
  --run-scenario=/workspace/my-test.js
```

#### Docker Compose

A `docker-compose.yml` example is provided in the `bundle/` directory that includes:
- Multiple Spt service configurations
- Volume mounts for scenarios and logs
- Environment variable support for storage configuration
- Example `.env.example` file for easy setup

```bash
# Quick setup using the provided example
cd bundle
cp .env.example .env
# Edit .env with your S3 credentials
docker-compose up spt-basic

# Or set environment variables directly
export SPT_S3_ENDPOINT=your-s3-endpoint:9000
export SPT_S3_ACCESS_KEY=your-access-key
export SPT_S3_SECRET_KEY=your-secret-key
export SPT_S3_BUCKET=your-bucket-name
make docker-compose-run

# Run custom scenario
make docker-compose-custom

# Run weighted load test
make docker-compose-weighted
```

### S3 Configuration

To run Spt against an S3-compatible storage system, you need to configure the following environment variables:

#### Required Environment Variables

```bash
# S3-compatible endpoint (without http:// prefix)
export SPT_S3_ENDPOINT=<your-endpoint>:9000

# S3 access credentials
export SPT_S3_ACCESS_KEY=<your-access-key>
export SPT_S3_SECRET_KEY=<your-secret-key>

# Target bucket name
export SPT_S3_BUCKET=<your-bucket-name>
```

#### Example Configuration

For AWS S3:
```bash
export SPT_S3_ENDPOINT=s3.us-east-1.amazonaws.com
export SPT_S3_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE
export SPT_S3_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export SPT_S3_BUCKET=my-test-bucket
```

For MinIO or other S3-compatible storage:
```bash
export SPT_S3_ENDPOINT=minio.example.com:9000
export SPT_S3_ACCESS_KEY=minioadmin
export SPT_S3_SECRET_KEY=minioadmin
export SPT_S3_BUCKET=test-bucket
```

#### Security Note

Never commit credentials to version control. Consider using:
- Environment variable files (add to .gitignore)
- Secret management tools
- CI/CD secret variables

## Project Structure

```
spt/
├── core/
│   └── spt-base/          # Core functionality and APIs
├── extensions/
│   ├── load-steps/            # Load pattern extensions
│   │   ├── pipeline/          # Sequential operations
│   │   └── weighted/          # Weighted random operations
│   └── storage-drivers/       # Storage protocol implementations
│       ├── primitives/        # Base driver implementations
│       ├── protocols/         # Protocol-specific drivers
│       └── implementations/   # Storage-specific drivers
├── bundle/                    # Bundle aggregator configuration
└── docs/                      # Additional documentation
```

## Documentation

- [Getting Started Guide](core/spt-base/doc/getstarted/README.md)
- [Architecture Overview](core/spt-base/doc/design/architecture/README.md)
- [Configuration Reference](core/spt-base/doc/usage/input/configuration/README.md)
- [Scenarios Guide](core/spt-base/doc/usage/input/scenarios/README.md)
- [API Documentation](core/spt-base/doc/usage/api/remote/README.md)

### Health & Readiness Endpoints

- `GET /health` — liveness probe; returns 200 with `{status:"ok", scope:"node", role, node_id[, cluster_id]}`.
- `GET /ready` — readiness probe; returns 503 while starting, 200 once services are ready. Body includes `{ready:bool, status:"starting|ready", ...}`.

These endpoints are useful for orchestrators and for spt to simplify startup logic. See full details in the API docs above.

## Supported Storage Types

- **S3**: Amazon S3 and S3-compatible storage (MinIO, Ceph, etc.)
- **Swift**: OpenStack Swift
- **Atmos**: Dell Dell Atmos
- **Filesystem**: Local or network filesystems
- **Custom**: Extensible framework for custom storage drivers

## Contributing

We welcome contributions! Please see our [Contributing Guide](core/spt-base/doc/contributing/README.md) for details on how to get started.

## License

See the [LICENSE](core/spt-base/LICENSE) file for license rights and limitations.

## Support

- [Documentation](core/spt-base/doc/README.md) - Comprehensive documentation

## Migration Notes

This is the monorepo version of Spt, consolidating multiple repositories into a single codebase. For information about the migration, see:
- [MONOREPO.md](MONOREPO.md) - Monorepo structure and rationale
