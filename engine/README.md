# SPT Engine

## Overview

The SPT Engine is the high-performance Java core of the Dell Storage Performance Tool. It executes storage workloads, collects metrics, and supports distributed testing across multiple nodes. The engine is typically managed by the [SPT CLI](../README.md), but can also be run standalone for advanced use cases.

This directory contains the engine source, extensions (storage drivers, load patterns), and Docker bundle configuration.

## Key Features

- **High Performance**: Leverages Virtual Threads to sustain millions of concurrent operations
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
git clone https://github.com/dell/storage-performance-tool.git
cd storage-performance-tool/engine

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
# bundle/build/distributions/spt-bundle-<version>.zip
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

Note: The run scripts automatically handle setting up the extensions in the correct location (`~/.spt/<version>/ext/`).

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

- **spt.jar** - The main application JAR containing core SPT functionality
- **ext/** - Directory containing all extension JARs (storage drivers, load patterns)
- **run.sh/run.bat** - Platform-specific launcher scripts that properly configure the classpath

The launcher scripts handle:
- Setting up the correct extension directory structure
- Linking/copying extensions to `~/.spt/<version>/ext/`
- Launching SPT with the proper classpath configuration

### IDE Integration

For development in VS Code:

```bash
# Create VS Code launch configurations
./gradlew :bundle:createVSCodeLaunch
# OR
make vscode
```

This creates debug configurations for:
- Running SPT with `--help`
- Running S3 performance tests
- Checking version with `--version`

### Using Docker

```bash
# Build the Docker image locally
make docker

# Run a simple test
docker run --rm ghcr.io/dell/storage-performance-tool:latest \
  --storage-driver-type=s3 \
  --storage-net-node-addrs=your-s3-endpoint.com \
  --storage-auth-uid=your-access-key \
  --storage-auth-secret=your-secret-key

# Run with a built-in scenario
docker run --rm ghcr.io/dell/storage-performance-tool:latest \
  --run-scenario=/opt/spt/scenarios/js/types/weighted.js \
  --storage-driver-type=s3 \
  --storage-net-node-addrs=localhost:9000

# Run with your custom scenario
docker run --rm \
  -v $(pwd):/workspace \
  -v $(pwd)/logs:/home/spt/log \
  ghcr.io/dell/storage-performance-tool:latest \
  --run-scenario=/workspace/my-scenario.js

# List available built-in scenarios
make docker-scenarios

# Run with a scenario using make
make docker-scenario SCENARIO=example-scenario.js
```

#### Docker Volumes

The SPT Docker image supports several volume mount points:

- `/home/spt/log` - Log files output directory
- `/workspace` - Custom scenarios and data files
- `/data` - Input/output data files

Example with all volumes:
```bash
docker run --rm \
  -v $(pwd)/scenarios:/workspace \
  -v $(pwd)/logs:/home/spt/log \
  -v $(pwd)/data:/data \
  ghcr.io/dell/storage-performance-tool:latest \
  --run-scenario=/workspace/my-test.js
```

#### Docker Compose

A `docker-compose.yml` example is provided in the `bundle/` directory that includes:
- Multiple SPT service configurations
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

To run SPT against an S3-compatible storage system, you need to configure the following environment variables:

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

For Dell ObjectScale (ECS) with multiple S3 endpoints:
```bash
# Comma-separated list of data node addresses
export SPT_S3_ENDPOINT=http://ecs-node1:9020,http://ecs-node2:9020,http://ecs-node3:9020
export SPT_S3_ACCESS_KEY=your-objectscale-access-key
export SPT_S3_SECRET_KEY=your-objectscale-secret-key
export SPT_S3_BUCKET=perf-test
```

#### Security Note

Never commit credentials to version control. Consider using:
- Environment variable files (add to .gitignore)
- Secret management tools
- CI/CD secret variables

## Project Structure

```
engine/
├── core/
│   └── spt-base/              # Core functionality and APIs
├── extensions/
│   ├── load-steps/            # Load pattern extensions
│   │   ├── pipeline/          # Sequential operations
│   │   └── weighted/          # Weighted random operations
│   └── storage-drivers/       # Storage protocol implementations
│       ├── primitives/        # Base driver implementations
│       ├── protocols/         # Protocol-specific drivers
│       └── implementations/   # Storage-specific drivers (S3, S3-RDMA, etc.)
├── bundle/                    # Docker bundle and distribution packaging
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
- **Atmos**: Dell Atmos
- **Filesystem**: Local or network filesystems
- **Custom**: Extensible framework for custom storage drivers

## S3-RDMA Acceleration

The S3-RDMA storage driver provides an optional RDMA (Remote Direct Memory Access) data path for S3 PUT and GET operations. When enabled, object data bypasses the kernel networking stack entirely — the storage server reads from and writes to the client's memory directly over the RDMA fabric.

### How It Works

SPT acts as an **RDMA target**: it registers a memory buffer, creates a DC (Dynamically Connected) Target endpoint, and sends a standard S3 HTTP request with an additional `x-amz-rdma-token` header. The storage server uses the token to perform the actual RDMA data transfer.

| S3 Operation | RDMA Action |
|-------------|-------------|
| PUT (write) | Server performs **RDMA READ** from client memory |
| GET (read)  | Server performs **RDMA WRITE** to client memory |

Because the server initiates the data transfer, the HTTP request body is empty and **`Content-Length` is set to 0**. The actual object size is encoded in the RDMA token. This is critical for compatibility with HTTP frameworks (e.g. Jetty) that block until the declared body bytes arrive.

### RDMA Token Format

The `x-amz-rdma-token` header carries all information the server needs to perform the transfer:

```
addr:size:rkey:lid:dctn:g:gid
```

| Field  | Hex Digits | Description |
|--------|-----------|-------------|
| `addr` | 16 | Virtual address of the registered buffer |
| `size` | 8  | Buffer size in bytes |
| `rkey` | 8  | Remote key for RDMA memory access |
| `lid`  | 4  | Local ID (0 for RoCE, used in InfiniBand) |
| `dctn` | 6  | DC Target Number (connection endpoint) |
| `g`    | 1  | Global addressing flag (1 for RoCE) |
| `gid`  | 32 | Global ID for RoCE routing |

Example token for a 1 MB buffer:
```
00007fffc3200000:00100000:00004d16:0000:00133f:1:fe80000000000000000000000012ab34
```

### Architecture

The driver is implemented in three layers:

- **`S3RdmaStorageDriver`** (Java) — extends `S3StorageDriver`, overrides `submit()` to route operations above a configurable size threshold to the RDMA path. Falls back to `super.submit()` for small objects or when RDMA is unavailable.
- **`RdmaTransport`** (Java) — JNI bridge that manages buffer registration, token generation, and native lifecycle.
- **`libspt_rdma.so`** (`rdma_native.c`, ~675 lines of C) — native implementation using `libibverbs`, `libmlx5`, and `librdmacm`. Handles device initialization, DC Target creation, memory registration, and token formatting.

### Requirements

- **Linux only** (RDMA stack depends on `libibverbs` / kernel verbs)
- RDMA-capable NIC (NVIDIA/Mellanox ConnectX-4 or newer)
- An S3-compatible storage target with RDMA support (e.g. Dell ObjectScale / ECS)
- `rdma-core` system packages (`libibverbs`, `librdmacm`, `libmlx5`)
- Docker device passthrough (`--device /dev/infiniband`) for containerized deployments

On systems without RDMA hardware, the driver detects this at initialization and fails by default. Set `storage.rdma.fallback` to `true` to fall back to HTTP instead.

### Engine Configuration

RDMA settings are passed through the scenario YAML under the `storage.rdma` namespace:

| Key | Default | Description |
|-----|---------|-------------|
| `storage.rdma.thresholdBytes` | `1048576` | Minimum object size (bytes) for RDMA transfer. Set to `0` to use RDMA for all sizes. |
| `storage.rdma.timeoutMs` | `30000` | RDMA operation timeout in milliseconds |
| `storage.rdma.device` | `auto` | RDMA device name or `auto` for auto-detection |
| `storage.rdma.fallback` | `false` | Fall back to HTTP if RDMA initialization fails |

## Contributing

We welcome contributions! Please see our [Contributing Guide](core/spt-base/doc/contributing/README.md) for details on how to get started.

## License

See the [LICENSE](core/spt-base/LICENSE) file for license rights and limitations.

## Support

- [Documentation](core/spt-base/doc/README.md) - Comprehensive documentation

