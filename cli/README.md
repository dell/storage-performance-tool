# spt - Spt CLI Wrapper

A modern command-line interface wrapper for the Spt benchmarking tool, designed to simplify S3-compatible storage performance testing.

## Overview

`spt` provides a user-friendly interface to execute various benchmark tests against S3-compatible storage endpoints. It wraps the Spt benchmarking tool with an intuitive CLI similar to Docker's command structure and includes both an interactive TUI (Terminal User Interface) and a headless mode for automated environments.

## Quick Start

```bash
# Build the tool
git clone https://github.com/dell/storage-performance-tool.git
cd storage-performance-tool/cli
make build

# Run a simple mock benchmark (no S3 endpoint required!)
./spt run mock --duration 30s --threads 4
```

**TUI Navigation Tips:**
- Press **'g'** to show/hide the performance graphs (they start hidden)
- Press **'m'** to show/hide the spt messages window (also starts hidden)
- Press **TAB** to switch between viewports
- Use **arrow keys** or **j/k** to scroll
- Press **'q'** or **Ctrl+C** to quit

The mock mode is perfect for demos and testing without any storage setup. You'll see real-time performance data once you press 'g' to reveal the charts!

Once comfortable with the tool, try a real S3 benchmark:

```bash
# Run a write benchmark against S3
./spt run write \
  --endpoints http://your-s3:9000 \
  --access-key your-key \
  --secret-key your-secret \
  --bucket test-bucket \
  --duration 1m \
  --threads 8 \
  --object-size 1MB
```

Need to profile how fast an existing namespace can be enumerated? The list workload reuses the same credential handling but never creates or deletes data:

```bash
# Run a list benchmark against S3 and stop after two minutes
./spt run list \
  --endpoints https://s3.example.com \
  --access-key "$S3_ACCESS_KEY" \
  --secret-key "$S3_SECRET_KEY" \
  --bucket analytics-data \
  --prefix reports/2025/ \
  --threads 4 \
  --auto-terminate-seconds 120
```

Tip: If you omit both `--object-count` and `--duration`, the list workload will continue until you stop it. Always set `--auto-terminate-seconds` for unattended runs so CI jobs do not hang.
Authentication defaults to Signature Version 4. Add `--auth-version 2` only when working with a legacy S3 implementation that rejects SigV4.

## Features

- **Intuitive CLI**: Docker-style command structure (`spt run`, `spt replay`, `spt results`)
- **Multiple Workload Types**: Support for write, read, list, mixed, and mock operations today; delete benchmarking remains on the roadmap
- **Dual Execution Modes**:
  - **Interactive TUI**: Built-in terminal interface for monitoring benchmark progress
  - **Headless Mode**: Non-interactive mode for CI/CD, scripting, and automated environments
- **Real-time Performance Visualization**: Dual synchronized ASCII bar charts showing ops/sec and latency
- **Historical Data Navigation**: Interactive time-based navigation through performance samples
- **Auto-Detection**: Automatically switches to headless mode when no TTY is available
- **Trace File Support**: Comprehensive output capture for debugging and analysis
- **Flexible Configuration**: Extensive command-line options for customizing tests
- **SigV4-First Auth**: Defaults to Signature Version 4 while allowing an opt-in downgrade for legacy V2-only targets
- **Comprehensive Logging**: Configurable file-based logging for debugging
- **Infrastructure Verification**: Pre-flight checks for distributed testing nodes
- **Replay Archived Workloads**: Import archived SPT or legacy Mongoose artifacts and replay the equivalent S3 workload against a current target
- **S3 Compatible**: Works with any S3-compatible storage endpoint

## Installation

### Prerequisites

- Go 1.25 or higher
- Docker (Docker daemon must be running)
- Local CI checks: golangci-lint v2.2+ (required for `make ci-local`).

## Lint Policy

We lint production and test code differently to keep tests practical while maintaining strong quality for shipped code.

- Production code: `govet`, `staticcheck`, and `errcheck` are enforced.
- Tests (`*_test.go`): only `unused` is enforced; other linters are intentionally relaxed to avoid noisy failures in mocks and scaffolding.

Run locally:

```bash
make ci-local        # includes two-pass linter enforcement
```

Notes:

- `golangci-lint` v2.2.0 or newer is required. The `ci-local` script checks the version and fails with upgrade instructions if outdated.
- You can still run `make lint`, but `ci-local` is the source of truth for the two‑pass policy.

Dependency management: this project does not vendor dependencies. Builds and tests use Go modules via the module proxy. Ensure your build environment has network access to fetch modules (or a warmed module cache). If your environment requires a proxy mirror, set `GOPROXY` accordingly.

### Docker Requirements

spt requires Docker to be installed and running. The tool uses the Spt container image (`ghcr.io/dell/storage-performance-tool`) to execute benchmarks.

If you encounter Docker-related errors:
- Verify Docker is installed: `docker --version`
- Check Docker daemon is running: `docker ps`
- Ensure you have permission to run Docker commands

**Note on Port Conflicts:** Spt uses port 9999 (Spt standard) for its API. If you see port conflict errors, use the `--force` flag to automatically resolve them, or manually stop any existing Spt containers with `docker stop <container-id>`. For backward compatibility, you can specify a custom API port with `--api-port`.

### Building from Source

```bash
git clone https://github.com/dell/storage-performance-tool.git
cd storage-performance-tool/cli
make build
```

### Installing

```bash
make install
```

This will install the `spt` binary to your `$GOPATH/bin` directory.

## Distributed Testing Support

### Infrastructure Verification

Use the `spt verify` command to validate your infrastructure readiness:

```bash
# Verify local machine (localhost) - perfect for getting started!
spt verify

# Verify all nodes are ready for distributed testing
spt verify --test-hosts "node1,node2,node3"

# Verify with specific user credentials
spt verify --test-hosts "root@server1,root@server2,root@server3"

# Allow partial readiness (at least 2 of 3 nodes must pass)
spt verify --test-hosts "node1,node2,node3" --min-hosts 2

# Automatically clean up conflicting containers
spt verify --test-hosts "node1,node2,node3" --force-cleanup
```

The verification process checks each node for:
- SSH connectivity (for remote nodes)
- Docker daemon availability
- Spt container startup capability
- Ports required for distributed runs:
  - RMI Registry: 1099
  - REST API: 9999 (Spt standard)
  - RMI object range: 40000–40009 (10 ports, configurable via `--rmi-port-start/--rmi-port-count`)
- REST API endpoint functionality
- Clean container shutdown

Preflight details (2025‑09):
- Shared logic: `spt verify` and the multi‑host orchestrator share the same preflight checks (Docker, image, ports).
- Image ensure: verify ensures the Spt image exists on each host (pulls if missing).
- Remote Docker: remote hosts are accessed via SSH + Docker CLI (not the Docker SDK `ssh://` transport).

If existing Spt containers are found on ports 1099 or 9999, the tool will:
- In interactive mode: Prompt for resolution (cleanup, skip, retry, abort)
- With `--force-cleanup`: Automatically remove conflicting containers
- Report which containers are blocking ports

### Attaching Prestarted Worker Nodes

Some operator workflows pre-launch Spt worker containers (for example with `--run-node`) and want `spt` to attach without touching those workers. Use the `--attach-existing` flag when running in multi-host mode:

```bash
# entry node first, followed by one or more workers
./spt run write \
  --test-hosts entry,worker1,worker2 \
  --attach-existing \
  --threads 8 \
  --object-size 1MB \
  --object-count 2000
```

Key points:
- `spt` still launches and manages the entry node container; only the worker nodes are reused.
- Workers must already be running the Spt API (`--run-node`) and listening on the standard API (9999) and RMI (1099 plus the configured range) ports.
- Preflight checks tolerate expected port usage on worker hosts but continue to flag unknown listeners.
- Attached workers remain unmanaged during shutdown—`spt` stops the entry node but leaves prestarted workers running.
- The host list must include at least two entries (entry + worker). `spt` enforces this and fails fast otherwise.

## Usage

### Interactive Mode (Default with TTY)

When run in a terminal with TTY support, spt displays an interactive interface:

```bash
# Run a mock benchmark with interactive TUI (no S3 required!)
spt run mock --duration 30s --threads 8
# Remember to press 'g' to see the performance graphs!

# Run a write benchmark with interactive TUI
spt run write \
  --endpoints http://minio:9000 \
  --access-key your-access-key \
  --secret-key your-secret-key \
  --bucket test-bucket \
  --duration 5m
```

### Headless Mode (Automatic in CI/CD)

When no TTY is available (CI/CD, scripts, etc.), spt automatically runs in headless mode:

```bash
# Auto-detects headless mode in CI/CD environments
spt run write \
  --endpoints http://minio:9000 \
  --access-key your-access-key \
  --secret-key your-secret-key \
  --bucket test-bucket \
  --duration 5m

# Force headless mode explicitly
spt run mock --headless --duration 30s

# Headless mode with trace file for debugging
spt run write \
  --endpoints http://minio:9000 \
  --access-key your-access-key \
  --secret-key your-secret-key \
  --bucket test-bucket \
  --duration 5m \
  --trace-file benchmark-trace.log

# Mock benchmark with detailed output
spt run mock --duration 1m --threads 16 --verbose --trace-file mock-test.log
```

### Advanced Usage

```bash
# Run with custom settings and logging
spt run write \
  --endpoints http://minio:9000 \
  --access-key your-access-key \
  --secret-key your-secret-key \
  --bucket test-bucket \
  --threads 16 \
  --object-size 1MB \
  --object-count 10000 \
  --debug \
  --trace-file detailed-trace.log

# Target multiple S3 endpoints (round‑robin across addrs)
spt run write \
  --endpoints http://s3a:9000,http://s3b:9000 \
  --access-key your-access-key \
  --secret-key your-secret-key \
  --bucket test-bucket \
  --threads 32 \
  --duration 10m

# Distributed run with endpoint slicing (each node gets a subset)
spt run write \
  --endpoints http://s3a:9000,http://s3b:9000,http://s3c:9000 \
  --slice-endpoints \
  --test-hosts node1,node2,node3 \
  --access-key your-access-key \
  --secret-key your-secret-key \
  --bucket test-bucket \
  --threads 32 \
  --duration 10m

## Multi‑Endpoint Usage

When your S3 system exposes multiple front‑end endpoints, you can tell spt to target all of them in a single run.

- Syntax: use `--endpoints url1,url2` (CSV) or repeat the flag: `--endpoints url1 --endpoints url2`.
- Uniform scheme: all URLs must be `http` or all `https`.
- Ports:
  - If all endpoints share the same port, spt sets a common `node.port` and lists hosts only.
  - If ports differ, spt embeds `host:port` for each address.
- Distributed runs: add `--slice-endpoints` so each Spt node targets a subset of the endpoints (partitioning), reducing cross‑traffic and contention.
- Environment: set `S3_ENDPOINTS="http://s3a:9000,http://s3b:9000"` as a convenient default. Precedence for endpoints is `--endpoints` > `S3_ENDPOINTS` > `S3_ENDPOINT`. Use `S3_AUTH_VERSION` (default `4`) only when you must force Signature Version 2 for legacy storage.

Examples:

```bash
# Simple multi‑endpoint write
spt run write \
  --endpoints http://s3a:9000,http://s3b:9000 \
  --access-key X --secret-key Y --bucket bench --threads 32 --duration 10m

# Distributed with slicing
spt run write \
  --endpoints http://s3a:9000,http://s3b:9000,http://s3c:9000 \
  --slice-endpoints \
  --test-hosts node1,node2,node3 \
  --access-key X --secret-key Y --bucket bench --threads 32 --duration 10m
```

# Generate scenario file without execution
```bash
spt run write \
  --endpoints http://minio:9000 \
  --access-key your-access-key \
  --secret-key your-secret-key \
  --bucket test-bucket \
  --duration 5m \
  --generate-only
```

### Command Reference

#### Global Flags

These flags are available for all commands:

- `--debug`: Run in debug mode (alias for --log-level debug)
- `--log-level string`: Set logging level (debug, info, warn, error) - default: "info"
- `--log-file string`: Specify log file path - default: "spt.log"
- `--log-append`: Append to existing log file (default is to create new)

#### `spt run <type>`

Executes a benchmark test with the specified workload type.

**Workload Types:**

- `write`: Write-only test, creating new objects
- `read`: Read-only test on pre-existing objects
- `mixed`: Concurrent GET/PUT/DELETE/STAT with weighted distribution
- `delete`: Test to measure object deletion performance (coming soon)
- `mock`: Run tests with dummy-mock driver (no S3 endpoint required)

**Required Flags (for S3 workloads, optional for mock):**

- Connection: `--endpoints, -e <url[,url,...]>` — one or more S3 endpoint URLs (comma-separated or repeatable). Supplying a single value is fine for the common single-host case.
- Credentials and bucket:
  - `--access-key, -a`: S3 access key credential
  - `--secret-key, -s`: S3 secret key credential
  - `--bucket, -b`: Target bucket for the test

**Workload Definition (one required):**

- `--object-count, -n`: Fixed number of objects to process
- `--duration, -d`: Fixed time duration (e.g., 5m, 1h)

**Optional Flags:**

- `--threads, -t`: Number of parallel client threads (default: 1)
- `--object-size, -o`: Size of each object (e.g., 1MB, 256KB, 4GB)
- `--part-size`: Enable S3 multipart upload with the given part size (e.g., 5MB, 64MB). The engine schedules multipart objects and parts safely without requiring a `load.batch.size` override. Individual parts are retried automatically (up to 3 times) and incomplete uploads are aborted on failure. Per-part checksums are applied when checksum is enabled. See [`SPT_SYNTAX.md`](docs/SPT_SYNTAX.md) for details
- `--checksum`: Enable S3 checksum validation with the specified algorithm: `crc32`, `crc32c`, `sha1`, `sha256`, `crc64-nvme`. When used with `--part-size`, checksums are applied per part. (env: `SPT_CHECKSUM`)
- `--object-data-compressibility`: Target compressibility percentage for generated object data, 0-100 (default: 0 = fully random). Each 4KB chunk is split into random and zero-filled portions according to the percentage. (env: `SPT_OBJECT_DATA_COMPRESSIBILITY`)
- `--object-data-dedupable`: Whether generated data remains dedupe-friendly (default: true). Set `false` to stamp every 4KB with a unique object-id + offset header that defeats inline deduplication. Incompatible with `--items-file` / file-based data input. (env: `SPT_OBJECT_DATA_DEDUPABLE`)
- `--cleanup`: Delete all created objects after test completion
- `--create-prefix`: Ensure target prefix exists before testing
- `--output-dir, -O`: Directory to save detailed Spt reports
- `--generate-only`: Generate scenario file without executing Docker
- `--force`: Automatically resolve port conflicts without user interaction. Spt uses port 9999 for its API - if another Spt instance is running, this flag will automatically stop it before starting the new test
- `--api-port`: Specify custom Spt API port (defaults to 9999, legacy: 43234)
- `--spt-image`: Override the engine image ref. By default, release builds use an image tag matching the CLI version (for example, `...:v5.10.3`) and local/dev builds use `...:spt_dev`.
- `--skip-image-pull`: Use the locally cached Spt image instead of pulling before each run. Dev images such as `spt_dev` automatically skip pulls because they are local-only.
- `--keep-scenario`: Keep the generated JavaScript scenario file after test completes (useful for debugging)

Multi-endpoint options:
- `--endpoints`: Comma-separated list (or repeat the flag) to target multiple S3 endpoints.
- `--slice-endpoints`: In distributed runs, partition the endpoint list across Spt nodes instead of having every node target all endpoints.

**Headless Mode Flags:**

- `--headless`: Force headless (non-interactive) mode even with TTY
- `--trace-file`: Save all output to specified trace file
- `--trace-append`: Append to existing trace file (default: overwrite)
- `--verbose`: Show detailed Docker API calls and debug information

#### `spt replay`

Imports archived SPT or legacy Mongoose workload artifacts from a result-folder
URL, remaps the workload to your current S3 target configuration, and launches
an equivalent replay workload.

```bash
spt replay \
  --from 'https://archive.example.com/results/2031/result.2031-04-05.06:07:08/' \
  --endpoints https://s3.example.com \
  --access-key "$S3_ACCESS_KEY" \
  --secret-key "$S3_SECRET_KEY" \
  --bucket replay-bucket \
  --test-hosts worker1,worker2,worker3 \
  --headless
```

Use `--generate-only --output-dir ./replay-preview` to inspect the generated
scenario, defaults, metadata, warnings, and command transformations without
launching containers. RDMA replay launch is not implemented yet; `--s3-driver
rdma` is limited to generate-only inspection. See [`REPLAY.md`](docs/REPLAY.md)
for source archive requirements, supported transformations, limitations, and
troubleshooting.

#### `spt verify`

Pre-flight verification of distributed testing infrastructure. Ensures all specified nodes are ready for coordinated Spt testing.

**Required Flags:**

- `--test-hosts`: Comma-separated list of hosts to verify (e.g., "node1,node2" or "user@host1,user@host2")

**Optional Flags:**

- `--min-hosts`: Minimum number of hosts that must pass verification (default: all)
- `--network-mode`: Docker network mode - "host" (default) or "bridge". Host networking is required for Java RMI distributed communication.
- `--rmi-port-start`: Starting port for RMI object port range verification (default: 40000)
- `--rmi-port-count`: Number of RMI ports to verify (default: 10)
- `--force-cleanup`: Automatically clean up conflicting containers without prompting

**Example Output:**
```
═══ Spt Distributed Testing Verification ═══
Nodes tested: 3
Time: 12.5s

Node: node1.example.com
✅ SSH Connectivity: SSH connection successful (0.2s)
✅ Docker Available: Docker 28.3.3-ce accessible via SSH (0.1s)
✅ Container Start: Container started: abc123def456 (2.5s)
✅ Ports Accessible: Essential service ports accessible (1099 RMI registry, 9999 REST API) (0.3s)
✅ Metrics Endpoint: REST API responsive (0.2s)
✅ Control Endpoint: Control API functional (0.1s)
✅ Container Cleanup: Container removed successfully (0.5s)

Node: node2.example.com
[similar output...]

═══ Summary ═══
✅ READY: 3 of 3 nodes passed verification
You can run distributed tests with these nodes.
```

**Port Conflict Resolution:**

When the verify command detects existing Spt containers on required ports, it provides options:

1. **Cleanup** - Remove conflicting containers and continue
2. **Skip** - Mark this host as failed and continue with others
3. **Retry** - Re-check after manual intervention
4. **Abort** - Cancel entire verification

Use `--force-cleanup` to automatically choose cleanup without prompting.

### Remote Multi‑Node Networking

**Host networking is required** for distributed testing due to Java RMI's local-host security restrictions. Bridge networking fails because RMI rejects registry operations from Docker's bridge gateway IP (`172.17.0.1`) as "non-local". See `mike/planning/RMI_HOST_NETWORKING_REQUIREMENT.md` for technical details.

- Workers and the entry node run with `--network host` (the default since v5.1.0).
- spt detects a routable "advertised" IP per worker (default‑route IP) and sets both:
  - `JAVA_OPTS=-Djava.rmi.server.hostname=<ip>`
  - `JAVA_TOOL_OPTIONS=-Djava.rmi.server.hostname=<ip>`
- The entry node receives `--load-step-node-addrs=<ip:1099,...>` built from the detected worker IPs.
- Required open ports between nodes: 1099 (RMI registry) and 9999 (REST API). Ensure firewalls allow TCP connectivity between the PRIMARY and all workers.

**Readiness + Metrics Handshake**

- `spt` does not start the workload until every node returns HTTP 200 from `/ready`; the status payload is recorded in debug logs together with `/health` identity data.
- Once the entry node is ready you should see the idle JSON metrics sample shortly thereafter (typically <1 s). If metrics lag, check `/metrics/json?verbose=1` and the node logs.

Environment override: if detection is flaky on specific hosts, set `ADVERTISED_IPS` to a comma‑separated mapping like `ADVERTISED_IPS="workerA=10.0.0.11,root@workerB=10.0.0.12"`. When present, spt uses the mapped IPv4 for those hosts instead of auto‑detecting.

Single‑node local runs use host networking by default (same as distributed). For local development, optionally set `SPT_LOCAL_RMI_LOOPBACK=1` to force the JVM's advertised RMI hostname to loopback.

Quick start (distributed):

```bash
# Verify connectivity and Docker on all nodes
spt verify --test-hosts "primary,worker1,worker2" --min-hosts 3

# Run a distributed mock test (entry on first host, workers on the rest)
spt run mock --test-hosts "primary,worker1,worker2" --threads 8 --duration 5m

# Run a distributed S3 write test
spt run write --test-hosts "primary,worker1,worker2" \
  --endpoints http://minio:9000 --access-key KEY --secret-key SECRET --bucket bench \
  --threads 8 --duration 5m --object-size 1MB
```

Troubleshooting:
- If worker logs show `AccessException: Registry.rebind disallowed; origin /172.17.x.x is non-local host`, it indicates an incorrect advertised address. spt’s host‑networking + detection eliminates this; re‑run `spt verify` and check firewalls.
- Validate endpoints quickly:
  - `curl -I http://<worker>:9999/run` → 200 or 204
  - `curl -s http://<worker>:9999/metrics/json?verbose=1 | jq '.'` → non‑empty array during runs
  - `curl -I http://<primary>:9999/run` → 200 or 204

### Helper Script

- `tools/testmulti.sh`: Example multi‑node write run. Includes `--force` and `--verbose` to clean conflicts and surface details. Edit `HOSTS`, S3 endpoint, and credentials at the top before use.
- `tools/stopmulti.sh`: Stops Spt‑like containers across multiple hosts (SSH or local). Usage: `tools/stopmulti.sh --hosts "root@host1,root@host2,127.0.0.1" [--dry-run] [--parallel]`.

#### `spt results` (Coming Soon)

Lists and inspects past benchmark results. This feature is under development.

## Development

### Project Structure

``` text
cli/
├── cmd/                  # Command implementations
├── tui/                  # Terminal UI components
├── internal/
│   ├── logging/          # Logging utilities
│   ├── scenario/         # JavaScript scenario generation
│   └── portcheck/        # Port conflict resolution
├── headless/             # Headless mode implementation
├── tools/                # Test scripts and utilities
├── main.go               # Entry point
├── go.mod                # Go module definition
└── Makefile              # Build automation
```

### Running Tests

```bash
make test          # Run all tests
make test-coverage # Run tests with coverage report
make ci-local      # Run local CI-like checks (tidy, fmt-check, vet, build, optional lint, compile tests)
```

## Execution Modes

### Interactive Mode (TUI)

When run in a terminal with TTY support, spt displays an interactive interface with multiple viewports:

**Layout:**

- **Top Viewport**: Spt container output (logs only)
- **Middle Viewports**: Dual real-time performance charts side-by-side:
  - Operations/sec (left) - tracks throughput over time
  - Mean Latency (right) - displays response times in μs/ms/s
- **Bottom Viewport**: spt messages and errors
- **Status Line**: Current date, time, container status, and elapsed time

**Navigation Controls:**

- **'g'**: Show/hide performance graphs (starts hidden - press this to see charts!)
- **'m'**: Show/hide spt messages viewport (starts hidden)
- **TAB**: Switch between Spt output and spt messages viewports
- **↑/↓** or **k/j**: Scroll up/down in the active viewport
- **PgUp/PgDn**: Page up/down in the active viewport
- **q** or **Ctrl+C**: Quit the TUI

**Metrics Source (JSON-only):**

- Charts and percent values are driven exclusively by the Spt JSON endpoint (`/metrics/json`).

Auto Results and Shutdown
- `--auto-results` (default: true): automatically discovers step IDs, waits for terminal state via `/status` + idle JSON, then fetches artifacts (preferring per-step `/logs/<stepId>/index.json`).
- When auto-results completes successfully, `spt` now writes a human-readable summary alongside the fetched artifacts (`spt_<runID>_results_summary.txt`) and prints a shortened version to the console. The summary pulls from the same metrics `.csv` files, `spt_run_params.json`, and the copied scenario file so the bundle stays self-contained.
- `--results-dir`: directory where fetched artifacts and a manifest `index.json` are saved.
- `--label`: prefix for the output directory name and step ID prefix used in filenames.
- `--shutdown-on-complete` (default: true): after a successful fetch, POST `/shutdown` to all Spt hosts and wait for `/status` to linger in a terminal state.

Environment configuration
- spt loads environment variables from `.env` files at startup using the `godotenv` library: first from `$HOME/.env` (if present) and then from `./.env` (if present). Existing OS environment variables are not overridden; the local `./.env` overrides values from `$HOME/.env` loaded by spt.
- Variable expansion follows `godotenv` rules. Use `$VAR` or `${VAR}`. Command substitutions like `$(pwd)` are not supported; use `$PWD` instead.
- Hosts: if `--test-hosts` is not specified, spt will use the `HOSTS` environment variable (from OS or `.env`). If neither is set, it falls back to localhost.
- S3 defaults: you can provide `S3_ENDPOINTS` (CSV) or `S3_ENDPOINT` (single), plus `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET`, and (optionally) `S3_AUTH_VERSION` via environment or `.env`. `S3_AUTH_VERSION` defaults to `4`; set it to `2` only when targeting legacy services that cannot accept SigV4.
- Image selection: by default, release CLIs run the matching engine image tag (`ghcr.io/dell/storage-performance-tool:v<version>`), while local/dev builds use the local-only `ghcr.io/dell/storage-performance-tool:spt_dev` image.
- Image override (optional): set `SPT_IMAGE` or pass `--spt-image` to override the default Docker image spt uses for verify and run. Overrides are used verbatim, including floating tags such as `:latest`.
- Image pull control: set `SPT_SKIP_IMAGE_PULL=1` to skip pulling the Spt image before each run. For non-dev images, spt still pulls if the image is missing locally. Dev images such as `spt_dev` are never pulled; if missing, build them with `make docker-local` and distribute them to workers with `engine/tools/push-worker-image.sh`.
- Data shaping: `SPT_OBJECT_DATA_COMPRESSIBILITY` (0-100, default 0) sets target compressibility; `SPT_OBJECT_DATA_DEDUPABLE` (true/false, default true) controls anti-dedupe stamping.
- Precedence: CLI flags > OS environment > `./.env` > `$HOME/.env` > built-in defaults. For endpoints specifically: `--endpoints` > `S3_ENDPOINTS` > `S3_ENDPOINT`. For authentication: CLI flag `--auth-version` overrides `S3_AUTH_VERSION`.
- Quick start: copy `.env.example` to `.env` and edit placeholders for your environment.

Tools scripts configuration
- Helper scripts under `tools/` (e.g., `nodes-up.sh`, `nodes-down.sh`, `nodes-status.sh`, `test-nodes-s3.sh`) use environment variables beyond what spt itself reads.
- See `tools/.env.example` for a comprehensive template of script variables. Copy it to the repository root as `.env` if you plan to use the scripts.
- `--shutdown-linger` (default: 5): seconds to require `/status` to keep returning a terminal state after `/shutdown` before considering shutdown successful.
- The top viewport shows container logs for visibility and debugging; it is not parsed for metrics.
- This simplifies behavior, eliminates flicker, and keeps a single source of truth.

**Historical Chart Navigation:**

The TUI supports interactive time-based navigation through performance samples:

- **←/→** or **h/l**: Navigate backward/forward in time through historical samples
- **ESC**: Jump immediately back to live mode
- When in historical mode:
  - Selected sample is highlighted in yellow across both charts
  - Chart headers show the selected sample's timestamp and value in yellow
  - Charts automatically scroll to keep the selected sample visible
  - New samples continue to be collected while viewing historical data

This feature is particularly useful for identifying performance anomalies or comparing different time periods during a benchmark run.

### Headless Mode

When no TTY is available (CI/CD environments, Docker containers, etc.), spt automatically switches to headless mode:

**Features:**

- **Auto-Detection**: Automatically detects when TTY is not available
- **Structured Output**: Timestamped, categorized console output
- **Real-time Metrics**: Parsed performance data displayed in human-readable format
- **Trace Files**: Complete output capture for post-analysis
- **Signal Handling**: Graceful shutdown on interruption (Ctrl+C)

**Output Format:**

```
[2025-08-06 14:23:45] [INIT] Starting spt in headless mode
[2025-08-06 14:23:45] [DOCKER] Starting container with image: ghcr.io/dell/storage-performance-tool
[2025-08-06 14:23:46] [DOCKER] Container started: abc123def456
[2025-08-06 14:23:47] [SPT] Spt v5.0.2 starting...
[2025-08-06 14:23:48] [METRICS] ops/sec=45 latency=1024µs type=CREATE success=100 concurrency=8.0
[2025-08-06 14:23:49] [METRICS] ops/sec=52 latency=956µs ttfb=430µs type=READ success=156 concurrency=8.0
```

The `ttfb` field is emitted only for samples that include Time to First Byte data, currently READ and LIST operation metrics.

**Trace Files:**

Use `--trace-file` to capture complete execution logs:

```bash
spt run mock --duration 30s --trace-file benchmark.log
```

Trace files include:
- Complete console output
- Raw Spt output with ANSI codes preserved
- System metadata and command information
- Execution timeline with precise timestamps

When `--auto-results` is enabled and `--trace-file` is not set, spt now auto-creates a per-run trace file:
- Path: `<results-root>/spt-<runTimestamp>.trace.log`
- Applies to both headless and TUI runs
- Auto traces always start fresh (no append)
- If the results-root trace path cannot be initialized, spt falls back to `./spt-<runTimestamp>.trace.log` and prints a warning
- The trace is recorded in the results bundle manifest (`index.json` → `runFiles`) and run metadata (`spt_run_params.json` → `traceFile`, `traceAuto`)

### Logging and Debugging

`spt` provides multiple debugging approaches depending on your needs:

**Standard Logging (both modes):**
- **Log Levels**: debug, info (default), warn, error
- **Log Output**: All logs written to file (default: `spt.log`)
- **Debug Mode**: Use `--debug` flag for detailed information
- **Log Management**: Choose between creating new files or appending

**Headless Mode Debugging:**
- **Trace Files**: Complete execution capture with `--trace-file`
- **Verbose Mode**: Docker API details with `--verbose`
- **Structured Output**: Categorized real-time output

**Examples:**

```bash
# Interactive mode with debug logging
spt --debug run write --endpoints http://minio:9000 --access-key test --secret-key test --bucket test --duration 30s

# Headless mode with comprehensive tracing
spt run mock --duration 30s --verbose --trace-file debug-trace.log

# CI/CD friendly (auto-detects headless mode)
./spt run write --endpoints $S3_ENDPOINT --access-key $ACCESS_KEY --secret-key $SECRET_KEY --bucket $BUCKET --duration 5m > benchmark-output.log 2>&1
```

## License

Copyright © 2025 Dell Technologies

## Acknowledgments

- Built with [Cobra](https://github.com/spf13/cobra) for CLI structure
- Uses [Bubble Tea](https://github.com/charmbracelet/bubbletea) for the TUI
- Wraps the [Spt](https://github.com/dell/storage-performance-tool/tree/main/engine) benchmarking tool
