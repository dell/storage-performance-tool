# spt: Spt CLI Wrapper - Syntax Documentation

This document outlines the CLI syntax for `spt`, a Go-based wrapper designed to simplify the use of the Spt benchmarking tool for S3-compatible storage.

The command structure is modeled after the `docker` CLI (`command subcommand [options]`) for familiarity and clear separation of concerns.

**Current Status (2025-10-02)**: `write`, `list`, and `mock` workload types are implemented. Additional workloads (`read`, `mixed`, `delete`) remain planned for a future milestone.

## Main Commands

The CLI is organized around a few primary commands:

- `spt run <workload>`: The primary command for executing a benchmark test.
- `spt verify`: Validate nodes for distributed testing infrastructure readiness.
- `spt status`: Inspect live readiness and metrics snapshots for running nodes.
- `spt results`: (Future Scope) A command group for listing, inspecting, and managing past benchmark results.

---

## Environment and .env

On startup, spt loads environment variables from `$HOME/.env` and then from `./.env` if present using the `godotenv` library. Existing OS environment variables are not overridden, and the local `./.env` takes precedence over `$HOME/.env` for variables not already present in the OS environment.

You can use these variables to avoid repeating sensitive or commonly used parameters:

- S3 defaults for `run`: `S3_ENDPOINTS` (CSV) or `S3_ENDPOINT` (single), plus `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET`.
- Signature version defaults: `S3_AUTH_VERSION` (set to `2` only for legacy targets; default `4`).
- Hosts defaults for `run` and `verify`: `HOSTS` (comma-separated list of `[user@]host`).

Variable expansion: use `$VAR` or `${VAR}`. Command substitutions like `$(pwd)` are not supported; use `$PWD` instead.

Precedence: CLI flags > OS environment > `./.env` > `$HOME/.env` > built-in defaults. For endpoints specifically: `--endpoints` > `S3_ENDPOINTS` > `S3_ENDPOINT`.

---

## Core Command: `spt run`

The `run` command executes a benchmark. Its structure is `spt run <type> [options]`, where `<type>` is a mandatory argument specifying the workload.

### Workload Types (Positional Argument)

This argument defines the high-level goal of the test.

- `write`: Create objects to measure ingest performance. **Implemented**
- `list`: Enumerate existing objects and report listing throughput. **Implemented (September 2025)**
- `mock`: Exercise the CLI with in-memory drivers (no S3 required). **Implemented**
- `read`: Perform a read-only test on pre-existing objects. *(Planned)*
- `mixed`: Perform a test with a specified mix of read and write operations. *(Planned)*
- `delete`: Measure object deletion performance. *(Planned)*

### Options (Flags)

Flags are grouped by function for clarity.

#### **1. Target Connection Options (Required for S3 workloads, optional for mock)**

These flags specify the S3 storage endpoint(s) to target. When using the `mock` workload type, these flags are optional and ignored.

- Connection:
  - `--endpoints <url[,url,...]>`: One or more S3 endpoint URLs (comma-separated or repeatable). Supplying a single value is fine for one-host setups.
- Credentials and bucket:
  - `--access-key <key>`: The S3 access key credential.
  - `--secret-key <key>`: The S3 secret key credential.
  - `--bucket <name>`: The target bucket to use for the test.
- Authentication:
  - `--auth-version <2|4>`: Signature version for S3 authentication. Default is `4`; use `2` only for legacy endpoints that cannot accept SigV4.
- List filtering (list workload only):
  - `--prefix <string>`: Optional key prefix to limit object enumeration to a subset of the bucket.

#### **2. Workload Definition Options**

These flags define the load to be generated.

- `--threads <int>`: Number of parallel client threads to run (e.g., `16`).
- `--object-size <size>`: The size of each object using human-readable units (e.g., `1MB`, `256KB`, `4GB`). Ignored for `list` workloads because no new objects are created.
- `--object-count <int>`: Defines the workload by a fixed number of objects to process.
- `--duration <time>`: Defines the workload by a fixed time duration (e.g., `5m`, `1h`).

*Note: A user should typically specify either `--object-count` or `--duration`.*

#### **3. Test Behavior Options**

These flags control how the test is executed and how it handles data.

- `--cleanup`: A boolean flag to automatically delete all created objects after the test completes. *(Not allowed for `list` workloads; the CLI returns a validation error if set.)*
- `--create-prefix`: A boolean flag to ensure the target prefix (directory) is created if it doesn't exist. *(Not allowed for `list`; listings never create objects.)*
- `--output-dir <path>`: Specifies a local directory on the host machine to save the detailed Spt report files (e.g., `./results/test-01`).
- `--api-port <port>`: Specifies the Spt API port (defaults to 9999, the Spt standard port). For backward compatibility, legacy port 43234 is supported.
- `--slice-endpoints`: In distributed runs, partition the provided endpoint list among nodes (optional; default off).

---

## Example Usage

This example translates a command from a similar tool (`elbencho`) into the proposed `spt` syntax.

**Original `elbencho` command:**
```bash
elbencho ./elbencho.seq.1M --threads 8 --size 1M --block 1M --write --delfiles
```

**Proposed `spt` equivalent:**

This command runs a write-only test against a specified S3 endpoint. It uses 8 concurrent threads to write 1024 objects, each 1MB in size. After the test, it cleans up by deleting the objects it created.

```bash
spt run write \
    --endpoints http://s3a:9000,http://s3b:9000 \
    --access-key "YOUR_ACCESS_KEY" \
    --secret-key "YOUR_SECRET_KEY" \
    --bucket "benchmark-test" \
    --threads 8 \
    --object-size 1MB \
    --object-count 1024 \
    --cleanup \
    --output-dir ./results/write-test-01
```

### Mock Mode Example

Mock mode is useful for testing spt itself or for QA scripts where an S3 endpoint may not be available:

```bash
# Simple mock test with duration
spt run mock --duration 30s

# Mock test with custom settings
spt run mock \
    --threads 8 \
    --object-size 512KB \
    --object-count 1000

# Mock test with all parameters (S3 flags are ignored)
spt run mock \
    --threads 4 \
    --duration 1m

### List Workload Example

`spt run list` reuses the same credential handling as write runs but only enumerates existing keys. Combine a prefix filter and auto-termination so test runs end predictably in CI or unattended environments.

```bash
spt run list \
    --endpoints https://s3.example.com \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket analytics-data \
    --prefix logs/2025/09/ \
    --threads 4 \
    --auto-terminate-seconds 120
```

Notes:
- If neither `--object-count` nor `--duration` is provided, the scenario runs until stopped. Prefer `--auto-terminate-seconds` for unattended runs.
- The list workload does not modify storage, so cleanup flags and object-size settings are unused.
- SigV4 signing is enabled by default. Use `--auth-version 2` when you must work with an endpoint that only supports Signature Version 2.

### Distributed Attach Mode Example

If operators have already started Spt worker containers (e.g., via `docker run --run-node`), `spt` can attach to those workers and only launch the entry node. Use `--attach-existing` with the entry host listed first:

```bash
spt run write \
    --test-hosts entry,worker1,worker2 \
    --attach-existing \
    --endpoints http://minio:9000 \
    --access-key demo \
    --secret-key demo123 \
    --bucket perf-test \
    --threads 8 \
    --object-size 1MB \
    --object-count 2000
```

Notes:
- Workers must already expose the Spt API on 9999 and the RMI registry range on the standard ports.
- `spt` still launches and manages the entry node; worker containers remain untouched during shutdown.
- The host list must include at least one worker; the tool enforces this when `--attach-existing` is set.

### TUI Live View Indicators

When you launch the interactive TUI (`spt run …` without `--headless`), the Host column reflects the orchestrator's lifecycle phases:

- `❌` — the node is still pending; SSH/Docker contact has not succeeded yet or the host dropped offline.
- `🔵` — the node has been contacted, the container is starting, or `/ready` is reachable, but metrics are not flowing yet.
- `✅` — the node is responding to metrics polls and streaming data successfully.

If a node regresses (for example metrics parsing fails while `/ready` stays healthy), the indicator automatically drops from green back to blue so you can spot transient issues without mistaking them for a full outage.
```

### Design Rationale

1.  **Clarity:** `spt run write` is immediately understandable.
2.  **Simplicity:** Hides the complexity of the underlying Spt configuration for common use cases.
3.  **Extensibility:** The structure allows for future expansion (e.g., `spt run mixed --read-ratio 0.8`) without altering the core design.

---

## Infrastructure Command: `spt verify`

The `verify` command validates that distributed testing infrastructure is properly configured and ready for coordinated Spt benchmarks.

### Purpose

Distributed Spt testing requires multiple nodes to be properly configured with:
- Docker installed and accessible
- Network connectivity between nodes
- Required ports available (RMI: 1099, REST API: 9999)
- Ability to run Spt containers in node mode

The `verify` command performs comprehensive pre-flight checks to ensure all nodes meet these requirements.

### Syntax

```bash
spt verify [--test-hosts <hosts>] [options]
```

**Note**: If `--test-hosts` is not specified, spt first looks for a `HOSTS` environment variable (from OS or `.env`). If not set, it defaults to localhost (127.0.0.1).

### Options (Flags)

#### **1. Host Configuration**

- `--test-hosts <hosts>`: Comma-separated list of hosts to verify. If omitted, spt uses `HOSTS` from the environment (see .env support) or defaults to localhost.
  - Local host: `"127.0.0.1"` or `"localhost"` 
  - Remote hosts: `"server1,server2,server3"`
  - With SSH users: `"root@server1,admin@server2"`
  - Mixed: `"localhost,root@remote1,remote2"`

#### **2. Verification Parameters**

- `--min-hosts <int>`: Minimum number of hosts that must pass (default: all)
  - Useful for partial cluster readiness
  - Example: `--min-hosts 3` with 5 hosts means at least 3 must pass

- `--network-mode <mode>`: Docker network mode (default: "bridge")
  - Options: "bridge" or "host"
  - Affects port mapping validation

#### **3. RMI Configuration**

- `--rmi-port-start <port>`: Starting port for RMI object ports (default: 40000)
- `--rmi-port-count <count>`: Number of RMI ports to allocate (default: 10)
  - These define the range for dynamic RMI object ports during testing

#### **4. Conflict Resolution**

- `--force-cleanup`: Automatically clean up conflicting containers
  - Without this flag: Interactive prompt for resolution
  - With this flag: Automatic removal of conflicting Spt containers
  - Useful for CI/CD and automation scenarios

### Verification Process

For each specified host, the verify command:

1. **Checks Connectivity**
   - Local: Direct Docker API access
   - Remote: SSH connectivity validation

2. **Validates Docker**
   - Ensures Docker daemon is running
   - Verifies API accessibility
   - Checks version compatibility

3. **Detects Port Conflicts**
   - Checks if ports 1099 and 9999 are available
   - Identifies any existing Spt containers
   - Offers resolution options (cleanup/skip/retry/abort)

4. **Starts Test Container**
   - Launches Spt in node mode
   - Validates container startup
   - Ensures proper port mapping

5. **Verifies Services**
   - Tests RMI Registry port (1099)
   - Tests REST API port (9999)
   - Validates API responses

6. **Performs Cleanup**
   - Removes test container
   - Ensures clean state for testing

### Examples

```bash
# Verify localhost (default - perfect for getting started)
spt verify

# Basic verification of three nodes
spt verify --test-hosts "node1,node2,node3"

# Verification with SSH users
spt verify --test-hosts "root@prod1,root@prod2,root@prod3"

# Partial cluster readiness (2 of 4 must pass)
spt verify --test-hosts "node1,node2,node3,node4" --min-hosts 2

# Automated cleanup of conflicts
spt verify --test-hosts "test1,test2" --force-cleanup

# Custom RMI port configuration
spt verify --test-hosts "server1,server2" \
  --rmi-port-start 30000 \
  --rmi-port-count 20

# Host network mode verification
spt verify --test-hosts "host1,host2" --network-mode host
```

Tip: If you omit `--test-hosts`, define `HOSTS` in your environment or `.env` and spt will use it. Otherwise it defaults to localhost (127.0.0.1).

### Output Interpretation

The command provides detailed feedback for each node:

- **✅ Success**: Component passed verification
- **❌ Failure**: Component failed with specific error
- **⚠️ Warning**: Non-critical issue detected

Final summary indicates overall readiness:
- **READY**: Minimum required nodes passed all checks
- **NOT READY**: Insufficient nodes passed verification

### Integration with Distributed Testing

Once verification passes, the validated nodes can be used for distributed Spt testing. The verified infrastructure ensures:

- All nodes can communicate via RMI
- Docker containers can be orchestrated across nodes
- Required ports are available for coordination
- No conflicting Spt instances are running

This verification step is crucial before launching distributed benchmarks to avoid runtime failures and ensure consistent test results.

---

## Observability Command: `spt status`

The `status` command provides a concise snapshot of the nodes participating in a run. It polls each host's Spt API (`/ready`, `/health`, `/status`, and `/metrics/json`) with short timeouts and prints readiness, run state, and the most recent metrics sample.

### Syntax

```bash
spt status [--test-hosts <hosts>] [--api-port <port>]
```

### Options

- `--test-hosts <hosts>`: Same host format used by `run` and `verify`. Defaults to `HOSTS` env/`.env`, and finally to localhost (`127.0.0.1`).
- `--api-port <port>`: Spt REST API port to query (default: `9999`).

### Example

```bash
$ spt status --test-hosts entry,worker1,worker2
Node status (port 9999)
- [entry] entry: READY (http 200, status=ready, node=entry-0)
  run: state=RUNNING, run=run-123, progress=78.5%, message="Active test"
  metrics: state=RUNNING, completion=78%, overall=77%, ops=1540/s, throughput=7.3MB/s, sample=2025-09-24T13:36:52Z (2s ago)
- [worker] worker1: READY (http 200, status=ready, node=worker-01)
  metrics: state=RUNNING, completion=76%, ops=1520/s, sample=2025-09-24T13:36:51Z (3s ago)
- [worker] worker2: NOT READY (http 503, status=starting, node=worker-02)
  warn: metrics probe failed: metrics/json status 503
```

### When to Use

- Spot-check distributed runs from another terminal without streaming full logs.
- Confirm that pre-started workers in `--attach-existing` workflows remain healthy.
- Quickly identify which node is lagging (e.g., stuck in `starting`, no metrics).

The output is intentionally terse compared to `spt verify`: one header block followed by a few lines per node, plus warnings only when probes fail or return unexpected data.

---

## Implementation Status

### Phase 1 (Current)
- ✅ `write` workload type
- ✅ `mock` workload type
- ✅ `verify` command for infrastructure validation
- ✅ Port conflict detection and resolution
- ✅ Multi-node parallel verification
- ✅ All connection options (endpoint, access-key, secret-key, bucket)
- ✅ Basic workload options (threads, object-size, object-count, duration)
- ⏳ Test behavior options (cleanup, create-prefix, output-dir) - flags exist but not yet implemented

### Future Phases
- ⏳ `read` workload type
- ⏳ `mixed` workload type with read/write ratio options
- ⏳ `delete` workload type
- ⏳ `spt results` command for managing benchmark results
