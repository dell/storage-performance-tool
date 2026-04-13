# spt CLI Syntax Reference

This document covers the CLI syntax for `spt`, a Go-based tool for benchmarking S3-compatible storage using the SPT engine.

The command structure follows the `docker` CLI pattern (`command subcommand [options]`) for familiarity and clear separation of concerns.

## Main Commands

- `spt run <workload>`: Execute a benchmark test.
- `spt verify`: Validate nodes for distributed testing infrastructure readiness.
- `spt status`: Inspect live readiness and metrics snapshots for running nodes.
- `spt results`: *(Stub — not yet implemented)* Manage past benchmark results.
- `spt version`: Print build metadata (version, commit, build date).

---

## Global Flags

These flags are available on all commands:

| Flag | Default | Description |
|------|---------|-------------|
| `--debug` | `false` | Run in debug mode (alias for `--log-level debug`) |
| `--log-level` | `info` | Log level: `debug`, `info`, `warn`, `error` |
| `--log-file` | `spt.log` | Log file path |
| `--log-append` | `false` | Append to existing log file instead of overwriting |

---

## Environment and .env

On startup, spt loads environment variables from `$HOME/.env` and then from `./.env` if present using the `godotenv` library. Existing OS environment variables are not overridden, and the local `./.env` takes precedence over `$HOME/.env` for variables not already present in the OS environment.

You can use these variables to avoid repeating sensitive or commonly used parameters:

- **S3 connection:** `S3_ENDPOINTS` (CSV) or `S3_ENDPOINT` (single), `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET`
- **Authentication:** `S3_AUTH_VERSION` (set to `2` only for legacy targets; default `4`)
- **Hosts:** `HOSTS` (comma-separated list of `[user@]host`)
- **Workload:** `THREADS` (parallel client threads)
- **Docker:** `SPT_SKIP_IMAGE_PULL` (skip pulling the engine image)
- **Engine tuning:** `SPT_SERVICE_THREADS` (virtual-thread carrier parallelism)
- **Multipart upload:** `SPT_PART_SIZE` (part size, e.g. `64MB`)
- **Checksum:** `SPT_CHECKSUM` (algorithm: `crc32`, `crc32c`, `sha1`, `sha256`)
- **Storage driver:** `SPT_S3_DRIVER` (driver backend: `default`, `aws`, `rdma`)
- **RDMA:** `SPT_RDMA_ENABLED`, `RDMA_LOCAL_IP`, `RDMA_DEVICE`, `RDMA_LOG_LEVEL`, `RDMA_THRESHOLD_BYTES`, `RDMA_TIMEOUT_MS`, `RDMA_FALLBACK_ENABLED`

Variable expansion: use `$VAR` or `${VAR}`. Command substitutions like `$(pwd)` are not supported; use `$PWD` instead.

**Precedence:** CLI flags > OS environment > `./.env` > `$HOME/.env` > built-in defaults. For endpoints specifically: `--endpoints` > `S3_ENDPOINTS` > `S3_ENDPOINT`.

---

## Core Command: `spt run`

The `run` command executes a benchmark. Its structure is `spt run <type> [options]`, where `<type>` is a mandatory argument specifying the workload.

### Workload Types

| Type | Status | Description |
|------|--------|-------------|
| `write` | Implemented | Create objects to measure ingest performance |
| `list` | Implemented | Enumerate existing objects and report listing throughput |
| `read` | Implemented | Read pre-existing objects to measure read performance |
| `mock` | Implemented | Exercise the CLI with in-memory drivers (no S3 required) |
| `tables` | Implemented | Benchmark S3 Tables (Iceberg) operations — see [S3_TABLES.md](S3_TABLES.md) |
| `mixed` | Implemented | Run a weighted mix of GET, PUT, DELETE, and STAT operations concurrently |
| `delete` | Planned | Measure object deletion performance |

### Options (Flags)

Flags are grouped by function for clarity.

#### 1. Target Connection Options

Required for S3 workloads, optional/ignored for `mock`.

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--endpoints` | `-e` | *(required)* | One or more S3 endpoint URLs (comma-separated or repeatable) |
| `--access-key` | `-a` | *(required)* | S3 access key credential |
| `--secret-key` | `-s` | *(required)* | S3 secret key credential |
| `--bucket` | `-b` | *(required)* | Target bucket to use for the test |
| `--prefix` | | `""` | Optional object key prefix (list workload only) |
| `--auth-version` | | `4` | S3 signature version (`2` or `4`) |
| `--slice-endpoints` | | `false` | Partition endpoints across nodes in distributed runs |

#### 2. Workload Definition Options

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--threads` | `-t` | `1` | Number of parallel client threads |
| `--object-size` | `-o` | `""` | Size of each object (e.g., `1MB`, `256KB`, `4GB`). Ignored for `list` |
| `--part-size` | | `""` | Enable multipart upload with the given part size (e.g., `5MB`, `64MB`, `256MB`). When set, `load.batch.size` is automatically forced to `1`. Applies to `write` workloads and `read` seed phases |
| `--object-count` | `-n` | `0` | Fixed number of objects to process |
| `--duration` | `-d` | `""` | Fixed time duration (e.g., `5m`, `1h`) |
| `--seed-objects` | | `2500` | Objects to pre-create for `read` benchmarks |
| `--checksum` | | `""` | Enable S3 checksum validation with the specified algorithm: `crc32`, `crc32c`, `sha1`, `sha256`. Omit to disable checksums. When set with `--part-size`, checksums are applied per part. (env: `SPT_CHECKSUM`) |
| `--save-items` | | `false` | Save `items.csv` listing created objects (`write` only) |
| `--items-file` | | `""` | Path to a saved `items.csv` for `read` (skips seed phase) |

*Typically specify either `--object-count` or `--duration`, not both.*

**`--save-items` / `--items-file` workflow:** By default, `read` workloads seed their own objects via an internal write phase. When you need independent control over the write and read phases (different concurrency, duration, or to reuse a data set across multiple reads), use `--save-items` on a `write` run to persist the object list, then pass the resulting `items.csv` to a `read` run via `--items-file`. See [Write-Then-Read Workflow](#write-then-read-workflow) below for examples.

#### 3. Test Behavior Options

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--cleanup` | | `false` | Delete created objects after the test completes |
| `--generate-only` | | `false` | Generate the scenario file without running it |
| `--auto-terminate-seconds` | | `0` | Auto-terminate headless runs after N seconds (0 = unlimited) |
| `--keep-scenario` | | `false` | Keep the scenario file after test completion |
| `--force` | | `false` | Automatically resolve port conflicts without prompting |
| `--api-port` | | `9999` | SPT engine API port |
| `--skip-image-pull` | | `false` | Use locally cached Docker image without pulling |
| `--output-dir` | `-O` | `""` | Local directory to save detailed SPT report files |
| `--service-threads` | | `0` | Engine virtual-thread carrier parallelism (0 = JVM default `max(2, cpus/4)`) |

#### 4. Results Retrieval Options

| Flag | Default | Description |
|------|---------|-------------|
| `--auto-results` | `true` | Automatically retrieve results artifacts at end of run |
| `--results-dir` | `./results` | Directory to write retrieved results artifacts |
| `--label` | `""` | Label for output directory naming and step ID prefix (default: `mt`) |
| `--auto-results-debug` | `false` | Enable verbose debug logs for results retrieval |
| `--shutdown-on-complete` | `true` | Request `/shutdown` on all hosts after fetching results |
| `--shutdown-linger` | `5` | Seconds to wait for `/status` linger after `/shutdown` |

#### 5. Multi-Host / Distributed Options

| Flag | Default | Description |
|------|---------|-------------|
| `--test-hosts` | `127.0.0.1` | Comma-separated Docker hosts: `[user@]host[,...]` |
| `--min-hosts` | `0` (all) | Minimum hosts that must connect (0 = all must succeed) |
| `--attach-existing` | `false` | Attach to pre-started worker nodes; spt still launches the entry node |
| `--network-mode` | `host` | Docker network mode: `host` (required for RMI) or `bridge` |
| `--rmi-port-start` | `40000` | Starting port for RMI range |
| `--rmi-port-count` | `10` | Number of RMI ports to allocate |

#### 6. Storage Driver Selection

| Flag | Default | Description |
|------|---------|-------------|
| `--s3-driver` | `default` | S3 storage driver backend (see below) |

Available driver values:

| Value | Engine driver | Description |
|-------|---------------|-------------|
| `default` | `s3` (Netty) | The standard REST/Netty-based S3 driver. Default for all workloads |
| `netty` | `s3` (Netty) | Alias for `default` |
| `aws` | `s3-aws` | AWS SDK v2 synchronous client. Useful for compatibility testing or environments where the Netty driver is not suitable |
| `rdma` | `s3-rdma` | RDMA-accelerated S3 driver. Equivalent to `--use-rdma` |

**Flag interaction with `--use-rdma`:**

- `--use-rdma` remains the primary way to enable RDMA and is equivalent to `--s3-driver rdma`.
- If both `--s3-driver` and `--use-rdma` are specified, they must agree — `--s3-driver rdma --use-rdma` is fine, but `--s3-driver aws --use-rdma` is an error.
- When `--s3-driver rdma` is used, the RDMA acceleration flags (below) apply normally.

#### 7. RDMA Acceleration Options

See [S3_RDMA.md](S3_RDMA.md) for detailed documentation, architecture, and troubleshooting.

| Flag | Default | Description |
|------|---------|-------------|
| `--use-rdma` | `false` | Use RDMA-accelerated S3 driver (requires RDMA hardware) |
| `--rdma-local-ip` | `""` | Local RDMA interface IP address |
| `--rdma-threshold` | `1MB` | Minimum object size for RDMA transfer (e.g., `0`, `256KB`, `4MB`) |
| `--rdma-fallback` | `false` | Fall back to HTTP if RDMA initialization fails |
| `--rdma-device` | `auto` | RDMA device name or `auto` for auto-detection |
| `--rdma-log-level` | `WARN` | RDMA native library log level |
| `--rdma-timeout-ms` | `30000` | RDMA operation timeout in milliseconds |

#### 8. S3 Tables Options

These flags apply only to the `tables` workload type. See [S3_TABLES.md](S3_TABLES.md) for detailed documentation.

| Flag | Default | Description |
|------|---------|-------------|
| `--test-vector` | `tps` | Test vector: `tps`, `compaction`, or `catalog` |
| `--table-bucket` | `spt-tables` | S3 Table bucket name |
| `--namespace` | `default` | Namespace within the table bucket |
| `--table-name` | `spt-bench` | Table name (auto-suffixed with timestamp if default) |
| `--concurrent-writers` | `10` | Concurrent Iceberg commit threads |
| `--commit-freq-ms` | `500` | Target ms between commits per writer |
| `--target-file-size` | `64MB` | Target Parquet file size |
| `--ingest-file-size` | `100KB` | Small Parquet file size for compaction seed |
| `--total-ingest` | `1GB` | Total data volume for compaction seed |
| `--namespace-count` | `100` | Namespaces to create for catalog test |
| `--tables-per-ns` | `100` | Tables per namespace for catalog test |
| `--read-concurrency` | `10` | Concurrent catalog readers |
| `--compaction-timeout` | `4h` | Max wait for compaction to complete |
| `--no-provision` | `false` | Skip table bucket/namespace/table creation |

#### 9. Mixed Workload Options

These flags apply only to the `mixed` workload type.

| Flag | Default | Description |
|------|---------|-------------|
| `--get-distrib` | `45` | Percentage of GET (read) operations |
| `--put-distrib` | `30` | Percentage of PUT (write) operations |
| `--delete-distrib` | `15` | Percentage of DELETE operations |
| `--stat-distrib` | `10` | Percentage of STAT (HEAD) operations |
| `--seed-objects` | `2500` | Objects to pre-create before the mixed benchmark |
| `--read-items-file` | `""` | Items file for the READ pool (skips seed phase) |
| `--delete-items-file` | `""` | Items file to pre-populate the DELETE queue |

**Distribution rules:**

- All four percentages must sum to exactly **100**.
- At least **2** operation types must have a non-zero weight.
- `--delete-distrib` must be ≤ `--put-distrib` unless `--delete-items-file` is provided (without an external seed, PUTs must sustain the DELETE queue).
- `--duration` is required (mixed workloads are time-based only).
- `--seed-objects` must be > 0 unless `--read-items-file` is provided.
- `--cleanup` cannot be combined with `--read-items-file` or `--delete-items-file`.

Set any operation weight to `0` to exclude it. For example, `--get-distrib 60 --put-distrib 40 --delete-distrib 0 --stat-distrib 0` runs a GET/PUT-only mix.

#### 10. TUI / Headless Options

By default, `spt run` launches an interactive TUI. Use `--headless` for CI or unattended runs.

| Flag | Default | Description |
|------|---------|-------------|
| `--headless` | `false` | Force headless (non-interactive) mode |
| `--minimal` | `false` | Start TUI with only live stats panel visible |
| `--verbose` | `false` | Show detailed Docker API calls and debug info (headless mode) |
| `--trace-file` | `""` | Save all output to a trace file |
| `--trace-append` | `false` | Append to existing trace file instead of overwriting |

---

## Examples

### Write Workload

```bash
# Write 1024 objects at 1MB each with 8 threads, then clean up
spt run write \
    --endpoints http://s3a:9000,http://s3b:9000 \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket benchmark-test \
    --threads 8 \
    --object-size 1MB \
    --object-count 1024 \
    --cleanup
```

```bash
# Duration-based write: write for 5 minutes, then clean up
spt run write \
    --endpoints https://s3.example.com \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket benchmark-test \
    --threads 16 \
    --object-size 1MB \
    --duration 5m \
    --cleanup
```

### Multipart Upload Write

Use `--part-size` to enable S3 multipart upload. Objects larger than the part size are automatically split into parts and uploaded in parallel using the engine's concurrency pool.

```bash
# Write 100 x 1GB objects using 64MB multipart parts
spt run write \
    --endpoints http://s3a:9000,http://s3b:9000 \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket benchmark-test \
    --threads 16 \
    --object-size 1GB \
    --part-size 64MB \
    --object-count 100
```

```bash
# Duration-based multipart write with cleanup
spt run write \
    --endpoints https://s3.example.com \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket benchmark-test \
    --threads 16 \
    --object-size 1GB \
    --part-size 256MB \
    --duration 10m \
    --cleanup
```

**How it works:**

When `--part-size` is set, each object goes through a four-phase lifecycle:

1. **Initiate** -- `POST ?uploads` creates the multipart upload and returns an upload ID.
2. **Upload Parts** -- each part is uploaded in parallel via `PUT ?partNumber=N&uploadId=ID`.
3. **Complete** -- `POST ?uploadId=ID` finalizes the upload with the collected part ETags.
4. **Abort** (on failure) -- `DELETE ?uploadId=ID` cleans up the incomplete upload on the storage target.

**Fault tolerance:**

- Individual parts are retried up to **3 times** before the upload is considered failed. This means a transient network error on one part does not waste the successfully uploaded parts immediately.
- If all retries for a part are exhausted, the engine automatically sends an `AbortMultipartUpload` request to clean up the incomplete upload. This prevents orphaned parts from accumulating on the storage target.

**Checksums:**

When the engine's checksum feature is enabled (`storage-checksum-enabled=true` in defaults or scenario config), checksums are computed and sent for **each individual part upload**, not just the final object. Supported algorithms: `md5`, `crc32`, `crc32c`, `sha1`, `sha256`.

**Results artifacts:**

When multipart uploads are used, the engine produces a `parts.upload.csv` artifact (fetched as `<stepID>.multipart.csv` in the results directory) containing one row per completed upload with columns: `ItemPath`, `UploadId`, `RespLatency[us]`.

**Notes:**

- `--part-size` must be smaller than `--object-size` (multipart with a single part is pointless).
- No minimum part size is enforced by the CLI -- different S3-compatible storage systems have varying constraints, so the storage system reports errors for invalid sizes at runtime.
- When `--part-size` is set, the engine's `load.batch.size` is automatically forced to `1`. This is required because each multipart upload spawns N sub-operations for its parts; the default batch size of 4096 would flood the internal operation queue and cause silently dropped operations.
- Part uploads share the `--threads` concurrency pool with regular operations.
- If `--part-size` is omitted, objects are uploaded as single PUTs regardless of size.
- `--part-size` also applies to the seed (precondition) phase of `read` workloads, so large seed objects are written using multipart upload.

### Read Workload

```bash
# Seed 5000 objects, then read them for 5 minutes
spt run read \
    --endpoints https://s3.example.com \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket benchmark-test \
    --threads 16 \
    --object-size 1MB \
    --seed-objects 5000 \
    --duration 5m \
    --cleanup
```

### Write-Then-Read Workflow

The `--save-items` and `--items-file` flags enable decoupled write-then-read workflows where write and read phases run as independent commands with separate concurrency, duration, and tuning.

```bash
# Step 1: Write objects and save the item list
spt run write \
    --endpoints https://s3.example.com \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket benchmark-test \
    --threads 16 \
    --object-size 1MB \
    --duration 5m \
    --save-items \
    --label w-1mb

# The items.csv artifact is saved to the results directory:
#   ./results/w-1mb-<timestamp>/w-1mb-*-create.items.csv

# Step 2: Read using the saved item list (no seed phase)
spt run read \
    --endpoints https://s3.example.com \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket benchmark-test \
    --threads 64 \
    --object-size 1MB \
    --duration 5m \
    --items-file ./results/w-1mb-*/w-1mb-*.items.csv \
    --label r-1mb
```

This pattern is useful for:
- **Independent concurrency tuning** — different thread counts for writes vs. reads.
- **Naturally-sized data sets** — the write phase produces an item set determined by actual throughput rather than an upfront `--seed-objects` guess.
- **Separate write metrics** — write performance is measured and reported independently.
- **Re-running reads** — the same `items.csv` can feed multiple read passes with different settings (duration, RDMA, concurrency) without recreating objects.

### List Workload

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
- If neither `--object-count` nor `--duration` is provided, the scenario runs until stopped. Use `--auto-terminate-seconds` for unattended runs.
- The list workload does not modify storage, so `--cleanup` and `--object-size` are unused.

### Mock Mode

Mock mode is useful for testing spt itself or for CI where an S3 endpoint may not be available:

```bash
# Simple mock test with duration
spt run mock --duration 30s

# Mock test with custom settings
spt run mock \
    --threads 8 \
    --object-size 512KB \
    --object-count 1000
```

### S3 Tables

See [S3_TABLES.md](S3_TABLES.md) for full documentation. Quick examples:

```bash
# TPS test: 10 concurrent Iceberg writers for 5 minutes
spt run tables \
    --endpoint https://s3tables.us-east-1.amazonaws.com \
    --access-key "$AWS_ACCESS_KEY_ID" \
    --secret-key "$AWS_SECRET_ACCESS_KEY" \
    --table-bucket my-bucket \
    --test-vector tps \
    --concurrent-writers 10 \
    --duration 5m

# Catalog test: 100 namespaces x 100 tables, 5m read phase
spt run tables \
    --endpoint https://s3tables.us-east-1.amazonaws.com \
    --access-key "$AWS_ACCESS_KEY_ID" \
    --secret-key "$AWS_SECRET_ACCESS_KEY" \
    --table-bucket my-bucket \
    --test-vector catalog \
    --namespace-count 100 \
    --tables-per-ns 100 \
    --duration 5m
```

### RDMA-Accelerated Write

```bash
spt run write \
    --endpoints https://ecs.example.com \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket benchmark-test \
    --threads 16 \
    --object-size 4MB \
    --duration 5m \
    --use-rdma \
    --rdma-local-ip 10.247.128.125 \
    --rdma-threshold 1MB
```

### AWS SDK Driver

```bash
# Write using the AWS SDK driver instead of the default Netty driver
spt run write \
    --endpoints https://s3.example.com \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket benchmark-test \
    --threads 16 \
    --object-size 1MB \
    --duration 5m \
    --s3-driver aws \
    --cleanup
```

### Checksum Validation

Enable per-object (or per-part, when using multipart upload) checksum validation with `--checksum`:

```bash
# Write with CRC32C checksums
spt run write \
    --endpoints https://s3.example.com \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket benchmark-test \
    --threads 16 \
    --object-size 1MB \
    --duration 5m \
    --checksum crc32c \
    --cleanup
```

```bash
# Multipart upload with per-part SHA-256 checksums
spt run write \
    --endpoints https://s3.example.com \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket benchmark-test \
    --threads 16 \
    --object-size 1GB \
    --part-size 64MB \
    --object-count 50 \
    --checksum sha256
```

Supported algorithms: `crc32`, `crc32c`, `sha1`, `sha256`. The flag works with both the default Netty driver and the AWS SDK driver (`--s3-driver aws`).

### Mixed Workload

The `mixed` workload runs GET, PUT, DELETE, and STAT operations concurrently with configurable weights. A seed phase pre-creates objects, then the benchmark phase issues operations at the specified distribution for the given duration.

```bash
# Default 4-op mix (GET 45% / PUT 30% / DELETE 15% / STAT 10%) for 5 minutes
spt run mixed \
    --endpoints https://s3.example.com \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket benchmark-test \
    --threads 16 \
    --object-size 1MB \
    --duration 5m \
    --seed-objects 5000
```

```bash
# Heavy-read mix with cleanup
spt run mixed \
    --endpoints https://s3.example.com \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket benchmark-test \
    --threads 32 \
    --object-size 256KB \
    --duration 10m \
    --get-distrib 70 --put-distrib 20 --delete-distrib 5 --stat-distrib 5 \
    --cleanup
```

```bash
# GET/PUT-only mix (no deletes or stats)
spt run mixed \
    --endpoints https://s3.example.com \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket benchmark-test \
    --threads 16 \
    --object-size 1MB \
    --duration 5m \
    --get-distrib 60 --put-distrib 40 --delete-distrib 0 --stat-distrib 0
```

```bash
# Use a pre-existing item set (skip seed), with AWS SDK driver
spt run mixed \
    --endpoints https://s3.example.com \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket benchmark-test \
    --threads 16 \
    --object-size 1MB \
    --duration 5m \
    --read-items-file ./results/w-1mb-*/w-1mb-*.items.csv \
    --s3-driver aws
```

**How it works:**

1. **Seed phase** — writes `--seed-objects` objects to populate the read/delete pools. Skipped when `--read-items-file` is provided.
2. **Mixed benchmark** — runs for `--duration`, issuing operations at the specified weights. The engine's `MixedLoad` step draws from the item set for GETs, STATs, and DELETEs while PUTs create new objects.
3. **Cleanup** (optional, `--cleanup`) — deletes the seed objects and any objects created by PUT operations during the benchmark.

### Distributed / Attach Mode

If operators have already started SPT worker containers, `spt` can attach to those workers and only launch the entry node:

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
- Workers must already expose the SPT API on 9999 and the RMI registry range on the standard ports.
- `spt` still launches and manages the entry node; worker containers remain untouched during shutdown.
- The host list must include at least one worker; the tool enforces this when `--attach-existing` is set.

### Headless CI Mode

```bash
spt run write \
    --headless \
    --endpoints https://s3.example.com \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket ci-bench \
    --threads 8 \
    --object-size 1MB \
    --duration 2m \
    --auto-terminate-seconds 300 \
    --verbose
```

### TUI Live View Indicators

When you launch the interactive TUI (`spt run` without `--headless`), the Host column reflects the orchestrator's lifecycle phases:

- Red — the node is still pending; SSH/Docker contact has not succeeded yet or the host dropped offline.
- Blue — the node has been contacted, the container is starting, or `/ready` is reachable, but metrics are not flowing yet.
- Green — the node is responding to metrics polls and streaming data successfully.

If a node regresses (for example metrics parsing fails while `/ready` stays healthy), the indicator automatically drops from green back to blue so you can spot transient issues without mistaking them for a full outage.

---

## Infrastructure Command: `spt verify`

The `verify` command validates that distributed testing infrastructure is properly configured and ready for coordinated SPT benchmarks.

### Purpose

Distributed SPT testing requires multiple nodes to be properly configured with:
- Docker installed and accessible
- Network connectivity between nodes
- Required ports available (RMI: 1099, REST API: 9999)
- Ability to run SPT containers in node mode

The `verify` command performs comprehensive pre-flight checks to ensure all nodes meet these requirements.

### Syntax

```bash
spt verify [--test-hosts <hosts>] [options]
```

**Note**: If `--test-hosts` is not specified, spt first looks for a `HOSTS` environment variable (from OS or `.env`). If not set, it defaults to localhost (127.0.0.1).

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--test-hosts` | `""` (localhost) | Comma-separated list of hosts to verify |
| `--min-hosts` | `0` (all) | Minimum number of hosts that must pass |
| `--network-mode` | `host` | Docker network mode: `host` (required for RMI) or `bridge` |
| `--api-port` | `9999` | SPT API port to verify |
| `--rmi-port-start` | `40000` | Starting port for RMI range |
| `--rmi-port-count` | `10` | Number of RMI ports to verify |
| `--force-cleanup` | `false` | Automatically clean up conflicting containers without prompting |
| `--use-rdma` | `false` | Include RDMA hardware and configuration checks |

### Verification Process

For each specified host, the verify command:

1. **Checks Connectivity** — local Docker API access or remote SSH connectivity
2. **Validates Docker** — daemon running, API accessible, version compatibility
3. **Detects Port Conflicts** — checks ports 1099 and 9999, identifies existing SPT containers
4. **Starts Test Container** — launches SPT in node mode, validates startup and port mapping
5. **Verifies Services** — tests RMI Registry (1099) and REST API (9999)
6. **Performs Cleanup** — removes test container, ensures clean state

### Examples

```bash
# Verify localhost (default)
spt verify

# Verify three remote nodes
spt verify --test-hosts "root@node1,root@node2,root@node3"

# Partial cluster readiness (2 of 4 must pass)
spt verify --test-hosts "node1,node2,node3,node4" --min-hosts 2

# Automated cleanup of conflicts
spt verify --test-hosts "test1,test2" --force-cleanup

# Include RDMA hardware checks
spt verify --test-hosts "rdma1,rdma2" --use-rdma
```

### Output Interpretation

- **READY**: Minimum required nodes passed all checks
- **NOT READY**: Insufficient nodes passed verification

---

## Observability Command: `spt status`

The `status` command provides a concise snapshot of the nodes participating in a run. It polls each host's SPT API (`/ready`, `/health`, `/status`, and `/metrics/json`) with short timeouts and prints readiness, run state, and the most recent metrics sample.

### Syntax

```bash
spt status [--test-hosts <hosts>] [--api-port <port>]
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--test-hosts` | `""` (localhost) | Comma-separated hosts to inspect |
| `--api-port` | `9999` | SPT REST API port to query |

### Example

```bash
$ spt status --test-hosts entry,worker1,worker2
Node status (port 9999)
- [entry] entry: READY (http 200, status=ready, node=entry-0)
  run: state=RUNNING, run=run-123, progress=78.5%, message="Active test"
  metrics: state=RUNNING, completion=78%, ops=1540/s, throughput=7.3MB/s
- [worker] worker1: READY (http 200, status=ready, node=worker-01)
  metrics: state=RUNNING, completion=76%, ops=1520/s
- [worker] worker2: NOT READY (http 503, status=starting, node=worker-02)
  warn: metrics probe failed: metrics/json status 503
```

### When to Use

- Spot-check distributed runs from another terminal without streaming full logs.
- Confirm that pre-started workers in `--attach-existing` workflows remain healthy.
- Quickly identify which node is lagging (e.g., stuck in `starting`, no metrics).

---

## Implementation Status

| Feature | Status |
|---------|--------|
| `write` workload | Implemented |
| `list` workload | Implemented |
| `read` workload | Implemented |
| `mock` workload | Implemented |
| `tables` workload (S3 Tables / Iceberg) | Implemented |
| `verify` command | Implemented |
| `status` command | Implemented |
| Post-test cleanup (`--cleanup`) | Implemented |
| Auto-results retrieval | Implemented |
| Save/reuse item lists (`--save-items` / `--items-file`) | Implemented |
| Checksum validation (`--checksum`) | Implemented |
| RDMA acceleration | Implemented |
| TUI live dashboard | Implemented |
| Headless / CI mode | Implemented |
| Distributed multi-host orchestration | Implemented |
| `mixed` workload | Implemented |
| `delete` workload | Planned |
| `results` command | Planned (stub exists) |
