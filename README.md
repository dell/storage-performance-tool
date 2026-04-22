# Dell Storage Performance Tool (SPT)

The Dell **Storage Performance Tool (SPT)** is an open-source benchmark suite purpose-built for S3-compatible storage. SPT couples a task-oriented CLI/TUI with a high-performance engine so you can:

- spin up realistic object workloads in minutes;
- orchestrate single-node or distributed runs without handcrafting scripts;
- monitor live latency/throughput in an interactive terminal UI or headless CI logs;
- reuse the same configuration flow for both mock runs and production endpoints.

SPT packages two tightly integrated components:

- **SPT CLI/TUI** – a Go-based command-line and terminal UI experience for configuring, launching, and monitoring workloads.
- **SPT Engine** – a Java-based benchmarking engine that executes those workloads inside managed containers.

The CLI orchestrates the engine for you: it prepares configurations, pulls Docker images, starts benchmark runs, and streams live metrics in both interactive and headless modes.

---

## Project Layout

```
storage-performance-tool/
├── cli/     # Go CLI/TUI source (builds the `spt` binary)
└── engine/  # Java engine source (builds spt.jar and the Docker bundle)
```

The top-level repository represents the SPT product. The `cli` and `engine` directories have their own README files for deep dives, but most users interact only with the CLI binary. Direct use of the Java artifacts is reserved for contributors and CI builds.

---

## Quick Start

### Download a pre-built binary

Pre-built `spt` binaries for Linux, macOS, and Windows are published with each [GitHub Release](https://github.com/dell/storage-performance-tool/releases). Download the latest release for your platform, extract, and run:

```bash
# Example: Linux amd64
gunzip spt-*-linux-amd64.gz
chmod +x spt-*-linux-amd64
mv spt-*-linux-amd64 spt
```

### Prerequisites

- Docker (daemon running)

### Environment setup (optional but recommended)

SPT automatically reads `.env` files from the current directory — no manual sourcing required. Copy the example and fill in your defaults:

```bash
cp cli/.env.example .env
$EDITOR .env
```

Key variables:

```bash
S3_ENDPOINT=https://s3.example.com
S3_ACCESS_KEY=your-access-key
S3_SECRET_KEY=your-secret-key
S3_BUCKET=test-bucket

# For distributed runs, SPT uses SSH to launch and manage engine
# containers on remote hosts. List them as comma-separated user@host pairs:
HOSTS=root@node1,root@node2,root@node3
```

### Verify your environment

```bash
./spt verify
```

This checks localhost by default. When `HOSTS` is set, it verifies SSH connectivity, Docker availability, and port accessibility on each remote node.

---

## Running Your First Test

Run a local mock workload (no S3 endpoint required):

```bash
./spt run mock --duration 30s --threads 4 --auto-terminate-seconds 60
```

Run an S3 write workload:

```bash
./spt run write \
  --endpoints https://s3.example.com \
  --access-key "$S3_ACCESS_KEY" \
  --secret-key "$S3_SECRET_KEY" \
  --bucket test-bucket \
  --duration 2m \
  --threads 8 \
  --object-size 1MB \
  --auto-terminate-seconds 180
```

Run a mixed workload (concurrent GET/PUT/DELETE/STAT):

```bash
./spt run mixed \
  --endpoints https://s3.example.com \
  --access-key "$S3_ACCESS_KEY" \
  --secret-key "$S3_SECRET_KEY" \
  --bucket test-bucket \
  --duration 5m \
  --threads 16 \
  --object-size 1MB \
  --seed-objects 5000 \
  --auto-terminate-seconds 600
```

The default distribution is GET 45% / STAT 30% / PUT 15% / DELETE 10%. Use `--get-distrib`, `--put-distrib`, `--delete-distrib`, and `--stat-distrib` to customize (must sum to 100).

> ✅ Tip: Always set `--auto-terminate-seconds` for unattended runs to prevent long-lived containers from hanging CI jobs.

### TUI Navigation Highlights

- `g` toggles the live performance charts (hidden by default)
- `m` toggles the CLI message pane
- `TAB` switches between viewports
- Arrow keys or `j/k` scroll
- `q` or `Ctrl+C` exits the session

Headless mode activates automatically when no TTY is available; use `--headless` to force it manually.

For the full CLI reference (all flags, workload types, distributed options, RDMA, S3 Tables), see [`cli/docs/SPT_SYNTAX.md`](cli/docs/SPT_SYNTAX.md).

---

## Features

- **Unified Experience** -- SPT CLI orchestrates the engine inside Docker, so users never touch raw JARs.
- **Multi-Workload Support** -- write, read, list, mixed, and mock workloads out of the box, with S3 Tables (Iceberg) benchmarks.
- **Pluggable S3 Storage Drivers** -- choose the backend that fits your target: `default` (Netty), `aws` (AWS SDK v2), or `rdma` (hardware-accelerated). Select with `--s3-driver`.
- **S3 Multipart Upload** -- upload large objects in parallel parts with automatic abort on failure, per-part retry (up to 3 attempts), and per-part checksum support. Enable with `--part-size`.
- **S3 Checksum Validation** -- compute and send checksums on write requests with `--checksum` (`crc32`, `crc32c`, `sha1`, `sha256`). When combined with multipart upload, checksums are applied per part. Supported by both the Netty and AWS SDK drivers.
- **Data Compressibility & Deduplication Controls** -- shape generated object data for storage-efficiency benchmarks with `--object-data-compressibility` (0-100% target compressibility) and `--object-data-dedupable=false` (per-4KB anti-dedupe stamping).
- **Interactive & Headless** -- flip between a terminal UI for live monitoring and headless mode for CI/CD.
- **Distributed Runs** – preflight checks, node orchestration, and attachment support are built into the CLI.
- **Scenario Generation** – the CLI generates scenario files on the fly for the engine, sparing users from manual scripting.
- **Decoupled Write-Then-Read** – save item lists from write benchmarks (`--save-items`) and replay them in independent read passes (`--items-file`) with different concurrency, duration, or RDMA settings.
- **Mixed Workloads** – run GET, PUT, DELETE, and STAT operations concurrently with configurable weights (`spt run mixed`). Seed objects automatically, tune the operation distribution, and optionally clean up when done.
- **SigV4-first Authentication** – defaults to AWS Signature Version 4 with opt-in fallback for legacy targets.
- **S3-RDMA Acceleration** – optional hardware-accelerated data path for compatible storage targets (see [`cli/docs/S3_RDMA.md`](cli/docs/S3_RDMA.md)).
- **S3 Tables (Iceberg)** – benchmark Amazon S3 Tables across three vectors: snapshot commit TPS, compaction latency, and catalog discovery latency (see [`cli/docs/S3_TABLES.md`](cli/docs/S3_TABLES.md)).
- **Logging & Trace Capture** – configurable logs plus optional trace files for deeper troubleshooting.

---

## Storage Drivers

SPT supports multiple S3 storage driver backends. Use `--s3-driver` to select one:

| Driver | Flag value | Description |
|--------|-----------|-------------|
| Netty (default) | `default` or `netty` | Custom Netty-based HTTP client. Battle-tested, highest throughput on most targets. |
| AWS SDK | `aws` | AWS SDK for Java v2 with the Apache HTTP client. Broadest S3 compatibility, standard error handling. |
| RDMA | `rdma` | Hardware-accelerated data path for compatible storage targets (e.g., Dell ECS). |

Example:

```bash
# Use the AWS SDK driver for a read workload
./spt run read --s3-driver aws \
  --endpoints https://s3.example.com \
  --bucket test-bucket \
  --duration 2m --threads 8 --object-size 1MB
```

The driver flag works with all S3 workload types (write, read, list, mixed). See [`cli/docs/SPT_SYNTAX.md`](cli/docs/SPT_SYNTAX.md) for the full reference.

---

## S3-RDMA Acceleration

SPT supports an optional RDMA (Remote Direct Memory Access) data path for S3 workloads. When enabled, object transfers bypass the kernel networking stack for significantly lower latency and higher throughput on supported hardware. Add `--use-rdma` to any S3 write or read command to activate the RDMA driver.

**Requirements:** Linux, RDMA-capable NICs (Mellanox ConnectX-4+), an RDMA-capable storage target (e.g., Dell ECS), and `rdma-core` system packages.

See [`cli/docs/S3_RDMA.md`](cli/docs/S3_RDMA.md) for CLI flags, threshold tuning, architecture details, and troubleshooting.

---

## Build & Contribution Overview

- The **MIT License** applies to the entire project (see [`LICENSE`](LICENSE)).
- Contributions and bug reports are welcome via GitHub issues and pull requests.
- Support follows standard open-source conventions; there is no dedicated escalation path.

### Building from Source

Prerequisites: Go 1.25+, Java 21 (JDK), Docker, GNU Make.

```bash
git clone https://github.com/dell/storage-performance-tool.git
cd storage-performance-tool
make build-cli        # produces ./cli/spt
```

### CLI (Go) Tooling Highlights

- `make build` / `make run` – compile or run the CLI locally.
- `make test` / `make lint` – execute the Go test suite and two-pass lint policy (production vs. test rules).
- `make ci-local` – the local CI workflow (tidy, fmt-check, vet, build, lint policy).
- Dependencies use Go modules (no vendoring); ensure outbound network access or a pre-warmed module cache.

### Engine (Java) Tooling Highlights

- `./gradlew build` (from `engine/`) – builds `spt.jar` and supporting artifacts.
- `./gradlew :bundle:build` – produces the Docker-ready bundle consumed by the CLI.
- See `engine/README.md` for detailed module structure and advanced usage.

---

## Roadmap Snapshot

- Continue expanding CI/CD pipeline: automated release publishing of `spt` binaries and versioned Docker images to GitHub Releases and GHCR.
- Streamline documentation so contributors and users find SPT-first concepts across CLI and engine guides.

For detailed engineering notes and planning documents, browse the `docs/` directories inside `cli/` and `engine/`.

- [`cli/docs/SPT_SYNTAX.md`](cli/docs/SPT_SYNTAX.md) — Full CLI syntax reference (all commands, flags, and examples)
- [`cli/docs/S3_RDMA.md`](cli/docs/S3_RDMA.md) — S3-RDMA acceleration setup, tuning, and architecture
- [`cli/docs/S3_TABLES.md`](cli/docs/S3_TABLES.md) — S3 Tables (Iceberg) test vectors and usage guide

---

## Getting Help / Providing Feedback

- File bugs or feature requests via [GitHub Issues](https://github.com/dell/storage-performance-tool/issues).
- Join the conversation via [GitHub Discussions](https://github.com/dell/storage-performance-tool/discussions).
- Contributions should follow the standard fork-and-PR workflow.

---

## License

SPT is released under the [MIT License](LICENSE). By submitting a pull request, you agree that your contribution will be licensed under MIT.
