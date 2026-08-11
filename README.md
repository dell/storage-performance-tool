# Dell Storage Performance Tool (SPT)

The Dell **Storage Performance Tool (SPT)** is an open-source benchmark suite
for S3-compatible storage. It combines a task-oriented CLI/TUI with a
high-performance engine so you can:

- launch realistic object workloads without handcrafting engine scenarios;
- orchestrate local or distributed runs;
- monitor throughput and latency interactively or in headless automation; and
- use the same workflow for mock tests and real S3 endpoints.

SPT consists of two integrated components:

- **SPT CLI/TUI**: a Go application for configuring, launching, and monitoring
  workloads.
- **SPT Engine**: a Java benchmarking engine that the CLI normally runs in
  managed containers. Advanced users can also run the engine standalone.

## Quick Start

### Download a pre-built binary

Pre-built `spt` binaries for Linux, macOS, and Windows are published with each
[GitHub Release](https://github.com/dell/storage-performance-tool/releases).
Download the archive for your platform, extract it, and make the binary
executable when required:

```bash
# Example: Linux amd64 (use linux-arm64 on Arm systems)
gunzip spt-*-linux-amd64.gz
chmod +x spt-*-linux-amd64
mv spt-*-linux-amd64 spt

# Later, check for CLI updates
./spt update --check
```

### Prerequisites

- Docker with a running daemon.
- An SSH client and key-based access for distributed runs.
- Credentials for a dedicated test target when running real S3 workloads.

### Engine image selection

The CLI runs a matching SPT engine container:

1. Release builds select
   `ghcr.io/dell/storage-performance-tool:v<cli-version>`.
2. Local development builds select
   `ghcr.io/dell/storage-performance-tool:spt_dev`.
3. `--spt-image <ref>` or `SPT_IMAGE` overrides either default verbatim.

SPT never silently falls back to `latest`; a missing version-matched image is
an error. Development images are local-only and are not pulled. Build the
default development image from the repository root with:

```bash
make -C engine docker-local
```

For distributed development, preload that image on the workers with
`engine/tools/push-worker-image.sh`.

### Configure a test target

SPT automatically reads `$HOME/.env` and `.env` in the current directory; no
manual sourcing is required. Create `.env` in the directory where you run SPT.
Common settings are:

```dotenv
S3_ENDPOINT=https://s3.example.com
S3_ACCESS_KEY=your-access-key
S3_SECRET_KEY=your-secret-key
S3_BUCKET=test-bucket

# Optional: comma-separated user@host entries for distributed runs
HOSTS=user@node1,user@node2,user@node3
```

Verify the local environment and configured target hosts:

```bash
./spt verify
```

With no `HOSTS` setting, this checks localhost. When `HOSTS` is set, SPT also
checks SSH connectivity, Docker availability, and required ports on each
remote node.

### Run your first test

Start with a local mock workload, which requires no S3 endpoint:

```bash
./spt run mock --duration 30s --threads 4
```

> **Data safety:** S3 write and mixed workloads mutate the target, and mixed
> workloads include DELETE operations. Use a dedicated benchmark bucket or an
> isolated prefix; never point them at production data.

After configuring `.env`, run an S3 write workload in an isolated prefix:

```bash
./spt run write \
  --prefix spt-quickstart/write/ \
  --duration 2m \
  --threads 8 \
  --object-size 1MB
```

For unattended execution, add `--headless` and a bounded
`--auto-terminate-seconds` value. Add `--cleanup` when you want SPT to remove
objects created by the workload.

### TUI navigation

- `g` toggles the live performance charts, which start hidden.
- `m` toggles the CLI message pane.
- `Tab` switches between viewports.
- Arrow keys or `j`/`k` scroll.
- `q` or `Ctrl+C` exits the session.

Headless mode activates automatically when no TTY is available; use
`--headless` to force it.

## Core Capabilities

- **Workload coverage**: run write, read, write-verify, read-verify, list,
  mixed, mock, and S3 Tables workloads.
- **Pluggable S3 drivers**: select the Netty, AWS SDK v2, or optional RDMA
  backend with `--s3-driver`.
- **Multipart uploads and checksums**: exercise multipart object creation and
  request checksums including CRC32, CRC32C, SHA-1, SHA-256, and CRC64-NVME.
- **Persisted-data verification**: write SHA-256 integrity metadata and verify
  stored objects later with resumable manifests and corruption-specific exit
  status. See [S3 integrity testing](cli/docs/S3_INTEGRITY.md).
- **Data-shaping controls**: tune compressibility and deduplication resistance
  for storage-efficiency tests.
- **Distributed execution**: run preflight checks and orchestrate benchmark
  containers across multiple clients.
- **Interactive and automated operation**: monitor runs in the TUI or emit
  headless logs and result artifacts for CI/CD.
- **Replay and decoupled workflows**: replay archived workloads or save object
  lists for independent read passes.
- **Modern transport options**: use SigV4-first authentication and optional
  post-quantum TLS negotiation on supported Netty HTTPS paths.
- **Specialized backends**: exercise compatible S3-RDMA targets and Amazon S3
  Tables workloads.

## Storage Drivers

Use `--s3-driver` to select an S3 backend:

| Driver | Flag value | Description |
|---|---|---|
| Netty (default) | `default` or `netty` | SPT's asynchronous Netty HTTP implementation. |
| AWS SDK | `aws` | AWS SDK for Java v2 using the Apache HTTP client. |
| RDMA | `rdma` | Optional RDMA data path for compatible Linux clients and storage targets. |

For example, run a read workload through the AWS SDK driver while using the
target settings from `.env`:

```bash
./spt run read --s3-driver aws \
  --duration 2m \
  --threads 8 \
  --object-size 1MB
```

Driver and workload compatibility differs by feature. See the
[CLI syntax reference](cli/docs/SPT_SYNTAX.md) for the complete matrix. RDMA
acceleration requires compatible hardware and currently applies to write and
read data paths; see the [S3-RDMA guide](cli/docs/S3_RDMA.md).

## Documentation

- [CLI overview](cli/README.md) - installation, commands, configuration, and
  distributed operation.
- [CLI syntax reference](cli/docs/SPT_SYNTAX.md) - all commands, flags, and
  examples.
- [S3 integrity testing](cli/docs/S3_INTEGRITY.md) - persisted-object write/read
  verification, artifacts, and automation contracts.
- [Archived workload replay](cli/docs/REPLAY.md) - import and replay SPT or
  legacy Mongoose workloads.
- [S3-RDMA](cli/docs/S3_RDMA.md) - setup, tuning, architecture, and
  troubleshooting.
- [S3 Tables](cli/docs/S3_TABLES.md) - Iceberg benchmark vectors and usage.
- [Post-quantum TLS](cli/docs/PQC_TLS.md) - negotiation behavior and engine
  configuration.
- [Engine overview](engine/README.md) - standalone usage, architecture, and
  extension development.

## Project Layout

```text
storage-performance-tool/
├── cli/     # Go CLI/TUI source; builds the spt binary
└── engine/  # Java engine source; builds spt.jar and the Docker bundle
```

Most users interact only with the CLI. Contributors and advanced users can use
the component documentation for lower-level build, runtime, and extension
details.

## Building and Contributing

Run `make setup` from the repository root to check the required Go, Java,
Docker, and build tooling for full-project development.

For CLI-only development:

```bash
make -C cli build
make -C cli test
make -C cli lint
make -C cli ci-local
```

For engine development:

```bash
make -C engine bundle
make -C engine test
make -C engine lint
```

Use the root `make build`, `make test`, and `make lint` targets when working
across both components. First-time builds may require network access or
pre-populated Go and Gradle caches.

See the [CLI contribution guide](cli/CONTRIBUTING.md) and
[engine contribution guide](engine/CONTRIBUTING.md) for component-specific
guidance.

## Getting Help and Providing Feedback

- File bugs and feature requests through
  [GitHub Issues](https://github.com/dell/storage-performance-tool/issues).
- Submit changes using the standard fork-and-pull-request workflow.

## License

SPT is released under the [MIT License](LICENSE). By submitting a pull request,
you agree that your contribution will be licensed under MIT.
