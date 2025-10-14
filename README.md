### WARNING: TECH PREVIEW RELEASE - This warning will be removed when CI/CD builds are in place and tool is fully ready for use

# Dell Storage Performance Tool (SPT)

The Dell **Storage Performance Tool (SPT)** is an open-source benchmark suite purpose-built for S3-compatible storage. SPT couples a task-oriented CLI/TUI with a high-performance engine so you can:

- spin up realistic object workloads in minutes;
- orchestrate single-node or distributed runs without handcrafting scripts;
- monitor live latency/throughput in an interactive terminal UI or headless CI logs;
- reuse the same configuration flow for both mock runs and production endpoints.

SPT packages two tightly integrated components:

- **SPT CLI/TUI** – a Go-based command-line and terminal UI experience for configuring, launching, and monitoring workloads.
- **SPT Engine** – a Java-based benchmarking engine that executes those workloads inside managed containers.

The CLI orchestrates the engine for you: it prepares configurations, builds or pulls Docker images, starts benchmark runs, and streams live metrics in both interactive and headless modes.

---

## Project Layout

```
storage-performance-tool/
├── cli/     # Go CLI/TUI source (builds the `spt` binary)
└── engine/  # Java engine source (builds spt.jar and the Docker bundle)
```

The top-level repository represents the SPT product. The `cli` and `engine` directories have their own README files for deep dives, but most users interact only with the CLI binary. Direct use of the Java artifacts is reserved for contributors and CI builds.

---

## Quick Start (from source)

Prerequisites:

- Go 1.23+
- Docker (daemon running)
- Java 21 (JDK) for building the engine bundle
- Gradle tooling (wrapper provided via `./gradlew`)
- GNU Make

```bash
# Clone the SPT source
git clone https://github.com/dell/storage-performance-tool.git
cd storage-performance-tool

# Build the Go CLI (produces ./spt)
make build-cli

# Copy and edit the sample environment file (optional but recommended)
cp cli/.env.example cli/.env
# Edit cli/.env with your endpoints, credentials, and defaults
$EDITOR cli/.env

# Load the environment variables when running from the repo root
set -a
source cli/.env
set +a
```
Validate your environment before running your first test:

```bash
./cli/spt verify 
```

Point `--test-hosts` at remote nodes (comma-separated) to confirm SSH, Docker, and port availability across the cluster. Use `--force-cleanup` if you want SPT to remove conflicting containers automatically.



Run a local mock workload (no S3 endpoint required):

```bash
./cli/spt run mock --duration 30s --threads 4 --auto-terminate-seconds 60
```

Run an S3 write workload:

```bash
./cli/spt run write \
  --endpoints https://s3.example.com \
  --access-key "$S3_ACCESS_KEY" \
  --secret-key "$S3_SECRET_KEY" \
  --bucket test-bucket \
  --duration 2m \
  --threads 8 \
  --object-size 1MB \
  --auto-terminate-seconds 180
```

> ✅ Tip: Always set `--auto-terminate-seconds` for unattended runs to prevent long-lived containers from hanging CI jobs.

### TUI Navigation Highlights

- `g` toggles the live performance charts (hidden by default)
- `m` toggles the CLI message pane
- `TAB` switches between viewports
- Arrow keys or `j/k` scroll
- `q` or `Ctrl+C` exits the session

Headless mode activates automatically when no TTY is available; use `--headless` to force it manually.

---

## Features

- **Unified Experience** – SPT CLI orchestrates the engine inside Docker, so users never touch raw JARs.
- **Multi-Workload Support** – write, list, mock today; additional operations land iteratively in upcoming releases.
- **Interactive & Headless** – flip between a terminal UI for live monitoring and headless mode for CI/CD.
- **Distributed Runs** – preflight checks, node orchestration, and attachment support are built into the CLI.
- **Scenario Generation** – the CLI generates scenario files on the fly for the engine, sparing users from manual scripting.
- **SigV4-first Authentication** – defaults to AWS Signature Version 4 with opt-in fallback for legacy targets.
- **Logging & Trace Capture** – configurable logs plus optional trace files for deeper troubleshooting.

---

## Build & Contribution Overview

- The **MIT License** applies to the entire project (see [`LICENSE`](LICENSE)).
- Contributions and bug reports are welcome via GitHub issues and pull requests.
- Support follows standard open-source conventions; there is no dedicated escalation path.

### CLI (Go) Tooling Highlights

- `make build` / `make run` – compile or run the CLI locally.
- `make test` / `make lint` – execute the Go test suite and two-pass lint policy (production vs. test rules).
- `make ci-local` – the local CI workflow (tidy, fmt-check, vet, build, lint policy).
- Dependencies use Go modules (no vendoring); ensure outbound network access or a pre-warmed module cache.

### Engine (Java) Tooling Highlights

- `./gradlew build` (from `engine/`) – builds `spt.jar` and supporting artifacts.
- `./gradlew :bundle:build` – produces the Docker-ready bundle consumed by the CLI.
- See `engine/README.md` for detailed module structure and advanced usage.

> 🚧 Planned Update: once GitHub Actions is in place, official release bundles (CLI binary, Docker image, engine JAR) will be published automatically to GitHub Releases and GHCR/Docker Hub. This README will be updated with download links when that workflow is live.

---

## Roadmap Snapshot

- Expand workload catalog beyond write/list/mock, including read/mixed/delete templates.
- Automate release publishing: build pipeline uploads `spt` binaries and pushes versioned Docker images (`ghcr.io/dell/storage-performance-tool`).
- Streamline documentation so contributors and users find SPT-first concepts across CLI and engine guides.

For detailed engineering notes and planning documents, browse the `docs/` directories inside `cli/` and `engine/`.

---

## Getting Help / Providing Feedback

- File bugs or feature requests via GitHub Issues (future public repo: `github.com/dell/storage-performance-tool`).
- Join discussions via GitHub Discussions once the repository is public.
- Contributions should follow standard fork-and-PR workflow.

---

## License

SPT is released under the [MIT License](LICENSE). By submitting a pull request, you agree that your contribution will be licensed under MIT.
