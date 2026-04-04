# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- **AWS SDK S3 storage driver (`s3-aws`)** — new pluggable driver built on AWS SDK for Java v2 with the Apache HTTP client. Supports CREATE, READ, UPDATE, DELETE, and LIST operations. Select with `--s3-driver aws`. (PR #61)
- **`--s3-driver` CLI flag** — choose between `default` (Netty), `aws` (AWS SDK), or `rdma` at runtime. Environment variable: `SPT_S3_DRIVER`. (PR #67)
- **S3 Multipart Upload enhancements** — per-part checksum verification, per-part retry (up to 3 attempts), automatic abort on failure, and MPU ABORT operation. Enable with `--part-size`. (PR #63)
- **Parallel range-read** for composite READ operations and per-phase latency breakdown in MPU metrics. (PR #63)
- **`--save-items` / `--items-file`** — decoupled write-then-read workflows: save item lists from write benchmarks and replay them in independent read passes. (PR #60)
- **`--s3-driver` support in test scripts** — `testread.sh`, `testlocal.sh`, and `testrdma.sh` accept `--s3-driver` for driver selection. (PR #67)
- **`--image` / `--skip-image-pull` in test scripts** — `testread.sh`, `testlocal.sh`, and `testmock.sh` now support Docker image override and pull control via flags or environment variables (`SPT_IMAGE`, `SPT_SKIP_IMAGE_PULL`).
- **Storage Drivers section in README** — root README now documents the available S3 drivers and the `--s3-driver` flag.

### Fixed

- **s3-aws error classification** — `classifyFailure()` now maps HTTP 504 to `FAIL_TIMEOUT` and 507 to `RESP_FAIL_SPACE`, matching the Netty driver's behavior. (PR #69)
- **s3-aws error logging** — exceptions in `invokeNio` are now logged at DEBUG/TRACE level instead of being silently swallowed. `resolveBucketName` errors are also logged. (PR #69)
- **s3-aws recycle fix** — strip `dstPath` prefix from mutated item names during operation recycling to prevent path corruption. (PR #61)
- **s3-aws endpoint resolution** — read port and SSL settings from standard config paths instead of hardcoded defaults. (PR #61)
- **Netty exclusion from s3-aws shadow JAR** — prevents Netty 4.1.x / 4.2.x version conflicts at runtime. (PR #61)
- **NioStorageDriverBase semaphore permit leak** — fix permit leak that could stall the driver under sustained load. (PR #65)
- **JVM performance args** — add tuned JVM settings to the bundled `run.sh` launch command. (PR #64)
- **items.csv path** — use engine `home_dir` via ThreadContext for correct artifact placement. (PR #62)
- **LoadStepClient test regression** — fix 10-minute test hang caused by remote node retries. (PR #57)
- **Docker publish CI gate** — verify all CI checks pass before publishing images. (PR #55)
- **README quickstart** — fix download instructions for pre-built binaries. (PR #56)

### Changed

- **Dependency updates** — Scala 2.13.18, Jackson 2.21.2, Guava 33.5.0-jre, Mockito 5.23.0, commons-codec 1.21.0, commons-lang 3.20.0, fastutil 8.5.18, and others. AWS SDK dependencies centralized in `libs.versions.toml`. Added `com.github.ben-manes.versions` Gradle plugin for dependency management. (PR #68)
- **JitPack removed** — akurilov JARs vendored locally, eliminating the JitPack repository dependency. (PR #58)
- **Gradle Actions** — bumped `gradle/actions` from 5 to 6. (PR #59)

## [5.5.0] - 2026-02-21

public release with CLI/TUI, engine orchestration, write/read/list/mock workloads, S3-RDMA acceleration, S3 Tables benchmarking, distributed multi-node runs, and headless CI mode.
