# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [5.7.3] - 2026-04-13

### Fixed

- **`result.xml` well-formedness fix** — replace log4j2 header/footer with explicit wrapper-tag management in `MetricsManagerImpl`. The engine now logs `<result>` and `</result>` tags at step lifecycle boundaries, with safety cleanup in `doClose()` for abnormal shutdowns. The CLI's artifact fetcher includes a `normalizeResultXML()` safety net that strips stale wrappers and re-wraps entries. This eliminates the timing bug where log4j2's async rollover wrote `</result>` before the entries, producing malformed XML.

## [5.7.2] - 2026-04-13

### Fixed

- **`result.xml` format restored** — revert incorrect 5.7.1 change that collapsed per-entry `<result>` wrappers into a single document-level wrapper. Each metrics entry is again wrapped individually as `<result><result .../></result>`, matching the original Mongoose v3-compatible format.

## [5.7.1] - 2026-04-13

### Added

- **`--checksum` CLI flag** — enable per-object (and per-part for MPU) checksum validation with `crc32`, `crc32c`, `sha1`, or `sha256`. Environment variable: `SPT_CHECKSUM`. Works with both the Netty and AWS SDK S3 drivers. (PR #80)
- **S3-AWS driver checksum support** — the `s3-aws` driver now reads `storage.checksum.enabled` / `storage.checksum.algorithm` from config and sets the corresponding `ChecksumAlgorithm` on `PutObjectRequest` via the AWS SDK flexible checksum API. Supported algorithms: CRC32, CRC32C, SHA1, SHA256. (PR #80)
- **Netty S3 driver MPU checksum completeness** — multipart upload init requests now include the `x-amz-checksum-algorithm` header, the response handler captures per-part checksum headers, and the complete-upload XML body includes per-part checksum elements. (PR #80)
- **`result.xml` output** — engine now generates a `result.xml` artifact with per-operation metrics in a format compatible with Mongoose v3, including duration, latency, concurrency, byte counts, and success/fail tallies. The CLI fetches `result.xml` and `result-threshold.xml` automatically. (PR #81)

### Fixed

- **`result.xml` well-formedness** — closing XML tags moved from the log4j2 pattern to the appender footer so the output is always valid XML. (PR #81)

### Changed

- **Bump `actions/github-script` from 8 to 9** in the `github-actions` dependency group. (PR #82)

## [5.7.0] - 2026-04-09

### Changed

- **Docker runtime upgraded from JDK 21 to JDK 25** — enables compact object headers (JEP 519) and improved virtual thread scheduling for better small-object throughput. (PR #76)
- **Read throughput optimization** — replace `ArrayBlockingQueue` with lock-free `ConcurrentLinkedQueue` for operation recycling, reducing contention at high concurrency. (PR #73)
- **Fast-recycle dispatch** — short-circuit dispatch path for low-concurrency workloads; gate VT quiesce on concurrency level to avoid T8/T32 regression. (PR #74)
- **Write throughput optimization** — replace timed dispatch awaits with untimed to reduce VT-unparker overhead, eliminate per-request `Matcher` allocation in S3 header normalization, add JVM tuning flag `-XX:-DoJVMTIVirtualThreadTransitions`. (PR #75)

### Fixed

- **Backpressure signal-loss race** — fix `await()` signal-loss race condition in dispatch task that could cause stalls under sustained load. (PR #75)
- **Connection failure permit leak** — fix semaphore permit leak when Netty connections fail during setup. (PR #75)
- **RDMA native library not packaged in release artifacts** — release workflow was missing RDMA build dependencies, so `libspt_rdma.so` was never compiled into the shadow JAR. Added build-time logging when the native build is skipped. (PR #79)

### Tests

- **Netty and HTTP driver test coverage** — add unit tests covering HTTP response status mapping, data URI path generation, range-header serialization, exception-to-status mapping, and chunked NIO streams. HTTP module line coverage 23% to 43%, Netty module 30% to 38%. (PR #77)

## [5.6.0] - 2026-04-04

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
- **DataItemInputStream mark/reset** — save and restore `doneSize` counter so the stream can be re-read after reset, required by the upgraded AWS SDK's retry mechanism. (PR #71)
- **LoadStepClient test regression** — fix 10-minute test hang caused by remote node retries. (PR #57)
- **Docker publish CI gate** — verify all CI checks pass before publishing images. (PR #55)
- **README quickstart** — fix download instructions for pre-built binaries. (PR #56)

### Changed

- **AWS SDK upgraded from 2.21.29 to 2.42.28** — ~2.4 years of improvements; s3-aws driver updated to use `DataItemInputStream` (supports mark/reset) and `RequestBody.fromFile()` for compatibility. (PR #71)
- **Dependency updates** — Scala 2.13.18, Jackson 2.21.2, Guava 33.5.0-jre, Mockito 5.23.0, commons-codec 1.21.0, commons-lang 3.20.0, fastutil 8.5.18, and others. AWS SDK dependencies centralized in `libs.versions.toml`. Added `com.github.ben-manes.versions` Gradle plugin for dependency management. (PR #68)
- **JitPack removed** — akurilov JARs vendored locally, eliminating the JitPack repository dependency. (PR #58)
- **Gradle Actions** — bumped `gradle/actions` from 5 to 6. (PR #59)

## [5.5.0] - 2026-02-21

public release with CLI/TUI, engine orchestration, write/read/list/mock workloads, S3-RDMA acceleration, S3 Tables benchmarking, distributed multi-node runs, and headless CI mode.
