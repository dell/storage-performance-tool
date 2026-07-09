# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- **`load-op-retryLimit`** — bounds `load-op-retry` (previously unlimited) to a configurable maximum attempts per operation (default 10, matching common S3-client SDK retry defaults such as minio-go's `MaxRetry`; must be `>= 0`, and `0` disables retry even if `load-op-retry` is `true`). A retried operation is redispatched after a full-jitter exponential backoff (200ms base, 1s cap) instead of immediately. Once an operation exceeds its retry limit it is counted as a failure (`CountFail`) as before. Previously, `load-op-retry` had no attempt cap or backoff at all: an operation that kept failing (e.g. from a recurring transient network stall) would retry indefinitely, which could compound with the socket/idle timeouts above into a longer effective stall than not retrying at all, and made the `load-op-limit-fail-count`/`load-op-limit-fail-rate` circuit breaker unable to ever fire (it only counts operations that reach `CountFail`, which a permanently-retrying operation never does).
- `load-op-retry` now only retries transient failure statuses (`FAIL_IO`, `FAIL_TIMEOUT`, `FAIL_UNKNOWN`, `RESP_FAIL_UNKNOWN`, `RESP_FAIL_SVC`) — matching common S3-client SDK retry policy (minio-go/aws-sdk-cpp). Permanent failures (auth, not-found, client request errors, data corruption, out-of-space) are counted as failed immediately, since retrying them wastes the retry budget on something that can never succeed and only delays an actionable result.
- `load-op-retry` is now validated against the configured load generator at step start: generators that don't support requeueing a failed operation for retry (currently: mixed-mode) reject the configuration with a clear error instead of silently dropping failed operations — neither retried nor counted as failed.
- Load generators have a new, dedicated retry-redispatch path (`LoadGenerator#retry`), separate from the existing recycle queue used by duration-based read-loop (`load-op-recycle-mode`) workloads. A retry is a re-attempt of an operation already counted once at its original dispatch, not a new unit of work, so it is never gated by, or counted against, `load-op-limit-count`, and is drained with priority ahead of the generator's normal count-limit/throttle-gated dispatch logic.

### Changed

- **Binary byte-unit labels** — SPT now labels 1024-based byte sizes and bandwidth with IEC units (`KiB`, `MiB`, `GiB`, `MiB/s`, XML `MiBps`) instead of decimal-style labels (`KB`, `MB`, `GB`, `MB/s`, XML `MBps`). New result CSV files use `BWAvg[MiB/s]` and `BWLast[MiB/s]`, XML result files use `bw_unit="MiBps"`, and XML `filesize` values now render IEC size labels, including ranges such as `1KiB-2KiB`. The current CLI can still read legacy CSV totals with `BWAvg[MB/s]` and `BWLast[MB/s]`, but older CLIs and external scripts or dashboards that key on the exact old labels should be updated.

### Fixed

- **Live/status bandwidth conversion** — live TUI metrics and `spt status` now divide engine byte rates by 1 MiB before displaying `MiB/s`; previously those paths divided by 1,000,000 while using a binary-style label.
- **`load-op-retry` could hang a finite, non-recycle workload forever.** The load generator used to internally treat `load-op-retry` like recycle mode (so it would keep polling for late-arriving retries after item input exhaustion instead of ever signaling "finished"), and completion detection had no way to distinguish "no work has happened yet" from "there was never any work to do" for a genuinely empty item input. Both are fixed by the new dedicated retry path above plus a completion check based on comparing terminal results against operations actually generated (which correctly accounts for retries still resolving), rather than relying on the generator's own self-reported stopped state.
- **A retryable failure could be forced to a terminal failure with zero retry attempts used** if its original dispatch happened to exhaust `load-op-limit-count`, *or* if it was simply the generator's last dispatch before its item input ran out — in both cases the generator used to self-stop the instant it had handed every generated operation to the driver, regardless of whether any of those dispatches had actually completed yet (a real, especially asynchronous/cooperative, driver reports completion well after accepting a request), let alone whether a retry decision had been made for one that failed. The generator now only self-stops on either signal when `load-op-retry` is off; with it on, it defers entirely to confirmation (the terminal-results-vs-generated comparison above) that nothing is still in flight before stopping.
- **A retry could be abandoned if the generator stopped while it was in flight** (an explicit shutdown, or the generator reaching its own natural completion) — the operation could end up queued somewhere nothing would ever poll again. A scheduled retry checks whether the step has begun shutting down or the generator has otherwise already stopped right before redispatching, and resolves to a definite failure instead of retrying into a dead/dying generator if either is true. A step stopping proactively cancels any retry still in its backoff delay (resolving it to a definite failure immediately), waits for any retry that was already past cancellation to finish redispatching, and then - since redispatching only enqueues into the generator, it doesn't wait for the generator to actually hand the operation off to the storage driver - waits (boundedly) again for the generator to actually drain that redispatch before it is stopped, closing the window where a just-enqueued retry could otherwise be abandoned in the generator's own internal queue. If that bounded wait times out (the generator is stuck, output is backpressured, a throttle keeps denying permits, or the driver simply cannot accept the redispatch), whatever is still queued is now actively drained and terminal-failed directly, rather than logging a warning and stopping the generator anyway - which would have left it neither retried, nor redispatched, nor counted as failed, until a later, unrelated internal queue-clearing step silently discarded it with no outcome ever recorded. Retry cancellation also now uses an explicit per-retry state machine rather than relying on `CompletableFuture#cancel`'s return value: a successful `cancel(false)` does not reliably mean a `runAsync`-scheduled task's body will never run, so treating it as sufficient proof could let both the cancellation path and the task's own body resolve (e.g. count as failed, or retry) the same operation a second time.
- **A retried operation bypassed configured rate/index throttles**, unlike its original dispatch — only `load-op-limit-count` is intentionally bypassed for a retry (it's a re-attempt of something already counted once, not new work); a burst of retries during a failure storm should still be paced the same as everything else, or actual request rate could exceed the configured limit by roughly the original traffic plus retry traffic.
- **A `PENDING` operation result (the driver couldn't finish it and wants it attempted again) could be silently stranded on a non-recycle-mode workload** — it was always recycled via the same queue duration-based recycle-mode workloads drain, but the generator only drains that queue when true recycle-mode is actually enabled, while the accounting used to advance immediately regardless, as though the operation had actually resolved. With `load-op-retry` enabled, `PENDING` now routes through the same dedicated, always-drained retry path as a retried failure instead, and isn't counted until it actually reaches a terminal outcome. Note this redispatch is immediate and unbounded (no backoff delay, not counted against `load-op-retryLimit`), unlike a retried failure — `PENDING` represents the driver's own "not finished yet" signal rather than a definite failure worth eventually giving up on, so it keeps the same unlimited-immediate-retry contract true recycle-mode already gives it.
- **`load-op-retry`'s counter wasn't reset on success**, so a recycled (duration-based read-loop) or Netty fast-recycled operation that failed and retried once before eventually succeeding kept an elevated retry count forever across every subsequent, unrelated successful cycle — eventually exhausting its retry budget from accumulated non-consecutive failures rather than genuine repeated failures.
- **Unbounded distributed-coordination RMI waits** — the production JVM defaults (Docker entrypoint and `run.sh`) now set `-Dsun.rmi.transport.tcp.responseTimeout=10000`. Previously a transient network stall on the RMI connection used for entry-worker coordination (start/stop/await polling) could block a test indefinitely instead of failing fast into the existing retry/backoff path, inflating run duration and deflating measured throughput with no corresponding failure recorded.
- **Netty idle timeout too long to ever catch a stuck operation in a short run** — `storage-net-timeoutMilliSec` default lowered from 300000ms (5 minutes) to 30000ms (matches the sibling S3-AWS driver's own `socketTimeoutMs` default). This is an idle timeout (fires only on zero read/write activity), so it does not penalize large-but-progressing transfers. A stuck S3 operation previously had to survive 5 minutes before Netty's own `IdleStateHandler` would fail it — far longer than most test runs — so it would silently vanish uncounted rather than being recorded as a failure. Workloads that legitimately need more idle headroom (e.g. LIST against large buckets, per `engine/tools/listtest.local.sh`'s existing `LIST_HTTP_TIMEOUT_MS` override) should set `--storage-net-timeoutMilliSec` explicitly.

## [5.11.2] - 2026-06-29

### Changed

- **Dependency updates** — bumped `golang.org/x/tools` in the Go dependency group.

### Fixed

- **Local auto-results node log capture** — single-host local container runs now persist engine-side logs under a writable results-backed `.node-home` bind mount, allowing `--save-items` and other log artifacts to be written successfully without requiring host/container UID alignment.

## [5.11.1] - 2026-06-26

### Added

- **Bounded read-phase shuffle controls** — `spt run read` now supports `--shuffle` and `--shuffle-batch-size` to widen read randomness within the `ReadLoad` phase without globally exposing unbounded `load.batch.size`. The batch override is read-only, defaults to a bounded window when omitted, and is capped to avoid pathological buffer growth.

### Fixed

- **External items files in Docker runs** — `--items-file`, mixed `--read-items-file`, and mixed `--delete-items-file` are now rewritten to stable container paths and mounted into local or remote Docker test containers, allowing saved item catalogs to be reused by later read/delete workflows.
- **Large auto-results artifacts** — CLI artifact retrieval now downloads engine log artifacts larger than the engine log page size with HTTP range requests, preserving large `items.csv` files instead of truncating them at the first page.

## [5.11.0] - 2026-06-25

### Added

- **Archived workload replay** — added `spt replay --from <archive-folder-url>` to import archived SPT or legacy Mongoose workload artifacts, convert archived JSON or JavaScript scenarios, sanitize credentials, remap target environments, apply supported safe file-preparation transforms, generate equivalent S3 workloads, and launch single-host, remote-host, or distributed replay runs with trace capture and auto-results.
- **Linux arm64 CLI release binaries** — official GitHub releases now publish `spt-<version>-linux-arm64.gz` alongside the existing CLI assets, with checksum coverage for every packaged CLI binary.
- **SPT CLI self-update command** — added `spt update` for checking GitHub Releases and installing verified CLI release binaries. The command supports lightweight `--check` probes with distinct exit codes, prerelease selection, `--output` downloads, checksum verification, archive hardening, pre-download replace-access checks, and hardened GitHub transport. Windows running-binary replacement is intentionally disabled for now; use `--output` to download a verified Windows binary.
- **READ/LIST TTFB reporting** — Time to First Byte is now tracked for body-returning READ and LIST operations, including local filesystem reads and AWS SDK S3 LIST responses. Live/headless metrics and final summaries report TTFB only when samples are available, avoiding misleading mixed-workload or non-body-operation TTFB values.

### Changed

- **Mixed workload reporting** — mixed runs now render per-operation breakdowns in console/TUI final summaries, correct mixed interval/total CSV row shapes, suppress false artifact-health warnings when seed steps intentionally skip summary persistence, and emit final auto-results summaries after shutdown completes.
- **Release artifact gating** — GitHub release publication now waits for the published multi-architecture engine image before attaching release artifacts, and prerelease Docker tag validation now handles prerelease tags correctly.
- **Dependency updates** — refreshed Go indirect modules, GitHub Actions dependencies, and JVM dependencies for the 5.11.0 release prep, including Jackson Core/Databind/YAML 2.22.0 with Jackson Annotations 2.22, AWS SDK 2.46.17, AWS CRT 0.47.1, Bouncy Castle 1.84, JUnit Jupiter 5.14.4, SnakeYAML 2.6, commons-codec 1.22.0, Javassist 3.32.0-GA, hadoop-shaded-guava 1.5.0, and stax2-api 4.3.0.

## [5.10.4] - 2026-06-01

### Changed

- **Version-pinned default engine image** — the CLI now defaults to the matching versioned engine image instead of the moving `latest` tag, making CLI/engine behavior reproducible across runs. Use `--spt-image` to select a different engine image explicitly.

### Fixed

- **Headless completion detection** — fixed premature `--headless` completion when transient aggregate metrics briefly reported `Completed`, while preserving running state through temporary concurrency gaps. (PR #123)
- **Headless auto-results shutdown** — fixed a shutdown race that could prevent headless runs from reliably collecting final auto-results artifacts. (PR #125)
- **Headless multi-step read runs** — fixed multi-step completion detection so seeded read workflows wait for the actual read step instead of treating the seed step as the terminal workload. (PR #126)

## [5.10.3] - 2026-05-23

### Fixed

- **MPU completion race handling** — fixed race condition in multipart upload completion logic that could cause incorrect finalization timing. (PR #121)
- **CLI lint issues** — resolved linting warnings in CLI code. (PR #121)
- **CLI MPU batch override removal** — removed unnecessary CLI-level MPU batch override that conflicted with engine-level configuration. (PR #121)
- **MPU completion timing** — corrected timing logic in MPU completion operations to ensure accurate metrics reporting. (PR #121)
- **Terminal metrics refresh** — fixed TUI metrics display refresh to ensure real-time updates are rendered correctly. (PR #121)
- **Composite read batch dispatch** — fixed dispatch logic for composite read operations to prevent batch handling errors. (PR #121)
- **MPU scheduling and finalization** — improved MPU task scheduling and finalization to handle edge cases in distributed runs. (PR #121)
- **Jar-based MPU test helpers** — updated test helper utilities for jar-based MPU testing to improve reliability. (PR #121)
- **Multi-host headless completion hangs** — fixed issue where multi-host headless runs would hang after completion instead of exiting cleanly. (PR #120)

## [5.10.2] - 2026-05-19

### Fixed

- **Config YAML load.op key confusion** — config.yaml dumps now trim irrelevant `load.op.weight` keys based on step type. `MixedLoad` steps show only `load.op.weights` (distribution map); non-mixed steps (CreateLoad, ReadLoad, etc.) show only scalar `load.op.weight`. This eliminates the confusing dual-key output that made it impossible to distinguish step types from config dumps alone. (PR #118)

## [5.10.1] - 2026-05-08

### Fixed

- **S3 MPU object URI mismatch with trailing output paths** — fixed HTTP data URI joining when `item-output-path` ends with `/` and generated item names are bare. MPU init, part upload, complete, and abort requests now use the same object URI, avoiding backend-specific `404 Not Found` failures on storage systems that treat repeated slashes as significant object key characters. (PR #115)

## [5.10.0] - 2026-05-07

### Added

- **Real-time latency percentiles** — latency is now tracked in memory and reported as percentiles (`p50`, `p90`, `p99`, `p99.9`) through the Metrics API and final CLI/TUI summaries. The TUI now shows `p50` in place of mean.
- **Time-to-first-byte tracking** — TTFB is now recorded for body-returning operations, including LIST, and included in final reporting when available.

### Changed

- **Mixed workload distribution config key** — renamed the mixed workload engine config from `load.op.weight.{get,put,delete,stat}` to `load.op.weights.{get,put,delete,stat}` to avoid colliding with `WeightedLoad`'s scalar `load.op.weight` key. CLI-generated `spt run mixed` scenarios use the new key automatically; hand-written `MixedLoad` scenarios should be updated.
- **Trace output capture** — trace runs now record invocation details and attach trace output to run metadata and auto-results.

### Fixed

- **Mixed workload weighting and TTFB edge cases** — corrected mixed workload weighting behavior and tightened TTFB start timing so valid zero-length body-returning operations are not dropped.

## [5.9.2] - 2026-05-05

### Fixed

- **S3 multipart NPE on error responses** — `S3ResponseHandler` no longer throws `NullPointerException` when an MPU part PUT returns a non-2xx response without an `ETag` header. The handler previously attempted to record a null ETag into the parent task's context map (a `ConcurrentHashMap`, which rejects null values), surfacing as misleading "Premature channel closure" warnings that masked the real upstream 4xx/5xx failure. (PR #105)
- **Item name corruption with versioning enabled** — when versioning was enabled and a response omitted the `x-amz-version-id` header, the literal string "null" was appended to the item name (compounding `~null~null~…` across retries). Both header reads now early-out cleanly when absent. (PR #105)

## [5.9.1] - 2026-05-01

### Added

- **CRC64-NVME checksum support** — added support for the `crc64-nvme` checksum algorithm across all S3 drivers (Netty, AWS SDK, and RDMA). The Netty driver now includes explicit checksum strategy metadata and multipart checksum wiring for CRC64-NVME. The CLI `--checksum crc64-nvme` flag is now accepted for all S3 drivers (previously AWS-only). Added RDMA checksum parity tests covering CRC32, CRC32C, and CRC64-NVME known vectors. (PRs #101, #102)

## [5.9.0] - 2026-04-29

### Added

- **Post-quantum TLS handshake support** — the Netty HTTPS driver now offers hybrid post-quantum key exchange groups (`X25519MLKEM768`, `x25519`, `secp256r1`) during TLS 1.3 handshakes via the Bouncy Castle JSSE provider. Enabled by default in `prefer` mode with automatic classical fallback. Covers the `default`, `rdma`, and S3 Tables drivers. Engine config keys: `storage.net.ssl.pqcMode` (`off`/`prefer`/`require`), `storage.net.ssl.jsseProvider`, `storage.net.ssl.namedGroups`. See [`cli/docs/PQC_TLS.md`](cli/docs/PQC_TLS.md).
- **Data compressibility control** (`--object-data-compressibility`) — set a target compressibility percentage (0-100) for generated object data. Each 4KB chunk is split into pseudo-random and zero-filled portions, forcing the storage system's compression to evaluate every block. Engine config key: `item.data.input.compressibility`. (env: `SPT_OBJECT_DATA_COMPRESSIBILITY`)
- **Anti-dedupe stamping** (`--object-data-dedupable=false`) — stamp every 4KB of generated data with a 16-byte header (64-bit object identifier + 64-bit stream offset) to practically eliminate inline deduplication on typical fixed/variable chunking systems. Stamps are deterministic and consistent across all transfer paths (Netty, SSL, AWS SDK, RDMA). Engine config key: `item.data.dedupable`. (env: `SPT_OBJECT_DATA_DEDUPABLE`)
- **MPU concurrency limits** — add `--mpu-object-concurrency` and `--mpu-part-concurrency` flags to control object-level and part-level concurrency for multipart uploads. Prevents resource exhaustion and improves throughput control. Engine config keys: `load.op.create.mpu.objectConcurrency` and `load.op.create.mpu.partConcurrency`.
- **AWS CRT driver optimization** — optimize s3-aws storage driver to use AWS CRT (Common Runtime) library for improved performance and efficiency.

### Fixed

- **Post-5.6.0 AWS Driver performance regression** — fix performance issue introduced after 5.6.0 in the s3-aws driver.
- **Mixed-mode generator correctness** — fix correctness issues in mixed workload generator.
- **headObject rebase fix** — fix headObject to use s3AsyncClient after rebase.
- **Async stamp buffer lifetime race** — fix race condition in async stamp buffer lifetime management.
- **MPU init condition bug** — resolve MPU init condition bug in complete method.
- **Operation timing fields reset** — reset operation timing fields when generating MPU complete/abort requests.
- **Prometheus metric family names** — stabilize prometheus metric family names for consistency.
- **Operation result timing snapshot test** — stabilize operation result timing snapshot test.

### Changed

- **MPU dispatch backpressure** — harden coop MPU dispatch backpressure for better flow control.
- **Multipart limit warning noise** — reduce multipart limit warning noise.
- **Deduplication stamping** — use 64-bit object id in dedupe stamps for larger object space.

## [5.8.0] - 2026-04-17

### Added

- **Mixed Workload Benchmarking Support** — added native support for testing mixed operations simultaneously (`READ`, `CREATE`, `DELETE`, `STAT`). The CLI now exposes a `spt run mixed` command and scenario builder, supporting dynamic percentage distributions (e.g., `GET=45%`, `PUT=15%`, `DELETE=10%`, `STAT=30%`).
- **MixedLoad Engine Extension** — new native load step extension. Implements a single-generator, multi-operation state machine. Employs a non-blocking `deleteQueue` directly fed by `CREATE` completions, bounded spin locks, and re-rolls empty queues uniformly to prevent starvation or blocking.

### Fixed

- **Netty IO Buffer Sizing for Mixed Workloads** — fixed severe packet fragmentation and high context switching during mixed-object payloads. `MixedLoadStepLocal` now properly initializes socket buffers for `OpType.NOOP`, bringing `spt` mixed performance on par with competitive tools (e.g. Warp).

### Changed

- **Engine Threading Performance** — replaced busy `parkNanos` waits with cooperative thread `yield` across core engine queues and thread pools to reduce CPU waste during I/O stalls.
- **Dependency Updates** — bumped `netty` to `4.1.132.Final` (addressing 5 CVEs), `guava` to `33.6.0-jre`, and rolled out safe minor upgrades to Go CLI packages.

## [5.7.4] - 2026-04-16

### Fixed

- **Netty connection pool exhaustion failure** — resolved a `ConnectException: Connection is not active` bug where the `s3` (Netty) driver aggressively failed entire load operations if handed a stale/idle connection by the pool. The driver now automatically closes the inactive channel and loops to lease a fresh active connection without disrupting the request stream.

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
