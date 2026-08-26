# spt CLI Syntax Reference

This document covers the CLI syntax for `spt`, a Go-based tool for benchmarking S3-compatible storage using the SPT engine.

The command structure follows the `docker` CLI pattern (`command subcommand [options]`) for familiarity and clear separation of concerns.

## Main Commands

- `spt run <workload>`: Execute a benchmark test.
- `spt replay`: Replay archived SPT or legacy Mongoose workload artifacts against a current S3 target.
- `spt verify`: Validate nodes for distributed testing infrastructure readiness.
- `spt status`: Inspect live readiness and metrics snapshots for running nodes.
- `spt update`: Check for and install newer SPT CLI releases.
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
- **Checksum:** `SPT_CHECKSUM` (algorithm: `crc32`, `crc32c`, `sha1`, `sha256`, `crc64-nvme`)
- **Integrity qualification:** `SPT_DEFER_VERIFICATION` (true/false), `SPT_INTEGRITY_MAX_CONSOLE_FAILURES`, `SPT_INTEGRITY_RUNTIME_IDENTITY_TIER` (`image` or `payload`)
- **Data shaping:** `SPT_OBJECT_DATA_COMPRESSIBILITY` (0-100, default 0), `SPT_OBJECT_DATA_DEDUPABLE` (true/false, default true)
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
| `write-verify` | Implemented | Write objects with persisted SHA-256 metadata, then verify every successful write now or defer readback |
| `read-verify` | Implemented | Independently verify v1 metadata objects selected by LIST or `--items-file` |
| `mock` | Implemented | Exercise the CLI with in-memory drivers (no S3 required) |
| `tables` | Implemented | Benchmark S3 Tables (Iceberg) operations — see [S3_TABLES.md](S3_TABLES.md) |
| `mixed` | Implemented | Run a weighted mix of GET, PUT, DELETE, and STAT operations concurrently |
| `delete` | Implemented | Measure single-object or batched object deletion against a frozen inventory — see [S3_DELETE.md](S3_DELETE.md) |

### Options (Flags)

Flags are grouped by function for clarity.

#### 1. Target Connection Options

Required for S3 workloads, optional/ignored for `mock`.

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--endpoints` | `-e` | *(required)* | One or more S3 endpoint URLs (comma-separated or repeatable) |
| `--access-key` | `-a` | *(required)* | S3 access key credential |
| `--secret-key` | `-s` | *(required)* | S3 secret key credential |
| `--bucket` | `-b` | *(required)* | Target bucket to use for the test. In explicit-manifest DELETE mode it is an optional safety assertion checked against every source row; omit it to permit multiple buckets |
| `--prefix` | | `""` | Generated-key namespace for `write-verify`; owned namespace root for seeded DELETE; listing constraint for `list` and LIST-based `read-verify` |
| `--auth-version` | | `4` | S3 signature version (`2` or `4`) |
| `--slice-endpoints` | | `false` | Partition endpoints across nodes in distributed runs |

#### 2. Workload Definition Options

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--threads` | `-t` | `1` | Number of parallel client threads |
| `--object-size` | `-o` | `""` | Size of each generated object (e.g., `1MiB`, `256KiB`, `4GiB`; legacy `MB`, `KB`, and `GB` remain accepted as 1024-based aliases). Seeded DELETE resolves an omitted value explicitly to `1KiB`; ignored for `list` and manifest DELETE |
| `--part-size` | | `""` | Enable multipart upload with the given part size (e.g., `5MiB`, `64MiB`, `256MiB`; legacy `MB` remains accepted as a 1024-based alias). Applies to `write`, the CREATE phase of `write-verify`, and `read` seed phases |
| `--mpu-concurrent-objects` | | `0` | Max concurrent multipart objects in flight (`0` = unlimited). Requires `--part-size` |
| `--mpu-concurrent-parts` | | `0` | Max concurrent parts in flight per multipart object (`0` = unlimited). Requires `--part-size` |
| `--object-count` | `-n` | `0` | Fixed number of objects to process. Seeded DELETE creates and selects exactly this many global identities (`0` resolves to 2,500 in finite default mode). With `read-verify --versions=all`, caps canonical version identities rather than distinct keys. In manifest or existing-prefix DELETE, it caps the globally sorted, de-duplicated object selection rather than DELETE requests (`0` means all discovered identities) |
| `--duration` | `-d` | `""` | Fixed time duration (e.g., `5m`, `1h`). Standalone DELETE requires enough finite live inventory to remain schedulable for the full interval |
| `--prefix-shards` | | `-1` | Prefix directories for generated write, write-verify, mixed, and seeded-read object keys. `-1` derives the count from aggregate configured concurrency, `0` disables sharding, and a positive value selects an exact count |
| `--seed-objects` | | `2500` | Objects to pre-create for `read` benchmarks and duration-based standalone DELETE |
| `--checksum` | | `""` | Enable S3 checksum validation with the specified algorithm: `crc32`, `crc32c`, `sha1`, `sha256`, `crc64-nvme`. Omit to disable checksums. When set with `--part-size`, checksums are applied per part. (env: `SPT_CHECKSUM`) |
| `--object-data-compressibility` | | `0` | Target compressibility percentage for generated object data (0-100). Each 4KB chunk is split into random and zero-filled portions. 0 = fully random, 100 = fully compressible. (env: `SPT_OBJECT_DATA_COMPRESSIBILITY`) |
| `--object-data-dedupable` | | `true` | Whether generated data remains dedupe-friendly. Set `false` to stamp every 4KB with a 16-byte object-id + offset header that practically eliminates inline deduplication. Incompatible with file-based data input. (env: `SPT_OBJECT_DATA_DEDUPABLE`) |
| `--save-items` | | `false` | Save `items.csv` listing created objects (`write` only) |
| `--items-file` | | `""` | Path to an item manifest for `read`, or a canonical manifest for `read-verify` and explicit-manifest DELETE. Omit it for owned seeded DELETE; mutually exclusive with `--delete-existing` |
| `--delete-batch-size` | | `100` | Standalone DELETE canonical identities per logical request (`1` through `1000`); multi-bucket manifests require `1` |
| `--delete-existing` | | `false` | Destructive DELETE opt-in: discover and freeze current keys under the exact `--bucket` and explicitly supplied `--prefix` before timing |
| `--allow-empty-prefix` | | `false` | Second destructive opt-in required with `--delete-existing --prefix=''` to select an entire bucket; a prompt cannot replace it |
| `--max-failed-objects` | | `100000` | Standalone DELETE operational failed-object budget. Permits exactly this many failed targets and trips only when the global count is greater; zero is strict. Mutually exclusive with `--max-failure-percent` |
| `--max-failure-percent` | | *(unset)* | Alternative cumulative operational failed-object percentage, inclusive from 0 through 100. Zero is enforced immediately; positive values are evaluated after the grace period and at completion |
| `--failure-budget-grace` | | `30s` | Measured-phase delay before evaluating a positive `--max-failure-percent`; whole seconds only, and an explicit value is accepted only with a positive percentage budget |
| `--validate-inventory` | | `false` | Standalone DELETE only. Require every selected current-key or exact-version identity to be present before timing; also enables post-verification unless `--verify=false` is explicit |
| `--verify` | | `false` | Standalone DELETE only. Verify the full frozen inventory after DELETE drain; by itself it does not enable pre-validation |
| `--verification-timeout` | | `30s` | Independent positive whole-millisecond settle timeout for each enabled DELETE validation or verification phase |
| `--allow-empty-selection` | | `false` | `read-verify` only. Allow a clean empty discovery/input selection to succeed |
| `--defer-verification` | | `false` | `write-verify` only. Stop after durable, nonempty CREATE evidence and preserve `written.csv` for later `read-verify`; incompatible with `--cleanup` (env: `SPT_DEFER_VERIFICATION`) |
| `--versions` | | `current` | `read-verify` bucket/prefix discovery only. `current` uses ordinary object listing; `all` uses `ListObjectVersions`, preserves exact version IDs, and excludes delete markers. Omit with `--items-file` |
| `--integrity-max-console-failures` | | `20` | Verification only. Maximum corruption samples printed to the console (`0` suppresses samples; env: `SPT_INTEGRITY_MAX_CONSOLE_FAILURES`) |
| `--shuffle` | | `false` | `read` only. Shuffle items within each fetched read batch before issuing reads |
| `--shuffle-batch-size` | | `0` | `read` only. Batch size override used with `--shuffle` (`0` = bounded default `512000`, max `1000000`) |

*Typically specify either `--object-count` or `--duration`, not both.*

`write-verify` accepts a finite object count or duration for its CREATE phase,
then verifies every successful write once by default. With
`--defer-verification`, it ends after committing the successful-write manifest;
use that run's `written.csv` as a later `read-verify --items-file`. Deferred mode
does not permit `--cleanup`. `read-verify` is always finite and never deletes
objects: `--object-count` caps its deterministic discovery selection and
`--items-file` bypasses discovery. Verification workloads do not support
`--attach-existing`; both require automatic result collection. See
[S3_INTEGRITY.md](S3_INTEGRITY.md) for metadata, artifacts, resumability, empty
selection behavior, and exit codes `0`, `1`, and `20`.

#### Count and duration DELETE contract

The public `delete` command has three finite-inventory source modes. With no external
source-selection flag, seeded mode is selected. It writes
only beneath `spt-delete-<run-id>/`, or `<prefix-root>/spt-delete-<run-id>/` when
`--prefix` is supplied. The prefix is therefore a seed namespace root and never an
existing-data selection opt-in. With neither count nor duration, seeded mode creates
and selects exactly 2,500 objects. `--object-count=N` creates and selects exactly N
global identities. A duration run instead uses `--seed-objects=N`, also defaulting to
2,500; there is no automatic calibration. An omitted size resolves explicitly to 1 KiB.
When the requested count is smaller than the configured load-step node count,
the seed phase activates only enough slices to give every participant a positive
share; the shares still sum to exactly N. The timed DELETE phase may use all
configured nodes because its finite manifest slicing represents empty shares safely.

The seed CREATE step writes a canonical `written.csv` from successful PUT results.
Each nonempty version returned by PUT is frozen for exact-version DELETE; a missing
returned version records current-key semantics. The timed DELETE step requires the
CREATE step's matching completion evidence and reads only that frozen manifest—it
never regenerates names. A seed operation failure, unavailable terminal failure
count, or frozen record count different from the requested count fails the seed step
before DELETE configuration or I/O begins. Seed and DELETE remain distinct steps, so
seed elapsed time, PUT latency, and PUT throughput do not enter DELETE measurements.

An inventory below `threads * delete-batch-size` is valid but cannot fill one complete
concurrency wave. The CLI emits one bounded warning and reports
`floor(objects / (threads * batch-size))` as the maximum number of full request waves.
SPT does not increase the inventory automatically. `--cleanup` remains false by default and is
accepted only for this SPT-owned seeded source. Explicit-manifest and existing-prefix modes reject
it before orchestration side effects.

When requested, cleanup is a distinct best-effort phase after post-verification, or directly after
the timed DELETE drain when verification is disabled. It also runs after an operational
failure-budget stop. Before cleanup begins, the measured DELETE step commits its canonical residual
`items.csv`: failed, unattempted, unresolved, still-present, or verification-inconclusive identities
as applicable. The cleanup step consumes exactly those current-key or exact-version identities by
the legacy single-object DELETE path; deleting an identity already absent is idempotent. Seed PUT
metrics, measured DELETE request/object metrics, and cleanup metrics/errors remain separate.
Cleanup duration is appended only to cleanup and total-wall reporting, never to DELETE request
latency, duration, or rate denominators. A partial or failed cleanup is reported once by the
scenario finalizer but cannot alter the measured benchmark verdict or exit code. Cleanup never
rewrites `written.csv` or the pre-cleanup residual, leaving the original seed inventory, measured
recovery inventory, and cleanup outcome independently inspectable in stored results.

Explicit-manifest mode is selected by `--items-file`.

Explicit-manifest DELETE consumes a frozen CSV with the
exact header `bucket,key,size,version_id`. Normal CSV quoting is required, so keys containing commas
remain one field; `size` is a non-negative integer and `version_id` may be empty or name an exact
version. Before orchestration, the CLI rejects malformed rows, an empty selection, identical
identities with conflicting sizes, and any row that violates an optional `--bucket` assertion.
Identical canonical identities with the same size collapse to one record.

The private staged manifest is sorted by canonical identity, then `--object-count` selects the
global prefix (`0` means all). The CLI records source, unique, and selected counts plus the staged
SHA-256 completion evidence, and reports that canonical order may differ from another tool's input
order. A multi-bucket input must use `--delete-batch-size=1`; same-bucket input may use batching or
single-object requests. Duration mode consumes this frozen inventory without recycling and is invalid
if the inventory exhausts before the requested deadline. Explicit-manifest mode rejects `--prefix`
and `--cleanup`. A failed
validation or staging attempt stops before container/remote orchestration, and the private staging
directory is removed.

Existing-prefix mode is selected only by `--delete-existing` and is mutually exclusive with
`--items-file`. Its command shape is deliberately explicit:

```bash
spt run delete --endpoints https://s3.example.com \
  --access-key "$S3_ACCESS_KEY" --secret-key "$S3_SECRET_KEY" \
  --bucket exact-bucket --prefix exact/root/ --delete-existing --object-count 1000
```

`--bucket` and an explicitly supplied `--prefix` are mandatory. The prefix must be nonempty by
default. Intentional whole-bucket deletion requires all three destructive scope tokens:
`--delete-existing --prefix='' --allow-empty-prefix`; SPT never replaces either opt-in with an
interactive confirmation. A destructive prefix beginning with `/` is rejected because the S3
drivers remove that separator before LIST. A prefix without `--delete-existing` remains only the
seeded namespace root described above and does not activate discovery.

The first engine step performs completeness-preserving current-version LIST, freezes canonical
current-key identities, then applies `--object-count` globally (`0` selects all). It commits
source/unique/selected counts, a SHA-256 selection hash, and LIST-step provenance before the second
step can start. Empty discovery and any delimiter shard or response identity outside the immutable
requested prefix are fatal; incomplete node artifacts are removed, and `--allow-empty-selection`
cannot permit DELETE I/O. Distributed discovery proves one immutable engine identity across every
worker before startup. LIST
has separate setup metrics and its duration is excluded from DELETE request latency, duration, and
throughput. Duration mode consumes this frozen current-key inventory without recycling and is invalid
if it exhausts before the requested deadline. Existing-prefix mode rejects
`--versions=all`, delete-marker selection, and `--cleanup`. Keep the exact namespace
quiescent for the entire run: a concurrent writer can replace a frozen current-key identity before
the timed DELETE reaches it.

Inventory validation and absence verification are optional and operate on the complete frozen
selection, never a sample. Their flag truth table is:

| `--validate-inventory` | `--verify` | Pre-validation | Post-verification |
|---|---|---|---|
| omitted | omitted | off | off |
| omitted | true | off | on |
| true | omitted | on | on |
| true | false | on | off |
| true | true | on | on |

Each enabled phase makes one complete pass. Its independent `--verification-timeout` starts with
that pass; after the pass, unresolved probes are retried, and post-verification also retries
still-present identities until the phase deadline. A complete pass is never truncated merely to
meet the settle deadline, so a slow full inventory can make wall time exceed the configured timeout.
Strict pre-validation requires every selected identity to be present and stops before timed DELETE
admission on absent or unresolved input. When that happens with post-verification configured, the
post phase remains enabled but is reported as skipped: `pre_validation_complete=true`,
`post_verification_complete=false`, and `post_verification_skipped=true`. Its phase duration is
`null`, no post classifications are fabricated, and the residual inventory remains conservative.
For distributed runs, one slice's strict failure propagates this skipped-post state to every slice,
including slices whose local pre-validation passed, before shutdown or artifact finalization.

A manifest row without `version_id` uses a HEAD of the current object: older historical versions may
remain. A row with `version_id` HEADs that exact version: other versions may remain. Post-verification
joins every observation to its timed outcome. Accepted-and-absent is verified success;
accepted-and-present is a correctness failure; accepted-and-unresolved is both correctness and
inconclusive. Failed-and-present remains an operational failure and residual without a second
correctness failure. Failed-and-absent remains operationally failed but is removed from the residual.
Failed-and-unresolved remains operationally failed and uncertain. Unattempted identities retain their
own absent, present, or unresolved classifications and are never relabeled as correctness failures.
An operationally unresolved dispatched target likewise remains distinct from unattempted and is
cross-classified as absent, present, or probe-unresolved.
Operational failure-budget room cannot excuse validation, correctness, or inconclusive failures.

With post-verification, the pre-cleanup residual `items.csv` contains exactly selected identities
observed present or unresolved; identities observed absent are excluded regardless of their API
outcome. Without it, the residual remains conservative (failed, unresolved, and unattempted). Output
uses *accepted* for successful logical DELETE API outcomes. Only post-verification establishes
post-run absence, and without successful pre-validation even absence does not prove that this run
removed an object which existed beforehand.

Stored DELETE artifact sets use completion version 2 when verification evidence is available. The
additive `delete.verification.csv` v1 companion has one canonical `target_id`/`target_index` row for
every frozen selection identity and joins the operational outcome, pre/post observation, correctness,
inconclusive, and residual classifications. The existing DELETE totals, request trace, and target
reconciliation files remain schema v1. Stored artifact-set version 1 remains readable for runs made
before verification evidence existed, but it cannot substantiate enabled verification.

Count and duration are mutually exclusive. A duration run must retain enough live identities to
schedule logical DELETE requests until the deadline; early exhaustion invalidates the result and
seeded mode instructs the operator to increase `--seed-objects`. Stable auto-termination, recycle,
and engine retries remain incompatible, and no public DELETE request-rate control is exposed.
At the deadline the controller closes generator and driver admission across every local or
distributed input slice before permitting recovery on any slice. Generator-buffered and
driver-queued targets become unattempted. Actually dispatched requests drain for at most
`load.op.wait.limit` (30 seconds by default); their terminal outcomes and latency remain measured.
Dispatched targets without terminal results after the bound are unresolved and invalidate the run.
Slices drain through a bounded coordinator against one step-wide remaining-time budget, so the
bound is not multiplied by input count. It retains at most one lifecycle call per frozen input,
offers every input each phase, and does not duplicate blocked calls on cleanup retry. Each worker
compares source-monotonic exhaustion with its own scheduled deadline and retains only the semantic
verdict; the controller requires a reached-deadline verdict from every slice after admission closes.
Delayed, missing, or failed evidence cannot validate a run, and monotonic timestamps are never
compared across hosts. Before scheduling begins, every slice is initialized with admission held;
the controller prepares the same requested interval everywhere, then releases all ready slices in a
separate concurrent phase. Long worker
drains are started once and polled through short control-plane calls, so the 30-second drain remains
safe with the shipped 10-second RMI response timeout. Exhaustion at the deadline is valid.
Scheduled time and drain time are logged separately. `--threads` bounds concurrent logical DELETE
requests, not object targets within each request.

The controller enforces one global operational failed-object policy across all workers. The fixed
default is 100,000 failed DELETE targets—an object-unit default distinct from the deprecated legacy
failed-operation controls. The fixed boundary is inclusive. Percentage mode divides operational
failures by cumulative accepted-plus-operationally-failed target outcomes; zero is immediate,
positive values wait for grace during the run, and every value is reevaluated at completion.
Workers only publish counters, and missing terminal counters fail closed.

When a budget is exceeded, the same coordinated stop closes admission, recovers undispatched work,
and drains dispatched requests. Drain reconciliation can increase the final failed count beyond the
trigger, so output never describes the threshold as a hard cap. Only timed-phase operational target
failures consume budget. Setup, discovery, manifest, verification, protocol/correctness, and
unresolved failures retain separate fatal classifications. Cleanup failures are reported separately
and never change the standalone DELETE benchmark verdict or exit code. Output prominently reports
the selected policy and threshold, failed-object count, and observed percentage. Completed-cleanly and
completed-within-budget outcomes exit 0; failed or inconclusive outcomes exit nonzero. A 100%
budget still cannot validate zero fully successful requests or zero accepted objects.

### Standalone DELETE metric units and schema

The generic operation count/rate remains one logical DELETE API request. A full-success request is
one success; a partial or failed reconciliation is one failure. Additive JSON metrics schema v4
reports separate request (`attempted`, full-success, partial, failed, unresolved, requests/s) and
object (`selected`, attempted, accepted, failed, unattempted, unresolved, objects/s) units. Batch
detail uses the explicit `logical_api_requests` unit and includes configured size, observed
request/object totals, mean objects/request, full and
partial counts, and full-batch percentage. Version counts distinguish current-key and exact-version
targets; the deferred all-version/delete-marker mode is not exposed.

DELETE request/object rates divide cumulative attempted dispatches by the scheduled DELETE interval.
That interval starts at actual worker admission release, after setup and the controller barrier. A
count run ends it when the finite generator exhausts or admission closes; a duration run ends it at
the configured deadline (or an earlier admission close). Controller wait and the separately reported
drain interval do not enter either rate denominator.

Seed and discovery durations use monotonic clocks. Total wall time is one independently measured
monotonic interval from setup start through DELETE drain rather than a sum of named phases. It
therefore includes configuration, slicing, orchestration, inter-phase overhead, and the controller
admission barrier even though those intervals remain outside the request/object rate clock. The
boundary is epoch-aligned with a fixed per-runtime offset so distributed workers do not compare
private `System.nanoTime()` origins. Distributed participants must have synchronized system clocks
when their runtimes establish that offset. If clock skew makes the shared setup boundary appear to
be in a worker's future, that worker falls back to its local DELETE-step start instead of reporting a
negative interval. Skew in the other direction can conservatively enlarge total wall time. Neither
case changes the scheduled DELETE interval used by request/object rate denominators.

Per-node and aggregate views use the same fields and independent request/object completion.
Multi-bucket metrics contain selected, attempted, accepted, and failed counts for at most 100 named
buckets plus `__other__`; distributed slicing freezes one canonical retained-name set before any
slice is scattered, so the same bucket never splits between a named series and `__other__` across
workers. They never add bucket or per-object latency dimensions. Result identity is
`single` or `batch` plus configured batch size and `canonical` selection order, and unlike identities
cannot be combined. The top-level `delete_detail_expected=true` marker identifies standalone rows
whose detail is mandatory. Generic and cleanup DELETE rows with the marker false or absent remain
valid schema-v4 operation metrics without a `delete` block.

Fleet schema v4 leaves the existing `nodes_present` remote-address field unchanged and publishes
the current identity evidence separately as `contributors_present` (`local` plus fresh remote
contributors). The latter drives exact fleet timing comparison and duplicate/missing contributor
guards; headless output includes both representations. Local presentation normalizes the expected
set to exactly one `local` contributor. Once the engine reports terminal status, the CLI makes three
bounded attempts to capture and splice the controller-authoritative detailed DELETE result; a
persistent capture failure is returned as a run failure instead of completing with stale `running`
detail. A 404 without previously observed standalone DELETE detail remains the compatibility path
for engines that do not expose detailed metrics.

Request latency remains first request byte sent through first response byte received. Request
duration remains request formulation through the last response byte. Netty records the request
boundary from its first nonempty encoded (or TLS-encrypted) outbound buffer and the response boundary
from the first nonempty inbound transport buffer, before HTTP headers have been decoded. The AWS
adapter carries the operation through the SDK interceptor
into its HTTP client wrapper. A dedicated CRT connection-manager seam records native stream
`send-start` and `receive-start` timestamps for both header-only single DELETE and batched DELETE.
Publisher subscription, request-body delivery, signed-request handoff, and SDK-future completion are
not byte markers. Native `receive-start` marks response arrival, while the response-header callback
hands the response to the SDK and the response publisher's completion marks the last byte.
Transparent retries retain the first send
marker while replacing earlier response timing. A terminal failure without a completed response stream
therefore contributes neither stale latency nor fabricated duration. Authoritative
fleet timing therefore accepts only the available population, with
`0 <= latency samples <= duration samples <= terminal requests`; it never manufactures samples for
failed transport phases. Both retain p50, p90, p99, and p99.9. Object latency,
object size, data moved, bandwidth, and TTFB are N/A. Phase fields distinguish seed, discovery,
pre-validation, scheduled DELETE, drain, post-verification, cleanup, and total wall time; unavailable
phases remain unset rather than zero. Live JSON serializes an unmeasured phase as `null`; numeric
zero is reserved for an applicable interval actually measured as zero. Enabled validation and
verification report their measured phase durations. Schema v4's `verification` object reports the
flag state, phase completion/skipped state, timeout, pre-validation failures, aggregate
absent/present/unresolved observations,
accepted/failed/unattempted observation matrices, correctness and inconclusive failures, residual
count, causal notice, and removal-confirmed state. `accepted` is the required outcome term. When
verification is disabled, it describes a logical API result and does not confirm object removal.
Schema v4 removes or renames no schema v2/v3 field, so the existing TUI continues to
show logical request rate/count while safely ignoring additional detail.

The failure-policy observation is operationally failed objects divided by accepted plus
operationally failed objects. Protocol/correctness failures are reported as excluded failures and
do not enter that denominator. The controller owns `failure_policy.outcome`: live metrics use
`running`, and terminal metrics use `completed_cleanly`,
`completed_within_failure_budget`, or `failed`. Worker rows do not infer a fleet verdict; the
controller publishes its authoritative terminal outcome before results capture. The generic
`metrics.total.csv` layout remains unchanged and
request-based. Auto-results captures complete terminal schema-v4 DELETE detail into the existing
stored run-metadata model before engine shutdown, and the loader carries that model through aggregate
and rendered summaries. In the stored summary's `Performance by Phase` table, LIST success counts
are labeled as object identities and DELETE success counts are labeled as logical API requests;
other operation rows retain their established count presentation. Rate cells remain `objects/s` for
LIST and `ops/s` for DELETE. Auto-results also fetches the committed raw DELETE evidence set:
`delete.metrics.total.csv` v1, one-row-per-invocation `delete.requests.csv` v1,
per-target `delete.objects.csv` v1, selection-indexed `delete.verification.csv` v1, the pre-cleanup
residual `items.csv`, frozen `verify-input.csv` plus its provenance completion record, and the final
`delete.complete.json`. Current completion v2 records `verification_rows` and hashes that seven-file
evidence set. Completion v1 remains readable for pre-verification runs, hashes the original six
files, and cannot substantiate enabled inventory validation or post-verification.
The loader prefers DELETE totals v1 for request/object/batch detail while leaving ordinary and older
result sets compatible. It verifies every completion hash, selection source/unique/selected count,
producer identity, request link, target identity, and residual row before rendering recovery counts.
The request and batch unit is `logical_api_requests`; the object unit is `object_identities`.
Single/batch mode, configured batch size, and `canonical` selection order form the merge identity.
Missing or conflicting terminal evidence leaves the step incomplete; successfully fetched evidence
remains available for recovery and is never silently promoted to a complete result. The residual is
the conservative measured-phase inventory before optional cleanup, not the seed inventory, and is
safe to reuse as idempotent retry input. Capture accepts only the exact run cluster and expected
DELETE steps after their units, counters, timing populations, and terminal reconciliation validate.
Distributed runs require the authoritative fleet view; only a declared single-node run may fall back
to its node view. Missing, duplicate, partial, stale, or malformed terminal rows fail the results
artifact and the DELETE command with the workload-failure exit code instead of silently storing
incomplete metrics; run metadata records the capture error and omits DELETE metrics.

### Standalone DELETE topology and recovery contract

Standalone DELETE uses three distinct command routes:

- No `--test-hosts`, or one local host, uses the local Docker/controller route. Only this route runs
  the controller's local API-port conflict check.
- One non-local host uses the remote orchestrator. It skips local Docker and controller-port
  preflight; that host runs the entry engine and owns the API used for submission and results.
- Two or more hosts use the distributed entry/worker route. The first ready host is the entry and
  every other ready host is an additional worker. The command never falls back to the local runner.

Seeded output, an explicit manifest, or guarded existing-prefix discovery is canonicalized and
frozen before the timed DELETE step. The entry scatters the frozen input with one persistent
round-robin position across input read boundaries, so each nonempty identity has exactly one owner.
Workers assemble batches locally. A worker may therefore emit a partial tail even when the global
selected count is divisible by the configured batch size; request and object totals are always
aggregated from terminal worker evidence and are never derived from the global count.

Guarded existing-prefix deletion performs immutable image inspection after hosts connect and before
the launcher or auto-results monitor starts. `--integrity-runtime-identity-tier=image` requires one
image ID across the execution participants; `payload` additionally requires the same canonical
`/opt/spt` hash. A mismatch stops before destructive discovery or DELETE submission. Release or
comparison evidence should also record the CLI identity, immutable image reference/ID, and payload
identity described by the repository's distributed image gate.

The controller is the only failure-policy authority. Every worker publishes terminal request and
object counters; the controller aggregates them, closes admission on deadline or budget breach, and
drains already-dispatched requests within the shared bound. Missing, duplicate, conflicting, or
non-reconciling contributors fail closed. Worker-local percentage decisions are not accepted, and a
budget-triggered drain may honestly finish above the configured threshold.

Aggregate and per-node schema-v4 metrics retain the stable contributor and request identities used
by `delete.requests.csv` and `delete.objects.csv`. The entry validates every node artifact and
publishes canonical totals, selection evidence, traces, reconciliation rows, and the pre-cleanup
residual exactly once. `delete.complete.json` is the commit record. If cancellation, entry/worker
failure, verification timeout, or cleanup failure prevents that commit, retain the fetched source
files and `items.csv` as recovery evidence, but do not present them as a complete benchmark. Cleanup
runs after evidence is frozen and cannot rewrite the measured verdict or residual inventory.

All-version discovery requires the target's list-version permission
(`s3:ListBucketVersions` in AWS IAM). Authorization failure is fatal and never
falls back to current-version discovery. SPT follows version pagination,
de-duplicates exact `(bucket,key,version_id)` identities, and excludes and
reports delete markers. For completeness, this version LIST phase uses one
serialized exact-prefix stream; `--threads` controls the later verification GETs,
not discovery LIST concurrency. Use a quiescent prefix because version pagination
is not a snapshot of concurrent mutations. Unversioned-bucket results are target-specific;
see [S3_INTEGRITY.md](S3_INTEGRITY.md).

**`--save-items` / `--items-file` workflow:** By default, `read` workloads seed their own objects via an internal write phase. When you need independent control over the write and read phases (different concurrency, duration, or to reuse a data set across multiple reads), use `--save-items` on a `write` run to persist the object list, then pass the resulting `items.csv` to a `read` run via `--items-file`. See [Write-Then-Read Workflow](#write-then-read-workflow) below for examples.

**`--shuffle` / `--shuffle-batch-size` behavior:** These flags apply only to `read` workloads and only affect the `ReadLoad` phase. `--shuffle` enables engine-side item shuffling within each fetched batch. `--shuffle-batch-size` requires `--shuffle`, uses the bounded default when omitted, and is capped to avoid large buffer growth in seed/scatter/cleanup paths.

**`--prefix-shards` behavior:** SPT normally places generated object keys into
fixed-width prefix directories so namespace work can be distributed across the
target. In automatic mode, the shard count is the configured threads per
worker multiplied by the number of configured workers. A local run with
`--threads 16` therefore uses 16 shards, while three `--test-hosts` at the same
thread count use 48. This is configured aggregate concurrency: `--min-hosts`
does not reduce the value if fewer hosts eventually connect. The generated
directories are named `s0000000/`,
`s0000001/`, and so on using base-36 shard identifiers; any configured naming
prefix appears before the shard directory.

Use `--prefix-shards N` to select an exact positive count or
`--prefix-shards 0` to retain legacy flat generated keys. The setting applies
to write and mixed workloads and to objects generated by a read workload's seed
phase. Standalone DELETE does not support a positive shard count, and automatic
mode resolves to no sharding for all three DELETE source modes. Seeded DELETE
does contain a generating CREATE phase, but its keys stay beneath the run-owned
`spt-delete-<run-id>/` namespace without CLI prefix sharding. Explicit-manifest
and existing-prefix DELETE consume frozen names and cannot rewrite them.
Automatic mode is also a no-op for non-generating list workloads. Do not combine
a positive or automatic CLI setting with an `item.naming.shards` engine override.

```bash
# Automatic: three workers x 16 threads creates 48 prefix directories.
spt run write \
    --test-hosts worker1,worker2,worker3 \
    --endpoints https://s3.example.com \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket benchmark-test \
    --threads 16 \
    --object-size 128KiB \
    --duration 5m

# Preserve flat generated object keys.
spt run write \
    --prefix-shards 0 \
    --endpoints https://s3.example.com \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket benchmark-test \
    --threads 16 \
    --object-size 128KiB \
    --duration 5m
```

#### 3. Test Behavior Options

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--cleanup` | | `false` | Best-effort deletion of SPT-created objects after the test. Seeded DELETE retries its immutable measured residual in a separate phase; explicit-manifest and existing-prefix DELETE reject it. `write-verify` deletes only successfully verified objects; unsupported for `read-verify` and deferred verification |
| `--generate-only` | | `false` | Generate the scenario file without running it |
| `--auto-terminate-seconds` | | `0` | Auto-terminate headless runs after N seconds (0 = unlimited) |
| `--keep-scenario` | | `false` | Keep the scenario file after test completion |
| `--force` | | `false` | Resolve port conflicts and permit known engine build mismatches; invalid build information and collection failures remain non-forceable |
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
| `--test-hosts` | from `HOSTS` or `127.0.0.1` | Comma-separated Docker hosts: `[user@]host[,...]` |
| `--min-hosts` | `0` (all) | Minimum hosts that must connect (0 = all must succeed) |
| `--attach-existing` | `false` | Attach to pre-started worker nodes; spt still launches the entry node. Unsupported for verification workloads |
| `--integrity-runtime-identity-tier` | `image` | Distributed verification and guarded existing-prefix DELETE only. `image` proves one immutable image ID across participants; `payload` additionally proves identical canonical `/opt/spt` content (env: `SPT_INTEGRITY_RUNTIME_IDENTITY_TIER`) |
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
| `--use-rdma` | `false` | Use the S3-RDMA driver. Standalone DELETE requests use HTTP, but driver startup still requires RDMA access unless `--rdma-fallback` is enabled |
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
| `--stat-distrib` | `30` | Percentage of STAT (HEAD) operations |
| `--put-distrib` | `15` | Percentage of PUT (write) operations |
| `--delete-distrib` | `10` | Percentage of DELETE operations |
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
- `--read-items-file` supplies only the mixed READ pool; prefix sharding still
  applies to object names generated for mixed PUT operations.

**Note for distributed runs:** The DELETE queue is node-local. Each node only deletes objects that it created via PUT during the benchmark. In a 3-node cluster, Node 1 will never delete objects written by Node 2 or 3. This means per-node DELETE throughput is bounded by that node's PUT throughput, and cross-node deletion is not supported.

Set any operation weight to `0` to exclude it. For example, `--get-distrib 60 --put-distrib 40 --delete-distrib 0 --stat-distrib 0` runs a GET/PUT-only mix.

#### 10. TUI / Headless Options

By default, `spt run` launches an interactive TUI. Use `--headless` for CI or unattended runs.

Live metrics include throughput, latency, duration, and progress. TUI charts continue to update in normal mode; textual live-metric messages in headless output and the TUI messages window require `--verbose`. Time to First Byte (TTFB) is reported for READ and LIST samples when the engine records first response body bytes; headless text/JSON output omits the TTFB field when it is unavailable.

| Flag | Default | Description |
|------|---------|-------------|
| `--headless` | `false` | Force headless (non-interactive) mode |
| `--minimal` | `false` | Start TUI with only live stats panel visible |
| `--verbose` | `false` | Show detailed Docker API calls, debug information, and textual live metrics |
| `--trace-file` | `""` | Save all output to a trace file |
| `--trace-append` | `false` | Append to existing trace file instead of overwriting |

---

## Core Command: `spt replay`

The `replay` command imports archived SPT or legacy Mongoose workload artifacts
from a result-folder URL, remaps environment-specific settings to your current
target configuration, and generates an equivalent S3 replay workload.

```bash
spt replay --from <archive-folder-url> [options]
```

Use `--generate-only` first when inspecting an unfamiliar archive:

```bash
spt replay \
    --from 'https://archive.example.com/results/2031/result.2031-04-05.06:07:08/' \
    --generate-only \
    --output-dir ./replay-preview
```

Launch replay against a current target:

```bash
spt replay \
    --from 'https://archive.example.com/results/2031/result.2031-04-05.06:07:08/' \
    --endpoints https://s3.example.com \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket replay-bucket \
    --test-hosts worker1,worker2,worker3 \
    --headless \
    --auto-terminate-seconds 300
```

### Replay Source Options

| Flag | Default | Description |
|------|---------|-------------|
| `--from` | *(required)* | HTTP or HTTPS archive folder URL containing the launch script and scenario artifacts |
| `--generate-only` | `false` | Generate replay scenario/defaults/metadata without launching containers |
| `--output-dir`, `-O` | private temp directory for `--generate-only`; run results root for launch mode | Directory for generated replay artifacts |
| `--label` | `replay` | Prefix for generated replay step IDs and output directories |

The source URL must point to an archive folder, not an individual launch script
or scenario file. The folder should expose a directory listing or index page
that lets SPT discover the launch script and referenced scenario. Each fetched
artifact is limited to 16 MiB. See [REPLAY.md](REPLAY.md) for source archive
requirements.

### Replay Target Options

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--endpoints` | `-e` | from `S3_ENDPOINTS` / `S3_ENDPOINT` | One or more current S3 endpoint URLs |
| `--access-key` | `-a` | from `S3_ACCESS_KEY` | Current target access key |
| `--secret-key` | `-s` | from `S3_SECRET_KEY` | Current target secret key |
| `--bucket` | `-b` | from `S3_BUCKET` or archived bucket | Current target bucket override |
| `--auth-version` | | `4` | S3 signature version (`2` or `4`) |
| `--s3-driver` | | from `SPT_S3_DRIVER` or `default` | S3 storage driver backend: `default`, `netty`, `aws`, or `rdma`. RDMA replay launch is not implemented yet; use `rdma` only with `--generate-only` |

Archived credentials are sanitized from processed configuration files and are
never reused. Provide credentials for the current target with flags or
environment variables.

### Replay Execution Options

| Flag | Default | Description |
|------|---------|-------------|
| `--headless` | `false` | Force headless (non-interactive) mode |
| `--minimal` | `false` | Start TUI with only live stats panel visible |
| `--auto-terminate-seconds` | `0` | Automatically terminate runs after N seconds (`0` = unlimited) |
| `--force` | `false` | Resolve port conflicts and permit known engine build mismatches; invalid build information and collection failures remain non-forceable |
| `--api-port` | `9999` | SPT engine API port |
| `--trace-file` | `""` | Save all output to a trace file |
| `--trace-append` | `false` | Append to an existing trace file |
| `--verbose` | `false` | Show detailed Docker API calls, debug information, and textual live metrics |
| `--skip-image-pull` | from `SPT_SKIP_IMAGE_PULL` or `false` | Use locally cached Docker image without pulling |
| `--spt-image` | from `SPT_IMAGE` or release/dev default | Override the engine image ref |

### Replay Results Options

| Flag | Default | Description |
|------|---------|-------------|
| `--auto-results` | `true` | Automatically retrieve results artifacts at end of replay |
| `--results-dir` | `./results` | Directory to write retrieved replay results artifacts |
| `--auto-results-debug` | `false` | Enable verbose auto-results logs |
| `--shutdown-on-complete` | `true` | Request `/shutdown` after fetching results |
| `--shutdown-linger` | `5` | Seconds to wait for `/status` linger after `/shutdown` |

### Replay Distributed Options

| Flag | Default | Description |
|------|---------|-------------|
| `--test-hosts` | from `HOSTS` or `127.0.0.1` | Comma-separated Docker hosts: `[user@]host[,...]` |
| `--min-hosts` | `0` (all) | Minimum replay hosts that must connect |
| `--attach-existing` | `false` | Attach to prestarted worker nodes; replay still launches the entry node |
| `--network-mode` | `host` | Docker network mode: `host` or `bridge` |
| `--rmi-port-start` | `40000` | Starting port for RMI range |
| `--rmi-port-count` | `10` | Number of RMI ports to verify |

Run `spt verify --test-hosts ...` before distributed replay. Host networking is
recommended unless your environment supports the engine's inter-node RMI traffic
through another Docker network mode.

For supported transformations, safety rules, limitations, and troubleshooting,
see [REPLAY.md](REPLAY.md).

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

**Concurrency Controls:**

- By default, all parts from all active multipart objects compete freely for the global `--threads` pool.
- Use `--mpu-concurrent-objects N` to restrict how many top-level objects can be in the "in-flight" uploading state simultaneously.
- Use `--mpu-concurrent-parts N` to restrict how many parts of a *single* object can be uploading simultaneously (creating a sliding window of parts).
- These limits operate *within* the global `--threads` limit and are useful for preventing connection pool exhaustion or excessive memory pressure when testing with high concurrency.

**Checksums:**

When the engine's checksum feature is enabled (`storage-checksum-enabled=true` in defaults or scenario config), checksums are computed and sent for **each individual part upload**, not just the final object. Supported algorithms: `md5`, `crc32`, `crc32c`, `sha1`, `sha256`, `crc64-nvme`.

**Results artifacts:**

When multipart uploads are used, the engine produces a `parts.upload.csv` artifact (fetched as `<stepID>.multipart.csv` in the results directory) containing one row per completed upload with columns: `ItemPath`, `UploadId`, `RespLatency[us]`.

**Notes:**

- `--part-size` must be smaller than `--object-size` (multipart with a single part is pointless).
- No minimum part size is enforced by the CLI -- different S3-compatible storage systems have varying constraints, so the storage system reports errors for invalid sizes at runtime.
- Multipart upload scheduling is handled by the engine. The CLI does not override `load.batch.size` when `--part-size` is set; direct JAR users also do not need `load-batch-size=1` for MPU correctness.
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

# Same read pattern, but widen randomness within each fetched batch
spt run read \
    --endpoints https://s3.example.com \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket benchmark-test \
    --threads 16 \
    --object-size 10KB \
    --seed-objects 500000 \
    --duration 5m \
    --shuffle \
    --shuffle-batch-size 512000 \
    --cleanup
```

Notes:
- `--shuffle` only changes the read benchmark phase; it does not widen the seed or cleanup phases.
- The default shuffle window is `512000` when `--shuffle` is enabled without `--shuffle-batch-size`.
- `--shuffle-batch-size` is capped at `1000000` to keep engine buffer growth bounded.

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

Supported algorithms: `crc32`, `crc32c`, `sha1`, `sha256`, `crc64-nvme`. The flag works with the default Netty driver, the AWS SDK driver (`--s3-driver aws`), and the RDMA driver (`--s3-driver rdma` / `--use-rdma`).

### Data Compressibility & Deduplication

Use `--object-data-compressibility` and `--object-data-dedupable` to shape the generated object data for storage-efficiency benchmarks. These controls are useful for testing how a storage target handles compressible or deduplicated data.

```bash
# Write 75% compressible data (each 4KB chunk: ~1KB random + ~3KB zeros)
spt run write \
    --endpoints https://s3.example.com \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket benchmark-test \
    --threads 16 \
    --object-size 1MB \
    --duration 5m \
    --object-data-compressibility 75
```

```bash
# Write dedupe-resistant data (per-4KB anti-dedupe stamping)
spt run write \
    --endpoints https://s3.example.com \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket benchmark-test \
    --threads 16 \
    --object-size 4MB \
    --duration 5m \
    --object-data-dedupable=false
```

```bash
# Combine both: 50% compressible + dedupe-resistant
spt run write \
    --endpoints https://s3.example.com \
    --access-key "$S3_ACCESS_KEY" \
    --secret-key "$S3_SECRET_KEY" \
    --bucket benchmark-test \
    --threads 16 \
    --object-size 4MB \
    --duration 10m \
    --object-data-compressibility 50 \
    --object-data-dedupable=false
```

**How it works:**

- **Compressibility** (`--object-data-compressibility`): Each 4KB chunk of generated data is split into a pseudo-random (incompressible) portion and a zero-filled (compressible) portion. At 75%, each 4KB chunk contains ~1KB random data and ~3KB zeros. The target percentage is an input characteristic, not an exact compression-ratio guarantee -- actual ratios depend on the storage system's compression algorithm.

- **Dedupe resistance** (`--object-data-dedupable=false`): Every 4KB of the output stream is stamped with a 16-byte header containing a deterministic 64-bit object identifier and an 8-byte absolute stream offset. This practically eliminates inline deduplication for full-object writes on typical fixed/variable chunking systems. The stamp is deterministic: the same object produces the same byte sequence on repeated reads. The stamp overhead (~0.39%) slightly reduces the effective compressibility of the data.

**Constraints:**

- `--object-data-compressibility` must be in the range `[0, 100]`. Values outside this range are rejected.
- `--object-data-dedupable=false` is incompatible with file-based data input (`item.data.input.file`). If both are configured, the engine rejects the configuration with a clear error.
- When `--object-data-dedupable=false` is set, data integrity verification is disabled in incompatible paths and a warning is logged. Full stamp-aware verification is planned for a future release.

### Mixed Workload

The `mixed` workload runs GET, PUT, DELETE, and STAT operations concurrently with configurable weights. A seed phase pre-creates objects, then the benchmark phase issues operations at the specified distribution for the given duration.

```bash
# Default 4-op mix (GET 45% / STAT 30% / PUT 15% / DELETE 10%) for 5 minutes
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
2. **Mixed benchmark** — runs for `--duration`, issuing operations at the specified weights. The engine's `MixedLoad` step draws from the item set for GETs, STATs, and DELETEs while PUTs create new objects. PUT-created objects are eligible for DELETE during the benchmark. Exact final reporting of PUT-created objects that remain after mixed DELETEs is deferred.
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
  metrics: state=RUNNING, completion=78%, ops=1540/s, throughput=7.3MiB/s
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

## Maintenance Command: `spt update`

The `update` command checks GitHub Releases for newer SPT CLI binaries and can
install a verified release binary. Release downloads are checked against the
release `SHA256SUMS` file before any output file or running binary is replaced.
This is integrity verification, not signed authenticity verification.

### Syntax

```bash
spt update [--check] [--pre] [--yes] [--timeout 30s] [--output <path>] [--token <token>]
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--check` | `false` | Report current/latest CLI versions without downloading or writing files |
| `--pre` | `false` | Include prerelease tags such as `-rc.1` when selecting the latest release |
| `--yes`, `-y` | `false` | Skip the interactive confirmation prompt for self-replacement |
| `--timeout` | `30s` | Network timeout for GitHub API and asset downloads |
| `--output` | `""` | Write the release binary to an explicit path instead of replacing the running binary |
| `--token` | `""` | GitHub token for rate limits/private assets; prefer `SPT_GITHUB_TOKEN` or `GITHUB_TOKEN` |

### Exit Codes for `--check`

| Exit code | Meaning |
|-----------|---------|
| `0` | Check succeeded and the CLI is already up to date |
| `10` | Check succeeded and a newer release is available |
| `1` | Check failed, for example due to network, rate-limit, parse, or unsupported-platform errors |

`--check` prints one stable line, for example:

```text
current=5.10.4 latest=5.11.0 available=true
```

### Notes

- Local/dev builds such as `dev`, `*-dev+<commit>`, and `*-SNAPSHOT` refuse running-binary self-update so local development binaries are not overwritten by a GitHub release.
- Dev builds may still use `--output <path>` to download and verify a release binary without replacing themselves.
- Windows running-binary self-update is disabled for now; use `--output <path>` to download a verified Windows release binary.
- Before downloading assets for running-binary replacement, `spt update` verifies that the resolved target can be replaced and reports whether to re-run with elevated privileges or use `--output`.
- If `SPT_IMAGE` is set, self-update warns that engine runs will continue using the pinned image until the override is changed or removed.
- `--check` does not query GHCR and does not create or truncate the default `spt.log`; an explicitly supplied `--log-file` is still honored.

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
| `update` command | Implemented (`--output` only for Windows self-update) |
| Post-test cleanup (`--cleanup`) | Implemented |
| Auto-results retrieval | Implemented |
| Save/reuse item lists (`--save-items` / `--items-file`) | Implemented |
| Checksum validation (`--checksum`) | Implemented |
| RDMA acceleration | Implemented |
| TUI live dashboard | Implemented |
| Headless / CI mode | Implemented |
| Distributed multi-host orchestration | Implemented |
| `mixed` workload | Implemented |
| Data compressibility control (`--object-data-compressibility`) | Implemented |
| Anti-dedupe stamping (`--object-data-dedupable`) | Implemented |
| Post-quantum TLS (`pqcMode`: `off`/`prefer`/`require`) | Implemented |
| `delete` workload | Implemented |
| `results` command | Planned (stub exists) |
