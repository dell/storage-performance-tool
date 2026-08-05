# S3 Persisted-Data Integrity Verification

SPT's `write-verify` and `read-verify` workloads validate that complete S3
objects can be read back with exactly the bytes that SPT wrote. They are
correctness workloads, not ordinary performance benchmarks.

## What SPT stores and verifies

For each generated object, `write-verify` computes SHA-256 over the final bytes
and writes this versioned S3 user metadata on the original PUT or multipart
initiation request:

| Metadata header | Value |
|---|---|
| `x-amz-meta-spt-integrity-version` | `1` |
| `x-amz-meta-spt-integrity-algorithm` | `sha256` |
| `x-amz-meta-spt-integrity-digest` | 64 lowercase hexadecimal characters |
| `x-amz-meta-spt-integrity-size` | Object size in decimal bytes |

The digest covers the complete logical object after SPT's compressibility and
deduplication shaping. It excludes HTTP, metadata, and multipart framing. A
verification GET obtains the bytes and metadata from the same response, drains
the full body, checks its size, recomputes SHA-256, and compares the result.

This differs from three other SPT facilities:

- `--checksum` asks S3 to validate transfer checksums. It does not independently
  prove that a later GET returns the bytes written by SPT.
- `ReadVerifyLoad`, `ReadVerifyRandomRangeLoad`, and `item.data.verify` are the
  older deterministic-content verifier. They are not the persisted metadata
  contract described here.
- `spt verify` checks host and container readiness. It does not read or verify
  S3 objects.

S3 user metadata cannot be updated in place. SPT therefore attaches the digest
on the original write; it does not issue a post-write metadata update or keep a
separate checksum database.

## CLI workflows

Write a finite object set and verify every successful write once:

```bash
spt run write-verify \
  --endpoints https://s3.example.com \
  --access-key "$S3_ACCESS_KEY" \
  --secret-key "$S3_SECRET_KEY" \
  --bucket qualification \
  --object-size 1MiB \
  --object-count 10000 \
  --threads 32 \
  --prefix campaign-42/ \
  --headless
```

`write-verify` runs CREATE followed by one finite full-body READ. `--cleanup`
adds a DELETE phase that consumes only `verified.csv`: corrupt, unreadable,
failed, and unattempted objects are preserved for investigation.

To seed a reusable integrity set without reading it back immediately:

```bash
spt run write-verify \
  --endpoints https://s3.example.com \
  --access-key "$S3_ACCESS_KEY" \
  --secret-key "$S3_SECRET_KEY" \
  --bucket qualification \
  --object-size 1MiB \
  --object-count 10000 \
  --threads 32 \
  --prefix campaign-42/ \
  --defer-verification \
  --headless
```

`--defer-verification` omits the verification READ and rejects `--cleanup`;
deleting the seed would defeat the later campaign. Automatic result collection
still validates durable, nonempty CREATE evidence. Exit code `0` means the seed
and its manifest completed successfully; it does not claim that any object was
read or verified during that run.

Use the run's canonical `written.csv` as a later
`read-verify --items-file`, preserving the exact seeded identities for each
campaign. The LIST workflow below is also available when verification should
select the current qualifying objects under a prefix instead.

Verify SPT integrity objects hours or weeks later by discovering the current
objects under an isolated prefix:

```bash
spt run read-verify \
  --endpoints https://s3.example.com \
  --access-key "$S3_ACCESS_KEY" \
  --secret-key "$S3_SECRET_KEY" \
  --bucket qualification \
  --prefix campaign-42/ \
  --object-count 10000 \
  --threads 32 \
  --headless
```

`--object-count` is a deterministic maximum selection count after discovery;
omit it to verify the full discovered set. `read-verify` requires valid v1 SPT
metadata. Missing or malformed metadata is an integrity failure; SPT does not
fall back to generated content or accept arbitrary objects written by older SPT
versions and other tools. It never deletes selected objects and rejects
`--cleanup`.

For QA-controlled selection, supply a canonical manifest instead of LIST:

```bash
spt run read-verify \
  --endpoints https://s3.example.com \
  --access-key "$S3_ACCESS_KEY" \
  --secret-key "$S3_SECRET_KEY" \
  --bucket qualification \
  --items-file ./objects-to-check.csv \
  --threads 32 \
  --headless
```

The CLI fully validates the file, copies it into private run staging, publishes
completion evidence bound to this run, and leaves the user's file unchanged.
The canonical RFC 4180 CSV header is:

```text
bucket,key,size,version_id
```

`key` is the complete object key. `size` is a nonnegative byte count. A blank
`version_id` means the current version; a value requests that exact version.
Record identity is `(bucket,key,version_id)`.
SPT emits canonical integrity CSV records with UTF-8 encoding and LF (`\n`)
record separators on every platform. Readers accept valid RFC 4180 input with
either LF or CRLF separators, but completion byte lengths and digests bind the
exact bytes that were published.

## Completion, empty selections, and exit status

Verification commands require automatic result collection and do not return
until required counters and artifacts have been evaluated. `--auto-results=false`
is rejected.

| Exit code | Meaning |
|---:|---|
| `0` | Every required phase and integrity check succeeded |
| `1` | Non-corruption workload, orchestration, artifact, cleanup, or completeness failure |
| `20` | At least one persisted-data corruption was observed |

Code `20` takes precedence if corruption and another failure both occur. The
results still retain the primary structured failure cause.

Writing zero objects proves nothing, so immediate and deferred `write-verify`
runs return `1` and reject `--allow-empty-selection`. An empty `read-verify`
selection also returns `1` by default. Use `--allow-empty-selection` only when
an empty, successfully discovered or staged set is the expected answer. For an
immediate or later verification run, a nonempty selection with zero successful
verification operations can never pass.

## Results and resumability

The run directory contains ordinary per-step metrics plus these canonical
run-level files:

| Artifact | Meaning |
|---|---|
| `written.csv` | Successful completed writes selected by `write-verify` |
| `verify-input.csv` | Discovery or staged input selected by `read-verify` |
| `verified.csv` | Objects whose full GET passed metadata, size, and digest checks; absent when verification is deferred |
| `*.complete.json` | Atomic commit evidence for the matching manifest |
| `verify-remaining.csv` | Selected objects not in the successful verified set; absent when verification is deferred |
| `integrity.failures.csv` | One diagnostic row for each integrity-failed attempt; absent when verification is deferred |
| `integrity.performance.csv` | Digest work and initial-write-delay telemetry |
| `multipart.lifecycle.csv` | Completion, abort, and possible-orphan state for initiated MPUs |
| `<step>.multipart.csv` | Existing engine `parts.upload.csv` timing artifact |
| `index.json` | Machine-readable artifact inventory and integrity summary |

The summary and `index.json` report whether verification was deferred, plus
selected, attempted, verified, corrupt, and remaining counts. A deferred seed
reports its exact selected count and zero attempted, verified, corrupt, and
remaining objects. For completed verification runs, `verify-remaining.csv`
includes corrupt, failed, and unattempted objects and is the safest input to a
retry:

```bash
spt run read-verify \
  --endpoints https://s3.example.com \
  --access-key "$S3_ACCESS_KEY" \
  --secret-key "$S3_SECRET_KEY" \
  --bucket qualification \
  --items-file ./results/campaign-42/verify-remaining.csv \
  --threads 16 \
  --headless
```

Once the remaining manifest is empty, an idempotent confirmation run normally
uses `--allow-empty-selection` and should return `0`. Investigate failures before
using an ordinary item-file deletion scenario to reclaim preserved objects.

Manifest completion records use a crash-durable two-file commit. SPT flushes
and synchronizes each completed file, atomically creates its final name without
replacing an existing name, and synchronizes the containing directory. The CSV
is committed before its JSON marker. The marker contains exactly these v1
members: `version`, `status`, `run_id`, `producer_kind`, `producer_id`,
`artifact`, `source_record_count`, `unique_record_count`,
`selected_record_count`, `manifest_bytes`, and `manifest_sha256`. Unknown
members are rejected. A missing, staging-only, stale, or mismatched marker
prevents a dependent engine step from starting.
The marker must contain exactly one JSON object plus optional trailing
whitespace. Publication never replaces an existing canonical name. A retry
may reuse a fully validated matching pair or complete a matching manifest-only
state by publishing its marker. A mismatched pair or manifest, or a marker
without its manifest, fails closed. Uncommitted marker-staging files are ignored
as commit evidence and retained for diagnosis.

This guarantee requires a filesystem that honors file and directory fsync and
same-directory atomic no-replace publication semantics. It is supported on the local Linux
filesystems used by the SPT container when those primitives are provided by the
host. SPT fails the integrity run rather than weakening publication when a
primitive is unavailable. Network, clustered, FUSE, and other userspace
filesystems are crash-durable only when their provider explicitly guarantees
equivalent server-side persistence semantics. Persisted-data verification
requires the Linux CLI. Other platforms, including Windows, reject `write-verify` and
`read-verify` before item staging, scenario generation, or results evidence is
created because they cannot establish the required persistence contract.
Ordinary workloads remain supported on those platforms.

Distributed engine sources remain under
`${home_dir}/log/${step_id}/<artifact-stem>.node-NNN.csv`; the validated merge is
`${home_dir}/log/${step_id}/<artifact>`. CLI retrieval keeps the step-prefixed
and node-level evidence and promotes canonical run files without deleting the
sources.

## Digest-cost reporting

Metadata writes pre-hash the full final object before dispatching PUT or
`CreateMultipartUpload`. Verification hashes the GET body inline. Consequently,
verification throughput is not presented as ordinary write or read benchmark
performance.

`integrity.performance.csv`, the console summary, and `index.json` separately
report digest objects, bytes, cumulative worker seconds, mean worker MiB/s,
maximum-node delay before the initial write request, and additional full
payload passes. Cumulative worker seconds are not wall-clock overhead under
concurrency. Ordinary PUT/GET rates remain in the normal step metrics. SPT does
not calculate a percentage overhead or impose a verification throughput target.

Multipart objects use the same whole-object contract. SPT must finish the
pre-hash before initiating the upload. If an initiated upload subsequently
fails, `multipart.lifecycle.csv` records whether abort succeeded or whether a
possible orphan needs cleanup.

## Distributed runtime gates

Before any distributed verification I/O, the CLI proves that every participant
resolves the selected engine image to the same immutable image ID. A matching
mutable tag is not sufficient. It records each participant and the selected
identity tier in `spt_run_params.json`, then checks the entry node's existing
`/config/schema` endpoint for the four required integrity paths.

Controlled comparison and release-evidence procedures additionally require the
strong payload tier: every participant must have an identical canonical
relative-path hash of `/opt/spt`. This detects mutable-container or worker
filesystem drift beyond image identity. Qualification evidence is invalid if a
required identity cannot be obtained or differs. Verification currently does
not support `--attach-existing`, because the CLI cannot prove the image identity
of unmanaged workers before I/O.

Select the stronger tier explicitly:

```bash
spt run read-verify \
  --test-hosts entry,worker1,worker2 \
  --integrity-runtime-identity-tier payload \
  ...
```

The default is `image`; `SPT_INTEGRITY_RUNTIME_IDENTITY_TIER=payload` is the
equivalent environment setting. Payload probing runs the resolved immutable
image with no network and a read-only root filesystem, hashes the regular files
under `/opt/spt` in canonical relative-path order, and records the hash for
every participant.

Local and single-remote verification runs do not claim a cross-host equality
proof. A single-remote run nevertheless requires immutable image inspection
before startup and launches the container by that verified image ID, not by a
second resolution of the requested tag. Selecting the `payload` tier on one
remote participant additionally hashes the prepared image payload and rechecks
the exact running container before scenario submission. Any required remote
inspection failure is fatal. A local run records the available image evidence
or its collection error in `spt_run_params.json`.

## Version 1 boundaries

- Verification commands require the Linux CLI; Windows fails preflight before
  evidence creation because its required directory-durability primitive is unavailable.
- SHA-256 metadata and complete-object GET only; range verification is out of scope.
- Generated object content for writes; file-backed write payloads are out of scope.
- Isolated, unversioned prefixes are the normal discovery contract. Automatic
  LIST discovery selects current versions only; it does not enumerate historical
  versions. Exact historical versions require a manifest with `version_id`
  values, and automatic all-version discovery is deferred to a later release.
- Concurrent overwrite or replication change during current-version discovery
  can otherwise be mistaken for corruption.
- Copy and update operations do not maintain the v1 metadata contract.
- Release qualification did not include a PowerStore target or a real versioned
  bucket. Those compatibility paths remain unvalidated; exact-version manifest
  support is implemented but was not target-qualified in this release.
- Netty and AWS S3 paths have execution qualification. The RDMA metadata path is
  implemented and software-path tested, but its hardware path was not qualified;
  do not infer RDMA hardware parity from this release.

## Direct `spt.jar` use

QA harnesses that do not use the Go CLI can enable the same engine feature from
a custom JavaScript scenario. See the runnable
[`write-verify` scenario](../../engine/core/spt-base/doc/usage/input/scenarios/s3_integrity_write_verify.js),
[`CREATE-only seed` scenario](../../engine/core/spt-base/doc/usage/input/scenarios/s3_integrity_seed.js),
and [`read-only` scenario](../../engine/core/spt-base/doc/usage/input/scenarios/s3_integrity_read_verify.js).

`--defer-verification` is CLI lifecycle policy, not a new engine configuration
key. Direct-JAR users defer readback by running a metadata-mode CREATE scenario
that publishes `written.csv` without composing a dependent `ReadLoad`, then run
a read-only verification scenario later. The engine's metadata and manifest
contracts are unchanged.

The direct-JAR contract intentionally differs in artifact retrieval and process
exit policy; those examples document the harness requirements.
