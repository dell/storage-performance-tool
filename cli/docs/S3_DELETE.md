# S3 DELETE Workload

`spt run delete` measures logical S3 `DeleteObject` and `DeleteObjects` requests against a
finite, frozen inventory. DELETE is destructive: review the selected source and recovery contract
before using an existing namespace.

Successful S3 DELETE responses are described as **accepted**, not removed. S3 can accept deletion
of a missing key. Only enabled post-verification establishes that the selected current key or exact
version is absent after the run; without successful pre-validation, absence still does not prove
that this run removed a previously existing object.

## Safe seeded default

With neither `--items-file` nor `--delete-existing`, SPT owns the selected inventory. It creates a
run-unique `spt-delete-<run-id>/` namespace, freezes the successful PUT identities, and times DELETE
against that manifest.

```bash
spt run delete \
  --endpoints https://s3.example.com \
  --access-key "$S3_ACCESS_KEY" \
  --secret-key "$S3_SECRET_KEY" \
  --bucket benchmark-test \
  --headless
```

The shipped defaults create and delete 2,500 objects of 1 KiB each, use one client thread, and put
up to 100 objects in each logical request. `--prefix team/root/` changes only the owned namespace
root; it never opts into discovery of existing objects. A nonempty PUT response version is retained
and exact-deleted. Otherwise, the row uses current-key semantics.

For a finite count, `--object-count N` is the global number of selected object identities, not the
number of API requests. For a duration run, `--seed-objects N` controls the finite live inventory:

```bash
spt run delete \
  --endpoints https://s3.example.com \
  --access-key "$S3_ACCESS_KEY" \
  --secret-key "$S3_SECRET_KEY" \
  --bucket benchmark-test \
  --duration 2m \
  --seed-objects 100000 \
  --threads 16 \
  --delete-batch-size 100 \
  --headless
```

A duration result is invalid if live identities exhaust before the deadline; increase
`--seed-objects`. SPT warns, but does not reject, when the inventory cannot fill a complete
`threads * delete-batch-size` concurrency wave. DELETE does not support recycle, SPT engine retries,
stable auto-termination, or a public request-rate limit.

## Explicit canonical manifest

`--items-file` selects an RFC 4180 CSV with this exact header:

```csv
bucket,key,size,version_id
benchmark-test,prefix/current-key,1024,
benchmark-test,prefix/exact-key,1024,version-123
```

An empty `version_id` means current-key deletion; a nonempty value means exact-version deletion.
The CLI validates and sorts the complete source, collapses identical rows, rejects conflicting
sizes, applies the global `--object-count` cap, and freezes source/unique/selected counts plus a
SHA-256 selection hash before orchestration. An optional `--bucket` is a safety assertion applied
to every row. Omitting it permits a multi-bucket manifest, but multi-bucket input requires
`--delete-batch-size=1`.

```bash
spt run delete \
  --endpoints https://s3.example.com \
  --access-key "$S3_ACCESS_KEY" \
  --secret-key "$S3_SECRET_KEY" \
  --items-file ./delete-input.csv \
  --delete-batch-size 1 \
  --object-count 1000 \
  --headless
```

Empty selections fail. `--items-file` is mutually exclusive with `--delete-existing` and
`--prefix`, and it cannot use `--cleanup` because SPT did not create the selected objects.

## Guarded existing-prefix selection

Existing data is eligible only with all required scope controls:

```bash
spt run delete \
  --endpoints https://s3.example.com \
  --access-key "$S3_ACCESS_KEY" \
  --secret-key "$S3_SECRET_KEY" \
  --bucket exact-bucket \
  --prefix exact/root/ \
  --delete-existing \
  --object-count 1000 \
  --headless
```

`--delete-existing` requires an exact bucket and an explicitly supplied prefix. A normally
forbidden empty prefix selects the whole bucket only when the command also includes both
`--prefix=''` and `--allow-empty-prefix`; an interactive prompt never substitutes for either
opt-in. A destructive prefix cannot begin with `/`. Omitting `--object-count` selects **all
discovered current-key identities** in the scope and is unbounded; set a positive object count to
cap the globally sorted, de-duplicated selection.

SPT discovers current keys, rejects an empty result, validates every returned identity against the
immutable scope, freezes the canonical manifest, and only then starts timed DELETE. Discovery is a
setup phase and is excluded from DELETE latency and throughput. Keep the namespace quiescent: a
concurrent writer can replace a frozen current-key identity before deletion. Existing-prefix mode
does not expose all-version or delete-marker discovery and cannot use `--cleanup`.

## Requests, concurrency, and exact versions

`--delete-batch-size=1` issues one `DeleteObject`. Values from 2 through 1,000 issue one same-bucket,
non-quiet `DeleteObjects` request containing up to that many targets. There is no one-target batch
mode and no fallback from a requested batch to repeated single-object calls. `--threads` bounds
concurrent logical API requests, not object targets. Distributed workers form batches locally, so
partial tails are expected and actual request/object counts are recorded rather than inferred.

Netty `s3`, AWS SDK `s3-aws`, and the inherited HTTP path of `s3-rdma` support standalone DELETE.
All preserve current-key versus exact-version identity. The RDMA driver uses HTTP for DELETE even
when its RDMA transport is available, which permits non-hardware contract qualification of the
DELETE path. An operator selecting `s3-rdma` must still satisfy its startup device requirements or
explicitly enable `--rdma-fallback`; the HTTP DELETE path does not bypass driver initialization.
Unsupported drivers fail during engine initialization. Existing cleanup and mixed-workload DELETE
retain their legacy single-object behavior.

## Verification

Verification is full-inventory and disabled by default. The CLI prints:

```text
Verification disabled; results describe logical DELETE API outcomes, not confirmed object removal.
```

Pre-validation and post-verification are independently controlled:

| `--validate-inventory` | `--verify` | Before timing | After DELETE |
|---|---|---|---|
| omitted | omitted | off | off |
| omitted | true | off | on |
| true | omitted | on | on |
| true | false | on | off |
| true | true | on | on |

`--verification-timeout` defaults to 30 seconds for each enabled phase. Each phase makes one
complete pass and then retries only unresolved identities; post-verification also retries identities
still observed present. Strict pre-validation failure stops before DELETE timing.

A current-key probe requires the current object to be absent and permits older versions to remain.
An exact-version probe requires only that version to be absent. Accepted-but-present and
accepted-but-unresolved targets are correctness/inconclusive failures. Operationally failed targets
keep their operational classification; their verification observation is not double-counted as an
independent correctness failure. Unattempted targets remain separate.

## Failure policy and shutdown

The default controller policy permits 100,000 operationally failed object targets. This is a new
object-unit budget, not the deprecated engine failed-operation limit. `--max-failed-objects N`
permits exactly N and trips above it. `--max-failure-percent P` accepts 0 through 100 and divides
failed targets by cumulative accepted-plus-failed target outcomes. Zero is strict immediately;
positive percentages begin continuous evaluation after `--failure-budget-grace` (30 seconds by
default) and are always reevaluated at completion. The fixed and percentage controls are mutually
exclusive.

The controller alone evaluates the global policy. A breach closes scheduling and recovers queued
work as unattempted, but already-dispatched requests drain. The final failed count may therefore
exceed the trigger. Protocol defects, correctness/inconclusive failures, unresolved requests,
missing worker counters, zero full-success requests, and zero accepted objects fail outside budget
room. Clean and within-budget outcomes exit 0; a failed outcome exits nonzero.

When scheduling closes at a duration deadline, generator-buffered and driver-queued identities are
unattempted. Dispatched requests drain for `load.op.wait.limit`, 30 seconds by default. Their outcomes
and request timings remain measured. A dispatched request without a terminal result after the bound
is unresolved and invalidates the run. Scheduled DELETE and drain durations are reported separately.

## Cleanup and recovery

`--cleanup` is optional, defaults false, and is valid only for the SPT-owned seeded mode. It runs
after post-verification, or directly after DELETE drain when verification is off, including after a
failure-budget stop. Cleanup is best effort and never changes the measured DELETE verdict or exit
code.

Before cleanup, the measured step freezes its canonical residual `items.csv`. Without verification
it conservatively includes failed, unattempted, and unresolved targets. With post-verification it
contains targets observed present or unresolved and excludes targets proven absent. Cleanup does not
rewrite this artifact, so it remains safe idempotent recovery input.

## Results and artifacts

One generic DELETE operation is one logical API request. Full reconciliation records one request
success; partial or failed reconciliation records one request failure. DELETE-specific metrics keep
request and object units separate and report configured/actual batching, per-node and bounded
per-bucket outcomes, current-key/exact-version counts, verification, and seed/discovery/
pre-validation/scheduled DELETE/drain/post-verification/cleanup/total-wall phases.

Request latency is first request byte sent through first response byte received. Request duration is
request formulation through last response byte. Both expose p50, p90, p99, and p99.9 when samples
exist. Object latency is never invented. Object size, data moved, bandwidth, and TTFB render as N/A.
Result identity includes `single`/`batch`, configured batch size, and `canonical` selection order;
unlike identities are not merged. Canonical order may differ from tools that shuffle input.

The engine publishes additive JSON metrics schema v4. Standalone DELETE rows set
`delete_detail_expected=true` and include the DELETE detail block; older/generic DELETE remains
compatible. The existing `metrics.total.csv` stays request-based. A complete stored DELETE step also
contains:

| Artifact | Schema and meaning |
|---|---|
| `delete.metrics.total.csv` | v1; identity, explicit request/object/batch units and counters, and terminal lifecycle reconciliation |
| `delete.requests.csv` | v1; one row per API invocation with stable request/batch identity and request timing |
| `delete.objects.csv` | v1; per-target reconciliation linked to the request, with no object timing |
| `delete.verification.csv` | v1 when verification evidence is available; pre/post observations and classifications |
| `verify-input.csv` and completion record | Frozen canonical selection, provenance, counts, and hash |
| `items.csv` | Immutable pre-cleanup residual recovery inventory |
| `delete.complete.json` | Hash-committed artifact-set completion evidence |

Failure policy, phase intervals, and request timing distributions are carried by JSON v4 and stored
run metadata. The request trace carries per-invocation start, duration, and latency. None of those
fields is part of DELETE totals v1.

Headless output, engine output, stored summaries, and raw artifacts carry the same terminal detail.
The existing TUI continues to show logical request rate/count. Missing, stale, duplicate, malformed,
or non-reconciling terminal evidence fails closed while retaining fetched recovery files.

## Local and distributed execution

A local host uses the Docker/controller route. One non-local `--test-hosts` entry uses the remote
orchestrator and skips local Docker/controller-port preflight. Multiple hosts use entry/worker
orchestration. Every frozen identity has exactly one worker owner; the controller aggregates actual
per-worker request/object outcomes and is the sole failure-policy authority.

Guarded existing-prefix distributed runs verify that every participant uses the same immutable
engine identity before discovery. For controlled comparisons and release evidence, record the CLI
version, commit, and SHA-256 plus the immutable engine image reference/digest/ID and canonical
`/opt/spt` payload identity on every worker. Do not compare results until those identities match.

## Deliberate exclusions

The first public release does not expose all-version/delete-marker discovery, selection shuffle,
multi-bucket batching, mixed-workload batching, a public DELETE request-rate limiter, stable
auto-termination, bucket deletion, or a DELETE-specific TUI. `--generate-only` is supported and emits
the same positive timed DELETE phase without starting an engine.

For every flag, compatibility rule, topology detail, and engine schema key, see
[SPT_SYNTAX.md](SPT_SYNTAX.md), the
[engine configuration reference](../../engine/core/spt-base/doc/usage/input/configuration/README.md),
and the [engine standalone DELETE contract](../../engine/core/spt-base/README.md#2341-standalone-delete-request-contract).
