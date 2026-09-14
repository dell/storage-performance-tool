# Partial-object READs

Partial-object READs request one fixed-length byte range per logical READ using the
Netty S3 driver. Ordinary READs are unchanged when range size is omitted.
AWS S3 and S3-RDMA support are deferred.

**Release qualification:** READ/WRITE performance qualification is still pending.
Functional tests and manual testing do not establish performance parity.

## Choose a range

| Option | Meaning |
| --- | --- |
| `--range-size L` | Positive requested length in bytes. Omit to retain ordinary READ behavior. |
| `--range-offset O` | Fixed starting offset, including explicit `0`. Omit for random selection. |
| `--range-align A` | Alignment in bytes; omitted, `0` and `1` all mean effective alignment one. |

Values accept unsigned decimal integers and binary units such as `64KiB`, `1MiB`
and legacy `64KB` (also 65,536 bytes). Fractions, signs and signed-64-bit overflow
are rejected. Offset/alignment require size. Alignment need not be a power of two.
A fixed offset must be divisible by the effective alignment, and `O + L - 1` must
fit a signed 64-bit integer.

A fixed request uses `Range: bytes=O-(O+L-1)` without adjusting it to inventory
size. It is sent even when the recorded object size is too small. For random mode,
the recorded whole-object size `S` determines legal offsets: multiples of effective
alignment from zero through `S-L`. Selection is uniform over those offsets. For
example, `S=10`, `L=3`, `A=4` permits offsets 0 and 4. Missing, empty or undersized
inventory sizes produce one local failure and no GET or metadata probe.

Selection belongs to the logical operation. A retry retains the same range; a new
recycled operation selects again. There is no whole-object fallback or automatic
metadata refresh. A full-span request (`O=0`, `L=S`) still uses Range and requires
a valid HTTP 206 response.

## Examples

These commands assume a configured test endpoint and authentication. Use an
inventory produced by SPT for existing objects:

```bash
spt run read --s3-driver netty --bucket partial-read-demo \
  --items-file ./items.csv --object-count 100 \
  --range-size 64KiB --range-offset 0
```

Create a dataset, then read random aligned ranges for a duration and clean up:

```bash
spt run read --s3-driver netty --bucket partial-read-demo \
  --object-size 1MiB --seed-objects 100 --duration 30s \
  --range-size 64KiB --range-align 4KiB --cleanup
```

The phase order remains PreconditionLoad → inventory → ReadLoad → optional
DeleteLoad. `--items-file` skips preparation. Only ReadLoad receives the range
policy. Count mode recycles successful reads to reach the requested operation count,
even when the inventory is smaller. Duration mode reuses the inventory until the
deadline. Each recycled logical read selects a fresh range; retries retain their
original selection. Failed reads retire under the ordinary READ failure policy.
The count limit is not a promise of that many successful reads: failures count toward completed
operations, and a run can exhaust its inventory if no reads remain to recycle.
Cleanup of an existing-items READ operates on the supplied inventory, so use
`--cleanup` only when those objects are intended for deletion.

## Response validation and changing objects

Success requires HTTP 206, one matching Content-Range, a valid total size (or `*`),
and exactly `L` completed response-body bytes. The inclusive span must match the
request. If Content-Length is present, it must be unique and equal to `L`.
Multipart responses, conflicting framing, ignored Range responses (HTTP 200),
short bodies and oversized bodies fail validation. Representation metadata in
trailers also invalidates the response. Status lines and header blocks use the
same size limits as ordinary HTTP reads (1024 and 2048 bytes respectively). Receiving `L` bytes alone is
insufficient: response framing must finish successfully. Rejected responses close
the connection before it can be reused.

The inventory retains the original object size and identity. Growth or shrinkage
can succeed when the response still contains the exact requested span. A span
that no longer exists can return an HTTP error; deletion normally returns 404.
Same-size replacement may succeed because this mode validates response structure,
not content. It does not pin an object version or add conditional reads.

## Unsupported combinations

Use DATA READ with Netty `s3`. READ-VERIFY, MIXED, other operations, other drivers,
content verification, metadata-integrity verification, metadata-only reads,
object tagging and recycled content updates are unsupported. Active legacy
`item.data.ranges.fixed`, `.random`, or a positive `.threshold` conflict with this
mode, including a positive `--part-size`. Inactive legacy settings remain allowed.
The network timeout must be positive.

With public range flags enabled, advanced overrides are checked after merging,
including parent-map replacements and dotted/dashed paths. Use the range flags
rather than global `load.op.read.range` overrides: global defaults would also
apply to preparation and cleanup. The engine independently validates effective
configuration before starting the selected workload.

## Read the results

The normal successful-transfer metrics count validated range bytes, not the
inventory's whole-object size. Failed transfers do not contribute successful
bytes. The summary adds partial-read policies and logical outcome totals.

`range.read.csv` schema v1 contains one terminal row per worker/context. The
coordinator preserves `range.read.node-NNN.csv` sources and collects their rows
into the canonical file. The CLI fetches and indexes both. Summaries read only
the canonical file, so source copies are not counted again. Different context
policies remain visible separately.

| Fields | Interpretation |
| --- | --- |
| `mode`, `size`, `fixed_offset_present`, `fixed_offset`, `alignment` | Effective policy; absent offset is distinct from fixed zero. |
| `selected`, `accepted`, `failed`, `unattempted`, `unresolved` | Selected logical reads: successful, failed, stopped before an attempt, or without a determinate outcome at the drain deadline. Here `accepted` means successful reads, not admission into a queue. |
| `attempted`, `terminal_results` | `attempted = accepted + failed + unresolved`; `terminal_results = accepted + failed`. Local selection failures count here even though no HTTP request is sent. |
| `requests_sent` | Transport requests handed off, including retries. |
| `successful_bytes` | Validated successful body bytes. |
| `local_selection_errors`, `http_failures`, `response_validation_failures`, `transport_failures` | Final logical failure categories. |
| `http_attempt_failures`, `response_validation_attempt_failures`, `transport_attempt_failures` | Failed transport attempts, including attempts followed by a successful retry. |
| `failed_received_bytes`, `unresolved_received_bytes` | Received response-body bytes attributed to those outcomes; not network wire bytes. |

A terminal row reconciles `selected = accepted + failed + unattempted + unresolved`,
`attempted = accepted + failed + unresolved`,
`terminal_results = accepted + failed`, and
`successful_bytes = accepted × size`. Residual buffered, queued and in-flight
counts must be zero. The four final failure-category counters must sum to
`failed`. A recovered retry can increase attempt-failure counters while the final
logical result is successful. Overflow, malformed policy, duplicate worker/context rows,
mixed run identities and inconsistent terminal counters are rejected by the
summary reader. Incomplete rows mark the step partial. Known partial-read steps
require the artifact; ordinary runs retain optional/absent compatibility.
For known partial-read steps, the summary compares distinct worker IDs with the
READ metrics' reported node count and rejects mismatches. Multiple contexts on one
worker count once. If those metrics do not provide a node count, coverage is
explicitly unverified. This count check does not independently authenticate worker
identities or replace distributed runtime and performance qualification.

## Diagnose failed reads

To request per-operation evidence, add this option to a READ command:

```bash
--engine-override output.metrics.trace.persist=true
```

This enables the engine's `op.trace.csv` output (off by default), which the CLI
can fetch with the other results. Each determinate logical result is traced once,
including local selection errors and final HTTP, validation, or transport failures.
Retries do not create extra terminal rows. The existing trace format records the
item path and status; it does not add the selected range or a detailed
selection-error reason. Local failures have no storage request, so transport
timing columns are blank and successful transfer bytes are zero. Use `local_selection_errors` in
`range.read.csv` to distinguish that category.

The CLI's `--trace-file` captures console output; it does not enable operation
CSVs. Per-operation tracing adds output work, so leave it disabled for performance
comparisons unless tracing is part of the declared test configuration.

| Symptom | What to check |
| --- | --- |
| Local selection errors with zero requests | Random-mode inventory sizes must be known and at least `--range-size`; no metadata probe repairs them. |
| HTTP 200 instead of success | The endpoint ignored Range; the driver does not accept a whole-object fallback. |
| HTTP 416 | The fixed span or a span selected from stale inventory no longer fits the target object. |
| Response-validation failures | Check Content-Range, body length, framing, trailers, and HTTP size limits. |
| Unresolved reads | Requests were handed off but lacked a determinate outcome at the drain deadline; inspect timeout and target behavior. |
| Publication failure | A committed outcome could not be delivered within the drain bound or result output failed. The run is invalid even though its underlying read counters may reconcile. |

Successful and failed results remain in drain custody until publication finishes.
Local errors publish after accounting without creating a transport request.
Publication failures do not turn a previously committed read success into a
transport failure, and publication is not retried.

For direct-engine settings and legacy range operations, see the
[engine byte-range reference](../../engine/core/spt-base/doc/usage/load/operations/byte_ranges/README.md).
