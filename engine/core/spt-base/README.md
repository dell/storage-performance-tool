# Contents

1. [Overview](#1-overview)
2. [Features](#2-features)<br/>
3. [Bundles & Extenstions](#3-bundles-and-extenstions)
4. [Get started](doc/getstarted) 🏁 <br/>
5. [Comparison With Similar Tools](doc/comparision)<br/>
6. [Documentation](doc) 📄 <br/>
7. [Contributing](doc/contributing)<br/>
8. [Changelog](doc/changelog)<br/>
9. [FAQ](doc/faq) ❓ <br/>

# 1. Overview

Spt is a distributed storage performance testing tool. This repo contains the basic functionality only. See the 
[extensions](#23-extension) for the actual use. 

# 2. Features

## 2.1. Scalability

### 2.1.1. Vertical

Using Virtual Threads allows to sustain millions of concurrent operations easily without
significant performance degradation.

### 2.1.2. Horizontal

The [distributed mode](doc/design/modes/distributed_mode) in Spt was designed as P2P network. Each peer/node performs
independently as much as possible. This eliminates the excess network interaction between the nodes which may be a
bottleneck.

## 2.2. Customization

### 2.2.1. Flexible Configuration

* Safe: the configuration options are being checked against the schema
* Extensible: Spt's plugins may come up with own configuration options making them available from the joint CLI and being checked against the schema 
* [Expressions](doc/usage/input/configuration#124-expression) allow to specify the dynamically changing values 

### 2.2.2. Load Generation Patterns

* CRUD operations and the extensions: Noop, [Copy](doc/design/modes/copy_mode), etc

* [Parial Operations](doc/usage/load/operations/byte_ranges)

* [Composite Operations](doc/usage/load/operations/composite)

* Complex Load Steps
    * [Pipeline Load](https://github.com/dell/storage-performance-tool/tree/main/engine/extensions/load-steps/pipeline)
    * [Weighted Load](https://github.com/dell/storage-performance-tool/tree/main/engine/extensions/load-steps/weighted)
* [Recycle Mode](doc/design/modes/recycle_mode)

* [Data Reentrancy](doc/design/data_reentrancy)

  Allows to validate the data read back from the storage successfully even after the data items have been randomly
  updated multiple times before

* Custom Payload Data

### 2.2.3. [Scenarios](doc/usage/input/scenarios)

Scenarios allow to organize the load steps in the required order and reuse the complex performance tests

### 2.2.4. [Metrics Reporting](doc/usage/output#2-metrics)

The metrics reported by Spt are designed to be most useful for the performance analysis. The following metrics are
available:

* Counts

  * Items
  * Bytes transferred
  * Time
    * Effective
    * Elapsed

* Rates

  * Items per second
  * Bytes per second

* Timing distributions for:

  * Operation durations
  * Network latencies

* Actual concurrency

  It's possible to limit the rate and measure the sustained actual concurrency

The *average* metrics output is being done periodically while a load step is running. The *summary* metrics output is
done once when a load step is finished. Also, it's possible to obtain the highest precision metrics (for each operation,
so called *I/O trace* records).

## 2.3. [Extension](src/main/java/com/dell/spt/base/env)

Spt is designed to be agnostic to the particular extensions implementations. This allows to support any storage,
scenario language, different load step kinds.

### 2.3.1. Load Steps

The load step is needed to define how to generate the load (operations type/order/ratio/etc).
Spt basically includes the linear load step implementation which may be considered as a straightforward way to
generate a load. Other load step implementations allow to specify some custom and more complex load pattern. See the
load step extensions in the [extensions/load-steps](../../extensions/load-steps) directory.

### 2.3.2. Storage Drivers

The storage driver is used by Spt to interact with the given storage. It translates the Spt's abstract
operations into the actual I/O requests and executes them. Spt basically includes the dummy storage driver only
which does nothing actually and useful only for demo/testing purposes. See the storage driver extensions in the
[extensions/storage-drivers](../../extensions/storage-drivers) directory.

### 2.3.3. Scenario Engine

Any Spt scenario may be written using any JSR-223 compliant scripting language. Javascript support is available
out-of-the-box.

### 2.3.4. Operation Assembly

The load generator separates input-item cardinality from logical-operation cardinality. An
`OperationAssembler` appends logical operations to a caller-owned reusable buffer and reports both the number of input
identities it consumed and the number of operations it emitted. Generated-operation counts, pending counts, count
limits, throttle permits, output ranges, and completion checks all use emitted operations.
`LoadGenerator.consumedItemCount()` exposes the separate consumed-identity count. Bounded
standalone-DELETE cancellation keeps an unread manifest suffix out of that consumed count and reports
it through `aggregateUnattemptedItemCount()`; object-level `selected` is the exact sum of both.

Existing extensions do not need to change their `OperationsBuilder` implementations. The compatible
`OperationsBuilderAssembler` preserves the historical one-item/one-operation behavior, including input order,
operation type, origin index, naming, throttle indexing, close behavior, and ownership of builder resources. A custom
assembler accepts every identity in the supplied input batch, must not emit more operations than the generator's
available operation-buffer slots, and owns any resources it retains until `close()`.

#### 2.3.4.1. Standalone DELETE request contract

The standalone DELETE spine is explicitly enabled by `load.op.delete.standalone`; the engine default is
off so cleanup steps, read-workload cleanup, and mixed-workload DELETE keep their existing single-item
`DataOperation` behavior. A capable storage driver must opt in through
`StorageDriver.supportsStandaloneDeleteRequests()` before the step can initialize.
`load.op.delete.duration` is also shipped off. When enabled it requires a positive
`load.step.limit.time`, forbids generic request-count limiting, and treats finite input exhaustion
before that deadline as an invalid run. The CLI sizes seeded duration inventory with
`--seed-objects` (2,500 by default); manifest and prefix modes use their frozen selections without
recycling. Recycle and engine retries remain forbidden in both count and duration modes.

`load.op.delete.preValidation` and `load.op.delete.postVerification` are shipped off, while
`load.op.delete.verificationTimeoutMillis` defaults to `30000` and must be positive whenever either
phase is enabled. The CLI applies its tri-state policy before scenario generation: inventory
validation enables both phases unless post-verification was explicitly disabled. Each phase HEADs
the complete frozen inventory once, then uses its own timeout to settle retryable observations.
Pre-validation retries unresolved probes and requires every current-key or exact-version identity
to be present before timed admission. Post-verification retries present and unresolved probes.
If any distributed slice fails strict pre-validation, the controller propagates the abort to every
slice so locally passing slices also skip post-verification and retain conservative residuals.
Current-key HEAD permits older versions to remain; exact-version HEAD permits other versions to
remain. The Netty S3 and inherited s3-rdma HTTP paths and the AWS S3 adapter all implement this
neutral presence probe.

Post-verification joins selection-indexed observations to the immutable target outcome ledger.
Accepted-and-present is a correctness failure, accepted-and-unresolved is correctness plus
inconclusive, and failed outcomes retain their operational classification without double-counting.
Unattempted outcomes remain distinct, as do dispatched operations whose transport outcome is
unresolved; each is cross-classified against absent, present, or probe-unresolved observations.
Verification failures are outside operational failure-budget room. The DELETE metrics snapshot and
schema-v4 output carry the full observation matrix and phase timing. When present, post-verification
refines residual `items.csv` to identities observed present or unresolved; without it, failed,
unresolved, and unattempted targets remain conservative recovery input. Absence proves causal removal
only when strict pre-validation also succeeded. Artifact-set completion v2 adds a selection-indexed
`delete.verification.csv` v1 companion without changing the DELETE totals, request trace, or target
reconciliation v1 schemas; artifact-set v1 remains readable only as unverified compatibility evidence.

The durable standalone DELETE artifact contract is:

| File | Versioned contents |
|---|---|
| `delete.metrics.total.csv` | v1 identity and explicit request/object/batch units, request/object/batch counters, and terminal lifecycle reconciliation |
| `delete.requests.csv` | v1 one-row-per-invocation request trace with request and batch identities, node, target count, outcome, start, duration, and latency |
| `delete.objects.csv` | v1 per-target reconciliation linked to request identity, including outcome and error classification but no object timing |
| `delete.verification.csv` | v1 selection-indexed pre/post observation and correctness/inconclusive classification when artifact-set completion v2 is used |
| `items.csv` | Canonical pre-cleanup residual inventory; it is recovery evidence, not ordinary successful-item output |
| `delete.complete.json` | Hash-committed completion record for the complete artifact set and frozen selection provenance |

Failure policy, phase intervals, and request timing distributions belong to JSON schema v4 and
stored metadata. Per-invocation start, duration, and latency belong to the request trace; they are
not fields in DELETE totals v1.

Generic `metrics.total.csv` remains request-based. Metrics JSON schema v4 adds the standalone DELETE
block without removing v2/v3 fields; `delete_detail_expected=true` makes that block mandatory only
for the standalone request spine. Request latency is first request byte sent through first response
byte received, request duration is formulation through last response byte, and no per-object timing
is derived from a batch.

A `DeleteRequestOperation` represents one logical API request and owns an immutable ordered list of 1 through 1,000
canonical targets. Every target has the same bucket and effective credential and snapshots its key, size, and optional
requested version. The inherited `item()` method projects only the first target for extension compatibility; request
execution, completion, accounting, and reconciliation must use `deleteRequest().targets()`. The request is deliberately
not a data-transfer operation, so it always contributes zero transferred bytes. Its completed `result()` snapshot
retains both the complete immutable request and the ordered `DeleteRequestResult`.

`DeleteRequestAssembler` streams across engine input reads, retains at most one partial batch, and emits same-bucket,
same-credential requests. Normal input exhaustion flushes its one tail request. Closing admission recovers a retained
tail as one unattempted logical request rather than dispatching it. `DeleteRequestReconciler` matches neutral transport
responses by key plus requested version, restores request order, and distinguishes operational target failures from
protocol defects. Missing, duplicate, malformed, or unexpected response identities conservatively fail every target
with the protocol classification. Generic success/failure metrics remain request-based, while
`deleteObjectLifecycle()` separately reports selected, attempted, accepted, failed, unattempted, unresolved, and
protocol-failed object identities together with the terminal reconciliation invariant.

CLI-staged explicit DELETE uses a strict `bucket,key,size,version_id` manifest and a matching
completion record. The CLI sorts and de-duplicates the complete source before applying its global
object cap, so engine `load.op.limit.count` is intentionally unset: request batching must not change
the selected object cardinality. File inputs are assigned to worker slices by one persistent
round-robin cursor across input reads, preserving exactly-one ownership even when engine read-batch
boundaries do not align with the number of slices. Multi-bucket inputs therefore require batch size
one, while same-bucket inputs may use the configured batch size.

CLI-seeded finite DELETE instead runs a metadata-mode CREATE step with
`load.op.limit.count` set to the exact global inventory size and writes canonical
`written.csv` output. This phase alone sets
`storage.integrity.output.requireExactCount=true`; the shipped false default preserves
write-verification's existing partial-success manifest behavior. When the count is smaller than
the configured load-step node count, the controller activates only enough seed slices for every
slice to receive a positive count share. The integrity writer records the PUT response version when present and an
empty version otherwise. Controller aggregation requires an available zero seed-failure count and
requires the frozen selected-record count to equal the CREATE count before publishing completion
evidence. The following standalone DELETE step declares `engine_step` provenance for that exact
CREATE step and consumes only the committed manifest. Consequently setup and DELETE keep separate
step metrics, and a partial seed cannot enter timed DELETE.

CLI guarded existing-prefix finite DELETE instead runs a completeness-preserving metadata-mode LIST
for one exact bucket/prefix. LIST freezes canonical current-key identities, applies the global
`storage.integrity.selection.maxCount` cap, and sets the normally disabled
`storage.integrity.selection.requireNonEmpty` guard. Zero selected identities fail before completion
publication. A delimiter-derived shard or returned identity outside the immutable LIST root prefix
also fails and removes incomplete node artifacts, so the subsequent standalone DELETE cannot begin.
Successful completion records the
source/unique/selected counts, SHA-256 selection, and LIST-step provenance consumed by the DELETE
step. Discovery and DELETE use separate step metrics; only the latter is timed as deletion. Versions
and delete markers are not selected. Operators must keep the namespace quiescent because a
concurrent write can replace a frozen current-key identity before deletion.

At a duration deadline the controller closes generator and driver admission across every local or
distributed input slice before permitting queued-work recovery on any slice. Generator-buffered,
assembler-tail, and driver-queued-but-undispatched identities are
unattempted. Requests that reached actual driver dispatch remain attempted and drain for at most
`load.op.wait.limit`, whose shipped default is 30 seconds; drained outcomes and latencies remain in
the DELETE phase. Any dispatched target without a terminal result after that bound is unresolved and
invalidates the run. A bounded coordinator passes each slice the remaining part of one step-wide
drain budget, so the configured bound is not multiplied by input count. It retains one lifecycle
invocation per frozen slice, offers every slice each phase, and reuses incomplete invocations rather
than multiplying blocked work on cleanup retry. During setup each duration generator holds
admission while its driver initializes; after every slice is ready, the controller first prepares
the same requested interval everywhere and then releases scheduling in a separate concurrent phase
on each worker's local monotonic clock.
Remote drains start once and expose a short status-poll RPC, keeping control calls below the shipped
RMI response timeout even when the configured drain bound is longer. The generator records source-monotonic scheduling
exhaustion. Each worker compares that timestamp with its own deadline and retains a semantic
verdict; after admission closes, the controller requires every slice to report that it reached its
deadline. Remote monotonic timestamps are never compared, and delayed, missing, or failed evidence
fails closed. Exhaustion is invalid only when it is strictly earlier than the scheduled deadline. The lifecycle invariant must reconcile after normal completion, deadline stop,
cancellation, and bounded drain. Monotonic scheduled and drain intervals are observable separately.
Storage concurrency (`--threads` in CLI-generated scenarios) limits logical DELETE requests rather
than the number of object targets held by those requests.

Standalone DELETE additionally uses `load.op.failureBudget`, evaluated globally by the controller
from worker lifecycle snapshots. Its shipped default is the new object-unit policy
`mode=fixed,maxFailedObjects=100000`; the legacy `load.op.limit.fail` operation controls remain
compatible and deprecated rather than being repurposed. Standalone DELETE bypasses those
per-worker request-unit controls; existing workloads retain them unchanged. Count-mode workers
hold scheduling until every slice is ready and the controller releases one concurrent admission
barrier. Fixed mode permits the exact threshold and trips above it. Percentage mode uses
operational failures divided by accepted-plus-operationally-
failed object outcomes, accepts 0 through 100, enforces zero immediately, defers positive live
evaluation for `graceSeconds` (default 30), and always reevaluates at completion.

A breach uses the coordinated admission-close, queued-work recovery, and dispatched-request drain.
Consequently terminal failures may overshoot the trigger; it is not a hard cap. Protocol and
correctness failures, unresolved identities, and setup/manifest/verification failures remain
outside budget room and fatal. Cleanup failures are reported separately and never change the
standalone DELETE benchmark verdict or exit code. Missing terminal worker counters fail closed. A
terminal run is reported as completed cleanly, completed within budget, or failed, including policy,
threshold, failed-object count, and observed percentage. The first two exit successfully; failed is
nonzero. A 100% policy cannot validate zero fully successful requests or zero accepted objects.

# 3. Bundles and Extenstions

This directory (`spt-base`) contains the core functionality. All extensions and additional spt tools are located in the [extensions](../../extensions) directory of this repository. Each component has its own documentation.
