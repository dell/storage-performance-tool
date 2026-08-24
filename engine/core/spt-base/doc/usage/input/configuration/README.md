# Configuration

1. [Overview](#1-overview)<br/>
1.1. [Reference Table](#11-reference-table)<br/>
1.2. [Specific Types](#12-specific-types)<br/>
1.2.1. [Time](#121-time)<br/>
1.2.2. [Size](#122-size)<br/>
1.2.3. [Dictionary](#123-dictionary)<br/>
1.2.4. [Expression](#124-expression)<br/>
1.3. [Persisted-data integrity](#13-persisted-data-integrity)<br/>
2. [Aliasing](#2-aliasing)<br/>

## 1. Overview

All the configuration values have the default values which may be seen
in the file [```<SPT_DIR>/config/defaults.yaml```](../../../../src/main/resources/config/defaults.yaml). The file contains
the comments so it's quite self-descriptive and may be used as quick
reference.

### 1.1. Reference Table


| Name                                           | Type         | Default Value    | Description                                      |
|:-----------------------------------------------|:-------------|:-----------------|:-------------------------------------------------|
| item-data-input-file                           | Path         | null             | The source file for the content generation       |
| item-data-input-layer-cache                    | Integer > 0  | 25               | The maximum count of the data "layers" to be cached into the memory
| item-data-input-layer-heap                     | Boolean      | false            | Specifies the type of memory for the data (payload) generation. Direct (off-heap) memory buffers are used by default.
| item-data-input-layer-size                     | Fixed Size   | 4MB              | The size of the content source ring buffer |
| item-data-input-seed                           | String (hex) | 7a42d9c483244167 | The initial value for the random data generation |
| item-data-ranges-concat                        | Range        | null             | The number/range of numbers of the source objects used to concatenate every destination objec
| item-data-ranges-fixed                         | Byte Range<br/> **list** | null | The fixed byte ranges to update or read (depends on the specified load type) |
| item-data-ranges-random                        | Integer >= 0 | 0                | The count of the random ranges to update or read |
| item-data-ranges-threshold                     | Size | 0                        | The size threshold to enable multipart upload (also used as the part size). When set to a value like `"64MiB"`, objects larger than this are split into parts of this size and uploaded via the S3 MPU API. Exposed by the CLI as `--part-size`. Accepts human-readable size strings (e.g., `"5MiB"`, `"64MiB"`) or raw byte counts. 0 disables MPU. |
| item-data-size                                 | Size | 1MB                      | The size of the data items to process. IEC suffixes such as `KiB` and `MiB` are accepted; legacy `KB` and `MB` suffixes remain binary aliases. Doesn't have any effect if item.type=container. |
| item-data-verify                               | Flag | false                    | Specifies whether to verify the content while reading the data items or not. Doesn't have any effect if load-op-type != read |
| item-input-file                                | Path | null                     | The source file for the items to process. If null the behavior depends on the load type. |
| item-input-path                                | String | null                   | The source path which may be used as items input if not "item-input-file" is specified. Also used for the copy mode as the path containing the items to be copied into the output path. |
| item-naming-length                             | Integer > 0 | 12                | The name length for the new items. Has effect only in the case of create (if not partial) load
| item-naming-seed                               | Integer or Expression | %{math:xor(<br/>int64:reverse(time:millisSinceEpoch()),<br/>int64:reverseBytes(time:nanos())<br/>)} | The initial id for the new item ids
| item-naming-prefix                             | String or Expression | null     | The name prefix for the processed items. A correct value is neccessary to pass the content verification in the case of read load.
| item-naming-shards                             | Integer >= 0 | 0                 | The number of fixed-width prefix directories used for generated item names. 0 preserves flat names. Positive values produce `s0000000/`, `s0000001/`, and subsequent base-36 shard directories beneath `item-naming-prefix`. The direct engine default is intentionally not automatic; the SPT CLI supplies its aggregate-concurrency-derived value through this setting. |
| item-naming-radix                              | Integer >= 2 | 36               | The radix for the item ids. May be in the range of 2..36. A correct value is neccessary to pass the content verification in the case of read load.
| item-naming-step                               | Integer | 1                     | The item naming step. Makes sense in case of "serial" naming type. Negative values cause descending order.
| item-naming-type                               | Enum | random                   | Specifies the new items naming order. Has effect only in the case of create load. "serial": the new items are named in a sequential order, "random": the new items are named randomly |
| item-output-file                               | Path | null                     | Specified the target file for the items processed successfully. If null the items info is not saved.
| item-output-path                               | String or Expression | %{date:<br/>format(\"yyyyMMdd.HHmmss.SSS\")<br/>.format(date:from(time:millisSinceEpoch()<br/>)} | The target path. By default the expression will once generate the constant value equal to the timestamp.
| item-type                                      | Enum | data                     | The type of the item to use, the possible values are: "data", "path", "token". In case of filesystem "data" means files and "path" means directories
| load-batch-size                                | Integer >= 1| 4096              | The count of the items/operations processed by a single invocation. For multipart upload (MPU) and other composite writes, cooperative storage drivers derive MPU scheduling limits and apply bounded child-operation backpressure. Setting this to `1` remains a conservative troubleshooting option, but it is no longer required for MPU correctness. |
| load-op-limit-count                            | Integer >= 0 | 0                 | The maximum number of the load operations to execute for a load step. 0 means infinite
| load-op-limit-fail-count                       | Integer >= 0 | 100000            | Deprecated legacy failed-operation count control, retained unchanged for existing workloads; it is not the standalone DELETE failed-object budget. 0 means no limit. |
| load-op-limit-fail-rate                        | Boolean | false                  | Deprecated legacy operation failure-rate switch, retained unchanged for existing workloads; it is not the standalone DELETE failed-object percentage. |
| load-op-limit-rate                             | Float >= 0 | 0                   | The maximum number of the load operations to execute per second (throughput limit). 0 means no rate limit.
| load-op-limit-recycle                          | Integer >= 1 | 1000000           | The load operations and results queues size limit
| load-op-output-duplicates                      | Flag | false                     | Specifies whether to add duplicates to output items list when in recycle mode or only print them once. No duplicates by default |
| load-op-delete-standalone                      | Flag | false                     | Engine capability gate for first-class standalone DELETE requests. It requires `load-op-type=delete`, `item-type=data`, a finite input with an exact unread-identity count, a driver that explicitly supports standalone DELETE requests, and a terminal topology. Public `spt run delete` scenarios enable it; cleanup and mixed DELETE leave it off. |
| load-op-delete-batchSize                       | Integer 1..1000 | 100              | Number of canonical object identities in each standalone DELETE logical request. The assembler carries one partial batch across input reads and emits one tail at normal exhaustion. This setting does not alter legacy cleanup or mixed-workload DELETE. |
| load-op-delete-duration                        | Flag | false                     | Marks standalone DELETE as duration mode. Requires positive `load-step-limit-time`, no generic `load-op-limit-count`, no recycle, and no engine retry. Frozen input is never recycled; exhausting it before the deadline invalidates the run. |
| load-op-delete-selectionOrder                  | String | canonical                | Controller-populated result identity for the target selection order. Public standalone DELETE workflows currently use only `canonical`; this field records that contract in metrics and does not reorder the engine input. |
| load-op-delete-selected                        | Long >= -1 | -1                    | Controller-populated total selected object-identity count. `-1` means unavailable and makes metrics use the runtime finite-input selection count. |
| load-op-delete-selectedCurrentKey              | Long >= -1 | -1                    | Controller-populated count of selected current-key identities. `-1` means unavailable and makes metrics use the count observed from dispatched targets. |
| load-op-delete-selectedExactVersion            | Long >= -1 | -1                    | Controller-populated count of selected exact-version identities. `-1` means unavailable and makes metrics use the count observed from dispatched targets. The deferred all-version mode is not exposed. |
| load-op-delete-selectedBuckets                 | List of `bucket=count` strings | &lt;EMPTY&gt; | Controller-populated selected counts by bucket for DELETE metrics. An empty list derives attempted/accepted/failed bucket counts from dispatched targets; selected identities not attributed to a named bucket are combined under the bounded `__other__` metrics entry. |
| load-op-delete-seedMillis                      | Long >= -1 | -1                    | Internal measured seed-phase wall time supplied by the generated seeded workflow. `-1` means the phase was not applicable. It never enters request latency or duration. |
| load-op-delete-discoveryMillis                 | Long >= -1 | -1                    | Internal measured discovery-phase wall time supplied by the generated existing-prefix workflow. `-1` means the phase was not applicable. It never enters request latency or duration. |
| load-op-delete-workflowStartedEpochNanos       | Long >= -1 | -1                    | Internal controller-supplied absolute workflow start in Unix-epoch nanoseconds. Workers translate it to their monotonic clock to report total wall time; `-1` falls back to the local DELETE-step start. Distributed hosts therefore require reasonably synchronized wall clocks, and future skew falls back locally while past skew conservatively enlarges total wall time. It never changes scheduled DELETE request/object rate denominators. |
| load-op-delete-preValidation                   | Flag | false                     | Runs a complete HEAD pass over the frozen standalone DELETE inventory before timed admission. Every current-key or exact-version identity must be present; absent or unresolved identities stop the workflow before DELETE timing begins. If post-verification is also configured, it remains enabled but is reported skipped with no timing or fabricated classifications. In a distributed step, one slice's strict failure propagates that skipped state to every slice. |
| load-op-delete-postVerification                | Flag | false                     | Runs a complete HEAD pass after the bounded DELETE drain unless strict pre-validation stopped admission. Accepted-and-present or accepted-and-unresolved identities are correctness failures; failed and unattempted outcomes remain separately classified. Probe requests do not enter timed DELETE metrics or traces. |
| load-op-delete-verificationTimeoutMillis       | Long > 0 | 30000                  | Independent retry/settling timeout for each enabled inventory phase. A complete first pass is always performed; pre-validation retries only unresolved identities, while post-verification retries present and unresolved identities. Current-key probes permit older versions to remain, and exact-version probes permit other versions to remain. |
| load-op-failureBudget-mode                     | `fixed` or `percentage` | `fixed` | Controller-owned standalone DELETE operational failed-object policy. Existing workloads continue using their legacy controls. |
| load-op-failureBudget-maxFailedObjects         | Integer >= 0 | 100000 | Fixed object-unit threshold. Exactly the configured count is permitted; scheduling stops only when the global operational failed-object count is greater. Zero is strict. |
| load-op-failureBudget-maxFailurePercent        | Float 0..100 | 0 | Percentage of cumulative accepted-plus-operationally-failed DELETE object outcomes. Zero is immediate; positive values observe grace and all values are reevaluated at completion. |
| load-op-failureBudget-graceSeconds             | Integer >= 0 | 30 | Measured-phase grace before live evaluation of a positive percentage budget. It does not suppress completion evaluation. |
| load-op-recycle-mode                           | Flag | false                     | Specifies whether to recycle the successfully finished operations multiple times or not
| load-op-recycle-contents-update                | Flag | false                     | Specifies whether to update the contents of the recycled object. Note: usually you just want to have a new object. This is rarely used. E.g. s3 versioning.
| load-op-retry                                  | Flag | false                     | Specifies whether to retry the failed operations or not. **Note:** For multipart uploads, individual part retries (up to 3 per part) happen automatically regardless of this flag. This flag controls whole-operation-level retry. A retried operation is redispatched after a full-jitter exponential backoff (200ms base, 1s cap) rather than immediately. Only transient failures are retried (`FAIL_IO`, `FAIL_TIMEOUT`, `FAIL_UNKNOWN`, `RESP_FAIL_UNKNOWN`, `RESP_FAIL_SVC`) — permanent failures (auth, not-found, client request errors, corruption, out-of-space) are counted as failed immediately regardless of the retry limit. Not supported by every load generator (e.g. mixed-mode workloads); enabling it against one that doesn't support requeueing raises a configuration error at step start rather than silently dropping failures. |
| load-op-retryLimit                             | Integer >= 0 | 10                | Maximum retry attempts per operation when `load-op-retry` is enabled, before the operation is counted as a failure (`CountFail`) instead of retried again. Must be `>= 0`; `0` disables retry even when `load-op-retry` is true. Has no effect when `load-op-retry` is false. |
| load-op-shuffle                                | Flag | false                     | Defines whether to shuffle or not the items got from the item input, what should make the order of the load operations execution randomized
| load-op-type                                   | Enum | create                    | The operation to process the items, may be "create", "update", "read", "delete", "noop" or "list"
| log-level                                      | String | info                     | Global logging verbosity. Accepts standard Log4j levels such as `trace`, `debug`, `info`, `warn`, `error`. |
| load-op-wait-finish                            | Flag | true                      | Wait for operations that reached actual driver dispatch to finish when a step stops. Generator-buffered and driver-queued work is recovered as unattempted instead of being included in this drain. When false, dispatched work is classified unresolved immediately. |
| load-op-wait-limit                             | Integer >= 0 | 30                 | Maximum drain time in seconds for actually dispatched operations after admission closes. Dispatched work still lacking a terminal result after this bound is unresolved; the bound does not convert it into an ordinary operation failure. |
| load-service-threads                           | Integer >= 0 | 0                 | Virtual-thread scheduler carrier parallelism for the remaining virtual-thread paths. The legacy option name is retained for compatibility; it does **not** size the long-lived platform service-task threads. 0 uses the JVM default (`max(2, available processors / 4)`).
| load-step-id                                   | String | null                    | The test step id. Generated automatically if not specified (null). Specifies also the logs sub directory path: `log/<STEP_ID>/`
| load-step-idAutoGenerated                      | Flag | false                     | Internal
| load-step-limit-size                           | Fixed size >= 0 | 0              | The maximum size of the data items to process. 0 means no size limit.
| load-step-limit-time                           | Time >= 0 | 0                    | The maximum time to perform a load step. 0 means no time limit
| load-step-node-addrs                           | List of strings | <EMPTY>        | Distributed mode: the list of the slave node IPs or hostnames, may include port numbers to override the default port number value. Standalone mode is used if empty (default behaviour).
| load-step-node-port                            | Integer > 0 | 1099               | Distributed mode: the common port number to start/connect the slave node
| output-color                                   | Flag | true                      | Use colored standard output flag
| output-metrics-average-period                  | Time >= 0 | 0                    | The time period for the load step's metrics console output. 0 means to not to output the metrics to the console
| output-metrics-average-aggregation-period      | Int > 0 | 100                    | The time period in ms for the load step's metrics to get aggregated to entry node. Happens not often than the specified value (meaning for 100ms it can happen 10 or less times).
| output-metrics-average-persist                 | Flag | true                      | Persist the average (periodic) metrics if true
| output-metrics-average-table-header-period     | Integer > 0 | 20                 | Output the metrics table header every N rows
| output-metrics-quantiles                       | List |0.25,0.5,0.75              | Extra output quantiles for timing metrics. SPT always reports p50, p90, p99, and p99.9.
| output-metrics-summary-persist                 | Flag | true                      | Persist the load step's summary (total) metrics if true
| output-metrics-timing-persist                  | Flag | false                     | Persist raw per-operation timing files for diagnostics. Normal percentile reporting uses in-memory histograms and does not require this.
| output-metrics-trace-persist                   | Flag | false                     | Persist the information about each load operation if true
| output-metrics-threshold                       | 0 <= Float <= 1 | 0              | The concurrency threshold to enable intermediate statistics calculation, 0 means no threshold
| run-comment                                    | String | ""                      | A user defined comment to run the scenario via the Control API
| run-cluster-id                                 | String | ""                      | Stable cluster identity shared by every participant in one run. The public CLI derives it from the preallocated run ID and publishes it on active schema-v4 node and fleet metrics. |
| run-node                                       | Flag | false                     | Run in the slave node or not
| run-port                                       | Integer > 0 | 9999               | Port for REST API
| server-metrics-expose_fleet                    | Boolean | true                   | Controls whether the entry node exposes the `/metrics/cluster/json` endpoint (and the legacy `/metrics/fleet/json` alias).
| run-scenario                                   | Path | null                      | The default file scenario to run, null means invoking the default.js scenario bundled into the distribution
| run-version                                    | String | (current)               | The Spt version (set automatically from defaults.yaml)
| run-id                                         | long | 0                         | The run identifier (see Runs API). If not specified, it takes the value of timestam
| storage-auth-file                              | Path | null                      | The path to a credentials list file, containing the lines of comma-separated item path, user id and secret key
| storage-auth-uid                               | String | null                    | The authentication identifier
| storage-auth-secret                            | String | null                    | The authentication secret
| storage-auth-token                             | String | null                    | S3: no effect, Atmos: subtenant, Swift: token
| storage-driver-limit-concurrency               | Integer >= 0 | 1                 | The concurrency limit (per node in case of distributed mode). In case of filesystem this is the max number of open files at any moment. In case of HTTP this is the max number of the active connections at any moment.
| storage-driver-limit-multipart-objects         | Integer >= 0 | 0                 | The max concurrent multipart objects in flight at any moment. `0` = unlimited.
| storage-driver-limit-multipart-parts           | Integer >= 0 | 0                 | The max concurrent parts in flight per multipart object at any moment. `0` = unlimited.
| storage-driver-limit-queue-input               | Integer > 0 | 1000000            | Storage drivers internal input operations queue size limit
| storage-driver-threads                         | Integer >= 0 | 0                 | The count of the shared/global I/O executor threads. 0 means automatic value (CPU cores/threads count)
| storage-driver-type                            | String | s3                      | The identifier pointing to the one of the registered storage driver implementations to use
| storage-namespace                              | String | null                    | The storage namespace
| storage-net-node-slice                         | Boolean | false                  | Try (or not) to distribute the storage endpoints between the Spt nodes using greatest common divisor

### Operation admission and shutdown lifecycle

Admission is the period in which a generator and storage driver may accept new operations.
Stopping a step closes both admission gates before recovering buffered work. Operations retained
by the generator or accepted into a cooperative-driver queue have not yet been attempted and are
reported as unattempted when recovered.

Dispatch begins immediately before the storage driver starts transport execution. Netty connection
acquisition is part of that execution: the lifecycle crosses dispatch before leasing a connection,
so a lease failure is terminal and a hung lease remains subject to the configured drain bound.
Only dispatched operations participate in the bounded drain controlled by `load-op-wait-finish`
and `load-op-wait-limit`. Completion remains in flight while its result snapshot is constructed and
offered to result output. Output acceptance linearizes before the lifecycle tracker atomically
commits the terminal outcome. A deadline may win while output is blocked; a later output return
cannot overwrite that unresolved outcome. Output rejection or failure is also unresolved rather
than terminal, without retaining an unbounded secondary copy of rejected results. Active
concurrency alone is not a complete count of outstanding work because generator buffers and driver
queues are outside the transport-concurrency limit.

Built-in cooperative drivers mark dispatch at their actual transport handoff. Compatibility
extensions which predate that hook retain their successful `submit` return as the dispatch
boundary; the dispatcher binds that fallback to the exact queued circulation so a synchronous
recycle cannot dispatch the next circulation accidentally. If a compatibility `submit` call does
not return within the dispatcher's bounded stop wait, its outcome is indeterminate and therefore
unresolved, never reported as unattempted. Untracked compatibility operations also retain their
historical synchronous same-instance recycle ordering; tracked built-in operations use the stronger
output-before-terminal ordering above. Built-in drivers which use the explicit hook keep later
members of a blocked batch driver-queued, so closure recovers those members as unattempted rather
than applying the compatibility fallback to the whole batch. Driver bases which do not opt in to
the lifecycle contract retain the historical active-concurrency drain; only bases which publish
every ownership edge activate lifecycle-only draining. A load-step context is single-run:
create a new context for another run instead of restarting a stopped context with exhausted
generator state.

### Standalone DELETE engine topology

Standalone DELETE is a terminal engine step. Initialization rejects recycle mode, SPT operation
retries, unsupported or lifecycle-disabled storage drivers, inputs which cannot report their exact
unread identity count without consuming it, a `TransferConvertBuffer` single-item pipeline,
ordinary successful-item output, and generic single-item timing output. Canonical manifest inputs
validate every row while calculating that finite count before the step starts, so a malformed suffix
cannot silently truncate the frozen selection. These checks run in both the generator builder and the
load-step context so override-injected configuration cannot bypass them.

Count mode ends when its selected identities resolve. Duration mode additionally requires a positive
`load-step-limit-time` and rejects generic `load-op-limit-count`; the CLI keeps object count and
duration mutually exclusive. Seeded duration inventory is controlled by `--seed-objects` and defaults
to 2,500. Manifest and prefix duration modes consume their frozen inventories without recycling.
Input exhaustion before the deadline invalidates the run. Stable automatic termination remains
incompatible, and there is no public DELETE request-rate flag.

For CLI-staged explicit DELETE, the source header is exactly
`bucket,key,size,version_id`. The CLI validates the entire source, enforces any bucket assertion,
sorts and de-duplicates canonical identities, then applies the global object-count cap and publishes
source/unique/selected counts plus a SHA-256 completion record. It does not set
`load-op-limit-count`, because that setting counts logical requests and would make selection depend
on DELETE batch size. Multi-bucket manifests use `load-op-delete-batchSize=1`; same-bucket manifests
may batch. Worker file slicing uses one persistent round-robin cursor across input read batches so
each selected identity has exactly one worker owner.

For CLI-seeded finite DELETE, the CREATE phase sets `load.op.limit.count` to the requested
global inventory count and writes a canonical metadata-mode `written.csv`. Returned PUT versions
are stored as exact identities; absent returned versions remain current-key identities. The seed
sets `storage.integrity.output.requireExactCount=true`; this boolean defaults to false, is valid
only for a metadata-mode CREATE with a positive `load.op.limit.count` and `item.output.file`, and
does not change write-verification's partial-success manifest contract. If the exact count is less
than the configured load-step node count, only enough seed slices are activated to keep every
active slice's count positive; their shares still sum to the requested global count. CREATE
aggregation requires both zero terminal operation failures and exactly the configured count before
publishing its completion record. The timed standalone DELETE phase then sets
`storage.integrity.input.provenance=engine_step` with the CREATE step ID and consumes that committed
file. `load.op.limit.count` is intentionally absent from the DELETE phase because its unit would be
logical requests after batching, not selected object identities.

For guarded existing-prefix finite DELETE, a metadata-mode LIST step writes the complete current-key
inventory and sets `storage.integrity.selection.maxCount` for the canonical global cap plus
`storage.integrity.selection.requireNonEmpty=true`. The latter is valid only for metadata LIST and
defaults to false so ordinary discovery retains its existing empty-result behavior. When enabled,
aggregation refuses a zero selected count and removes staging before publishing completion; the
following DELETE step therefore cannot start or mutate storage. A successful LIST completion records
source, unique, and selected counts, the selection SHA-256, and LIST-step provenance. Metadata LIST
also rejects any delimiter-derived shard or response key outside its immutable root prefix and
removes incomplete per-node artifacts. The DELETE step consumes that committed manifest with
`engine_step` provenance. LIST and DELETE remain separate steps,
so discovery is setup and does not enter DELETE request timing. Only current-key LIST is used; version
and delete-marker discovery are outside this mode. The namespace must remain quiescent because a
concurrent writer can replace a frozen current-key identity before DELETE.

One standalone `DeleteRequestOperation` is one request metric and owns its entire immutable target
list. Its compatibility `item()` accessor is only the first target and must not be used for batch
execution or output. A completed snapshot preserves the request and its ordered per-target
reconciliation. Full reconciliation records one generic request success; partial reconciliation,
transport failure, and protocol failure each record one generic request failure. DELETE is not a
data-transfer operation and records zero bytes. Object outcomes use the separate standalone DELETE
lifecycle snapshot, whose terminal identities are:

```text
selected = accepted + failed + unattempted + unresolved
attempted = accepted + failed + unresolved
```

On cancellation, the generator classifies the exact unread count as aggregate unattempted work in
constant memory without reading the frozen manifest suffix, waiting indefinitely for an
interrupt-ignoring input read, or retaining one recovery operation per identity. Buffered requests
still retain their canonical targets for concrete recovery. If an assembler call fails, abort
recovery includes its prior tail and every valid identity in the entire bounded input batch,
including identities after an invalid entry; invalid entries remain aggregate unattempted work and
no tentative request escapes the failed transaction. Request and object outcome counters commit
under the same terminal-vs-unresolved lifecycle decision, so a completion which loses to the
bounded drain cannot publish success or failure metrics for unresolved targets.

At the duration deadline the controller closes scheduling and driver admission across every local
or distributed input slice before permitting recovery on any slice. Generator-buffered,
assembler-tail, and driver-queued-but-undispatched identities are unattempted, not operational
failures. Actually dispatched requests drain for at most `load-op-wait-limit` (30 seconds by
default); their terminal outcomes and latency remain part of the measured DELETE phase. Dispatched
targets without terminal results after the bound are unresolved and invalidate the run. A bounded
coordinator gives each slice the remaining part of one step-wide drain budget, so the bound is not
multiplied by input count. It owns at most one active lifecycle invocation per frozen input, offers
every input each phase, and retains incomplete invocations across cleanup retries. Finite generators
record the source-monotonic scheduling-exhaustion transition. Each worker compares that transition
with its own deadline and retains a semantic verdict; after admission closes, the controller
requires every slice to report that it reached its deadline. Monotonic timestamps are never
compared across hosts, and delayed, missing, or failed evidence fails closed. Only a transition
strictly before the scheduled deadline invalidates duration mode. Monotonic
scheduled and drain intervals are reported separately. `storage-driver-limit-concurrency` (the
CLI's `--threads`) bounds logical DELETE requests, not the object targets inside a batch.

For standalone DELETE, workers publish lifecycle counters and only the controller aggregates them
and decides the global `load.op.failureBudget` outcome. The shipped fixed default permits 100,000
operationally failed object targets and is a new object-unit default, not continuity with the
deprecated `load.op.limit.fail` operation controls. Fixed and percentage thresholds are inclusive:
they stop only when the observed value is greater. Percentage mode uses cumulative accepted plus
operationally failed object outcomes; zero is enforced immediately, positive values begin live
evaluation after grace, and completion always reevaluates.

A breach closes scheduling, recovers undispatched targets, and drains dispatched requests, so the
final failure total may exceed the trigger—the threshold is not a hard cap. Only operational target
failures in the timed DELETE phase consume the budget. Setup, discovery, manifest, verification,
protocol/correctness, and unresolved failures retain separate fatal classifications. Cleanup
failures are reported separately and never change the standalone DELETE benchmark verdict or exit
code. Missing counters from any participant make terminal evidence inconclusive and failed. Terminal
output distinguishes completed cleanly, completed within budget, and failed while reporting policy,
threshold, failed objects, and observed percentage; the completed outcomes exit 0 and failed exits
nonzero. Even 100% cannot validate zero fully successful requests or zero accepted objects.

The reconciler keys each response by object key plus requested version. Missing, duplicate,
malformed, or unexpected identities are protocol defects: the request fails closed and every target
is reported failed under the separate protocol classification. A service/transport failure likewise
produces one operational failure for every request target.

#### 1.2. Specific Types

##### 1.2.1. Time

The configuration parameters supporting the time type:
* item-output-delay
* output-metrics-average-period
* load-step-limit-time

| Value | Effect
| ----- | ------
| "0"   | 0/infinite/not set
| "-1"  | Invalid value
| "1"   | 1 second
| "1s"  | 1 second
| "2m"  | 2 minutes
| "3h"  | 3 hours
| "4d"  | 4 days
| "5w"  | Invalid value
| "6M"  | Invalid value
| "7y"  | Invalid value

##### 1.2.2. Size

The configuration parameters supporting the size type:

* item-data-input-layer-size
* item-data-size
* item-data-ranges-threshold
* storage-net-rcvBuf
* storage-net-sndBuf
* load-step-limit-size

| Value   | Effect
| ------- | ------
| "-1"    | Invalid Value
| "0"     | 0 bytes (Infinity in case of `load-step-limit-size`)
| "1"     | 1 bytes
| "1024"  | 1024 bytes or 1KiB
| "0B"    | 0 bytes (Infinity in case of `load-step-limit-size`)
| "1024B" | 1024 bytes or 1KiB
| "1KiB"  | 1024 bytes
| "2MiB"  | 2,097,152 bytes
| "1KB"   | 1024 bytes (legacy binary alias)
| "2MB"   | 2,097,152 bytes (legacy binary alias)
| "6EiB"  | 6 EiB (exbibytes)
| "7YB"   | Invalid Value

All conversions between specified sizes use 2^10 (1024) multipliers. IEC suffixes (`KiB` through `EiB`) and legacy aliases (`KB` through `EB`) have the same binary values.

##### 1.2.3. Dictionary

Some configuration values support the dictionary type. Don't use the command line arguments for the dictionary values
setting.

##### 1.2.4. Expression

The [expression language](../../../../src/main/java/com/dell/spt/base/config/el/README.md) allows to assign the dynamic values 
to some configuration parameters.

### 1.3. Persisted-data integrity

The metadata integrity feature is configured as a nested scenario object. It is
disabled by default:

```javascript
"storage": {
    "integrity": {
        "mode": "metadata",
        "algorithm": "sha256",
        "input": {
            "provenance": "engine_step",
            "expectedProducerId": "direct-integrity-create"
        },
        "selection": {
            "maxCount": 0
        }
    }
}
```

| Nested setting | Default | Contract |
|---|---|---|
| `storage.integrity.mode` | `none` | `none` preserves ordinary behavior; `metadata` enables v1 persisted-data integrity |
| `storage.integrity.algorithm` | `sha256` | Version 1 supports only `sha256` |
| `storage.integrity.input.provenance` | `none` | `none`, `engine_step`, `cli_stager`, or `external`; metadata READ/DELETE must choose a non-`none` source |
| `storage.integrity.input.expectedProducerId` | empty | Exact producing step ID, or the CLI stager ID, when that provenance requires completion evidence |
| `storage.integrity.selection.maxCount` | `0` | Deterministic maximum after LIST discovery; `0` selects all discovered records |
| `storage.integrity.selection.requireNonEmpty` | `false` | Metadata LIST only. Fail before completion publication when the canonical selected inventory is empty; guarded existing-prefix DELETE enables it |

`engine_step` requires the matching manifest completion JSON from the named
CREATE or LIST step. `cli_stager` requires producer ID
`spt-cli-items-stager-v1`. `external` accepts a QA-owned finite item file without
an engine completion sidecar; the scenario author is responsible for its
completeness. `run.id` is the common positive correlation ID in completion
records and worker slices. Direct-JAR startup initializes it once when the
configured value is `0`.

For direct `spt.jar` use, supply these settings through a custom scenario's
`.config()` map. Flattened `--storage-integrity-*` startup arguments are not a
supported v1 entry point. See the
[write/read example](../scenarios/s3_integrity_write_verify.js),
[CREATE-only seed example](../scenarios/s3_integrity_seed.js), and
[read-only example](../scenarios/s3_integrity_read_verify.js).

This facility is separate from `item.data.verify`, `ReadVerifyLoad`, and
`ReadVerifyRandomRangeLoad`, which select legacy deterministic-content
verification. Metadata integrity uses `CreateLoad` and `ReadLoad`.

## 2. Aliasing

The configuration aliasing is used primarily for backward compatibility to map old configuration paths to the new ones.
Also there's a shortcut alias for the load operation types:

| Alias  | Meaning
|--------|--------
| create | load-op-type=create
| read   | load-op-type=read
| update | load-op-type=update
| delete | load-op-type=delete
| noop   | load-op-type=noop
| list   | load-op-type=list
| list-accel | load-op-list-sharding-mode

## 3. LIST Operation Specific Settings

| Path                      | Type          | Default | Description |
|---------------------------|---------------|---------|-------------|
| load-op-list-delimiter    | String/null   | null    | Optional delimiter for pseudo-directory listings. When null, full object keys are returned. |
| load-op-list-fetch_metadata | Flag        | false   | When true, aggregate size/etag metadata available in `ListObjectsV2` responses; remains lightweight when false. |
| load-op-list-include_versions | Flag     | false   | Switches to paginated `ListObjectVersions`, returning each data version with its exact version ID and excluding delete markers while counting them separately. Unversioned-bucket results are target-specific and may be empty or contain a literal `null` version. |
| load-op-list-max_keys     | Integer >= 0  | 0       | Overrides the per-request page size. 0 defers to S3 defaults; positive values are capped at the service limit (1000). |
| load-op-list-sharding-mode | String       | auto      | When unset, `auto` selects `none` for single-threaded runs and `static` when concurrency > 1. Explicit values override the default: `none` runs a single LIST stream; `static` seeds multiple prefixes locally so each worker enumerates a unique prefix. |
| load-op-list-sharding-radix | Integer >= 0 | 0      | Number of static prefixes to seed. 0 defaults to the length of `load-op-list-sharding-charset`. Profiles set this automatically but you can override it. |
| load-op-list-sharding-charset | String    | 0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ | Character set used to build static prefixes when `mode=static`. Defaults to Base62; override when targeting a narrower namespace (e.g., hex). |

The shortcut CLI flag `--list-accel=auto|none|static` maps to `load-op-list-sharding-mode`. When you specify `charset` or `radix` directly they apply only when `mode=static`.
| load-op-list-sharding-stall-timeout | Duration/String | 30s | Emits WARN logs when a shard sees no progress for this interval; also feeds adaptive heuristics stall counters. |
| load-op-list-sharding-progress-log-interval | Duration/String | 10s | TRACE logging cadence for shard progress updates when verbose logging is enabled. |
| load-op-list-sharding-adaptive-full_page_threshold | Integer >= 0 | 4 | Consecutive full pages (objects == max_keys) required before adaptive splitting is considered. 0 disables the check. |
| load-op-list-sharding-adaptive-stall_warning_threshold | Integer >= 0 | 2 | Number of stall warnings a shard must accumulate before it becomes eligible for splitting. 0 disables the check. |
| load-op-list-sharding-adaptive-backlog_ratio | Double > 0 | 1.5 | Pending shard depth divided by active leases required to trigger backlog-based splitting. |
| load-op-list-sharding-adaptive-min_pending_shards | Integer >= 0 | 4 | Minimum queue depth that must be present before backlog-based splitting fires. |
| load-op-list-sharding-adaptive-cooldown | Duration/String | 0s | Minimum time between splits of the same prefix; prevents thrashing when adaptive heuristics are aggressive. |
