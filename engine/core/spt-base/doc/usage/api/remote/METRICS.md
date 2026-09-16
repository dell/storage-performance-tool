# Metrics API guide

SPT exposes live metrics through JSON endpoints and the Prometheus text exposition endpoint. This guide describes schema 4, how metrics are scoped in standalone and distributed runs, and how consumers should interpret them.

- [Quick start](#quick-start)
- [Endpoint and node matrix](#endpoint-and-node-matrix)
- [JSON response model](#json-response-model)
- [Identity, freshness, and lifecycle](#identity-freshness-and-lifecycle)
- [Common metric groups](#common-metric-groups)
- [Timing distributions](#timing-distributions)
- [Distributed aggregation](#distributed-aggregation)
- [Operation-specific metrics](#operation-specific-metrics)
- [Prometheus metrics](#prometheus-metrics)
- [Consumer guidance](#consumer-guidance)
- [Troubleshooting](#troubleshooting)
- [Schema and examples](#schema-and-examples)

## Quick start

The REST API listens on port `9999` by default.

```bash
# Metrics local to one node
curl -s http://NODE:9999/metrics/json | jq .

# Distributed aggregate from the entry node
curl -s http://ENTRY:9999/metrics/cluster/json | jq .

# Include diagnostic fields
curl -s 'http://ENTRY:9999/metrics/cluster/json?verbose=1' | jq .

# Prometheus text exposition
curl -s http://ENTRY:9999/metrics
```

For a distributed run:

```text
entry:9999/metrics/json          entry node's local workload slice
entry:9999/metrics/cluster/json  distributed aggregate
worker:9999/metrics/json         that worker's local workload slice
worker:9999/metrics/cluster/json normally 404
```

Do not add the entry aggregate to worker values. The aggregate already represents its current contributors.

## Endpoint and node matrix

| Endpoint | Entry node | Worker node | Representation | SPT scope |
|:--|:--:|:--:|:--|:--|
| `GET /metrics/json` | Yes | Yes | JSON array | Work local to the queried node |
| `GET /metrics/cluster/json` | Yes | Normally 404 | JSON array | Distributed aggregate |
| `GET /metrics/fleet/json` | Yes | Normally 404 | JSON array | Deprecated alias of `/metrics/cluster/json` |
| `GET /metrics` | Yes | Yes | Prometheus text | SPT aggregate series on the entry; default registry/process series may exist on any node |

The cluster endpoints are registered only when `server.metrics.expose_fleet` is enabled. It is enabled by default. A cluster endpoint returns 404 when that node has no distributed metrics to return.

JSON responses use `application/json` and include `Access-Control-Allow-Origin: *`. The query parameter `verbose=1` or `verbose=true` adds diagnostic fields to live and terminal JSON samples. It does not change metric values.

### Which endpoint should I scrape?

- Use entry `/metrics/cluster/json` for one authoritative distributed view.
- Use each node's `/metrics/json` when diagnosing imbalance or collecting per-node detail.
- Use `/metrics/json` for a standalone run.
- Use entry `/metrics` when a Prometheus scraper is required.

## JSON response model

A successful JSON response is an array. Each element represents one load step or a retained terminal step. `/metrics/json` emits a single idle element when no local or retained step is available, so it is never an empty successful response. The cluster endpoint returns 404 instead of an empty successful response.

Schema 4 is additive. Consumers should ignore unknown fields, tolerate optional operation-specific sections, and check `metrics_schema` before applying schema-specific rules.

### Top-level fields

| Field | Type | Presence | Meaning |
|:--|:--|:--|:--|
| `metrics_schema` | integer | Always | Metrics contract version; this guide describes `4` |
| `scope` | string | Always | `node` for local metrics or `fleet` for a cluster aggregate |
| `role` | string | Always | `entry`, `worker`, or `aggregate` |
| `cluster_id` | string | When configured | Cluster identity shared by distributed participants |
| `node_id` | string | Always | Identity of the HTTP server producing the sample |
| `run_id` | string | Always | Run identity; empty in a pre-run idle sample |
| `sample_ts` | string | Always | RFC 3339 timestamp when the JSON sample was built |
| `step_id` | string | Always | Load-step identity; empty in an idle sample |
| `op_type` | string | Always | Operation type such as `READ`, `CREATE`, `DELETE`, or `none` for idle |
| `timestamp` | integer | Always | Unix epoch milliseconds for the sample or retained completion |
| `elapsed_time_seconds` | number | Always | Elapsed step time in seconds |
| `test_state` | integer | Always | Lifecycle state described below |
| `terminal` | boolean | Terminal only | `true` for a retained final sample |
| `delete_detail_expected` | boolean | Non-idle samples | Whether a standalone DELETE detail block is required |

`node_id` names the server that emitted the document. On an aggregate sample it identifies the entry node, not every contributing worker.

## Identity, freshness, and lifecycle

### Selecting the correct sample

Live node and fleet samples use `run.cluster.id` from the effective load-step configuration, including defaults submitted through `/run` after the API starts. Retained terminal samples preserve that run's cluster identity. For contexts created by extensions without run cluster metadata, the API's startup-configured cluster identity remains the fallback.

Treat this tuple as the sample identity:

```text
metrics_schema + cluster_id + run_id + step_id + scope + node_id
```

Do not join or replace samples using `step_id` alone. In particular, reject stale rows from another run or cluster before using them as distributed evidence.

Use the timestamps for different purposes:

- `sample_ts` says when the HTTP response element was created.
- `timestamp` is the live snapshot time; for a retained terminal row it is the completion time.
- `elapsed_time_seconds` is workload time, not wall-clock sample age.

### Lifecycle states

| `test_state` | State | Interpretation |
|:--:|:--|:--|
| `0` | Idle | No active or retained local step was available |
| `1` | Running | A step is starting, has operations in flight, or has not reached its configured bound |
| `2` | Completed | The bounded work is complete or the row is a retained terminal sample |

An idle sample has blank `run_id` and `step_id`, `op_type: "none"`, and zero-valued metric groups. It is a readiness signal, not a completed zero-throughput test.

A retained row has `terminal: true`. Its cumulative counts and timing distributions remain available after the active context is removed. Its `success_rate_last`, `failed_rate_last`, and `bytes_rate_last` values are zero because there is no longer a live sampling interval.

### Limits and progress

| Field | Meaning |
|:--|:--|
| `unbounded` | This context has neither a positive operation-count nor time limit |
| `limit.type` | `op_count`, `time`, or `none` |
| `limit.op_count` | Operation bound when `type` is `op_count` |
| `limit.time_sec` | Duration bound in seconds when `type` is `time` |
| `completion_percent` | Progress for the represented context, rounded to an integer from 0 through 100 |
| `overall_completion_percent` | Overall progress selected for this step |
| `overall_unbounded` | No represented context provides a count or time bound |

Count progress uses successful plus failed logical operations. Time progress uses elapsed time divided by the configured duration. Count takes precedence when both metadata values exist. An unbounded step reports zero progress because no denominator exists; do not interpret that as inactivity.

## Common metric groups

### Operations

| Field | Unit and behavior |
|:--|:--|
| `operations.success_count` | Cumulative successful logical operations |
| `operations.failed_count` | Cumulative failed logical operations |
| `operations.corrupt_count` | Cumulative operations whose integrity check failed |
| `operations.success_rate_last` | Latest exponentially weighted successful-operation rate per second |
| `operations.failed_rate_last` | Latest exponentially weighted failed-operation rate per second |

The logical operation represented by these counters depends on `op_type`. For standalone batch DELETE, these are API request counts, not object counts.

### Bandwidth

| Field | Unit and behavior |
|:--|:--|
| `bandwidth.bytes_total` | Cumulative bytes transferred |
| `bandwidth.bytes_rate_last` | Latest exponentially weighted byte rate per second |

Fields ending in `_rate_last` are exponentially weighted moving-average rates, updated on one-second ticks and smoothed by the configured metrics averaging period. They are not unsmoothed deltas from the most recent HTTP poll. Mean rates divide cumulative totals by elapsed step time.

DELETE retains this compatibility group with zero values. Use `delete.performance` to determine applicability instead of presenting zero as measured DELETE bandwidth.

### Concurrency

| Field | Unit and behavior |
|:--|:--|
| `concurrency.current` | Latest observed in-flight operation count |
| `concurrency.mean` | Mean observed in-flight operation count |

A momentary `current` value of zero does not by itself prove that a bounded run has completed. Use `test_state`, limits, and terminal status.

## Timing distributions

`timing.latency`, `timing.duration`, and, when available, `timing.ttfb` use this shape:

```json
{
  "count": 1000,
  "mean_us": 1250.4,
  "min_us": 700,
  "p50_us": 1100,
  "p90_us": 1800,
  "p99_us": 3500,
  "p999_us": 5200,
  "max_us": 6100,
  "overflow_count": 0
}
```

All values except `count` and `overflow_count` are microseconds. `p999_us` is the 99.9th percentile. `overflow_count` counts observations outside the histogram's representable range.

The compatibility fields `timing.latency_mean_us` and `timing.duration_mean_us` repeat the corresponding means. New consumers should use the nested distributions.

`timing.ttfb` is `null` when no body timing sample applies or has been recorded. For response-body operations, TTFB is measured from request completion to the first non-empty response body byte.

Cluster timing distributions are merged from contributor histograms. SPT computes aggregate percentiles from the merged population; never average worker percentiles.

## Distributed aggregation

A cluster row has:

```json
{
  "scope": "fleet",
  "role": "aggregate",
  "nodes_count": 3,
  "nodes_present": ["worker-a:1099", "worker-b:1099"],
  "contributors_present": ["local", "worker-a:1099", "worker-b:1099"],
  "partial": false
}
```

| Field | Meaning |
|:--|:--|
| `nodes_count` | Independently expected participant count, including the entry's local slice |
| `nodes_present` | Configured remote RMI addresses currently represented |
| `contributors_present` | Contributor identities represented by the latest successful refresh; `local` is the entry slice |
| `partial` | The aggregate is incomplete or contributor evidence is inconsistent |

`partial` becomes true when a node is missing, a contributor identity is duplicated, or an expected standalone DELETE contributor does not provide its detailed DELETE block. A failed worker refresh invalidates its cached evidence rather than silently reusing stale metrics.

Aggregation behavior:

- Operation, byte, DELETE request/object/batch/version, bucket counters, and applicable rates are additive.
- Timing histograms are merged; aggregate quantiles are computed from the merged histogram.
- Concurrency represents the distributed snapshot rather than an entry-only value.
- DELETE cluster phase durations use the maximum participating-node wall interval, not a sum.
- Worker rows are not embedded in the cluster response.

Check completeness before accepting an aggregate:

```bash
curl -s http://ENTRY:9999/metrics/cluster/json |
  jq '.[] | {
    run_id,
    step_id,
    partial,
    nodes_count,
    nodes_present,
    contributors_present
  }'
```

For per-worker comparisons, scrape `/metrics/json` on each worker and retain `node_id`. Do not sum those rows with the cluster row.

## Operation-specific metrics

### CREATE, READ, UPDATE, and other data operations

The common operation, bandwidth, timing, and concurrency groups describe these operations. Body-bearing reads may include `timing.ttfb`. `operations.corrupt_count` is relevant when content verification is enabled.

Partial reads report transferred bytes for the selected byte range. They do not report the full source-object size as transferred data.

### DELETE

Schema 4 adds `delete` only for the first-class standalone DELETE path. Generic or cleanup DELETE rows may have `delete_detail_expected: false` and no `delete` object. When `delete_detail_expected` is true, consumers should require a valid `delete` object.

#### Units and outcomes

- `delete.units.requests` and `delete.units.batches` are `logical_api_requests`.
- `delete.units.objects` is `object_identities`.
- `accepted` means accepted by the storage API; it does not prove physical removal.
- Generic `operations.success_count` records fully reconciled requests.
- Generic `operations.failed_count` records partial or failed requests.

#### Request and object accounting

| Object | Fields |
|:--|:--|
| `requests` | `attempted`, `full_success`, `partial`, `failed`, `unresolved`, `per_second` |
| `objects` | `selected`, `attempted`, `accepted`, `failed`, `unattempted`, `unresolved`, `per_second` |
| `completion` | `request_percent`, `object_percent`, `terminal_reconciled` |

Request rate counts dispatched `DeleteObject` or `DeleteObjects` calls. Object rate counts attempted identities. Both use the scheduled DELETE interval; setup, barrier wait, and drain time are excluded.

Object completion counts accepted, failed, unattempted, and unresolved identities as accounted. Request completion and object completion are intentionally independent.

#### Batch and identity accounting

`delete.batches` contains:

- `configured_size`
- `actual_request_count`
- `actual_object_count`
- `mean_objects_per_request`
- `full_batch_count`
- `partial_batch_count`
- `full_batch_percent`

Actual counts are observed, not inferred from configured size. `delete.identity` reports `mode` (`single` or `batch`), `configured_batch_size`, and `selection_order` (`canonical`). Aggregate contributors must agree on these values.

#### Versions and buckets

`delete.versions` distinguishes `current_key` and `exact_version`. Schema 4 does not expose all-version or delete-marker counters.

`delete.buckets` contains `{bucket, selected, attempted, accepted, failed}`. At most 100 named buckets are retained; additional names are combined under `__other__`. Bucket latency is intentionally omitted to bound cardinality.

#### Phases

`delete.phases` contains:

- `seed`
- `discovery`
- `pre_validation`
- `scheduled_delete_seconds`
- `drain_seconds`
- `post_verification`
- `cleanup`
- `total_wall_seconds`

Values are seconds. `null` means the phase does not apply; zero means it applied but had no observable duration. `total_wall_seconds` is independently measured from setup through drain and is not reconstructed by adding named phases.

#### Request timing and performance applicability

`delete.timing.latency` measures first request byte sent through first response byte received. `delete.timing.duration` measures request formulation through the last response byte received. `delete.timing.object_latency` is always `null` because batch object latency cannot be derived from request timing.

`delete.performance.object_size`, `data_moved`, `bandwidth`, and `ttfb` are `not_applicable`.

#### Failure policy

`delete.failure_policy` reports:

- `mode`: `fixed` or `percentage`
- `outcome`: `running`, `completed_cleanly`, `completed_within_failure_budget`, or `failed`
- `max_failed_objects`
- `max_failure_percent`
- `grace_seconds`
- `operational_failed_objects`
- `excluded_failed_objects`
- `observed_failure_percent`

The observed percentage is operational failures divided by accepted plus operational failures. Excluded protocol or correctness failures do not enter that denominator.

#### Verification

`delete.verification` separates API outcomes from observed object state. It reports:

- Whether pre-validation or post-verification is enabled and complete
- Whether post-verification was skipped
- Verification timeout and pre-validation failures
- Aggregate `verified_absent`, `still_present`, and `unresolved` counts
- The `accepted_*`, `failed_*`, `operational_unresolved_*`, and `unattempted_*` outcome-by-observation matrix
- Derived `correctness_failures`, `inconclusive_failures`, and `residual`
- `removal_confirmed` and a human-readable `notice`

`removal_confirmed` is true only when strict pre-validation and post-verification both completed, every identity existed beforehand, and no correctness, inconclusive, operational failure, unresolved, or unattempted target remains. Post-verification without pre-validation establishes observed absence, not causation.

`delete.terminal_reconciled` separately states whether final object lifecycle accounting balances.

### LIST shard discovery

When adaptive LIST shard metrics are enabled, `list_shards` contains:

| Field | Meaning |
|:--|:--|
| `total_pages` | Pages returned by all recorded shards |
| `total_objects` | Objects discovered by all recorded shards |
| `avg_page_millis` | Mean page-response duration in milliseconds |
| `captured_at_millis` | Unix epoch milliseconds when the recorder snapshot was captured |
| `total_splits` | Number of shard splits |
| `max_depth` | Maximum split-tree depth |
| `split_reasons` | Map from reason name to split count |
| `shards` | Per-prefix shard records |

Each shard record contains `prefix`, page and object totals, average page time, first and last update times, activity and stall state, full-page streak information, and split history. Time fields ending in `_millis` are Unix epoch milliseconds except `avg_page_millis`, which is a duration.

`list_shards` is conditional. Its absence means no LIST shard recorder was attached; it does not imply zero LIST activity.

## Prometheus metrics

`GET /metrics` exposes the process-wide Prometheus registry. JVM, process, or library collectors may appear in addition to SPT series.

SPT's custom collector is registered for distributed metrics contexts. Therefore, use the entry node for SPT aggregate Prometheus series. A worker can expose registry/process metrics without exposing worker-local SPT operation series. Use worker `/metrics/json` for the supported local workload view.

### SPT metric families

| Series | Unit | Meaning |
|:--|:--|:--|
| `spt_duration_count` | operations | Duration observations |
| `spt_duration_sum` | seconds | Sum of duration observations |
| `spt_duration_mean` | seconds | Mean duration |
| `spt_duration_min` / `spt_duration_max` | seconds | Minimum and maximum duration |
| `spt_latency_count` | operations | Latency observations |
| `spt_latency_sum` | seconds | Sum of latency observations |
| `spt_latency_mean` | seconds | Mean latency |
| `spt_latency_min` / `spt_latency_max` | seconds | Minimum and maximum latency |
| `spt_concurrency_mean` | operations | Mean concurrency |
| `spt_concurrency_last` | operations | Latest concurrency |
| `spt_byte_count` | bytes | Cumulative transferred bytes |
| `spt_byte_rate_mean` | bytes/second | Mean transfer rate since step start |
| `spt_byte_rate_last` | bytes/second | Latest exponentially weighted transfer rate |
| `spt_success_op_count` | operations | Cumulative successful operations |
| `spt_success_op_rate_mean` | operations/second | Mean successful-operation rate since step start |
| `spt_success_op_rate_last` | operations/second | Latest exponentially weighted successful-operation rate |
| `spt_failed_op_*` | corresponding count/rate | Failed operations |
| `spt_corrupt_op_*` | corresponding count/rate | Integrity failures, when available |
| `spt_elapsed_time_value` | seconds | Elapsed step time |
| `spt_test_state_value` | enum gauge | `0` idle, `1` running, `2` completed |
| `spt_completion_percent_value` | percent | Bounded context progress |

Prometheus timing values are seconds; JSON timing distribution values are microseconds. The current Prometheus collector does not export percentile series. Use JSON schema-4 timing distributions or the final metrics artifact when percentiles are required.

### Labels

SPT custom series use these labels:

- `load_step_id`
- `load_op_type`
- `storage_driver_limit_concurrency`
- `item_data_size`
- `start_time`
- `node_list`
- `user_comment`
- `run_id`

Treat `user_comment`, step identifiers, and node lists as potential cardinality sources. Avoid copying arbitrary run-specific values into long-retention recording rules unless needed.

### Example PromQL

```promql
# Latest aggregate operation rate
spt_success_op_rate_last{load_op_type="READ"}

# Latest aggregate bandwidth in MiB/s
spt_byte_rate_last / 1024 / 1024

# Cumulative failures
spt_failed_op_count

# Mean latency in milliseconds
spt_latency_mean * 1000

# Bounded completion
spt_completion_percent_value
```

## Consumer guidance

1. Poll the entry cluster endpoint for a distributed summary.
2. Poll workers only when per-node diagnosis is required.
3. Match `metrics_schema`, `cluster_id`, `run_id`, and `step_id` before replacing or combining evidence.
4. Flag or reject `partial: true` according to the application's completeness requirements.
5. Treat `terminal: true` rows as retained final totals.
6. Never add a cluster aggregate to worker rows.
7. Never average worker percentiles.
8. Use DELETE object counters for object throughput; generic operation counters are request units.
9. Treat `null` and absent optional sections as unavailable, not zero.
10. Ignore unknown additive fields within schema 4.
11. Require explicit support before interpreting another `metrics_schema` value.
12. Use `sample_ts` to detect stale HTTP samples and `timestamp` to identify retained completion time.

A safe cluster-row selector can begin with:

```bash
curl -s http://ENTRY:9999/metrics/cluster/json |
  jq --arg run "$RUN_ID" --arg step "$STEP_ID" '
    .[]
    | select(.metrics_schema == 4)
    | select(.run_id == $run and .step_id == $step)
    | select(.scope == "fleet" and .role == "aggregate")
    | select(.partial == false)
  '
```

## Troubleshooting

| Observation | Explanation or action |
|:--|:--|
| Blank run and step IDs with `test_state: 0` | Normal pre-run idle sample |
| Cluster endpoint returns 404 | The node is a worker, fleet exposure is disabled, or no distributed context is available |
| `timing.ttfb` is `null` | TTFB does not apply or no body sample was recorded |
| `partial` is true | Compare expected nodes with `nodes_present` and `contributors_present`; inspect worker health and logs |
| Terminal rates are zero | Expected; terminal rows retain totals but have no live interval |
| Metrics remain after completion | Expected terminal retention |
| `cluster_id` is absent | No cluster identity was configured; avoid combining the sample with another node without external identity evidence |
| Diagnostic fields are absent | Add `?verbose=1` or `?verbose=true` |
| Worker `/metrics` lacks `spt_*` workload series | Use worker `/metrics/json`; custom Prometheus workload collectors are distributed-context collectors |
| JSON and Prometheus timing units differ | JSON uses microseconds; Prometheus timing series use seconds |

Verbose JSON can add:

- `diag_distributed_contexts`: distributed contexts visible to the responder
- `diag_local_contexts`: local non-distributed contexts visible to the responder
- `diag_cached_progress`: the row used retained progress data

## Schema and examples

The machine-readable schema is [metrics-schema-v4.json](metrics-schema-v4.json). Published examples are under [examples](examples):

- [Idle node](examples/metrics-v4-idle.json)
- [Local READ](examples/metrics-v4-node-read.json)
- [Aggregate READ](examples/metrics-v4-cluster-read.json)
- [Local DELETE](examples/metrics-v4-node-delete.json)
- [Aggregate DELETE](examples/metrics-v4-cluster-delete.json)
- [LIST shard discovery](examples/metrics-v4-list-shards.json)

Contract tests validate these examples and the engine's schema-4 DELETE fixtures against the schema. When the runtime contract changes, update the responder tests, schema, examples, and this guide together. Preserve older versioned schemas and examples for consumers that still support them.
