# Content

1. [Introduction](#1-introduction)<br/>
2. [Limitations](#2-limitations)<br/>
3. [Requirements](#3-requirements)<br/>
4. [Approach](#4-approach)<br/>
&nbsp;&nbsp;4.1. [Integrations](#41-integrations)<br/>
&nbsp;&nbsp;4.2. [API](#42-api)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.2.1. [Config](#421-config)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.2.2. [Run](#422-run)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;4.2.2.1. [Standalone mode](#4221-standalone-mode)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;4.2.2.2. [Distributed mode](#4222-distributed-mode)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.2.3. [Logs](#423-logs)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;4.2.3.1. [Available log names](#4231-available-log-names)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;4.2.3.2. [Get the log file from the beginning](#4232-get-the-log-file-page-from-the-beginning)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;4.2.3.3. [Get the specified log file part](#4233-get-the-specified-log-file-part)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;4.2.3.4. [Delete the log file](#4234-delete-the-log-file)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.2.4. [Metrics](#424-metrics)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.2.5. [Shutdown](#425-shutdown)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.2.6. [Status](#426-status)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.2.7. [Health](#427-health)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.2.8. [Readiness](#428-readiness)<br/>
5. [Configuration](#5-configuration)<br/>
6. [Output](#6-output)<br/>
&nbsp;&nbsp;6.1. [Metrics](#61-metrics)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;6.1.1. [Custom quantiles](#611-custom-quantiles)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;6.1.2. [Labels](#612-labels)<br/>

# 1. Introduction

The specific remote APIs are required to build the full-featured storage performance testing services on top of
Spt. The application may be a monitoring system either control UI.

# 2. Limitations

| # | Description |
|:--|:------------|
| 2.1 | Run mode should be "node" instead of default ("interactive"). See the [Configuration](#5-configuration) section for the details

# 3. Requirements

| # | Description |
|:--|-------------|
| 3.1 | A remote API user should be able to fetch aggregated configuration defaults from the Spt node
| 3.2 | A remote API user should be able to fetch the aggregated configuration schema from the Spt node
| 3.3 | A remote API user should be able to run a new scenario on the Spt node
| 3.4 | A remote API user should be able to stop the running scenario on the Spt node
| 3.5 | A remote API user should be able to determine if the Spt node is running a scenario or not
| 3.6 | A remote API user should be able to identify the scenario running on the Spt node
| 3.7 | A remote API user should be able to fetch the log file content from the Spt node
| 3.8 | A remote API user should be able to fetch the current metrics in the Prometheus export format from the Spt node

# 4. Approach

## 4.1. Integrations

To serve the Remote API the following libraries are used:
* [Jetty](https://www.eclipse.org/jetty/) to serve the HTTP requests
* [Prometheus instrumentation](https://github.com/prometheus/client_java) library to export the metrics

## 4.2. API

> See the full documentation [here](https://app.swaggerhub.com/apis/veronikaKochugova/Spt/4.2.3)

### 4.2.1. Config

Get config from node:
```bash
curl GET http://localhost:9999/config
```
> More about configuration [here](../../input/configuration)

Get schema from node:
```bash
curl GET http://localhost:9999/config/schema
```
> The schema relates configuration parameters to the required types.

### 4.2.2. Run

#### 4.2.2.1. Standalone mode

Start a new scenario run:
```bash
curl -v -X POST \
    -F defaults=@src/test/robot/api/remote/data/aggregated_defaults.yaml \
    -F scenario=@src/test/robot/api/remote/data/scenario_dummy.js \
    http://localhost:9999/run
```

It's possible to omit the `defaults` and `scenario` parts (default ones may be used):
```bash
curl -v -X POST http://localhost:9999/run
```

Also, the partial defaults configuration may be supplied too:
```bash
curl -v -X POST \
    -F "defaults={storage:{driver:{type: dummy-mock}}};type=application/yaml" \
    http://localhost:9999/run
```
> **Note**: use this example above as the most simple way to start via the remote API.

If successful, the response will contain the ETag header with the hexadecimal timestamp (Unix epoch time):
```bash
...
< HTTP/1.1 202 Accepted
< Date: Mon, 26 Nov 2018 18:35:50 GMT
< ETag: 167514e6082
< Content-Length: 0
...
```

This ETag should be considered as a **run id** and may be used to check the run state (using HEAD/GET request) either stop
it (using DELETE request).

Checking if the given node executes a scenario:
```bash
curl -v -X HEAD http://localhost:9999/run
...
< HTTP/1.1 200 OK
< Date: Mon, 26 Nov 2018 18:40:10 GMT
< ETag: 167514e6082
< Content-Length: 0
...
```

The `If-Match` header with the hexadecimal run id value may be used also:

Checking the run state:
```bash
curl -v -X GET -H "If-Match: 167514e6082" http://localhost:9999/run
...
< HTTP/1.1 200 OK
< Date: Mon, 26 Nov 2018 18:40:10 GMT
< Content-Length: 0
...
```

Stopping the run:
```bash
curl -v -X DELETE -H "If-Match: 167514e6082" http://localhost:9999/run
...
< HTTP/1.1 200 OK
< Date: Mon, 26 Nov 2018 18:41:26 GMT
< Content-Length: 0
```

#### 4.2.2.2. Distributed mode
To start Spt in distributed mode via REST you need to have 2 Spt nodes in `--run-node` mode. Let's say you have started them via docker. One using the default ports: 

```docker run --network host ghcr.io/dell/storage-performance-tool:4.2.17 --run-node```

And one using port 1098: 

```docker run -p 1098:1099 ghcr.io/dell/storage-performance-tool:4.2.17 --run-node```

Notice that we use `--network host` on the first node only. As both nodes run on the same machine in this example, it's important to remember not to use the same ports for different nodes. As Spt expects REST calls on 9999 port by default and we only open this port for the first node, this will be the node we send requests to.

Then in the `defaults.yaml` that you pass to the node you need to specify additional node address as usual (see [distributed mode docs](../../../design/modes/distributed_mode)):

```
load:
  step:
    node:
      addrs:
      - localhost:1098
 ```
 
Then send the request like you would do in the standalone mode. E.g.

```
curl -v -X POST \
	-H "Content-Type:multipart/form-data" \
    -F defaults=@/path/to/defaults.yaml \
    http://localhost:9999/run
```

### 4.2.3. Logs

#### 4.2.3.1. Available Log Names

| Log Name | Purpose |
|:--|:--|
| Cli | Command line arguments dump
| Config | Full load step configuration dump
| Errors | Error messages
| OpTraces | Load operation traces (transfer byte count, latency, duration, etc)
| metrics.File | Load step periodic metrics
| metrics.FileTotal | Load step total metrics log
| metrics.threshold.File | Load step periodic threshold metrics
| metrics.threshold.FileTotal | Load step total threshold metrics log
| Messages | Generic messages
| Scenario | Scenario dump

The log names may be also obtained using the request:

```bash
curl -X GET http://localhost:9999/logs
{
  "Cli" : "CLI args",
  "metrics.File" : "Metrics",
  "metrics.FileTotal" : "Metrics Total",
  "Config" : "Base config",
  "Errors" : "Errors",
  "Scenario" : "Scenario",
  "metrics.threshold.FileTotal" : "Threshold Metrics Total",
  "OpTraces" : "Operation Traces",
  "Messages" : "Messages"
}
```

#### 4.2.3.2. Get The Log File Page From The Beginning

```bash
curl http://localhost:9999/logs/123/Messages
```
In this example: step id = "123", log name = "Messages"

#### 4.2.3.3. Get The Specified Log File Part

```bash
curl -H "Range: bytes=100-200" http://localhost:9999/logs/123/Messages
r the type "dummy-mock"
2018-11-27T16:19:34,982 | DEBUG | LinearLoadStepClient | main | com.dell.spt.storage.driver.mock.DummyStorageDriverMock@6aecbb8d: shut down
2018-11-27T16:19:34,982 | DEBUG |
```

#### 4.2.3.4. Delete The Log File

```bash
curl -X DELETE http://localhost:9999/logs/123/Messages
```

#### 4.2.3.5. HEAD For Logs (existence/size check)

Use `HEAD /logs/<stepId>/<logger>` to check for availability without downloading content. Returns:
- 200 OK with `Content-Length` and `Last-Modified` when present;
- 404 Not Found if the step or logger is missing.

Also note: unknown or invalid logger names now return 404 (instead of 400) for consistency.

#### 4.2.3.6. Per-Step Log Index

List all available artifacts for a specific step id:

```bash
curl http://localhost:9999/logs/<stepId>/index.json
```

Response:

```json
{"step_id":"<stepId>","items":[
  {"logger":"metrics.FileTotal","href":"/logs/<stepId>/metrics.FileTotal","size":12345,
   "modified":"Wed, 10 Sep 2025 12:00:00 GMT","content_type":"text/csv"}
]}
```

If no files exist yet the endpoint still returns 200 with `"items": []`.

### 4.2.4 Metrics

For real-time monitoring the metrics are exposed in the [Prometheus's](https://github.com/prometheus/client_java) format.
Notice that since 4.3.0 only mean latency and duration are available during the test run. The qunatile values can be
accessed through the metrics.total.csv log file.

Example using the command:
```bash
curl http://localhost:9999/metrics
```

->

```
# HELP spt_duration 
# TYPE spt_duration gauge
spt_duration_count{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 559.0
spt_duration_sum{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 0.083571
spt_duration_mean{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 1.4950089445438282E-4
spt_duration_min{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 2.0E-6
spt_duration_quantile_0_25{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 2.0E-6
spt_duration_quantile_0_5{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 6.0E-6
spt_duration_quantile_0_75{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 9.0E-6
spt_duration_max{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 0.011517
# HELP spt_latency 
# TYPE spt_latency gauge
spt_latency_count{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 559.0
spt_latency_sum{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 0.029502
spt_latency_mean{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 5.2776386404293386E-5
spt_latency_min{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 1.0E-6
spt_latency_quantile_0_25{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 1.0E-6
spt_latency_quantile_0_5{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 1.0E-6
spt_latency_quantile_0_75{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 5.0E-6
spt_latency_max{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 0.011512
# HELP spt_concurrency 
# TYPE spt_concurrency gauge
spt_concurrency_mean{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 0.0
spt_concurrency_last{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 0.0
# HELP spt_byte 
# TYPE spt_byte gauge
spt_byte_count{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 2.628929978368E12
spt_byte_rate_mean{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 8.763099927893334E11
spt_byte_rate_last{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 7.021527038085463E11
# HELP spt_success_op 
# TYPE spt_success_op gauge
spt_success_op_count{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 2507143.0
spt_success_op_rate_mean{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 835714.3333333334
spt_success_op_rate_last{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 669624.9998174155
# HELP spt_failed_op 
# TYPE spt_failed_op gauge
spt_failed_op_count{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 0.0
spt_failed_op_rate_mean{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 0.0
spt_failed_op_rate_last{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 0.0
# HELP spt_elapsed_time 
# TYPE spt_elapsed_time gauge
spt_elapsed_time_value{load_step_id="linear_20190304.123915.606",load_op_type="READ",storage_driver_limit_concurrency="1",item_data_size="1MB",start_time="1551703155695",node_list="[]",user_comment="",run_id="123"} 3.778
```

Additionally, JSON-formatted endpoints are available for clients that prefer structured data:

- `GET /metrics/json` (always present) returns per-node metrics. Each array element includes:
  - Metadata: `metrics_schema` (currently `4`), `scope` (`"node"`), `role` (`"entry"` or `"worker"`), `run_id`, `node_id`, `cluster_id` for configured active runs, and `sample_ts` (RFC3339 timestamp). The public CLI supplies one nonempty cluster identity to every participant from the preallocated run ID.
  - Step data: `step_id`, `op_type`, `timestamp`, `elapsed_time_seconds`, `test_state`.
  - Metric groups: `operations{success_count,failed_count,success_rate_last,failed_rate_last}`, `bandwidth{bytes_total,bytes_rate_last}`, `timing`, `concurrency{current,mean}`.
  - Timing: schema 4 retains the schema 2/3 compatibility fields `timing.latency_mean_us` and `timing.duration_mean_us`, and the nested `timing.latency` and `timing.duration` objects with `count`, `mean_us`, `min_us`, `p50_us`, `p90_us`, `p99_us`, `p999_us`, `max_us`, and `overflow_count`.
  - Time to first byte: `timing.ttfb` has the same nested shape when available, or `null` when the operation type has no body timing sample. TTFB is measured from request completion to the first non-empty response body byte.
  - Progress helpers: `completion_percent`, `overall_completion_percent`, `overall_unbounded`, plus `limit{type,op_count?,time_sec?}`.
  - Terminal entries (`"terminal": true`) persist after a step finishes so idle nodes still return context; their rate gauges are set to zero and `timestamp` reflects completion time.

Schema 4 adds a `delete` object only for the first-class standalone DELETE request path. It is
additive: every schema 2/3 field above keeps its name and meaning, and tolerant older consumers may
ignore `delete`. Generic `operations` remain logical API request units: a fully reconciled request
increments `success_count`, while a partial or failed request increments `failed_count`. They never
count object targets.

The DELETE object has the following contract:

- `units` labels `requests` and `batches` as `logical_api_requests` and `objects` as
  `object_identities`.
- `requests` contains `attempted`, `full_success`, `partial`, `failed`, `unresolved`, and
  `per_second`. One request is one `DeleteObject` or `DeleteObjects` invocation. `per_second` is
  cumulative attempted request dispatches divided by the scheduled DELETE interval.
- `objects` contains `selected`, `attempted`, `accepted`, `failed`, `unattempted`, `unresolved`,
  and `per_second`. Its denominator is the same scheduled DELETE interval; the numerator is
  attempted object identities. `accepted` describes the logical API outcome; it does not assert
  removal.
- `batches` contains `configured_size`, `actual_request_count`, `actual_object_count`,
  `mean_objects_per_request`, `full_batch_count`, `partial_batch_count`, and
  `full_batch_percent`. Actual counts are observed and are not inferred from configured size.
- `completion` contains request and object percentages plus `terminal_reconciled`. Object
  completion uses accounted object identities, independently of request completion.
- `versions` contains `current_key` and `exact_version`. No all-version or delete-marker metric is
  exposed by this schema.
- `buckets` contains bounded `{bucket,selected,attempted,accepted,failed}` entries. At most 100
  named buckets are retained; additional buckets are combined as `__other__`. Distributed slicing
  freezes one canonical retained-name set before scattering, and every worker receives that same
  set (including zero counts), so names cannot split across worker-local overflow decisions. Bucket
  latency is intentionally absent, keeping metric cardinality bounded.
- The top-level `delete_detail_expected` boolean is true only for standalone DELETE rows. Consumers
  strictly validate a missing or malformed `delete` block only when this marker is true. Generic and
  cleanup DELETE rows with the marker false or absent remain valid additive schema-v4 metrics.
- `phases` contains `seed`, `discovery`, `pre_validation`, `scheduled_delete_seconds`,
  `drain_seconds`, `post_verification`, `cleanup`, and `total_wall_seconds`. A phase that is not
  part of the current engine step is JSON `null`; zero is reserved for an applicable measured
  interval that took no observable time. Scheduled DELETE starts at actual worker admission release,
  after setup and the controller barrier. Count mode ends it at finite-generator exhaustion or an
  earlier admission close; duration mode ends it at the configured deadline or an earlier admission
  close. Setup, controller wait, and drain are excluded from request/object rate denominators. Seed
  and discovery use monotonic clocks. `total_wall_seconds` is one independently measured monotonic
  interval from setup start through DELETE drain; it includes configuration, slicing, orchestration,
  inter-phase overhead, and the controller barrier instead of being reconstructed from named phases.
  Its fixed-offset epoch alignment lets participating JVMs exchange the monotonic boundary without
  comparing private `System.nanoTime()` origins. Distributed participants must have synchronized
  system clocks when their runtimes establish the offset. If clock skew puts the shared setup
  boundary in a worker's future, that worker falls back to its local DELETE-step start instead of
  reporting a negative interval. Skew in the other direction can conservatively enlarge total wall
  time. Neither case changes the scheduled DELETE interval used by request/object rate denominators.
- `timing.latency` and `timing.duration` use the standard distribution shape, including p50, p90,
  p99, and p99.9. DELETE request latency remains first request byte sent through first response
  byte received; duration remains request formulation through the last response byte. Netty marks
  the first nonempty encoded (or TLS-encrypted) outbound buffer and the first readable inbound
  transport buffer before HTTP status and headers are decoded.
  The AWS adapter records native CRT stream `send-start` and `receive-start` timestamps for both
  header-only single DELETE and batched DELETE. Publisher subscription, request-body delivery,
  signed-request handoff, and SDK-future completion are not byte markers. The adapter marks the first
  received response headers and completes duration only when the response publisher itself completes.
  Transparent retries retain the logical request's first send marker while replacing prior-attempt
  response timing, so a terminal failure does not manufacture stale samples. Consumers validate the
  available fleet population as
  `0 <= latency count <= duration count <= terminal request count`; sparse transport failures do not
  invalidate an otherwise authoritative terminal aggregate.
  `object_latency` is always
  `null` because batch target latency cannot be derived from request timing.
- `performance.object_size`, `data_moved`, `bandwidth`, and `ttfb` are `not_applicable`. The
  compatibility `bandwidth` group remains present and zero-valued; consumers must use DELETE's
  applicability labels instead of presenting zero as a transfer measurement.
- `identity` records `mode` (`single` or `batch`), `configured_batch_size`, and canonical
  `selection_order`. Aggregates reject contributors with different identity values.
- `failure_policy` records `mode`, `max_failed_objects`, `max_failure_percent`, `grace_seconds`,
  `operational_failed_objects`, `excluded_failed_objects`, and `observed_failure_percent` in object
  units. The observed percentage is operational failures divided by accepted plus operational
  failures; excluded protocol/correctness failures do not enter the denominator.
- `outcome_terminology` is always `accepted`. `verification` reports the independently configured
  pre-validation and post-verification phases: `enabled`, `pre_validation_enabled`,
  `post_verification_enabled`, their complete/skipped states, `timeout_seconds`, and
  `pre_validation_failures`. `verified_absent`, `still_present`, and `unresolved` are the aggregate
  post-observation counts. The `accepted_*`, `failed_*`, `operational_unresolved_*`, and
  `unattempted_*` fields form the outcome-by-observation matrix; `correctness_failures`,
  `inconclusive_failures`, and `residual` are derived from that matrix. `removal_confirmed` is true
  only when strict pre-validation and post-verification both completed, pre-validation found every
  identity, and there are no correctness, inconclusive, operationally failed, operationally
  unresolved, or unattempted targets. Post-verification without strict pre-validation establishes
  observed absence but not causal removal, so `removal_confirmed` remains false. `notice` states one
  of four conditions: verification disabled, post-verification skipped after strict pre-validation
  failure, post-verification without causal pre-validation, or full validation/verification
  classifications reported. `terminal_reconciled` separately states whether the final object
  lifecycle invariants hold.

Worker `/metrics/json`, entry `/metrics/json`, live cluster aggregates, and retained terminal
entries use the same DELETE shape. Cluster phases use the maximum node wall interval rather than a
sum, while request/object/batch/version/bucket counters and rates are additive. Clients use the
fleet aggregate's merged request histograms for exact multi-node quantiles; quantiles are never
averaged or reconstructed from node percentiles.

- `GET /metrics/cluster/json` (entry nodes only, see `server.metrics.expose_fleet` below) returns cluster-wide aggregates. Items reuse the fields above with `scope = "fleet"` and `role = "aggregate"`, and add `nodes_count`, the existing `nodes_present` list of configured remote RMI addresses, the additive schema-v4 `contributors_present` list of identities represented by the current successful fetch, and `partial` (true when a node is missing, a contributor identity is duplicated, or any expected DELETE contributor lacks its detailed DELETE block). `contributors_present` contains `local` for the entry slice plus each fresh participating remote RMI address; `nodes_count` is the independently expected participant count. Keeping contributor evidence separate preserves the pre-v4 `nodes_present` representation. A failed worker refresh invalidates its cached snapshot rather than reusing stale evidence. Schema-v4 fleet and contributor rows require nonempty, exact run, cluster, step, and contributor identities; schema 2/3 rows retain their documented compatibility tolerance. The CLI selects one current run/cluster/step row per operation and normalizes polled hosts to the same identities, so stale, missing, duplicate, or cross-cluster evidence cannot authorize an exact fleet DELETE timing replacement. A legacy alias is still exposed at `/metrics/fleet/json` for older clients.

Distributed vs worker behavior:
- Entry node `/metrics/json` now emits only the entry’s local workload. Clients should rely on `/metrics/cluster/json` (or the `/metrics/fleet/json` alias) for cluster totals.
- Worker nodes continue to expose their local snapshot at `/metrics/json`; `/metrics/cluster/json` and `/metrics/fleet/json` return 404 on workers.

Diagnostics:
- `?verbose=1` on either endpoint appends `diag_distributed_contexts` and `diag_local_contexts` to help debug registry visibility.

### 4.2.5 Shutdown

Gracefully stop a node via the REST API. This closes internal services and then stops the HTTP server. The `/status` endpoint remains available for a short linger window (see `api.linger.sec`) before the server exits.

Endpoint:
- `POST /shutdown` → 202 Accepted with a small JSON body `{ "accepted": true }`.

Example:
```bash
curl -v -X POST http://localhost:9999/shutdown
```

Notes:
- The request returns immediately; shutdown proceeds asynchronously.
- During the linger period the server may still respond to `/status` with a terminal state (e.g., `STOPPED`).
- If you need to stop only the current run (but keep the node up), use `DELETE /run` with the `If-Match: <run_id>` header.

### 4.2.6 Status

Report node/run status in a stable JSON form; useful for clients to determine lifecycle state without scraping logs.

Endpoint:
- `GET /status` → 200 OK, `Content-Type: application/json`

Example body:

```json
{"state":"RUNNING","since":"2025-09-10T12:34:56Z","run_id":1757500000000,"step_id":"my-step-001"}
```

Fields:
- `state`: one of `IDLE`, `RUNNING`, `COMPLETED`, `STOPPED`.
- `since`: ISO‑8601 instant indicating when the state was entered.
- `run_id` and `step_id`: included when known.

Availability:
- Served during a run and for a short window after terminal cleanup, controlled by `api.linger.sec` (default 20s).

# 5. Configuration

| Option | Type | Default Value | Description
|:--|:--|:--|:--|
| output-metrics-quantiles | List of numbers each in the range (0; 1] | [0.25,0.5,0.75] | Extra quantile values to calculate and report for timing metrics. The standard final/API set includes p50, p90, p99, and p99.9.
| run-node | Boolean | `false` | Run in the mode node. Should be enabled to serve the Remote API
| run-port | Integer in the range (0; 65536) | 9999 | The port to listen the Remote API requests
| server.metrics.expose_fleet | Boolean | `true` | Controls whether the entry node exposes `/metrics/fleet/json`. Disable when you want only per-node metrics.

# 6. Output

## 6.1. Metrics

Format of the metric name : `<app name>_<metric name>_<agrigation type>`.
All metric units are using [SI](https://prometheus.io/docs/practices/naming/#base-units).

There metrics being exposed:
* duration
* latency
* concurrency
* successful operation count
* failed operation count
* transferred size in bytes (BYTE)
* elapsed time
* completion_percent (0–100): per-context completion based on op-count limit, or time limit if no count limit
* step_completion_percent (0–100): aggregated completion across all sub-steps sharing the same step id

and 3 Primitive Types: Timing, Rate, Concurrency. Depends on the type of metric, which aggregation types are exported. The table below provides a description:

<table>
    <thead>
        <tr>
            <th>Metric name</th>
            <th>Primitive type</th>
            <th>Aggregation types</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Duration</td>
            <td rowspan=2>Timing</td>
            <td rowspan=2> <ul><li>count<li>sum<li>mean<li>min<li>max<li>quntile_'value' (<a href="#611-custom-quantiles">configured</a>)<ul> </td>
        </tr>
        <tr>
            <td>Latency</td>
        </tr>
        <tr>
            <td>Concurrency</td>
            <td>Concurrency</td>
            <td><ul><li>mean<li>last</td>
        </tr>
        <tr>
            <td>Bytes</td>
            <td rowspan=3>Rate</td>
            <td rowspan=3> <ul><li>count<li>sum<li>meanRate<li>lastRate<ul> </td>
        </tr>
         <tr>
            <td>Success</td>
        </tr>
        <tr>
            <td>Fails</td>
        </tr>
        <tr>
            <td>Elapsed time</td>
            <td>Gauge</td>
            <td>value</td>
        </tr>
    </tbody>
</table>

### 6.1.1. Custom Quantiles

SPT always reports the standard timing percentiles p50, p90, p99, and p99.9 for duration and latency, and for TTFB when a body timing sample is available. It is also possible to export extra quantile values via the `output-metrics-quantiles` configuration option. Timing percentiles are calculated from bounded in-memory histograms and do not require raw per-operation timing files.

CLI example:
```bash
java -jar spt-<VERSION>.jar ... --output-metrics-quantiles=0.5,0.95,0.999
``` 

To specify extra quantiles, use the `--output-metrics-quantiles` parameter. By default `output-metrics-quantiles=[0.25,0.5,0.75]`, which adds the historical low/median/high quartile columns alongside the standard percentile set.

### 6.1.2. Labels
Each metric contains also the following labels/tags:

|Label name|Configured param|Type|
|:---|:---|---|
|`load_step_id`|load-step-id|string|
|`load_op_type`|load-op-type|string, [takes one of these values](../../../usage/load/operations/types#load-operation-types)|
|`storage_driver_limit_concurrency`|storage-driver-limit-concurrency|integer|
|`node_count`|the count of the Spt nodes involved into the given load step|integer|
|`item_data_size`|item-data-size|string with the unit suffix (KiB, MiB, ...; legacy KB/MB aliases are accepted)|
|`user_comment`|run-comment|string|
|`run_id`|run-id|string|

### 4.2.7 Health

Lightweight liveness probe. Returns 200 with basic node identity.

Request:
```bash
curl -s http://localhost:9999/health | jq
```

Response:
```json
{
  "status": "ok",
  "scope": "node",
  "role": "worker",         
  "node_id": "host:9999",
  "cluster_id": "c-01"      
}
```

Notes:
- `role` is `entry` on the entry node (when distributed contexts are present), otherwise `worker`.
- `node_id` defaults to `<hostname[:port]>` if not configured.

### 4.2.8 Readiness

Readiness for orchestrators. Returns 503 until the API and core services are ready; switches to 200 when ready. Includes a boolean flag and identity fields mirroring `/health`.

Request:
```bash
curl -i http://localhost:9999/ready
```

Responses:
- During startup:
  - `503 Service Unavailable`
  - Body: `{"ready":false,"status":"starting",...}`
- Once ready:
  - `200 OK`
  - Body: `{"ready":true,"status":"ready",...}`

Readiness criteria:
- Set to ready after core services are started on the node.
- Also considered ready if any metrics context exists (useful for worker-only runs).
