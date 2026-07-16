# Configuration

1. [Overview](#1-overview)<br/>
1.1. [Reference Table](#11-reference-table)<br/>
1.2. [Specific Types](#12-specific-types)<br/>
1.2.1. [Time](#121-time)<br/>
1.2.2. [Size](#122-size)<br/>
1.2.3. [Dictionary](#123-dictionary)<br/>
1.2.4. [Expression](#124-expression)<br/>
2. [Aliasing](#2-aliasing)<br/>

## 1. Overview

All the configuration values have the default values which may be seen
in the file [```<SPT_DIR>/config/defaults.yaml```](/src/main/resources/config/defaults.yaml). The file contains
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
| item-naming-radix                              | Integer >= 2 | 36               | The radix for the item ids. May be in the range of 2..36. A correct value is neccessary to pass the content verification in the case of read load.
| item-naming-step                               | Integer | 1                     | The item naming step. Makes sense in case of "serial" naming type. Negative values cause descending order.
| item-naming-type                               | Enum | random                   | Specifies the new items naming order. Has effect only in the case of create load. "serial": the new items are named in a sequential order, "random": the new items are named randomly |
| item-output-file                               | Path | null                     | Specified the target file for the items processed successfully. If null the items info is not saved.
| item-output-path                               | String or Expression | %{date:<br/>format(\"yyyyMMdd.HHmmss.SSS\")<br/>.format(date:from(time:millisSinceEpoch()<br/>)} | The target path. By default the expression will once generate the constant value equal to the timestamp.
| item-type                                      | Enum | data                     | The type of the item to use, the possible values are: "data", "path", "token". In case of filesystem "data" means files and "path" means directories
| load-batch-size                                | Integer >= 1| 4096              | The count of the items/operations processed by a single invocation. For multipart upload (MPU) and other composite writes, cooperative storage drivers derive MPU scheduling limits and apply bounded child-operation backpressure. Setting this to `1` remains a conservative troubleshooting option, but it is no longer required for MPU correctness. |
| load-op-limit-count                            | Integer >= 0 | 0                 | The maximum number of the load operations to execute for a load step. 0 means infinite
| load-op-limit-fail-count                       | Integer >= 0 | 100000            | The maximum number of the failed load operations before the step will be stopped, 0 means no limit
| load-op-limit-fail-rate                        | Boolean | false                  | Stop the step if failures rate is more than success rate and if the flag is set to true
| load-op-limit-rate                             | Float >= 0 | 0                   | The maximum number of the load operations to execute per second (throughput limit). 0 means no rate limit.
| load-op-limit-recycle                          | Integer >= 1 | 1000000           | The load operations and results queues size limit
| load-op-output-duplicates                      | Flag | false                     | Specifies whether to add duplicates to output items list when in recycle mode or only print them once. No duplicates by default |
| load-op-recycle-mode                           | Flag | false                     | Specifies whether to recycle the successfully finished operations multiple times or not
| load-op-recycle-contents-update                | Flag | false                     | Specifies whether to update the contents of the recycled object. Note: usually you just want to have a new object. This is rarely used. E.g. s3 versioning.
| load-op-retry                                  | Flag | false                     | Specifies whether to retry the failed operations or not. **Note:** For multipart uploads, individual part retries (up to 3 per part) happen automatically regardless of this flag. This flag controls whole-operation-level retry. A retried operation is redispatched after a full-jitter exponential backoff (200ms base, 1s cap) rather than immediately. Only transient failures are retried (`FAIL_IO`, `FAIL_TIMEOUT`, `FAIL_UNKNOWN`, `RESP_FAIL_UNKNOWN`, `RESP_FAIL_SVC`) — permanent failures (auth, not-found, client request errors, corruption, out-of-space) are counted as failed immediately regardless of the retry limit. Not supported by every load generator (e.g. mixed-mode workloads); enabling it against one that doesn't support requeueing raises a configuration error at step start rather than silently dropping failures. |
| load-op-retryLimit                             | Integer >= 0 | 10                | Maximum retry attempts per operation when `load-op-retry` is enabled, before the operation is counted as a failure (`CountFail`) instead of retried again. Must be `>= 0`; `0` disables retry even when `load-op-retry` is true. Has no effect when `load-op-retry` is false. |
| load-op-shuffle                                | Flag | false                     | Defines whether to shuffle or not the items got from the item input, what should make the order of the load operations execution randomized
| load-op-type                                   | Enum | create                    | The operation to process the items, may be "create", "update", "read", "delete", "noop" or "list"
| log-level                                      | String | info                     | Global logging verbosity. Accepts standard Log4j levels such as `trace`, `debug`, `info`, `warn`, `error`. |
| load-op-wait-finish                            | Flag | true                      | Specifies whether it should wait until unfinished operations at the end of the step are completed. True by default, if set to false can leave garbage data on a system as no more requests are done once time is out.
| load-service-threads                           | Integer >= 0 | 0                 | The **global** count of the service threads. 0 means automatic value (CPU cores/threads count)
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
| load-op-list-include_versions | Flag     | false   | Switches to `ListObjectVersions`, returning each version as a distinct success. Buckets without versioning return empty results and emit a warning. |
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
