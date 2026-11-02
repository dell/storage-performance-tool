# S3-RDMA Acceleration

SPT supports an optional RDMA (Remote Direct Memory Access) data path for S3 workloads. When enabled, object transfers bypass the kernel networking stack for significantly lower latency and higher throughput on supported hardware.

RDMA acceleration applies to `write` and `read` payloads. The `rdma` driver also supports
standalone DELETE through its inherited Netty HTTP `DeleteObject`/`DeleteObjects` path. DELETE
does not transfer an object payload and does not use the configured RDMA threshold. Selecting the
driver still performs RDMA initialization at startup, however, so an operator needs the documented
device access or must explicitly enable `--rdma-fallback`.

> **Driver startup requirement:** the RDMA data path requires RDMA-capable NICs, an RDMA-capable
> storage target (e.g., Dell ECS), and Linux. Standalone DELETE uses HTTP after startup, but it does
> not bypass driver initialization; use `--rdma-fallback` explicitly when hardware is unavailable.
> See [Requirements](#requirements) for data-path details.

---

## Quick Start

```bash
# RDMA-accelerated write: 16 threads, 1MB objects, 5 minutes
spt run write \
  --endpoints https://ecs.example.com \
  --access-key "$S3_ACCESS_KEY" \
  --secret-key "$S3_SECRET_KEY" \
  --bucket benchmark-test \
  --threads 16 \
  --object-size 1MB \
  --duration 5m \
  --use-rdma

# RDMA-accelerated read
spt run read \
  --endpoints https://ecs.example.com \
  --access-key "$S3_ACCESS_KEY" \
  --secret-key "$S3_SECRET_KEY" \
  --bucket benchmark-test \
  --threads 16 \
  --object-size 1MB \
  --seed-objects 5000 \
  --duration 5m \
  --use-rdma

# RDMA read from a saved item list (skips seed phase)
spt run read \
  --endpoints https://ecs.example.com \
  --access-key "$S3_ACCESS_KEY" \
  --secret-key "$S3_SECRET_KEY" \
  --bucket benchmark-test \
  --threads 64 \
  --object-size 1MB \
  --duration 5m \
  --items-file ./results/w-1mb-*/w-1mb-*.items.csv \
  --use-rdma
```

---

## CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--use-rdma` | `false` | Enable the RDMA-accelerated S3 driver |
| `--rdma-local-ip` | `""` | Local RDMA interface IP address. When unset and `--rdma-device` is `auto`, each worker uses the local address it routes toward the first S3 endpoint; leave it unset for multi-host runs |
| `--rdma-threshold` | `1MB` | Minimum object size for RDMA transfer (e.g., `0`, `256KB`, `4MB`) |
| `--rdma-fallback` | `false` | Use HTTP when RDMA initialization or per-operation buffer preparation fails, and count GET bodies returned by a server that declines RDMA. When disabled, those operations fail |
| `--rdma-device` | `auto` | RDMA device name or `auto` for auto-detection. A named device disables automatic local address selection |
| `--rdma-log-level` | `WARN` | RDMA native library log level |
| `--rdma-timeout-ms` | `30000` | RDMA operation timeout in milliseconds |
| `--rdma-allow-missing-bytes-header` | `false` | Legacy servers only: accept an RDMA GET success without `x-amz-rdma-bytes-transferred` and count the requested size. By default such a response fails as corrupt |
| `--rdma-buffer-pool` | `true` | Reuse registered RDMA buffers across operations. `false` allocates and registers a buffer for every operation (the previous behavior) |
| `--rdma-pool-shortfall` | `fail` | With a NIC set, what a short pool does: `fail` stops the run, `warn` runs on with per-operation buffers (env: `RDMA_POOL_SHORTFALL`) |
| `--rdma-copy-threads` | `1` | Threads that copy one PUT payload into its RDMA buffer, per NIC with a NIC set. `1` copies on the driver's dispatcher thread alone. A higher value splits each payload of 2 MiB or more across that many threads; each worker then keeps that many threads less one for the copy |

**Environment variable overrides:** `SPT_RDMA`, `RDMA_LOCAL_IP`, `RDMA_DEVICE`, `RDMA_LOG_LEVEL`, `RDMA_THRESHOLD_BYTES`, `RDMA_TIMEOUT_MS`, `RDMA_FALLBACK_ENABLED`, `RDMA_ALLOW_MISSING_BYTES_HEADER`, `RDMA_BUFFER_POOL`, `RDMA_COPY_THREADS`

---

## Threshold-Based Routing

Not all objects benefit from RDMA. Small objects have higher per-operation overhead from memory registration and token generation, while large objects amortize this cost easily.

The `--rdma-threshold` flag controls the cutoff:

- Objects **at or above** the threshold are transferred via RDMA.
- Objects **below** the threshold use standard HTTP.
- Set to `0` to force all objects through RDMA.

The default of `1MB` is a good starting point.

Some operations are never proposed for RDMA and use HTTP regardless of size: ranged reads (fixed or random byte ranges), multipart initiate/complete requests (their upload parts do use RDMA), and objects larger than one RDMA buffer (2 GiB − 1).

---

## Transfer Validation and Data Path Summary

Selecting the RDMA driver does not by itself prove that payloads moved over RDMA. SPT checks the server's answer to every RDMA-proposed request:

| Server response | Result |
|-----------------|--------|
| `x-amz-rdma-reply: 200` (or 204/206) with an empty HTTP body | Transfer counted. GET bytes come from `x-amz-rdma-bytes-transferred`, which must equal the requested size |
| `x-amz-rdma-reply: 501`, or no reply header, on a PUT | The server declined RDMA and stored nothing: the operation fails |
| `x-amz-rdma-reply: 501`, or no reply header, on a GET | The object arrived in the HTTP body: counted once as an HTTP read with `--rdma-fallback`, otherwise the operation fails |
| Reply 200 with an HTTP body, a byte-count mismatch, or a GET without `x-amz-rdma-bytes-transferred` | Contract violation: the operation fails as corrupt (see `--rdma-allow-missing-bytes-header` for legacy servers) |

At the end of each step the engine logs one `RDMA data path summary` line in `messages.log` with counts of operations transferred over RDMA, declined by the server, failed, timed out, and sent over HTTP (RDMA unavailable, below threshold, ineligible, oversize, or preparation fallback). Check it before trusting RDMA results.

The summary also reports the buffer pool. The counters are:

- `poolHits`: operations that reused a registered buffer.
- `poolCreated`: buffers allocated and registered.
- `poolExhausted`: operations that found their size class or the pool's memory budget fully in use and used a per-operation buffer.
- `poolUnpooled`: operations larger than 1 GiB, which always use a per-operation buffer.
- `poolEvicted`: idle buffers deregistered to make room for another size, or to free direct memory for a per-operation buffer.
- `poolDiscarded`: buffers deregistered because their request ended without the server's final response (timeout, lost connection, informational-only reply, or shutdown) or because a GET that integrity verification checks did not deliver verified content.
- `poolInvalidReturns`: must be 0.

A buffer is reused only after the server's final response to its request, when the server can no longer access it. All pooled buffers together stay within half of the JVM's direct-memory limit (`-XX:MaxDirectMemorySize`). If a per-operation buffer cannot be allocated because direct memory is exhausted, the idle pooled buffers are released and the allocation is retried once. If it still fails, the operation fails, or uses HTTP with `--rdma-fallback`. A PUT whose payload cannot be copied into its buffer because memory is exhausted is handled the same way.

---

## Driver Statistics

The `s3-rdma` driver reports the same counts, and the levels of its buffer pool, as *driver statistics*: in the engine's metrics JSON while a step runs, and in the run summary and `spt_run_params.json` when the run has finished.

Two terms are used below. A *request sent* is a request the driver handed to its connection layer. An *outcome* is how a request sent over RDMA ended. Every statistic counts requests or events, never operations and never data: an operation that is retried, and each part of a multipart upload, counts once per request.

### Run summary

For every step in which the driver sent payload requests or met a problem, the run summary shows the counts:

```
RDMA Data Path — Read
  • Requests           52113 transfers reported over RDMA, 14 payload requests sent over HTTP
  • Problem            3 RDMA requests timed out
```

- *Transfers reported over RDMA* is `rdma_transferred`.
- *Payload requests sent over HTTP* is the sum of the five `http_*` counters and `rdma_declined_served_over_http`.
- A `Problem` line appears for each of these that is not zero: `rdma_declined_failed`, `rdma_request_failed`, `rdma_protocol_error`, `rdma_timed_out`, `prepare_failed`, the registration, allocation, and token failures, and the requests sent over RDMA that had no outcome when the step ended.
- A mixed step has one such block. The driver counts the step's payload operations together, so the block is labelled `all payload operations of the step` and is not divided between READ and CREATE.
- A step that sends no payload, such as a DELETE or a list step, has no block.

The summary gives counts and no percentage. The counts do not say how much data moved over either path.

Two warnings can appear in the summary's warnings:

- `<step>: the S3-RDMA driver ran and no payload moved over RDMA`. The step has a block as above and its transfers reported over RDMA are 0.
- `S3-RDMA driver statistics are not available: <reason>`. The run selected `--s3-driver rdma` and the CLI stored no statistics. The reason is the check that failed, or that the engine reported none, as an engine older than this feature does. The warning needs the driver selection and the reason that are stored with the run: results of a scenario the CLI did not generate, and results stored by an earlier CLI release, show no such warning.

### Statistic names

Counters only grow during a step. The names are part of the JSON and of the stored results, and keep their meaning.

| Counter | Counts | Not to be read as |
|---------|--------|-------------------|
| `rdma_requests_sent` | Payload requests sent with an RDMA proposal | Operations, or transfers: see the outcomes |
| `rdma_transferred` | Outcome: the server reported that the payload moved over RDMA | Successful operations. A READ can still fail integrity verification after its transfer |
| `rdma_bytes_assumed` | The part of `rdma_transferred` whose reply gave no byte count, so the requested size was counted (`--rdma-allow-missing-bytes-header`) | A number of bytes |
| `rdma_declined_served_over_http` | Outcome: a GET the server declined or ignored for RDMA and answered with the object in the HTTP body (`--rdma-fallback`) | A failure. The read is counted once, as an HTTP read |
| `rdma_declined_failed` | Outcome: a PUT the server declined, or a declined GET without `--rdma-fallback`. The operation failed | |
| `rdma_request_failed` | Outcome: the request ended without success, for example with an HTTP error status or a lost connection | Only HTTP error statuses |
| `rdma_protocol_error` | Outcome: the reply broke the RDMA reply contract. The operation failed as corrupt | |
| `rdma_timed_out` | Outcome: the client ended the request after its timeout | |
| `http_rdma_unavailable` | Payload requests sent over HTTP because RDMA is disabled or did not initialize | |
| `http_below_threshold` | Payload requests sent over HTTP because the size is below `--rdma-threshold` | A problem. This is the configured routing |
| `http_ineligible` | Payload requests sent over HTTP because the read is ranged or partial | A problem |
| `http_oversize` | Payload requests sent over HTTP because the object is larger than one RDMA buffer | |
| `http_fallback` | Payload requests sent over HTTP because RDMA preparation failed (`--rdma-fallback`) | Server declines, which are `rdma_declined_served_over_http` |
| `prepare_failed` | Attempts that failed before any request because RDMA preparation failed without `--rdma-fallback` | Requests sent |
| `registrations_pooled` | Pool buffers allocated and registered (`poolCreated` in the log line) | Requests. One buffer serves many |
| `registrations_on_demand` | Per-operation buffers registered: the pool is disabled or gave none | |
| `registration_failed_pooled`, `registration_failed_on_demand` | The RDMA device refused to register a buffer | Allocation failures: the remedy is the locked-memory limit or the device |
| `allocation_failed_pooled`, `allocation_failed_on_demand` | JVM direct memory for a buffer could not be allocated. A per-operation buffer is counted when the retry after releasing idle pool buffers also failed | Registration failures: the remedy is `-XX:MaxDirectMemorySize` |
| `token_generation_failed` | The transport gave no token for a registered buffer | |
| `pool_hits` | Requests that reused a registered buffer | |
| `pool_exhausted` | Requests that found their size class or the pool's budget in use and used a per-operation buffer | A failure |
| `pool_oversize` | Requests larger than 1 GiB, which never use the pool (`poolUnpooled` in the log line) | `http_oversize` |
| `pool_evicted` | Idle buffers deregistered to make room | |
| `pool_discarded` | Buffers deregistered because their request ended without the server's final response, or a verified GET did not verify | |
| `pool_invalid_returns` | Must be 0 | |

`rdma_requests_sent` equals the sum of the six outcomes plus the requests that have no outcome yet.

Gauges are levels at the moment they are read.

| Gauge | Value |
|-------|-------|
| `drivers` | 1 for each driver, so a fleet total is the number of drivers in the sum |
| `transport_available` | 1 while the driver's RDMA transport is initialized, otherwise 0 |
| `pool_reserved_bytes` | Capacity the pool has reserved, including a buffer still being created |
| `pool_budget_bytes` | The most the pool may reserve |
| `pool_buffers` | Buffers the pool holds, in use or idle |
| `pool_idle_buffers` | Pool buffers no request holds |
| `pool_peak_reserved_bytes` | The largest `pool_reserved_bytes` since the driver started |

Every name is always reported. With `--rdma-buffer-pool=false` the pool statistics are 0.

### Reading the metrics JSON

`/metrics/json` on a node returns that node's rows, and `/metrics/fleet/json` on the entry node returns the rows of the whole run. A row of a step whose driver reports statistics carries them in `driver_stats`. A mixed step has several rows, one per operation; one of them carries the object, and it covers the whole step. After the step ends the engine keeps one row for it, and that row carries the object.

```json
"driver_stats": {
  "driver": "s3-rdma",
  "counters": { "rdma_requests_sent": 52116, "rdma_transferred": 52113, "rdma_timed_out": 3 },
  "gauges": { "drivers": 1, "transport_available": 1, "pool_buffers": 64 }
}
```

A fleet row sums the statistics of the nodes and adds two fields:

- `reporting`: the number of nodes whose statistics are in the sum.
- `partial`: `true` unless every expected node is present and reported statistics with the same driver and the same names. When nodes report different drivers the object has no sums: it carries `partial: true` and `drivers`, the names seen.

When reading these rows:

- A fleet gauge is a sum of the node gauges, not a value the fleet held at one moment. In particular a fleet `pool_peak_reserved_bytes` is the sum of each node's peak, and the nodes may have reached their peaks at different times.
- A fleet `transport_available` below `drivers` means some nodes run without RDMA. Their node rows show which, and count their payload requests as `http_rdma_unavailable`.
- While a step runs, the counters are read one after another and requests complete in between. `rdma_requests_sent` can therefore differ from the sum of its outcomes by the requests in flight, and briefly by a few more in either direction. Compare counters exactly only on a finished step.
- To find the node behind a fleet problem count, read `/metrics/json` on each node: a node row has the same names and no `reporting` or `partial`.
- A weighted or pipeline step, which a scenario run with the engine directly can use, has one driver for each of its operations. While it runs, each of its rows carries the statistics of its own driver. The one row kept after it ends carries the statistics of one of these drivers, not their sum.
- The engine keeps the row of a finished step for a short time after the step ends (currently 20 seconds) and then drops it. In a run of several steps, the rows of the earlier steps are gone before the run ends. The stored values below keep the final statistics of every step.

### On the Prometheus endpoint

`/metrics` on the entry node exports the fleet total of each running step: every counter as a Prometheus counter `spt_driver_<name>_total` and every gauge as `spt_driver_<name>`, with `spt_driver_reporting`, `spt_driver_partial` and a `driver` label. The counters and gauges are left out while the total is partial, a counter never falls below a value it showed, and the series disappear when the step ends, so use the stored values below for a step's final statistics. The engine's [metrics guide](../../engine/core/spt-base/doc/usage/api/remote/METRICS.md#storage-driver-statistics) has the details.

### Stored values

When a run finishes, the CLI stores each step's final statistics in `spt_run_params.json` under `driverStats`, keyed by step ID, as reported: names the CLI does not know are kept. The stored values are those of the fleet row, which a single-host run has as well, as a total of one node. The CLI reads them while it monitors the run, as each step ends, so the steps that end early are stored as well.

The final values are taken when the step ends, after its requests have completed and the driver has stopped, and before the driver closes. The `RDMA data path summary` log line is written at close. The two agree on every count, with two exceptions: `poolDiscarded` in the log line also counts the buffers released at close, and a request that was still without an outcome when the step ended is counted by the log line if it ends before the driver closes. The stored pool gauges describe the pool as the step left it, not the empty pool after close.

The CLI stores statistics only when they are complete. For each step the row is the final one of this run and step, every expected node contributed, and `partial` is `false`. No step may be missing either: a step whose rows showed its driver at work (a counter above zero) must have its final statistics read, and so must a step of a run with `--s3-driver rdma` whose rows counted completed or failed operations, since the `s3-rdma` driver always reports statistics. The CLI must also not have gone without reading the engine's metrics for as long as the engine keeps a finished step's row, since a step could have ended and been dropped unseen. That retention is the engine's `api.linger.sec`, 20 seconds by default; the CLI does not change it and assumes the default. Otherwise it stores none, records the reason in `driverStatsError`, and logs one warning. This does not fail the run. A step that completed no operations, or in which a driver other than `s3-rdma` counted nothing, can be absent from the stored values. A run of `--s3-driver rdma` in which no step reported statistics records that the engine reported none.

---

## Infrastructure Verification

Use `spt verify --use-rdma` to pre-check RDMA readiness on your test nodes:

```bash
spt verify --test-hosts "rdma1,rdma2" --use-rdma
```

This adds RDMA-specific checks on top of the standard Docker/port verification: hardware presence, device accessibility, and driver availability.

---

## Requirements

- **OS:** Linux only
- **NICs:** RDMA-capable NICs — NVIDIA/Mellanox ConnectX-4 or newer. Bonded interfaces (`mlx5_bond_0`) are supported.
- **Storage target:** An RDMA-capable S3 endpoint (e.g., Dell ECS with RDMA enabled)
- **System packages:** `rdma-core` (provides `libibverbs`, `librdmacm`, `libmlx5`)
- **Docker:** Device passthrough for RDMA hardware (`--device /dev/infiniband`)

If RDMA hardware is not available at runtime, the driver fails by default. Set `--rdma-fallback` to fall back to HTTP instead.

---

## How It Works

SPT implements S3-RDMA by extending the standard S3 storage driver. The `S3RdmaStorageDriver` overrides only the data transfer path — all S3 authentication, signing, and metadata operations continue to use the existing Netty HTTP engine.

### Client role in S3-RDMA

The SPT client does **not** perform RDMA data transfers directly. The **storage server** initiates the transfer:

| Operation | What happens |
|-----------|-------------|
| **PUT** (write) | Client registers a memory buffer, generates an RDMA token, and sends it as an `x-amz-rdma-token` HTTP header. The server performs an RDMA READ from the client's buffer. |
| **GET** (read) | Same token flow. The server performs an RDMA WRITE into the client's buffer. |

This means the client-side implementation is lightweight: register memory, generate a token, send the HTTP request, and wait for the server to complete the transfer.

### Architecture

```
SPT Engine (Java)
├── S3RdmaStorageDriver    — routing: RDMA vs HTTP based on threshold
├── RdmaTransport          — JNI bridge + memory registration lifecycle
└── libspt_rdma.so         — ~675 lines of C using libibverbs/librdmacm
    └── rdma-core           — system packages (libibverbs, libmlx5, librdmacm)
```

The native layer uses Mellanox DC (Dynamically Connected) transport for scalable connections and RoCE v2 for Ethernet-based RDMA. By default, registered buffers are reused across operations (`--rdma-buffer-pool`). A PUT payload is still copied into its buffer on the driver's dispatch path; for large objects and high request rates, this copy can limit the PUT throughput of a single driver. `--rdma-copy-threads` spreads the copy of one payload over several threads.

---

## NIC sets

`spt run --s3-driver rdma --interfaces auto`, or a list of NICs, gives one engine several client NICs, as it does for the `s3` driver ([Network Interfaces](NETWORK_INTERFACES.md)). The engine setting is `storage.net.interfaces`. `--host-interfaces` gives a host its own value. An `--rdma-device` other than `auto` and an `--rdma-local-ip` are rejected, from a flag, the environment or `cli/.env`: each NIC brings its own device and address. Without a NIC set, the driver is as before.

- **One RDMA context per NIC.** Each NIC opens exactly its own `mlx5` device, on port 1, at the RoCE v2 GID of its address, with no fallback to another device or GID. A context that does not open fails the step, whatever `storage.rdma.fallback` says. `storage.rdma.device` other than `auto` and a non-empty `storage.rdma.localIp` are rejected.
- **One NIC per attempt.** An attempt keeps its NIC until it ends: its token names that NIC's DC target, and all of its requests use that NIC's Paths, payloads sent over HTTP included. A retry chooses again. Control requests keep the kernel's route.
- **One buffer pool per NIC.** Each pool takes an equal share of the driver's budget, half of `MaxDirectMemorySize` (or of the heap limit). Per size class it holds at most the NIC's share of the concurrency limit plus one buffer, the limit plus one with fewer threads than NICs, or 256 with no limit. It reuses buffers by the same rules as without a NIC set.
- **One preparation thread per NIC.** It allocates and registers buffers, copies PUT payloads and makes tokens, so a slow NIC delays only itself. `--rdma-copy-threads` and `storage.rdma.copyThreads` count per NIC here, each NIC's copier having its own threads. Waiting operations per NIC are bounded: its share of the concurrency limit, one with fewer threads than NICs, or 8 with no limit.
- **Stopping.** An operation still waiting for its preparation or its connection writes nothing, gives back its buffer and its NIC, and is reported as unattempted. At close, the driver waits at most 5 s in all for the preparation threads. One still in a call that cannot be interrupted, such as an allocation or a registration, logs a warning naming the NIC and the call, and its NIC's pool and context close when the call returns.
- **Logs.** `RDMA data path summary` is written once per NIC, as `<step>: nic=<name>: RDMA data path summary ...`, and then once for the driver, in its usual form, which is the line to read for the whole step. With the message log at DEBUG, each pool buffer's address and capacity are logged when it is registered, for checking where its pages landed.

- **Pool demand.** Before any object operation, each engine works out what each NIC's pool can hold at once: the operations that would use RDMA (CREATE and whole-object READ at or above the threshold), their transfer sizes (the part size for a multipart upload; a transfer above 1 GiB adds nothing) and each NIC's share of `--threads`, lowered by the multipart limits. With `--part-size`, a read of an object above the part size goes as HTTP range requests, so it adds nothing. A size range counts its largest class, and only its sizes at or above the threshold. An item file's sizes come from the file: the entry node records each worker's sizes as it writes the worker's slice. A malformed item, a read or write failure, or a read past its budget (a quarter of `load.step.preparation.timeout`) leaves that operation unknown, as do a bucket listing and unlimited concurrency, without failing the step.
- **The two checks.** Each NIC's pool must fit its share of half of `MaxDirectMemorySize`, and the pools of the drivers that run at once, plus 8 GiB of headroom for Netty and per-operation buffers, must fit in `MaxDirectMemorySize`. Both are reported apart. An unknown part never hides a known one: the checks cover what is known and warn that they are partial, or say they cannot check when nothing is known.
- **A short pool.** `--rdma-pool-shortfall` (`RDMA_POOL_SHORTFALL`, engine `storage.rdma.poolShortfall`) decides. Under `fail`, the default, the step fails before any object operation, naming the host, the NIC, both amounts and the setting that fits. Under `warn`, it warns once and goes on: the pool holds what fits, and the rest use per-operation buffers or fail their preparation, as without a NIC set.
- **Sizing `MaxDirectMemorySize`.** When `SPT_JAVA_OPTS` does not set `-XX:MaxDirectMemorySize`, `spt run` sets it on each engine it launches, to `max(64g, min(need, capacity))`, and prints it. The need comes from the run's steps on each host's own NIC plan, which the image's planner works out on that host before anything starts, an item file read on the CLI's host included; the largest step sets it. Capacity is the smaller of the host's `MemTotal` and the `memory.max` of the engine's cgroup, ancestors included, less the heap and a 4 GiB reserve. The 64g floor is never lowered. A need above capacity follows the shortfall policy: under `fail` the run stops before launch with both amounts, and under `warn` it launches with `max(64g, capacity)` and warns. With part of the demand unknown, it sizes from what is known, and names the rest; with nothing known, or no demand, the shipped default stays. A value in `SPT_JAVA_OPTS` wins, unchanged, and the engine's check applies to it. Workers under `--attach-existing` are not sized: the CLI says so, and each engine checks its own limit. `spt_run_params.json` records each engine's explicit, calculated and passed values, its inputs and its capacity source under `poolSizing`, and each step's effective value in `nicEngines`. `spt replay` does not launch `s3-rdma`, so it sizes nothing.
- **Pre-warm.** Each NIC's preparation thread registers its minimum in the class of its known demand, on every NIC at once, before the load interval, so the load starts with warm pools and pre-warm time counts in no rate. A size range or item sizes spanning classes, unknown demand and fewer threads than NICs skip it, with a line saying the load starts with cold pools. A registration or allocation failure is a short pool: it fails the step under `fail`, and keeps what was registered under `warn`. A stop or a preparation timeout ends it as any other preparation, and releases its buffers.
- **What each NIC reports.** One step-start line per NIC gives its demand, minimum, cap, headroom and budget, and its pre-warm time and buffer count or why it was skipped; one line per step gives the two checks and the limit the step needs. A difference from the CLI's estimate is logged as `pool demand discrepancy`, and the engine's figure decides. `nic.set.json` has them all under `pool`.

`spt verify --use-rdma` previews this for a host: see [`spt verify`](SPT_SYNTAX.md#rdma-pools).

The per-path CSV's RDMA columns are not in this version yet.

---

## Troubleshooting

### RDMA not available

If SPT reports that RDMA is not available:
1. Verify RDMA hardware: `ibv_devinfo` should list your device
2. Check `rdma-core` packages are installed
3. Ensure `/dev/infiniband` is accessible inside the Docker container
4. Run `spt verify --use-rdma` to diagnose

### Fallback to HTTP

If `--rdma-fallback` is set and RDMA initialization fails, all operations use HTTP; check the engine log for `RDMA unavailable, falling back to HTTP`. The `RDMA data path summary` line at the end of each step then reports `transport unavailable` and counts those operations as `httpRdmaUnavailable`. Per-operation fallbacks and server declines are counted in the same line. The run summary shows these counts under `RDMA Data Path` and warns when a step moved no payload over RDMA; see [Driver Statistics](#driver-statistics).

### Performance lower than expected

- If you set `--rdma-local-ip`, ensure it is the address of your RDMA interface (not a management NIC); in multi-host runs leave it unset so each worker selects its own
- Check that object sizes are above `--rdma-threshold`
- In a multi-host run against several storage nodes, try one node per worker: `--slice-endpoints` (engine YAML `storage.net.node.slice: true`) divides the endpoint list among the workers instead of connecting every worker to every node. In the only RDMA environment available for testing (four workers, four nodes), this gave equal or higher RDMA throughput, most clearly for GET. The effect may be specific to that environment, so compare both layouts
- For PUT of large objects, try more copy threads: `--rdma-copy-threads 4` (engine YAML `storage.rdma.copyThreads: 4`). In the same environment, with one node per worker, four threads gave about 7–11% more PUT throughput with 128 MiB objects, a smaller and less certain gain (about 3–5%) with 1 GiB objects, and no measurable gain with 4 MiB objects, for roughly a quarter to a half more client CPU. GET does not use this copy. The effect may be specific to that environment, so compare both settings
- With large objects and many threads, give the buffer pool room: it holds at most half of the engine's direct-memory limit (`-XX:MaxDirectMemorySize`, 64g in the engine image), and an operation that finds no pooled buffer allocates, zero-fills, and registers its own. For 1 GiB objects, set `SPT_JAVA_OPTS=-XX:MaxDirectMemorySize=<2 × (threads + 1)>g` in the CLI's environment; the RDMA data path summary reports `poolExhausted` when the pool ran out
- Each pooled buffer is allocated and registered the first time an operation needs it, so with large objects a run takes a while to reach `--threads` operations in flight; with 1 GiB objects and 96 threads it took about 50 seconds. Measure such runs over a few minutes, or leave out their start
- On a client with several RDMA NICs, run one `spt run` per NIC: one driver uses one device. Give each instance its own `--rdma-device` and `--rdma-local-ip`, its own `--api-port` and `--rmi-registry-port`, and, when there are several storage nodes, its own node in `--endpoints`, so that no NIC receives from every node at once. In the only such environment available for testing (one client with four 400 GbE ports, four storage nodes, 1 GiB objects), four instances read about four times as fast as one instance on one port receiving from all four nodes, without that layout's long GET tails; PUT gained less, as the storage nodes became the limit
- Verify RoCE v2 is properly configured on the network (PFC/ECN flow control)
- Use `--rdma-log-level DEBUG` for detailed native-layer diagnostics
