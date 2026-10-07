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

**Environment variable overrides:** `SPT_RDMA`, `RDMA_LOCAL_IP`, `RDMA_DEVICE`, `RDMA_LOG_LEVEL`, `RDMA_THRESHOLD_BYTES`, `RDMA_TIMEOUT_MS`, `RDMA_FALLBACK_ENABLED`, `RDMA_ALLOW_MISSING_BYTES_HEADER`, `RDMA_BUFFER_POOL`

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

At the end of each step the engine logs one `RDMA data path summary` line in `messages.log` with counts of operations transferred over RDMA, declined by the server, failed, timed out, and sent over HTTP (below threshold, ineligible, oversize, or preparation fallback). Check it before trusting RDMA results.

The summary also reports the buffer pool: `poolHits` operations reused a registered buffer, `poolCreated` buffers were allocated and registered, `poolExhausted` operations found every buffer of their size in use and registered their own, and `poolDiscarded` buffers were deregistered because their request ended without a server response (timeout, lost connection, or shutdown). A buffer is reused only after the server answered its request, so the server can no longer access it.

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

The native layer uses Mellanox DC (Dynamically Connected) transport for scalable connections and RoCE v2 for Ethernet-based RDMA. Each RDMA operation currently allocates and registers its own buffer on the driver's dispatch path; for large objects and high request rates, this per-operation preparation can limit the throughput of a single driver.

---

## Troubleshooting

### RDMA not available

If SPT reports that RDMA is not available:
1. Verify RDMA hardware: `ibv_devinfo` should list your device
2. Check `rdma-core` packages are installed
3. Ensure `/dev/infiniband` is accessible inside the Docker container
4. Run `spt verify --use-rdma` to diagnose

### Fallback to HTTP

If `--rdma-fallback` is set and RDMA initialization fails, all operations use HTTP; check the engine log for `RDMA unavailable, falling back to HTTP`. Per-operation fallbacks and server declines are counted in the `RDMA data path summary` line at the end of each step.

### Performance lower than expected

- If you set `--rdma-local-ip`, ensure it is the address of your RDMA interface (not a management NIC); in multi-host runs leave it unset so each worker selects its own
- Check that object sizes are above `--rdma-threshold`
- Verify RoCE v2 is properly configured on the network (PFC/ECN flow control)
- Use `--rdma-log-level DEBUG` for detailed native-layer diagnostics
