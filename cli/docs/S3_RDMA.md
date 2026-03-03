# S3-RDMA Acceleration

SPT supports an optional RDMA (Remote Direct Memory Access) data path for S3 workloads. When enabled, object transfers bypass the kernel networking stack for significantly lower latency and higher throughput on supported hardware.

RDMA acceleration applies to `write` and `read` workloads. Objects below the configured threshold are transparently sent over standard HTTP.

> **Hardware required:** S3-RDMA requires RDMA-capable NICs, an RDMA-capable storage target (e.g., Dell ECS), and Linux. See [Requirements](#requirements) for details.

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
  --use-rdma \
  --rdma-local-ip 10.247.128.125

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
  --use-rdma \
  --rdma-local-ip 10.247.128.125
```

---

## CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--use-rdma` | `false` | Enable the RDMA-accelerated S3 driver |
| `--rdma-local-ip` | `""` | Local RDMA interface IP address |
| `--rdma-threshold` | `1MB` | Minimum object size for RDMA transfer (e.g., `0`, `256KB`, `4MB`) |
| `--rdma-fallback` | `false` | Fall back to HTTP if RDMA initialization fails |
| `--rdma-device` | `auto` | RDMA device name or `auto` for auto-detection |
| `--rdma-log-level` | `WARN` | RDMA native library log level |
| `--rdma-timeout-ms` | `30000` | RDMA operation timeout in milliseconds |

**Environment variable overrides:** `SPT_RDMA_ENABLED`, `RDMA_LOCAL_IP`, `RDMA_DEVICE`, `RDMA_LOG_LEVEL`, `RDMA_THRESHOLD_BYTES`, `RDMA_TIMEOUT_MS`, `RDMA_FALLBACK_ENABLED`

---

## Threshold-Based Routing

Not all objects benefit from RDMA. Small objects have higher per-operation overhead from memory registration and token generation, while large objects amortize this cost easily.

The `--rdma-threshold` flag controls the cutoff:

- Objects **at or above** the threshold are transferred via RDMA.
- Objects **below** the threshold use standard HTTP.
- Set to `0` to force all objects through RDMA.

The default of `1MB` is a good starting point. At 1MB, RDMA memory registration overhead is approximately 2% of total operation time.

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

The native layer uses Mellanox DC (Dynamically Connected) transport for scalable connections and RoCE v2 for Ethernet-based RDMA. Memory registration is done on-demand per operation — the overhead (~88 microseconds at 1MB) is negligible relative to the transfer time.

---

## Troubleshooting

### RDMA not available

If SPT reports that RDMA is not available:
1. Verify RDMA hardware: `ibv_devinfo` should list your device
2. Check `rdma-core` packages are installed
3. Ensure `/dev/infiniband` is accessible inside the Docker container
4. Run `spt verify --use-rdma` to diagnose

### Fallback to HTTP

If `--rdma-fallback` is set and RDMA initialization fails, all operations silently use HTTP. Check engine logs for `RDMA initialization failed, falling back to HTTP` to confirm.

### Performance lower than expected

- Ensure `--rdma-local-ip` matches your RDMA interface (not a management NIC)
- Check that object sizes are above `--rdma-threshold`
- Verify RoCE v2 is properly configured on the network (PFC/ECN flow control)
- Use `--rdma-log-level DEBUG` for detailed native-layer diagnostics
