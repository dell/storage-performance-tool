# S3 Tables Benchmarking (Experimental)

SPT supports benchmarking [Amazon S3 Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html) — a managed Apache Iceberg table storage service built on S3. S3 Tables separates a **control plane** (the `s3tables` API for namespace/table lifecycle) from a **data plane** (Parquet files and Iceberg metadata stored as ordinary S3 objects). This separation means three distinct performance dimensions are worth measuring independently:

- **Write throughput** — how fast can concurrent writers commit Iceberg snapshots?
- **Compaction latency** — how long does the storage system take to compact many small files?
- **Catalog discovery latency** — how fast can the catalog serve `GetTable` and `ListTables` at scale?

SPT's `spt run tables` subcommand covers all three via selectable *test vectors*.

> **Experimental:** S3 Tables support is new. It has been validated against [LocalStack Pro](https://localstack.cloud/) and is intended for early feedback. ObjectScale and AWS production targets are supported in principle but have not yet been validated at scale.

---

## Prerequisites

- A running S3 Tables-compatible endpoint (LocalStack Pro, AWS, or ObjectScale)
- The table bucket does not need to exist in advance — the `provision` phase creates it

---

## Quick Start

```bash
# TPS test: 10 concurrent Iceberg writers for 5 minutes
spt run tables \
  --endpoint https://s3tables.us-east-1.amazonaws.com \
  --access-key "$AWS_ACCESS_KEY_ID" \
  --secret-key "$AWS_SECRET_ACCESS_KEY" \
  --table-bucket my-bucket \
  --test-vector tps \
  --concurrent-writers 10 \
  --duration 5m

# Catalog test: seed 100 namespaces × 100 tables, then measure GetTable/ListTables latency
spt run tables \
  --endpoint https://s3tables.us-east-1.amazonaws.com \
  --access-key "$AWS_ACCESS_KEY_ID" \
  --secret-key "$AWS_SECRET_ACCESS_KEY" \
  --table-bucket my-bucket \
  --test-vector catalog \
  --namespace-count 100 \
  --tables-per-ns 100 \
  --read-concurrency 10 \
  --duration 5m
```

---

## Test Vectors

Select with `--test-vector <name>`.

### `tps` (default)

Measures Iceberg snapshot commit throughput under concurrent write load.

**Phases:**
1. **Provision** — creates the table bucket, namespace, and a single table (idempotent; skipped with `--no-provision`)
2. **Write** — `N` concurrent writers each append a Parquet row group and commit an Iceberg snapshot for the configured duration

**What it measures:** snapshot commits per second, commit latency, and HTTP 409 version-conflict retry rate.

**Key flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--concurrent-writers` | `10` | Number of parallel Iceberg writers |
| `--duration` | `5m` | Write phase duration |
| `--commit-freq-ms` | `500` | Milliseconds between commits per writer |
| `--target-file-size` | `64MB` | Target Parquet file size |

---

### `compaction`

Measures the time the storage system takes to compact many small files into fewer large files.

**Phases:**
1. **Provision** — creates the table bucket, namespace, and table
2. **Seed** — ingests a configurable volume of small Parquet files to create a fragmented table
3. **Compaction poll** — triggers `PutTableMaintenanceConfiguration` and polls snapshot file counts until convergence

**What it measures:** compaction duration (trigger → convergence) and file count reduction ratio.

**Key flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--ingest-file-size` | `100KB` | Size of each small seed file |
| `--total-ingest` | `1GB` | Total data to ingest in the seed phase |
| `--target-file-size` | `64MB` | Target file size for compacted output |
| `--compaction-timeout` | `4h` | Maximum time to wait for compaction to complete |

---

### `catalog`

Measures catalog API latency (`GetTable` and `ListTables`) at scale — with many namespaces and tables populated.

**Phases:**
1. **Provision** — creates the table bucket and a root namespace
2. **Seed** — bulk-creates `namespaceCount × tablesPerNs` namespaces and tables
3. **Catalog** — `readConcurrency` workers continuously alternate between `GetTable` and `ListTables` for the configured duration

**What it measures:** catalog ops/sec and mean latency under concurrent load.

**Key flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--namespace-count` | `100` | Namespaces to create in the seed phase |
| `--tables-per-ns` | `100` | Tables per namespace |
| `--read-concurrency` | `10` | Concurrent catalog readers in the benchmark phase |
| `--duration` | `5m` | Catalog benchmark phase duration |

---

## Common Flags

These flags apply to all test vectors:

| Flag | Default | Description |
|------|---------|-------------|
| `--endpoint` / `--endpoints` | *(required)* | S3 Tables API endpoint |
| `--access-key` | *(required)* | AWS access key ID |
| `--secret-key` | *(required)* | AWS secret access key |
| `--table-bucket` | `spt-tables` | Table bucket name |
| `--namespace` | `default` | Root namespace |
| `--table-name` | `spt-bench` | Table name (TPS and compaction vectors) |
| `--no-provision` | `false` | Skip provisioning (reuse existing bucket/namespace/table) |
| `--duration` | `5m` | Benchmark phase duration (TPS and catalog vectors) |
| `--auto-terminate-seconds` | `0` | Hard kill timeout in seconds for unattended/CI runs (0 = unlimited; test scripts default to 3600) |

---

## LocalStack Development Setup

LocalStack Pro is the recommended local target for development and CI.

```bash
# Start LocalStack Pro
docker run --rm -d \
  -p 4566:4566 \
  -e LOCALSTACK_AUTH_TOKEN="$LOCALSTACK_AUTH_TOKEN" \
  localstack/localstack-pro

# Run the catalog smoketest (10×10 tables, 30s benchmark)
cd cli
./tools/testcatalog.sh --smoketest

# Run the full catalog test (100×100 tables, 5m benchmark)
./tools/testcatalog.sh
```

The test scripts at `cli/tools/testtables.sh` and `cli/tools/testcatalog.sh` cover all three vectors and accept environment variable overrides for all key parameters.

---

## How It Works

SPT implements the S3 Tables REST API directly using its custom Netty HTTP engine — no AWS SDK is involved. All control-plane requests use SigV4 with `service=s3tables`. Data-plane writes (Parquet files, Iceberg metadata) use the standard S3 API.

The Iceberg table format is managed internally:
- **Parquet files** are written with a fixed schema (configurable target file size)
- **Iceberg snapshots** use optimistic concurrency via `UpdateTableMetadataLocation` with automatic retry and jitter on HTTP 409 version conflicts
- **Iceberg manifests** use the Apache Avro library for standard-compliant output
