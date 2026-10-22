# Target Buckets

Before each phase of an S3 workload, SPT checks the phase's target buckets. A missing bucket is created only for the phase that writes the run's objects, and only when target-bucket provisioning is on. Provisioning is on by default.

This applies to the `s3` (Netty), `s3-rdma` and `s3-aws` drivers. The `tables` workload creates its table bucket in its own provision phase (see [S3_TABLES.md](S3_TABLES.md)). The `mock` workload has no buckets.

## Options

| Flag | Environment | Engine setting | Default |
|------|-------------|----------------|---------|
| `--provision-target-bucket` | | `storage.targetBucket.provision` | `true` |
| `--region` | `S3_REGION` | `storage.region` | empty: the region of a standard AWS endpoint, else `us-east-1` |

- `--provision-target-bucket=false` still checks that every target bucket exists and is accessible, and fails the run before any object operation when one is not. It only forbids creating a bucket.
- An explicit `--region` wins over `S3_REGION`.
- An engine override, for example `--engine-override storage.targetBucket.provision=false`, wins over both flags.

## What each phase does

| Workload | Creates a missing bucket in | Checks only |
|----------|-----------------------------|-------------|
| `write`, `write --cleanup` | the write phase | cleanup |
| `write-verify` | the write phase | verification, cleanup |
| `read` (seeded) | the seed phase | read, cleanup |
| `read --items-file` | no phase | read, cleanup |
| `read-verify` | no phase | listing, verification |
| `list` | no phase | listing |
| `delete` (seeded) | the seed phase | delete, cleanup |
| `delete --items-file`, `delete --delete-existing` | no phase | discovery, delete |
| `mixed` (seeded) | the seed phase | mixed, cleanup |
| `mixed` with `--read-items-file` | no phase | mixed, cleanup |

A phase that only checks needs its buckets to exist already. For `read --items-file` and manifest DELETE, SPT checks every bucket the items file names.

## Safety

- Provisioning creates a bucket only after the target answers that it does not exist (HTTP 404). An authorization failure (401, 403), a redirect, a timeout, a connection failure or a server error stops the run and never leads to a create request.
- An existing bucket is used as it is. SPT never clears it, deletes it, or changes its ACLs, policy, encryption, tags, lifecycle, object lock or versioning. Cleanup phases delete only objects.
- When two runs create the same bucket at once, the run whose create request fails checks the bucket once more. It continues only if the bucket is now accessible.

## Distributed runs

The engine on the entry node checks and creates the buckets once per phase, before any node starts the phase, using the entry node's network path and credentials. Worker nodes never create a bucket.

## Bucket names

The bucket must be a literal name. A per-operation expression such as `${...}` or `#{...}` in the bucket segment of an engine output path fails the run with a configuration error. Dynamic prefixes and object keys after the bucket remain supported, for example `/bench/${...}`.

An initialization expression `%{...}` in the bucket segment is evaluated once per phase on the entry node, so every node uses the same bucket. This includes the engine's own default output path, a timestamped bucket used when a custom scenario sets none.

## Region

SPT signs requests for, and creates buckets in, the effective region:

1. a final engine override of `storage.region`
2. `--region`
3. `S3_REGION`
4. the region of a standard AWS endpoint: `s3.amazonaws.com` is `us-east-1`, `s3.<region>.amazonaws.com` is `<region>`
5. `us-east-1`

An explicit region that differs from the endpoint's region wins, and SPT logs one warning. A bucket created on a standard AWS endpoint outside `us-east-1` gets the matching location constraint. Every other endpoint, including S3-compatible services on any host, gets a CreateBucket request without a body.

Virtual-hosted, dual-stack, FIPS, VPC and access-point endpoints are not recognized as AWS endpoints.

## Messages

The engine log `messages.log` reports each bucket once per run at INFO, for example:

```text
mt-001-20261022.120000.000-create: created target bucket "bench"
mt-002-20261022.120000.000-read: using existing target bucket "bench"
```

Later phases that check the same bucket log at DEBUG.

A failure ends the run with exit code 1 and names the phase and the cause, for example:

```text
step mt-001-20261022.120000.000-create failed (execution): target bucket "bench" is not accessible (HTTP 403)
step mt-001-20261022.120000.000-create failed (execution): target bucket "bench" does not exist and target-bucket provisioning is disabled (--provision-target-bucket=false, storage.targetBucket.provision: false)
step mt-001-20261022.120000.000-read failed (execution): target bucket "bench" does not exist; this step uses existing objects only and never creates a bucket
```

Failure messages name only the bucket and the kind of failure. They never include credentials, request headers, query strings, response bodies or endpoint addresses.

## Changes from 5.16.1

- Before 5.16.2, a worker created a missing bucket on the first object it wrote. The Netty and RDMA drivers also treated any failed bucket check, an authorization failure included, as a missing bucket. Bucket creation now happens once, on the entry node, before the phase starts, and only after a not-found answer.
- The `s3-aws` driver used `eu-west-2` when no region was set. It now uses the same order as the other drivers, ending in `us-east-1`.
- `--create-prefix` never had an effect. It is deprecated and will be removed in the next minor release; remove it from scripts.
- Per-operation expressions in the bucket segment of a custom scenario's output path are rejected. Split such a workload into steps with one static bucket each.
