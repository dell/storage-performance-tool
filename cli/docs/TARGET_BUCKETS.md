# Target Buckets

Before each phase of an S3 workload, SPT checks the phase's target buckets. A missing bucket is created only for the phase that writes the run's objects, and only when target-bucket provisioning is on. Provisioning is on by default.

This applies to the `s3` (Netty), `s3-rdma` and `s3-aws` drivers. The `tables` workload creates its table bucket in its own provision phase (see [S3_TABLES.md](S3_TABLES.md)). The `mock` workload has no buckets.

## Options

| Flag | Environment | Engine setting | Default |
|------|-------------|----------------|---------|
| `--provision-target-bucket` | | `storage.targetBucket.provision` | `true` |
| `--bucket-metrics` | | `output.metrics.bucket.enabled` | `true` |
| `--bucket-count` | | `storage.targetBucket.count` | `1` |
| `--buckets` | `S3_BUCKETS` | `storage.targetBucket.names` | empty: the set is generated from `--bucket-count` |
| `--buckets-file` | | `storage.targetBucket.names` (the CLI reads the file) | none |
| `--region` | `S3_REGION` | `storage.region` | empty: the region of a standard AWS endpoint, else `us-east-1` |

- `--provision-target-bucket=false` still checks that every target bucket exists and is accessible, and fails the run before any object operation when one is not. It only forbids creating a bucket.
- An explicit `--region` wins over `S3_REGION`.
- An engine override, for example `--engine-override storage.targetBucket.provision=false`, wins over both flags.

## Bucket sets

`--bucket-count N` (1 to 1000) spreads the objects a run creates over `N` buckets named after `--bucket`:

```bash
spt run write --bucket ps --bucket-count 12 --threads 64 --duration 5m
```

To try a set, `cli/tools/testmultibucket.sh [BUCKETS]` runs a short headless write over `BUCKETS` buckets (default 16) with the connection settings from `cli/.env` or its options, then deletes the objects it created and leaves the buckets in place. `--workload` selects `write-verify`, seeded `read`, seeded `delete` or seeded `mixed` instead, `--bucket-count N` is the same as `BUCKETS`, and `--buckets` or `--buckets-file` names the buckets instead of counting them (see [Bucket lists](#bucket-lists)); `--help` lists the options.

- **Names:** `ps-0` to `ps-11`. The index is not padded, so a bucket's name does not depend on the count: `ps-3` is `ps-3` with 4 buckets or with 12. `--bucket` names only the base. With more than one bucket, SPT never uses the base as one of the set; it is contacted only when an items file names objects in it.
- **New objects:** each new object goes to a bucket chosen from its object id. With the default random naming every worker spreads its objects evenly over all the buckets, so all of them receive writes at the same time. The spread is statistical: a short run can be slightly uneven, and requests, bytes or concurrency per bucket are not equal at every instant.
- **Existing objects:** reads, deletes and cleanup address each object in the bucket its items file or manifest records. The count never changes where an existing object is looked for.
- **Prefix shards:** with `--prefix-shards`, every bucket still gets every shard directory.
- **Setup:** the phase that writes the run's objects checks every bucket of the set and creates the missing ones, as for one bucket. Later phases check them. A `mixed` run from `--read-items-file` (and `--delete-items-file`) has no seed phase, so with more than one bucket its own setup creates missing buckets of the set; the buckets its items files name are only checked.
- **Workloads:** `write` (with or without `--cleanup`), `write-verify`, seeded `read`, seeded `delete` and `mixed`. Seeded `delete` needs `--delete-batch-size=1`, because a DELETE request covers one bucket. `read --items-file`, `delete --items-file`, `delete --delete-existing`, `read-verify`, `list`, `tables` and `mock` reject a count above 1.
- **Bucket names:** each generated name must follow the [AWS bucket naming rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html): 3 to 63 lowercase letters, digits, hyphens and periods, no `..`, no reserved prefix or suffix such as `xn--`. With 1000 buckets the base can be at most 59 characters. An invalid name fails the run before anything is sent.
- **Results:** the existing metrics stay totals across the set, and [bucket metrics](#bucket-metrics) add per-bucket files. The run's settings record the bucket count. The console's run parameters show `Bucket Count: N (<bucket>-0 ... <bucket>-<N-1>)`, and the results summary shows `Buckets: N (<bucket>-0 ... <bucket>-<N-1>)`.
- **Storage capacity:** check that the storage system can hold the extra buckets. Some systems give every bucket its own resources: SeaweedFS, for example, allocates volumes per bucket and refuses writes into new buckets once its volume slots run out, while existing buckets stay writable.
- **First requests:** each worker's `s3` or `s3-rdma` driver checks each bucket once, with one HEAD request on its first operation to that bucket, so a run with many buckets spends its first operations on these checks.
- **Engine overrides:** set the count with `--bucket-count`; an `--engine-override` that sets `storage.targetBucket.count`, directly or inside a map such as `storage.targetBucket={count: 4}`, is rejected. An override that replaces the `storage.targetBucket` map, such as `storage.targetBucket={provision: false}`, keeps the `--bucket-count` value. In a custom scenario or direct engine run, `storage.targetBucket.count` applies to a step's newly generated objects; with more than one bucket the engine rejects `storage.auth.file`, copy steps, a pipeline CREATE sub-step after the first (it writes the items the previous sub-step passes on) and expressions after the bucket in `item.output.path`.

### Bucket lists

To use buckets with names of your own, for example buckets a storage administrator configured in a particular way, name them instead of generating them:

```bash
spt run write --buckets ps-eu,ps-us,ps-ap --provision-target-bucket=false --prefix spt-run1/ --threads 64 --duration 5m
spt run write --buckets-file buckets.txt --threads 64 --duration 5m
```

`--buckets` takes names comma-separated or repeated (`--buckets ps-eu --buckets ps-us`). `--buckets-file` reads a regular file with one name per line; blank lines and lines starting with `#` are ignored, so a pipe or process substitution doesn't work. The `S3_BUCKETS` environment variable, in the environment or `.env`, is comma-separated like `--buckets`. A workload that targets a bucket but cannot use a list stops with an error while `S3_BUCKETS` holds two or more names: choose one bucket with `--bucket`, or for `delete --items-file`, whose manifest names its own buckets, clear both variables with `S3_BUCKET= S3_BUCKETS= spt run ...`. `mock` and `tables` ignore the variable. A list holds 1 to 1000 names.

- **Names are used as written and in the given order.** The AWS naming rules of generated names do not apply: a listed name is your statement of what the storage accepts, as `--bucket` is. The exception is creating a bucket with `--s3-driver aws`, described below. SPT rejects only what it cannot use: an empty name, a character that means something in a list, a request path or an expression (`/`, `,`, `%`, `\`, `?`, `#`, `{`, `}`), whitespace (any Unicode space), a control character and a name listed twice (names that differ only in case are different names). A storage system that refuses a name fails the bucket check or create, and the phase stops before any object operation.
- **Placement:** member `i` is the `i`-th name, and new objects are spread over the list exactly as over a generated set. Reordering the list changes where new objects go and nothing else.
- **Creation:** a missing listed bucket is created by the phase that writes the run's objects, as a generated one is. To use only buckets that already exist, add `--provision-target-bucket=false`: every listed bucket is then checked, none is created, and a missing one fails the run before any object operation. Buckets are checked in list order and the first failure stops the phase, so buckets it already created stay.
- **`--s3-driver aws` cannot create a name the AWS SDK's bucket name check rejects,** such as one with uppercase letters or `_`. The SDK refuses the request before sending it, whatever the storage accepts, and the run stops with the SDK's reason. Such a bucket has to exist already: the check finds it and nothing is created, with or without `--provision-target-bucket=false`. The default driver sends the request as written and leaves the decision to the storage.
- **Existing contents:** a listed bucket is used as it is, with whatever it holds. SPT never lists, reads or deletes an object it did not write, except the objects an items file you supply names: reading an existing fill through `mixed --read-items-file` is a supported use. Cleanup deletes only the objects the run recorded.
- **Name collisions:** a new object overwrites an existing one only if a generated key collides with it, which is negligible with the default random names. A distinct `--prefix` rules this out even next to a very large fill, as long as it does not overlap keys the bucket already holds.
- **Credentials:** one access key and secret serve every listed bucket. With two or more names, the engine rejects `storage.auth.file`.
- **Exclusions:** `--buckets` and `--buckets-file` exclude each other, any explicit `--bucket-count` (1 included) and an explicit `--bucket`; each error names both flags. `--bucket NAME --bucket-count N` stays the way to choose a generated set.
- **Environment:** an explicit bucket flag always wins over the environment. `--bucket` or `--bucket-count` ignores `S3_BUCKETS`, and `S3_BUCKET` still supplies a missing `--bucket`. With no bucket flag, `S3_BUCKETS` wins over `S3_BUCKET`, and SPT prints `Using S3_BUCKETS for the target buckets; S3_BUCKET is ignored` when both are set.
- **One name:** `--buckets NAME` is exactly `--bucket NAME`, with every rule of a single bucket: `list`, `read --items-file`, `delete --items-file` and `storage.auth.file` work as usual, and the generated scenario is the same.
- **Workloads:** with two or more names, the same as `--bucket-count`: `write`, `write-verify`, seeded `read`, seeded `delete` (with `--delete-batch-size=1`) and `mixed`. `read --items-file`, `delete --items-file`, `delete --delete-existing`, `read-verify`, `list`, `tables` and `mock` reject a list of two or more names.
- **Results:** the console's run parameters and the results summary show `Buckets: N (first ... last)`, and the run's settings (`spt_run_params.json`, `scenarioParams.BucketNames`) keep the full list.
- **Engine overrides:** set the names with `--buckets` or `--buckets-file`; an `--engine-override` that sets `storage.targetBucket.names`, directly or inside a map, is rejected. An override that replaces the `storage.targetBucket` map keeps the listed names.
- **Replay:** `spt replay` refuses an archive that sets a bucket list, before anything runs: replay targets every step at one bucket, so it cannot keep a list's buckets. See [Replay](REPLAY.md).
- **Custom scenarios:** in a custom scenario or direct engine run, `storage.targetBucket.names` is a list of names, and a non-empty list together with `storage.targetBucket.count` above 1 fails. The bucket segment of `item.output.path` is then a placeholder that is never evaluated, and never contacted unless an input names it; each listed bucket replaces it and keeps the rest of the path. A JavaScript array passes as its comma-separated string: `names: []` becomes one empty name, which counts as no list and falls back to the count, and a name that is empty or holds a comma fails the scenario.

## Bucket metrics

A step with more than one target bucket also records its counts and timings per bucket, and writes them to two files beside `metrics.total.csv`:

| File | Values |
|------|--------|
| `metrics.bucket.total.csv` | The whole step |
| `metrics.bucket.steady.total.csv` | Each bucket's own window, shifted from the aggregate's Steady-State Window |

A single bucket, a one-name list, a phase that persists no totals such as the seed of a `read`, and `--bucket-metrics=false` record none of this and write neither file, so a missing file means it was off. Existing files, the terminal output and the live endpoints are unchanged, and they still report totals across the set. An `--engine-override` of `output.metrics.bucket.enabled` wins over the flag.

**Rows.** A row is identified by `BucketKind`, `MemberIndex` and `OpType`. `BucketKind` is `member` or `catch-all`. `MemberIndex` is the bucket's place in the set, from 0, and `Bucket` is its exact name. A step has one `catch-all` row per operation type, written even when empty, for results whose bucket is not in the set, such as an items file that names another bucket. A bucket that is really named `other` is still a `member`. Rows are ordered by operation type, then member index, with the catch-all last.

**Summing.** For each operation type, the member rows and the catch-all row of `metrics.bucket.total.csv` sum to the step's `metrics.total.csv` when every row says `BucketData` `complete`. A steady row does not sum to the aggregate's steady row: it is an estimate over its own interval.

**Steady rows.** Each bucket window starts and ends with its own cuts, taken just after the aggregate's, so it is shifted later: it leaves out what completed before its start cut and includes what completed before its end cut. `StartDelay[ms]` and `EndDelay[ms]` say by how much, and `PassDuration[ms]` is how long the end cut took for that row. `SteadyTurnovers`, `SteadyTrend[%]` and `SteadyCV` are the aggregate's values, repeated. `SteadyStatus` follows the aggregate's rule on the row's own window, so a row can be `short` while the aggregate is `valid`, and a bucket with no completions in its window is `short` with empty timing cells.

**`BucketData`.**

| Value | Meaning | Compare with |
|-------|---------|--------------|
| `complete` | Every node in `NodesExpected` answered | The totals, for whole-step rows |
| `incomplete` | Some node did not answer. The row is the subtotal of `NodesReceived` nodes, and a steady row leaves its statistics empty with `SteadyStatus` `incomplete` | Only a subtotal of the same nodes |
| `unmeasured` | Steady file only: a node's bucket window could not be obtained, because a cut failed or missed the stop path's deadline. Statistics are empty and `SteadyStatus` is `incomplete` | Nothing |

If no node answers, the file holds only its header. `messages.log` has one `bucket-metrics` record per file and operation type, with the `expected`, `received` and `missing` nodes, a reason for each missing one, and the `data` outcome. It is INFO when every node answered and WARN otherwise. Use it to see which nodes a subtotal covers.

**Memory.** Each worker holds a set of meters per bucket and operation type, plus the copies a steady window needs: about 160 KiB per set, or about 640 MiB for 1,000 buckets and four operation types, and it reads the set size before any load. A step that would need more than 40% of the worker's maximum heap fails before it starts, with a message that says so. Use `--bucket-metrics=false`, or raise the engine's maximum heap.

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
- Each credential is checked separately. When per-path credentials (`storage.auth.file`) give two phases or paths different users for the same bucket, SPT checks the bucket as each user. A configured credentials file or items file that cannot be read fails the run before the phase starts, rather than skipping the check.

## Distributed runs

The engine on the entry node checks and creates the buckets once per phase, before any node starts the phase, using the entry node's network path and credentials. Bucket requests follow `--endpoint-selection` as object requests do: round robin keeps `--endpoint-hostname` for Host, signing and TLS SNI, and per-request DNS uses `--dns-server`. Worker nodes never create a bucket.

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
