# Items Input

Items input is a source of the items which should be used to perform the load operations (create/read/etc).

## 1. File

The items once created with Spt may be stored into the output CSV file. This file may be reused on input for
other load operations, such as a `copy`, `read`, `update`, `delete` and `noop`.
```bash
java spt-<VERSION>.jar --item-input-file=items.csv ...
```

## 2. Item Path Listing Input

In some cases the items list file is not available. The items list may be obtained on the fly from the *storage path*
listing.
```bash
java spt-<VERSION>.jar --item-input-path=/path_to_list ...
```
**Note**:
> It may be useful to combine this option with `noop` to enumerate the items on the storage located in the specified
> path. Combining also with `item-output-file` option allows to store the enumerated items information for a further
> usage.

## 3. New Items Input

New items generator is used if no *items input file* neither *item input path* is specified and the configured load
operations type is `create`.

### 3.1. Naming

A new items generator may use custom items naming scheme.

#### 3.1.1. Types

The item naming type defines the function used to calculate the next item id from the previous one.

##### 3.1.1.1. Random

Random item ids are used by default

##### 3.1.1.2. Serial

Generate new item ids in the ascending order (starting from 0):
```bash
java spt-<VERSION>.jar --item-naming-type=serial --item-naming-seed=-1 --item-naming-step=1 ...
```

Generate new item ids in the descending order (starting from 999):
```bash
java spt-<VERSION>.jar --item-naming-type=serial --item-naming-seed=1000 --item-naming-step=-1 ...
```

#### 3.1.2. Prefix

The prefix option may be used to prepend some value for each new item.

```bash
java spt-<VERSION>.jar --item-naming-prefix=item_prefix_ ...
```

The prefix may be dynamic (see [expressions](../../../../src/main/java/com/dell/spt/base/config/el/README.md)).

#### 3.1.3. Prefix Shards

`item-naming-shards` places generated item names into a bounded set of
fixed-width prefix directories. The default is `0`, which preserves flat item
names. A positive value selects the exact number of directories:

```bash
java spt-<VERSION>.jar --item-naming-shards=16 ...
```

With 16 shards, generated paths use `s0000000/` through `s000000f/` before the
ordinary generated item id. If `item-naming-prefix` is also configured, it
appears before this directory, for example `benchmark/s000000f/<item-id>`.
Shard identifiers use base 36 and are selected from the generated item id
modulo the configured count.

The engine does not derive this value from concurrency or cluster size. Direct
JAR and scenario users must choose an explicit positive value when sharding is
desired. The higher-level SPT CLI defaults its `--prefix-shards` option to
automatic mode and supplies `item.naming.shards` using aggregate configured
concurrency. Use CLI `--prefix-shards 0` when flat generated keys are required.

This setting affects newly generated items. It does not reorganize existing
items or rewrite paths read from an item input file. Keep the same naming
configuration when a later phase must regenerate the names from the same seed.
For a direct-JAR non-create operation that regenerates names, include the storage
bucket/container in `item-naming-prefix`, or supply `item-input-file` or
`item-input-path`; a shard directory by itself is not a storage bucket.

#### 3.1.4. Radix

The radix option is used to encode the source number into the id. The radix should be in the range of \[2; 36].

```bash
java spt-<VERSION>.jar --item-naming-radix=16 ...
```

#### 3.1.5. Seed

The item ids will start from the next value calculated using the specified seed.

```bash
java spt-<VERSION>.jar --item-naming-seed=9876543210 ...
```

#### 3.1.6. Length

The length option determines the id length for the new item ids. The minor bits are used if the source number is
truncated.

```bash
java spt-<VERSION>.jar --item-naming-lenth=15 ...
```

## 4. Integrity manifest input

Metadata verification auto-detects a canonical RFC 4180 CSV only when its first
record exactly matches:

```text
bucket,key,size,version_id
```

The complete object key is CSV-escaped and is never parsed as a generated
numeric ID. `size` is a nonnegative decimal byte count. `version_id` may be
blank for current-version semantics or contain the exact S3 version to request.
Identity for de-duplication and resumability is `(bucket,key,version_id)`.
Quoted keys, including embedded newlines, are supported. A file without the
exact header continues through the legacy item-file reader, preserving existing
QA formats for current-version reads.
Engine integrity writers use UTF-8 and LF (`\n`) record separators for canonical
output. Readers continue to accept valid RFC 4180 input using LF or CRLF.

Engine-produced `written.csv`, discovery-produced `verify-input.csv`, and
verification-produced `verified.csv` have matching `*.complete.json` records.
The marker is published only after its CSV and binds version/status, positive
`run_id`, producer kind and ID, source/unique/selected counts, byte length, and
SHA-256. `engine_step` provenance requires the exact producing step ID;
`cli_stager` requires `spt-cli-items-stager-v1`. A missing, malformed, stale, or
mismatched marker prevents dependent I/O. Completion v2 adds
`excluded_delete_marker_count`, which is zero outside all-version discovery.
Each completion version is a closed schema: unknown JSON members and trailing
JSON values are rejected.

LIST discovery streams complete object entries into per-node sources, merges
RFC 4180 records, canonical external-sorts them by `(bucket,key,version_id)`,
de-duplicates shard overlap, applies the configured selection maximum,
publishes `verify-input.csv`, and only then starts READ. QA-owned `external`
input is parsed and validated but intentionally does not require an engine
completion marker.

With `load-op-list-include_versions=true`, LIST discovery preserves every data
version's exact ID in the canonical identity and excludes delete markers while
recording their count in completion v2. Distributed source counts include
overlapping per-node observations; unique and selected counts describe the
canonical post-de-duplication manifest.
