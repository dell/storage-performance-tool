# Byte Ranges Operations

Partial write/read and append operations performance is the subject of interest also in some cases.
To configure the partial/append operations it's necessary to specify the byte ranges to work with somehow.
There are two ways - use *random* byte ranges and to specify the *fixed* byte ranges.
The *random* byte ranges are specified by the count of the arbitrary ranges selected randomly by internal algorithm.
There's also **[RFC 7233](https://tools.ietf.org/html/rfc7233)** specification describing how to specify the *fixed*
byte ranges.

## 1. Limitations

* Effective only if load type is set to **update** or **read**.

* Random byte range count should not be more than maximum for the particular data item size used.
  (For details please refer to [this section](../../../../design/data_reentrancy/#random-range-update))

* It's not allowed to specify both random and fixed byte ranges simultaneously. The fixed byte ranges configuration will
  be used in this case only.

## 2. Configuration

* Update/Read load type should be used to use the feature: `--update`/`--read` or
    `--load-op-type=update`/`--load-op-type=read`

* Fixed byte ranges may be specified using the `--item-data-ranges-fixed=<VALUE>` configuration parameter.
    Multiple byte ranges may be specified using the comma as ranges separator.

* Random byte ranges may be specified using the `--item-data-ranges-random=<COUNT>` configuration parameter.

## 3. Effect

| Byte Ranges Configuration | Effect |
|---------------------------|--------|
| No byte ranges configured | Read/Overwrite the entire data item.  |
| A random count N          | Random Byte Ranges Read or Update with new data, N ranges per request. N should be > 0. |
| Fixed value: "N-"         | Read/Overwrite the part of the data item with the same data starting from the position of N bytes to the end of the data item. N should be less than data item size. |
| Fixed value: "-N"         | Read/Overwrite last N bytes of the data item. N should be less than data item size. |
| Fixed value: "N1-N2"      | Read/Overwrite the part of the data tiem with the same data in the range of N1-N2 bytes.<br/>**Note that according RFC 7233 the N2 is included in the range**.<br/>N1 should be not more than N2.<br/>N2 may be more than data item size. |
| Fixed value: "-N-"        | Append N bytes to the data item using the same data source. |

## 4. Examples

### 4.1. Random Ranges

Random ranges read example:
```bash
java -jar spt-<VERSION>.jar \
	--read \
	--item-data-ranges-random=2 \
	--item-input-file=items.csv \
	...
```

Random ranges update example:
```bash
java -jar spt-<VERSION>.jar \
	--update \
	--item-data-ranges-random=2 \
	--item-input-file=items2update.csv \
	--item-output-file=items_updated.csv \
	...
```

### 4.2. Fixed Ranges

Partial read of the data items from 2KB *(2048th byte, the 1st byte after 2048 bytes)* to the end:
```bash
java -jar spt-<VERSION>.jar \
	--read \
	--item-data-ranges-fixed=2KB- \
	--item-input-file=items.csv \
	...
```

Overwrite the data items from the *second byte (including it)* to the end:
```bash
java -jar spt-<VERSION>.jar \
	--update \
	--item-data-ranges-fixed=1- \
	--item-input-file=items2overwrite_tail2KBs.csv \
	--item-output-file=items_with_overwritten_tails.csv \
	...
```

Read the last 1234 bytes of the data items:
```bash
java -jar spt-<VERSION>.jar \
	--read \
	--item-data-ranges-fixed=-1234 \
	--item-input-file=items.csv \
	...
```

Overwrite the last 1234 bytes of the data items:
```bash
java -jar spt-<VERSION>.jar \
	--update \
	--item-data-ranges-fixed=-1234 \
	--item-input-file=items2overwrite_tail2KBs.csv \
	--item-output-file=items_with_overwritten_tails.csv \
	...
```

Partially read the data items each in the range from 2KB to *5KB + 1*:
```bash
java -jar spt-<VERSION>.jar \
	--read \
	--item-data-ranges-fixed=2KB-5KB \
	--item-input-file=items.csv \
	...
```

Overwrite the data items in the range from 2KB to *5KB + 1*:
```bash
java -jar spt-<VERSION>.jar \
	--update \
	--item-data-ranges-fixed=2KB-5KB \
	--item-input-file=items2overwrite_range.csv \
	--item-output-file=items_overwritten_in_the_middle.csv \
	...
```

Partially read the data items using multiple fixed ranges configuration:
```bash
java -jar spt-<VERSION>.jar \
	--read \
	--item-data-ranges-fixed=0-1KB,2KB-5KB,8KB- \
	--item-input-file=items.csv \
	...
```

#### 4.2.1. Append

Append 16KB to the data items:
```bash
java -jar spt-<VERSION>.jar \
	--update \
	--item-data-ranges-fixed=-16KB- \
	--item-input-file=items2append_16KB_tails.csv \
	--item-output-file=items_appended.csv \
	...
```

### Fixed-size single-range READ configuration (draft)

The separate `load.op.read.range` configuration uses nullable string values:

```yaml
load:
  op:
    read:
      range:
        size: "65536"
        offset: null
        align: "4096"
```

Null size disables this mode. Offset and alignment cannot be supplied without size.
Null offset selects a random aligned range; `"0"` selects fixed offset zero.
Null alignment and values `"0"` or `"1"` all mean effective alignment one.
Size must be positive, and offset and alignment must be nonnegative. A fixed offset
must be divisible by the effective alignment, and the inclusive endpoint must fit
in a signed 64-bit integer. Alignment need not be a power of two.

Direct-engine setters use nested paths, for example
`--load-op-read-range-size=64KiB --load-op-read-range-align=4KiB`.
Add `--load-op-read-range-offset=0` for fixed zero. Use quoted decimal byte strings
in generated configurations. Binary suffixes from B through EiB are also accepted;
fractions, signs, unknown suffixes and overflow are rejected.

This mode is restricted to DATA READ with the Netty `s3` driver. AWS, native RDMA,
other drivers, mixed workloads, content/metadata verification and recycled content
updates are unsupported. Active legacy fixed/random ranges or a positive splitting
threshold conflict with this mode. Disabled single-range configuration preserves
legacy behavior; inert legacy settings remain allowed.

The draft Netty S3 factory installs a dedicated range driver and matching runtime.
Linear, pipeline and weighted steps pass their effective policy during construction.
Other factories must explicitly implement `RangeReadDriverFactory`; configured range
mode cannot silently execute a whole-object READ. Disabled mode retains ordinary
factory construction.

Range mode requires a positive `storage.net.timeoutMilliSec` (the shipped default
is 30000), body READ rather than metadata-only READ, and object tagging disabled.
The driver sends one signed GET with the selected inclusive Range header and no
preflight object or bucket request. A fixed span is sent unchanged even when the
inventory size is smaller. Responses require strict status, range metadata, body
length and framing validation; rejected responses close the connection.

This remains draft functionality. Local packaged-engine scenario activation,
artifact collection, log API discovery and CLI retrieval have been exercised.
Distributed runtime, real-target enablement and READ/WRITE performance qualification
remain required; local checks are not release qualification.

For CLI examples, aligned selection, full-span requests and mutable datasets, see
[Partial-object READs](../../../../../../../../cli/docs/PARTIAL_READS.md).

### Terminal partial-read evidence (draft)

Partial READ contexts publish `range.read.csv` through the `RangeRead` logger after
stop. The coordinator collects worker rows and preserves `range.read.node-NNN.csv`
sources. The CLI fetches and indexes these optional artifacts; ordinary workloads
produce no range artifact.

Schema version 1 identifies the engine run, step, worker and context index. Each
row records fixed/random mode, requested size, explicit fixed-offset presence and
value, and effective alignment. Different context policies remain separate rows.
Successful bytes include only structurally validated responses. Logical outcomes
(selected, accepted, failed, unattempted, unresolved) are separate from attempted
logical reads, sent requests, and per-transport-attempt failure counters. HTTP,
response-validation, transport and local-selection failures have separate fields.
Failed/unresolved received bytes count delivered response-body bytes, not network
wire bytes. No object keys, credentials, or per-operation records are emitted.

`terminal=true` requires completed contexts and reconciled counters; inspect the
lifecycle fields and `overflow` alongside it. Source rows must be counted once:
do not sum the canonical collected file together with its node source copies.
AWS SDK retry accounting is deferred with the AWS driver implementation.
