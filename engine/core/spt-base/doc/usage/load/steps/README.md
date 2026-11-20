# Load Steps

## 1. Identification

The load step ids are used primarily to distinguish the load step results.
```bash
java -jar spt-<VERSION>.jar --load-step-id=custom_test_0 ...
```

See also the [output reference](../../output#111-load-step-id)

## 2. Type

### 2.1. Linear

*Linear* load step type is used by default and may be considered as a straightforward way to generate a load.

### 2.2. Weighted

*Weighted* load step type is additional and requires the [WeightedLoad](https://github.com/dell/storage-performance-tool/tree/main/engine/extensions/load-steps/weighted) extention. 
It runs several sub-steps, such as writes and reads, with a relative weight for each. The
achieved mix follows the weights only in some cases; see the
[weighted load step](../../../../../../extensions/load-steps/weighted/README.md#when-the-mix-follows-the-weights).
Ids set in the step's appended contexts decide where its results go: see [Step ids](../../../../../../extensions/load-steps/weighted/README.md#step-ids).


### 2.3. Pipeline

*Pipeline* load step type is additional and requires the [PipelineLoad](https://github.com/dell/storage-performance-tool/tree/main/engine/extensions/load-steps/pipeline) extention. 
Load operations pipeline (create, delay, read-then-update, for example), extension.


## 3. Limits

By default the load steps are not limited explicitly. There are several ways to limit the load steps execution.

### 3.1. Operations Count

Limit the load step by the operation count:
```bash
java -jar spt-<VERSION>.jar --load-op-limit-count=1000000 ...
```

### 3.2. Time

Limit the load step by the time (5 minutes):
```bash
java -jar spt-<VERSION>.jar --load-step-limit-time=5m ...
```

### 3.3. Transfer Size

Limit the load step by the transfer size:
```bash
java -jar spt-<VERSION>.jar --load-step-limit-size=1.234TB ...
```

The size limit counts transferred bytes: the data that the load's operations write or read. An
operation that transfers no bytes cannot bring the load nearer to the limit, so the size limit never
stops a DELETE, STAT or NOOP load, or a load of zero-size items. Such a load ends at its count
limit, the step's time limit, or the end of its items; with recycling enabled it may not end at all.
A DELETE, STAT or NOOP load started with a size limit logs a warning in its `errors.log`.

* *Mixed* step: only the first enabled type's bytes count, in the order get, put, delete, stat.
  A mixed step whose first enabled type is delete or stat is not stopped by the size limit.
* *Weighted* step: a sub-load that reached the limit keeps running until the other sub-loads finish.
  At an unlimited rate it can run far past the limit.
* *Pipeline* step: a later stage that reaches its own count or size limit does not stop the stages
  before it; without a time limit the step ends only when those earlier stages end by themselves.

### 3.4. End Of Input

> *"EOI" = "End Of Input"*

A load step is also limited by the load operations *EOI*. End of the load operations input is reached if:
* Load operations recycling is disabled and end of the items *EOI* is reached
* Load operations recycling is enabled but all the load operations are failed (there's no successfull load operations to
  recycle)


