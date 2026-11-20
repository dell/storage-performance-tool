# Introduction

Frequently there's a requirement to perform a specific load which may be
defined by a ratio between different operation types. For example the
load step may be described as:
* 20 % write operations
* 80 % read operations

The weighted load step aims at such a mix. The weights set how many
operations each sub-step may offer, not how many it sends, so the
achieved mix follows the weights only in the cases listed in
[When the mix follows the weights](#when-the-mix-follows-the-weights).
For an exact mix of operation types, see
[Getting a chosen mix](#getting-a-chosen-mix).

# Requirements

1. Ability to specify the relative operations weight for a set of load
sub-steps. A weight is relative to the other weights of the step, not a
guaranteed share of the operations sent.

# Approach

The weighted load is implemented as a specific scenario step:

```javascript
WeightedLoad
    .append(weightedWriteConfig)
    .append(weightedReadConfig)
    .run();
```

Internally, the weighted load step contains a
[load generator](../../../core/spt-base/doc/design/architecture/README.md#22-load-generator)
and a
[load step context](../../../core/spt-base/doc/design/architecture/README.md#23-load-step-context)
for each configuration element (sub-step) supplied. Each context has its
own storage driver, input queue and concurrency limit. Only the weight
throttle is shared among all the load generators of the step.

A load generator takes a weight permit, then a rate permit if a rate
limit is set, and then offers the operations to its storage driver. A
weight therefore sets how many operations a context may offer in each
round of the weight throttle. A context limited by its concurrency, by
its target's latency or by a rate limit runs at its own pace, and the
achieved mix then follows those limits and not the weights.

## Configuration

The configuration option `load-op-weight` (`load.op.weight` in a
scenario) sets the relative operations weight for the given sub-step.
The weight divided by the weights sum is the sub-step's share of the
operations offered, not of the operations sent. Weights summing below
100, including the default of 1, are multiplied at step start by the
smallest integer that brings the sum to at least 100, so they keep their
ratio and do not limit throughput.

```javascript
var weightedWriteConfig = {
    ...
    "load": {
        "op": {
            "weight": 20
        }
    },
    ...
};
var weightedReadConfig = {
    ...
    "load": {
        "op": {
            "weight": 80,
            "type": "read"
        }
    },
    ...
};
```

**By default** weights are distributed evenly between steps. For example
* if 1 step  - weight == 100%
* if 2 steps - weight == 50% for each
* if 3 steps - weight == 33% for each, etc.

**Notes**
> * Full example may be found in the `example/scenario/js/types/weighted.js` scenario file.
> * Weight throttle will never permit the operations if the corresponding weight is 0

### When a sub-load finishes first

Each cycle gives every sub-load up to its weight in operations, and the next
cycle starts when those shares are used up. A sub-load finishes scheduling when
it reaches its count limit or, if it does not recycle its operations, when its
item input runs out. It then leaves the cycle: the other sub-loads carry on to
their own limits and share the cycle by their weights, so the configured ratio
no longer applies to the finished sub-load. A retry of one of its operations
still runs, outside the cycle.

A sub-load that recycles, which includes every path listing (`list` with
`item.type=path`), finishes only at its count limit. If it has nothing left to
issue before then, it keeps its place in the cycle and the other sub-loads stop
at the end of their shares. A sub-load with work beyond its share never gets
to it, so the step then ends only at its time limit, and without one it does
not end.

## When the mix follows the weights

The achieved mix follows the weights when:

1. Every context is limited by its load generator rather than by its
   target, as with the `dummy-mock` storage driver.
2. A context recycles its items (`load.op.recycle.mode`) and its whole
   item set fits in its input queue (`storage-driver-limit-queue-input`).
   It then holds the other context down to its weight. This works as a
   cap only: it does not speed up a context that is slower than its
   weight. The `example/scenario/js/types/weighted.js` scenario is this
   case.
3. Two contexts both recycle their items. The mix follows the weights
   once the start-up backlog has cleared.
4. Each context's operation count limit (`load-op-limit-count`) is in
   proportion to its weight. The final counts then follow the weights.

Otherwise the contexts share the work according to their concurrency,
their target's latency and any rate limits. Where the mix follows the
weights, it is counted in operations, not in bytes.

A rate limit set for the step (`load-op-limit-rate`) applies to each
context separately, not to the step as a whole.

## Getting a chosen mix

* Use the `MixedLoad` step for an exact mix of read, create, delete and
  stat operations over one item set. The CLI runs it as the `mixed`
  workload; see the
  [mixed workload options](../../../../cli/docs/SPT_SYNTAX.md#9-mixed-workload-options).
* Set each context's concurrency (`storage-driver-limit-concurrency`) in
  proportion to its weight multiplied by its operation latency.
* Set each context's rate limit (`load-op-limit-rate`) in proportion to
  its weight.

## Step ids

A weighted load step can set `load.step.id` with `config()`, and each context passed to
`append()` can set its own. These ids decide the folders under `log/` that hold the step's
results:

* A context that sets `load.step.id` in its `append()` map writes its metrics files and
  `result.xml` under `log/<that id>/`.
* The id of the last context also names the step, so the step's own files, such as the
  operation lifecycle files, go to that folder too.
* A context that sets no id of its own writes under the id set on the step with `config()`, or
  else under a generated `weighted_<yyyyMMdd.HHmmss.SSS>` id. It does not take the last
  context's id. When the last context sets no id, the step's own files go to that folder too.

For example, with no id set by `config()`:

```javascript
WeightedLoad
    .append({"load": {"op": {"weight": 50, "limit": {"count": 20}}}})
    .append({"load": {"op": {"weight": 50, "limit": {"count": 20}}, "step": {"id": "ctx_b"}}})
    .run();
```

writes:

```
log/
├── none-<timestamp>/       engine start-up files
├── weighted_<timestamp>/   first context: metrics*.csv, result.xml
└── ctx_b/                  last context: metrics*.csv, result.xml; and the step's own files
```

The exact files in each folder depend on the step and its configuration.

A context's id is used as given each time a step with that context runs:

* When the scenario runs such a step again and the context is not the last, both steps run and
  the scenario exits 0, but both write that context's results to one folder: its `result.xml`
  holds two `<result>` documents, and its `metrics.total.csv` and `metrics.steady.total.csv`
  repeat their header.
* When the context is the last, its id names the step, so the second step is handled as any
  step whose id is already used in the log directory: it is refused (`StepIdInUseException`)
  and the scenario fails when a result file it would write under that id already exists, as for
  a CREATE after a CREATE; otherwise it runs and can add a second `<result>` to that
  `result.xml`, as a CREATE after a NOOP does.

So give a context a different id on each run, or set no id in the contexts.

## Limitations

1. Weighted load step should contain at least 2 configuration elements.

## Output

Specific log messages:

1. `Run the weighted load step "<STEP_ID>"`
2. `Weighted load step "<STEP_ID>" started`
3. `Weighted load step "<STEP_ID>" done`
4. `Weighted load step "<STEP_ID>" timeout`

## Jar & Sources
https://mvnrepository.com/artifact/io.github.dell.spt/spt-load-step-weighted 
