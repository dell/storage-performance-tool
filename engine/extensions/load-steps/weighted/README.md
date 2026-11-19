# Introduction

Frequently there's a requirement to perform a specific load which may be
defined by a ratio between different operation types. For example the
load step may be described as:
* 20 % write operations
* 80 % read operations

# Requirements

1. Ability to specify the relative operations weight for a set of load
sub-steps

# Approach

The weighted load is implemented as a specific scenario step:

```javascript
WeightedLoad
    .append(weightedWriteConfig)
    .append(weightedReadConfig)
    .run();
```

Internally, the weighted load step contains several
[load generator](../../../doc/design/architecture/README.md#22-load-generator)s
(for each configuration element supplied) and single
[load step context](../../../doc/design/architecture/README.md#23-load-step-context).
The load step context contains the
[weight throttle](https://github.com/akurilov/java-commons/blob/master/src/main/java/com/github/akurilov/commons/concurrent/throttle/SequentialWeightsThrottle.java)
shared among the load generators configured.

## Configuration

The configuration option `load-generator-weight` should be used to set
the relative operations weight for the given load generator. The actual
weight is calculated as the specified weight value divided by the
weights sum.

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
