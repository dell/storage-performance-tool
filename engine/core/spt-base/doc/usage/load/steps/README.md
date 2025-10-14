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

*Weighted* load step type is additional and requires the [WeightedLoad](https://github.com/dell-spt/spt-load-step-weighted) extention. 
Weighted load extension, allowing to generate 20% write and 80% read operations, for example.


### 2.3. Pipeline

*Pipeline* load step type is additional and requires the [PipelineLoad](https://github.com/dell-spt/spt-load-step-pipeline) extention. 
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

### 3.4. End Of Input

> *"EOI" = "End Of Input"*

A load step is also limited by the load operations *EOI*. End of the load operations input is reached if:
* Load operations recycling is disabled and end of the items *EOI* is reached
* Load operations recycling is enabled but all the load operations are failed (there's no successfull load operations to
  recycle)


