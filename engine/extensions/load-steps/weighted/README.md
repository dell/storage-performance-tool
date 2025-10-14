[![Gitter chat](https://badges.gitter.im/dell-spt.png)](https://gitter.im/dell-spt)
[![Issue Tracker](https://img.shields.io/badge/Issue-Tracker-red.svg)](https://spt-issues.atlassian.net/projects/GOOSE)
[![CI status](https://gitlab.com/dell-spt/spt-load-step-weighted/badges/master/pipeline.svg)](https://gitlab.com/dell-spt/spt-load-step-weighted/commits/master)
[![Tag](https://img.shields.io/github/tag/dell-spt/spt-load-step-weighted.svg)](https://github.com/dell-spt/spt-load-step-weighted/tags)
[![Maven metadata URL](https://img.shields.io/maven-metadata/v/http/central.maven.org/maven2/com/github/dell-spt/spt-load-step-weighted/maven-metadata.xml.svg)](http://central.maven.org/maven2/com/github/dell-spt/spt-load-step-weighted)
[![Sonatype Nexus (Releases)](https://img.shields.io/nexus/r/http/oss.sonatype.org/com.github.dell-spt/spt-load-step-weighted.svg)](http://oss.sonatype.org/com.github.dell-spt/spt-load-step-weighted)
[![Docker Pulls](https://img.shields.io/docker/pulls/dellspt/spt-load-step-weighted.svg)](https://hub.docker.com/r/dellspt/spt-load-step-weighted/)

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

## Limitations

1. Weighted load step should contain at least 2 configuration elements.

## Output

Specific log messages:

1. `Run the weighted load step "<STEP_ID>"`
2. `Weighted load step "<STEP_ID>" started`
3. `Weighted load step "<STEP_ID>" done`
4. `Weighted load step "<STEP_ID>" timeout`

## Jar & Sources
https://mvnrepository.com/artifact/com.github.dell-spt/spt-load-step-weighted 
