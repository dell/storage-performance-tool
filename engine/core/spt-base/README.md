# Contents

1. [Overview](#1-overview)
2. [Features](#2-features)<br/>
3. [Bundles & Extenstions](#3-bundles-and-extenstions)
4. [Get started](doc/getstarted) 🏁 <br/>
5. [Comparison With Similar Tools](doc/comparision)<br/>
6. [Documentation](doc) 📄 <br/>
7. [Contributing](doc/contributing)<br/>
8. [Changelog](doc/changelog)<br/>
9. [FAQ](doc/faq) ❓ <br/>

# 1. Overview

Spt is a distributed storage performance testing tool. This repo contains the basic functionality only. See the 
[extensions](#23-extension) for the actual use. 

# 2. Features

## 2.1. Scalability

### 2.1.1. Vertical

Using [fibers](https://github.com/akurilov/fiber4j) allows to sustain millions of concurrent operations easily without
significant performance degradation.

### 2.1.2. Horizontal

The [distributed mode](doc/design/modes/distributed_mode) in Spt was designed as P2P network. Each peer/node performs
independently as much as possible. This eliminates the excess network interaction between the nodes which may be a
bottleneck.

## 2.2. Customization

### 2.2.1. Flexible Configuration

* Safe: the configuration options are being checked against the schema
* Extensible: Spt's plugins may come up with own configuration options making them available from the joint CLI and being checked against the schema 
* [Expressions](doc/usage/input/configuration#124-expression) allow to specify the dynamically changing values 

### 2.2.2. Load Generation Patterns

* CRUD operations and the extensions: Noop, [Copy](doc/design/modes/copy_mode), etc

* [Parial Operations](doc/usage/load/operations/byte_ranges)

* [Composite Operations](doc/usage/load/operations/composite)

* Complex Load Steps
    * [Pipeline Load](https://github.com/dell/storage-performance-tool/tree/main/engine/extensions/load-steps/pipeline)
    * [Weighted Load](https://github.com/dell/storage-performance-tool/tree/main/engine/extensions/load-steps/weighted)
* [Recycle Mode](doc/design/modes/recycle_mode)

* [Data Reentrancy](doc/design/data_reentrancy)

  Allows to validate the data read back from the storage successfully even after the data items have been randomly
  updated multiple times before

* Custom Payload Data

### 2.2.3. [Scenarios](doc/usage/input/scenarios)

Scenarios allow to organize the load steps in the required order and reuse the complex performance tests

### 2.2.4. [Metrics Reporting](doc/usage/output#2-metrics)

The metrics reported by Spt are designed to be most useful for the performance analysis. The following metrics are
available:

* Counts

  * Items
  * Bytes transferred
  * Time
    * Effective
    * Elapsed

* Rates

  * Items per second
  * Bytes per second

* Timing distributions for:

  * Operation durations
  * Network latencies

* Actual concurrency

  It's possible to limit the rate and measure the sustained actual concurrency

The *average* metrics output is being done periodically while a load step is running. The *summary* metrics output is
done once when a load step is finished. Also, it's possible to obtain the highest precision metrics (for each operation,
so called *I/O trace* records).

## 2.3. [Extension](src/main/java/com/dell/spt/base/env)

Spt is designed to be agnostic to the particular extensions implementations. This allows to support any storage,
scenario language, different load step kinds.

### 2.3.1. Load Steps

The load step is needed to define how to generate the load (operations type/order/ratio/etc).
Spt basically includes the linear load step implementation which may be considered as a straightforward way to
generate a load. Other load step implementations allow to specify some custom and more complex load pattern. See the
load step extensions in the [extensions/load-steps](../../extensions/load-steps) directory.

### 2.3.2. Storage Drivers

The storage driver is used by Spt to interact with the given storage. It translates the Spt's abstract
operations into the actual I/O requests and executes them. Spt basically includes the dummy storage driver only
which does nothing actually and useful only for demo/testing purposes. See the storage driver extensions in the
[extensions/storage-drivers](../../extensions/storage-drivers) directory.

### 2.3.3. Scenario Engine

Any Spt scenario may be written using any JSR-223 compliant scripting language. Javascript support is available
out-of-the-box.

# 3. Bundles and Extenstions

This directory (`spt-base`) contains the core functionality. All extensions and additional spt tools are located in the [extensions](../../extensions) directory of this repository. Each component has its own documentation.

