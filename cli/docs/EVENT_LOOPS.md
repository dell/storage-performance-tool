# Event Loops

Every engine reports, for each step, the Netty event loops its storage drivers own and what
sized them. The CLI fetches each report with the step's other files, stores them in
`spt_run_params.json` and names the count in the run summary. A run with a
[NIC set](NETWORK_INTERFACES.md#results) shows its NIC-set report's line instead.

## Report

- **Single engine:** each step writes `<step>.event.loops.json`, with or without a NIC set. See
  the engine's [event-loop report](../../engine/core/spt-base/README.md#event-loop-report) for
  its fields.
- **Distributed steps:** the Entry Node keeps each node's report as
  `<step>.event.loops.node-NNN.json`, numbered as the
  [NIC files](NETWORK_INTERFACES.md#results) are: `node-000` is the Entry Node, followed by the
  workers that started, in `--test-hosts` order.
- **Missing reports:** once one node's report of a step arrives, a node copy that is absent is
  recorded as `missing` in `index.json`, such as
  `expected from node-001 (worker-b.example); not listed in index.json`, and Artifact Health
  names it. The run does not fail. Engines that predate the report write none, so a step
  without any report records nothing missing.

## Run metadata

`spt_run_params.json` stores the reports under `eventLoopEngines`, per step in node order:

- `node` and `host`: the engine's slice and, when it is known, its host;
- `hardwareThreads`: the engine's hardware threads;
- `eventLoops`: the engine's total over its contexts;
- `contexts`: each driver context that owns event loops, with its `contextIndex`, `route`
  (`default` or `nic_set`), `driverThreads` (the configured `storage.driver.threads`, 0 for
  the default), `source` (`storage.driver.threads` when that setting is above 0, otherwise
  `default`) and `eventLoops`. It is left out when the step's drivers own none.

`eventLoopEnginesError` names each report that could not be read, and why. The run does not
fail.

```json
"eventLoopEngines": {
  "read1": [
    {"node": 0, "host": "client-a.example", "hardwareThreads": 192, "eventLoops": 24,
     "contexts": [{"contextIndex": 0, "route": "default", "driverThreads": 0,
                   "source": "default", "eventLoops": 24}]}
  ]
}
```

## Run summary

A run without a NIC set adds an `Event loops` line to Workload Configuration, after
`Endpoints`:

```text
  • Event loops        24 per engine (default, 192 hardware threads)
```

- `--engine-override storage.driver.threads=12` reads `12 per engine (storage.driver.threads)`.
- Several drivers that agree, such as a `pipeline` or `weighted` step's, read
  `48 per engine (2 drivers × 24, default, 192 hardware threads)`; drivers that differ read
  `36 per engine (2 drivers; see eventLoopEngines in spt_run_params.json)`.
- When engines differ, the line gives each node's value, such as
  `node-000 24 (default, 192 hardware threads); node-001 4 (default, 6 hardware threads)`.
- When an engine changed between steps, it reads
  `vary by step; see eventLoopEngines in spt_run_params.json`.
- When an expected report is missing or could not be read, the line gives each node that
  reported and ends `no readable report from the other engines`, since those engines may
  differ. Artifact Health names a missing report, and Warnings one that could not be read, as
  `Event-loop reports not read: <file>: <reason>`.

A step whose drivers own no event loops, such as a driver that is not Netty-based, adds
nothing to the line. Results from engines that predate the report have no line.
