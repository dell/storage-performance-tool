# Network Interfaces

By default an engine's S3 connections leave the client by the kernel's route to each endpoint, so one engine uses one NIC per destination. `--interfaces` gives each engine a NIC set instead: it spreads the engine's connections over several client NICs, each connection bound to its NIC's device and source address. One engine can then drive every port of a multi-port client.

This applies to the default Netty `s3` driver and to `s3-rdma`, which also gets one RDMA context, buffer pool and preparation thread per NIC: see [S3-RDMA with a NIC set](S3_RDMA.md#nic-sets). Preview a host's plan with [`spt verify --interfaces`](SPT_SYNTAX.md#nics-and-the-nic-plan) before a run.

## Options

| Flag | Environment | Engine setting | Default |
|------|-------------|----------------|---------|
| `--interfaces` | `SPT_INTERFACES` | `storage.net.interfaces` | none: the kernel's route |
| `--host-interfaces` | `SPT_HOST_INTERFACES` | `storage.net.nodeInterfaces` | none: every host follows `--interfaces` |

- **Values:** `auto`, or a comma-separated list of entries. An entry is an interface name (`ens1f0`), an IPv4 address held by an interface (`192.0.2.11`), or an RDMA device (`mlx5_0`), which names the interface that holds it. `auto` must be the only entry.
- **Sources:** the flag wins over `SPT_INTERFACES`, which `cli/.env` can also set. An error about the value names its source, for example `SPT_INTERFACES (environment or .env)`.
- **Commands:** `spt run` and `spt replay` take it with the same checks. `spt verify` takes it to preview the plan.
- **Without it,** the generated scenario is unchanged and the run behaves as before.

`spt run` rejects `--interfaces` with:

- `--s3-driver aws`, the driver that cannot bind to a NIC;
- the `tables` or `mock` workload;
- `--network-mode bridge`, where a container sees only its own interface;
- an `--endpoint-selection` other than `default`;
- an `--rdma-device` other than `auto`, or an `--rdma-local-ip`, from a flag, `RDMA_DEVICE`, `RDMA_LOCAL_IP` or `cli/.env`;
- an `--engine-override` of `storage.net.interfaces` or `storage.net.nodeInterfaces`, in either spelling. Set them with `--interfaces` and `--host-interfaces` only.

```bash
spt run read --interfaces auto --threads 64 \
  -e http://192.0.2.101:9000,http://192.0.2.102:9000
```

## Host overrides

Hosts whose NICs differ can each have their own value. `--host-interfaces <host>=<value>` replaces `--interfaces` on that host, and is repeatable; hosts without one follow `--interfaces`, or keep the kernel's route without it.

- **`<host>`** matches a `--test-hosts` entry by its host part, so `root@wrk2` and `wrk2` name the same host.
- **`<value>`** is `auto`, a list as for `--interfaces`, or `none`, which turns NICs off on that host. `none` is valid only here, and only alone.
- **Environment:** `SPT_HOST_INTERFACES`, with `;` between hosts: `wrk1=none;wrk2=mlx5_0,mlx5_1`. The flag replaces the whole variable.
- **Rejected:** a host not in `--test-hosts`, a host given twice, an empty value, and a value `--interfaces` would reject. A value that asks for NICs also has the rejections listed above; a run whose values are all `none` has none of them.
- **A single local host** is the entry node, so its override applies to it as to any other host.
- **A worker that does not start** (`--min-hosts`) has no slice, so its override is simply unused.

The usual case is a virtual entry node, which coordinates the run and has no NICs worth choosing, so it keeps its route while the workers use theirs:

```bash
spt run read --test-hosts entry,wrk2,wrk3 --interfaces auto --host-interfaces entry=none \
  --threads 64 -e http://192.0.2.101:9000,http://192.0.2.102:9000
```

The CLI passes the entry node one engine setting per host in `storage.net.nodeInterfaces`, keyed by `entry` or the worker's address as `load.step.node.addrs` lists it. The entry node applies each value to that node's `storage.net.interfaces` when it slices a step, so drivers see only that setting. A key that names no configured node is a configuration error. `config.yaml` on the entry node keeps the map; each slice's `config.yaml` shows its effective `interfaces` and an empty map.

## How each engine chooses its NICs

Every engine that runs a slice of the step, the Entry Node included, resolves the value on its own host. `auto` therefore handles clients with different NICs, while a named list must resolve on every host.

**`auto`** takes the NICs that meet all of these:

- the interface is up;
- it is an Ethernet interface backed by a PCI device, directly or through a bond or VLAN. This includes virtio-net. Loopback, bridges, veth, tun/tap, Docker, Tailscale and WireGuard interfaces, wireless interfaces and bond members are excluded;
- it reaches at least one of the engine's endpoints on a connected subnet, after endpoint slicing and after the endpoint's name is resolved on that engine.

Then, for each endpoint, among the NICs that reach it:

- of several interfaces on one physical port, the first by name is kept;
- of the rest, those with the highest link speed are kept. A NIC whose speed is unknown is kept only when every remaining NIC for that endpoint has an unknown speed.

A NIC kept for at least one endpoint is used, and serves exactly the endpoints it was kept for. With no NIC left, the step fails.

**An explicit list** skips these rules. Each listed NIC must reach at least one endpoint, on a connected subnet or by a gateway route through its device in the main routing table, and serves every endpoint it reaches. A list whose NICs have different link speeds is accepted with a warning.

### Virtual clients

Container `veth` interfaces are virtual, so a client that has only veth interfaces finds nothing under `auto`. A PCI-backed virtual NIC, such as virtio-net or many cloud NICs, is a candidate; when it reports no speed, it is kept unless a NIC of known speed reaches the same endpoint. An explicit list works on every client.

### Bonds and shared ports

- **Bonds:** a bond counts as one NIC. Spreading its traffic over the member links is the bond's hash, not SPT's. When a chosen bond's `xmit_hash_policy` is `layer2` or `layer2+3`, every connection to one endpoint uses one member link; verify and the step's start warn and suggest `layer3+4`.
- **Shared physical ports:** interfaces backed by one physical port, such as two VLANs on one port or a VLAN and its parent, share that port's bandwidth. `auto` keeps the first by name for each endpoint that several of them reach, so VLANs that reach different endpoints are all used. An explicit list may name several of them only when no endpoint is served by two of them; otherwise the step fails. Verify and the step-start log name each shared port and the NICs on it.

## Host settings

SPT never changes host routing or sysctls. When a chosen NIC shares its IPv4 subnet with another interface that is up and has an address, chosen or not, the host needs these settings. "Effective" is the larger of the `net.ipv4.conf.all` value and the interface's own.

| Setting | Required | When it is lower |
|---------|----------|------------------|
| `arp_ignore` | effective 1 or more | The step fails. With 0, any interface answers ARP for every local address, so the server can send replies for one NIC to another NIC's MAC, and a connection bound to its device resets at random. |
| `arp_announce` | effective 2 | Warning. |
| `rp_filter` | effective 0 or 2 | The step fails on 1 (strict), which drops the replies to every NIC after the first on a shared subnet. |

For example, in `/etc/sysctl.d/90-spt-nics.conf`, applied with `sysctl --system`:

```text
net.ipv4.conf.all.arp_ignore = 1
net.ipv4.conf.all.arp_announce = 2
net.ipv4.conf.all.rp_filter = 2
```

Interfaces on one Layer 2 segment but in different subnets can still answer ARP for each other's addresses; SPT does not check that case.

## Supported routing model

- **Reaching an endpoint** means a connected IPv4 subnet, and for an explicit list also a gateway route through the NIC's device in the main routing table.
- **Policy routing** (`ip rule`) is not followed. Verify lists the rules other than the defaults, and a NIC that the model excludes is reported as excluded, not as a failed connection.
- **Other configurations,** such as `arp_filter` with source routing, can be valid, but SPT does not support them.
- **Endpoints:** each engine resolves each endpoint once at the start of a step and uses its first IPv4 address for the whole step. An endpoint given twice fails the step; two endpoints that resolve to the same address and port warn.

## Transport and containers

- **Transport:** binding a connection to a device needs the `epoll` transport, the shipped default of `storage.net.transport`. Its native library is bundled for x86_64 only, so on arm64, or with `nio` or `kqueue`, a NIC set is a configuration error that names the transport.
- **Containers:** the engine needs host networking, the `--network-mode` default.
- **Custom scenarios** that set `storage.net.interfaces` directly also need `storage.net.node.connAttemptsLimit` at 0, the default, and no copy steps or NOOP operations. In JavaScript, give the value as an array or a comma-separated string; `[]` or `""` turns off a NIC set a sub-load inherits.

## Paths and pairing

A Path is one NIC paired with one endpoint. The engine groups its endpoints by the set of chosen NICs that serve each one; each group pairs its own NICs and endpoints. Within a group of `E` endpoints and `N` NICs, in configured order, there are `max(E, N)` Paths, and Path `k` pairs NIC `k mod N` with endpoint `k mod E`:

| NICs | Endpoints | Paths |
|------|-----------|-------|
| 4 | 4 | NIC `i` to endpoint `i` |
| 2 | 4 | NIC 0 to endpoints 0 and 2, NIC 1 to endpoints 1 and 3 |
| 4 | 2 | NICs 0 and 2 to endpoint 0, NICs 1 and 3 to endpoint 1 |
| 3 | 4 | NIC 0 to endpoints 0 and 3, NIC 1 to endpoint 1, NIC 2 to endpoint 2 |

Pairing avoids the full cross product, in which every client port receives from every storage node at once. Its cost is uneven load when neither `E` nor `N` divides the other: in the last row, endpoints 0 and 3 share one NIC's load, so each gets half the load of endpoints 1 and 2. Verify and the step's start warn about uneven pairing, and about a NIC that is in several groups, whose Paths in all of them share its permits.

With `--slice-endpoints`, each engine pairs its own slice of the endpoints. A chosen NIC that serves no endpoint, or an endpoint that no chosen NIC serves, fails the step.

## Concurrency and event loops

- **Permits:** the concurrency limit (`--threads`) is split over the engine's NICs. With 64 threads and 2 NICs, each NIC carries at most 32 operations; with 10 threads and 4 NICs, 3, 3, 2 and 2. With fewer threads than NICs, each NIC carries one operation at a time. Each operation takes the next NIC with room, and waits in the driver's queue while none has room.
- **Event loops:** each NIC gets its own event loops. `storage.driver.threads` therefore counts loops per NIC with a NIC set, not per driver. Without it, each NIC gets `max(4, hardware threads / 8)` with the `s3` driver, and `max(4, hardware threads / 48)` with `s3-rdma` while RDMA is enabled, since RDMA payloads don't pass through the loops: on a client with 64 hardware threads and 2 NICs, 8 per NIC and 16 in all over HTTP, 4 per NIC and 8 in all over RDMA. With `storage.rdma.enabled=false`, `s3-rdma` carries every payload over HTTP and takes the `s3` default. Each NIC-set driver also has a base group of 2 loops for its control requests. Set it with `--engine-override storage.driver.threads=6`. Fewer loops use less client CPU per operation, and can cap peak HTTP throughput: on a client with four 400G NICs and 192 hardware threads, 12 loops per NIC instead of the default 24 used 20% less CPU per operation and carried 13% less at GET 4 MiB over HTTP with 768 threads, and matched it at 192 threads. Over RDMA on the same client, 4 loops per NIC carried as much as 24 at GET 4 MiB and GET 1 MiB on four NICs, and 9% more at GET 64 KiB on one NIC, for 12% to 33% less CPU per operation.

## Start of a step

A step in which any engine has a NIC set, from `--interfaces` or a host override, starts behind an admission barrier. Without one, a step starts as it did before.

1. **Preparation.** The entry node distributes any item list, then every engine resolves its endpoints and NICs, runs the checks above, builds its Path Pools and probes every Path, with operation admission held. A failed connect on any Path names the host, the NIC, the endpoint and the cause, as do the plan's other failures. An engine without a NIC set in such a step reports ready as soon as its driver has started, and keeps the kernel's route.
2. **Release.** The entry node opens admission on every engine only when all are ready. One INFO line gives the time: `Step read1: admission opened after 12.4 s of preparation (item distribution 3.1 s; slowest 198.51.100.12:1099, 9.1 s)`.
3. **Failure.** If any engine fails, or the deadline passes, every engine is fenced, so nothing later can open admission for that step. The step fails, naming each engine:
   - `Step read1 did not start: 1 of 3 engines failed preparation — 198.51.100.12:1099: <its message>`
   - `Step read1 did not start: preparation did not finish within 10m (load.step.preparation.timeout) — 198.51.100.12:1099: still probing paths`
   - `Step read1 did not start: the engine on 198.51.100.12:1099 does not support the admission barrier, which a step with a NIC set needs; run the same SPT image on every host`

   A slice that cannot be released stops the step: `Step read1 stopped: admission could not be released on 198.51.100.12:1099: <cause>`. Each engine still writes its per-path file, with the rows it reached and an `end` row with `complete=false`.

**The Load Interval starts at the release.** With a barrier, a step's duration, time limit, elapsed time and rates count from the moment admission opens, so preparation counts in none of them; the Steady-State Window lies inside the interval. A run with a NIC set therefore takes longer on the clock than its `--duration` by its preparation time.

**The deadline** is the engine setting `load.step.preparation.timeout`, default `10m`, set with `--engine-override load.step.preparation.timeout=15m`. It covers item distribution and every engine's preparation, and each worker also enforces it on its own clock, so a worker fences itself even if the entry node is gone. A timeout message names the setting.

Item distribution takes little of this on a fast network. On the lab, three workers took a list of 5 million items in 4.5 s, about 1.1 million items per second; at that rate the default would cover several hundred million, though that is an extrapolation from one network and from lists no larger than 5 million. Item files on the CLI's host have a tighter limit first: on each remote host, staging them, all files together, must finish within 30 s.

**During the step,** after a failed connect, that NIC's connects to that endpoint back off from 100 ms, doubling to 5 s; an operation that needs a new connection there fails at once until the next trial, while open connections keep serving. A NIC whose last attempt got no response (a failed or refused connect, a connect in back-off, a lost connection or a request timeout) is skipped while another NIC is working: it takes one trial operation at a time, none while its connects back off, the first 100 ms after the failure and then at intervals that double with each later failure up to 5 s, and its full share again once an attempt on it gets a response, an error response included. Failures while an interval runs, such as those of its other operations in flight, do not lengthen it. Its share is not given to the other NICs, so the step runs at their shares. When every NIC is failing, operations take the next NIC with room, and the step fails at its failure limit. Control requests keep the kernel's route: bucket checks, listing a bucket as a step's item input, and multipart abort.

## Results

- **Per-Path counts:** each step writes `<step>.nic.paths.csv`, and the run summary names the files in one line, such as `NIC paths: read1.nic.paths.csv`. See the engine's [per-path NIC evidence](../../engine/core/spt-base/README.md#per-path-nic-evidence).
- **NIC-set report:** each step writes `<step>.nic.set.json`, each engine's NICs and event loops. See the engine's [NIC-set report](../../engine/core/spt-base/README.md#nic-set-report).
- **Distributed steps:** the Entry Node keeps each node's files as `<step>.nic.paths.node-NNN.csv` and `<step>.nic.set.node-NNN.json`. `node-000` is the Entry Node, followed by the workers that started, in `--test-hosts` order; a worker that fails to start under `--min-hosts` gets no slice. An expected file that is absent is recorded as `missing` in `index.json`, and the run does not fail.
- **Run metadata** in `spt_run_params.json`:
  - `scenarioParams.Interfaces`: the requested entries;
  - `nicNodes`: each node with its host, the value it received, and `source`: `host` for a host override, `run` otherwise. A node whose value is `none` writes no NIC files, and the fetcher does not report them as missing;
  - `hostInterfaces`: the `--host-interfaces` values by host, as given;
  - `preparation`: by step, the entry node's item distribution and each engine's preparation in milliseconds, which lie outside the Load Interval;
  - `nicEngines`: per step, each engine's report, with its node, host, number of NIC-set drivers, `driverThreads` (0 for the default), `hardwareThreads`, `eventLoopsPerNic`, `nics` and `eventLoops`. A step with several NIC-set drivers, such as a `pipeline` or `weighted` step, adds `contexts` with each driver's values, and a field the drivers disagree on is left out at the top;
  - `nicEnginesError`: why a report could not be read. The run does not fail.

```json
"nicEngines": {
  "read1": [
    {"node": 0, "host": "client-a.example", "drivers": 1, "driverThreads": 0,
     "hardwareThreads": 64, "eventLoopsPerNic": 8, "nics": ["ens1f0", "ens1f1"],
     "eventLoops": 16}
  ]
}
```

- **Run summary:** Workload Configuration adds these lines after `Endpoints`; each of the first two appears only with its flag:

  ```text
    • Interfaces         auto
    • Host interfaces    wrk1=none; wrk2=mlx5_0,mlx5_1
    • Event loops        16 per engine (2 NICs × 8, default) + 2 base = 18
  ```

  `storage.driver.threads` replaces `default` when it is set. When engines differ, the line gives each node's value, such as `node-000 16 (2 NICs × 8, default); node-001 8 (1 NIC × 8, default)`; when an engine changed between steps, it reads `vary by step; see nicEngines in spt_run_params.json`. When an expected report is missing or could not be read, the line gives each node that reported and ends `no readable report from the other engines`, since those engines may differ; Artifact Health names a missing report, and Warnings one that could not be read. A step that also has drivers without a NIC set adds `, not counting 1 driver without a NIC set`. Without reports, only the `Interfaces` line appears.

## Replay

`spt replay` takes `--interfaces`, `--host-interfaces` and their environment variables as `spt run` does, with the same checks; a value that fails them fails the replay as `invalid_interfaces`. The CLI builds the per-node map from the current workers at every run, so a replay that changes `--test-hosts` gets new keys, and an override whose host is not in `--test-hosts` is rejected, naming the host. Replay never keeps a source run's NIC set: an archived `storage.net.interfaces` or `storage.net.nodeInterfaces` is dropped with one warning, such as `the source run used storage.net.interfaces=auto; replay ignores it, pass --interfaces to use NICs`. In a JavaScript archive, a setting replay cannot remove from a literal `storage.net` object (one in a variable that `net` or `storage` refers to, an assignment, a flat key such as `storage-net-interfaces`, or a `net` object outside `storage`) fails the replay as `invalid_interfaces`, because left in place it could override the replay's NIC set past these checks. Replay does not run scenario code, so a name the code computes is not detected.
