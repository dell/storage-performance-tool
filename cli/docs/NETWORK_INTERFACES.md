# Network Interfaces

By default an engine's S3 connections leave the client by the kernel's route to each endpoint, so one engine uses one NIC per destination. `--interfaces` gives each engine a NIC set instead: it spreads the engine's connections over several client NICs, each connection bound to its NIC's device and source address. One engine can then drive every port of a multi-port client.

This applies to the default Netty `s3` driver. Preview a host's plan with [`spt verify --interfaces`](SPT_SYNTAX.md#nics-and-the-nic-plan) before a run.

## Options

| Flag | Environment | Engine setting | Default |
|------|-------------|----------------|---------|
| `--interfaces` | `SPT_INTERFACES` | `storage.net.interfaces` | none: the kernel's route |

- **Values:** `auto`, or a comma-separated list of entries. An entry is an interface name (`ens1f0`), an IPv4 address held by an interface (`192.0.2.11`), or an RDMA device (`mlx5_0`), which names the interface that holds it. `auto` must be the only entry.
- **Sources:** the flag wins over `SPT_INTERFACES`, which `cli/.env` can also set. An error about the value names its source, for example `SPT_INTERFACES (environment or .env)`.
- **Commands:** `spt run` and `spt replay` take it with the same checks. `spt verify` takes it to preview the plan.
- **Without it,** the generated scenario is unchanged and the run behaves as before.

`spt run` rejects `--interfaces` with:

- a driver other than the default Netty `s3` driver: `--s3-driver aws`, `--s3-driver rdma` or `--use-rdma`;
- the `tables` or `mock` workload;
- `--network-mode bridge`, where a container sees only its own interface;
- an `--endpoint-selection` other than `default`;
- an `--rdma-device` other than `auto`, or an `--rdma-local-ip`, from a flag, `RDMA_DEVICE`, `RDMA_LOCAL_IP` or `cli/.env`;
- an `--engine-override` of `storage.net.interfaces` or `storage.net.nodeInterfaces`, in either spelling. Set the NIC set with `--interfaces` only.

```bash
spt run read --interfaces auto --threads 64 \
  -e http://192.0.2.101:9000,http://192.0.2.102:9000
```

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
- **Custom scenarios** that set `storage.net.interfaces` directly also need `storage.net.node.connAttemptsLimit` at 0, the default, and no copy steps or NOOP operations.

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
- **Event loops:** each NIC gets its own event loops. `storage.driver.threads` therefore counts loops per NIC with a NIC set, not per driver. Without it, each NIC gets `max(4, hardware threads / 8)`: on a client with 64 hardware threads and 2 NICs, 8 per NIC and 16 in all. Set it with `--engine-override storage.driver.threads=6`.

## Start of a step

Before the first operation, each engine connects once on every Path. A failure fails the step and names the host, the NIC, the endpoint and the cause, as do the plan's other failures. After a failed connect during the step, that NIC's connects to that endpoint back off from 100 ms, doubling to 5 s; an operation that needs a new connection there fails at once until the next trial, while open connections keep serving. Control requests keep the kernel's route: bucket checks, listing a bucket as a step's item input, and multipart abort.

## Results

- **Per-Path counts:** each step writes `<step>.nic.paths.csv`, and the run summary names the files in one line, such as `NIC paths: read1.nic.paths.csv`. See the engine's [per-path NIC evidence](../../engine/core/spt-base/README.md#per-path-nic-evidence).
- **NIC-set report:** each step writes `<step>.nic.set.json`, each engine's NICs and event loops. See the engine's [NIC-set report](../../engine/core/spt-base/README.md#nic-set-report).
- **Distributed steps:** the Entry Node keeps each node's files as `<step>.nic.paths.node-NNN.csv` and `<step>.nic.set.node-NNN.json`. `node-000` is the Entry Node, followed by the workers in `--test-hosts` order. An expected file that is absent is recorded as `missing` in `index.json`, and the run does not fail.
- **Run metadata** in `spt_run_params.json`:
  - `scenarioParams.Interfaces`: the requested entries;
  - `nicNodes`: each node with its host and the value it received;
  - `nicEngines`: per step, each engine's report, with its node, host, number of NIC-set drivers, `driverThreads` (0 for the default), `hardwareThreads`, `eventLoopsPerNic`, `nics` and `eventLoops`. A step with several NIC-set drivers, such as a `mixed` step, adds `contexts` with each driver's values, and a field the drivers disagree on is left out at the top;
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

- **Run summary:** Workload Configuration adds two lines after `Endpoints`:

  ```text
    • Interfaces         auto
    • Event loops        16 per engine (2 NICs × 8, default)
  ```

  `storage.driver.threads` replaces `default` when it is set. When engines differ, the line gives each node's value, such as `node-000 16 (2 NICs × 8, default); node-001 8 (1 NIC × 8, default)`; when an engine changed between steps, it reads `vary by step; see nicEngines in spt_run_params.json`. Without reports, only the `Interfaces` line appears.

## Replay

`spt replay` takes `--interfaces` and `SPT_INTERFACES` as `spt run` does, with the same checks; a value that fails them fails the replay as `invalid_interfaces`. Replay never keeps a source run's NIC set: an archived `storage.net.interfaces` or `storage.net.nodeInterfaces` is dropped with one warning, such as `the source run used storage.net.interfaces=auto; replay ignores it, pass --interfaces to use NICs`.
