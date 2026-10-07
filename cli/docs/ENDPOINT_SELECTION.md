# Endpoint Selection

By default, the Netty S3 driver keeps a pool of connections to the configured
endpoints and reuses whichever connection is free. Endpoint selection replaces
that with an explicit choice of connect destination for **every S3 request
attempt**. This is useful when a test must spread load across storage nodes, or
exercise an S3 service that balances load through DNS.

Two opt-in modes are available with the default Netty S3 driver:

| Mode | Connect destination of each request attempt |
|---|---|
| `round-robin` | The next address in `--endpoints`, in order, starting with the first |
| `per-request-dns` | A fresh DNS lookup of the single `--endpoints` hostname |

Every request a workload sends follows the mode:
- ordinary object requests;
- bucket checks and setup;
- listing pages and probes;
- delete verification;
- multipart initiate, part, complete and abort;
- composite READ range requests.

Retries select again, and the previous destination remains eligible.

These modes drive QA load; they are not throughput benchmarks of the storage
system. New connections, DNS lookups and strict rotation cost throughput, so
results are not comparable with the default mode.

## Round Robin

```bash
spt run write \
  --endpoints http://10.0.0.1:9020,http://10.0.0.2:9020,http://10.0.0.3:9020 \
  --endpoint-selection round-robin \
  --endpoint-hostname s3.example.com \
  --access-key "$S3_ACCESS_KEY" --secret-key "$S3_SECRET_KEY" \
  --bucket qa-bucket --threads 16 --duration 5m
```

- **Endpoints:** must be IPv4 addresses. Each entry keeps its own port. Duplicate entries are rejected.
- **Rotation:** each driver instance on each worker rotates independently, starting with the first entry. Rotation is per request attempt; it does not follow MPU part numbers.
- **Connection reuse:** idle connections are reused per address, so a request never runs on a connection to a different address than the one selected.
- **No skipping:** an unreachable address is not skipped. Its attempts fail after the connect timeout while the other addresses keep serving, and the achieved load drops sharply.
- **`--endpoint-hostname` (optional):** sets the HTTP `Host`, the request signature and TLS SNI. Without it, `Host` is the selected `ip:port`, as in the default mode.
- **Helper Host:** bucket checks and the other helper requests are signed before a connection is chosen. Their `Host` carries the first entry's port, or, without a hostname, the first entry itself.
- **Order matters:** to alternate between nodes, list the addresses in node order.

## Per-Request DNS

```bash
spt run write \
  --endpoints https://s3.example.com:9021 \
  --endpoint-selection per-request-dns \
  --dns-server 10.0.0.53 \
  --access-key "$S3_ACCESS_KEY" --secret-key "$S3_SECRET_KEY" \
  --bucket qa-bucket --threads 16 --duration 5m
```

- **One lookup per attempt:** each request attempt resolves the hostname again, with no SPT or JVM cache, no hosts-file entries, no search domains and no sharing of concurrent lookups. The first IPv4 answer is used, and the request goes to that address on the endpoint's port.
- **One connection per attempt:** each attempt uses a new connection and sends `Connection: close`. The client waits up to one second for the server to close the connection, then closes it itself. Letting the server close first keeps TIME_WAIT sockets off the client's ephemeral ports.
- **Explicit DNS server:** `--dns-server` (`IPv4[:port]`, port 53 by default) sends every query to that server only, with no fallback.
- **Host DNS configuration:** without `--dns-server`, the name servers in the worker's `/etc/resolv.conf` are queried in order, and a warning is logged when the driver starts. A caching resolver on that path can answer from its cache, so each lookup is not guaranteed to reach the DNS load-balancing service. Inside containers the worker's resolver configuration can differ from the host's.
- **Failed lookups:** NXDOMAIN, an answer with no IPv4 address, a server failure and a timeout all fail the attempt. Existing retry settings apply. No other answer is tried.
- **Referrals:** a server that answers with only a referral to other name servers can cause those servers to be queried. Recursive resolvers and authoritative load-balancing services answer directly.

## Options

| Flag | Default | Applies to |
|---|---|---|
| `--endpoint-selection` | `default` | `default`, `round-robin`, `per-request-dns` |
| `--endpoint-hostname` | none | Round robin |
| `--dns-server` | host DNS configuration | Per-request DNS |
| `--dns-timeout` | `5s` | Per-request DNS: total deadline of one lookup |
| `--endpoint-connect-timeout` | `30s` | Both modes: TCP connect deadline |

Durations must be positive whole milliseconds. These cannot be combined with
endpoint selection, and are rejected:
- `--slice-endpoints`;
- `--s3-driver aws` or `rdma`;
- the `tables` workload;
- partial-object reads (`--range-size`);
- IPv6 addresses.

The flags are also accepted by `spt replay`.

The engine settings are `storage.net.endpoint.selection`, `.hostname`,
`.dns.server`, `.dns.timeoutMilliSec` and `.connect.timeoutMilliSec`. See the
S3 driver README for direct engine use.

## What to Expect

- **Timing:** HTTP latency metrics keep their existing meaning and exclude DNS and connection setup. Low request latency can therefore coexist with low throughput.
- **Request trace:** `op.trace.csv` records the selected `ip:port` for each operation.
- **Engine log:** each driver writes a start line and, at close, a summary. The summary has selections per address (at most 256 addresses, then `other`), new, reused, failed and closed connections, and, for per-request DNS, lookup counts, failure kinds and latency.
- **TLS:** certificate trust is unchanged: the S3 driver accepts any server certificate. Session resumption is allowed.
- **Port limits:** per-request DNS opens one connection per request. The connection rate, not only concurrency, determines socket and port pressure on the workers and the storage system.
