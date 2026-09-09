# Replay Archived Workloads

`spt replay` imports archived SPT or legacy Mongoose workload artifacts and
generates an equivalent S3 replay workload for your current environment.

Replay is designed to preserve supported workload shape and intent. It does not
guarantee a byte-for-byte or environment-identical reproduction of the original
run. Endpoints, bucket names, credentials, execution hosts, and other
environment-specific values are remapped to the target configuration you provide
when replaying.

## When to Use Replay

Use replay when you want to:

- rerun an archived workload against a current S3-compatible object storage
  target;
- compare behavior across software versions, hardware configurations, or lab
  topologies;
- inspect an archived workload before running it by using `--generate-only`;
- preserve workload structure without manually translating legacy scenario
  files.

Replay currently targets S3-compatible object storage workloads. Non-S3
protocols are reported as unsupported rather than silently converted.

Before submission, replay checks the Engine Build Information of only the
participants executing the new run. A known mismatch is rejected by default;
`--force` permits that mismatch but cannot override invalid build information
or collection failures. Source-run identity remains provenance rather than an
equality requirement. See [Engine Build Information](ENGINE_BUILD_INFO.md).

## Quick Start

Start with a generate-only preview:

```bash
spt replay \
  --from 'https://archive.example.com/results/2031/result.2031-04-05.06:07:08/' \
  --generate-only \
  --output-dir ./replay-preview
```

Review the preflight output and generated files. When the replay looks correct,
run it against your current target:

```bash
spt replay \
  --from 'https://archive.example.com/results/2031/result.2031-04-05.06:07:08/' \
  --endpoints https://s3.example.com \
  --access-key "$S3_ACCESS_KEY" \
  --secret-key "$S3_SECRET_KEY" \
  --bucket replay-bucket \
  --test-hosts worker1,worker2,worker3 \
  --auth-version 4 \
  --headless \
  --auto-terminate-seconds 300
```

For distributed replay, run `spt verify` first:

```bash
spt verify --test-hosts worker1,worker2,worker3
```

Host networking is recommended for distributed replay unless you know your
environment supports the engine's inter-node RMI traffic through another Docker
network mode.

## Source Archive Requirements

`spt replay` starts from an archive folder URL:

```bash
spt replay --from <archive-folder-url>
```

The archive folder must be reachable over HTTP or HTTPS from the machine running
`spt`. The first release expects the archive URL to be directly reachable by
`spt`. HTTP archive sources are supported, but replay reports a preflight warning
because the source is not authenticated by TLS.

The source folder should expose a directory listing or index page and contain:

- one or more `.sh` launch scripts, such as `run.sh`;
- the scenario file referenced by the selected launch script;
- any scenario-adjacent files needed by supported replay conversion.

Each fetched artifact is limited to 16 MiB. Archives with larger launch scripts
or scenario files are rejected during preflight.

SPT fetches the folder listing, scores candidate shell scripts, parses the
selected launch script, resolves the referenced scenario path, and fetches the
scenario. If no suitable launch script is found, or if multiple candidates are
ambiguous, replay fails during preflight instead of guessing.

Current releases do not require or support direct run-script or scenario-file
URLs. Future versions may support a manifest file that identifies the launch
script, scenario, and related artifacts without relying on directory-listing
discovery.

Folder URLs may contain raw timestamp separators or URL-encoded equivalents.
When in doubt, copy the URL exactly as your browser or archive system provides
it. For example, both forms are valid if the archive server accepts them:

```text
https://archive.example.com/results/2031/result.2031-04-05.06:07:08/
https://archive.example.com/results/2031/result.2031-04-05.06%3A07%3A08/
```

## How Replay Remaps Archived Workloads

Replay prepares archived workloads to run in your current environment. The
following transformations are applied when the corresponding archived values are
present and supported.

| Archived input or config | Replay behavior | Reason |
|---|---|---|
| Archived S3 endpoints or storage node addresses | Replaced by `--endpoints`, `S3_ENDPOINTS`, or `S3_ENDPOINT` | Replay targets the current S3-compatible storage system |
| Archived bucket or item-output path | Replaced by `--bucket` or `S3_BUCKET` | Avoid writing to the original environment |
| Archived access keys and secret keys | Sanitized from processed config and never reused | Credentials must come from the current environment |
| Archived client hosts | Replaced by `--test-hosts` or `HOSTS` | Execute on current worker hosts |
| Archived step IDs | Rewritten to generated replay step IDs | Avoid collisions and keep generated artifacts grouped by replay run |
| Item output and input file paths | Rewritten to generated replay step paths | Preserve read/delete dependencies on item files produced during replay |
| Archived explicit concurrency | Preserved | Maintain workload shape when the original setting is clear |
| Omitted or mixed concurrency | Uses a conservative replay default | Avoid inventing higher concurrency than the archive specified |
| Archived HTTP/HTTPS scheme | Preserved when compatible; warning when mismatched with local endpoint scheme | Prevent silent protocol changes |

## Supported Command Transformations

Replay does not execute arbitrary archived shell commands. The supported command
transformations are:

| Archived command pattern | Replay behavior |
|---|---|
| `sleep N` | Converted to a replay pause |
| Recognized `sed input > output` item-file filters | Converted to no-shell helper calls |
| Recognized `split ...; mv ...` item-file preparation | Converted to no-shell helper calls |
| Recognized `cp ...; cat ... >> ...; rm ...` item-file duplication | Converted to no-shell helper calls |
| `shuf input > output` | Converted to a no-shell helper call |
| `for i in {1..N}; do cat input >> output; done` | Converted to a generated loop using a no-shell helper call |
| Unrecognized shell commands | Rejected before launch |

The command conversion boundary is intentionally narrow. If replay cannot map a
command to one of the supported safe forms, it fails during preflight instead of
executing the archived command.

## Credentials and Sensitive Values

Archived credentials are sanitized from processed configuration files and are
never reused. Provide credentials for the current target with CLI flags or
environment variables:

```bash
spt replay \
  --from <archive-folder-url> \
  --endpoints https://s3.example.com \
  --access-key "$S3_ACCESS_KEY" \
  --secret-key "$S3_SECRET_KEY" \
  --bucket replay-bucket
```

Useful environment variables include:

- `S3_ENDPOINTS` or `S3_ENDPOINT`
- `S3_ACCESS_KEY`
- `S3_SECRET_KEY`
- `S3_BUCKET`
- `S3_AUTH_VERSION`
- `HOSTS`
- `SPT_S3_DRIVER`

See [SPT_SYNTAX.md](SPT_SYNTAX.md) for the full environment and flag reference.

## Preflight Output

Before launching, replay prints a preflight summary. Use it to confirm what will
run and how the archive was adapted.

Common sections include:

- **Source** — archive folder, selected launch script, scenario file, and
  scenario format.
- **Target** — local protocol, endpoints, bucket, credentials source, and worker
  hosts.
- **Workload** — replay steps, operations, object sizes, concurrency, and limits.
- **Path rewrites** — archived step/path labels mapped to generated replay step
  IDs.
- **Command operations** — command steps converted to safe helpers or rejected.
- **Warnings** — replay can proceed, but behavior may differ from the original
  environment.
- **Errors** — replay will not launch.
- **Generated files** — generated scenario, defaults, and metadata locations in
  generate-only mode.

Warnings are expected when an archived workload references environment-specific
details that must be remapped. Errors indicate an unsupported or unsafe case.

## Generate-Only Workflow

Use `--generate-only` before running an unfamiliar archive:

```bash
spt replay \
  --from 'https://archive.example.com/results/2031/result.2031-04-05.06:07:08/' \
  --generate-only \
  --output-dir ./replay-preview
```

In generate-only mode, SPT writes generated replay artifacts such as the
scenario, defaults, and metadata to the output directory, then exits without
launching containers.

Inspect:

- the preflight summary;
- generated step IDs and operation types;
- local endpoint, bucket, credential, and host remapping;
- command conversions;
- warnings and errors.

## Running Replay

By default, replay launches the same terminal UI used by `spt run`. Use
`--headless` for CI, automation, or non-interactive shells.

Recommended options for first runs:

```bash
spt replay \
  --from <archive-folder-url> \
  --endpoints https://s3.example.com \
  --bucket replay-bucket \
  --headless \
  --auto-terminate-seconds 300 \
  --trace-file replay-trace.log \
  --results-dir ./replay-results
```

`--auto-terminate-seconds` is useful for initial canaries of long archived
workloads. It stops the run after the specified time even if the archived
workload duration is much longer.

RDMA replay launch is not implemented yet. You may use `--s3-driver rdma` with
`--generate-only` to inspect generated replay artifacts, but launching that
replay fails before containers start.

## Distributed Replay

For distributed replay:

1. Verify the worker hosts:

   ```bash
   spt verify --test-hosts worker1,worker2,worker3
   ```

2. Launch replay with the same host list:

   ```bash
   spt replay \
     --from <archive-folder-url> \
     --endpoints https://s3.example.com \
     --bucket replay-bucket \
     --test-hosts worker1,worker2,worker3 \
     --network-mode host \
     --headless
   ```

Host networking is the default and is recommended for distributed replay because
the engine uses Java RMI between nodes. If you choose another Docker network
mode, verify that inter-node API and RMI traffic can still connect.

## Limitations

The first replay release does not support:

- non-S3 storage protocols;
- parallel scenarios;
- arbitrary shell command execution;
- remote or external tool execution;
- RDMA replay launch; `--s3-driver rdma` is limited to `--generate-only`;
- archived credential reuse;
- environment-local payload files unless they are present or mapped in the
  replay environment;
- exact reproduction of the original infrastructure or topology.

Replay preserves supported workload shape and intent. It does not clone the
original storage system, bucket, credentials, clients, or network environment.

## Troubleshooting

| Symptom | Likely cause | Action |
|---|---|---|
| Archive folder cannot be fetched | URL is unreachable from the controller, or archive access is blocked | Verify the URL with a browser or `curl` from the machine running `spt` |
| No launch script found | Folder listing does not expose a recognizable `.sh` launch script | Check the source archive requirements and folder contents |
| Ambiguous launch script | Multiple `.sh` files look like valid launch scripts | Use a cleaner archive folder or wait for future manifest support |
| Unsupported protocol | Archived workload is not S3-compatible object storage | This is outside first-release replay scope |
| Unsupported command step | Command is not one of the supported safe transformations | Inspect the preflight command section |
| Artifact exceeds max size | A fetched folder listing, launch script, or scenario is larger than 16 MiB | Reduce the source artifact size or use a smaller archive |
| HTTP/HTTPS scheme warning | Archived scenario and local endpoint use different schemes | Use a local endpoint scheme that matches the intended replay target |
| Unauthenticated HTTP warning | The archive source uses `http://` instead of `https://` | Use HTTPS when available, or confirm the source is trusted before replaying |
| Distributed launch fails | Worker host, Docker, API, or RMI connectivity issue | Run `spt verify --test-hosts ...` and use host networking unless your environment supports another mode |
| Missing payload file | Archived scenario referenced an environment-local file | Stage or map the file into the replay environment, or treat the archive as unsupported |

Use `--trace-file` to preserve replay output for debugging:

```bash
spt replay --from <archive-folder-url> --trace-file replay-trace.log
```
