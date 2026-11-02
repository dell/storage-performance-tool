# SPT Dashboard

A small Go web server with an embedded React dashboard for running a bounded SPT write/verify workload and reviewing its metrics.

## Build

Requirements: Go 1.25+, Node.js 22.6+ (22.22.2+ for the Vitest suite, because jsdom 30 requires it), and npm.

```sh
cd cli/internal/dashboard/frontend
npm ci
npm run typecheck
npm test
npm run build
cd ../../../
go test ./internal/dashboard/...
go build .
```

The frontend build writes the self-contained page to `cli/internal/dashboard/server/index.html`, which the Go server embeds at compile time. Rebuild SPT after changing the UI.

## Run

From the SPT CLI:

```sh
spt dashboard
```

This starts the dashboard, prints its local URL, and opens a browser. Keep the terminal open while using the dashboard. Closing the browser leaves the server and any active run running. Ctrl+C stops the server and interrupts an active benchmark.

The dashboard starts its own headless `write-verify` subprocess (`spt run mock` with the mock driver) using the same SPT executable that launched it. Run `spt dashboard --help` for command options. `--config path/to/dashboard.json` selects deployment branding and workload limits; `DASHBOARD_CONFIG` provides the default.

CLI mode reads `.env` from the directory where you launch SPT and from your home directory, with environment variables taking precedence. State is stored under your user configuration directory in `spt/dashboard` (`~/.config/spt/dashboard` on Linux). Results remain in `~/results`.

The server listens only on localhost. `--port` overrides `DASHBOARD_PORT`; the default is 8000. Set the port to `0` to choose a free port automatically:

```sh
spt dashboard --port 9000
```

On a server without a desktop, skip opening the browser:

```sh
spt dashboard --no-browser
```

For SSH access, forward the dashboard port from your local machine:

```sh
ssh -L 8000:127.0.0.1:8000 user@server
```

Run `spt dashboard --no-browser` on the server, then open `http://localhost:8000` in your local browser. If you choose another dashboard port, update the forwarding command to match. If browser opening fails during a local run, use the printed URL.

To choose the browser, set `BROWSER` (the first entry of a colon-separated list is run directly, with the URL as its argument, or in place of `%s`). `--no-browser` takes precedence; without it, `BROWSER=true` also opens nothing.

The server listens on `127.0.0.1:8000` by default. For remote access, use SSH port forwarding and keep the server bound to loopback. SPT's storage target and credentials must be configured on the host; the dashboard does not accept credentials from the browser.

The dashboard enforces bounded workload choices and allows one dashboard-managed run at a time. Run results are stored on the host in SPT's results directory.

## Monitor an existing SPT run

Use **Connect to active run** to monitor a run started separately with the SPT CLI. Choose its entry machine from the client machines saved under Storage configuration. The dashboard reads the documented SPT `GET /status` and `GET /metrics/json` APIs on port `9999`, refreshes them while attached, and keeps a bounded sample history for the charts. The connection is read only: it does not launch or stop an SPT process. If SPT is not running or its API cannot be reached, the page shows the endpoint error and offers a retry. Disconnect returns the dashboard to its local state.

The entry machine must already be listed in Storage configuration, which limits API requests to the selected SPT hosts. For a local run, use `127.0.0.1` as the client machine; for a remote VM, choose the configured entry host and keep dashboard access on the SSH tunnel.

## Run modes

The Guided/Expert switch in the top bar selects how the workload step is configured:

| Setting | Guided (presets) | Expert (any value in range) |
|---|---|---|
| Objects | 10, 25, 50, 100, 250, 320 | 1–100,000 |
| Object size | 64KiB, 256KiB, 1MiB, 4MiB, 16MiB | 1KiB–1GiB, whole KiB/MiB/GiB |
| Threads | 1, 2 | 1–64 |
| Runtime limit | 60, 120, 300, 600 s | 10–3600 s |
| Total test data | — | at most 100 GiB (objects × size) |

Defaults preserve the reviewed Capstone VM profile. Deployment-specific limits can be overridden with a JSON settings file; the server publishes the effective values in `/api/state` and validates every run request against them. Object sizes must match `<number><KiB|MiB|GiB>` exactly, so a value can never be passed to SPT as an option.

The settings file supports `branding.product_name`, `branding.environment_label`, and a `limits` object. The `limits.guided` object accepts arrays named `object_counts`, `object_sizes`, `threads`, and `max_runtime_seconds`. `limits.expert` accepts `object_count_min`, `object_count_max`, `object_size_bytes_min`, `object_size_bytes_max`, `threads_min`, `threads_max`, `max_runtime_seconds_min`, `max_runtime_seconds_max`, and `max_total_bytes`. Any omitted fields keep their defaults.

## Storage configuration

The app opens on **Configure run**, which has two steps. Workload settings stay locked until the storage step has been verified, and the server refuses to start a run without a passing verification.

- **Client machines**: the machines that generate load, entered as `host` or `user@host` (up to 8; `127.0.0.1` is the dashboard host). They are passed to SPT as `--test-hosts`, and the first one is the entry node. Each remote machine needs Docker and key-based SSH access from the dashboard host; passwords are not supported.
- **S3 endpoint** (optional): overrides the endpoint from `.env`. HTTP and HTTPS URLs are supported; use HTTPS outside a trusted private network. The bucket and access keys always come from `.env`.
- **Verify connection** saves these settings, calls SPT's typed verification API for the client machines, and checks that the endpoint answers HTTP(S) from the dashboard host. The endpoint check proves reachability only, not credentials or bucket access.

Changing the client machines or the endpoint requires verifying again. Verification and runs never overlap, because both start SPT containers on the same ports. Settings and the last job are kept in the dashboard state directory under the user configuration location.

## Storage driver: S3 write + verify or mock

Step 2 of **Configure run** offers two drivers:

- **S3 write + verify** (default) runs SPT's write/verify workload against the configured S3 target. It needs a passing storage verification, including the endpoint check, and the bucket and credentials from `.env`.
- **Mock driver** runs `spt run mock` (SPT's `dummy-mock` storage driver). No S3 endpoint, bucket, or credentials are used or passed to SPT, so only the client machines must pass verification. Use it to check the client machines, the dashboard, and the charts on a VM without touching storage. Mock results describe the mock driver, not storage performance; the UI labels them **Mock driver** everywhere.

`/api/state` reports `target.mock_ready` and `target.mock_reasons` next to the S3 `ready`/`reasons`. `POST /api/runs` accepts `"driver": "mock"`; omitting `driver` keeps the S3 workload, so existing clients are unaffected.

With more than one client machine, the live dashboard also shows a **per-machine** table read from each machine's own `/metrics/json`. These rows are shown next to the fleet totals and are never summed with them.

## Results catalog

**Results catalog** lists every SPT run found under the results root, both dashboard runs and runs started with the SPT CLI. The default root is `~/results`; set `DASHBOARD_RESULTS_ROOT` to scan another directory. The catalog is read only: it never changes, moves, or deletes results.

- A folder is a run when it holds SPT's `spt_run_params.json` or a results totals file. Folders are scanned at most 3 levels deep (symlinks and hidden folders are skipped); up to 1,000 runs are listed.
- Malformed or partial folders are listed with what is missing instead of being hidden. A folder without a recorded start time sorts by its modification time.
- Search matches the run name, label, size, and endpoint host. Filter by source (dashboard or CLI), workload, and status.
- **Details** shows the recorded settings, per-phase totals, integrity, and the run's engine logs.
- Credentials are never read into the catalog. `spt_run_params.json` is decoded through an allow-list of non-secret fields, so access and secret keys are not parsed, stored, or returned.

## Compare runs and regression flags

Select 2–6 runs in the catalog, choose a baseline, and choose **Compare**. The compare view shows:

- overlaid per-phase time series (IOPS, bandwidth, P50 and P99 latency) on a shared "seconds since the phase started" axis;
- a delta table per phase and metric against the baseline;
- a flag per delta: **Regressed**, **Improved**, **Within threshold**, **Insufficient data**, **No data**, or **Not comparable**.

The flag rule is a **threshold check, not statistical significance**. A metric is flagged when it is worse than the baseline by more than the threshold (default 10%). Latency changes under the latency floor (default 500 µs) and phases with fewer operations than the minimum (default 10) are not flagged. Any corrupt object is always flagged, and more failed operations than the baseline is flagged as a regression. Runs whose settings differ from the baseline (workload, object size, object count, threads, client machine count, or S3 driver) are marked **Not comparable**, and the difference is listed. One run per side cannot show variance, so repeat runs before drawing conclusions.

The three numbers can be changed in the compare view for one comparison, or as deployment defaults in the settings file:

```json
{ "compare": { "threshold_pct": 10, "latency_floor_us": 500, "min_ops": 10 } }
```

## Run logs

The live dashboard has a collapsed **Errors and logs** panel, and catalog **Details** shows a run's logs. Both filter by level (All, WARN+, ERROR), show at most the last 200 matching lines (up to 2,000 via the API), and name the files they read. While a run is active the live view shows the dashboard-captured console output; afterwards it reads SPT's `*.messages.log` and `*.errors.log` from the results folder. Values that look like credentials are redacted before they leave the server.

## HTTP API

All endpoints answer only on loopback with a loopback `Host` header; any other host gets `403`.

| Method and path | Purpose |
|---|---|
| `GET /api/state` | Dashboard state, limits, target readiness (S3 and mock), job, live metrics |
| `POST /api/storage/verify` | Save and verify client machines and the optional endpoint |
| `POST /api/runs` | Start a run; body is the workload options plus optional `"driver": "mock"` |
| `GET /api/runs`, `GET /api/runs/{id}`, `GET /api/runs/{id}/csv` | Dashboard run history and CSV export |
| `GET /api/runs/{id}/log?level=all\|warn\|error&tail=N&source=console\|results` | Logs for a dashboard run |
| `POST /api/attach`, `DELETE /api/attach` | Read-only monitoring of a CLI run |
| `GET /api/catalog?q=&source=&workload=&status=` | Results catalog |
| `GET /api/catalog/run?id=` | One catalog run |
| `GET /api/catalog/compare?id=A&id=B[&id=…]&baseline=A&threshold_pct=&latency_floor_us=&min_ops=` | Compare 2–6 runs |
| `GET /api/catalog/log?id=&level=&tail=` | Engine logs for a catalog run |

Catalog IDs are paths relative to the results root, for example `cli-baseline/base-20261020.100000.000`; URL-encode the `/`.

## Tests

```sh
cd cli
go test -race ./internal/dashboard/...
cd internal/dashboard/frontend
npm run typecheck && npm test
```

The Go tests use fixture result folders under `server/testdata/catalog/results` (baseline, regressed, corrupt, partial, malformed, different-thread runs) and a fake SPT executable for the mock driver, so they need neither S3 nor Docker. The Vitest suite covers the catalog/compare helpers, driver readiness, and the catalog, compare, and log components.
