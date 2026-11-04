# SPT core needs from the dashboard

The dashboard work keeps SPT's engine and CLI core unchanged (H-28). The items below need a change outside `cli/internal/dashboard/` before the dashboard can do them properly. Each one names what the dashboard does today and the smallest core change that would help. IDs refer to the requirements traceability list kept with the branch artifacts.

## 1. Structured log tap (H-38, H-11)

**Today:** the live run log is the SPT subprocess's console output captured by the dashboard (labelled interim in the UI). After a run, the dashboard reads `*.messages.log` and `*.errors.log` from the results folder and parses the `ts | LEVEL | …` lines. Both are text parsing.

**Needed:** a read-only way to receive SPT's info/warn/error messages as structured events while a run is active, for example a message-bus subscription or a `GET /logs?since=` endpoint on the SPT API port with level, timestamp, node, and step fields. The dashboard would then drop console scraping and keep the same UI.

## 2. Per-HTTP-status error counts (H-03, J-02)

**Today:** SPT exports successful, failed, and corrupt operation counts. The dashboard shows those. It cannot show 400/403/404/500/503/timeout separately because the counts are not exported, and Mike noted that SPT's own error count is "always zero" in some runs.

**Needed:** per-status-code (and timeout) counters in `/metrics/json`, `/metrics/cluster/json`, and the results totals, per step and per node.

## 3. Latency histogram export (J-03)

**Today:** the dashboard shows mean, max, P50, and P99 latency. It cannot draw a histogram because no bucketed latency data is exported.

**Needed:** histogram buckets (bounds plus counts, or an HDR histogram encoding) in the results folder for each step, and optionally in live metrics.

## 4. Results loading without credentials (catalog safety)

**Today:** `cli/internal/results/summary.Loader` decodes `spt_run_params.json` into `RunParams`, whose `ScenarioParams` includes the S3 access and secret key fields. The dashboard therefore does **not** reuse the loader for the catalog. It decodes `spt_run_params.json` through its own allow-list struct (`runParams` in `server/catalog.go`), so the keys are never parsed or held in memory as values. The older saved-run history (`savedRun` in `server/history.go`) still decodes the whole file into `map[string]any` and then reads named fields. It returns only non-secret fields, but the decoded map does hold the credential values in memory for the life of the request.

**Needed (either):**
- SPT stops writing credentials into `spt_run_params.json`, writing a redacted marker or an env/credential-file reference instead; or
- `summary` gains a loader option, or a separate public type, that decodes only non-secret run parameters, so the dashboard can share SPT's parsing instead of keeping a parallel allow-list.

Until then, `savedRun` should move to the same allow-list struct. This change is inside the dashboard and is in the backlog. It was not done on this branch, to keep the diff away from history code other teammates are editing.

## 5. In-process launch (H-44, open question)

**Today:** the dashboard starts runs by executing the same `spt` binary as a subprocess (`spt run write-verify …` or `spt run mock …`) and reads its status and metrics over SPT's documented HTTP API. This keeps a crashing run from taking the dashboard down and matches the 10-14 guidance that runs stay a separate process.

**Mike, 10-21:** launch through SPT's mainline functions.

**Needed to switch:** an exported Go entry point in the CLI core that runs a scenario from typed options (no argv), with a context for cancellation and a callback or channel for status and metrics. The two directions conflict, so this is left as a decision for the team and Mike; nothing was changed.

## 6. Stop-run API (H-43)

Stopping a run is teammate work (Omar, `feature/omar/gui-stop-run`). If SPT gains a documented `POST /stop` on the API port, both the dashboard and the CLI could use it instead of signalling the process.
