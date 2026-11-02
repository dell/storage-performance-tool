import { useCallback, useEffect, useRef, useState } from "react";
import type { HistoryRun, Metrics, Row, RunMode, State } from "./types";
import { MetricCard } from "./components/MetricCard";
import { Configure } from "./components/Configure";
import { Status } from "./components/Status";
import { PerformanceLimits } from "./components/PerformanceLimits";
import { MeasuredDetails, PhaseResults } from "./components/RunMetrics";
import { Charts } from "./components/RunCharts";
import { RunHistory } from "./components/RunHistory";
import { ConnectRun } from "./components/ConnectRun";
import { Catalog } from "./components/Catalog";
import { Compare } from "./components/Compare";
import { NodeTable } from "./components/NodeTable";
import { RunLog } from "./components/RunLog";
import { runWorkloadType, workloadLabel } from "./utils/catalog";
import {
  elapsed,
  formatNumber as fmt,
  numeric,
  phaseCode,
  phaseName,
  utc,
} from "./utils/metrics";
import { fallbackLimits, readJSON } from "./utils/validation";

const fields = [
  {
    key: "cards",
    title: "Performance cards",
    detail: "IOPS, bandwidth, latency, and failures",
  },
  {
    key: "charts",
    title: "Measured trends",
    detail: "Three graphs with a shared time window",
  },
  {
    key: "phases",
    title: "Phase results",
    detail: "Write, verify, and cleanup outcomes",
  },
  {
    key: "limits",
    title: "Performance limits",
    detail: "Thresholds saved for this run profile",
  },
  {
    key: "details",
    title: "SPT details",
    detail: "Timing, counters, concurrency, and source rows",
  },
  {
    key: "record",
    title: "Run record",
    detail: "Settings, timestamps, prefix, and exit code",
  },
];
const expertViewSelectionKey = "spt-expert-visible-sections-v1";
const allSectionKeys = () => fields.map((field) => field.key);
function loadVisibleSections(): string[] {
  try {
    const stored: unknown = JSON.parse(
      localStorage.getItem(expertViewSelectionKey) ?? "null",
    );
    if (!Array.isArray(stored)) return allSectionKeys();
    return allSectionKeys().filter((key) => stored.includes(key));
  } catch {
    return allSectionKeys();
  }
}

function useServer() {
  const [state, setState] = useState<State | null>(null),
    [error, setError] = useState(""),
    [received, setReceived] = useState("");
  const refresh = useCallback(async (signal?: AbortSignal) => {
    try {
      if (location.protocol === "file:")
        throw new Error(
          "Open the dashboard through its local server to load SPT results.",
        );
      const r = await fetch("/api/state", { cache: "no-store", signal });
      if (!(r.headers.get("content-type") ?? "").includes("application/json"))
        throw new Error(
          "The dashboard server is unavailable. Open the app through the dashboard server or SSH tunnel.",
        );
      const s = await r.json();
      if (!r.ok)
        throw new Error(s.error ?? `Could not read SPT state (${r.status}).`);
      if (!s.target || !("job" in s) || !("metrics" in s))
        throw new Error(
          "The server returned an unsupported dashboard response.",
        );
      setState(s);
      setError("");
      setReceived(new Date().toISOString());
    } catch (e) {
      if (!signal?.aborted)
        setError(e instanceof Error ? e.message : "Could not read SPT state.");
    }
  }, []);
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    const poll = async () => {
      await refresh(controller.signal);
      if (!controller.signal.aborted) timer = setTimeout(poll, 2000);
    };
    void poll();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [refresh]);
  return { state, error, received, refresh };
}

export default function App() {
  const { state, error, received, refresh } = useServer();
  const [page, setPage] = useState<
    "dashboard" | "configure" | "history" | "connect" | "catalog" | "compare"
  >("configure");
  const [compareIDs, setCompareIDs] = useState<string[]>([]),
    [compareBaseline, setCompareBaseline] = useState("");
  const landed = useRef(false);
  // Open on the live view only when a run is already in progress at first load.
  useEffect(() => {
    if (!state || landed.current) return;
    landed.current = true;
    if (state.job?.status === "running") setPage("dashboard");
  }, [state]);
  const clients = state?.storage?.hosts.length ?? 1;
  const [mode, setMode] = useState<RunMode>("guided");
  const [selection, setSelection] = useState<string[]>(loadVisibleSections);
  const [customizing, setCustomizing] = useState(false);
  // View preferences are browser-only. They do not affect the SPT run or API.
  useEffect(() => {
    try {
      localStorage.setItem(expertViewSelectionKey, JSON.stringify(selection));
    } catch {
      // The dashboard still works if browser storage is unavailable.
    }
  }, [selection]);
  const [historyRuns, setHistoryRuns] = useState<HistoryRun[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState("");
  const [historyState, setHistoryState] = useState<State | null>(null);
  const [historyVersion, setHistoryVersion] = useState(0);

  useEffect(() => {
    if (page !== "history") return;
    const controller = new AbortController();
    setHistoryLoading(true);
    setHistoryError("");
    fetch("/api/runs", { cache: "no-store", signal: controller.signal })
      .then(async (response) => {
        const body = await response.json();
        if (!response.ok)
          throw new Error(body.error ?? "Could not load run history.");
        setHistoryRuns(Array.isArray(body.runs) ? body.runs : []);
      })
      .catch((reason) => {
        if (!controller.signal.aborted)
          setHistoryError(
            reason instanceof Error
              ? reason.message
              : "Could not load run history.",
          );
      })
      .finally(() => {
        if (!controller.signal.aborted) setHistoryLoading(false);
      });
    return () => controller.abort();
  }, [page, historyVersion]);

  const openSavedRun = async (runID: string) => {
    setHistoryError("");
    try {
      const response = await fetch(`/api/runs/${encodeURIComponent(runID)}`, {
        cache: "no-store",
      });
      const detail = await response.json();
      if (!response.ok)
        throw new Error(detail.error ?? "Could not open saved run.");
      setHistoryState(detail as State);
      setCustomizing(false);
      setPage("dashboard");
    } catch (reason) {
      setHistoryError(
        reason instanceof Error ? reason.message : "Could not open saved run.",
      );
    }
  };

  const showDashboard = () => {
    setHistoryState(null);
    setCustomizing(false);
    setPage("dashboard");
  };

  return (
    <div className="app">
      <aside className="sidebar">
        <a className="brand" href="#dashboard" onClick={showDashboard}>
          <span>SPT</span>
          <div>
            {state?.branding?.product_name ?? "Storage Performance Tool"}
            <small>
              {(
                state?.branding?.environment_label ?? "BENCHMARK CONSOLE"
              ).toUpperCase()}
            </small>
          </div>
        </a>
        <div className="sidebar-label">WORKSPACE</div>
        <nav aria-label="Main navigation">
          <button
            className={page === "configure" ? "selected" : ""}
            aria-current={page === "configure" ? "page" : undefined}
            onClick={() => {
              setHistoryState(null);
              setPage("configure");
            }}
          >
            ＋ <span>Configure run</span>
          </button>
          <button
            className={page === "dashboard" ? "selected" : ""}
            aria-current={page === "dashboard" ? "page" : undefined}
            onClick={showDashboard}
          >
            ◫ <span>Live dashboard</span>
          </button>
          <button
            className={page === "history" ? "selected" : ""}
            aria-current={page === "history" ? "page" : undefined}
            onClick={() => {
              setHistoryState(null);
              setPage("history");
            }}
          >
            ◷ <span>Run history</span>
          </button>
          <button
            className={page === "catalog" ? "selected" : ""}
            aria-current={page === "catalog" ? "page" : undefined}
            onClick={() => setPage("catalog")}
          >
            ☰ <span>Results catalog</span>
          </button>
          <button
            className={page === "compare" ? "selected" : ""}
            aria-current={page === "compare" ? "page" : undefined}
            onClick={() => setPage("compare")}
          >
            ⇄ <span>Compare runs</span>
          </button>
          <button
            className={page === "connect" ? "selected" : ""}
            aria-current={page === "connect" ? "page" : undefined}
            onClick={() => setPage("connect")}
          >
            ↗ <span>Connect to active run</span>
          </button>
        </nav>
        <div className="sidebar-target">
          <span className="eyebrow">STORAGE TARGET</span>
          <strong>{state?.target.endpoint_label || "Awaiting server"}</strong>
          <p>{state?.target.bucket || "No bucket reported"}</p>
          {state && (
            <Status status={state.target.ready ? "ready" : "needs setup"} />
          )}
        </div>
        <div className="sidebar-footer">
          <span
            className={`source-dot ${state && !error ? "connected" : ""}`}
          />
          {error
            ? "Connection unavailable"
            : state
              ? "SPT server connected"
              : "Connecting to server"}
          <p>
            {clients} client machine{clients === 1 ? "" : "s"} ·{" "}
            {workloadLabel(runWorkloadType(state?.metrics, state?.job))}
          </p>
        </div>
      </aside>
      <div className="workspace">
        <header className="topbar">
          <span>Benchmark workspace</span>
          <div className="topbar-actions">
            <span className={`source-badge ${error ? "offline" : ""}`}>
              {error
                ? "OFFLINE"
                : state?.connection?.mode === "attached"
                  ? "ATTACHED · READ ONLY"
                  : state
                    ? "SPT API"
                    : "CONNECTING"}
            </span>
            <div className="mode-switch" role="group" aria-label="Display mode">
              {(["guided", "expert"] as const).map((m) => (
                <button
                  key={m}
                  className={mode === m ? "active" : ""}
                  aria-pressed={mode === m}
                  onClick={() => {
                    setMode(m);
                    setCustomizing(false);
                    // On Configure, the mode switches the workload form in place.
                    if (page !== "configure") showDashboard();
                  }}
                >
                  {m === "guided" ? "Guided" : "Expert"}
                </button>
              ))}
            </div>
          </div>
        </header>
        <main>
          {error && (
            <div className="error" role="alert">
              <strong>Connection unavailable. </strong>
              {error}
              {received && (
                <p>
                  Showing the last received state from {utc(received)}. Run
                  controls are disabled until the connection returns.
                </p>
              )}
            </div>
          )}
          {page === "connect" ? (
            <ConnectRun
              state={state}
              offline={!!error}
              onConnected={async () => {
                await refresh();
                setPage("dashboard");
              }}
            />
          ) : page === "catalog" ? (
            <Catalog
              onCompare={(ids, baseline) => {
                setCompareIDs(ids);
                setCompareBaseline(baseline);
                setPage("compare");
              }}
            />
          ) : page === "compare" ? (
            <Compare
              key={compareIDs.join(",") + compareBaseline}
              ids={compareIDs}
              baseline={compareBaseline}
              onBack={() => setPage("catalog")}
            />
          ) : page === "history" ? (
            <RunHistory
              runs={historyRuns}
              loading={historyLoading}
              error={historyError}
              onOpen={openSavedRun}
              onRefresh={() => setHistoryVersion((version) => version + 1)}
            />
          ) : page === "configure" ? (
            <Configure
              state={state}
              mode={mode}
              offline={!!error}
              onRefresh={refresh}
              onStarted={async () => {
                await refresh();
                showDashboard();
              }}
            />
          ) : (
            <>
              {mode === "expert" && (
                <div className="expert-edit">
                  <button type="button" onClick={() => setCustomizing(true)}>
                    Customize view
                  </button>
                </div>
              )}
              <Dashboard
                key={
                  historyState?.job?.run_id ??
                  state?.metrics?.run_id ??
                  state?.job?.run_id ??
                  "empty"
                }
                state={historyState ?? state}
                historical={!!historyState}
                onBackHistory={() => setPage("history")}
                selected={
                  mode === "guided" ? fields.map((f) => f.key) : selection
                }
                onConfigure={() => setPage("configure")}
              />
              {mode === "expert" && customizing && (
                <div
                  className="expert-view-overlay"
                  onKeyDown={(event) => {
                    if (event.key === "Escape") setCustomizing(false);
                  }}
                >
                  <button
                    className="expert-view-backdrop"
                    type="button"
                    aria-label="Close dashboard customization"
                    onClick={() => setCustomizing(false)}
                  />
                  <aside
                    className="expert-view-drawer"
                    role="dialog"
                    aria-modal="true"
                    aria-labelledby="expert-view-drawer-title"
                  >
                    <div className="expert-view-drawer-heading">
                      <div>
                        <span className="eyebrow">EXPERT VIEW</span>
                        <h2 id="expert-view-drawer-title">
                          Customize dashboard
                        </h2>
                      </div>
                      <button
                        type="button"
                        autoFocus
                        aria-label="Close customization"
                        onClick={() => setCustomizing(false)}
                      >
                        ✕
                      </button>
                    </div>
                    <p>
                      Show or hide sections. This only changes your display, not
                      the benchmark or its data.
                    </p>
                    <div className="expert-view-drawer-actions">
                      <button
                        type="button"
                        onClick={() => setSelection(allSectionKeys())}
                      >
                        Show all
                      </button>
                      <button type="button" onClick={() => setSelection([])}>
                        Hide all
                      </button>
                    </div>
                    <div className="expert-view-drawer-list">
                      {fields.map((field) => (
                        <label className="check-label" key={field.key}>
                          <input
                            type="checkbox"
                            checked={selection.includes(field.key)}
                            onChange={(event) =>
                              setSelection((previous) =>
                                event.target.checked
                                  ? [...previous, field.key]
                                  : previous.filter((key) => key !== field.key),
                              )
                            }
                          />
                          <span>
                            <strong>{field.title}</strong>
                            <small>{field.detail}</small>
                          </span>
                        </label>
                      ))}
                    </div>
                    <div className="expert-view-drawer-footer">
                      <span>
                        {selection.length} of {fields.length} sections shown
                      </span>
                      <button
                        className="primary"
                        type="button"
                        onClick={() => setCustomizing(false)}
                      >
                        Done
                      </button>
                    </div>
                  </aside>
                </div>
              )}
            </>
          )}
        </main>
        <footer>
          {state?.branding?.product_name ?? "Storage Performance Tool"} ·{" "}
          {received
            ? `State received ${utc(received)}`
            : "Waiting for server state"}{" "}
          · Refreshes every 2 seconds
        </footer>
      </div>
    </div>
  );
}

function Dashboard({
  state,
  selected,
  onConfigure,
  historical = false,
  onBackHistory,
}: {
  state: State | null;
  selected: string[];
  onConfigure: () => void;
  historical?: boolean;
  onBackHistory?: () => void;
}) {
  const m = state?.metrics,
    job = state?.job;
  const summaries = m?.phase_summaries ?? [];
  const history =
    m?.chart_samples ?? (m?.is_aggregate ? [] : (m?.samples ?? []));
  const available: string[] = [
    ...new Set([
      ...summaries.map((r) => phaseCode(r.phase)),
      ...history.map((r) => phaseCode(r.op_type)),
      ...(m?.samples ?? []).map((r) => phaseCode(r.op_type)),
    ]),
  ];
  const [phase, setPhase] = useState<string>(
    phaseCode(m?.is_active ? m.phase : "CREATE"),
  );
  const selectedPhase = available.includes(phase)
    ? phase
    : (available[0] ?? "CREATE");
  const final = summaries.find((r) => phaseCode(r.phase) === selectedPhase);
  const latest = [...(m?.samples ?? []), ...history]
    .filter((r) => phaseCode(r.op_type) === selectedPhase)
    .sort(
      (a, b) => Date.parse(a.timestamp ?? "") - Date.parse(b.timestamp ?? ""),
    )
    .at(-1);
  const row: Row | undefined =
    !m?.is_active && final
      ? {
          ...final,
          interval_iops: final.iops,
          cumulative_ops: final.successful_ops,
          count_failed: final.failed_ops,
        }
      : latest;
  const progress =
    numeric(row?.completion_percent) ??
    (m?.is_active && phaseCode(m.phase) === selectedPhase
      ? numeric(m.progress_percent)
      : null);
  const failureCount = numeric(m?.total_failed) ?? numeric(row?.count_failed);
  const outcome =
    job?.status === "failed"
      ? "Run failed"
      : job?.status === "interrupted"
        ? "Run interrupted"
        : job?.status === "unknown"
          ? "Run outcome needs review"
          : failureCount !== null && failureCount > 0
            ? "Failed operations reported"
            : job?.status === "running"
              ? "Benchmark in progress"
              : job?.status === "completed"
                ? "Run completed"
                : m
                  ? "Recorded SPT results"
                  : "Ready for your first benchmark";
  const has = (key: string) => selected.includes(key);
  return (
    <>
      <div className="page-heading">
        <div>
          <span className="eyebrow">
            {job?.status === "running" ? "LIVE BENCHMARK" : "BENCHMARK RESULTS"}
          </span>
          <h1>
            {job?.status === "running"
              ? "Run in progress"
              : "Storage performance"}
          </h1>
          <p>
            {job?.run_id ||
              m?.run_id ||
              "Configure a run to measure your storage target."}
          </p>
        </div>
        <div className="actions">
          {historical && (
            <button onClick={onBackHistory}>Back to run history</button>
          )}
          {job?.status === "completed" && summaries.length > 0 && m && (
            <a
              className="button-link"
              href={`/api/runs/${encodeURIComponent(m.run_id)}/csv?kind=summary`}
              download
            >
              Download CSV report
            </a>
          )}
          <button className="primary" onClick={onConfigure}>
            ＋ Configure run
          </button>
        </div>
      </div>
      {!job && !m ? (
        <section className="panel empty-state">
          <div className="empty-icon">◫</div>
          <h2>
            {state
              ? "No benchmark results yet"
              : "Waiting for dashboard server"}
          </h2>
          <p>
            Configure a write and verify benchmark. Measured results will appear
            here as SPT runs.
          </p>
          <button className="primary" onClick={onConfigure}>
            Configure a run
          </button>
        </section>
      ) : (
        <>
          <section className="run-strip" aria-label="Run status">
            <div>
              <span>Status</span>
              <Status status={job?.status ?? "recorded"} />
            </div>
            <div>
              <span>Elapsed</span>
              <strong>{elapsed(job?.elapsed_seconds)}</strong>
            </div>
            <div>
              <span>Current phase</span>
              <strong>{job?.phase || phaseName(m?.phase)}</strong>
            </div>
            <div>
              <span>Workload</span>
              <strong>{workloadLabel(runWorkloadType(m, job))}</strong>
            </div>
            <div className="progress">
              <div>
                <span>Reported phase progress</span>
                <strong>
                  {progress === null ? "Not reported" : `${fmt(progress, 1)}%`}
                </strong>
              </div>
              {progress !== null && (
                <progress
                  max="100"
                  value={Math.max(0, Math.min(100, progress))}
                  aria-label="Reported phase progress"
                />
              )}
            </div>
          </section>
          <section className="panel outcome">
            <div>
              <span className="eyebrow">RUN OUTCOME</span>
              <h2>{outcome}</h2>
              <p>
                {job?.message ||
                  "SPT measurements from the configured storage target."}
              </p>
            </div>
            <div className="outcome-facts">
              <span>
                Failed operations<strong>{fmt(failureCount)}</strong>
                <small>
                  {numeric(m?.total_failed) !== null
                    ? "Across all recorded phases"
                    : `In ${phaseName(selectedPhase).toLowerCase()} phase`}
                </small>
              </span>
              <span>
                History captured<strong>{history.length} samples</strong>
                <small>Reported interval measurements</small>
              </span>
            </div>
          </section>
          {m && (
            <>
              <div className="scope-bar">
                <label>
                  Inspect phase
                  <select
                    value={selectedPhase}
                    onChange={(e) => setPhase(e.target.value)}
                  >
                    {available.map((p) => (
                      <option key={p} value={p}>
                        {phaseName(p)}
                      </option>
                    ))}
                  </select>
                </label>
                <p>
                  {!m.is_active && final
                    ? "Final phase averages"
                    : "Latest measured phase snapshot"}{" "}
                  · Missing values appear as —
                </p>
              </div>
              {has("cards") && (
                <div className="metrics-grid">
                  <MetricCard
                    label={`${phaseName(selectedPhase)} IOPS`}
                    value={fmt(row?.interval_iops, 2)}
                    unit="ops/s"
                    detail={
                      !m.is_active && final
                        ? "Average successful operations per second"
                        : "Latest reported rate of successful operations"
                    }
                  />
                  <MetricCard
                    label="Bandwidth"
                    value={fmt(row?.bandwidth_mib_s, 2)}
                    unit="MiB/s"
                    detail="Data transferred per second"
                    accent="teal"
                  />
                  <MetricCard
                    label="P50 latency"
                    value={fmt(row?.p50_latency_us, 1)}
                    unit="µs"
                    detail="Half of measured operations were faster"
                    accent="purple"
                  />
                  <MetricCard
                    label="Successful operations"
                    value={fmt(row?.cumulative_ops)}
                    detail={`In ${phaseName(selectedPhase).toLowerCase()} phase · ${fmt(row?.count_failed)} failed`}
                    accent="orange"
                  />
                </div>
              )}
              {has("limits") && (
                <PerformanceLimits
                  state={state!}
                  row={row}
                  phase={selectedPhase}
                />
              )}
              {has("charts") && (
                <Charts
                  key={selectedPhase}
                  metrics={m}
                  phase={selectedPhase}
                  startedAt={job?.started_at}
                />
              )}
              {has("phases") && (
                <PhaseResults metrics={m} onSelect={setPhase} />
              )}
              {has("details") && (
                <MeasuredDetails row={row} phase={selectedPhase} metrics={m} />
              )}
              {!historical && m?.nodes && <NodeTable nodes={m.nodes} />}
            </>
          )}
          {job?.run_id && (
            <RunLog
              key={job.run_id}
              base={`/api/runs/${encodeURIComponent(job.run_id)}/log`}
              sources={["console", "results"]}
              title="Errors and logs"
              collapsed
              refreshMs={job.status === "running" ? 3000 : 0}
            />
          )}
          {has("record") && (
            <details className="panel">
              <summary>Run record and settings</summary>
              <div className="detail-strip">
                <span>Started: {utc(job?.started_at)}</span>
                <span>Finished: {utc(job?.finished_at)}</span>
                <span>Exit code: {fmt(job?.returncode)}</span>
              </div>
              <dl className="record-grid">
                <div>
                  <dt>Objects</dt>
                  <dd>{fmt(job?.options?.object_count)}</dd>
                </div>
                <div>
                  <dt>Object size</dt>
                  <dd>{job?.options?.object_size ?? "—"}</dd>
                </div>
                <div>
                  <dt>Threads</dt>
                  <dd>{fmt(job?.options?.threads)}</dd>
                </div>
                <div>
                  <dt>Safety time limit</dt>
                  <dd>{elapsed(job?.options?.max_runtime_seconds)}</dd>
                </div>
                <div>
                  <dt>Run prefix</dt>
                  <dd>{job?.prefix ?? "—"}</dd>
                </div>
                <div>
                  <dt>Source</dt>
                  <dd>{m?.source_file ?? "—"}</dd>
                </div>
              </dl>
            </details>
          )}
        </>
      )}
    </>
  );
}
