import { useCallback, useEffect, useState } from "react";
import type { HistoryRun, Row, RunMode, State } from "./types";
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
import { RunWarnings } from "./components/RunWarnings";
import { RunLog } from "./components/RunLog";
import { runWorkloadType, workloadLabel } from "./utils/catalog";
import { readJSON } from "./utils/validation";
import {
  elapsed,
  formatMetric,
  formatNumber as fmt,
  numeric,
  phaseCode,
  phaseName,
  utc,
} from "./utils/metrics";

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
  const [landed, setLanded] = useState(false);
  // Open on the live view only when a run is already in progress at first load.
  if (state && !landed) {
    setLanded(true);
    if (state.job?.status === "running") setPage("dashboard");
  }
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

  const showHistory = () => {
    if (page !== "history") {
      setHistoryLoading(true);
      setHistoryError("");
    }
    setPage("history");
  };

  const refreshHistory = () => {
    setHistoryLoading(true);
    setHistoryError("");
    setHistoryVersion((version) => version + 1);
  };

  return (
    <div data-spt-legacy="" className="app">
      <aside data-spt-legacy="" className="sidebar">
        <a
          data-spt-legacy=""
          className="brand"
          href="#dashboard"
          onClick={showDashboard}
        >
          <span data-spt-legacy="">SPT</span>
          <div data-spt-legacy="">
            {state?.branding?.product_name ?? "Storage Performance Tool"}
            <small data-spt-legacy="">
              {(
                state?.branding?.environment_label ?? "BENCHMARK CONSOLE"
              ).toUpperCase()}
            </small>
          </div>
        </a>
        <div data-spt-legacy="" className="sidebar-label">
          WORKSPACE
        </div>
        <nav data-spt-legacy="" aria-label="Main navigation">
          <button
            data-spt-legacy=""
            className={page === "configure" ? "selected" : ""}
            aria-current={page === "configure" ? "page" : undefined}
            onClick={() => {
              setHistoryState(null);
              setPage("configure");
            }}
          >
            ＋ <span data-spt-legacy="">Configure run</span>
          </button>
          <button
            data-spt-legacy=""
            className={page === "dashboard" ? "selected" : ""}
            aria-current={page === "dashboard" ? "page" : undefined}
            onClick={showDashboard}
          >
            ◫ <span data-spt-legacy="">Live dashboard</span>
          </button>
          <button
            data-spt-legacy=""
            className={page === "history" ? "selected" : ""}
            aria-current={page === "history" ? "page" : undefined}
            onClick={() => {
              setHistoryState(null);
              showHistory();
            }}
          >
            ◷ <span data-spt-legacy="">Run history</span>
          </button>
          <button
            data-spt-legacy=""
            className={page === "catalog" ? "selected" : ""}
            aria-current={page === "catalog" ? "page" : undefined}
            onClick={() => setPage("catalog")}
          >
            ☰ <span data-spt-legacy="">Results catalog</span>
          </button>
          <button
            data-spt-legacy=""
            className={page === "compare" ? "selected" : ""}
            aria-current={page === "compare" ? "page" : undefined}
            onClick={() => setPage("compare")}
          >
            ⇄ <span data-spt-legacy="">Compare runs</span>
          </button>
          <button
            data-spt-legacy=""
            className={page === "connect" ? "selected" : ""}
            aria-current={page === "connect" ? "page" : undefined}
            onClick={() => setPage("connect")}
          >
            ↗ <span data-spt-legacy="">Connect to active run</span>
          </button>
        </nav>
        <div data-spt-legacy="" className="sidebar-target">
          <span data-spt-legacy="" className="eyebrow">
            STORAGE TARGET
          </span>
          <strong data-spt-legacy="">
            {state?.target.endpoint_label || "Awaiting server"}
          </strong>
          <p data-spt-legacy="">
            {state?.target.bucket || "No bucket reported"}
          </p>
          {state && (
            <Status status={state.target.ready ? "ready" : "needs setup"} />
          )}
        </div>
        <div data-spt-legacy="" className="sidebar-footer">
          <span
            data-spt-legacy=""
            className={`source-dot ${state && !error ? "connected" : ""}`}
          />
          {error
            ? "Connection unavailable"
            : state
              ? "SPT server connected"
              : "Connecting to server"}
          <p data-spt-legacy="">
            {clients} client machine{clients === 1 ? "" : "s"} ·{" "}
            {workloadLabel(runWorkloadType(state?.metrics, state?.job))}
          </p>
        </div>
      </aside>
      <div data-spt-legacy="" className="workspace">
        <header data-spt-legacy="" className="topbar">
          <span data-spt-legacy="">Benchmark workspace</span>
          <div data-spt-legacy="" className="topbar-actions">
            <span
              data-spt-legacy=""
              className={`source-badge ${error ? "offline" : ""}`}
            >
              {error
                ? "OFFLINE"
                : state?.connection?.mode === "attached"
                  ? "ATTACHED · READ ONLY"
                  : state
                    ? "SPT API"
                    : "CONNECTING"}
            </span>
            <div
              data-spt-legacy=""
              className="mode-switch"
              role="group"
              aria-label="Display mode"
            >
              {(["guided", "expert"] as const).map((m) => (
                <button
                  data-spt-legacy=""
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
        <main data-spt-legacy="">
          {error && (
            <div data-spt-legacy="" className="error" role="alert">
              <strong data-spt-legacy="">Connection unavailable. </strong>
              {error}
              {received && (
                <p data-spt-legacy="">
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
              onRefresh={refreshHistory}
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
                <div data-spt-legacy="" className="expert-edit">
                  <button
                    data-spt-legacy=""
                    type="button"
                    onClick={() => setCustomizing(true)}
                  >
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
                onBackHistory={showHistory}
                selected={
                  mode === "guided" ? fields.map((f) => f.key) : selection
                }
                onConfigure={() => setPage("configure")}
                onRefresh={() => refresh()}
              />
              {mode === "expert" && customizing && (
                <div
                  data-spt-legacy=""
                  className="expert-view-overlay"
                  onKeyDown={(event) => {
                    if (event.key === "Escape") setCustomizing(false);
                  }}
                >
                  <button
                    data-spt-legacy=""
                    className="expert-view-backdrop"
                    type="button"
                    aria-label="Close dashboard customization"
                    onClick={() => setCustomizing(false)}
                  />
                  <aside
                    data-spt-legacy=""
                    className="expert-view-drawer"
                    role="dialog"
                    aria-modal="true"
                    aria-labelledby="expert-view-drawer-title"
                  >
                    <div
                      data-spt-legacy=""
                      className="expert-view-drawer-heading"
                    >
                      <div data-spt-legacy="">
                        <span data-spt-legacy="" className="eyebrow">
                          EXPERT VIEW
                        </span>
                        <h2 data-spt-legacy="" id="expert-view-drawer-title">
                          Customize dashboard
                        </h2>
                      </div>
                      <button
                        data-spt-legacy=""
                        type="button"
                        autoFocus
                        aria-label="Close customization"
                        onClick={() => setCustomizing(false)}
                      >
                        ✕
                      </button>
                    </div>
                    <p data-spt-legacy="">
                      Show or hide sections. This only changes your display, not
                      the benchmark or its data.
                    </p>
                    <div
                      data-spt-legacy=""
                      className="expert-view-drawer-actions"
                    >
                      <button
                        data-spt-legacy=""
                        type="button"
                        onClick={() => setSelection(allSectionKeys())}
                      >
                        Show all
                      </button>
                      <button
                        data-spt-legacy=""
                        type="button"
                        onClick={() => setSelection([])}
                      >
                        Hide all
                      </button>
                    </div>
                    <div data-spt-legacy="" className="expert-view-drawer-list">
                      {fields.map((field) => (
                        <label
                          data-spt-legacy=""
                          className="check-label"
                          key={field.key}
                        >
                          <input
                            data-spt-legacy=""
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
                          <span data-spt-legacy="">
                            <strong data-spt-legacy="">{field.title}</strong>
                            <small data-spt-legacy="">{field.detail}</small>
                          </span>
                        </label>
                      ))}
                    </div>
                    <div
                      data-spt-legacy=""
                      className="expert-view-drawer-footer"
                    >
                      <span data-spt-legacy="">
                        {selection.length} of {fields.length} sections shown
                      </span>
                      <button
                        data-spt-legacy=""
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
        <footer data-spt-legacy="">
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
  onRefresh,
}: {
  state: State | null;
  selected: string[];
  onConfigure: () => void;
  historical?: boolean;
  onBackHistory?: () => void;
  onRefresh?: () => Promise<void>;
}) {
  const m = state?.metrics,
    job = state?.job;
  const [stopping, setStopping] = useState(false);
  const [stopError, setStopError] = useState("");
  // An attached CLI run is monitored read-only, so it cannot be stopped here.
  const canStop =
    !historical &&
    job?.status === "running" &&
    state?.connection?.mode !== "attached";
  const stopPending = stopping || !!job?.stop_requested;
  async function stop() {
    if (!canStop || stopPending) return;
    if (
      !window.confirm(
        "Stop this run? It ends now and won't finish its remaining phases.",
      )
    )
      return;
    setStopping(true);
    setStopError("");
    try {
      const r = await fetch("/api/runs/stop", { method: "POST" });
      await readJSON(r, "Stop request failed.");
      await onRefresh?.();
    } catch (e) {
      setStopError(e instanceof Error ? e.message : "Could not stop the run.");
    } finally {
      setStopping(false);
    }
  }
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
      : job?.status === "stopped"
        ? "Run stopped"
        : job?.status === "running" && job.stop_requested
          ? "Stopping run…"
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
      <div data-spt-legacy="" className="page-heading">
        <div data-spt-legacy="">
          <span data-spt-legacy="" className="eyebrow">
            {job?.status === "running" ? "LIVE BENCHMARK" : "BENCHMARK RESULTS"}
          </span>
          <h1 data-spt-legacy="">
            {job?.status === "running"
              ? "Run in progress"
              : "Storage performance"}
          </h1>
          <p data-spt-legacy="">
            {job?.run_id ||
              m?.run_id ||
              "Configure a run to measure your storage target."}
          </p>
        </div>
        <div data-spt-legacy="" className="actions">
          {historical && (
            <button data-spt-legacy="" onClick={onBackHistory}>
              Back to run history
            </button>
          )}
          {job?.status === "completed" && summaries.length > 0 && m && (
            <a
              data-spt-legacy=""
              className="button-link"
              href={`/api/runs/${encodeURIComponent(m.run_id)}/csv?kind=summary`}
              download
            >
              Download CSV report
            </a>
          )}
          {canStop && (
            <button data-spt-legacy="" onClick={stop} disabled={stopPending}>
              {stopPending ? "Stopping…" : "Stop run"}
            </button>
          )}
          <button data-spt-legacy="" className="primary" onClick={onConfigure}>
            ＋ Configure run
          </button>
        </div>
      </div>
      {stopError && (
        <div data-spt-legacy="" className="error" role="alert">
          {stopError}
        </div>
      )}
      {!job && !m ? (
        <section data-spt-legacy="" className="panel empty-state">
          <div data-spt-legacy="" className="empty-icon">
            ◫
          </div>
          <h2 data-spt-legacy="">
            {state
              ? "No benchmark results yet"
              : "Waiting for dashboard server"}
          </h2>
          <p data-spt-legacy="">
            Configure a write and verify benchmark. Measured results will appear
            here as SPT runs.
          </p>
          <button data-spt-legacy="" className="primary" onClick={onConfigure}>
            Configure a run
          </button>
        </section>
      ) : (
        <>
          <section
            data-spt-legacy=""
            className="run-strip"
            aria-label="Run status"
          >
            <div data-spt-legacy="">
              <span data-spt-legacy="">Status</span>
              <Status status={job?.status ?? "recorded"} />
            </div>
            <div data-spt-legacy="">
              <span data-spt-legacy="">Elapsed</span>
              <strong data-spt-legacy="">
                {elapsed(job?.elapsed_seconds)}
              </strong>
            </div>
            <div data-spt-legacy="">
              <span data-spt-legacy="">Current phase</span>
              <strong data-spt-legacy="">
                {job?.phase || phaseName(m?.phase)}
              </strong>
            </div>
            <div data-spt-legacy="">
              <span data-spt-legacy="">Workload</span>
              <strong data-spt-legacy="">
                {workloadLabel(runWorkloadType(m, job))}
              </strong>
            </div>
            <div data-spt-legacy="" className="progress">
              <div data-spt-legacy="">
                <span data-spt-legacy="">Reported phase progress</span>
                <strong data-spt-legacy="">
                  {progress === null ? "Not reported" : `${fmt(progress, 1)}%`}
                </strong>
              </div>
              {progress !== null && (
                <progress
                  data-spt-legacy=""
                  max="100"
                  value={Math.max(0, Math.min(100, progress))}
                  aria-label="Reported phase progress"
                />
              )}
            </div>
          </section>
          <section data-spt-legacy="" className="panel outcome">
            <div data-spt-legacy="">
              <span data-spt-legacy="" className="eyebrow">
                RUN OUTCOME
              </span>
              <h2 data-spt-legacy="">{outcome}</h2>
              <p data-spt-legacy="">
                {job?.message ||
                  "SPT measurements from the configured storage target."}
              </p>
            </div>
            <div data-spt-legacy="" className="outcome-facts">
              <span data-spt-legacy="">
                Failed operations
                <strong data-spt-legacy="">{fmt(failureCount)}</strong>
                <small data-spt-legacy="">
                  {numeric(m?.total_failed) !== null
                    ? "Across all recorded phases"
                    : `In ${phaseName(selectedPhase).toLowerCase()} phase`}
                </small>
              </span>
              <span data-spt-legacy="">
                History captured
                <strong data-spt-legacy="">{history.length} samples</strong>
                <small data-spt-legacy="">Reported interval measurements</small>
              </span>
            </div>
          </section>
          <RunWarnings warnings={m?.warnings} />
          {m && (
            <>
              <div data-spt-legacy="" className="scope-bar">
                <label data-spt-legacy="">
                  Inspect phase
                  <select
                    data-spt-legacy=""
                    value={selectedPhase}
                    onChange={(e) => setPhase(e.target.value)}
                  >
                    {available.map((p) => (
                      <option data-spt-legacy="" key={p} value={p}>
                        {phaseName(p)}
                      </option>
                    ))}
                  </select>
                </label>
                <p data-spt-legacy="">
                  {!m.is_active && final
                    ? "Final phase averages"
                    : "Latest measured phase snapshot"}{" "}
                  · Missing values appear as —
                </p>
              </div>
              {has("cards") && (
                <div data-spt-legacy="" className="metrics-grid">
                  <MetricCard
                    label={`${phaseName(selectedPhase)} IOPS`}
                    value={formatMetric(
                      row?.interval_iops,
                      2,
                      m.is_active ? m.data_status : undefined,
                    )}
                    unit="ops/s"
                    detail={
                      !m.is_active && final
                        ? "Average successful operations per second"
                        : !row && m.data_status === "no_data"
                          ? "No live sample yet — not a measured zero"
                          : "Latest reported rate of successful operations"
                    }
                  />
                  <MetricCard
                    label="Bandwidth"
                    value={formatMetric(
                      row?.bandwidth_mib_s,
                      2,
                      m.is_active ? m.data_status : undefined,
                    )}
                    unit="MiB/s"
                    detail="Data transferred per second"
                    accent="teal"
                  />
                  <MetricCard
                    label="P50 latency"
                    value={formatMetric(
                      row?.p50_latency_us,
                      1,
                      m.is_active ? m.data_status : undefined,
                    )}
                    unit="µs"
                    detail="Half of measured operations were faster"
                    accent="purple"
                  />
                  <MetricCard
                    label="Successful operations"
                    value={formatMetric(
                      row?.cumulative_ops,
                      0,
                      m.is_active ? m.data_status : undefined,
                    )}
                    detail={`In ${phaseName(selectedPhase).toLowerCase()} phase · ${formatMetric(row?.count_failed, 0, m.is_active ? m.data_status : undefined)} failed`}
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
            <details data-spt-legacy="" className="panel">
              <summary data-spt-legacy="">Run record and settings</summary>
              <div data-spt-legacy="" className="detail-strip">
                <span data-spt-legacy="">Started: {utc(job?.started_at)}</span>
                <span data-spt-legacy="">
                  Finished: {utc(job?.finished_at)}
                </span>
                <span data-spt-legacy="">
                  Exit code: {fmt(job?.returncode)}
                </span>
              </div>
              <dl data-spt-legacy="" className="record-grid">
                <div data-spt-legacy="">
                  <dt data-spt-legacy="">Objects</dt>
                  <dd data-spt-legacy="">{fmt(job?.options?.object_count)}</dd>
                </div>
                <div data-spt-legacy="">
                  <dt data-spt-legacy="">Object size</dt>
                  <dd data-spt-legacy="">{job?.options?.object_size ?? "—"}</dd>
                </div>
                <div data-spt-legacy="">
                  <dt data-spt-legacy="">Threads</dt>
                  <dd data-spt-legacy="">{fmt(job?.options?.threads)}</dd>
                </div>
                <div data-spt-legacy="">
                  <dt data-spt-legacy="">Safety time limit</dt>
                  <dd data-spt-legacy="">
                    {elapsed(job?.options?.max_runtime_seconds)}
                  </dd>
                </div>
                <div data-spt-legacy="">
                  <dt data-spt-legacy="">Run prefix</dt>
                  <dd data-spt-legacy="">{job?.prefix ?? "—"}</dd>
                </div>
                <div data-spt-legacy="">
                  <dt data-spt-legacy="">Source</dt>
                  <dd data-spt-legacy="">{m?.source_file ?? "—"}</dd>
                </div>
              </dl>
            </details>
          )}
        </>
      )}
    </>
  );
}
