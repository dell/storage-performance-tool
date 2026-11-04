import {
  Badge,
  Box,
  Button,
  NavLink,
  SegmentedControl,
  Text,
  Title,
  UnstyledButton,
} from "@mantine/core";
import {
  ArrowLeftRight,
  ExternalLink,
  History,
  LayoutDashboard,
  ListFilter,
  Plus,
} from "lucide-react";
import { useEffect, useState } from "react";
import type { Row, RunMode, State } from "./types";
import { useStopRun } from "./api/mutations";
import { useSavedRun, useServerState } from "./api/queries";
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

export default function App() {
  const server = useServerState();
  const state = server.data ?? null;
  const error = server.error?.message ?? "";
  const received = server.dataUpdatedAt
    ? new Date(server.dataUpdatedAt).toISOString()
    : "";
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
  const [historyRunID, setHistoryRunID] = useState<string | null>(null);
  const savedRun = useSavedRun(
    page === "history" || page === "dashboard" ? historyRunID : null,
  );
  const historyState = historyRunID ? (savedRun.data ?? null) : null;
  // Keep the history list visible until the selected run has finished loading.
  if (
    page === "history" &&
    historyRunID &&
    savedRun.isSuccess &&
    !savedRun.isFetching
  ) {
    setCustomizing(false);
    setPage("dashboard");
  }

  const openSavedRun = (runID: string) => {
    if (historyRunID === runID) void savedRun.refetch();
    else setHistoryRunID(runID);
  };

  const showDashboard = () => {
    setHistoryRunID(null);
    setCustomizing(false);
    setPage("dashboard");
  };

  const showHistory = () => {
    setHistoryRunID(null);
    setPage("history");
  };

  return (
    <div className="flex min-h-screen [@media(max-width:760px)]:block">
      <Box
        component="aside"
        bg="var(--spt-surface-panel)"
        className="sticky top-0 flex h-screen w-[235px] shrink-0 flex-col border-r border-divider px-lg py-xl [@media(max-width:1150px)]:w-[195px] [@media(max-width:1150px)]:px-[14px] [@media(max-width:1150px)]:py-[25px] [@media(max-width:760px)]:static [@media(max-width:760px)]:h-auto [@media(max-width:760px)]:w-full [@media(max-width:760px)]:border-r-0 [@media(max-width:760px)]:border-b [@media(max-width:760px)]:px-comfortable [@media(max-width:760px)]:py-[15px]"
      >
        <UnstyledButton
          component="a"
          href="#dashboard"
          className="flex items-center gap-sm"
          onClick={showDashboard}
        >
          <Text
            component="span"
            bg="var(--mantine-primary-color-filled)"
            c="var(--spt-text-inverse)"
            bdrs="lg"
            size="emphasis"
            fw="var(--mantine-font-weight-heading)"
            lh="normal"
            className="grid size-[42px] shrink-0 place-items-center"
          >
            SPT
          </Text>
          <div className="min-w-0">
            <Text
              size="sm"
              c="var(--spt-text-primary)"
              fw="var(--mantine-font-weight-heading)"
              lh="normal"
            >
              {state?.branding?.product_name ?? "Storage Performance Tool"}
            </Text>
            <Text
              component="small"
              size="xxs"
              c="var(--spt-text-subtle)"
              lh="normal"
              className="mt-[6px] block tracking-[1px] [@media(max-width:760px)]:hidden"
            >
              {(
                state?.branding?.environment_label ?? "BENCHMARK CONSOLE"
              ).toUpperCase()}
            </Text>
          </div>
        </UnstyledButton>
        <Text
          size="xs"
          c="var(--spt-text-subtle)"
          lh="normal"
          className="mx-sm mt-xxxl mb-[14px] tracking-[1.5px] [@media(max-width:760px)]:hidden"
        >
          WORKSPACE
        </Text>
        <nav
          aria-label="Main navigation"
          className="grid gap-xs [@media(max-width:760px)]:mt-[15px] [@media(max-width:760px)]:flex [@media(max-width:760px)]:flex-wrap"
        >
          <NavLink
            component="button"
            type="button"
            label="Configure run"
            leftSection={<Plus size={16} aria-hidden="true" />}
            active={page === "configure"}
            aria-current={page === "configure" ? "page" : undefined}
            onClick={() => {
              setHistoryRunID(null);
              setPage("configure");
            }}
          />
          <NavLink
            component="button"
            type="button"
            label="Live dashboard"
            leftSection={<LayoutDashboard size={16} aria-hidden="true" />}
            active={page === "dashboard"}
            aria-current={page === "dashboard" ? "page" : undefined}
            onClick={showDashboard}
          />
          <NavLink
            component="button"
            type="button"
            label="Run history"
            leftSection={<History size={16} aria-hidden="true" />}
            active={page === "history"}
            aria-current={page === "history" ? "page" : undefined}
            onClick={() => {
              setHistoryRunID(null);
              showHistory();
            }}
          />
          <NavLink
            component="button"
            type="button"
            label="Results catalog"
            leftSection={<ListFilter size={16} aria-hidden="true" />}
            active={page === "catalog"}
            aria-current={page === "catalog" ? "page" : undefined}
            onClick={() => setPage("catalog")}
          />
          <NavLink
            component="button"
            type="button"
            label="Compare runs"
            leftSection={<ArrowLeftRight size={16} aria-hidden="true" />}
            active={page === "compare"}
            aria-current={page === "compare" ? "page" : undefined}
            onClick={() => setPage("compare")}
          />
          <NavLink
            component="button"
            type="button"
            label="Connect to active run"
            leftSection={<ExternalLink size={16} aria-hidden="true" />}
            active={page === "connect"}
            aria-current={page === "connect" ? "page" : undefined}
            onClick={() => setPage("connect")}
          />
        </nav>
        <div className="mx-sm mt-xxl mb-section border-t border-divider pt-section wrap-anywhere [@media(max-width:760px)]:hidden">
          <Text
            size="xxs"
            c="var(--spt-text-muted)"
            fw="var(--mantine-font-weight-heading)"
            lh="normal"
            className="mb-compact tracking-[1.3px]"
          >
            STORAGE TARGET
          </Text>
          <Text
            component="strong"
            size="sm"
            c="var(--spt-text-primary)"
            fw="var(--mantine-font-weight-bold)"
            lh="normal"
          >
            {state?.target.endpoint_label || "Awaiting server"}
          </Text>
          <Text size="caption" className="mt-xs mb-sm">
            {state?.target.bucket || "No bucket reported"}
          </Text>
          {state && (
            <Status status={state.target.ready ? "ready" : "needs setup"} />
          )}
        </div>
        <Text
          component="div"
          size="caption"
          c="var(--spt-text-muted)"
          lh="normal"
          className="mt-auto [@media(max-width:760px)]:hidden"
        >
          <Box
            component="span"
            aria-hidden="true"
            bg={state && !error ? "var(--spt-chart-bandwidth)" : "#e2a243"}
            bdrs="50%"
            className="mr-xs inline-block size-[7px]"
          />
          {error
            ? "Connection unavailable"
            : state
              ? "SPT server connected"
              : "Connecting to server"}
          <Text size="xs" className="my-xs">
            {clients} client machine{clients === 1 ? "" : "s"} ·{" "}
            {workloadLabel(runWorkloadType(state?.metrics, state?.job))}
          </Text>
        </Text>
      </Box>
      <div className="min-w-0 flex-1">
        <Box
          component="header"
          bg="#ffffffc9"
          className="flex h-[76px] items-center justify-between border-b border-divider px-xxl [@media(max-width:1150px)]:px-section [@media(max-width:760px)]:h-auto [@media(max-width:760px)]:min-h-[76px] [@media(max-width:760px)]:flex-wrap [@media(max-width:760px)]:gap-xs [@media(max-width:760px)]:p-md"
        >
          <Text
            component="span"
            size="sm"
            c="var(--spt-text-muted)"
            lh="normal"
          >
            Benchmark workspace
          </Text>
          <div className="flex items-center gap-lg [@media(max-width:760px)]:gap-compact">
            <Badge
              color={error ? "yellow" : "spt"}
              c={
                error
                  ? "var(--spt-status-warning-text)"
                  : "var(--mantine-color-spt-7)"
              }
              fz="xxs"
              tt="uppercase"
              lh="normal"
              px="xs"
              styles={{
                root: {
                  paddingBlock: "5px",
                  borderColor: error
                    ? "var(--spt-status-warning-border)"
                    : "var(--mantine-color-spt-2)",
                  letterSpacing: "0.8px",
                },
              }}
            >
              {error
                ? "OFFLINE"
                : state?.connection?.mode === "attached"
                  ? "ATTACHED · READ ONLY"
                  : state
                    ? "SPT API"
                    : "CONNECTING"}
            </Badge>
            <SegmentedControl<RunMode>
              aria-label="Display mode"
              name="display-mode"
              value={mode}
              data={[
                { value: "guided", label: "Guided" },
                { value: "expert", label: "Expert" },
              ]}
              onChange={(nextMode) => {
                setMode(nextMode);
                setCustomizing(false);
                // On Configure, the mode switches the workload form in place.
                if (page !== "configure") showDashboard();
              }}
            />
          </div>
        </Box>
        <main className="mx-auto max-w-[1550px] px-xxl py-[32px] [@media(min-width:1500px)]:pt-[40px] [@media(max-width:1150px)]:p-section [@media(max-width:760px)]:px-md [@media(max-width:760px)]:py-panel">
          {error && (
            <div data-spt-legacy="" className="error" role="alert">
              <strong data-spt-legacy="">Connection unavailable. </strong>
              {error}
              {received && (
                <Text>
                  Showing the last received state from {utc(received)}. Run
                  controls are disabled until the connection returns.
                </Text>
              )}
            </div>
          )}
          {page === "connect" ? (
            <ConnectRun
              state={state}
              offline={!!error}
              onConnected={showDashboard}
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
              opening={!!historyRunID && savedRun.isFetching}
              error={savedRun.error?.message ?? ""}
              onOpen={openSavedRun}
              onRefresh={() => setHistoryRunID(null)}
            />
          ) : page === "configure" ? (
            <Configure
              state={state}
              mode={mode}
              offline={!!error}
              onStarted={showDashboard}
            />
          ) : (
            <>
              {mode === "expert" && (
                <div data-spt-legacy="" className="expert-edit">
                  <Button type="button" onClick={() => setCustomizing(true)}>
                    Customize view
                  </Button>
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
                        <Title
                          order={2}
                          size="xl"
                          className="mt-[3px]"
                          id="expert-view-drawer-title"
                        >
                          Customize dashboard
                        </Title>
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
                    <Text size="sm">
                      Show or hide sections. This only changes your display, not
                      the benchmark or its data.
                    </Text>
                    <div
                      data-spt-legacy=""
                      className="expert-view-drawer-actions"
                    >
                      <Button
                        type="button"
                        onClick={() => setSelection(allSectionKeys())}
                      >
                        Show all
                      </Button>
                      <Button type="button" onClick={() => setSelection([])}>
                        Hide all
                      </Button>
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
                      <Button
                        variant="filled"
                        type="button"
                        onClick={() => setCustomizing(false)}
                      >
                        Done
                      </Button>
                    </div>
                  </aside>
                </div>
              )}
            </>
          )}
        </main>
        <Text
          component="footer"
          size="xs"
          c="var(--spt-text-subtle)"
          lh="normal"
          className="px-xxl pb-[25px] [@media(max-width:760px)]:px-md [@media(max-width:760px)]:pb-lg"
        >
          {state?.branding?.product_name ?? "Storage Performance Tool"} ·{" "}
          {received
            ? `State received ${utc(received)}`
            : "Waiting for server state"}{" "}
          · Refreshes every 2 seconds
        </Text>
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
  const stopping = useStopRun();
  const stopError = stopping.error?.message;
  // An attached CLI run is monitored read-only, so it cannot be stopped here.
  const canStop =
    !historical &&
    job?.status === "running" &&
    state?.connection?.mode !== "attached";
  const stopPending = stopping.isPending || !!job?.stop_requested;
  function stop() {
    if (!canStop || stopPending) return;
    if (
      window.confirm(
        "Stop this run? It ends now and won't finish its remaining phases.",
      )
    ) {
      stopping.mutate();
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
          <Title order={1}>
            {job?.status === "running"
              ? "Run in progress"
              : "Storage performance"}
          </Title>
          <Text size="sm" className="mt-xs wrap-anywhere">
            {job?.run_id ||
              m?.run_id ||
              "Configure a run to measure your storage target."}
          </Text>
        </div>
        <div data-spt-legacy="" className="actions">
          {historical && (
            <Button onClick={onBackHistory}>Back to run history</Button>
          )}
          {job?.status === "completed" && summaries.length > 0 && m && (
            <Button
              component="a"
              href={`/api/runs/${encodeURIComponent(m.run_id)}/csv?kind=summary`}
              download
            >
              Download CSV report
            </Button>
          )}
          {canStop && (
            <Button onClick={stop} disabled={stopPending}>
              {stopPending ? "Stopping…" : "Stop run"}
            </Button>
          )}
          <Button variant="filled" onClick={onConfigure}>
            ＋ Configure run
          </Button>
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
          <Title order={2}>
            {state
              ? "No benchmark results yet"
              : "Waiting for dashboard server"}
          </Title>
          <Text className="mt-[14px] mx-auto mb-section max-w-[450px]">
            Configure a write and verify benchmark. Measured results will appear
            here as SPT runs.
          </Text>
          <Button variant="filled" onClick={onConfigure}>
            Configure a run
          </Button>
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
              <Title order={2}>{outcome}</Title>
              <Text size="sm" className="mt-[7px]">
                {job?.message ||
                  "SPT measurements from the configured storage target."}
              </Text>
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
                <Text size="caption">
                  {!m.is_active && final
                    ? "Final phase averages"
                    : "Latest measured phase snapshot"}{" "}
                  · Missing values appear as —
                </Text>
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
