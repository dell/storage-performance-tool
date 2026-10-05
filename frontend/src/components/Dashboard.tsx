import type { Mode, Snapshot } from "../types";
import { GuidedDashboard } from "./GuidedDashboard";
import { allExpertFields } from "./ExpertSelection";
import type { ExpertField } from "./ExpertSelection";
import { MetricCard } from "./MetricCard";
import { FleetTelemetry } from "./FleetTelemetry";
import { ClientTable } from "./ClientTable";
import { ExpertDetails } from "./ExpertDetails";
import { ShardDiscovery } from "./ShardDiscovery";
import { MixedWorkload } from "./MixedWorkload";
import { DeleteDetails } from "./DeleteDetails";
import { failurePercent, formatElapsed, formatNumber } from "../utils/metrics";
export function Dashboard({
  snapshot: s,
  mode,
  error,
  selection = allExpertFields,
}: {
  snapshot: Snapshot;
  mode: Mode;
  error: string | null;
  selection?: ExpertField[];
}) {
  const has = (field: ExpertField) => selection.includes(field);
  const m = s.aggregate,
    failure = failurePercent(m.success, m.failures);
  const cards = [
    {
      key: "throughput",
      label:
        s.config.operation === "DELETE"
          ? "Current successful requests"
          : "Current throughput",
      value: formatNumber(m.currentOps),
      unit: "ops/s",
      detail: `Run average ${formatNumber(m.averageOps)} ops/s`,
      accent: "blue",
    },
    {
      key: "bandwidth",
      label: "Current bandwidth",
      value: formatNumber(m.bandwidthMiB, 1),
      unit: m.bandwidthMiB === null ? "" : "MiB/s",
      detail:
        m.bandwidthMiB === null
          ? "Not applicable to DELETE"
          : `${formatNumber(m.transferredBytes / 1073741824, 2)} GiB transferred`,
      accent: "teal",
    },
    {
      key: "latency",
      label: "P99 request latency",
      value: formatNumber(m.latency?.p99Ms ?? null, 2),
      unit: "ms",
      detail: `P50 ${formatNumber(m.latency?.p50Ms ?? null, 2)} ms · reporting window`,
      accent: "purple",
    },
    {
      key: "failures",
      label:
        s.config.operation === "DELETE"
          ? "Requests with errors"
          : "Failed operations",
      value: formatNumber(m.failures),
      detail: `${formatNumber(failure, 3)}% of resolved operations`,
      accent: m.failures ? "orange" : "blue",
    },
    {
      key: "concurrency",
      label: "In-flight operations",
      value: formatNumber(m.concurrency),
      detail: `${s.config.threadsPerClient * s.config.expectedNodes} configured threads`,
      accent: "teal",
    },
    {
      key: "success",
      label: "Successful operations",
      value: formatNumber(m.success),
      detail: "Cumulative run total",
      accent: "blue",
    },
  ];
  function exportCsv() {
    const rows = [
        [
          "elapsed_s",
          "throughput_ops_s",
          "bandwidth_mib_s",
          "p50_ms",
          "p99_ms",
        ],
        ...s.samples.map((p) => [
          p.elapsed,
          p.throughput,
          p.bandwidth ?? "",
          p.p50,
          p.p99,
        ]),
      ],
      url = URL.createObjectURL(
        new Blob([rows.map((r) => r.join(",")).join("\n")], {
          type: "text/csv",
        }),
      );
    const a = document.createElement("a");
    a.href = url;
    a.download = `${s.runId}.csv`;
    a.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }
  return (
    <>
      <div className="page-heading">
        <div>
          <span className="eyebrow">
            {s.runId} / {s.config.operation}
          </span>
          <h1>{s.config.name}</h1>
          <p>
            {mode === "guided"
              ? "Understand the run at a glance. Open details when you need them."
              : "Inspect performance, distributions, and individual clients."}
          </p>
        </div>
        <button onClick={exportCsv}>Export samples ↓</button>
      </div>
      {error && (
        <div className="error" role="alert">
          Connection error: {error}. Showing the last received snapshot.
        </div>
      )}
      <section className="run-strip">
        <div>
          <span className={`status ${s.state === "failed" ? "warn" : "good"}`}>
            <i />
            {s.state}
          </span>
          <strong>{s.phase}</strong>
        </div>
        <div>
          <span>Elapsed</span>
          <strong>{formatElapsed(s.elapsedSeconds)}</strong>
        </div>
        <div>
          <span>Reporting</span>
          <strong>
            {s.reportingNodes}/{s.expectedNodes} clients{" "}
            {s.partial ? "· partial" : ""}
          </strong>
        </div>
        <div className="progress">
          <div>
            <span>Run progress</span>
            <strong>
              {s.progressPercent === null
                ? "Unbounded"
                : `${formatNumber(s.progressPercent, 0)}%`}
            </strong>
          </div>
          {s.progressPercent !== null && (
            <progress max="100" value={s.progressPercent} />
          )}
        </div>
      </section>
      <div className="metrics-grid">
        {cards
          .filter((card) => has(card.key as ExpertField))
          .map(({ key, ...card }) => (
            <MetricCard key={key} {...card} />
          ))}
      </div>
      {has("fleet") && <FleetTelemetry samples={s.samples} />}
      {has("mixed") && s.mixedMetrics && (
        <MixedWorkload value={s.mixedMetrics} />
      )}
      {has("discovery") &&
        s.config.shardDiscovery?.enabled &&
        s.discoveryMetrics && <ShardDiscovery value={s.discoveryMetrics} />}
      {has("clients") && <ClientTable nodes={s.nodes} />}
      {has("core") && <GuidedDashboard snapshot={s} error={null} embedded />}
      {has("delete") && s.deleteMetrics && (
        <DeleteDetails value={s.deleteMetrics} />
      )}{" "}
      {has("timings") && <ExpertDetails snapshot={s} showRaw={has("raw")} />}
      {!has("timings") && has("raw") && (
        <section className="panel">
          <h2>Raw normalized snapshot</h2>
          <pre>{JSON.stringify(s, null, 2)}</pre>
        </section>
      )}
    </>
  );
}
