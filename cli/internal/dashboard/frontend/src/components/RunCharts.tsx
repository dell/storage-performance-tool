import { useMemo, useState } from "react";
import type { Metrics, Sample } from "../types";
import { TrendChart } from "./TrendChart";
import {
  elapsed,
  formatNumber as fmt,
  numeric,
  phaseCode,
  phaseName,
  utc,
} from "../utils/metrics";

const graphs = [
  {
    key: "throughput" as const,
    field: "interval_iops" as const,
    label: "IOPS",
    unit: "ops/s",
    color: "#347bd1",
  },
  {
    key: "bandwidth" as const,
    field: "bandwidth_mib_s" as const,
    label: "Bandwidth",
    unit: "MiB/s",
    color: "#188a82",
  },
  {
    key: "p50" as const,
    field: "p50_latency_us" as const,
    label: "Latency",
    unit: "µs",
    color: "#7a63ce",
  },
];

export function Charts({
  metrics: m,
  phase,
  startedAt,
}: {
  metrics: Metrics;
  phase: string;
  startedAt?: string;
}) {
  const samples = useMemo(() => {
    const history =
      m.chart_samples ?? (m.is_aggregate ? [] : (m.samples ?? []));
    const rows = history
      .filter(
        (r) =>
          phaseCode(r.op_type) === phase &&
          Number.isFinite(Date.parse(r.timestamp ?? "")),
      )
      .sort((a, b) => Date.parse(a.timestamp!) - Date.parse(b.timestamp!));
    const start = Date.parse(startedAt ?? ""),
      origin = Number.isFinite(start)
        ? start
        : Date.parse(rows[0]?.timestamp ?? "");
    return rows.map((r): Sample => ({
      row: r,
      timestamp: r.timestamp!,
      elapsed: Math.max(0, (Date.parse(r.timestamp!) - origin) / 1000),
      throughput: numeric(r.interval_iops),
      bandwidth: numeric(r.bandwidth_mib_s),
      p50: numeric(r.p50_latency_us),
      p99: numeric(r.latency_p99_us),
    }));
  }, [m.chart_samples, m.is_aggregate, m.samples, phase, startedAt]);
  const [mode, setMode] = useState<"timeline" | "summary">("timeline"),
    [cleanup, setCleanup] = useState(false),
    [active, setActive] = useState<number | null>(null);
  const first = samples[0]?.elapsed ?? 0,
    last = samples.at(-1)?.elapsed ?? first;
  const [windowSize, setWindowSize] = useState<number | null>(null),
    [position, setPosition] = useState<number | null>(null);
  const width = windowSize ?? Math.max(1, last - first),
    end = position === null ? last : Math.min(last, Math.max(first, position)),
    start = Math.max(first, end - width);
  const shown = samples.filter((s) => s.elapsed >= start && s.elapsed <= end);
  const display =
    !samples.length && (m.phase_summaries?.length ?? 0) > 0 ? "summary" : mode;
  const inspected = shown[active ?? shown.length - 1];
  const inspectionWindow = `${phase}:${start}:${end}:${shown.length}`;
  const [previousWindow, setPreviousWindow] = useState(inspectionWindow);
  if (inspectionWindow !== previousWindow) {
    setPreviousWindow(inspectionWindow);
    setActive(null);
  }
  const summaries = (m.phase_summaries ?? []).filter(
    (r) => cleanup || phaseCode(r.phase) !== "DELETE",
  );
  return (
    <section className="panel fleet">
      <div className="section-heading">
        <div>
          <h2>Measured performance</h2>
          <p>
            {display === "timeline"
              ? `${phaseName(phase)} phase · ${samples.length} recorded samples`
              : "Final averages by phase"}
          </p>
        </div>
        <select
          aria-label="Chart view"
          value={display}
          onChange={(e) => {
            setMode(e.target.value as "timeline" | "summary");
            setActive(null);
          }}
        >
          <option value="timeline" disabled={!samples.length}>
            Over time
          </option>
          <option value="summary" disabled={!m.phase_summaries?.length}>
            Phase comparison
          </option>
        </select>
      </div>
      {display === "timeline" ? (
        <>
          <div className="fleet-toolbar">
            <label className="check-label">
              Time window
              <select
                aria-label="Time window"
                value={windowSize ?? "all"}
                onChange={(e) => {
                  setWindowSize(
                    e.target.value === "all" ? null : Number(e.target.value),
                  );
                  setPosition(null);
                }}
              >
                {[
                  ["all", "Entire phase"],
                  ["60", "1 minute"],
                  ["300", "5 minutes"],
                  ["3600", "1 hour"],
                  ["86400", "1 day"],
                ].map(([v, label]) => (
                  <option key={v} value={v}>
                    {label}
                  </option>
                ))}
                {windowSize !== null &&
                  ![60, 300, 3600, 86400].includes(windowSize) && (
                    <option value={windowSize}>
                      Custom · {elapsed(windowSize)}
                    </option>
                  )}
              </select>
            </label>
            <div className="button-group">
              <button
                disabled={samples.length < 2}
                onClick={() => setWindowSize(Math.max(1, width / 2))}
              >
                Zoom in
              </button>
              <button
                disabled={windowSize === null}
                onClick={() =>
                  setWindowSize(width * 2 >= last - first ? null : width * 2)
                }
              >
                Zoom out
              </button>
              <button
                onClick={() => {
                  setWindowSize(null);
                  setPosition(null);
                }}
              >
                Fit phase
              </button>
              <button
                disabled={position === null}
                onClick={() => setPosition(null)}
              >
                Follow latest
              </button>
            </div>
            {!m.is_active && shown.length > 0 && (
              <a
                className="button-link"
                href={`/api/runs/${encodeURIComponent(m.run_id)}/csv?kind=window&phase=${phase}&start=${start}&end=${end}`}
                download
              >
                Export window CSV
              </a>
            )}
          </div>
          {samples.length > 1 && (
            <label className="timeline-pan">
              Move time window
              <input
                aria-label="Move time window"
                type="range"
                min={Math.min(last, first + width)}
                max={last}
                step="any"
                disabled={windowSize === null || width >= last - first}
                value={end}
                onChange={(e) => setPosition(Number(e.target.value))}
              />
              <span>
                {elapsed(start)} – {elapsed(end)} from run start
              </span>
            </label>
          )}
          {!samples.length && (
            <div className="notice">
              No interval history recorded for this phase. Final phase averages
              appear once SPT reports them.
            </div>
          )}
          {samples.length === 1 && (
            <div className="notice">
              One measured point is available. It cannot establish a trend.
            </div>
          )}
          {graphs.map((g) => (
            <TrendChart
              key={g.key}
              samples={shown}
              title={g.label}
              unit={g.unit}
              series={
                g.key === "p50"
                  ? [
                      { key: "p50", label: "P50", color: g.color },
                      { key: "p99", label: "P99", color: "#b096e7" },
                    ]
                  : [{ key: g.key, label: g.label, color: g.color }]
              }
              active={active !== null && active < shown.length ? active : null}
              onInspect={setActive}
            />
          ))}
          <p className="chart-readout" aria-live="polite">
            {inspected
              ? `${elapsed(inspected.elapsed)} · ${utc(inspected.timestamp)} · IOPS ${fmt(inspected.throughput, 2)} ops/s · bandwidth ${fmt(inspected.bandwidth, 2)} MiB/s · P50 ${fmt(inspected.p50, 1)} µs`
              : "Waiting for measured samples in this time window."}
          </p>
          <p className="subtle">
            All three graphs share the time window and inspector. Each uses its
            own scale. Missing measurements are shown as gaps.
          </p>
        </>
      ) : (
        <>
          <label className="check-label">
            <input
              type="checkbox"
              checked={cleanup}
              onChange={(e) => setCleanup(e.target.checked)}
            />
            Include cleanup in comparison
          </label>
          <div className="comparison-grid">
            {graphs.map((g) => {
              const field = g.key === "throughput" ? "iops" : g.field;
              const max = Math.max(
                1,
                ...summaries.map((r) => numeric(r[field]) ?? 0),
              );
              return (
                <article className="comparison" key={g.key}>
                  <h3>
                    {g.label} <span>{g.unit}</span>
                  </h3>
                  {summaries.map((r) => {
                    const v = numeric(r[field]);
                    return (
                      <div className="comparison-row" key={r.phase}>
                        <span>{phaseName(r.phase)}</span>
                        <div>
                          {v !== null && (
                            <i
                              style={{
                                width: `${(v / max) * 100}%`,
                                background: g.color,
                              }}
                            />
                          )}
                        </div>
                        <strong>{fmt(v, 2)}</strong>
                      </div>
                    );
                  })}
                </article>
              );
            })}
          </div>
          <p className="subtle">
            These bars are final phase averages. They do not represent interval
            history. Cleanup normally transfers no object payload.
          </p>
        </>
      )}
    </section>
  );
}
