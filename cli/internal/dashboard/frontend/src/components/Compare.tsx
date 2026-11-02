import { useEffect, useState } from "react";
import type { CompareDelta, CompareResult, SeriesPoint } from "../types";
import { formatNumber as fmt } from "../utils/metrics";
import {
  compareQuery,
  deltaText,
  flagText,
  groupDeltas,
  overlayExtent,
  phasesOf,
  regressionCount,
  segments,
  type OverlayMetric,
  type RuleOverrides,
} from "../utils/catalog";

const palette = [
  "#1d4ed8",
  "#c2410c",
  "#15803d",
  "#7e22ce",
  "#b91c1c",
  "#0f766e",
];
const metrics: { key: OverlayMetric; label: string }[] = [
  { key: "iops", label: "IOPS" },
  { key: "bandwidth_mib_s", label: "Bandwidth (MiB/s)" },
  { key: "p50_latency_us", label: "P50 latency (µs)" },
  { key: "p99_latency_us", label: "P99 latency (µs)" },
];

export function Compare({
  ids,
  baseline,
  onBack,
}: {
  ids: string[];
  baseline: string;
  onBack: () => void;
}) {
  const [base, setBase] = useState(baseline);
  const [rule, setRule] = useState<RuleOverrides>({});
  const [draft, setDraft] = useState<Record<string, string>>({});
  const [data, setData] = useState<CompareResult | null>(null);
  const [error, setError] = useState("");
  const [metric, setMetric] = useState<OverlayMetric>("iops");
  useEffect(() => {
    if (ids.length < 2) return;
    const controller = new AbortController();
    fetch(compareQuery(ids, base, rule), {
      cache: "no-store",
      signal: controller.signal,
    })
      .then(async (r) => {
        const body = await r.json();
        if (!r.ok) throw new Error(body.error ?? "Could not compare runs.");
        const result = body as CompareResult;
        setData(result);
        setError("");
        setDraft({
          threshold_pct: String(result.rule.threshold_pct),
          latency_floor_us: String(result.rule.latency_floor_us),
          min_ops: String(result.rule.min_ops),
        });
      })
      .catch((e) => {
        if (!controller.signal.aborted)
          setError(e instanceof Error ? e.message : "Could not compare runs.");
      });
    return () => controller.abort();
  }, [ids, base, rule]);
  const apply = () => {
    const next: RuleOverrides = {};
    for (const k of ["threshold_pct", "latency_floor_us", "min_ops"] as const) {
      const v = Number(draft[k]);
      if (draft[k]?.trim() !== "" && Number.isFinite(v) && v >= 0) next[k] = v;
    }
    setRule(next);
  };
  if (ids.length < 2)
    return (
      <div className="history-empty">
        <h2>Select runs to compare</h2>
        <p>Choose at least two runs in the results catalog.</p>
        <button onClick={onBack}>Open results catalog</button>
      </div>
    );
  const runIDs = data?.runs.map((r) => r.id) ?? ids;
  const color = (id: string) =>
    palette[Math.max(0, runIDs.indexOf(id)) % palette.length];
  const regressions = data ? regressionCount(data.deltas) : 0;
  return (
    <>
      <div className="page-heading">
        <div>
          <span className="eyebrow">COMPARE · {ids.length} RUNS</span>
          <h1>Compare runs</h1>
          <p>
            Overlaid per-phase time series and totals, checked against the
            baseline run.
          </p>
        </div>
        <div className="actions">
          <button onClick={onBack}>Back to catalog</button>
        </div>
      </div>
      {error && (
        <div className="error" role="alert">
          {error}
        </div>
      )}
      {!data ? (
        !error && <p className="subtle">Loading comparison…</p>
      ) : (
        <>
          <section className="panel compare-rule" aria-label="Regression rule">
            <div>
              <strong>{data.rule.label}</strong>
              <p className="form-help">{data.rule.description}</p>
            </div>
            <label>
              Baseline
              <select
                value={data.baseline}
                onChange={(e) => setBase(e.target.value)}
              >
                {data.runs.map((r) => (
                  <option key={r.id} value={r.id}>
                    {r.id}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Threshold %
              <input
                type="number"
                min={0}
                step={1}
                value={draft.threshold_pct ?? ""}
                onChange={(e) =>
                  setDraft({ ...draft, threshold_pct: e.target.value })
                }
              />
            </label>
            <label>
              Latency floor µs
              <input
                type="number"
                min={0}
                value={draft.latency_floor_us ?? ""}
                onChange={(e) =>
                  setDraft({ ...draft, latency_floor_us: e.target.value })
                }
              />
            </label>
            <label>
              Min operations
              <input
                type="number"
                min={0}
                value={draft.min_ops ?? ""}
                onChange={(e) =>
                  setDraft({ ...draft, min_ops: e.target.value })
                }
              />
            </label>
            <button className="primary" onClick={apply}>
              Apply
            </button>
            <p
              className={`compare-summary ${regressions ? "bad" : "good"}`}
              role="status"
            >
              {regressions
                ? `${regressions} metric${regressions === 1 ? "" : "s"} past the threshold vs ${data.baseline}.`
                : `No metric past the threshold vs ${data.baseline}.`}{" "}
              Threshold check, not statistical significance.
            </p>
          </section>
          {Object.entries(data.settings_differences ?? {}).some(
            ([, d]) => d.length,
          ) && (
            <section
              className="notice notice-warn"
              aria-label="Settings differences"
            >
              <strong>
                Settings differ from the baseline; deltas may reflect
                configuration, not storage.
              </strong>
              <ul>
                {Object.entries(data.settings_differences).map(([id, diffs]) =>
                  diffs.map((d) => (
                    <li key={id + d}>
                      {id}: {d}
                    </li>
                  )),
                )}
              </ul>
            </section>
          )}
          <section className="panel">
            <div className="run-log-heading">
              <h2>Time series</h2>
              <div className="mode-switch" role="group" aria-label="Metric">
                {metrics.map((m) => (
                  <button
                    key={m.key}
                    className={metric === m.key ? "active" : ""}
                    aria-pressed={metric === m.key}
                    onClick={() => setMetric(m.key)}
                  >
                    {m.label}
                  </button>
                ))}
              </div>
            </div>
            <ul className="compare-legend">
              {data.runs.map((r) => (
                <li key={r.id}>
                  <i style={{ background: color(r.id) }} />
                  {r.id}
                  {r.id === data.baseline ? " (baseline)" : ""}
                </li>
              ))}
            </ul>
            <div className="compare-charts">
              {phasesOf(data.series).map((phase) => (
                <OverlayChart
                  key={phase}
                  phase={phase}
                  metric={metric}
                  unit={metrics.find((m) => m.key === metric)?.label ?? ""}
                  lines={data.runs.map((r) => ({
                    id: r.id,
                    color: color(r.id),
                    points: data.series[r.id]?.[phase] ?? [],
                  }))}
                />
              ))}
              {phasesOf(data.series).length === 0 && (
                <p className="subtle">
                  None of the selected runs recorded interval metrics.
                </p>
              )}
            </div>
          </section>
          <DeltaTable deltas={data.deltas} baseline={data.baseline} />
        </>
      )}
    </>
  );
}

export function OverlayChart({
  phase,
  metric,
  unit,
  lines,
}: {
  phase: string;
  metric: OverlayMetric;
  unit: string;
  lines: { id: string; color: string; points: SeriesPoint[] }[];
}) {
  const { maxT, maxV, count } = overlayExtent(
    lines.map((l) => l.points),
    metric,
  );
  const W = 520,
    H = 180,
    left = 52,
    right = 12,
    top = 12,
    bottom = 28;
  const x = (t: number) =>
    left + (maxT > 0 ? t / maxT : 0.5) * (W - left - right);
  const yMax = (maxV || 1) * 1.1;
  const y = (v: number) => H - bottom - (v / yMax) * (H - top - bottom);
  return (
    <figure className="overlay-chart">
      <figcaption>
        {phase} · {unit}
      </figcaption>
      {count === 0 ? (
        <p className="subtle">No samples for this metric.</p>
      ) : (
        <svg
          viewBox={`0 0 ${W} ${H}`}
          role="img"
          aria-label={`${phase} ${unit} over elapsed seconds for ${lines.length} runs`}
        >
          <line
            x1={left}
            y1={H - bottom}
            x2={W - right}
            y2={H - bottom}
            className="axis"
          />
          <line x1={left} y1={top} x2={left} y2={H - bottom} className="axis" />
          <text x={left - 6} y={top + 8} textAnchor="end" className="tick">
            {fmt(yMax, yMax < 10 ? 1 : 0)}
          </text>
          <text x={left - 6} y={H - bottom} textAnchor="end" className="tick">
            0
          </text>
          <text x={W - right} y={H - 8} textAnchor="end" className="tick">
            {fmt(maxT, 0)} s
          </text>
          {lines.map((l) =>
            segments(l.points, metric).map((seg, i) =>
              seg.length === 1 ? (
                <circle
                  key={l.id + i}
                  cx={x(seg[0].t)}
                  cy={y(seg[0][metric] as number)}
                  r={3}
                  fill={l.color}
                />
              ) : (
                <polyline
                  key={l.id + i}
                  fill="none"
                  stroke={l.color}
                  strokeWidth={2}
                  points={seg
                    .map((p) => `${x(p.t)},${y(p[metric] as number)}`)
                    .join(" ")}
                />
              ),
            ),
          )}
        </svg>
      )}
    </figure>
  );
}

export function DeltaTable({
  deltas,
  baseline,
}: {
  deltas: CompareDelta[];
  baseline: string;
}) {
  const grouped = groupDeltas(deltas);
  return (
    <section className="panel run-history-panel" aria-label="Delta table">
      <h2>Deltas vs {baseline}</h2>
      {deltas.length === 0 ? (
        <p className="subtle">No shared phases to compare.</p>
      ) : (
        <div className="table-wrap">
          <table className="run-history-table delta-table">
            <thead>
              <tr>
                <th>Phase</th>
                <th>Metric</th>
                <th>Run</th>
                <th>Baseline</th>
                <th>Value</th>
                <th>Δ</th>
                <th>Flag</th>
              </tr>
            </thead>
            <tbody>
              {[...grouped].flatMap(([phase, byMetric]) =>
                [...byMetric].flatMap(([, rows]) =>
                  rows.map((d) => (
                    <tr
                      key={`${phase}-${d.metric}-${d.run_id}`}
                      className={`flag-${d.flag}`}
                    >
                      <td>{phase}</td>
                      <td>{d.label}</td>
                      <td className="history-run-id">{d.run_id}</td>
                      <td>{fmt(d.baseline, 2)}</td>
                      <td>{fmt(d.value, 2)}</td>
                      <td>{deltaText(d)}</td>
                      <td>
                        <span
                          className={`flag flag-${d.flag}`}
                          title={d.reason}
                        >
                          {flagText[d.flag] ?? d.flag}
                        </span>
                        {d.reason && <small>{d.reason}</small>}
                      </td>
                    </tr>
                  )),
                ),
              )}
            </tbody>
          </table>
        </div>
      )}
      <p className="history-footnote">
        Flags are a threshold check, not statistical significance. One run per
        side cannot show variance; repeat runs before drawing conclusions.
      </p>
    </section>
  );
}
