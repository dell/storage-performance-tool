import { Badge, Button, Text, Title } from "@mantine/core";
import { useState } from "react";
import type { CompareDelta, CompareResult, SeriesPoint } from "../types";
import { useComparison, useLastSuccessfulData } from "../api/queries";
import { formatNumber as fmt } from "../utils/metrics";
import {
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
  const comparison = useComparison(ids, base, rule);
  const data = useLastSuccessfulData(comparison.data);
  const error = comparison.error?.message ?? "";
  const [metric, setMetric] = useState<OverlayMetric>("iops");
  const [draftRule, setDraftRule] = useState<CompareResult["rule"] | null>(
    null,
  );
  // Initialize editable rule fields when a successful response supplies defaults.
  if (data && data.rule !== draftRule) {
    setDraftRule(data.rule);
    setDraft({
      threshold_pct: String(data.rule.threshold_pct),
      latency_floor_us: String(data.rule.latency_floor_us),
      min_ops: String(data.rule.min_ops),
    });
  }
  const apply = () => {
    const next: RuleOverrides = {};
    const fields = ["threshold_pct", "latency_floor_us", "min_ops"] as const;
    for (const k of fields) {
      const v = Number(draft[k]);
      if (draft[k]?.trim() !== "" && Number.isFinite(v) && v >= 0) next[k] = v;
    }
    if (fields.every((field) => next[field] === rule[field])) {
      void comparison.refetch();
    } else {
      setRule(next);
    }
  };
  if (ids.length < 2)
    return (
      <div data-spt-legacy="" className="history-empty">
        <Title order={2}>Select runs to compare</Title>
        <Text className="mt-xs">
          Choose at least two runs in the results catalog.
        </Text>
        <Button onClick={onBack}>Open results catalog</Button>
      </div>
    );
  const runIDs = data?.runs.map((r) => r.id) ?? ids;
  const color = (id: string) =>
    palette[Math.max(0, runIDs.indexOf(id)) % palette.length];
  const regressions = data ? regressionCount(data.deltas) : 0;
  return (
    <>
      <div data-spt-legacy="" className="page-heading">
        <div data-spt-legacy="">
          <span data-spt-legacy="" className="eyebrow">
            COMPARE · {ids.length} RUNS
          </span>
          <Title order={1}>Compare runs</Title>
          <Text size="sm" className="mt-xs wrap-anywhere">
            Overlaid per-phase time series and totals, checked against the
            baseline run.
          </Text>
        </div>
        <div data-spt-legacy="" className="actions">
          <Button onClick={onBack}>Back to catalog</Button>
        </div>
      </div>
      {error && (
        <div data-spt-legacy="" className="error" role="alert">
          {error}
        </div>
      )}
      {!data ? (
        !error && (
          <Text size="xs" c="var(--spt-text-subtle)">
            Loading comparison…
          </Text>
        )
      ) : (
        <>
          <section
            data-spt-legacy=""
            className="panel compare-rule"
            aria-label="Regression rule"
          >
            <div data-spt-legacy="">
              <strong data-spt-legacy="">{data.rule.label}</strong>
              <Text size="sm" className="mt-xxs">
                {data.rule.description}
              </Text>
            </div>
            <label data-spt-legacy="">
              Baseline
              <select
                data-spt-legacy=""
                value={data.baseline}
                onChange={(e) => setBase(e.target.value)}
              >
                {data.runs.map((r) => (
                  <option data-spt-legacy="" key={r.id} value={r.id}>
                    {r.id}
                  </option>
                ))}
              </select>
            </label>
            <label data-spt-legacy="">
              Threshold %
              <input
                data-spt-legacy=""
                type="number"
                min={0}
                step={1}
                value={draft.threshold_pct ?? ""}
                onChange={(e) =>
                  setDraft({ ...draft, threshold_pct: e.target.value })
                }
              />
            </label>
            <label data-spt-legacy="">
              Latency floor µs
              <input
                data-spt-legacy=""
                type="number"
                min={0}
                value={draft.latency_floor_us ?? ""}
                onChange={(e) =>
                  setDraft({ ...draft, latency_floor_us: e.target.value })
                }
              />
            </label>
            <label data-spt-legacy="">
              Min operations
              <input
                data-spt-legacy=""
                type="number"
                min={0}
                value={draft.min_ops ?? ""}
                onChange={(e) =>
                  setDraft({ ...draft, min_ops: e.target.value })
                }
              />
            </label>
            <Button variant="filled" onClick={apply}>
              Apply
            </Button>
            <Text
              fw="var(--mantine-font-weight-semibold)"
              c={regressions ? "#b42318" : "#1f7a4d"}
              className="basis-full"
              role="status"
            >
              {regressions
                ? `${regressions} metric${regressions === 1 ? "" : "s"} past the threshold vs ${data.baseline}.`
                : `No metric past the threshold vs ${data.baseline}.`}{" "}
              Threshold check, not statistical significance.
            </Text>
          </section>
          {Object.entries(data.settings_differences ?? {}).some(
            ([, d]) => d.length,
          ) && (
            <section
              data-spt-legacy=""
              className="notice notice-warn"
              aria-label="Settings differences"
            >
              <strong data-spt-legacy="">
                Settings differ from the baseline; deltas may reflect
                configuration, not storage.
              </strong>
              <ul data-spt-legacy="">
                {Object.entries(data.settings_differences).map(([id, diffs]) =>
                  diffs.map((d) => (
                    <li data-spt-legacy="" key={id + d}>
                      {id}: {d}
                    </li>
                  )),
                )}
              </ul>
            </section>
          )}
          <section data-spt-legacy="" className="panel">
            <div data-spt-legacy="" className="run-log-heading">
              <Title order={2} className="flex-auto">
                Time series
              </Title>
              <div
                data-spt-legacy=""
                className="mode-switch"
                role="group"
                aria-label="Metric"
              >
                {metrics.map((m) => (
                  <button
                    data-spt-legacy=""
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
            <ul data-spt-legacy="" className="compare-legend">
              {data.runs.map((r) => (
                <li data-spt-legacy="" key={r.id}>
                  <i data-spt-legacy="" style={{ background: color(r.id) }} />
                  {r.id}
                  {r.id === data.baseline ? " (baseline)" : ""}
                </li>
              ))}
            </ul>
            <div data-spt-legacy="" className="compare-charts">
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
                <Text size="xs" c="var(--spt-text-subtle)">
                  None of the selected runs recorded interval metrics.
                </Text>
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
    <figure data-spt-legacy="" className="overlay-chart">
      <figcaption data-spt-legacy="">
        {phase} · {unit}
      </figcaption>
      {count === 0 ? (
        <Text size="xs" c="var(--spt-text-subtle)">
          No samples for this metric.
        </Text>
      ) : (
        <svg
          data-spt-legacy=""
          viewBox={`0 0 ${W} ${H}`}
          role="img"
          aria-label={`${phase} ${unit} over elapsed seconds for ${lines.length} runs`}
        >
          <line
            data-spt-legacy=""
            x1={left}
            y1={H - bottom}
            x2={W - right}
            y2={H - bottom}
            className="axis"
          />
          <line
            data-spt-legacy=""
            x1={left}
            y1={top}
            x2={left}
            y2={H - bottom}
            className="axis"
          />
          <text
            data-spt-legacy=""
            x={left - 6}
            y={top + 8}
            textAnchor="end"
            className="tick"
          >
            {fmt(yMax, yMax < 10 ? 1 : 0)}
          </text>
          <text
            data-spt-legacy=""
            x={left - 6}
            y={H - bottom}
            textAnchor="end"
            className="tick"
          >
            0
          </text>
          <text
            data-spt-legacy=""
            x={W - right}
            y={H - 8}
            textAnchor="end"
            className="tick"
          >
            {fmt(maxT, 0)} s
          </text>
          {lines.map((l) =>
            segments(l.points, metric).map((seg, i) =>
              seg.length === 1 ? (
                <circle
                  data-spt-legacy=""
                  key={l.id + i}
                  cx={x(seg[0].t)}
                  cy={y(seg[0][metric] as number)}
                  r={3}
                  fill={l.color}
                />
              ) : (
                <polyline
                  data-spt-legacy=""
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
    <section
      data-spt-legacy=""
      className="panel run-history-panel"
      aria-label="Delta table"
    >
      <Title order={2}>Deltas vs {baseline}</Title>
      {deltas.length === 0 ? (
        <Text size="xs" c="var(--spt-text-subtle)">
          No shared phases to compare.
        </Text>
      ) : (
        <div data-spt-legacy="" className="table-wrap">
          <table data-spt-legacy="" className="run-history-table delta-table">
            <thead data-spt-legacy="">
              <tr data-spt-legacy="">
                <th data-spt-legacy="">Phase</th>
                <th data-spt-legacy="">Metric</th>
                <th data-spt-legacy="">Run</th>
                <th data-spt-legacy="">Baseline</th>
                <th data-spt-legacy="">Value</th>
                <th data-spt-legacy="">Δ</th>
                <th data-spt-legacy="">Flag</th>
              </tr>
            </thead>
            <tbody data-spt-legacy="">
              {[...grouped].flatMap(([phase, byMetric]) =>
                [...byMetric].flatMap(([, rows]) =>
                  rows.map((d) => (
                    <tr
                      data-spt-legacy=""
                      key={`${phase}-${d.metric}-${d.run_id}`}
                    >
                      <td data-spt-legacy="">{phase}</td>
                      <td data-spt-legacy="">{d.label}</td>
                      <td data-spt-legacy="" className="history-run-id">
                        {d.run_id}
                      </td>
                      <td data-spt-legacy="">{fmt(d.baseline, 2)}</td>
                      <td data-spt-legacy="">{fmt(d.value, 2)}</td>
                      <td data-spt-legacy="">{deltaText(d)}</td>
                      <td data-spt-legacy="">
                        <Badge
                          color={
                            d.flag === "regressed"
                              ? "red"
                              : d.flag === "improved"
                                ? "teal"
                                : [
                                      "not_comparable",
                                      "insufficient",
                                      "no_data",
                                    ].includes(d.flag)
                                  ? "yellow"
                                  : "gray"
                          }
                          radius="xl"
                          title={d.reason}
                        >
                          {flagText[d.flag] ?? d.flag}
                        </Badge>
                        {d.reason && (
                          <small data-spt-legacy="">{d.reason}</small>
                        )}
                      </td>
                    </tr>
                  )),
                ),
              )}
            </tbody>
          </table>
        </div>
      )}
      <Text size="caption" className="px-comfortable pb-md">
        Flags are a threshold check, not statistical significance. One run per
        side cannot show variance; repeat runs before drawing conclusions.
      </Text>
    </section>
  );
}
