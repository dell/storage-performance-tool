import { useState } from "react";
import type { Sample } from "../types";
import { formatNumber } from "../utils/metrics";
export function TrendChart({
  samples,
  title,
  unit,
  series,
  active,
  onInspect,
}: {
  samples: Sample[];
  title: string;
  unit: string;
  series: {
    key: "throughput" | "bandwidth" | "p50" | "p99";
    label: string;
    color: string;
  }[];
  active: number | null;
  onInspect: (index: number | null) => void;
}) {
  const [focused, setFocused] = useState(false);
  const values = samples.flatMap((s) =>
    series.map((line) => s[line.key]).filter((v): v is number => v !== null),
  );
  const max = Math.max(1, ...values) * 1.12,
    first = samples[0]?.elapsed ?? 0,
    last = samples.at(-1)?.elapsed ?? 1,
    span = Math.max(1, last - first);
  const x = (s: Sample) => 52 + ((s.elapsed - first) / span) * 700,
    y = (v: number) => 154 - (v / max) * 130;
  const inspected = active === null ? samples.at(-1) : samples[active];
  return (
    <article className="chart">
      <div className="chart-heading">
        <h3>{title}</h3>
        <span>{unit}</span>
      </div>
      <div className="legend">
        {series.map((line) => (
          <span key={line.key}>
            <i style={{ background: line.color }} />
            {line.label}{" "}
            <strong>
              {formatNumber(
                inspected?.[line.key] ?? null,
                unit === "ms" ? 2 : 0,
              )}
            </strong>
          </span>
        ))}
      </div>
      {values.length === 0 ? (
        <div className="empty-chart">Not applicable to this operation</div>
      ) : (
        <svg
          viewBox="0 0 780 190"
          role="img"
          aria-label={`${title} over elapsed time`}
          onPointerMove={(e) => {
            const r = e.currentTarget.getBoundingClientRect(),
              target =
                first +
                Math.min(
                  1,
                  Math.max(
                    0,
                    (((e.clientX - r.left) / r.width) * 780 - 52) / 700,
                  ),
                ) *
                  span;
            onInspect(
              samples.reduce(
                (best, s, i) =>
                  Math.abs(s.elapsed - target) <
                  Math.abs(samples[best].elapsed - target)
                    ? i
                    : best,
                0,
              ),
            );
          }}
          onPointerLeave={() => {
            if (!focused) onInspect(null);
          }}
        >
          {[0, 0.5, 1].map((f) => (
            <g key={f}>
              <line
                x1="52"
                x2="752"
                y1={y(max * f)}
                y2={y(max * f)}
                className="grid"
              />
              <text x="44" y={y(max * f) + 4} textAnchor="end">
                {formatNumber(max * f, max < 20 ? 1 : 0)}
              </text>
            </g>
          ))}
          {series.map((line) => (
            <polyline
              key={line.key}
              fill="none"
              stroke={line.color}
              strokeWidth="2.5"
              points={samples
                .filter((s) => s[line.key] !== null)
                .map((s) => `${x(s)},${y(s[line.key]!)}`)
                .join(" ")}
            />
          ))}
          {[0, 0.5, 1].map((f) => (
            <text
              key={f}
              x={52 + 700 * f}
              y="179"
              textAnchor={f === 0 ? "start" : f === 1 ? "end" : "middle"}
            >
              {Math.round(first + span * f)}s
            </text>
          ))}
          {active !== null && samples[active] && (
            <line
              x1={x(samples[active])}
              x2={x(samples[active])}
              y1="24"
              y2="154"
              stroke="#8393a8"
              strokeDasharray="4 4"
            />
          )}
        </svg>
      )}
      <label className="chart-inspector">
        Inspect time{" "}
        <input
          aria-label={`Inspect ${title} sample`}
          type="range"
          min="0"
          max={Math.max(0, samples.length - 1)}
          value={active ?? Math.max(0, samples.length - 1)}
          onFocus={() => setFocused(true)}
          onBlur={() => {
            setFocused(false);
            onInspect(null);
          }}
          onChange={(e) => onInspect(Number(e.target.value))}
        />
      </label>
    </article>
  );
}
