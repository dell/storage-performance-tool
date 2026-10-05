import { useState } from "react";
import type { Sample } from "../types";
import { formatNumber } from "../utils/metrics";
import { panWindow, zoomWindow } from "../utils/timeWindow";
const lines = [
  { key: "throughput", label: "Throughput", unit: "ops/s", color: "#2273d6" },
  { key: "bandwidth", label: "Bandwidth", unit: "MiB/s", color: "#148a82" },
  { key: "p99", label: "P99 latency", unit: "ms", color: "#9b64ce" },
] as const;
export function FleetTelemetry({ samples }: { samples: Sample[] }) {
  const [window, setWindow] = useState<[number, number] | null>(null);
  const [follow, setFollow] = useState(true),
    [span, setSpan] = useState(60);
  const [enabled, setEnabled] = useState<string[]>(lines.map((l) => l.key));
  const [inspected, setInspected] = useState<number | null>(null);
  const end = samples.at(-1)?.elapsed ?? 0,
    min = samples[0]?.elapsed ?? 0;
  const bounds: [number, number] = follow
    ? [Math.max(min, end - span), end]
    : (window ?? [min, end]);
  const shown = samples.filter(
    (s) => s.elapsed >= bounds[0] && s.elapsed <= bounds[1],
  );
  const chosen = lines.filter((l) => enabled.includes(l.key));
  const maxima = Object.fromEntries(
    lines.map((l) => [
      l.key,
      Math.max(1, ...shown.map((s) => s[l.key] ?? 0)) * 1.1,
    ]),
  );
  const point =
    inspected === null
      ? shown.at(-1)
      : shown.reduce<Sample | undefined>(
          (best, s) =>
            !best ||
            Math.abs(s.elapsed - inspected) < Math.abs(best.elapsed - inspected)
              ? s
              : best,
          undefined,
        );
  const x = (t: number) =>
    52 + ((t - bounds[0]) / Math.max(1, bounds[1] - bounds[0])) * 700;
  const change = (next: [number, number]) => {
    setFollow(false);
    setWindow(next);
    setInspected(null);
  };
  return (
    <section className="panel fleet">
      <div className="section-heading">
        <div>
          <h2>Fleet telemetry</h2>
          <p>
            Throughput, bandwidth and latency aligned to the same elapsed time
          </p>
        </div>
        <span className="status good">
          {follow ? "Following latest" : "Inspecting history"}
        </span>
      </div>
      <div className="fleet-toolbar">
        <div className="button-group">
          <button
            aria-label="Pan earlier"
            onClick={() => change(panWindow(bounds, -0.35, min, end))}
          >
            ←
          </button>
          <button
            aria-label="Zoom in"
            onClick={() => change(zoomWindow(bounds, 0.65, min, end))}
          >
            Zoom +
          </button>
          <button
            aria-label="Zoom out"
            onClick={() => change(zoomWindow(bounds, 1 / 0.65, min, end))}
          >
            Zoom −
          </button>
          <button
            aria-label="Pan later"
            onClick={() => change(panWindow(bounds, 0.35, min, end))}
          >
            →
          </button>
          <button onClick={() => change([min, end])}>Fit run</button>
        </div>
        <label className="check-label">
          <input
            type="checkbox"
            checked={follow}
            onChange={(e) => {
              setFollow(e.target.checked);
              setWindow(bounds);
            }}
          />
          Follow latest
        </label>
        <select
          aria-label="Following window"
          value={span}
          onChange={(e) => {
            setSpan(Number(e.target.value));
            setFollow(true);
            setInspected(null);
          }}
        >
          <option value={30}>30 seconds</option>
          <option value={60}>60 seconds</option>
          <option value={300}>5 minutes</option>
        </select>
      </div>
      <div className="fleet-legend">
        {lines.map((l) => (
          <label key={l.key} className="check-label">
            <input
              type="checkbox"
              checked={enabled.includes(l.key)}
              onChange={(e) =>
                setEnabled(
                  e.target.checked
                    ? [...enabled, l.key]
                    : enabled.filter((k) => k !== l.key),
                )
              }
            />
            <i style={{ background: l.color }} />
            <span>
              {l.label}
              <strong>
                {formatNumber(point?.[l.key] ?? null, l.unit === "ms" ? 2 : 0)}{" "}
                {l.unit}
              </strong>
              <small>
                Scale 0–{formatNumber(maxima[l.key], l.unit === "ms" ? 1 : 0)}{" "}
                {l.unit}
              </small>
            </span>
          </label>
        ))}
      </div>
      <p className="scale-note">
        Each line uses its own displayed scale. Compare the timing of changes,
        not the vertical values between metrics.
      </p>
      {shown.length === 0 ? (
        <div className="empty-chart">No samples in this time window</div>
      ) : chosen.length === 0 ? (
        <div className="empty-chart">Select a metric above</div>
      ) : (
        <svg
          className="fleet-plot"
          viewBox="0 0 780 245"
          role="img"
          aria-label="Aligned fleet telemetry with independent metric scales"
          onPointerMove={(e) => {
            const r = e.currentTarget.getBoundingClientRect();
            setInspected(
              bounds[0] +
                Math.max(
                  0,
                  Math.min(
                    1,
                    (((e.clientX - r.left) / r.width) * 780 - 52) / 700,
                  ),
                ) *
                  (bounds[1] - bounds[0]),
            );
          }}
          onPointerLeave={() => setInspected(null)}
        >
          {[0, 0.5, 1].map((f) => (
            <g key={f}>
              <line
                x1="52"
                x2="752"
                y1={205 - f * 170}
                y2={205 - f * 170}
                className="grid"
              />
              <text x="44" y={209 - f * 170} textAnchor="end">
                {Math.round(f * 100)}%
              </text>
            </g>
          ))}
          {chosen.map((l) => (
            <polyline
              key={l.key}
              fill="none"
              stroke={l.color}
              strokeWidth="2.5"
              strokeDasharray={l.key === "p99" ? "7 3" : undefined}
              points={shown
                .filter((s) => s[l.key] !== null)
                .map(
                  (s) =>
                    `${x(s.elapsed)},${205 - ((s[l.key] ?? 0) / maxima[l.key]) * 170}`,
                )
                .join(" ")}
            />
          ))}
          {[0, 0.25, 0.5, 0.75, 1].map((f) => (
            <text
              key={f}
              x={52 + 700 * f}
              y="231"
              textAnchor={f === 0 ? "start" : f === 1 ? "end" : "middle"}
            >
              {Math.round(bounds[0] + (bounds[1] - bounds[0]) * f)}s
            </text>
          ))}
          {inspected !== null && point && (
            <line
              x1={x(point.elapsed)}
              x2={x(point.elapsed)}
              y1="35"
              y2="205"
              stroke="#8393a8"
              strokeDasharray="4 4"
            />
          )}
        </svg>
      )}
      <div className="window-controls">
        <label>
          Window start
          <input
            aria-label="Window start"
            type="range"
            min={min}
            max={Math.max(min, end)}
            step="1"
            value={bounds[0]}
            onChange={(e) =>
              change([
                Math.min(Number(e.target.value), Math.max(min, bounds[1] - 1)),
                bounds[1],
              ])
            }
          />
        </label>
        <label>
          Window end
          <input
            aria-label="Window end"
            type="range"
            min={min}
            max={Math.max(min, end)}
            step="1"
            value={bounds[1]}
            onChange={(e) =>
              change([
                bounds[0],
                Math.max(Number(e.target.value), Math.min(end, bounds[0] + 1)),
              ])
            }
          />
        </label>
      </div>
      <p className="subtle" aria-live="polite">
        Window {formatNumber(bounds[0], 1)}–{formatNumber(bounds[1], 1)}s /{" "}
        {end}s · {point ? `Selected sample ${point.elapsed}s` : "No sample"} ·{" "}
        {shown.length} samples
      </p>
    </section>
  );
}
