import { useState } from "react";
import type { Row, State } from "../types";
import { defaults } from "../config";
import { formatNumber as fmt, numeric, phaseName } from "../utils/metrics";

type Limits = {
  iops: number | null;
  bandwidth: number | null;
  latency: number | null;
};
const noLimits: Limits = { iops: null, bandwidth: null, latency: null };

type Props = {
  state: State;
  row?: Row;
  phase: string;
};

function loadLimits(key: string): Limits {
  try {
    const saved = JSON.parse(localStorage.getItem(key) ?? "null") ?? noLimits;
    return {
      iops: numeric(saved.iops),
      bandwidth: numeric(saved.bandwidth),
      latency: numeric(saved.latency),
    };
  } catch {
    return noLimits;
  }
}

export function PerformanceLimits(props: Props) {
  const { state } = props;
  const o = state.job?.options ?? defaults;
  const key = [
    "spt-performance-limits-v1",
    `${state.target.endpoint_label ?? ""}/${state.target.bucket ?? ""}`,
    o.object_size,
    o.threads,
  ].join(":");
  return (
    <details data-spt-legacy="" className="panel limits">
      <LimitsForm key={key} {...props} storageKey={key} />
    </details>
  );
}

function LimitsForm({
  state,
  row,
  phase,
  storageKey: key,
}: Props & { storageKey: string }) {
  const o = state.job?.options ?? defaults;
  const [limits, setLimits] = useState<Limits>(() => loadLimits(key)),
    [draft, setDraft] = useState(() => ({
      iops: limits.iops === null ? "" : String(limits.iops),
      bandwidth: limits.bandwidth === null ? "" : String(limits.bandwidth),
      latency: limits.latency === null ? "" : String(limits.latency),
    })),
    [message, setMessage] = useState("");
  const checks = [
    {
      key: "iops" as const,
      value: numeric(row?.interval_iops),
      label: "IOPS",
      minimum: true,
    },
    {
      key: "bandwidth" as const,
      value: numeric(row?.bandwidth_mib_s),
      label: "Bandwidth",
      minimum: true,
    },
    {
      key: "latency" as const,
      value: numeric(row?.p50_latency_us),
      label: "P50 latency",
      minimum: false,
    },
  ];
  function save() {
    const parsed = Object.fromEntries(
      Object.entries(draft).map(([k, v]) => [
        k,
        v.trim() === "" ? null : Number(v),
      ]),
    ) as Limits;
    if (
      Object.values(parsed).some(
        (v) => v !== null && (!Number.isFinite(v) || v < 0),
      )
    ) {
      setMessage("Enter zero or a positive number.");
      return;
    }
    try {
      localStorage.setItem(key, JSON.stringify(parsed));
      setLimits(parsed);
      setMessage("Limits saved in this browser.");
    } catch {
      setMessage("Browser storage unavailable; limits could not be saved.");
    }
  }
  return (
    <>
      <summary data-spt-legacy="">
        Performance limits{" "}
        <span data-spt-legacy="" className="summary-note">
          {phaseName(phase)} ·{" "}
          {checks.filter((c) => limits[c.key] !== null).length}/3 set
        </span>
      </summary>
      <p data-spt-legacy="" className="form-help">
        Set your team’s expectations for {state.target.endpoint_label},{" "}
        {o.object_size} objects, and {o.threads} thread
        {o.threads === 1 ? "" : "s"}. These limits stay in this browser and
        apply to the selected phase.
      </p>
      <div data-spt-legacy="" className="form-grid limits-grid">
        {checks.map((c) => {
          const limit = limits[c.key],
            value = c.value;
          const result =
            limit === null
              ? "No limit set"
              : value === null
                ? "Not measured"
                : (c.minimum ? value >= limit : value <= limit)
                  ? "Within limit"
                  : "Outside limit";
          return (
            <label data-spt-legacy="" key={c.key}>
              {c.minimum ? "Minimum" : "Maximum"} {c.label} (
              {c.key === "iops"
                ? "ops/s"
                : c.key === "bandwidth"
                  ? "MiB/s"
                  : "µs"}
              )
              <input
                data-spt-legacy=""
                type="number"
                min="0"
                step="any"
                placeholder="No limit set"
                value={draft[c.key]}
                onChange={(e) =>
                  setDraft({ ...draft, [c.key]: e.target.value })
                }
              />
              <span
                data-spt-legacy=""
                className={result === "Outside limit" ? "limit-bad" : "subtle"}
              >
                {result} · measured {fmt(value, 2)}
              </span>
            </label>
          );
        })}
      </div>
      <div data-spt-legacy="" className="actions">
        <button data-spt-legacy="" className="primary" onClick={save}>
          Save limits
        </button>
        <button
          data-spt-legacy=""
          onClick={() => {
            try {
              localStorage.removeItem(key);
              setLimits(noLimits);
              setDraft({ iops: "", bandwidth: "", latency: "" });
              setMessage("Limits cleared.");
            } catch {
              setMessage("Browser storage unavailable.");
            }
          }}
        >
          Clear limits
        </button>
        <span data-spt-legacy="" className="subtle" role="status">
          {message}
        </span>
      </div>
    </>
  );
}
