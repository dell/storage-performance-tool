import type {
  CatalogRun,
  CompareDelta,
  CompareFlag,
  Job,
  Metrics,
  SeriesPoint,
} from "../types";

export const maxCompare = 6;

export type CatalogFilters = {
  q: string;
  source: "" | "dashboard" | "cli";
  status: "" | "complete" | "partial" | "malformed" | "failed";
};

export function catalogQuery(f: CatalogFilters): string {
  const p = new URLSearchParams();
  if (f.q.trim()) p.set("q", f.q.trim());
  if (f.source) p.set("source", f.source);
  if (f.status) p.set("status", f.status);
  const s = p.toString();
  return s ? `/api/catalog?${s}` : "/api/catalog";
}

export type RuleOverrides = {
  threshold_pct?: number;
  latency_floor_us?: number;
  min_ops?: number;
};

export function compareQuery(
  ids: string[],
  baseline: string,
  rule: RuleOverrides = {},
): string {
  const p = new URLSearchParams();
  for (const id of ids) p.append("id", id);
  if (baseline) p.set("baseline", baseline);
  for (const [k, v] of Object.entries(rule))
    if (typeof v === "number" && Number.isFinite(v)) p.set(k, String(v));
  return `/api/catalog/compare?${p.toString()}`;
}

/** Toggle a run in the compare selection, keeping at most maxCompare runs. */
export function toggleSelection(selected: string[], id: string): string[] {
  if (selected.includes(id)) return selected.filter((s) => s !== id);
  if (selected.length >= maxCompare) return selected;
  return [...selected, id];
}

export const flagText: Record<CompareFlag, string> = {
  regressed: "Regressed",
  improved: "Improved",
  within: "Within threshold",
  insufficient: "Too few operations",
  no_data: "No data",
  not_comparable: "Not comparable",
};

export function deltaText(d: Pick<CompareDelta, "delta_pct">): string {
  if (d.delta_pct === null || !Number.isFinite(d.delta_pct)) return "—";
  const sign = d.delta_pct > 0 ? "+" : "";
  return `${sign}${d.delta_pct.toFixed(1)}%`;
}

/** Group deltas by phase, then by metric, in the server's order. */
export function groupDeltas(
  deltas: CompareDelta[],
): Map<string, Map<string, CompareDelta[]>> {
  const out = new Map<string, Map<string, CompareDelta[]>>();
  for (const d of deltas) {
    const phase = out.get(d.phase) ?? new Map<string, CompareDelta[]>();
    const list = phase.get(d.metric) ?? [];
    list.push(d);
    phase.set(d.metric, list);
    out.set(d.phase, phase);
  }
  return out;
}

export function regressionCount(deltas: CompareDelta[]): number {
  return deltas.filter((d) => d.flag === "regressed").length;
}

export function phasesOf(
  series: Record<string, Record<string, SeriesPoint[]>>,
): string[] {
  const order = ["CREATE", "WRITE", "READ", "VERIFY", "DELETE"];
  const keys = new Set<string>();
  for (const run of Object.values(series))
    for (const k of Object.keys(run)) keys.add(k);
  return [...keys].sort((a, b) => {
    const ia = order.indexOf(a.split(":")[0]),
      ib = order.indexOf(b.split(":")[0]);
    return (ia < 0 ? 99 : ia) - (ib < 0 ? 99 : ib) || a.localeCompare(b);
  });
}

export type OverlayMetric =
  "iops" | "bandwidth_mib_s" | "p50_latency_us" | "p99_latency_us";

/** Extent of all non-null values across the overlaid runs, for a shared axis. */
export function overlayExtent(
  lines: SeriesPoint[][],
  metric: OverlayMetric,
): { maxT: number; maxV: number; count: number } {
  let maxT = 0,
    maxV = 0,
    count = 0;
  for (const line of lines)
    for (const p of line) {
      const v = p[metric];
      if (v === null || !Number.isFinite(v)) continue;
      count++;
      maxT = Math.max(maxT, p.t);
      maxV = Math.max(maxV, v);
    }
  return { maxT, maxV, count };
}

/** Split a series into runs of consecutive non-null values so gaps stay visible. */
export function segments(
  points: SeriesPoint[],
  metric: OverlayMetric,
): SeriesPoint[][] {
  const out: SeriesPoint[][] = [];
  let part: SeriesPoint[] = [];
  for (const p of points) {
    if (p[metric] === null || !Number.isFinite(p[metric] as number)) {
      if (part.length) out.push(part);
      part = [];
    } else part.push(p);
  }
  if (part.length) out.push(part);
  return out;
}

export function runSettings(run: CatalogRun): string {
  const parts = [
    run.workload,
    run.object_count !== undefined && run.object_size
      ? `${run.object_count} × ${run.object_size}`
      : run.object_size,
    run.threads !== undefined
      ? `${run.threads} thread${run.threads === 1 ? "" : "s"}`
      : "",
    run.host_count !== undefined
      ? `${run.host_count} host${run.host_count === 1 ? "" : "s"}`
      : "",
  ];
  return parts.filter(Boolean).join(" · ") || "Settings not recorded";
}

export function logURL(
  base: string,
  level: "all" | "warn" | "error",
  tail: number,
  extra: Record<string, string> = {},
): string {
  const [path, existing = ""] = base.split("?");
  const p = new URLSearchParams(existing);
  for (const [k, v] of Object.entries(extra)) p.set(k, v);
  p.set("level", level);
  p.set("tail", String(tail));
  return `${path}?${p.toString()}`;
}

/** Human label for metrics.workload_type ("write-verify", "mock", or a saved label). */
export function workloadLabel(type?: string): string {
  const t = (type ?? "").trim();
  if (!t || t === "write-verify") return "Write + verify";
  if (t === "mock") return "Mock driver";
  return t.charAt(0).toUpperCase() + t.slice(1);
}

/**
 * Workload type to label for a run: the measured run's own type, else the job's
 * chosen driver. A mock run stopped during setup has no metrics yet and must not
 * read as write + verify.
 */
export function runWorkloadType(
  metrics?: Pick<Metrics, "workload_type"> | null,
  job?: Pick<Job, "options"> | null,
): string | undefined {
  if (metrics?.workload_type) return metrics.workload_type;
  return job?.options?.driver === "mock" ? "mock" : undefined;
}
