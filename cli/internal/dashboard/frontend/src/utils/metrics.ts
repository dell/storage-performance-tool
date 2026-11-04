import type { DataStatus, Row } from "../types";
export function numeric(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}
export function formatNumber(value: unknown, digits = 0): string {
  const n = numeric(value);
  return n === null
    ? "—"
    : new Intl.NumberFormat("en-US", { maximumFractionDigits: digits }).format(
        n,
      );
}
/** AC1: missing/delayed telemetry must not look like a measured zero. */
export function formatMetric(
  value: unknown,
  digits = 0,
  status?: DataStatus,
): string {
  if (status === "no_data") return "No data";
  if (status === "delayed" && numeric(value) === null) return "Delayed";
  if (status === "delayed" && numeric(value) !== null) {
    return `${formatNumber(value, digits)} (delayed)`;
  }
  return formatNumber(value, digits);
}
export function elapsed(value: unknown): string {
  const n = numeric(value);
  if (n === null) return "—";
  if (n < 60) return `${formatNumber(n, 1)}s`;
  const s = Math.floor(n),
    d = Math.floor(s / 86400),
    h = Math.floor(s / 3600) % 24,
    m = Math.floor(s / 60) % 60;
  return [
    d ? `${d}d` : "",
    h ? `${h}h` : "",
    m ? `${m}m` : "",
    !d && !h ? `${s % 60}s` : "",
  ]
    .filter(Boolean)
    .join(" ");
}
export function phaseCode(name?: string) {
  const s = (name ?? "").toUpperCase();
  return /CREATE|WRITE/.test(s)
    ? "CREATE"
    : /VERIFY|READ/.test(s)
      ? "VERIFY"
      : /DELETE|CLEANUP/.test(s)
        ? "DELETE"
        : "SPT";
}
export function phaseName(name?: string) {
  return { CREATE: "Write", VERIFY: "Verify", DELETE: "Cleanup", SPT: "SPT" }[
    phaseCode(name)
  ];
}
export function utc(value?: string) {
  const t = Date.parse(value ?? "");
  return Number.isFinite(t)
    ? new Date(t).toISOString().replace("T", " ").replace("Z", " UTC")
    : "—";
}
