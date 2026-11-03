// Client-side mirrors of the dashboard server's input rules (main.go, storage.go).
// They give immediate feedback only; the server validates every request again.
import type { Options, RunLimits } from "../types";

export const sizeUnits = { KiB: 10, MiB: 20, GiB: 30 } as const;
export type SizeUnit = keyof typeof sizeUnits;
const sizePattern = /^([1-9][0-9]{0,6})(KiB|MiB|GiB)$/;

// Same values the server publishes; used until the first /api/state arrives.
export const fallbackLimits: RunLimits = {
  guided: {
    object_counts: [10, 25, 50, 100, 250, 320],
    object_sizes: ["64KiB", "256KiB", "1MiB", "4MiB", "16MiB"],
    threads: [1, 2],
    max_runtime_seconds: [60, 120, 300, 600],
  },
  expert: {
    object_count: { min: 1, max: 100_000 },
    object_size_bytes: { min: 1024, max: 1024 ** 3 },
    threads: { min: 1, max: 64 },
    max_runtime_seconds: { min: 10, max: 3600 },
    max_total_bytes: 100 * 1024 ** 3,
  },
};

export function parseSize(value: string): number | null {
  const m = sizePattern.exec(value);
  return m ? Number(m[1]) * 2 ** sizeUnits[m[2] as SizeUnit] : null;
}

export function splitSize(value: string): { amount: string; unit: SizeUnit } {
  const m = sizePattern.exec(value);
  return m
    ? { amount: m[1], unit: m[2] as SizeUnit }
    : { amount: "", unit: "MiB" };
}

export function formatBytes(bytes: number): string {
  for (const unit of ["GiB", "MiB", "KiB"] as const)
    if (bytes >= 2 ** sizeUnits[unit] && bytes % 2 ** sizeUnits[unit] === 0)
      return `${bytes / 2 ** sizeUnits[unit]}${unit}`;
  return `${bytes} bytes`;
}

// Parses a whole number typed by the user; anything else (blank, "1e3", "2.5") is null.
export function parseWhole(text: string): number | null {
  return /^[0-9]{1,9}$/.test(text.trim()) ? Number(text.trim()) : null;
}

export type OptionErrors = Partial<Record<keyof Options, string>>;

export function validateOptions(o: Options, limits: RunLimits): OptionErrors {
  const errors: OptionErrors = {};
  if (o.mode === "expert") {
    const e = limits.expert;
    const inRange = (v: number, r: { min: number; max: number }) =>
      Number.isInteger(v) && v >= r.min && v <= r.max;
    if (!inRange(o.object_count, e.object_count))
      errors.object_count = `Enter a whole number from ${e.object_count.min} to ${e.object_count.max.toLocaleString("en-US")}.`;
    const size = parseSize(o.object_size);
    if (size === null || !inRange(size, e.object_size_bytes))
      errors.object_size = `Enter a whole size from ${formatBytes(e.object_size_bytes.min)} to ${formatBytes(e.object_size_bytes.max)}.`;
    if (!inRange(o.threads, e.threads))
      errors.threads = `Enter a whole number from ${e.threads.min} to ${e.threads.max}.`;
    if (!inRange(o.max_runtime_seconds, e.max_runtime_seconds))
      errors.max_runtime_seconds = `Enter ${e.max_runtime_seconds.min} to ${e.max_runtime_seconds.max} seconds.`;
    if (
      size !== null &&
      !errors.object_count &&
      o.object_count * size > e.max_total_bytes
    )
      errors.object_count = `Objects × size must not exceed ${formatBytes(e.max_total_bytes)} of test data.`;
    return errors;
  }
  const g = limits.guided;
  if (!g.object_counts.includes(o.object_count))
    errors.object_count = "Choose one of the listed object counts.";
  if (!g.object_sizes.includes(o.object_size))
    errors.object_size = "Choose one of the listed object sizes.";
  if (!g.threads.includes(o.threads))
    errors.threads = "Choose one of the listed thread counts.";
  if (!g.max_runtime_seconds.includes(o.max_runtime_seconds))
    errors.max_runtime_seconds = "Choose one of the listed time limits.";
  return errors;
}

const userPattern = /^[A-Za-z_][A-Za-z0-9_-]{0,31}$/;
const labelPattern = /^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?$/;
const ipv4Pattern =
  /^(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}$/;

export function hostAddress(entry: string): string {
  const host = entry.slice(entry.lastIndexOf("@") + 1);
  return host === "localhost" ? "127.0.0.1" : host;
}

// Mirrors validateHostEntry: host or user@host, where host is an IP address or DNS name.
export function hostError(entry: string): string | null {
  const at = entry.lastIndexOf("@");
  if (at >= 0 && !userPattern.test(entry.slice(0, at)))
    return "The user name is not valid.";
  const host = entry.slice(at + 1);
  if (
    ipv4Pattern.test(host) ||
    (host.includes(":") && /^[0-9A-Fa-f:.]+$/.test(host))
  )
    return null;
  if (!host || host.length > 253) return "The host name is not valid.";
  const labels = host.replace(/\.$/, "").split(".");
  return labels.every((l) => labelPattern.test(l))
    ? null
    : "The host name is not valid.";
}

// Mirrors validateEndpoint: an HTTP(S) URL without credentials, query, fragment, or whitespace.
export function endpointError(value: string): string | null {
  if (!value) return null;
  if (/\s/.test(value)) return "Remove spaces from the endpoint.";
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    return "Enter a full URL such as https://s3.example.com.";
  }
  if (url.protocol !== "https:" && url.protocol !== "http:")
    return "The endpoint must use http:// or https://.";
  if (url.username || url.password)
    return "Do not put credentials in the endpoint URL.";
  if (url.search || url.hash || value.includes("?") || value.includes("#"))
    return "Remove the query string or fragment from the endpoint.";
  if (!url.hostname) return "Enter a host name in the endpoint.";
  return null;
}

// Per-row host errors, including duplicates of the same address.
export function hostErrors(
  hosts: string[],
  maxHosts: number,
): (string | null)[] {
  const seen = new Set<string>();
  let counted = 0;
  return hosts.map((raw) => {
    const entry = raw.trim();
    if (!entry) return null;
    counted += 1;
    if (counted > maxHosts) return `List at most ${maxHosts} client machines.`;
    const error = hostError(entry);
    if (error) return error;
    const key = hostAddress(entry).toLowerCase();
    if (seen.has(key)) return "This machine is already listed.";
    seen.add(key);
    return null;
  });
}
