import { describe, expect, it } from "vitest";
import type { CatalogRun, CompareDelta, SeriesPoint } from "../types";
import {
  catalogQuery,
  compareQuery,
  deltaText,
  groupDeltas,
  logURL,
  maxCompare,
  overlayExtent,
  phasesOf,
  runWorkloadType,
  regressionCount,
  runSettings,
  segments,
  toggleSelection,
  workloadLabel,
} from "./catalog";

const pt = (t: number, iops: number | null): SeriesPoint => ({
  t,
  iops,
  bandwidth_mib_s: null,
  p50_latency_us: null,
  p99_latency_us: null,
});

const delta = (over: Partial<CompareDelta>): CompareDelta => ({
  run_id: "b",
  phase: "WRITE",
  metric: "iops",
  label: "IOPS",
  baseline: 100,
  value: 90,
  delta_pct: -10,
  flag: "regressed",
  ...over,
});

describe("catalog queries", () => {
  it("omits empty filters and trims search", () => {
    expect(catalogQuery({ q: "", source: "", status: "" })).toBe(
      "/api/catalog",
    );
    expect(
      catalogQuery({ q: "  1MiB ", source: "cli", status: "failed" }),
    ).toBe("/api/catalog?q=1MiB&source=cli&status=failed");
  });
  it("repeats id and only sends finite rule overrides", () => {
    expect(
      compareQuery(["a", "b c"], "a", {
        threshold_pct: 5,
        latency_floor_us: Number.NaN,
        min_ops: 0,
      }),
    ).toBe(
      "/api/catalog/compare?id=a&id=b+c&baseline=a&threshold_pct=5&min_ops=0",
    );
  });
  it("adds level and tail to log URLs without dropping the id", () => {
    expect(logURL("/api/catalog/log?id=x", "warn", 200)).toBe(
      "/api/catalog/log?id=x&level=warn&tail=200",
    );
    expect(logURL("/api/runs/r/log", "error", 50, { source: "console" })).toBe(
      "/api/runs/r/log?source=console&level=error&tail=50",
    );
  });
});

describe("selection", () => {
  it("toggles and caps at maxCompare", () => {
    let s: string[] = [];
    for (let i = 0; i < maxCompare + 2; i++) s = toggleSelection(s, `r${i}`);
    expect(s).toHaveLength(maxCompare);
    expect(toggleSelection(s, "r0")).not.toContain("r0");
  });
});

describe("deltas", () => {
  it("formats signed percentages and missing values", () => {
    expect(deltaText({ delta_pct: 3.333 })).toBe("+3.3%");
    expect(deltaText({ delta_pct: -10 })).toBe("-10.0%");
    expect(deltaText({ delta_pct: null })).toBe("—");
  });
  it("groups by phase then metric and counts regressions", () => {
    const ds = [
      delta({}),
      delta({ metric: "p99_latency_us", flag: "within" }),
      delta({ phase: "READ", flag: "improved" }),
      delta({ run_id: "c" }),
    ];
    const g = groupDeltas(ds);
    expect([...g.keys()]).toEqual(["WRITE", "READ"]);
    expect(g.get("WRITE")?.get("iops")).toHaveLength(2);
    expect(regressionCount(ds)).toBe(2);
  });
});

describe("series helpers", () => {
  it("orders phases by SPT step order", () => {
    expect(
      phasesOf({ a: { DELETE: [], READ: [] }, b: { CREATE: [], ZZZ: [] } }),
    ).toEqual(["CREATE", "READ", "DELETE", "ZZZ"]);
  });
  it("ignores nulls in the shared extent", () => {
    expect(
      overlayExtent([[pt(0, 5), pt(4, null)], [pt(2, 9)]], "iops"),
    ).toEqual({
      maxT: 2,
      maxV: 9,
      count: 2,
    });
    expect(overlayExtent([[pt(0, null)]], "iops").count).toBe(0);
  });
  it("splits series at gaps", () => {
    const s = segments([pt(0, 1), pt(1, 2), pt(2, null), pt(3, 4)], "iops");
    expect(s.map((p) => p.length)).toEqual([2, 1]);
  });
});

describe("labels", () => {
  it("names workloads", () => {
    expect(workloadLabel(undefined)).toBe("Write + verify");
    expect(workloadLabel("write-verify")).toBe("Write + verify");
    expect(workloadLabel("mock")).toBe("Mock driver");
    expect(workloadLabel("live SPT run")).toBe("Live SPT run");
  });
  it("labels a mock run stopped before its first metrics as the mock driver", () => {
    const job = {
      options: {
        object_count: 320,
        object_size: "16MiB",
        threads: 1,
        max_runtime_seconds: 600,
        driver: "mock" as const,
      },
    };
    expect(workloadLabel(runWorkloadType(null, job))).toBe("Mock driver");
    expect(runWorkloadType({ workload_type: "write-verify" }, job)).toBe(
      "write-verify",
    );
    expect(workloadLabel(runWorkloadType(null, { options: undefined }))).toBe(
      "Write + verify",
    );
  });
  it("summarises run settings", () => {
    const run = {
      workload: "write-verify",
      object_count: 25,
      object_size: "1MiB",
      threads: 1,
      host_count: 2,
    } as CatalogRun;
    expect(runSettings(run)).toBe(
      "write-verify · 25 × 1MiB · 1 thread · 2 hosts",
    );
    expect(runSettings({} as CatalogRun)).toBe("Settings not recorded");
  });
});
