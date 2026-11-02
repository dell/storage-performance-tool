// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import type { CatalogRun, CompareResult, LogResponse } from "../types";
import { Catalog } from "./Catalog";
import { Compare, DeltaTable, OverlayChart } from "./Compare";
import { RunLog } from "./RunLog";
import { NodeTable } from "./NodeTable";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

const run = (id: string, over: Partial<CatalogRun> = {}): CatalogRun => ({
  id,
  name: id,
  source: "cli",
  status: "complete",
  integrity: "passed",
  started_at: "2026-10-16T22:02:30Z",
  workload: "write-verify",
  object_size: "1MiB",
  object_count: 25,
  threads: 1,
  phases: [
    {
      key: "WRITE",
      step: "write",
      op_type: "CREATE",
      source_file: "write.metrics.total.csv",
      successful_ops: 25,
      failed_ops: 0,
      corrupt_ops: 0,
      iops: 50,
      bandwidth_mib_s: 50,
      p50_latency_us: 900,
      p99_latency_us: 1500,
    },
  ],
  issues: [],
  modified_at: "2026-10-16T22:03:00Z",
  ...over,
});

function stubFetch(handler: (url: string) => unknown, status = 200) {
  const calls: string[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string) => {
      calls.push(url);
      return new Response(JSON.stringify(handler(url)), { status });
    }),
  );
  return calls;
}

const compareResult: CompareResult = {
  baseline: "base",
  runs: [run("base"), run("next", { threads: 4 })],
  series: {
    base: {
      WRITE: [
        {
          t: 0,
          iops: 40,
          bandwidth_mib_s: 40,
          p50_latency_us: 1,
          p99_latency_us: 2,
        },
        {
          t: 1,
          iops: 50,
          bandwidth_mib_s: 50,
          p50_latency_us: 1,
          p99_latency_us: 2,
        },
      ],
    },
    next: {
      WRITE: [
        {
          t: 0,
          iops: 30,
          bandwidth_mib_s: 30,
          p50_latency_us: 1,
          p99_latency_us: 2,
        },
      ],
    },
  },
  deltas: [
    {
      run_id: "next",
      phase: "WRITE",
      metric: "iops",
      label: "IOPS",
      baseline: 50,
      value: 30,
      delta_pct: -40,
      flag: "not_comparable",
      reason: "threads differ: 1 vs 4.",
    },
    {
      run_id: "next",
      phase: "WRITE",
      metric: "corrupt_ops",
      label: "Corrupt objects",
      baseline: 0,
      value: 2,
      delta_pct: null,
      flag: "regressed",
      reason: "Corrupt objects recorded; this is always flagged.",
    },
  ],
  settings_differences: { next: ["threads differ: 1 vs 4"] },
  rule: {
    label: "Threshold check, not statistical significance",
    threshold_pct: 10,
    latency_floor_us: 50,
    min_ops: 20,
    description: "Flags a change larger than the threshold.",
  },
};

describe("Catalog", () => {
  it("filters through the API and hands the selection to compare", async () => {
    const calls = stubFetch(() => ({
      root: "/results",
      runs: [
        run("a"),
        run("b"),
        run("bad", {
          status: "malformed",
          phases: [],
          issues: ["metrics.total.csv missing"],
        }),
      ],
      total: 3,
      truncated: false,
      read_only: true,
    }));
    const onCompare = vi.fn();
    render(<Catalog onCompare={onCompare} />);
    await screen.findByText("a");
    expect(screen.getByLabelText("Compare bad")).toHaveProperty(
      "disabled",
      true,
    );
    const compare = screen.getByRole("button", { name: /Compare\s+selected/ });
    expect(compare).toHaveProperty("disabled", true);
    fireEvent.click(screen.getByLabelText("Compare a"));
    fireEvent.click(screen.getByLabelText("Compare b"));
    fireEvent.click(screen.getByLabelText("Use b as baseline"));
    fireEvent.click(screen.getByRole("button", { name: /Compare 2 selected/ }));
    expect(onCompare).toHaveBeenCalledWith(["a", "b"], "b");
    fireEvent.change(screen.getByLabelText("Source"), {
      target: { value: "dashboard" },
    });
    await waitFor(() =>
      expect(calls.at(-1)).toBe("/api/catalog?source=dashboard"),
    );
  });
});

describe("Compare", () => {
  it("labels the rule honestly and shows settings differences", async () => {
    const calls = stubFetch(() => compareResult);
    render(
      <Compare ids={["base", "next"]} baseline="base" onBack={() => {}} />,
    );
    await screen.findByText("Threshold check, not statistical significance");
    expect(screen.getByRole("status").textContent).toContain(
      "Threshold check, not statistical significance",
    );
    expect(screen.getByText(/next: threads differ/)).toBeTruthy();
    expect(screen.getByText("Not comparable")).toBeTruthy();
    expect(calls[0]).toBe("/api/catalog/compare?id=base&id=next&baseline=base");
    fireEvent.change(screen.getByLabelText("Threshold %"), {
      target: { value: "5" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Apply" }));
    await waitFor(() => expect(calls.at(-1)).toContain("threshold_pct=5"));
  });
  it("asks for at least two runs", () => {
    render(<Compare ids={["only"]} baseline="" onBack={() => {}} />);
    expect(screen.getByText("Select runs to compare")).toBeTruthy();
  });
  it("draws one line per run and a dot for a single sample", () => {
    const { container } = render(
      <OverlayChart
        phase="WRITE"
        metric="iops"
        unit="IOPS"
        lines={[
          {
            id: "base",
            color: "#000",
            points: compareResult.series.base.WRITE,
          },
          {
            id: "next",
            color: "#111",
            points: compareResult.series.next.WRITE,
          },
        ]}
      />,
    );
    expect(container.querySelectorAll("polyline")).toHaveLength(1);
    expect(container.querySelectorAll("circle")).toHaveLength(1);
  });
  it("shows the footnote and reasons in the delta table", () => {
    render(<DeltaTable deltas={compareResult.deltas} baseline="base" />);
    expect(screen.getByText("Regressed")).toBeTruthy();
    expect(
      screen.getAllByText(/Corrupt objects recorded/).length,
    ).toBeGreaterThan(0);
    expect(screen.getByText(/not statistical significance/)).toBeTruthy();
  });
});

describe("RunLog", () => {
  it("marks console output as interim and refetches by level", async () => {
    const body: LogResponse = {
      run_id: "r",
      source: "console",
      level: "all",
      interim: true,
      truncated: false,
      lines: [
        { file: "console", level: "warn", text: "WARN slow" },
        { file: "console", level: "error", text: "ERROR [REDACTED]" },
      ],
    };
    const calls = stubFetch(() => body);
    render(<RunLog base="/api/runs/r/log" sources={["console", "results"]} />);
    await screen.findByText("WARN slow");
    expect(screen.getByText(/Interim console output/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "ERROR" }));
    await waitFor(() =>
      expect(calls.at(-1)).toBe(
        "/api/runs/r/log?source=console&level=error&tail=200",
      ),
    );
    fireEvent.click(screen.getByRole("button", { name: "Results logs" }));
    await waitFor(() => expect(calls.at(-1)).toContain("source=results"));
  });
  it("stays closed and does not fetch until opened when collapsed", async () => {
    const calls = stubFetch(() => ({
      run_id: "r",
      source: "results",
      level: "all",
      interim: false,
      truncated: false,
      lines: [{ file: "write.errors.log", level: "error", text: "ERROR boom" }],
    }));
    const { container } = render(
      <RunLog base="/api/catalog/log?id=r" title="Errors and logs" collapsed />,
    );
    expect(calls).toHaveLength(0);
    const details = container.querySelector("details")!;
    details.open = true;
    fireEvent(details, new Event("toggle"));
    await screen.findByText("ERROR boom");
    expect(screen.getByText("From: write.errors.log")).toBeTruthy();
  });
  it("shows the server error", async () => {
    stubFetch(() => ({ error: "Saved run not found." }), 404);
    render(<RunLog base="/api/catalog/log?id=x" />);
    await screen.findByText("Saved run not found.");
  });
});

describe("NodeTable", () => {
  it("renders reporting and unavailable machines", () => {
    render(
      <NodeTable
        nodes={[
          {
            host: "node-a",
            status: "reporting",
            iops: 12.5,
            op_type: "CREATE",
          },
          { host: "node-b", status: "unavailable" },
        ]}
      />,
    );
    expect(screen.getByText("Reporting")).toBeTruthy();
    expect(screen.getByText("Unavailable")).toBeTruthy();
  });
  it("renders nothing without nodes", () => {
    const { container } = render(<NodeTable nodes={[]} />);
    expect(container.innerHTML).toBe("");
  });
});
