import type {
  CoreStep,
  Metrics,
  Snapshot,
  MixedOperation,
  Sample,
  CoreScope,
} from "../types.ts";
import { createMixedMetrics } from "./mixedMock.ts";
import { createDeleteMetrics } from "./deleteMock.ts";
const scopedTiming = (median: number) => ({
  meanMs: median * 1.12,
  minMs: median * 0.34,
  p50Ms: median,
  p90Ms: median * 1.6,
  p99Ms: median * 2.5,
  p999Ms: median * 4.1,
  maxMs: median * 5.6,
});
const mean = (values: number[]) =>
  values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0;
const distribute = (n: number, i: number, count: number) =>
  Math.floor(n / count) + (i < n % count ? 1 : 0);
/** Step/operation fixture scopes. A real adapter must supply scoped metrics and histograms. */
export function createCoreSteps(s: Omit<Snapshot, "coreSteps">): CoreStep[] {
  const unbounded = s.unbounded;
  const definitions = [
    {
      id: "step-1",
      label: "Step 1",
      start: 0,
      end: unbounded ? null : s.config.durationSeconds,
    },
  ];
  const countAt = (t: number) => {
    if (s.config.operation === "DELETE") {
      const d = createDeleteMetrics(s.config, t, false);
      return {
        success: d.requests.fullSuccess,
        failures: d.requests.failed + d.requests.partial,
        bytes: 0,
      };
    }
    const success = s.samples
        .filter((p) => p.elapsed > 0 && p.elapsed <= t)
        .reduce((n, p) => n + p.throughput, 0),
      failures = Math.max(0, Math.min(t - 45, 15)) * 3;
    return {
      success,
      failures,
      bytes:
        success *
        s.config.objectSizeMiB *
        1048576 *
        (s.config.operation === "MIXED"
          ? 0.83
          : s.config.operation === "STAT"
            ? 0
            : 1),
    };
  };
  return definitions.map((def) => {
    const elapsed = Math.max(
        0,
        Math.min(s.elapsedSeconds, def.end ?? s.elapsedSeconds) - def.start,
      ),
      state =
        s.elapsedSeconds < def.start
          ? "idle"
          : def.end !== null && s.elapsedSeconds >= def.end
            ? "completed"
            : "running";
    const upper = Math.min(s.elapsedSeconds, def.end ?? s.elapsedSeconds),
      start = countAt(Math.min(def.start, s.elapsedSeconds)),
      finish = countAt(upper),
      success = finish.success - start.success,
      failures = finish.failures - start.failures;
    const rows = s.samples.filter(
        (p) => p.elapsed > def.start && p.elapsed <= upper,
      ),
      recent = rows.slice(-5);
    const ops: MixedOperation[] =
      s.config.operation === "MIXED"
        ? ["CREATE", "READ", "STAT", "DELETE"]
        : [s.config.operation as MixedOperation];
    const successByOp =
        s.config.operation === "MIXED" ? createMixedMetrics(success) : null,
      failuresByOp =
        s.config.operation === "MIXED" ? createMixedMetrics(failures) : null;
    const scopes: CoreScope[] = ops.map((operation) => {
      const sc =
          successByOp?.operations.find((o) => o.operation === operation)
            ?.attemptedCount ?? success,
        fc =
          failuresByOp?.operations.find((o) => o.operation === operation)
            ?.attemptedCount ?? failures;
      const share =
        s.config.operation === "MIXED"
          ? { READ: 0.55, CREATE: 0.28, STAT: 0.11, DELETE: 0.06 }[operation]
          : 1;
      const bytes =
        operation === "DELETE" || operation === "STAT"
          ? 0
          : sc * s.config.objectSizeMiB * 1048576;
      // Independent synthetic distributions for each operation/step, not node-percentile averages.
      const median = { CREATE: 3.2, READ: 2.8, STAT: 1.1, DELETE: 4.0 }[
        operation
      ];
      const latency = elapsed && sc ? scopedTiming(median) : null,
        duration =
          elapsed && sc
            ? scopedTiming(
                median +
                  (operation === "READ" || operation === "CREATE" ? 1.5 : 0.3),
              )
            : null;
      const allocatedAt = (t: number, failed: boolean) => {
        const c = countAt(t),
          count = failed
            ? c.failures - start.failures
            : c.success - start.success;
        return s.config.operation === "MIXED"
          ? createMixedMetrics(Math.max(0, count)).operations.find(
              (o) => o.operation === operation,
            )!.attemptedCount
          : count;
      };
      const currentSuccess =
        state === "running"
          ? mean(
              recent.map(
                (p) =>
                  allocatedAt(p.elapsed, false) -
                  allocatedAt(p.elapsed - 1, false),
              ),
            )
          : 0;
      const currentFailure =
        state === "running"
          ? mean(
              recent.map(
                (p) =>
                  allocatedAt(p.elapsed, true) -
                  allocatedAt(p.elapsed - 1, true),
              ),
            )
          : 0;
      const aggregate: Metrics = {
        ...s.aggregate,
        success: sc,
        failures: fc,
        corrupt: 0,
        currentOps: currentSuccess,
        currentFailureOps: currentFailure,
        rateWindowSeconds: 5,
        averageOps: elapsed ? sc / elapsed : 0,
        transferredBytes: bytes,
        bandwidthMiB:
          operation === "DELETE"
            ? null
            : operation === "STAT"
              ? 0
              : currentSuccess * s.config.objectSizeMiB,
        averageBandwidthMiB:
          operation === "DELETE"
            ? null
            : elapsed
              ? bytes / 1048576 / elapsed
              : 0,
        latency,
        duration,
        ttfb: operation === "READ" ? s.aggregate.ttfb : null,
        concurrency:
          state === "running" ? Math.floor(s.aggregate.concurrency * share) : 0,
        meanConcurrency: elapsed
          ? s.config.threadsPerClient * s.expectedNodes * 0.8 * share
          : 0,
      };
      const nodes = s.nodes.map((node, i) => ({
        ...node,
        metrics: {
          ...aggregate,
          currentOps: aggregate.currentOps / s.nodes.length,
          currentFailureOps: aggregate.currentFailureOps / s.nodes.length,
          averageOps: aggregate.averageOps / s.nodes.length,
          success: distribute(sc, i, s.nodes.length),
          failures: distribute(fc, i, s.nodes.length),
          transferredBytes: bytes / s.nodes.length,
          bandwidthMiB:
            aggregate.bandwidthMiB === null
              ? null
              : aggregate.bandwidthMiB / s.nodes.length,
          averageBandwidthMiB:
            aggregate.averageBandwidthMiB === null
              ? null
              : aggregate.averageBandwidthMiB / s.nodes.length,
          concurrency: distribute(aggregate.concurrency, i, s.nodes.length),
          meanConcurrency: aggregate.meanConcurrency / s.nodes.length,
        },
      }));
      return { operation, aggregate, nodes };
    });
    const mixed =
      s.config.operation === "MIXED"
        ? createMixedMetrics(success + failures)
        : null;
    if (mixed) mixed.scope = "cumulative step";
    if (mixed)
      for (const row of mixed.operations) {
        const op = scopes.find((o) => o.operation === row.operation)!;
        row.attemptedCount = op.aggregate.success + op.aggregate.failures;
        row.observedSharePercent = mixed.totalAttempted
          ? (row.attemptedCount / mixed.totalAttempted) * 100
          : null;
      }
    return {
      id: def.id,
      label: def.label,
      state:
        s.state === "failed" &&
        state === "completed" &&
        def.id === definitions.at(-1)?.id
          ? "failed"
          : state,
      elapsedSeconds: elapsed,
      completionPercent:
        def.end === null ? null : (elapsed / (def.end - def.start)) * 100,
      unbounded,
      configuredThreads: s.config.threadsPerClient * s.expectedNodes,
      expectedNodes: s.expectedNodes,
      reportingNodes: s.reportingNodes,
      partial: s.partial,
      scopes,
      mixedMetrics: mixed,
    };
  });
}
