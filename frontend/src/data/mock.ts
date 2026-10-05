import type {
  DataSource,
  Metrics,
  RunConfig,
  Sample,
  Snapshot,
  Timing,
} from "../types.ts";
import { createCoreSteps } from "./coreMock.ts";
import { createDiscoveryMetrics } from "./discoveryMock.ts";
import { createMixedMetrics } from "./mixedMock.ts";
import { createDeleteMetrics } from "./deleteMock.ts";
export const defaultConfig: RunConfig = {
  name: "Object storage benchmark",
  operation: "CREATE",
  durationSeconds: 120,
  threadsPerClient: 64,
  expectedNodes: 3,
  objectSizeMiB: 1,
};
const timing = (p50: number): Timing => ({
  meanMs: p50 * 1.12,
  minMs: p50 * 0.34,
  p50Ms: p50,
  p90Ms: p50 * 1.6,
  p99Ms: p50 * 2.5,
  p999Ms: p50 * 4.1,
  maxMs: p50 * 5.6,
});
// Synthetic fixtures, never interpreted as actual storage measurements.
export function createMockSource(now: () => number = Date.now): DataSource {
  let config = { ...defaultConfig },
    runId = "demo-001",
    started = now() - 24000,
    sequence = 1;
  function snapshot(): Snapshot {
    const elapsed = Math.min(
        config.unbounded ? Infinity : config.durationSeconds,
        Math.max(0, Math.floor((now() - started) / 1000)),
      ),
      completed = !config.unbounded && elapsed >= config.durationSeconds;
    const makePoint = (s: number): Sample & { p50: number; p99: number } => {
      const rate = Math.round(
        7200 + Math.sin(s * 0.18) * 500 + (s >= 45 && s < 60 ? -1700 : 0),
      );
      return {
        timestamp: new Date(started + s * 1000).toISOString(),
        elapsed: s,
        throughput: rate,
        bandwidth:
          config.operation === "DELETE"
            ? null
            : rate *
              config.objectSizeMiB *
              (config.operation === "MIXED"
                ? 0.83
                : config.operation === "STAT"
                  ? 0
                  : 1),
        p50: 3.2 + Math.sin(s * 0.1) * 0.3 + (s >= 45 && s < 60 ? 2 : 0),
        p99: 8.4 + Math.sin(s * 0.1) * 0.7 + (s >= 45 && s < 60 ? 5 : 0),
      };
    };
    const samples: Sample[] = Array.from({ length: elapsed + 1 }, (_, s) =>
        makePoint(s),
      ),
      point = makePoint(elapsed);
    const success = samples.slice(1).reduce((n, s) => n + s.throughput, 0),
      failures = elapsed > 45 ? Math.min(elapsed - 45, 15) * 3 : 0;
    const plannedRequests =
      Array.from(
        { length: config.durationSeconds },
        (_, i) => makePoint(i + 1).throughput,
      ).reduce((a, b) => a + b, 0) +
      Math.max(0, Math.min(config.durationSeconds - 45, 15)) * 3;
    const deletion =
      config.operation === "DELETE"
        ? createDeleteMetrics(config, elapsed)
        : null;
    const deletePrevious = deletion
      ? createDeleteMetrics(config, Math.max(0, elapsed - 1))
      : null;
    const aggregate: Metrics = {
      currentOps: completed ? 0 : point.throughput,
      currentFailureOps: 0,
      rateWindowSeconds: 5,
      averageBandwidthMiB:
        config.operation === "DELETE"
          ? null
          : elapsed
            ? (success *
                config.objectSizeMiB *
                (config.operation === "MIXED"
                  ? 0.83
                  : config.operation === "STAT"
                    ? 0
                    : 1)) /
              elapsed
            : 0,
      averageOps: elapsed ? success / elapsed : 0,
      bandwidthMiB:
        point.bandwidth === null ? null : completed ? 0 : point.bandwidth,
      transferredBytes:
        config.operation === "DELETE"
          ? 0
          : success *
            config.objectSizeMiB *
            1048576 *
            (config.operation === "MIXED"
              ? 0.83
              : config.operation === "STAT"
                ? 0
                : 1),
      success,
      failures,
      corrupt: 0,
      latency: { ...timing(point.p50), p99Ms: point.p99 },
      duration: timing(point.p50 + 1.5),
      ttfb: config.operation === "READ" ? timing(2.1) : null,
      concurrency: completed
        ? 0
        : Math.floor(config.threadsPerClient * config.expectedNodes * 0.82),
      meanConcurrency: config.threadsPerClient * config.expectedNodes * 0.8,
    };
    if (deletion && deletePrevious) {
      aggregate.currentOps = completed
        ? 0
        : deletion.requests.fullSuccess - deletePrevious.requests.fullSuccess;
      aggregate.averageOps = elapsed
        ? deletion.requests.fullSuccess / elapsed
        : 0;
      aggregate.success = deletion.requests.fullSuccess;
      aggregate.failures = deletion.requests.failed + deletion.requests.partial;
      aggregate.latency = deletion.requestTiming.latency;
      aggregate.duration = deletion.requestTiming.duration;
      aggregate.concurrency = deletion.requests.unresolved;
      for (const sample of samples) {
        const d = createDeleteMetrics(config, sample.elapsed, false),
          prev = createDeleteMetrics(
            config,
            Math.max(0, sample.elapsed - 1),
            false,
          );
        sample.throughput = d.requests.fullSuccess - prev.requests.fullSuccess;
        sample.p50 = d.requestTiming.latency?.p50Ms ?? null;
        sample.p99 = d.requestTiming.latency?.p99Ms ?? null;
      }
    }
    const recent = samples.filter((p) => p.elapsed > 0).slice(-5);
    aggregate.currentOps = completed
      ? 0
      : recent.length
        ? recent.reduce((n, p) => n + p.throughput, 0) / recent.length
        : 0;
    aggregate.bandwidthMiB =
      config.operation === "DELETE"
        ? null
        : completed
          ? 0
          : recent.length
            ? recent.reduce((n, p) => n + (p.bandwidth ?? 0), 0) / recent.length
            : 0;
    const failureAt = (t: number) =>
      deletion
        ? (() => {
            const d = createDeleteMetrics(config, t, false);
            return d.requests.failed + d.requests.partial;
          })()
        : Math.max(0, Math.min(t - 45, 15)) * 3;
    aggregate.currentFailureOps =
      completed || !recent.length
        ? 0
        : recent.reduce(
            (n, p) => n + failureAt(p.elapsed) - failureAt(p.elapsed - 1),
            0,
          ) / recent.length;
    const nodes = Array.from({ length: config.expectedNodes }, (_, i) => {
      const share = 1 / config.expectedNodes;
      return {
        id: `client-${i + 1}`,
        reporting: true,
        lastSeenAt: new Date(now()).toISOString(),
        metrics: {
          ...aggregate,
          currentOps: aggregate.currentOps * share,
          averageOps: aggregate.averageOps * share,
          currentFailureOps: aggregate.currentFailureOps * share,
          averageBandwidthMiB:
            aggregate.averageBandwidthMiB === null
              ? null
              : aggregate.averageBandwidthMiB * share,
          bandwidthMiB:
            aggregate.bandwidthMiB === null
              ? null
              : aggregate.bandwidthMiB * share,
          transferredBytes: aggregate.transferredBytes * share,
          success:
            Math.floor(aggregate.success * share) +
            (i === 0 ? aggregate.success % config.expectedNodes : 0),
          failures: i === 0 ? aggregate.failures : 0,
          latency: aggregate.latency
            ? timing(aggregate.latency.p50Ms * (1 + i * 0.04))
            : null,
          duration: aggregate.duration
            ? timing(aggregate.duration.p50Ms * (1 + i * 0.04))
            : null,
          concurrency: aggregate.concurrency * share,
          meanConcurrency: aggregate.meanConcurrency * share,
        },
      };
    });
    const base: Omit<Snapshot, "coreSteps"> = {
      schemaVersion: 3,
      unbounded: config.unbounded ?? false,
      source: "mock",
      runId,
      config: { ...config },
      state: completed
        ? deletion &&
          (deletion.failureBudget.outcome === "failed" ||
            deletion.verification.unresolved > 0)
          ? "failed"
          : "completed"
        : "running",
      phase: deletion
        ? deletion.currentPhase
        : config.operation === "MIXED"
          ? "Mixed operations"
          : config.operation === "READ"
            ? "Read objects"
            : config.operation === "STAT"
              ? "Stat objects"
              : "Create objects",
      elapsedSeconds: elapsed,
      progressPercent: config.unbounded
        ? null
        : (elapsed / config.durationSeconds) * 100,
      updatedAt: new Date(now()).toISOString(),
      expectedNodes: config.expectedNodes,
      reportingNodes: nodes.length,
      partial: false,
      aggregate,
      nodes,
      samples,
      deleteMetrics: deletion,
      discoveryMetrics: createDiscoveryMetrics(config, elapsed, started),
      mixedMetrics:
        config.operation === "MIXED"
          ? createMixedMetrics(success + failures)
          : null,
    };
    return { ...base, coreSteps: createCoreSteps(base) };
  }
  return {
    async getSnapshot() {
      return snapshot();
    },
    async startRun(next) {
      config = { ...next };
      sequence++;
      runId = `demo-${String(sequence).padStart(3, "0")}`;
      started = now();
      return snapshot();
    },
  };
}
