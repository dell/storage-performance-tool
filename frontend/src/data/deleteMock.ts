import type {
  BucketCounters,
  DeleteMetrics,
  DeleteOptions,
  RunConfig,
  Timing,
} from "../types.ts";
export const defaultDeleteOptions: DeleteOptions = {
  batchSize: 100,
  bucketCount: 103,
  allowedFailurePercent: 0.1,
  scenario: "within-budget",
};
const durations = [
  ["seed", 0.04, "Seed"],
  ["discovery", 0.1, "Discovery"],
  ["preValidation", 0.06, "Pre-validation"],
  ["scheduledDelete", 0.55, "Scheduled delete"],
  ["drain", 0.05, "Drain"],
  ["postVerification", 0.15, "Post-verification"],
  ["cleanup", 0.05, "Cleanup"],
] as const;
const timing = (v: number): Timing => ({
  meanMs: v * 1.2,
  minMs: v * 0.4,
  p50Ms: v,
  p90Ms: v * 1.7,
  p99Ms: v * 2.8,
  p999Ms: v * 4,
  maxMs: v * 6,
});
const ratio = (a: number, b: number) => (b === 0 ? null : (a / b) * 100);
/** Synthetic deletion lifecycle. This is not a raw SPT API mapping. */
export function createDeleteMetrics(
  config: RunConfig,
  elapsed: number,
  includeBuckets = true,
): DeleteMetrics {
  const options = { ...defaultDeleteOptions, ...config.deleteOptions },
    size = options.batchSize,
    short = Math.max(1, Math.floor(size * 0.6)),
    end = config.durationSeconds;
  const t = Math.min(end, Math.max(0, elapsed)),
    finished = t >= end;
  const deleteStart = end * 0.2,
    deleteEnd = end * 0.75,
    drainEnd = end * 0.8,
    verifyEnd = end * 0.95;
  const planned = Math.max(1, Math.round(end * 0.55 * 7200));
  const count = (time: number) => {
    const fraction = Math.max(
      0,
      Math.min(1, (time - deleteStart) / (deleteEnd - deleteStart)),
    );
    const attempted = Math.floor(planned * fraction);
    const unresolved =
      time < deleteStart || time >= drainEnd
        ? 0
        : Math.min(
            attempted,
            Math.floor(
              Math.min(80, config.threadsPerClient * config.expectedNodes) *
                Math.max(
                  0,
                  Math.min(1, (drainEnd - time) / (drainEnd - deleteEnd)),
                ),
            ),
          );
    const resolved = attempted - unresolved;
    const partialBatchCount = size === 1 ? 0 : Math.floor(attempted / 10),
      objectCount = (n: number) =>
        n * size - (size === 1 ? 0 : Math.floor(n / 10) * (size - short));
    const errorScenario =
      options.scenario === "within-budget" ||
      options.scenario === "over-budget";
    const partial = errorScenario
      ? Math.floor(resolved / (options.scenario === "over-budget" ? 20 : 200))
      : 0;
    const failed = errorScenario
      ? Math.floor(resolved / (options.scenario === "over-budget" ? 50 : 5000))
      : 0;
    const objectsAttempted = objectCount(attempted),
      objectsResolved = objectCount(resolved),
      objectsFailed = Math.min(
        objectsResolved,
        failed * short + partial * Math.max(1, Math.floor(short * 0.1)),
      );
    return {
      attempted,
      resolved,
      partial,
      failed,
      fullSuccess: resolved - partial - failed,
      unresolved,
      partialBatchCount,
      fullBatchCount: attempted - partialBatchCount,
      objectsAttempted,
      objectsAccepted: objectsResolved - objectsFailed,
      objectsFailed,
      objectsUnresolved: objectsAttempted - objectsResolved,
      selected: objectCount(planned),
    };
  };
  const c = count(t),
    prev = count(Math.max(0, t - 1)),
    dt = Math.min(1, t);
  const phaseTimings = {
    seed: 0,
    discovery: 0,
    preValidation: 0,
    scheduledDelete: 0,
    drain: 0,
    postVerification: 0,
    cleanup: 0,
    totalWallTime: t,
  };
  let cursor = 0,
    currentPhase = "Completed";
  for (const [key, share, label] of durations) {
    const duration = end * share;
    phaseTimings[key] = Math.max(0, Math.min(duration, t - cursor));
    if (t >= cursor && t < cursor + duration) currentPhase = label;
    cursor += duration;
  }
  const verifyProgress = Math.max(
      0,
      Math.min(1, (t - drainEnd) / (verifyEnd - drainEnd)),
    ),
    unknownTarget =
      options.scenario === "verification-unresolved"
        ? Math.max(1, Math.floor(c.selected * 0.001))
        : 0;
  const absent = Math.floor(
      (c.selected - c.objectsFailed - unknownTarget) * verifyProgress,
    ),
    present = Math.floor(c.objectsFailed * verifyProgress),
    unknown = Math.floor(unknownTarget * verifyProgress),
    unverified = c.selected - absent - present - unknown;
  const observed = ratio(c.objectsFailed, c.objectsAttempted),
    outcome = !finished
      ? "running"
      : (observed ?? 0) > options.allowedFailurePercent
        ? "failed"
        : c.objectsFailed === 0
          ? "completed cleanly"
          : "completed within budget";
  const distribute = (n: number, i: number) =>
    Math.floor(n / options.bucketCount) + (i < n % options.bucketCount ? 1 : 0);
  // Partition categories separately so each bucket's accounting and cluster sums agree.
  const all: BucketCounters[] = Array.from(
    { length: includeBuckets ? options.bucketCount : 0 },
    (_, i) => {
      const accepted = distribute(c.objectsAccepted, i),
        failed = distribute(c.objectsFailed, i),
        unresolved = distribute(c.objectsUnresolved, i),
        unattempted = distribute(c.selected - c.objectsAttempted, i);
      return {
        name: `bucket-${String(i + 1).padStart(3, "0")}`,
        selected: accepted + failed + unresolved + unattempted,
        attempted: accepted + failed + unresolved,
        accepted,
        failed,
      };
    },
  );
  const grouped = all.slice(100),
    other = grouped.length
      ? grouped.reduce<BucketCounters>(
          (sum, b) => ({
            name: "other",
            selected: sum.selected + b.selected,
            attempted: sum.attempted + b.attempted,
            accepted: sum.accepted + b.accepted,
            failed: sum.failed + b.failed,
          }),
          { name: "other", selected: 0, attempted: 0, accepted: 0, failed: 0 },
        )
      : null;
  return {
    requests: {
      planned,
      attempted: c.attempted,
      fullSuccess: c.fullSuccess,
      partial: c.partial,
      failed: c.failed,
      unresolved: c.unresolved,
      perSecond: finished || !dt ? 0 : (c.attempted - prev.attempted) / dt,
    },
    objects: {
      selected: c.selected,
      attempted: c.objectsAttempted,
      accepted: c.objectsAccepted,
      failed: c.objectsFailed,
      unattempted: c.selected - c.objectsAttempted,
      unresolved: c.objectsUnresolved,
      perSecond:
        finished || !dt ? 0 : (c.objectsAttempted - prev.objectsAttempted) / dt,
    },
    completion: {
      requestPercent: ratio(c.attempted, planned),
      objectPercent: ratio(c.objectsAttempted, c.selected),
    },
    batching: {
      configuredSize: size,
      observedMinSize: c.attempted
        ? c.partialBatchCount
          ? short
          : size
        : null,
      observedMaxSize: c.attempted ? size : null,
      meanObjectsPerRequest: c.attempted
        ? c.objectsAttempted / c.attempted
        : null,
      fullBatchCount: c.fullBatchCount,
      partialBatchCount: c.partialBatchCount,
      fullBatchPercent: ratio(c.fullBatchCount, c.attempted),
      partialBatchPercent: ratio(c.partialBatchCount, c.attempted),
    },
    buckets: {
      items: all.slice(0, 100),
      other,
      totalBucketCount: options.bucketCount,
      groupedBucketCount: grouped.length,
    },
    phaseTimings,
    currentPhase: finished ? "Completed" : currentPhase,
    requestTiming: {
      latency: c.resolved ? timing(3.4) : null,
      duration: c.resolved ? timing(5.1) : null,
    },
    failureBudget: {
      failedObjects: c.objectsFailed,
      attemptedObjects: c.objectsAttempted,
      observedFailurePercent: observed,
      allowedFailurePercent: options.allowedFailurePercent,
      outcome,
    },
    verification: {
      verifiedAbsent: absent,
      stillPresent: present,
      unresolved: unknown,
      unverified,
      removalConfirmed:
        finished &&
        unverified === 0 &&
        present === 0 &&
        unknown === 0 &&
        absent === c.selected,
    },
  };
}
