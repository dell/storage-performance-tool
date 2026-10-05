export type Operation = "CREATE" | "READ" | "DELETE" | "STAT" | "MIXED";
export type MixedOperation = "CREATE" | "READ" | "STAT" | "DELETE";
export interface MixedMetrics {
  basis: "attempted";
  scope: "cumulative run" | "cumulative step";
  totalAttempted: number;
  operations: {
    operation: MixedOperation;
    configuredSharePercent: number;
    attemptedCount: number;
    observedSharePercent: number | null;
  }[];
}
export type Mode = "guided" | "expert";
export interface RunConfig {
  name: string;
  operation: Operation;
  durationSeconds: number;
  unbounded?: boolean;
  threadsPerClient: number;
  expectedNodes: number;
  objectSizeMiB: number;
  deleteOptions?: DeleteOptions;
  shardDiscovery?: { enabled: boolean; simulateStall: boolean };
}
export interface Timing {
  meanMs: number;
  minMs: number;
  p50Ms: number;
  p90Ms: number;
  p99Ms: number;
  p999Ms: number;
  maxMs: number;
}
export interface Metrics {
  currentOps: number;
  averageOps: number;
  currentFailureOps: number;
  rateWindowSeconds: number;
  averageBandwidthMiB: number | null;
  bandwidthMiB: number | null;
  transferredBytes: number;
  success: number;
  failures: number;
  corrupt: number;
  latency: Timing | null;
  duration: Timing | null;
  ttfb: Timing | null;
  concurrency: number;
  meanConcurrency: number;
}
export interface NodeMetrics {
  id: string;
  lastSeenAt: string;
  reporting: boolean;
  metrics: Metrics;
}
export interface Sample {
  timestamp: string;
  elapsed: number;
  throughput: number;
  bandwidth: number | null;
  p50: number | null;
  p99: number | null;
}
export interface DeleteOptions {
  batchSize: number;
  bucketCount: number;
  allowedFailurePercent: number;
  scenario:
    "clean" | "within-budget" | "over-budget" | "verification-unresolved";
}
export interface BucketCounters {
  name: string;
  selected: number;
  attempted: number;
  accepted: number;
  failed: number;
}
export interface DeleteMetrics {
  requests: {
    planned: number;
    attempted: number;
    fullSuccess: number;
    partial: number;
    failed: number;
    unresolved: number;
    perSecond: number;
  };
  objects: {
    selected: number;
    attempted: number;
    accepted: number;
    failed: number;
    unattempted: number;
    unresolved: number;
    perSecond: number;
  };
  completion: { requestPercent: number | null; objectPercent: number | null };
  batching: {
    configuredSize: number;
    observedMinSize: number | null;
    observedMaxSize: number | null;
    meanObjectsPerRequest: number | null;
    fullBatchCount: number;
    partialBatchCount: number;
    fullBatchPercent: number | null;
    partialBatchPercent: number | null;
  };
  buckets: {
    items: BucketCounters[];
    other: BucketCounters | null;
    totalBucketCount: number;
    groupedBucketCount: number;
  };
  phaseTimings: {
    seed: number;
    discovery: number;
    preValidation: number;
    scheduledDelete: number;
    drain: number;
    postVerification: number;
    cleanup: number;
    totalWallTime: number;
  };
  currentPhase: string;
  requestTiming: { latency: Timing | null; duration: Timing | null };
  failureBudget: {
    failedObjects: number;
    attemptedObjects: number;
    observedFailurePercent: number | null;
    allowedFailurePercent: number;
    outcome:
      "running" | "completed cleanly" | "completed within budget" | "failed";
  };
  verification: {
    verifiedAbsent: number;
    stillPresent: number;
    unresolved: number;
    unverified: number;
    removalConfirmed: boolean;
  };
}
export interface DiscoveryMetrics {
  enabled: true;
  totalObjectsDiscovered: number;
  totalPagesReturned: number;
  averagePageResponseMs: number | null;
  totalSplits: number;
  maxDepth: number;
  splitsByReason: { reason: string; count: number }[];
  prefixes: {
    prefix: string;
    objectsDiscovered: number;
    expectedObjects: number | null;
    pagesReturned: number;
    progressPercent: number | null;
    status: "pending" | "active" | "stalled" | "completed";
    activeShards: number;
    lastProgressAt: string | null;
    averagePageResponseMs: number | null;
  }[];
}
export interface CoreScope {
  operation: MixedOperation;
  aggregate: Metrics;
  nodes: NodeMetrics[];
}
export interface CoreStep {
  id: string;
  label: string;
  state: "idle" | "running" | "completed" | "failed";
  elapsedSeconds: number;
  completionPercent: number | null;
  unbounded: boolean;
  configuredThreads: number;
  expectedNodes: number;
  reportingNodes: number;
  partial: boolean;
  scopes: CoreScope[];
  mixedMetrics: MixedMetrics | null;
}
export interface Snapshot {
  schemaVersion: 3;
  source: "mock" | "spt";
  runId: string;
  config: RunConfig;
  state: "idle" | "running" | "completed" | "failed";
  phase: string;
  unbounded: boolean;
  coreSteps: CoreStep[];
  elapsedSeconds: number;
  progressPercent: number | null;
  updatedAt: string;
  expectedNodes: number;
  reportingNodes: number;
  partial: boolean;
  aggregate: Metrics;
  nodes: NodeMetrics[];
  samples: Sample[];
  deleteMetrics: DeleteMetrics | null;
  mixedMetrics?: MixedMetrics | null;
  discoveryMetrics?: DiscoveryMetrics | null;
}
export interface DataSource {
  getSnapshot(signal?: AbortSignal): Promise<Snapshot>;
  startRun(config: RunConfig): Promise<Snapshot>;
}
