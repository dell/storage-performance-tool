import type { RunConfig, DiscoveryMetrics } from "../types.ts";
/** Independent synthetic discovery fixtures; never inferred from benchmark throughput. */
export function createDiscoveryMetrics(
  config: RunConfig,
  elapsed: number,
  started: number,
): DiscoveryMetrics | null {
  if (!config.shardDiscovery?.enabled) return null;
  const t = Math.max(0, Math.min(config.durationSeconds, elapsed)),
    end = config.durationSeconds,
    stallStart = end * 0.25,
    stallEnd = end * 0.5;
  const definitions = [
    { prefix: "analytics/", total: 24000, pageMs: 12.4 },
    { prefix: "archive/", total: 18000, pageMs: 15.6 },
    { prefix: "images/", total: 32000, pageMs: 10.8 },
    { prefix: "logs/", total: 12000, pageMs: 18.2 },
  ];
  const prefixes = definitions.map((d, i) => {
    const stall = config.shardDiscovery?.simulateStall && i === 3;
    const effective = stall
      ? t - Math.max(0, Math.min(t, stallEnd) - stallStart)
      : t;
    const available = stall ? end - (stallEnd - stallStart) : end;
    const progress = Math.min(1, effective / available),
      discovered = Math.floor(d.total * progress),
      pages = discovered === 0 ? 0 : Math.ceil(discovered / 500),
      stalled = Boolean(stall && t >= stallStart && t < stallEnd),
      status =
        t === 0
          ? "pending"
          : progress >= 1
            ? "completed"
            : stalled
              ? "stalled"
              : "active";
    const lastProgress =
      t === 0
        ? null
        : new Date(started + (stalled ? stallStart : t) * 1000).toISOString();
    return {
      prefix: d.prefix,
      objectsDiscovered: discovered,
      expectedObjects: d.total,
      pagesReturned: pages,
      progressPercent: progress * 100,
      status,
      lastProgressAt: lastProgress,
      activeShards:
        status === "active" ? Math.max(1, Math.floor(progress * 8)) : 0,
      averagePageResponseMs: pages === 0 ? null : d.pageMs,
    };
  });
  const totalObjectsDiscovered = prefixes.reduce(
      (n, p) => n + p.objectsDiscovered,
      0,
    ),
    totalPagesReturned = prefixes.reduce((n, p) => n + p.pagesReturned, 0);
  const splitsByReason = [
    { reason: "page-limit (mock)", count: Math.floor(totalPagesReturned / 8) },
    {
      reason: "dense-prefix (mock)",
      count: Math.floor(totalPagesReturned / 12),
    },
  ];
  const totalSplits = splitsByReason.reduce((n, r) => n + r.count, 0);
  return {
    enabled: true,
    totalObjectsDiscovered,
    totalPagesReturned,
    averagePageResponseMs: totalPagesReturned
      ? prefixes.reduce(
          (n, p) => n + (p.averagePageResponseMs ?? 0) * p.pagesReturned,
          0,
        ) / totalPagesReturned
      : null,
    totalSplits,
    maxDepth: totalSplits === 0 ? 0 : Math.min(4, totalSplits),
    splitsByReason,
    prefixes: prefixes.map((p) => ({
      ...p,
      status: p.status as "pending" | "active" | "stalled" | "completed",
    })),
  };
}
