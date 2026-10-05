import type { MixedMetrics, MixedOperation } from "../types.ts";
export const mixedOperations: MixedOperation[] = [
  "READ",
  "CREATE",
  "STAT",
  "DELETE",
];
export const configuredMixedShares: Record<MixedOperation, number> = {
  READ: 60,
  CREATE: 25,
  STAT: 10,
  DELETE: 5,
};
const observedWeights: Record<MixedOperation, number> = {
  READ: 55,
  CREATE: 28,
  STAT: 11,
  DELETE: 6,
};
/** Integer fixture counts; observed share is calculated from counts, never copied from configured share. */
export function createMixedMetrics(totalAttempted: number): MixedMetrics {
  const counts = mixedOperations.map((operation) => ({
    operation,
    count: Math.floor((totalAttempted * observedWeights[operation]) / 100),
    remainder: ((totalAttempted * observedWeights[operation]) / 100) % 1,
  }));
  let remaining =
    totalAttempted - counts.reduce((sum, row) => sum + row.count, 0);
  const byRemainder = [...counts].sort((a, b) => b.remainder - a.remainder);
  for (let i = 0; i < remaining; i++)
    byRemainder[i % byRemainder.length].count++;
  return {
    basis: "attempted",
    scope: "cumulative run",
    totalAttempted,
    operations: counts.map(({ operation, count }) => ({
      operation,
      configuredSharePercent: configuredMixedShares[operation],
      attemptedCount: count,
      observedSharePercent:
        totalAttempted === 0 ? null : (count / totalAttempted) * 100,
    })),
  };
}
