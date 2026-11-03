import type { LogLevel, LogResponse } from "../types";
import type { CatalogFilters, RuleOverrides } from "../utils/catalog";

export const queryKeys = {
  state: ["state"] as const,
  runs: ["runs", "list"] as const,
  savedRun: (runID: string | null) => ["runs", "detail", runID] as const,
  catalogRoot: ["catalog"] as const,
  catalog: (filters: CatalogFilters) =>
    ["catalog", "list", { ...filters, q: filters.q.trim() }] as const,
  comparison: (ids: string[], baseline: string, rule: RuleOverrides) =>
    ["catalog", "compare", { ids, baseline, ...rule }] as const,
  log: (
    base: string,
    level: LogLevel,
    source: LogResponse["source"] | "",
    live: boolean,
  ) => ["logs", base, { level, source, tail: 200, live }] as const,
  artifacts: (runID: string) => ["catalog", "artifacts", runID] as const,
};
