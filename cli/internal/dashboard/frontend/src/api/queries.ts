import { useState } from "react";
import { keepPreviousData, skipToken, useQuery } from "@tanstack/react-query";
import type { LogLevel, LogResponse } from "../types";
import type { CatalogFilters, RuleOverrides } from "../utils/catalog";
import { queryKeys } from "./queryKeys";
import {
  compareRuns,
  getArtifacts,
  getCatalog,
  getLog,
  getRuns,
  getSavedRun,
  getState,
} from "./requests";

export function useServerState() {
  return useQuery({
    queryKey: queryKeys.state,
    queryFn: ({ signal }) => getState(signal),
    refetchInterval: 2000,
    refetchIntervalInBackground: true,
  });
}

export function useRuns() {
  return useQuery({
    queryKey: queryKeys.runs,
    queryFn: ({ signal }) => getRuns(signal),
  });
}

export function useSavedRun(runID: string | null) {
  return useQuery({
    queryKey: queryKeys.savedRun(runID),
    queryFn: runID ? ({ signal }) => getSavedRun(runID, signal) : skipToken,
  });
}

export function useCatalog(filters: CatalogFilters) {
  return useQuery({
    queryKey: queryKeys.catalog(filters),
    queryFn: ({ signal }) => getCatalog(filters, signal),
    placeholderData: keepPreviousData,
  });
}

export function useComparison(
  ids: string[],
  baseline: string,
  rule: RuleOverrides,
) {
  return useQuery({
    queryKey: queryKeys.comparison(ids, baseline, rule),
    queryFn: ({ signal }) => compareRuns(ids, baseline, rule, signal),
    enabled: ids.length >= 2,
    placeholderData: keepPreviousData,
  });
}

export function useLog(
  base: string,
  level: LogLevel,
  source: LogResponse["source"] | "",
  refreshMs: number,
) {
  return useQuery({
    queryKey: queryKeys.log(base, level, source, refreshMs > 0),
    queryFn: ({ signal }) => getLog(base, level, source, signal),
    refetchInterval: refreshMs > 0 ? refreshMs : false,
    refetchIntervalInBackground: true,
    placeholderData: keepPreviousData,
  });
}

export function useArtifacts(runID: string) {
  return useQuery({
    queryKey: queryKeys.artifacts(runID),
    queryFn: ({ signal }) => getArtifacts(runID, signal),
  });
}

export function useLastSuccessfulData<T>(data: T | undefined): T | undefined {
  const [previous, setPrevious] = useState(data);
  if (data !== undefined && data !== previous) setPrevious(data);
  return data === undefined ? previous : data;
}
