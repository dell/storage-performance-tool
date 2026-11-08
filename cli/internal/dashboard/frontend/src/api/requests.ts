import type {
  CatalogArtifactsResponse,
  CatalogList,
  CompareResult,
  DetachResponse,
  LogLevel,
  LogResponse,
  Options,
  RunHistoryResponse,
  StartRunResponse,
  StopRunResponse,
  State,
  Storage,
  VerifyStorageResponse,
} from "../types";
import {
  catalogQuery,
  compareQuery,
  logURL,
  type CatalogFilters,
  type RuleOverrides,
} from "../utils/catalog";
import { ApiError, request } from "./client";

export async function getState(signal?: AbortSignal): Promise<State> {
  if (location.protocol === "file:")
    throw new ApiError(
      "Open the dashboard through its local server to load SPT results.",
    );
  const state = await request<State>(
    { url: "/api/state", signal },
    "Could not read SPT state.",
  );
  if (!state.target || !("job" in state) || !("metrics" in state))
    throw new ApiError(
      "The server returned an unsupported dashboard response.",
    );
  return state;
}

export const getRuns = (signal?: AbortSignal) =>
  request<RunHistoryResponse>(
    { url: "/api/runs", signal },
    "Could not load run history.",
  );

export const getSavedRun = (runID: string, signal?: AbortSignal) =>
  request<State>(
    { url: `/api/runs/${encodeURIComponent(runID)}`, signal },
    "Could not open saved run.",
  );

export const getCatalog = (filters: CatalogFilters, signal?: AbortSignal) =>
  request<CatalogList>(
    { url: catalogQuery(filters), signal },
    "Could not read results.",
  );

export const compareRuns = (
  ids: string[],
  baseline: string,
  rule: RuleOverrides,
  signal?: AbortSignal,
) =>
  request<CompareResult>(
    { url: compareQuery(ids, baseline, rule), signal },
    "Could not compare runs.",
  );

export const getLog = (
  base: string,
  level: LogLevel,
  source: LogResponse["source"] | "",
  signal?: AbortSignal,
) =>
  request<LogResponse>(
    { url: logURL(base, level, 200, source ? { source } : {}), signal },
    "Could not read the log.",
  );

export const getArtifacts = (runID: string, signal?: AbortSignal) =>
  request<CatalogArtifactsResponse>(
    { url: `/api/catalog/artifacts?id=${encodeURIComponent(runID)}`, signal },
    "Could not list artifacts.",
  );

export const verifyStorage = (settings: Pick<Storage, "hosts" | "endpoint">) =>
  request<VerifyStorageResponse>(
    { url: "/api/storage/verify", method: "POST", data: settings },
    "Verification request failed.",
  );

export const startRun = (options: Options) =>
  request<StartRunResponse>(
    { url: "/api/runs", method: "POST", data: options },
    "Run request failed.",
  );

export const attachRun = (host: string) =>
  request<State>(
    { url: "/api/attach", method: "POST", data: { host } },
    "Could not connect to the SPT run.",
  );

export const stopRun = () =>
  request<StopRunResponse>(
    { url: "/api/runs/stop", method: "POST" },
    "Stop request failed.",
  );

export const detachRun = () =>
  request<DetachResponse>(
    { url: "/api/attach", method: "DELETE" },
    "Could not disconnect from the run.",
  );
