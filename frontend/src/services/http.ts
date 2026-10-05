import type { DataSource, RunConfig, Snapshot } from "../types";
// Our normalized backend contract. Not the raw SPT JSON response.
export function createHttpSource(baseUrl: string): DataSource {
  const base = baseUrl.replace(/\/$/, "");
  async function request(
    path: string,
    options: RequestInit = {},
  ): Promise<Snapshot> {
    const response = await fetch(`${base}${path}`, {
      ...options,
      headers: { "Content-Type": "application/json", ...options.headers },
    });
    if (!response.ok)
      throw new Error(`Backend request failed (${response.status})`);
    const value = await response.json();
    if (
      value.schemaVersion !== 3 ||
      !["mock", "spt"].includes(value.source) ||
      !Array.isArray(value.coreSteps) ||
      !value.coreSteps.length ||
      typeof value.unbounded !== "boolean" ||
      !Array.isArray(value.nodes) ||
      !Array.isArray(value.samples) ||
      !value.aggregate ||
      !value.config
    )
      throw new Error(
        "Unsupported response. Implement the dashboard v3 adapter.",
      );
    return value as Snapshot;
  }
  return {
    getSnapshot(signal) {
      return request("/runs/current", { signal });
    },
    startRun(config: RunConfig) {
      return request("/runs", { method: "POST", body: JSON.stringify(config) });
    },
  };
}
