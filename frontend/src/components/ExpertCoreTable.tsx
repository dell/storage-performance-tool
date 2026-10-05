import type { CoreScope, CoreStep, Metrics, Timing } from "../types";
import { formatNumber } from "../utils/metrics";
const columns: {
  label: string;
  value: (m: Metrics) => number | null;
  digits?: number;
}[] = [
  { label: "Current success · ops/s", value: (m) => m.currentOps },
  { label: "Average success · ops/s", value: (m) => m.averageOps },
  { label: "Current failure · ops/s", value: (m) => m.currentFailureOps },
  { label: "Success count", value: (m) => m.success, digits: 0 },
  { label: "Failure count", value: (m) => m.failures, digits: 0 },
  { label: "Corrupt count", value: (m) => m.corrupt, digits: 0 },
  { label: "Transferred · bytes", value: (m) => m.transferredBytes, digits: 0 },
  { label: "Current bandwidth · MiB/s", value: (m) => m.bandwidthMiB },
  { label: "Average bandwidth · MiB/s", value: (m) => m.averageBandwidthMiB },
  ...(["latency", "duration"] as const).flatMap((kind) =>
    (["mean", "min", "p50", "p90", "p99", "p999", "max"] as const).map(
      (stat) => ({
        label: `${kind === "latency" ? "Latency" : "Duration"} ${stat} · ms`,
        value: (m: Metrics) => m[kind]?.[`${stat}Ms` as keyof Timing] ?? null,
      }),
    ),
  ),
  { label: "Current in-flight", value: (m) => m.concurrency, digits: 0 },
  { label: "Mean in-flight", value: (m) => m.meanConcurrency },
];
export function ExpertCoreTable({
  scope,
  step,
  threadsPerClient,
  showStep = true,
}: {
  scope: CoreScope;
  step: CoreStep;
  threadsPerClient: number;
  showStep?: boolean;
}) {
  const rows = [
    {
      id: "Cluster aggregate",
      reporting: !step.partial,
      metrics: scope.aggregate,
    },
    ...scope.nodes,
  ];
  return (
    <section className="panel">
      <div className="section-heading">
        <div>
          <h2>
            {scope.operation}
            {showStep ? ` · ${step.label}` : ""} · clients and cluster
          </h2>
          <p>
            Expected {step.expectedNodes} · Reporting {step.reportingNodes} ·
            Current rates: trailing {scope.aggregate.rateWindowSeconds}s average
          </p>
        </div>
        <span className={`status ${step.partial ? "warn" : "good"}`}>
          {step.partial ? "Partial cluster total" : "Complete cluster total"}
        </span>
      </div>
      <div
        className="expert-core-table"
        role="region"
        aria-label={`${showStep ? `${step.label} ` : ""}${scope.operation} node metrics`}
        tabIndex={0}
      >
        <table>
          <thead>
            <tr>
              <th scope="col">Scope</th>
              <th scope="col">Reporting</th>
              {columns.map((c) => (
                <th scope="col" key={c.label}>
                  {c.label}
                </th>
              ))}
              <th scope="col">Configured threads</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((n, i) => (
              <tr key={n.id} className={i === 0 ? "cluster-row" : ""}>
                <th scope="row">{n.id}</th>
                <td>
                  {i === 0
                    ? step.partial
                      ? "Partial"
                      : "Complete"
                    : n.reporting
                      ? "Reporting"
                      : "Missing"}
                </td>
                {columns.map((c) => (
                  <td key={c.label}>
                    {formatNumber(c.value(n.metrics), c.digits ?? 3)}
                  </td>
                ))}
                <td>
                  {formatNumber(
                    i === 0 ? step.configuredThreads : threadsPerClient,
                    0,
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {scope.operation === "DELETE" && (
        <p className="delete-note">
          DELETE transfers 0 bytes; bandwidth is not applicable. Counts and
          timing are per request.
        </p>
      )}
    </section>
  );
}
