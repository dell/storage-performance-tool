import type { NodeMetrics } from "../types";
import { formatNumber } from "../utils/metrics";
export function ClientTable({ nodes }: { nodes: NodeMetrics[] }) {
  return (
    <section className="panel">
      <div className="section-heading">
        <div>
          <h2>Client comparison</h2>
          <p>
            Same operation and reporting window · percentiles belong to each
            client
          </p>
        </div>
        <span className="subtle">{nodes.length} clients</span>
      </div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Client</th>
              <th>Status</th>
              <th>Current ops/s</th>
              <th>MiB/s</th>
              <th>P99 · ms</th>
              <th>Failures</th>
              <th>Last report</th>
            </tr>
          </thead>
          <tbody>
            {nodes.map((n) => (
              <tr key={n.id}>
                <td>
                  <strong>{n.id}</strong>
                </td>
                <td>
                  <span className={`status ${n.reporting ? "good" : "warn"}`}>
                    {n.reporting ? "Reporting" : "Missing"}
                  </span>
                </td>
                <td>{formatNumber(n.metrics.currentOps)}</td>
                <td>{formatNumber(n.metrics.bandwidthMiB, 1)}</td>
                <td>{formatNumber(n.metrics.latency?.p99Ms ?? null, 2)}</td>
                <td>{formatNumber(n.metrics.failures)}</td>
                <td>{new Date(n.lastSeenAt).toLocaleTimeString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
