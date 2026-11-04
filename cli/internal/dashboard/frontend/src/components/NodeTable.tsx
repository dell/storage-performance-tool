import type { NodeRow } from "../types";
import { formatMetric } from "../utils/metrics";

/** Per-client-machine live metrics, read from each node's own SPT API. */
export function NodeTable({ nodes }: { nodes: NodeRow[] }) {
  if (!nodes.length) return null;
  return (
    <section className="panel run-history-panel" aria-label="Client machines">
      <h2>Client machines</h2>
      <div className="table-wrap">
        <table className="run-history-table node-table">
          <thead>
            <tr>
              <th>Host</th>
              <th>Status</th>
              <th>Phase</th>
              <th>Successful</th>
              <th>Failed</th>
              <th>IOPS</th>
              <th>MiB/s</th>
              <th>P50 µs</th>
              <th>P99 µs</th>
            </tr>
          </thead>
          <tbody>
            {nodes.map((n) => {
              const status = n.data_status;
              return (
                <tr key={n.host + (n.node_id ?? "")}>
                  <td className="history-run-id">
                    {n.host}
                    {n.node_id && <small>{n.node_id}</small>}
                  </td>
                  <td>{statusLabel(n)}</td>
                  <td>{status === "no_data" ? "No data" : n.op_type || "—"}</td>
                  <td>{formatMetric(n.successful_ops, 0, status)}</td>
                  <td>{formatMetric(n.failed_ops, 0, status)}</td>
                  <td>{formatMetric(n.iops, 1, status)}</td>
                  <td>{formatMetric(n.bandwidth_mib_s, 2, status)}</td>
                  <td>{formatMetric(n.p50_latency_us, 1, status)}</td>
                  <td>{formatMetric(n.p99_latency_us, 1, status)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <p className="history-footnote">
        Live values from each machine’s SPT API. “No data” means this poll had
        no usable sample (not a measured zero). “Delayed” means the sample is
        older than the freshness window. Unavailable means the API could not be
        reached; that does not by itself mean the run failed.
      </p>
    </section>
  );
}

function statusLabel(n: NodeRow): string {
  if (n.data_status === "no_data" || n.status === "unavailable")
    return "No data";
  if (n.data_status === "delayed") return "Delayed";
  if (n.status === "reporting") return "Reporting";
  return n.status;
}
