import type { NodeRow } from "../types";
import { formatNumber as fmt } from "../utils/metrics";

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
            {nodes.map((n) => (
              <tr key={n.host + (n.node_id ?? "")}>
                <td className="history-run-id">
                  {n.host}
                  {n.node_id && <small>{n.node_id}</small>}
                </td>
                <td>
                  {n.status === "reporting" ? "Reporting" : "Unavailable"}
                </td>
                <td>{n.op_type || "—"}</td>
                <td>{fmt(n.successful_ops)}</td>
                <td>{fmt(n.failed_ops)}</td>
                <td>{fmt(n.iops, 1)}</td>
                <td>{fmt(n.bandwidth_mib_s, 2)}</td>
                <td>{fmt(n.p50_latency_us, 1)}</td>
                <td>{fmt(n.p99_latency_us, 1)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="history-footnote">
        Live values from each machine’s SPT API. A machine shows as unavailable
        when its API cannot be reached; that does not by itself mean its run
        failed.
      </p>
    </section>
  );
}
