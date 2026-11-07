import type { NodeRow } from "../types";
import { formatMetric } from "../utils/metrics";

/** Per-client-machine live metrics, read from each node's own SPT API. */
export function NodeTable({ nodes }: { nodes: NodeRow[] }) {
  if (!nodes.length) return null;
  return (
    <section
      data-spt-legacy=""
      className="panel run-history-panel"
      aria-label="Client machines"
    >
      <h2 data-spt-legacy="">Client machines</h2>
      <div data-spt-legacy="" className="table-wrap">
        <table data-spt-legacy="" className="run-history-table node-table">
          <thead data-spt-legacy="">
            <tr data-spt-legacy="">
              <th data-spt-legacy="">Host</th>
              <th data-spt-legacy="">Status</th>
              <th data-spt-legacy="">Phase</th>
              <th data-spt-legacy="">Successful</th>
              <th data-spt-legacy="">Failed</th>
              <th data-spt-legacy="">IOPS</th>
              <th data-spt-legacy="">MiB/s</th>
              <th data-spt-legacy="">P50 µs</th>
              <th data-spt-legacy="">P99 µs</th>
            </tr>
          </thead>
          <tbody data-spt-legacy="">
            {nodes.map((n) => {
              const status = n.data_status;
              return (
                <tr data-spt-legacy="" key={n.host + (n.node_id ?? "")}>
                  <td data-spt-legacy="" className="history-run-id">
                    {n.host}
                    {n.node_id && <small data-spt-legacy="">{n.node_id}</small>}
                  </td>
                  <td data-spt-legacy="">{statusLabel(n)}</td>
                  <td data-spt-legacy="">
                    {status === "no_data" ? "No data" : n.op_type || "—"}
                  </td>
                  <td data-spt-legacy="">
                    {formatMetric(n.successful_ops, 0, status)}
                  </td>
                  <td data-spt-legacy="">
                    {formatMetric(n.failed_ops, 0, status)}
                  </td>
                  <td data-spt-legacy="">{formatMetric(n.iops, 1, status)}</td>
                  <td data-spt-legacy="">
                    {formatMetric(n.bandwidth_mib_s, 2, status)}
                  </td>
                  <td data-spt-legacy="">
                    {formatMetric(n.p50_latency_us, 1, status)}
                  </td>
                  <td data-spt-legacy="">
                    {formatMetric(n.p99_latency_us, 1, status)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <p data-spt-legacy="" className="history-footnote">
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
