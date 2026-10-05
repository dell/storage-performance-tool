import type { DiscoveryMetrics } from "../types";
import { formatNumber } from "../utils/metrics";
import { MetricCard } from "./MetricCard";
export function ShardDiscovery({ value: d }: { value: DiscoveryMetrics }) {
  if (!d.enabled) return null;
  return (
    <section className="panel shard-discovery">
      <div className="section-heading">
        <div>
          <h2>Shard discovery · LIST metrics</h2>
          <p>
            Discovery counters are separate from benchmark operations and
            transfer rates.
          </p>
        </div>
        <span className="status good">Metrics enabled</span>
      </div>
      <div className="metrics-grid">
        <MetricCard
          label="Objects discovered"
          value={formatNumber(d.totalObjectsDiscovered)}
          detail="Cumulative discovery count"
        />
        <MetricCard
          label="Pages returned"
          value={formatNumber(d.totalPagesReturned)}
          detail="Cumulative LIST responses"
          accent="teal"
        />
        <MetricCard
          label="Average page response"
          value={formatNumber(d.averagePageResponseMs, 2)}
          unit="ms"
          detail="Page-count weighted average"
          accent="purple"
        />
        <MetricCard
          label="Shard splits"
          value={formatNumber(d.totalSplits)}
          detail={`Maximum depth ${d.maxDepth}`}
          accent="orange"
        />
      </div>
      <details className="delete-section">
        <summary>Splits by reason</summary>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Split reason</th>
                <th>Count</th>
              </tr>
            </thead>
            <tbody>
              {d.splitsByReason.map((r) => (
                <tr key={r.reason}>
                  <td>{r.reason}</td>
                  <td>{formatNumber(r.count)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </details>
      <div className="section-heading">
        <div>
          <h3>Per-prefix shard progress</h3>
          <p>Stalled is reported by the data source, not inferred by the UI.</p>
        </div>
      </div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Prefix</th>
              <th>Status</th>
              <th>Objects discovered</th>
              <th>Pages</th>
              <th>Progress</th>
              <th>Active shards</th>
              <th>Last progress</th>
            </tr>
          </thead>
          <tbody>
            {d.prefixes.map((p) => (
              <tr key={p.prefix}>
                <td>
                  <strong>{p.prefix}</strong>
                </td>
                <td>
                  <span
                    className={`status ${p.status === "stalled" ? "warn" : "good"}`}
                  >
                    {p.status}
                  </span>
                </td>
                <td>{formatNumber(p.objectsDiscovered)}</td>
                <td>{formatNumber(p.pagesReturned)}</td>
                <td>
                  {p.progressPercent === null ? (
                    "Unknown total"
                  ) : (
                    <div className="share-cell">
                      <span>{formatNumber(p.progressPercent, 1)}%</span>
                      <progress
                        aria-label={`${p.prefix} discovery progress`}
                        max="100"
                        value={p.progressPercent}
                      />
                    </div>
                  )}
                </td>
                <td>{formatNumber(p.activeShards)}</td>
                <td>
                  {p.lastProgressAt
                    ? new Date(p.lastProgressAt).toLocaleTimeString()
                    : "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="delete-note">
        Progress percentages require a known expected object count. With an
        unknown total, show discovered counts and status instead.
      </p>
    </section>
  );
}
