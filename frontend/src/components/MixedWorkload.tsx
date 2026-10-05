import type { MixedMetrics } from "../types";
import { formatNumber } from "../utils/metrics";
export function MixedWorkload({ value: m }: { value: MixedMetrics }) {
  return (
    <section className="panel mixed-workload">
      <div className="section-heading">
        <div>
          <h2>Mixed workload distribution</h2>
          <p>
            Configured share vs actual observed share · cumulative attempted
            operations
          </p>
        </div>
        <span className="status good">
          {formatNumber(m.totalAttempted)} attempted
        </span>
      </div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Operation</th>
              <th>Configured share</th>
              <th>Observed share</th>
              <th>Difference</th>
              <th>Attempted operations</th>
            </tr>
          </thead>
          <tbody>
            {m.operations.map((row) => {
              const delta =
                row.observedSharePercent === null
                  ? null
                  : row.observedSharePercent - row.configuredSharePercent;
              return (
                <tr key={row.operation}>
                  <td>
                    <strong>{row.operation}</strong>
                  </td>
                  <td>
                    <div className="share-cell">
                      <span>
                        {formatNumber(row.configuredSharePercent, 2)}%
                      </span>
                      <progress
                        aria-label={`${row.operation} configured share`}
                        max="100"
                        value={row.configuredSharePercent}
                      />
                    </div>
                  </td>
                  <td>
                    {row.observedSharePercent === null ? (
                      "—"
                    ) : (
                      <div className="share-cell observed">
                        <span>
                          {formatNumber(row.observedSharePercent, 2)}%
                        </span>
                        <progress
                          aria-label={`${row.operation} observed share`}
                          max="100"
                          value={row.observedSharePercent}
                        />
                      </div>
                    )}
                  </td>
                  <td>
                    {delta === null
                      ? "—"
                      : `${delta > 0 ? "+" : ""}${formatNumber(delta, 2)} pp`}
                  </td>
                  <td>{formatNumber(row.attemptedCount)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <p className="delete-note">
        Observed share = each operation’s attempted count ÷ all attempted
        operations × 100%. Difference is in percentage points (pp), not relative
        percent change.
      </p>
      {m.totalAttempted === 0 && (
        <p className="subtle">
          No operations attempted yet. Observed shares will appear after the
          first sample.
        </p>
      )}
    </section>
  );
}
