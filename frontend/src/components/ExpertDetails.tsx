import type { Snapshot, Timing } from "../types";
import { formatNumber } from "../utils/metrics";
function TimingTable({
  label,
  value,
}: {
  label: string;
  value: Timing | null;
}) {
  return (
    <div className="timing">
      <h3>{label}</h3>
      {!value ? (
        <p>Not available for this operation</p>
      ) : (
        <dl>
          {Object.entries(value).map(([k, v]) => (
            <div key={k}>
              <dt>{k.replace("Ms", "")}</dt>
              <dd>{formatNumber(v, 3)} ms</dd>
            </div>
          ))}
        </dl>
      )}
    </div>
  );
}
export function ExpertDetails({
  snapshot: s,
  showRaw = true,
}: {
  snapshot: Snapshot;
  showRaw?: boolean;
}) {
  return (
    <section className="panel">
      <div className="section-heading">
        <div>
          <h2>Expert details</h2>
          <p>
            Run totals and timing distributions · aggregate percentiles come
            from the data source
          </p>
        </div>
      </div>
      <div className="timing-grid">
        <TimingTable label="Request latency" value={s.aggregate.latency} />
        <TimingTable label="Operation duration" value={s.aggregate.duration} />
        <TimingTable label="Time to first byte" value={s.aggregate.ttfb} />
      </div>
      <div className="detail-strip">
        <span>
          In-flight: <strong>{formatNumber(s.aggregate.concurrency)}</strong>
        </span>
        <span>
          Mean in-flight:{" "}
          <strong>{formatNumber(s.aggregate.meanConcurrency, 1)}</strong>
        </span>
        <span>
          Configured threads:{" "}
          <strong>{s.config.threadsPerClient * s.config.expectedNodes}</strong>
        </span>
        <span>
          Integrity failures: <strong>{s.aggregate.corrupt}</strong> (included
          in failures)
        </span>
      </div>
      {showRaw && (
        <details>
          <summary>Raw normalized snapshot</summary>
          <pre>{JSON.stringify(s, null, 2)}</pre>
        </details>
      )}
    </section>
  );
}
