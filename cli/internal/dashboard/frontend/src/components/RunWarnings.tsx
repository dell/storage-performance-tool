import type { RunWarning } from "../types";

/** Live observations about unreliable telemetry. Text is what was observed only. */
export function RunWarnings({ warnings }: { warnings?: RunWarning[] }) {
  if (!warnings?.length) return null;
  return (
    <section
      className="panel notice-warn run-warnings"
      aria-label="Run warnings"
    >
      <div className="section-heading">
        <div>
          <h2>Run warnings</h2>
          <p>
            Observations from SPT metrics and reachability. These describe what
            was seen; they do not claim a root cause.
          </p>
        </div>
      </div>
      <ul className="run-warnings-list">
        {warnings.map((w, i) => (
          <li key={`${w.code}-${w.host ?? ""}-${w.metric ?? ""}-${i}`}>
            <strong>{label(w.code)}</strong>
            {w.host ? <span className="warn-host">{w.host}</span> : null}
            {w.metric ? <span className="warn-metric">{w.metric}</span> : null}
            <p>{w.observed}</p>
          </li>
        ))}
      </ul>
    </section>
  );
}

function label(code: string): string {
  switch (code) {
    case "fleet_partial":
      return "Incomplete fleet aggregate";
    case "node_no_data":
      return "Client metrics unavailable";
    case "sample_delayed":
      return "Delayed telemetry";
    case "metrics_gap":
      return "Telemetry gap";
    case "failed_operations":
      return "Failed operations reported";
    default:
      return code.replaceAll("_", " ");
  }
}
