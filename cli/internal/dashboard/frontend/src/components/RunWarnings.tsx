import type { RunWarning } from "../types";

/** Live observations about unreliable telemetry. Text is what was observed only. */
export function RunWarnings({ warnings }: { warnings?: RunWarning[] }) {
  if (!warnings?.length) return null;
  return (
    <section
      data-spt-legacy=""
      className="panel notice-warn run-warnings"
      aria-label="Run warnings"
    >
      <div data-spt-legacy="" className="section-heading">
        <div data-spt-legacy="">
          <h2 data-spt-legacy="">Run warnings</h2>
          <p data-spt-legacy="">
            Observations from SPT metrics and reachability. These describe what
            was seen; they do not claim a root cause.
          </p>
        </div>
      </div>
      <ul data-spt-legacy="" className="run-warnings-list">
        {warnings.map((w, i) => (
          <li
            data-spt-legacy=""
            key={`${w.code}-${w.host ?? ""}-${w.metric ?? ""}-${i}`}
          >
            <strong data-spt-legacy="">{label(w.code)}</strong>
            {w.host ? (
              <span data-spt-legacy="" className="warn-host">
                {w.host}
              </span>
            ) : null}
            {w.metric ? (
              <span data-spt-legacy="" className="warn-metric">
                {w.metric}
              </span>
            ) : null}
            <p data-spt-legacy="">{w.observed}</p>
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
