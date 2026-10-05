import type { Snapshot } from "../types";
export function RunNotes({ snapshot: s }: { snapshot: Snapshot }) {
  const m = s.aggregate;
  return (
    <aside className="panel watch">
      <span className="eyebrow">RUN NOTES</span>
      <h2>Things to watch</h2>
      <div className={`watch-item ${m.failures ? "attention" : ""}`}>
        <span>{m.failures ? "!" : "✓"}</span>
        <div>
          <h3>{m.failures ? "Failures recorded" : "No failures recorded"}</h3>
          <p>
            {m.failures
              ? `${m.failures} failed operations so far. Compare clients and timing.`
              : "The resolved operation counters currently contain no failures."}
          </p>
        </div>
      </div>
      <div className="watch-item">
        <span>↗</span>
        <div>
          <h3>
            {s.partial
              ? "Cluster totals are partial"
              : "All expected clients reporting"}
          </h3>
          <p>
            {s.reportingNodes} of {s.expectedNodes} clients included in this
            snapshot.
          </p>
        </div>
      </div>
      <div className="watch-item">
        <span>i</span>
        <div>
          <h3>No latency threshold set</h3>
          <p>
            Latency cannot be classified as normal without a baseline or target.
          </p>
        </div>
      </div>
      <div className="explanation">
        <h3>Reading the metrics</h3>
        <p>
          <strong>Throughput</strong> counts successful operations per second.
        </p>
        <p>
          <strong>P99 latency</strong> is the time within which 99% of measured
          requests responded.
        </p>
        <p>
          <strong>Failures</strong> are run totals; corrupt operations are a
          subset.
        </p>
      </div>
      <p className="subtle">
        Last received {new Date(s.updatedAt).toLocaleTimeString()}
      </p>
    </aside>
  );
}
