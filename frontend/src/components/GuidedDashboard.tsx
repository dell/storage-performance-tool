import type { Snapshot, Timing, Metrics } from "../types";
import { formatNumber, formatElapsed } from "../utils/metrics";
import { FleetTelemetry } from "./FleetTelemetry";
import { ExpertCoreTable } from "./ExpertCoreTable";
import { RunNotes } from "./RunNotes";
import { MixedWorkload } from "./MixedWorkload";
function Values({
  title,
  rows,
}: {
  title: string;
  rows: [string, number | null, string?][];
}) {
  return (
    <article className="timing">
      <h3>{title}</h3>
      <dl>
        {rows.map(([name, value, unit]) => (
          <div key={name}>
            <dt>{name}</dt>
            <dd>
              {formatNumber(value, unit === "count" ? 0 : 3)}
              {value === null ? "" : unit && unit !== "count" ? ` ${unit}` : ""}
            </dd>
          </div>
        ))}
      </dl>
    </article>
  );
}
function TimingValues({
  title,
  value,
}: {
  title: string;
  value: Timing | null;
}) {
  return (
    <Values
      title={title}
      rows={(["mean", "min", "p50", "p99", "max"] as const).map((k) => [
        k.toUpperCase(),
        value?.[`${k}Ms`] ?? null,
        "ms",
      ])}
    />
  );
}
export function GuidedDashboard({
  snapshot: s,
  error,
  embedded = false,
}: {
  snapshot: Snapshot;
  error: string | null;
  embedded?: boolean;
}) {
  const activeStep =
    s.coreSteps.find((step) => step.state === "running") ?? s.coreSteps.at(-1)!;
  const steps = embedded ? s.coreSteps : [activeStep];
  const multipleSteps = s.coreSteps.length > 1;
  return (
    <>
      {!embedded && (
        <div className="page-heading">
          <div>
            <span className="eyebrow">GUIDED / {s.runId}</span>
            <h1>{s.config.name}</h1>
            <p>
              {multipleSteps
                ? "Core KPIs for the current step."
                : `${s.config.operation} · Core KPIs`}
            </p>
          </div>
        </div>
      )}
      {error && (
        <div className="error" role="alert">
          {error} · Showing last received data.
        </div>
      )}
      <section className="panel">
        <div className="section-heading">
          <h2>Progress & run state</h2>
          <span className={`status ${s.state === "failed" ? "warn" : "good"}`}>
            {s.state}
          </span>
        </div>
        <ProgressBar label="Overall completion" value={s.progressPercent} />
        <p className="subtle">
          Run unbounded: {s.unbounded ? "Yes" : "No"} · Elapsed{" "}
          {formatElapsed(s.elapsedSeconds)}
        </p>
      </section>
      {!embedded && (
        <div className="overview-grid">
          <FleetTelemetry samples={s.samples} />
          <RunNotes snapshot={s} />
        </div>
      )}
      {steps.map((step) => (
        <div className="guided-step" key={step.id}>
          {multipleSteps && (
            <section className="panel">
              <div className="section-heading">
                <h2>{step.label}</h2>
                <span className="status">{step.state}</span>
              </div>
              <ProgressBar
                label={`${step.label} completion`}
                value={step.completionPercent}
              />
              <p className="subtle">
                Step elapsed: {formatElapsed(step.elapsedSeconds)} ·{" "}
                {step.unbounded ? "Unbounded" : "Bounded"}
              </p>
              <p>
                Expected nodes: {step.expectedNodes} · Reporting nodes:{" "}
                {step.reportingNodes}
              </p>
              <span className={`status ${step.partial ? "warn" : "good"}`}>
                {step.partial
                  ? "Partial cluster total"
                  : "All expected nodes included"}
              </span>
            </section>
          )}
          {(embedded ? step.scopes : step.scopes.slice(0, 1)).map((scope) => (
            <div key={scope.operation}>
              {embedded ? (
                <ExpertCoreTable
                  scope={scope}
                  step={step}
                  threadsPerClient={s.config.threadsPerClient}
                  showStep={multipleSteps}
                />
              ) : (
                <CoreMetrics
                  m={scope.aggregate}
                  title={`${scope.operation}${multipleSteps ? ` · ${step.label}` : ""} · Cluster aggregate`}
                  configuredThreads={step.configuredThreads}
                  operation={scope.operation}
                  reporting={step.partial ? "Partial" : "Complete"}
                />
              )}
            </div>
          ))}
          {!embedded && (
            <section className="panel">
              <div className="section-heading">
                <div>
                  <h2>Scale · clients and cluster</h2>
                  <p>
                    Expected nodes: {step.expectedNodes} · Reporting nodes:{" "}
                    {step.reportingNodes}
                  </p>
                </div>
                <span className={`status ${step.partial ? "warn" : "good"}`}>
                  {step.partial
                    ? "Partial cluster total"
                    : "All expected nodes included"}
                </span>
              </div>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Scope</th>
                      <th>Reporting</th>
                      <th>Success ops/s</th>
                      <th>Failure ops/s</th>
                      <th>MiB/s</th>
                      <th>P99 · ms</th>
                      <th>Success count</th>
                      <th>Failure count</th>
                    </tr>
                  </thead>
                  <tbody>
                    {[
                      {
                        id: "Cluster aggregate",
                        reporting: !step.partial,
                        metrics: step.scopes[0].aggregate,
                      },
                      ...step.scopes[0].nodes,
                    ].map((node) => (
                      <tr key={node.id}>
                        <td>{node.id}</td>
                        <td>
                          {node.id === "Cluster aggregate"
                            ? step.partial
                              ? "Partial"
                              : "Complete"
                            : node.reporting
                              ? "Reporting"
                              : "Missing"}
                        </td>
                        <td>{formatNumber(node.metrics.currentOps, 2)}</td>
                        <td>
                          {formatNumber(node.metrics.currentFailureOps, 2)}
                        </td>
                        <td>{formatNumber(node.metrics.bandwidthMiB, 2)}</td>
                        <td>
                          {formatNumber(node.metrics.latency?.p99Ms ?? null, 3)}
                        </td>
                        <td>{formatNumber(node.metrics.success)}</td>
                        <td>{formatNumber(node.metrics.failures)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          )}
          {step.mixedMetrics && <MixedWorkload value={step.mixedMetrics} />}
        </div>
      ))}
    </>
  );
}
function ProgressBar({
  label,
  value,
}: {
  label: string;
  value: number | null;
}) {
  return (
    <div className="guided-progress">
      <div>
        <strong>{label}</strong>
        <span>
          {value === null
            ? "Unbounded · continuously running"
            : `${formatNumber(value, 1)}%`}
        </span>
      </div>
      {value !== null && (
        <progress
          aria-label={label}
          max={100}
          value={Math.max(0, Math.min(100, value))}
        />
      )}
    </div>
  );
}
function CoreMetrics({
  m,
  title,
  configuredThreads,
  operation,
  reporting,
}: {
  m: Metrics;
  title: string;
  configuredThreads: number;
  operation: string;
  reporting: string;
}) {
  return (
    <section className="panel">
      <div className="section-heading">
        <div>
          <h2>{title}</h2>
          <p>
            {reporting} · Latency and duration in ms · Current rates smoothed
            over {m.rateWindowSeconds}s.
          </p>
        </div>
      </div>
      <div className="guided-core-grid">
        <Values
          title="Throughput"
          rows={[
            ["Current successful rate", m.currentOps, "ops/s"],
            ["Average successful rate", m.averageOps, "ops/s"],
          ]}
        />
        <Values
          title="Bandwidth"
          rows={[
            ["Cumulative transferred", m.transferredBytes, "bytes"],
            ["Current rate (smoothed)", m.bandwidthMiB, "MiB/s"],
            ["Average rate", m.averageBandwidthMiB, "MiB/s"],
          ]}
        />
        <TimingValues title="Request latency" value={m.latency} />
        <TimingValues title="Operation duration" value={m.duration} />
        <Values
          title="Operations"
          rows={[
            ["Success count", m.success, "count"],
            ["Failure count", m.failures, "count"],
            ["Corrupt count (subset of failures)", m.corrupt, "count"],
            ["Current success rate", m.currentOps, "ops/s"],
            ["Current failure rate", m.currentFailureOps, "ops/s"],
          ]}
        />
        <Values
          title="Concurrency"
          rows={[
            ["Current in-flight", m.concurrency, "count"],
            ["Mean in-flight", m.meanConcurrency],
            ["Configured thread count", configuredThreads, "count"],
          ]}
        />
      </div>
      {operation === "DELETE" && (
        <p className="delete-note">
          DELETE transfers 0 bytes; bandwidth rate is not applicable and shown
          as —. Counts are per request; failures include requests with partial
          errors.
        </p>
      )}
    </section>
  );
}
