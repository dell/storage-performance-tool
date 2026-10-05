import type { DeleteMetrics, Timing } from "../types";
import { formatNumber } from "../utils/metrics";
function Rows({ rows }: { rows: [string, number | null, string?][] }) {
  return (
    <dl>
      {rows.map(([label, value, unit]) => (
        <div key={label}>
          <dt>{label}</dt>
          <dd>
            {formatNumber(
              value,
              unit === "%" || unit === "s" || unit === "ms"
                ? 2
                : unit === "objects/request"
                  ? 2
                  : 0,
            )}
            {value === null ? "" : unit ? ` ${unit}` : ""}
          </dd>
        </div>
      ))}
    </dl>
  );
}
function RequestTiming({
  title,
  value,
}: {
  title: string;
  value: Timing | null;
}) {
  return (
    <div className="timing">
      <h3>{title} · per request</h3>
      {value ? (
        <Rows
          rows={Object.entries(value).map(([k, v]) => [
            k === "p999Ms" ? "P99.9" : k.replace("Ms", ""),
            v,
            "ms",
          ])}
        />
      ) : (
        <p>No completed requests measured yet</p>
      )}
    </div>
  );
}
export function DeleteDetails({ value: d }: { value: DeleteMetrics }) {
  return (
    <section className="panel delete-details">
      <div className="section-heading">
        <div>
          <h2>Delete details</h2>
          <p>
            Requests and objects are counted independently. Accepted means the
            API accepted the deletion, not that absence is proven.
          </p>
        </div>
        <span
          className={`status ${d.verification.removalConfirmed ? "good" : "warn"}`}
        >
          {d.verification.removalConfirmed
            ? "Removal confirmed"
            : "Removal not confirmed"}
        </span>
      </div>
      <div className="timing-grid">
        <div className="timing">
          <h3>Requests</h3>
          <Rows
            rows={[
              ["Attempted", d.requests.attempted],
              ["Full success", d.requests.fullSuccess],
              ["Partial success", d.requests.partial],
              ["Failed", d.requests.failed],
              ["Unresolved", d.requests.unresolved],
              ["Attempt rate", d.requests.perSecond, "requests/s"],
              ["Request completion", d.completion.requestPercent, "%"],
            ]}
          />
        </div>
        <div className="timing">
          <h3>Objects</h3>
          <Rows
            rows={[
              ["Selected", d.objects.selected],
              ["Attempted", d.objects.attempted],
              ["Accepted", d.objects.accepted],
              ["Failed", d.objects.failed],
              ["Unattempted", d.objects.unattempted],
              ["Unresolved", d.objects.unresolved],
              ["Attempt rate", d.objects.perSecond, "objects/s"],
              ["Object completion", d.completion.objectPercent, "%"],
            ]}
          />
        </div>
        <div className="timing">
          <h3>Failure budget</h3>
          <span
            className={`status ${d.failureBudget.outcome === "failed" ? "warn" : "good"}`}
          >
            {d.failureBudget.outcome}
          </span>
          <Rows
            rows={[
              ["Failed objects", d.failureBudget.failedObjects],
              ["Attempted objects", d.failureBudget.attemptedObjects],
              ["Observed failure", d.failureBudget.observedFailurePercent, "%"],
              ["Allowed maximum", d.failureBudget.allowedFailurePercent, "%"],
            ]}
          />
          <p>
            Failed ÷ attempted objects. A run within budget may still contain
            objects that remain present.
          </p>
        </div>
      </div>
      <p className="delete-note">
        Completion measures attempted requests / planned requests, and attempted
        objects / selected objects. It does not imply successful deletion or
        verification.
      </p>
      <details className="delete-section" open>
        <summary>Batching & verification</summary>
        <div className="timing-grid">
          <div className="timing">
            <h3>Batch sizes & counts</h3>
            <Rows
              rows={[
                ["Configured batch size", d.batching.configuredSize],
                ["Observed minimum", d.batching.observedMinSize],
                ["Observed maximum", d.batching.observedMaxSize],
                [
                  "Mean objects / request",
                  d.batching.meanObjectsPerRequest,
                  "objects/request",
                ],
                ["Full batches", d.batching.fullBatchCount],
                ["Full batches share", d.batching.fullBatchPercent, "%"],
                ["Partial batches", d.batching.partialBatchCount],
                ["Partial batches share", d.batching.partialBatchPercent, "%"],
              ]}
            />
            <p>
              A partial-sized batch is different from a request with partial
              success.
            </p>
          </div>
          <div className="timing">
            <h3>Post-delete verification</h3>
            <Rows
              rows={[
                ["Verified absent", d.verification.verifiedAbsent],
                ["Still present", d.verification.stillPresent],
                ["Verification unresolved", d.verification.unresolved],
                ["Not yet verified", d.verification.unverified],
              ]}
            />
            <p>
              Verification unresolved is separate from unresolved delete
              responses.
            </p>
          </div>
          <div className="timing">
            <h3>Phase timings</h3>
            <p>Current phase: {d.currentPhase}</p>
            <Rows
              rows={[
                ["Seed", d.phaseTimings.seed, "s"],
                ["Discovery", d.phaseTimings.discovery, "s"],
                ["Pre-validation", d.phaseTimings.preValidation, "s"],
                ["Scheduled delete", d.phaseTimings.scheduledDelete, "s"],
                ["Drain", d.phaseTimings.drain, "s"],
                ["Post-verification", d.phaseTimings.postVerification, "s"],
                ["Cleanup", d.phaseTimings.cleanup, "s"],
                ["Total wall time", d.phaseTimings.totalWallTime, "s"],
              ]}
            />
          </div>
        </div>
      </details>
      <details className="delete-section">
        <summary>DELETE request timing · mean & percentiles</summary>
        <div className="timing-grid">
          <RequestTiming title="Latency" value={d.requestTiming.latency} />
          <RequestTiming title="Duration" value={d.requestTiming.duration} />
          <div className="timing">
            <h3>Timing scope</h3>
            <p>
              Each measurement belongs to a DELETE request. A batch may contain
              multiple objects; no per-object latency is inferred.
            </p>
          </div>
        </div>
      </details>
      <details className="delete-section">
        <summary>
          Per-bucket counters · {d.buckets.totalBucketCount} buckets
        </summary>
        <p className="delete-note">
          Up to 100 named buckets. {d.buckets.groupedBucketCount} remaining
          buckets grouped as “other”.
        </p>
        <div className="table-wrap bucket-table">
          <table>
            <thead>
              <tr>
                <th>Bucket</th>
                <th>Selected</th>
                <th>Attempted</th>
                <th>Accepted</th>
                <th>Failed</th>
              </tr>
            </thead>
            <tbody>
              {[
                ...d.buckets.items,
                ...(d.buckets.other ? [d.buckets.other] : []),
              ].map((b) => (
                <tr key={b.name}>
                  <td>{b.name}</td>
                  <td>{formatNumber(b.selected)}</td>
                  <td>{formatNumber(b.attempted)}</td>
                  <td>{formatNumber(b.accepted)}</td>
                  <td>{formatNumber(b.failed)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </details>
    </section>
  );
}
