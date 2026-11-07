import type { HistoryRun } from "../types";
import { Status } from "./Status";
import { formatNumber as fmt, phaseName } from "../utils/metrics";

export function RunHistory({
  runs,
  loading,
  error,
  onOpen,
  onRefresh,
}: {
  runs: HistoryRun[];
  loading: boolean;
  error: string;
  onOpen: (runID: string) => void;
  onRefresh: () => void;
}) {
  const localTime = (value?: string) => {
    const date = new Date(value ?? "");
    return Number.isNaN(date.getTime())
      ? "Time unavailable"
      : new Intl.DateTimeFormat(undefined, {
          dateStyle: "medium",
          timeStyle: "short",
        }).format(date);
  };
  return (
    <>
      <div data-spt-legacy="" className="page-heading">
        <div data-spt-legacy="">
          <span data-spt-legacy="" className="eyebrow">
            SAVED BENCHMARKS
          </span>
          <h1 data-spt-legacy="">Run history</h1>
          <p data-spt-legacy="">SPT result folders saved on the project VM.</p>
        </div>
        <div data-spt-legacy="" className="actions">
          <button data-spt-legacy="" onClick={onRefresh} disabled={loading}>
            Refresh history
          </button>
        </div>
      </div>
      {error && (
        <div data-spt-legacy="" className="error" role="alert">
          {error}
        </div>
      )}
      <section data-spt-legacy="" className="panel run-history-panel">
        {loading && !runs.length ? (
          <p data-spt-legacy="" className="history-empty">
            Loading saved runs…
          </p>
        ) : runs.length ? (
          <div data-spt-legacy="" className="table-wrap">
            <table data-spt-legacy="" className="run-history-table">
              <thead data-spt-legacy="">
                <tr data-spt-legacy="">
                  <th data-spt-legacy="">Started · local time</th>
                  <th data-spt-legacy="">Run</th>
                  <th data-spt-legacy="">Workload settings</th>
                  <th data-spt-legacy="">Status</th>
                  <th data-spt-legacy="">
                    Phase results · successful / failed
                  </th>
                  <th data-spt-legacy=""></th>
                </tr>
              </thead>
              <tbody data-spt-legacy="">
                {runs.map((run) => (
                  <tr data-spt-legacy="" key={run.run_id}>
                    <td data-spt-legacy="">{localTime(run.started_at)}</td>
                    <td data-spt-legacy="" className="history-run-id">
                      {run.run_id}
                    </td>
                    <td data-spt-legacy="">
                      {run.options?.object_count ?? "—"} ×{" "}
                      {run.options?.object_size ?? "—"}
                      <small data-spt-legacy="">
                        {run.options?.threads ?? "—"} thread(s)
                      </small>
                    </td>
                    <td data-spt-legacy="">
                      <Status status={run.status} />
                    </td>
                    <td data-spt-legacy="">
                      {run.phase_summaries.length ? (
                        run.phase_summaries.map((phase) => (
                          <span
                            data-spt-legacy=""
                            className="history-phase"
                            key={phase.phase}
                          >
                            {phaseName(phase.phase)} {fmt(phase.successful_ops)}
                            /{fmt(phase.failed_ops)}
                          </span>
                        ))
                      ) : (
                        <span data-spt-legacy="" className="subtle">
                          No phase totals saved
                        </span>
                      )}
                    </td>
                    <td data-spt-legacy="">
                      <button
                        data-spt-legacy=""
                        onClick={() => onOpen(run.run_id)}
                      >
                        View results
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div data-spt-legacy="" className="history-empty">
            <h2 data-spt-legacy="">No saved benchmark runs</h2>
            <p data-spt-legacy="">
              Completed and partial SPT results will appear here.
            </p>
          </div>
        )}
        <p data-spt-legacy="" className="history-footnote">
          Times are shown in this computer’s local time. Older runs may have
          partial results if SPT stopped before writing its final summaries.
        </p>
      </section>
    </>
  );
}
