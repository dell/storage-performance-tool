import { Button, Text, Title } from "@mantine/core";
import { useRuns } from "../api/queries";
import { Status } from "./Status";
import { formatNumber as fmt, phaseName } from "../utils/metrics";

export function RunHistory({
  opening,
  error: openingError,
  onOpen,
  onRefresh,
}: {
  opening: boolean;
  error: string;
  onOpen: (runID: string) => void;
  onRefresh: () => void;
}) {
  const history = useRuns();
  const runs = Array.isArray(history.data?.runs) ? history.data.runs : [];
  const loading = history.isFetching || opening;
  const error = openingError || history.error?.message || "";
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
          <Title order={1}>Run history</Title>
          <Text size="sm" className="mt-xs wrap-anywhere">
            SPT result folders saved on the project VM.
          </Text>
        </div>
        <div data-spt-legacy="" className="actions">
          <Button
            onClick={() => {
              onRefresh();
              void history.refetch();
            }}
            disabled={loading}
          >
            Refresh history
          </Button>
        </div>
      </div>
      {error && (
        <div data-spt-legacy="" className="error" role="alert">
          {error}
        </div>
      )}
      <section data-spt-legacy="" className="panel run-history-panel">
        {loading && !runs.length ? (
          <Text className="px-section py-xxl text-center">
            Loading saved runs…
          </Text>
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
                      <Button onClick={() => onOpen(run.run_id)}>
                        View results
                      </Button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div data-spt-legacy="" className="history-empty">
            <Title order={2}>No saved benchmark runs</Title>
            <Text className="mt-xs">
              Completed and partial SPT results will appear here.
            </Text>
          </div>
        )}
        <Text size="caption" className="px-comfortable pb-md">
          Times are shown in this computer’s local time. Older runs may have
          partial results if SPT stopped before writing its final summaries.
        </Text>
      </section>
    </>
  );
}
