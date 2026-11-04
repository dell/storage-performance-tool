import { Button, Paper, Table, Text, Title } from "@mantine/core";
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
      <Paper component="section" className="mb-panel">
        {loading && !runs.length ? (
          <Text className="px-section py-xxl text-center">
            Loading saved runs…
          </Text>
        ) : runs.length ? (
          <Table.ScrollContainer
            minWidth="var(--spt-table-min-width)"
            aria-label="Run history scroll area"
          >
            <Table aria-label="Run history" stickyHeader>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th scope="col">Started · local time</Table.Th>
                  <Table.Th scope="col">Run</Table.Th>
                  <Table.Th scope="col">Workload settings</Table.Th>
                  <Table.Th scope="col">Status</Table.Th>
                  <Table.Th scope="col">
                    Phase results · successful / failed
                  </Table.Th>
                  <Table.Th scope="col" aria-label="Actions"></Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {runs.map((run) => (
                  <Table.Tr key={run.run_id}>
                    <Table.Td>{localTime(run.started_at)}</Table.Td>
                    <Table.Td ff="monospace" fz="caption">
                      {run.run_id}
                    </Table.Td>
                    <Table.Td>
                      {run.options?.object_count ?? "—"} ×{" "}
                      {run.options?.object_size ?? "—"}
                      <Text
                        component="small"
                        size="xs"
                        c="var(--spt-text-muted)"
                        lh="normal"
                        className="mt-[5px] block"
                      >
                        {run.options?.threads ?? "—"} thread(s)
                      </Text>
                    </Table.Td>
                    <Table.Td>
                      <Status status={run.status} />
                    </Table.Td>
                    <Table.Td>
                      {run.phase_summaries.length ? (
                        run.phase_summaries.map((phase) => (
                          <Text
                            component="span"
                            size="caption"
                            c="var(--spt-text-secondary)"
                            lh="normal"
                            className="my-[3px] mr-compact inline-block"
                            key={phase.phase}
                          >
                            {phaseName(phase.phase)} {fmt(phase.successful_ops)}
                            /{fmt(phase.failed_ops)}
                          </Text>
                        ))
                      ) : (
                        <Text
                          component="span"
                          size="xs"
                          c="var(--spt-text-subtle)"
                          lh="normal"
                        >
                          No phase totals saved
                        </Text>
                      )}
                    </Table.Td>
                    <Table.Td>
                      <Button onClick={() => onOpen(run.run_id)}>
                        View results
                      </Button>
                    </Table.Td>
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          </Table.ScrollContainer>
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
      </Paper>
    </>
  );
}
