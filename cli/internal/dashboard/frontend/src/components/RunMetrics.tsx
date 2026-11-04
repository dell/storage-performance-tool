import { Button, Text, Title } from "@mantine/core";
import type { Metrics, Row } from "../types";
import {
  elapsed,
  formatNumber as fmt,
  phaseCode,
  phaseName,
  utc,
} from "../utils/metrics";

export function PhaseResults({
  metrics: m,
  onSelect,
}: {
  metrics: Metrics;
  onSelect: (phase: string) => void;
}) {
  return (
    <section data-spt-legacy="" className="panel">
      <div data-spt-legacy="" className="section-heading">
        <div data-spt-legacy="">
          <Title order={2}>Phase results</Title>
          <Text size="caption" className="mt-[5px] max-[760px]:max-w-[250px]">
            Each phase has its own counters and timing.
          </Text>
        </div>
      </div>
      {m.phase_summaries?.length ? (
        <div data-spt-legacy="" className="table-wrap">
          <table data-spt-legacy="">
            <thead data-spt-legacy="">
              <tr data-spt-legacy="">
                {[
                  "Phase",
                  "Successful",
                  "Failed",
                  "Corrupt",
                  "Duration",
                  "IOPS",
                  "MiB/s",
                  "P50 (µs)",
                ].map((h) => (
                  <th data-spt-legacy="" key={h} scope="col">
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody data-spt-legacy="">
              {m.phase_summaries.map((r) => (
                <tr data-spt-legacy="" key={r.phase}>
                  <td data-spt-legacy="">
                    <Button
                      variant="subtle"
                      size="compact-xs"
                      onClick={() => onSelect(phaseCode(r.phase))}
                    >
                      {phaseName(r.phase)}
                    </Button>
                  </td>
                  <td data-spt-legacy="">{fmt(r.successful_ops)}</td>
                  <td data-spt-legacy="">{fmt(r.failed_ops)}</td>
                  <td data-spt-legacy="">{fmt(r.corrupt_ops)}</td>
                  <td data-spt-legacy="">{elapsed(r.duration_seconds)}</td>
                  <td data-spt-legacy="">{fmt(r.iops, 2)}</td>
                  <td data-spt-legacy="">{fmt(r.bandwidth_mib_s, 2)}</td>
                  <td data-spt-legacy="">{fmt(r.p50_latency_us, 1)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <Text size="sm" className="my-comfortable">
          Final phase results will appear as SPT writes its output.
        </Text>
      )}
    </section>
  );
}

export function MeasuredDetails({
  row,
  phase,
  metrics: m,
}: {
  row?: Row;
  phase: string;
  metrics: Metrics;
}) {
  const groups: { title: string; values: [string, string][] }[] = [
    {
      title: "Counts and data",
      values: [
        ["Successful operations", fmt(row?.cumulative_ops)],
        ["Failed operations", fmt(row?.count_failed)],
        ["Corrupt operations", fmt(row?.corrupt_ops)],
        ["Bytes processed", fmt(row?.bytes_processed)],
        ["Phase duration", elapsed(row?.duration_seconds)],
        ["Average IOPS", fmt(row?.average_iops ?? row?.iops, 2)],
      ],
    },
    {
      title: "Latency · µs",
      values: [
        ["P50", fmt(row?.p50_latency_us, 1)],
        ["P99", fmt(row?.latency_p99_us, 1)],
        ["Mean", fmt(row?.latency_mean_us, 1)],
        ["Minimum", fmt(row?.latency_min_us, 1)],
        ["Maximum", fmt(row?.latency_max_us, 1)],
      ],
    },
    {
      title: "Execution",
      values: [
        ["Concurrency", fmt(row?.concurrency)],
        ["Mean concurrency", fmt(row?.concurrency_mean, 2)],
        ["Nodes", fmt(row?.node_count)],
        ["Step", row?.step_id ?? "—"],
        ["Measurement time", utc(row?.timestamp)],
      ],
    },
  ];
  // The sample lists can run to megabytes; the CSV export below carries them.
  const { samples, chart_samples, ...metadata } = m;
  const raw = {
    ...metadata,
    sample_count: (chart_samples ?? samples ?? []).length,
  };
  return (
    <details data-spt-legacy="" className="panel">
      <summary data-spt-legacy="">
        SPT measured details{" "}
        <span data-spt-legacy="" className="summary-note">
          {phaseName(phase)} phase
        </span>
      </summary>
      <Text size="sm" className="my-comfortable">
        All fields available from the dashboard API are retained below. A dash
        means SPT did not report a value.
      </Text>
      {m.spt_summary_report && (
        <details data-spt-legacy="" className="raw-details">
          <summary data-spt-legacy="">SPT run summary</summary>
          <pre data-spt-legacy="">{m.spt_summary_report}</pre>
        </details>
      )}
      <div data-spt-legacy="" className="guided-core-grid">
        {groups.map((g) => (
          <section data-spt-legacy="" className="timing" key={g.title}>
            <Title order={3}>{g.title}</Title>
            <dl data-spt-legacy="">
              {g.values.map(([name, value]) => (
                <div data-spt-legacy="" key={name}>
                  <dt data-spt-legacy="">{name}</dt>
                  <dd data-spt-legacy="">{value}</dd>
                </div>
              ))}
            </dl>
          </section>
        ))}
      </div>
      <details data-spt-legacy="" className="raw-details">
        <summary data-spt-legacy="">Source measurements and metadata</summary>
        <div data-spt-legacy="" className="actions">
          <Button
            component="a"
            href={`/api/runs/${encodeURIComponent(m.run_id)}/csv?kind=samples`}
            download
          >
            Export all samples CSV
          </Button>
        </div>
        <pre data-spt-legacy="">{JSON.stringify(raw, null, 2)}</pre>
      </details>
    </details>
  );
}
