import {
  Button,
  Checkbox,
  Paper,
  Radio,
  Table,
  Text,
  Title,
} from "@mantine/core";
import { useState } from "react";
import { useDebouncedValue } from "@mantine/hooks";
import type { CatalogRun } from "../types";
import {
  useArtifacts,
  useCatalog,
  useLastSuccessfulData,
} from "../api/queries";
import { Status } from "./Status";
import { RunLog } from "./RunLog";
import { formatNumber as fmt } from "../utils/metrics";
import {
  maxCompare,
  previousRun,
  runSettings,
  toggleSelection,
  type CatalogFilters,
} from "../utils/catalog";

const integrityText: Record<CatalogRun["integrity"], string> = {
  passed: "Integrity passed",
  failed: "Integrity failed",
  incomplete: "Integrity incomplete",
  not_recorded: "Integrity not recorded",
};

export function Catalog({
  onCompare,
}: {
  onCompare: (ids: string[], baseline: string) => void;
}) {
  const [filters, setFilters] = useState<CatalogFilters>({
    q: "",
    source: "",
    status: "",
  });
  const [debouncedFilters] = useDebouncedValue(filters, 200);
  const catalog = useCatalog(debouncedFilters);
  const unfiltered = useCatalog({ q: "", source: "", status: "" });
  const data = useLastSuccessfulData(catalog.data);
  const allRuns = unfiltered.data?.runs ?? [];
  const error = catalog.error?.message ?? "";
  const loading = catalog.isFetching;
  const [selected, setSelected] = useState<string[]>([]);
  const [baseline, setBaseline] = useState("");
  const [open, setOpen] = useState("");
  const effectiveBaseline = selected.includes(baseline)
    ? baseline
    : (selected[0] ?? "");
  const runs = data?.runs ?? [];
  return (
    <>
      <div data-spt-legacy="" className="page-heading">
        <div data-spt-legacy="">
          <span data-spt-legacy="" className="eyebrow">
            RESULTS CATALOG · READ ONLY
          </span>
          <Title order={1}>Results catalog</Title>
          <Text size="sm" className="mt-xs wrap-anywhere">
            Dashboard and CLI runs found under{" "}
            {data?.root ?? "the results folder"}. Nothing here changes or
            deletes results.
          </Text>
        </div>
        <div data-spt-legacy="" className="actions">
          <Button
            onClick={() => {
              void catalog.refetch();
              if (
                debouncedFilters.q.trim() ||
                debouncedFilters.source ||
                debouncedFilters.status
              )
                void unfiltered.refetch();
            }}
            disabled={loading}
          >
            Rescan
          </Button>
          <Button
            variant="filled"
            disabled={selected.length < 2}
            onClick={() => onCompare(selected, effectiveBaseline)}
          >
            Compare {selected.length || ""} selected
          </Button>
        </div>
      </div>
      <section
        data-spt-legacy=""
        className="panel catalog-filters"
        aria-label="Filters"
      >
        <label data-spt-legacy="">
          Search
          <input
            data-spt-legacy=""
            type="search"
            value={filters.q}
            placeholder="Run name, label, size, endpoint host"
            onChange={(e) => setFilters({ ...filters, q: e.target.value })}
          />
        </label>
        <label data-spt-legacy="">
          Source
          <select
            data-spt-legacy=""
            value={filters.source}
            onChange={(e) =>
              setFilters({
                ...filters,
                source: e.target.value as CatalogFilters["source"],
              })
            }
          >
            <option data-spt-legacy="" value="">
              All sources
            </option>
            <option data-spt-legacy="" value="dashboard">
              Dashboard
            </option>
            <option data-spt-legacy="" value="cli">
              CLI
            </option>
          </select>
        </label>
        <label data-spt-legacy="">
          Status
          <select
            data-spt-legacy=""
            value={filters.status}
            onChange={(e) =>
              setFilters({
                ...filters,
                status: e.target.value as CatalogFilters["status"],
              })
            }
          >
            <option data-spt-legacy="" value="">
              All statuses
            </option>
            <option data-spt-legacy="" value="complete">
              Complete
            </option>
            <option data-spt-legacy="" value="partial">
              Partial
            </option>
            <option data-spt-legacy="" value="malformed">
              Malformed
            </option>
            <option data-spt-legacy="" value="failed">
              Integrity failed
            </option>
          </select>
        </label>
        <Text size="sm" className="basis-full">
          Select 2–{maxCompare} runs to compare; the baseline is the run the
          others are checked against. "Compare with previous" pairs this run
          with the next older complete run that shares the same workload, object
          size, threads, and (when recorded) object count, host count, and S3
          driver — not an SPT product version. Falls back to the next older
          complete run when no shape match exists.
        </Text>
      </section>
      {error && (
        <div data-spt-legacy="" className="error" role="alert">
          {error}
        </div>
      )}
      {data?.truncated && (
        <div data-spt-legacy="" className="notice notice-warn">
          The results folder is large; only part of it was scanned.
        </div>
      )}
      <Paper component="section" className="mb-panel">
        {loading && !data ? (
          <Text className="px-section py-xxl text-center">
            Scanning results…
          </Text>
        ) : runs.length ? (
          <Table.ScrollContainer
            minWidth="var(--spt-table-min-width)"
            aria-label="Results catalog scroll area"
          >
            <Table aria-label="Results catalog" stickyHeader>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th scope="col" ta="center">
                    Compare
                  </Table.Th>
                  <Table.Th scope="col" ta="center">
                    Baseline
                  </Table.Th>
                  <Table.Th scope="col">Run</Table.Th>
                  <Table.Th scope="col">Started · UTC</Table.Th>
                  <Table.Th scope="col">Settings</Table.Th>
                  <Table.Th scope="col">Status</Table.Th>
                  <Table.Th scope="col">Phases · IOPS</Table.Th>
                  <Table.Th scope="col" aria-label="Actions"></Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {runs.map((run) => (
                  <CatalogRow
                    key={run.id}
                    run={run}
                    checked={selected.includes(run.id)}
                    full={selected.length >= maxCompare}
                    isBaseline={effectiveBaseline === run.id}
                    open={open === run.id}
                    previous={previousRun(
                      allRuns.length ? allRuns : runs,
                      run.id,
                    )}
                    onToggle={() =>
                      setSelected((s) => toggleSelection(s, run.id))
                    }
                    onBaseline={() => setBaseline(run.id)}
                    onOpen={() => setOpen(open === run.id ? "" : run.id)}
                    onComparePrevious={(prevId) =>
                      onCompare([run.id, prevId], prevId)
                    }
                  />
                ))}
              </Table.Tbody>
            </Table>
          </Table.ScrollContainer>
        ) : (
          <div data-spt-legacy="" className="history-empty">
            <Title order={2}>No runs found</Title>
            <Text className="mt-xs">
              {filters.q || filters.source || filters.status
                ? "No runs match these filters."
                : "SPT result folders will appear here after a run."}
            </Text>
          </div>
        )}
        <Text size="caption" className="px-comfortable pb-md">
          {data ? `${runs.length} shown of ${data.total} found.` : ""} Malformed
          or partial folders are listed with what is missing.
        </Text>
      </Paper>
    </>
  );
}

function CatalogRow({
  run,
  checked,
  full,
  isBaseline,
  open,
  previous,
  onToggle,
  onBaseline,
  onOpen,
  onComparePrevious,
}: {
  run: CatalogRun;
  checked: boolean;
  full: boolean;
  isBaseline: boolean;
  open: boolean;
  previous: CatalogRun | null;
  onToggle: () => void;
  onBaseline: () => void;
  onOpen: () => void;
  onComparePrevious: (prevId: string) => void;
}) {
  return (
    <>
      <Table.Tr
        bg={
          run.status === "malformed"
            ? "var(--spt-status-warning-background)"
            : undefined
        }
      >
        <Table.Td ta="center">
          <Checkbox
            className="inline-flex"
            aria-label={`Compare ${run.id}`}
            checked={checked}
            disabled={!checked && (full || run.status === "malformed")}
            onChange={onToggle}
          />
        </Table.Td>
        <Table.Td ta="center">
          <Radio
            className="inline-flex"
            name="baseline"
            aria-label={`Use ${run.id} as baseline`}
            checked={isBaseline}
            disabled={!checked}
            onChange={onBaseline}
          />
        </Table.Td>
        <Table.Td
          ff="monospace"
          fz="caption"
          className="min-w-[180px] max-w-[300px] whitespace-normal wrap-anywhere"
        >
          {run.id}
          <Text
            component="small"
            size="caption"
            c="var(--spt-text-muted)"
            lh="normal"
            className="mt-[3px] block"
          >
            {run.source === "dashboard" ? "Dashboard" : "CLI"}
            {run.label ? ` · ${run.label}` : ""}
          </Text>
        </Table.Td>
        <Table.Td>
          {run.started_at
            ? run.started_at
                .replace("T", " ")
                .replace(/\.\d+/, "")
                .replace("Z", "")
            : "—"}
        </Table.Td>
        <Table.Td className="min-w-[160px] whitespace-normal">
          {runSettings(run)}
        </Table.Td>
        <Table.Td>
          <Status status={run.status} />
          <Text
            component="small"
            size="caption"
            c="var(--spt-text-muted)"
            lh="normal"
            className="mt-[3px] block"
          >
            {integrityText[run.integrity]}
          </Text>
        </Table.Td>
        <Table.Td>
          {run.phases.length ? (
            run.phases.map((p) => (
              <Text
                component="span"
                size="caption"
                c="var(--spt-text-secondary)"
                lh="normal"
                className="my-[3px] mr-compact inline-block"
                key={p.key}
              >
                {p.key} {fmt(p.iops, 1)}
              </Text>
            ))
          ) : (
            <Text
              component="span"
              size="xs"
              c="var(--spt-text-subtle)"
              lh="normal"
            >
              No phase totals
            </Text>
          )}
        </Table.Td>
        <Table.Td>
          <div className="flex flex-col items-stretch gap-[0.35rem]">
            <Button
              className="whitespace-nowrap"
              aria-expanded={open}
              onClick={onOpen}
            >
              {open ? "Hide" : "Details"}
            </Button>
            {run.status === "complete" && previous && (
              <Button
                className="whitespace-nowrap"
                type="button"
                title={`Compare with ${previous.name || previous.id}`}
                onClick={() => onComparePrevious(previous.id)}
              >
                Compare with previous
              </Button>
            )}
          </div>
        </Table.Td>
      </Table.Tr>
      {open && (
        <Table.Tr>
          <Table.Td
            bg="var(--spt-surface-subtle)"
            p="md"
            className="whitespace-normal wrap-anywhere"
            colSpan={8}
          >
            {run.issues.length > 0 && (
              <ul data-spt-legacy="" className="catalog-issues">
                {run.issues.map((i) => (
                  <li data-spt-legacy="" key={i}>
                    {i}
                  </li>
                ))}
              </ul>
            )}
            <dl data-spt-legacy="" className="record-grid">
              <div data-spt-legacy="">
                <dt data-spt-legacy="">Endpoint host</dt>
                <dd data-spt-legacy="">{run.endpoint_host ?? "—"}</dd>
              </div>
              <div data-spt-legacy="">
                <dt data-spt-legacy="">S3 driver</dt>
                <dd data-spt-legacy="">{run.s3_driver ?? "—"}</dd>
              </div>
              <div data-spt-legacy="">
                <dt data-spt-legacy="">CLI / engine</dt>
                <dd data-spt-legacy="">
                  {run.cli_version ?? "—"} / {run.engine_version ?? "—"}
                </dd>
              </div>
              <div data-spt-legacy="">
                <dt data-spt-legacy="">Summary file</dt>
                <dd data-spt-legacy="">{run.summary_file ?? "—"}</dd>
              </div>
            </dl>
            {run.phases.length > 0 && (
              <Table.ScrollContainer
                minWidth="var(--spt-table-min-width)"
                aria-label="Catalog phase details scroll area"
              >
                <Table aria-label="Catalog phase details" stickyHeader>
                  <Table.Thead>
                    <Table.Tr>
                      <Table.Th scope="col">Phase</Table.Th>
                      <Table.Th scope="col" ta="right">
                        Successful
                      </Table.Th>
                      <Table.Th scope="col" ta="right">
                        Failed
                      </Table.Th>
                      <Table.Th scope="col" ta="right">
                        Corrupt
                      </Table.Th>
                      <Table.Th scope="col" ta="right">
                        IOPS
                      </Table.Th>
                      <Table.Th scope="col" ta="right">
                        MiB/s
                      </Table.Th>
                      <Table.Th scope="col" ta="right">
                        P50 µs
                      </Table.Th>
                      <Table.Th scope="col" ta="right">
                        P99 µs
                      </Table.Th>
                    </Table.Tr>
                  </Table.Thead>
                  <Table.Tbody>
                    {run.phases.map((p) => (
                      <Table.Tr key={p.key}>
                        <Table.Td>{p.key}</Table.Td>
                        <Table.Td ta="right">{fmt(p.successful_ops)}</Table.Td>
                        <Table.Td ta="right">{fmt(p.failed_ops)}</Table.Td>
                        <Table.Td ta="right">{fmt(p.corrupt_ops)}</Table.Td>
                        <Table.Td ta="right">{fmt(p.iops, 2)}</Table.Td>
                        <Table.Td ta="right">
                          {fmt(p.bandwidth_mib_s, 2)}
                        </Table.Td>
                        <Table.Td ta="right">
                          {fmt(p.p50_latency_us, 1)}
                        </Table.Td>
                        <Table.Td ta="right">
                          {fmt(p.p99_latency_us, 1)}
                        </Table.Td>
                      </Table.Tr>
                    ))}
                  </Table.Tbody>
                </Table>
              </Table.ScrollContainer>
            )}
            <RunArtifacts key={run.id} runId={run.id} />
            {run.status !== "malformed" && (
              <RunLog
                base={`/api/catalog/log?id=${encodeURIComponent(run.id)}`}
                title="Engine logs"
              />
            )}
          </Table.Td>
        </Table.Tr>
      )}
    </>
  );
}

function RunArtifacts({ runId }: { runId: string }) {
  const artifacts = useArtifacts(runId);
  const items = artifacts.data?.artifacts;
  const error = artifacts.error?.message ?? "";
  return (
    <section
      data-spt-legacy=""
      className="catalog-artifacts"
      aria-label="Run artifacts"
    >
      <Title order={3} size="md" className="mb-[5px]">
        Artifacts
      </Title>
      <Text size="sm" className="my-comfortable">
        CSV, XML, and log files SPT left in this run folder. Only files the
        dashboard recognizes are listed.
      </Text>
      {error && (
        <div data-spt-legacy="" className="error" role="alert">
          {error}
        </div>
      )}
      {items === undefined && !error ? (
        <Text size="xs" c="var(--spt-text-subtle)">
          Loading artifacts…
        </Text>
      ) : items && items.length === 0 ? (
        <div
          data-spt-legacy=""
          className="history-empty catalog-artifacts-empty"
        >
          <Title order={4} size="md">
            No downloadable artifacts
          </Title>
          <Text className="mt-xs">
            This run folder has no CSV, XML, or log files the dashboard can
            offer. Engine log text may also be unavailable for the same reason.
          </Text>
        </div>
      ) : (
        <ul data-spt-legacy="" className="artifact-list">
          {(items ?? []).map((a) => (
            <li data-spt-legacy="" key={a.name}>
              <Button
                component="a"
                href={`/api/catalog/artifact?id=${encodeURIComponent(runId)}&file=${encodeURIComponent(a.name)}`}
                download={a.name}
              >
                Download {a.type}
              </Button>
              <span data-spt-legacy="" className="artifact-meta">
                <span data-spt-legacy="" className="artifact-kind">
                  {a.kind.toUpperCase()}
                </span>
                <span data-spt-legacy="" className="subtle">
                  {a.name}
                </span>
                <span data-spt-legacy="" className="subtle">
                  {formatBytes(a.size)}
                </span>
              </span>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}

function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MiB`;
}
