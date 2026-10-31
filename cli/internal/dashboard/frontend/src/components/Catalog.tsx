import { useEffect, useState } from "react";
import type { CatalogList, CatalogRun } from "../types";
import { Status } from "./Status";
import { RunLog } from "./RunLog";
import { formatNumber as fmt } from "../utils/metrics";
import {
  catalogQuery,
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
  const [data, setData] = useState<CatalogList | null>(null);
  const [allRuns, setAllRuns] = useState<CatalogRun[]>([]);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [selected, setSelected] = useState<string[]>([]);
  const [baseline, setBaseline] = useState("");
  const [open, setOpen] = useState("");
  const [version, setVersion] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    const timer = setTimeout(() => {
      setLoading(true);
      fetch(catalogQuery(filters), {
        cache: "no-store",
        signal: controller.signal,
      })
        .then(async (r) => {
          const body = await r.json();
          if (!r.ok) throw new Error(body.error ?? "Could not read results.");
          setData(body as CatalogList);
          setError("");
        })
        .catch((e) => {
          if (!controller.signal.aborted)
            setError(
              e instanceof Error ? e.message : "Could not read results.",
            );
        })
        .finally(() => {
          if (!controller.signal.aborted) setLoading(false);
        });
    }, 200);
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [filters, version]);
  // Unfiltered snapshot for "Compare with previous" so a status/source filter
  // cannot hide the next older complete match.
  useEffect(() => {
    const controller = new AbortController();
    fetch(catalogQuery({ q: "", source: "", status: "" }), {
      cache: "no-store",
      signal: controller.signal,
    })
      .then(async (r) => {
        const body = await r.json();
        if (!r.ok) throw new Error(body.error ?? "Could not read results.");
        if (controller.signal.aborted) return;
        setAllRuns((body as CatalogList).runs ?? []);
      })
      .catch(() => {
        /* filtered list still works; previous-run just falls back below */
      });
    return () => controller.abort();
  }, [version]);
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
          <h1 data-spt-legacy="">Results catalog</h1>
          <p data-spt-legacy="">
            Dashboard and CLI runs found under{" "}
            {data?.root ?? "the results folder"}. Nothing here changes or
            deletes results.
          </p>
        </div>
        <div data-spt-legacy="" className="actions">
          <button
            data-spt-legacy=""
            onClick={() => setVersion((v) => v + 1)}
            disabled={loading}
          >
            Rescan
          </button>
          <button
            data-spt-legacy=""
            className="primary"
            disabled={selected.length < 2}
            onClick={() => onCompare(selected, effectiveBaseline)}
          >
            Compare {selected.length || ""} selected
          </button>
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
        <p data-spt-legacy="" className="form-help">
          Select 2–{maxCompare} runs to compare; the baseline is the run the
          others are checked against. "Compare with previous" pairs this run
          with the next older complete run that shares the same workload, object
          size, threads, and (when recorded) object count, host count, and S3
          driver — not an SPT product version. Falls back to the next older
          complete run when no shape match exists.
        </p>
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
      <section data-spt-legacy="" className="panel run-history-panel">
        {loading && !data ? (
          <p data-spt-legacy="" className="history-empty">
            Scanning results…
          </p>
        ) : runs.length ? (
          <div data-spt-legacy="" className="table-wrap">
            <table
              data-spt-legacy=""
              className="run-history-table catalog-table"
            >
              <thead data-spt-legacy="">
                <tr data-spt-legacy="">
                  <th data-spt-legacy="">Compare</th>
                  <th data-spt-legacy="">Baseline</th>
                  <th data-spt-legacy="">Run</th>
                  <th data-spt-legacy="">Started · UTC</th>
                  <th data-spt-legacy="">Settings</th>
                  <th data-spt-legacy="">Status</th>
                  <th data-spt-legacy="">Phases · IOPS</th>
                  <th data-spt-legacy=""></th>
                </tr>
              </thead>
              <tbody data-spt-legacy="">
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
              </tbody>
            </table>
          </div>
        ) : (
          <div data-spt-legacy="" className="history-empty">
            <h2 data-spt-legacy="">No runs found</h2>
            <p data-spt-legacy="">
              {filters.q || filters.source || filters.status
                ? "No runs match these filters."
                : "SPT result folders will appear here after a run."}
            </p>
          </div>
        )}
        <p data-spt-legacy="" className="history-footnote">
          {data ? `${runs.length} shown of ${data.total} found.` : ""} Malformed
          or partial folders are listed with what is missing.
        </p>
      </section>
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
      <tr
        data-spt-legacy=""
        className={run.status === "malformed" ? "catalog-malformed" : ""}
      >
        <td data-spt-legacy="">
          <input
            data-spt-legacy=""
            type="checkbox"
            aria-label={`Compare ${run.id}`}
            checked={checked}
            disabled={!checked && (full || run.status === "malformed")}
            onChange={onToggle}
          />
        </td>
        <td data-spt-legacy="">
          <input
            data-spt-legacy=""
            type="radio"
            name="baseline"
            aria-label={`Use ${run.id} as baseline`}
            checked={isBaseline}
            disabled={!checked}
            onChange={onBaseline}
          />
        </td>
        <td data-spt-legacy="" className="history-run-id">
          {run.id}
          <small data-spt-legacy="">
            {run.source === "dashboard" ? "Dashboard" : "CLI"}
            {run.label ? ` · ${run.label}` : ""}
          </small>
        </td>
        <td data-spt-legacy="">
          {run.started_at
            ? run.started_at
                .replace("T", " ")
                .replace(/\.\d+/, "")
                .replace("Z", "")
            : "—"}
        </td>
        <td data-spt-legacy="" className="catalog-settings">
          {runSettings(run)}
        </td>
        <td data-spt-legacy="">
          <Status status={run.status} />
          <small data-spt-legacy="">{integrityText[run.integrity]}</small>
        </td>
        <td data-spt-legacy="">
          {run.phases.length ? (
            run.phases.map((p) => (
              <span data-spt-legacy="" className="history-phase" key={p.key}>
                {p.key} {fmt(p.iops, 1)}
              </span>
            ))
          ) : (
            <span data-spt-legacy="" className="subtle">
              No phase totals
            </span>
          )}
        </td>
        <td data-spt-legacy="" className="catalog-row-actions">
          <button data-spt-legacy="" aria-expanded={open} onClick={onOpen}>
            {open ? "Hide" : "Details"}
          </button>
          {run.status === "complete" && previous && (
            <button
              data-spt-legacy=""
              type="button"
              title={`Compare with ${previous.name || previous.id}`}
              onClick={() => onComparePrevious(previous.id)}
            >
              Compare with previous
            </button>
          )}
        </td>
      </tr>
      {open && (
        <tr data-spt-legacy="" className="catalog-detail">
          <td data-spt-legacy="" colSpan={8}>
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
              <div data-spt-legacy="" className="table-wrap">
                <table data-spt-legacy="" className="run-history-table">
                  <thead data-spt-legacy="">
                    <tr data-spt-legacy="">
                      <th data-spt-legacy="">Phase</th>
                      <th data-spt-legacy="">Successful</th>
                      <th data-spt-legacy="">Failed</th>
                      <th data-spt-legacy="">Corrupt</th>
                      <th data-spt-legacy="">IOPS</th>
                      <th data-spt-legacy="">MiB/s</th>
                      <th data-spt-legacy="">P50 µs</th>
                      <th data-spt-legacy="">P99 µs</th>
                    </tr>
                  </thead>
                  <tbody data-spt-legacy="">
                    {run.phases.map((p) => (
                      <tr data-spt-legacy="" key={p.key}>
                        <td data-spt-legacy="">{p.key}</td>
                        <td data-spt-legacy="">{fmt(p.successful_ops)}</td>
                        <td data-spt-legacy="">{fmt(p.failed_ops)}</td>
                        <td data-spt-legacy="">{fmt(p.corrupt_ops)}</td>
                        <td data-spt-legacy="">{fmt(p.iops, 2)}</td>
                        <td data-spt-legacy="">{fmt(p.bandwidth_mib_s, 2)}</td>
                        <td data-spt-legacy="">{fmt(p.p50_latency_us, 1)}</td>
                        <td data-spt-legacy="">{fmt(p.p99_latency_us, 1)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
            <RunArtifacts key={run.id} runId={run.id} />
            {run.status !== "malformed" && (
              <RunLog
                base={`/api/catalog/log?id=${encodeURIComponent(run.id)}`}
                title="Engine logs"
              />
            )}
          </td>
        </tr>
      )}
    </>
  );
}

type Artifact = {
  name: string;
  kind: string;
  type: string;
  size: number;
};

function RunArtifacts({ runId }: { runId: string }) {
  const [items, setItems] = useState<Artifact[] | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    const controller = new AbortController();
    fetch(`/api/catalog/artifacts?id=${encodeURIComponent(runId)}`, {
      cache: "no-store",
      signal: controller.signal,
    })
      .then(async (r) => {
        const body = await r.json();
        if (!r.ok) throw new Error(body.error ?? "Could not list artifacts.");
        if (controller.signal.aborted) return;
        setItems((body.artifacts as Artifact[]) ?? []);
        setError("");
      })
      .catch((e) => {
        if (!controller.signal.aborted)
          setError(
            e instanceof Error ? e.message : "Could not list artifacts.",
          );
      });
    return () => controller.abort();
  }, [runId]);
  return (
    <section
      data-spt-legacy=""
      className="catalog-artifacts"
      aria-label="Run artifacts"
    >
      <h3 data-spt-legacy="">Artifacts</h3>
      <p data-spt-legacy="" className="form-help">
        CSV, XML, and log files SPT left in this run folder. Only files the
        dashboard recognizes are listed.
      </p>
      {error && (
        <div data-spt-legacy="" className="error" role="alert">
          {error}
        </div>
      )}
      {items === null && !error ? (
        <p data-spt-legacy="" className="subtle">
          Loading artifacts…
        </p>
      ) : items && items.length === 0 ? (
        <div
          data-spt-legacy=""
          className="history-empty catalog-artifacts-empty"
        >
          <h4 data-spt-legacy="">No downloadable artifacts</h4>
          <p data-spt-legacy="">
            This run folder has no CSV, XML, or log files the dashboard can
            offer. Engine log text may also be unavailable for the same reason.
          </p>
        </div>
      ) : (
        <ul data-spt-legacy="" className="artifact-list">
          {(items ?? []).map((a) => (
            <li data-spt-legacy="" key={a.name}>
              <a
                data-spt-legacy=""
                className="button-link"
                href={`/api/catalog/artifact?id=${encodeURIComponent(runId)}&file=${encodeURIComponent(a.name)}`}
                download={a.name}
              >
                Download {a.type}
              </a>
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
